use tracing::info;

use crate::to_num;
use crate::utils::to_sql_type;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use tokio_postgres::types::ToSql;
use tokio_postgres::Row;
use uuid::Uuid;

use super::db_client::DBClient;
use num_decimal::Num;

use super::Settings;
use anyhow::bail;
use anyhow::Result;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum LockerStatus {
    Active,
    Disabled,
    Finished,
}

impl fmt::Display for LockerStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum StopType {
    Percent,
    ATR,
    Combo,
}

impl FromStr for StopType {
    type Err = String;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        match val {
            "Percent" => Ok(StopType::Percent),
            "ATR" => Ok(StopType::ATR),
            "Combo" => Ok(StopType::Combo),
            _ => Err(format!("Failed to parse stop type, unknown: {}", val)),
        }
    }
}

impl fmt::Display for StopType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TransactionType {
    Order,
    Position,
}

impl fmt::Display for TransactionType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug)]
pub struct Locker {
    stops: HashMap<String, TrailingStop>,
    settings: Settings,
    db: Arc<DBClient>,
}

impl Locker {
    pub fn new(settings: &Settings, db: &Arc<DBClient>) -> Self {
        Locker {
            stops: HashMap::new(),
            settings: settings.clone(),
            db: Arc::clone(db),
        }
    }

    pub async fn load_stop_from_db(&mut self, symbol: &str, local_id: Uuid) {
        let stop = match self
            .db
            .fetch("locker", vec!["local_id"], vec![to_sql_type(local_id)])
            .await
        {
            Ok(rows) => {
                if rows.len() == 0 {
                    panic!(
                        "Failed to find locker with local_id: {}, closing app",
                        local_id
                    );
                }
                TrailingStop::load_from_db(&rows[0])
            }
            Err(err) => panic!(
                "Db fetch for local_id: {} failed, error={}, closing app",
                err, local_id
            ),
        };
        self.stops.insert(symbol.to_string(), stop);
    }

    pub fn create_new_stop(
        &mut self,
        symbol: &str,
        entry_price: Num,
        strategy: &str,
        t_type: TransactionType,
    ) -> Uuid {
        let strategy_cfg = &self.settings.strategies.configuration[strategy];
        let stop = TrailingStop::new(
            strategy,
            symbol,
            entry_price,
            strategy_cfg.trailing_size,
            t_type,
        );
        let stop_id = stop.local_id;
        if self.stops.contains_key(symbol) {
            info!(
                "Strategy[{}] locker monitoring update symbol: {} entry price: {} transaction: {:?}",
                strategy,
                symbol,
                entry_price.round_with(2),
                t_type
            );
            *self.stops.get_mut(symbol).unwrap() = stop;
        } else {
            info!(
                "Strategy[{}] locker monitoring new symbol: {} entry price: {} transaction: {:?}",
                strategy,
                symbol,
                entry_price.round_with(2),
                t_type
            );
            self.stops.insert(symbol.to_string(), stop);
        }
        stop_id
    }

    pub fn complete(&mut self, symbol: &str) {
        if let Some(mut stop) = self.stops.remove(symbol) {
            info!("Locker tracking symbol: {symbol} marked as complete");
            stop.status = LockerStatus::Finished;
            stop.write_to_db(&self.db);
        }
    }

    pub fn revive(&mut self, symbol: &str) {
        if let Some(stop) = self.stops.get_mut(symbol) {
            info!("Locker tracking symbol: {symbol} re-enabled");
            stop.status = LockerStatus::Active;
        }
    }

    pub fn get_transaction_type(&mut self, symbol: &str) -> &TransactionType {
        &self.stops[symbol].t_type
    }

    pub fn get_status(&self, symbol: &str) -> LockerStatus {
        self.stops[symbol].status
    }

    pub fn print_stop(&mut self, symbol: &str) -> String {
        format!("{}", self.stops[symbol].to_string())
    }

    pub fn update_status(&mut self, symbol: &str, status: LockerStatus) {
        if let Some(locker) = self.stops.get_mut(symbol) {
            locker.status = status
        }
    }

    pub fn should_close(&mut self, symbol: &str, trade_price: &Num) -> bool {
        if !self.stops.contains_key(symbol) {
            info!("Symbol: {symbol:?} not being tracked in locker");
            return false;
        }
        if let Some(stop) = &mut self.stops.get_mut(symbol) {
            let stop_price = stop.price_update(trade_price.clone(), &self.db);
            if stop_price > *trade_price {
                stop.status = LockerStatus::Disabled;
                return true;
            }
        }
        false
    }

    pub fn print_snapshot(&self) {
        for stop in self.stops.values() {
            info!("{}", stop);
        }
    }
}

#[derive(Debug, Clone)]
struct TrailingStop {
    local_id: Uuid,
    strategy: String,
    symbol: String,
    entry_price: Num,
    current_price: Num,
    pivot_points: [(i8, f64, f64); 4],
    high_low: Num,
    stop_price: Num,
    zone: i8,
    stop_type: StopType,
    multiplier: f64,
    status: LockerStatus,
    t_type: TransactionType,
}

impl fmt::Display for TrailingStop {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "symbol[{}], price[{}], stop[{}], zone[{}] status[{}] type[{}]",
            self.symbol,
            self.current_price.round_with(2),
            self.stop_price.round_with(2),
            self.zone,
            self.status,
            self.t_type
        )
    }
}

impl TrailingStop {
    fn new(
        strategy: &str,
        symbol: &str,
        entry_price: Num,
        multiplier: f64,
        t_type: TransactionType,
    ) -> Self {
        let pivot_points = Self::calculate_pivot_points(multiplier);
        let stop_price = entry_price.clone() * to_num!(1.0 - pivot_points[0].1);
        let high_low = entry_price.clone();
        let current_price = entry_price.clone();
        TrailingStop {
            local_id: Uuid::new_v4(),
            strategy: strategy.to_string(),
            symbol: symbol.to_string(),
            entry_price,
            current_price,
            pivot_points,
            high_low,
            stop_price,
            zone: 0,
            stop_type: StopType::Percent,
            multiplier,
            status: LockerStatus::Active,
            t_type,
        }
    }

    fn load_from_db(row: &Row) -> Self {
        let local_id = row.get::<&str, Uuid>("local_id");
        let strategy = row.get::<&str, String>("strategy");
        let symbol = row.get::<&str, String>("symbol");
        let stop_price = to_num!(row.get::<&str, f64>("stop_price"));
        let entry_price = to_num!(row.get::<&str, f64>("entry_price"));
        let stop_type = row.get::<&str, &str>("stop_type").parse().unwrap();
        let multiplier = row.get::<&str, f64>("multiplier");
        let high_low = entry_price.clone();
        let current_price = entry_price.clone();
        let pivot_points = Self::calculate_pivot_points(multiplier);
        TrailingStop {
            local_id,
            strategy,
            symbol,
            entry_price,
            current_price,
            pivot_points,
            high_low,
            stop_price,
            zone: 0,
            stop_type,
            multiplier,
            status: LockerStatus::Active,
            t_type: TransactionType::Position,
        }
    }

    fn calculate_pivot_points(multiplier: f64) -> [(i8, f64, f64); 4] {
        [
            (1, (multiplier / 100.0), 1.0),
            (2, (multiplier * 2.0 / 100.0), 0.0),
            (3, (multiplier * 3.0 / 100.0), 2.0),
            (4, (multiplier * 4.0 / 100.0), 2.0 - (1.0 / multiplier)),
        ]
    }

    fn price_update(&mut self, current_price: Num, db: &Arc<DBClient>) -> Num {
        self.current_price = current_price.clone();
        let price = current_price.to_f64().unwrap();
        let price_change = price - self.high_low.to_f64().unwrap();
        if price_change <= 0.0 || self.status == LockerStatus::Disabled {
            return self.stop_price.clone();
        }
        let entry_price = self.entry_price.to_f64().unwrap();
        let mut stop_loss_level = self.stop_price.to_f64().unwrap();
        for pivot in self.pivot_points.iter() {
            let (zone, percentage_change, new_trail_factor) = pivot;
            match zone {
                4 => {
                    if price > (entry_price * (1.0 + percentage_change)) {
                        // final trail at 1%
                        stop_loss_level = price - (entry_price * 0.01)
                    } else {
                        // close distance X% -> 1%
                        stop_loss_level += price_change * new_trail_factor
                    }
                }
                _ => {
                    if price > entry_price * (1.0 + percentage_change) {
                        continue;
                    }
                    // set trail based on zone
                    stop_loss_level += new_trail_factor * price_change;
                }
            }
            if *zone > self.zone {
                info!(
                    "Price update for symbol: {}, new stop level: {} in zone: {}",
                    self.symbol,
                    self.stop_price.clone().round_with(2),
                    zone
                );
                self.zone = *zone;
                if self.t_type == TransactionType::Position {
                    let _ = self.write_to_db(db);
                }
            }
            break;
        }
        if self.stop_price != to_num!(stop_loss_level) {
            self.stop_price = Num::new((stop_loss_level * 100.0) as i64, 100);
        }
        if current_price > self.high_low {
            self.high_low = current_price;
        }
        //write to db
        self.stop_price.clone()
    }

    async fn write_to_db(&self, db: &Arc<DBClient>) -> Result<()> {
        let (columns, values) = self.serialise();
        if let Err(err) = db.update("locker", columns, values).await {
            bail!("Failed to write to db in locker");
        }
        Ok(())
    }

    fn serialise(&self) -> (Vec<&str>, Vec<Box<dyn ToSql + Sync>>) {
        (
            vec![
                "strategy",
                "symbol",
                "entry_price",
                "stop_price",
                "stop_type",
                "multiplier",
                "status",
                "local_id",
            ],
            vec![
                to_sql_type(self.strategy.clone()),
                to_sql_type(self.symbol.clone()),
                to_sql_type(self.entry_price.round_with(2).to_f64()),
                to_sql_type(self.stop_price.round_with(2).to_f64()),
                to_sql_type(self.stop_type.to_string()),
                to_sql_type(self.multiplier),
                to_sql_type(self.status.to_string()),
                to_sql_type(self.local_id),
            ],
        )
    }
}
