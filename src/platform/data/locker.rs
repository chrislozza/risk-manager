use anyhow::Ok;
use tracing::info;
use tracing::warn;

use crate::to_num;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

use super::db_client::DBClient;
use sqlx::postgres::PgRow;
use sqlx::FromRow;
use sqlx::Row;

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

impl FromStr for LockerStatus {
    type Err = String;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        match val {
            "Active" => std::result::Result::Ok(LockerStatus::Active),
            "Disabled" => std::result::Result::Ok(LockerStatus::Disabled),
            "Finished" => std::result::Result::Ok(LockerStatus::Finished),
            _ => Err(format!("Failed to parse stop type, unknown: {}", val)),
        }
    }
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
            "Percent" => std::result::Result::Ok(StopType::Percent),
            "ATR" => std::result::Result::Ok(StopType::ATR),
            "Combo" => std::result::Result::Ok(StopType::Combo),
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
    stops: HashMap<Uuid, TrailingStop>,
    settings: Settings,
    db: Arc<DBClient>,
}

impl Locker {
    pub fn new(settings: &Settings, db: Arc<DBClient>) -> Self {
        Locker {
            stops: HashMap::new(),
            settings: settings.clone(),
            db,
        }
    }

    pub async fn startup(&mut self) -> Result<()> {
        let columns = vec!["status"];
        let stmt = self
            .db
            .query_builder
            .prepare_fetch_statement("locker", &columns);
        let status = LockerStatus::Active.to_string();
        let rows = match sqlx::query(&stmt)
            .bind(status)
            .fetch_all(&self.db.pool)
            .await
        {
            sqlx::Result::Ok(val) => val,
            Err(_err) => panic!("Failed to fetch locker entries from db, closing app"),
        };

        for row in &rows {
            let local_id = row.try_get("local_id")?;
            let stop = TrailingStop::load_from_db(local_id, &self.db).await?;
            self.stops.insert(local_id, stop);
        }
        Ok(())
    }

    pub async fn create_new_stop(
        &mut self,
        symbol: &str,
        strategy: &str,
        entry_price: Num,
        t_type: TransactionType,
    ) -> Uuid {
        let strategy_cfg = &self.settings.strategies.configuration[strategy];
        let stop = TrailingStop::new(
            strategy,
            symbol,
            entry_price.clone(),
            strategy_cfg.trailing_size,
            t_type,
            &self.db,
        )
        .await;
        let stop_id = stop.local_id;
        info!(
            "Strategy[{}] locker monitoring new symbol: {} entry price: {} transaction: {:?}",
            strategy,
            symbol,
            entry_price.round_with(2),
            t_type
        );
        self.stops.insert(stop_id, stop);
        stop_id
    }

    pub fn update_stop(&mut self, stop_id: Uuid, entry_price: Num) {
        if let Some(stop) = self.stops.get_mut(&stop_id) {
            stop.refresh_entry_price(entry_price)
        } else {
            warn!("Failed to update stop, cannot find stop_id: {} ", stop_id);
        }
    }

    pub async fn complete(&mut self, locker_id: Uuid) {
        if let Some(mut stop) = self.stops.remove(&locker_id) {
            info!("Locker tracking symbol: {} marked as complete", stop.symbol);
            stop.status = LockerStatus::Finished;
            stop.persist_to_db(&self.db).await.unwrap();
        }
    }

    pub fn revive(&mut self, locker_id: Uuid) {
        if let Some(stop) = self.stops.get_mut(&locker_id) {
            info!("Locker tracking symbol: {} re-enabled", stop.symbol);
            stop.status = LockerStatus::Active;
        }
    }

    pub fn get_transaction_type(&mut self, locker_id: &Uuid) -> TransactionType {
        self.stops[locker_id].t_type
    }

    pub fn get_status(&self, locker_id: &Uuid) -> LockerStatus {
        self.stops[locker_id].status
    }

    pub fn print_stop(&mut self, locker_id: &Uuid) -> String {
        format!("{}", self.stops[locker_id])
    }

    pub fn update_status(&mut self, locker_id: &Uuid, status: LockerStatus) {
        if let Some(locker) = self.stops.get_mut(locker_id) {
            locker.status = status
        }
    }

    pub fn should_close(&mut self, locker_id: &Uuid, trade_price: &Num) -> bool {
        if !self.stops.contains_key(locker_id) {
            info!("Symbol: {locker_id:?} not being tracked in locker");
            return false;
        }
        if let Some(stop) = &mut self.stops.get_mut(locker_id) {
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
    watermark: Num,
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

impl FromRow<'_, PgRow> for TrailingStop {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        let multiplier: f64 = row.try_get("quantity")?;
        let entry_price: f64 = row.try_get("entry_price")?;
        let stop_price: f64 = row.try_get("stop_price")?;

        sqlx::Result::Ok(Self {
            local_id: row.try_get("local_id")?,
            strategy: row.try_get("strategy")?,
            symbol: row.try_get("symbol")?,
            entry_price: to_num!(entry_price),
            current_price: to_num!(entry_price),
            pivot_points: Self::calculate_pivot_points(multiplier),
            watermark: to_num!(stop_price),
            stop_price: to_num!(stop_price),
            zone: 0,
            multiplier,
            stop_type: StopType::from_str(row.try_get("stop_type")?).unwrap(),
            status: LockerStatus::from_str(row.try_get("status")?).unwrap(),
            t_type: TransactionType::Position,
        })
    }
}

impl TrailingStop {
    async fn new(
        strategy: &str,
        symbol: &str,
        entry_price: Num,
        multiplier: f64,
        t_type: TransactionType,
        db: &Arc<DBClient>,
    ) -> Self {
        let pivot_points = Self::calculate_pivot_points(multiplier);
        let stop_price = entry_price.clone() * to_num!(1.0 - pivot_points[0].1);
        let high_low = entry_price.clone();
        let current_price = entry_price.clone();
        let mut stop = TrailingStop {
            local_id: Uuid::nil(),
            strategy: strategy.to_string(),
            symbol: symbol.to_string(),
            entry_price,
            current_price,
            pivot_points,
            watermark: high_low,
            stop_price,
            zone: 0,
            stop_type: StopType::Percent,
            multiplier,
            status: LockerStatus::Active,
            t_type,
        };
        let _ = stop.persist_to_db(db).await;
        stop
    }

    async fn load_from_db(local_id: Uuid, db: &Arc<DBClient>) -> Result<Self> {
        let columns = vec!["local_id"];
        let stmt = db.query_builder.prepare_fetch_statement("locker", &columns);

        match sqlx::query_as::<_, TrailingStop>(&stmt)
            .bind(local_id)
            .fetch_one(&db.pool)
            .await
        {
            Err(err) => bail!("Failed to pull data from db, error={}", err),
            core::result::Result::Ok(val) => Ok(val),
        }
    }

    fn refresh_entry_price(&mut self, _entry_price: Num) {
        self.pivot_points = Self::calculate_pivot_points(self.multiplier);
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
        let price_change = price - self.watermark.to_f64().unwrap();
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
                    let _ = self.persist_to_db(db);
                }
            }
            break;
        }
        let stop_loss_level = to_num!(stop_loss_level);
        if self.stop_price != stop_loss_level {
            self.stop_price = stop_loss_level;
        }
        if current_price > self.watermark {
            self.watermark = current_price;
        }
        //write to db
        self.stop_price.clone()
    }

    async fn persist_to_db(&mut self, db: &Arc<DBClient>) -> Result<()> {
        let columns = vec![
            "strategy",
            "symbol",
            "entry_price",
            "stop_price",
            "stop_type",
            "multiplier",
            "status",
            "local_id",
        ];

        let mut stmt = String::default();
        if Uuid::is_nil(&self.local_id) {
            self.local_id = Uuid::new_v4();
            stmt = db
                .query_builder
                .prepare_insert_statement("mktorder", &columns);
        } else {
            stmt = db
                .query_builder
                .prepare_update_statement("mktorder", &columns);
        }

        if let Err(err) = sqlx::query(&stmt)
            .bind(self.strategy.clone())
            .bind(self.symbol.clone())
            .bind(self.entry_price.round_with(2).to_f64())
            .bind(self.stop_price.round_with(2).to_f64())
            .bind(self.stop_type.to_string())
            .bind(self.multiplier)
            .bind(self.status.to_string())
            .bind(self.local_id)
            .execute(&db.pool)
            .await
        {
            bail!("Failed to publish to db, error={}", err)
        }
        Ok(())
    }
}
