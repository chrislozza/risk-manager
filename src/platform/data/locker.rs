use anyhow::Ok;
use tracing::error;
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

#[derive(Debug, PartialEq, Clone, Copy, Default)]
pub enum LockerStatus {
    Active,
    #[default]
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

#[derive(Debug, PartialEq, Clone, Copy, Default)]
pub enum StopType {
    #[default]
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

#[derive(Debug, PartialEq, Clone, Copy, Default)]
pub enum TransactionType {
    Order,
    #[default]
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
        async fn fetch_stops(
            stmt: String,
            status: String,
            db: &Arc<DBClient>,
        ) -> Vec<TrailingStop> {
            match sqlx::query_as::<_, TrailingStop>(&stmt)
                .bind(status)
                .fetch_all(&db.pool)
                .await
            {
                sqlx::Result::Ok(val) => val,
                Err(err) => panic!(
                    "Failed to fetch locker entries from db, closing app, error={}",
                    err
                ),
            }
        }

        let columns = vec!["status"];
        let stmt = self
            .db
            .query_builder
            .prepare_fetch_statement("locker", &columns);

        let status = LockerStatus::Disabled.to_string();
        let mut rows = fetch_stops(stmt.clone(), status, &self.db).await;

        let status = LockerStatus::Active.to_string();
        let active_stops = fetch_stops(stmt, status, &self.db).await;
        rows.extend(active_stops);
        for stop in rows {
            let local_id = stop.local_id;
            self.stops.insert(local_id, stop);
        }
        Ok(())
    }

    pub async fn create_new_stop(
        &mut self,
        symbol: &str,
        strategy: &str,
        entry_price: Num,
        transact_type: TransactionType,
    ) -> Uuid {
        let strategy_cfg = &self.settings.strategies.configuration[strategy];
        let stop = TrailingStop::new(
            strategy,
            symbol,
            entry_price.clone(),
            strategy_cfg.trailing_size,
            transact_type,
            &self.db,
        )
        .await;
        let stop_id = stop.local_id;
        info!(
            "Strategy[{}] locker monitoring new symbol: {} entry price: {} transaction: {:?}",
            strategy,
            symbol,
            entry_price.round_with(2),
            transact_type
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
            stop.persist_to_db(self.db.clone()).await.unwrap();
        }
    }

    pub async fn revive(&mut self, locker_id: Uuid) {
        if let Some(stop) = self.stops.get_mut(&locker_id) {
            stop.status = LockerStatus::Active;
            stop.persist_to_db(self.db.clone()).await.unwrap();
        }
    }

    pub fn print_stop(&mut self, locker_id: &Uuid) -> String {
        match self.stops.get_mut(locker_id) {
            Some(stop) => format!("{}", stop),
            None => panic!("Failed to find stop with locker_id {}", locker_id),
        }
    }

    pub async fn update_status(&mut self, locker_id: &Uuid, status: LockerStatus) {
        if let Some(stop) = self.stops.get_mut(locker_id) {
            stop.status = status;
            stop.persist_to_db(self.db.clone()).await.unwrap();
        }
    }

    pub async fn should_close(&mut self, locker_id: &Uuid, trade_price: &Num) -> bool {
        if !self.stops.contains_key(locker_id) {
            info!("Symbol: {locker_id:?} not being tracked in locker");
            return false;
        }
        if let Some(stop) = self.stops.get_mut(locker_id) {
            if stop.status.ne(&LockerStatus::Active) {
                return false;
            }
            let stop_price = stop.price_update(trade_price.clone(), &self.db).await;
            if stop_price > *trade_price {
                stop.status = LockerStatus::Disabled;
                return true;
            }
        }
        false
    }
}

#[derive(Debug, Clone, Default)]
struct TrailingStop {
    local_id: Uuid,
    strategy: String,
    symbol: String,
    entry_price: Num,
    current_price: Num,
    pivot_points: [(i8, f64, f64); 4],
    stop_price: Num,
    zone: i8,
    stop_type: StopType,
    multiplier: f64,
    watermark: Num,
    status: LockerStatus,
    transact_type: TransactionType,
}

impl fmt::Display for TrailingStop {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "symbol[{}], price[{}], stop[{}], zone[{}] status[{}] type[{}]",
            self.symbol,
            self.current_price.round_with(3).to_f64().unwrap(),
            self.stop_price.round_with(3).to_f64().unwrap(),
            self.zone,
            self.status,
            self.transact_type
        )
    }
}

impl FromRow<'_, PgRow> for TrailingStop {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        fn sqlx_to_num(row: &PgRow, value: &str) -> sqlx::Result<Num> {
            match row.try_get::<f64, _>(value) {
                std::result::Result::Ok(val) => std::result::Result::Ok(to_num!(val)),
                Err(err) => Err(err),
            }
        }
        let strategy = row.try_get("strategy")?;
        let symbol = row.try_get("symbol")?;
        let multiplier = row.try_get("multiplier")?;
        let entry_price = sqlx_to_num(row, "entry_price")?;
        let watermark = sqlx_to_num(row, "watermark")?;
        let stop_price = sqlx_to_num(row, "stop_price")?;
        let zone: i8 = row.try_get("zone")?;
        let pivot_points = Self::calculate_pivot_points(strategy, symbol, &entry_price, multiplier);

        sqlx::Result::Ok(Self {
            local_id: row.try_get("local_id")?,
            strategy: strategy.to_string(),
            symbol: symbol.to_string(),
            entry_price: entry_price.clone(),
            current_price: entry_price.clone(),
            pivot_points,
            watermark,
            stop_price,
            zone,
            multiplier,
            stop_type: StopType::from_str(row.try_get("stop_type")?).unwrap(),
            status: LockerStatus::from_str(row.try_get("status")?).unwrap(),
            transact_type: TransactionType::Position,
        })
    }
}

impl TrailingStop {
    async fn new(
        strategy: &str,
        symbol: &str,
        entry_price: Num,
        multiplier: f64,
        transact_type: TransactionType,
        db: &Arc<DBClient>,
    ) -> Self {
        let pivot_points = Self::calculate_pivot_points(strategy, symbol, &entry_price, multiplier);
        let stop_price = entry_price.clone() * to_num!(1.0 - pivot_points[0].1);
        let watermark = entry_price.clone();
        let current_price = entry_price.clone();
        let mut stop = TrailingStop {
            strategy: strategy.to_string(),
            symbol: symbol.to_string(),
            entry_price,
            current_price,
            pivot_points,
            stop_price,
            multiplier,
            watermark,
            transact_type,
            ..Default::default()
        };
        if let Err(err) = stop.persist_to_db(db.clone()).await {
            error!("Failed to persist stop to db, error={}", err);
        }
        stop
    }

    fn refresh_entry_price(&mut self, entry_price: Num) {
        self.pivot_points = Self::calculate_pivot_points(
            &self.strategy,
            &self.symbol,
            &entry_price,
            self.multiplier,
        );
    }

    fn calculate_pivot_points(
        strategy: &str,
        symbol: &str,
        entry_price: &Num,
        multiplier: f64,
    ) -> [(i8, f64, f64); 4] {
        fn pivot_to_price(
            entry_price: &Num,
            pivot_points: [(i8, f64, f64); 4],
            index: usize,
        ) -> f64 {
            (entry_price.clone() * to_num!(1.0 + pivot_points[index].1))
                .round_with(3)
                .to_f64()
                .unwrap()
        }
        let entry_price = entry_price.round_with(3);
        let pivot_points = [
            (1, (multiplier / 100.0), 1.0),
            (2, (multiplier * 2.0 / 100.0), 0.0),
            (3, (multiplier * 3.0 / 100.0), 2.0),
            (4, (multiplier * 4.0 / 100.0), 2.0 - (1.0 / multiplier)),
        ];
        info!(
            "Strategy[{}] Symbol[{}], adding new stop with entry_price[{}], zones at price targets 1[{}], 2[{}], 3[{}], 4[{}]",
            strategy,
            symbol,
            entry_price.clone(),
            pivot_to_price(&entry_price, pivot_points, 0),
            pivot_to_price(&entry_price, pivot_points, 1),
            pivot_to_price(&entry_price, pivot_points, 2),
            pivot_to_price(&entry_price, pivot_points, 3),
        );
        pivot_points
    }

    async fn price_update(&mut self, current_price: Num, db: &Arc<DBClient>) -> Num {
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
            }
            break;
        }
        let stop_loss_level = to_num!(stop_loss_level);
        if self.stop_price != stop_loss_level {
            self.stop_price = stop_loss_level.clone();
        }
        if current_price > self.watermark {
            self.watermark = current_price;
        }

        if self.transact_type == TransactionType::Position {
            let _ = self.persist_to_db(db.clone()).await;
        }
        stop_loss_level
    }

    async fn persist_to_db(&mut self, db: Arc<DBClient>) -> Result<()> {
        let columns = vec![
            "strategy",
            "symbol",
            "entry_price",
            "stop_price",
            "stop_type",
            "multiplier",
            "watermark",
            "zone",
            "status",
            "local_id",
        ];

        fn get_sql_stmt(local_id: &Uuid, columns: Vec<&str>, db: &Arc<DBClient>) -> String {
            if Uuid::is_nil(local_id) {
                db.query_builder
                    .prepare_insert_statement("locker", &columns)
            } else {
                db.query_builder
                    .prepare_update_statement("locker", &columns)
            }
        }

        let stmt = get_sql_stmt(&self.local_id, columns, &db);
        if Uuid::is_nil(&self.local_id) {
            self.local_id = Uuid::new_v4();
        }

        if let Err(err) = sqlx::query(&stmt)
            .bind(self.strategy.clone())
            .bind(self.symbol.clone())
            .bind(self.entry_price.round_with(3).to_f64().unwrap())
            .bind(self.stop_price.round_with(3).to_f64().unwrap())
            .bind(self.stop_type.to_string())
            .bind(self.multiplier)
            .bind(self.watermark.round_with(3).to_f64().unwrap())
            .bind(self.zone)
            .bind(self.status.to_string())
            .bind(self.local_id)
            .execute(&db.pool)
            .await
        {
            bail!("Locker failed to publish to db, error={}", err)
        }
        Ok(())
    }
}
