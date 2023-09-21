use anyhow::Ok;
use anyhow::Result;
use num_decimal::Num;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;
use tracing::warn;
use uuid::Uuid;

mod atr_stop;
mod trailing_stop;

use super::locker::trailing_stop::TrailingStop;
use super::DBClient;
use super::MktData;
use super::Settings;
use crate::events::Direction;

#[derive(Debug, PartialEq, Clone, Copy, Default)]
pub enum LockerStatus {
    #[default]
    Disabled,
    Active,
    Finished,
}

impl FromStr for LockerStatus {
    type Err = String;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        match val {
            "Disabled" => std::result::Result::Ok(LockerStatus::Disabled),
            "Active" => std::result::Result::Ok(LockerStatus::Active),
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
pub enum TransactionType {
    Order,
    #[default]
    Position,
}

impl FromStr for TransactionType {
    type Err = String;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        match val {
            "Order" => std::result::Result::Ok(TransactionType::Order),
            "Position" => std::result::Result::Ok(TransactionType::Position),
            _ => Err(format!(
                "Failed to parse transaction type, unknown: {}",
                val
            )),
        }
    }
}

impl fmt::Display for TransactionType {
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

#[derive(Debug)]
pub struct Locker {
    stops: HashMap<Uuid, TrailingStop>,
    settings: Settings,
    db: Arc<DBClient>,
    mktdata: Arc<Mutex<MktData>>,
}

impl Locker {
    pub fn new(settings: &Settings, db: Arc<DBClient>, mktdata: &Arc<Mutex<MktData>>) -> Self {
        Locker {
            stops: HashMap::new(),
            settings: settings.clone(),
            db,
            mktdata: Arc::clone(mktdata),
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
        direction: Direction,
    ) -> Uuid {
        let strategy_cfg = &self.settings.strategies[strategy];
        let stop_cfg = &self.settings.stops[&strategy_cfg.locker];
        let stop = TrailingStop::new(
            strategy,
            symbol,
            entry_price.clone(),
            stop_cfg.multiplier,
            transact_type,
            direction,
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

    pub async fn update_stop(&mut self, stop_id: Uuid, entry_price: Num) {
        if let Some(stop) = self.stops.get_mut(&stop_id) {
            stop.refresh_entry_price(entry_price);
            stop.persist_to_db(self.db.clone()).await.unwrap();
        } else {
            warn!("Failed to update stop, cannot find stop_id: {} ", stop_id);
        }
    }

    pub async fn complete(&mut self, locker_id: Uuid) {
        if let Some(mut stop) = self.stops.get_mut(&locker_id) {
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
            let result = match stop.direction {
                Direction::Long => stop_price > *trade_price,
                Direction::Short => stop_price < *trade_price,
            };
            if result {
                stop.status = LockerStatus::Disabled;
                info!(
                    "Closing transaction: {} as last price: {} has crossed the stop price: {}",
                    stop.symbol,
                    trade_price.clone(),
                    stop_price
                );
                return true;
            }
        }
        false
    }
}
