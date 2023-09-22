use anyhow::bail;
use anyhow::Ok;
use anyhow::Result;
use num_decimal::Num;
use sqlx::postgres::PgRow;
use sqlx::FromRow;
use sqlx::Row;
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

use super::locker::atr_stop::AtrStop;
use super::locker::trailing_stop::TrailingStop;
use super::DBClient;
use super::MktData;
use super::Settings;
use crate::events::Direction;
use crate::to_num;

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
    Atr,
}

impl FromStr for StopType {
    type Err = String;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        match val {
            "Percent" => std::result::Result::Ok(StopType::Percent),
            "ATR" => std::result::Result::Ok(StopType::Atr),
            _ => Err(format!("Failed to parse stop type, unknown: {}", val)),
        }
    }
}

impl fmt::Display for StopType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Clone)]
pub enum Stop {
    Atr(AtrStop),
    Smart(TrailingStop),
}

impl fmt::Display for Stop {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug)]
pub struct Locker {
    stops: HashMap<Uuid, Stop>,
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
        async fn fetch_stops<Type>(
            stmt: String,
            statuses: Vec<LockerStatus>,
            stop_type: StopType,
            db: &Arc<DBClient>,
        ) -> Vec<Type>
        where
            Type: for<'r> FromRow<'r, PgRow> + Send + Unpin,
        {
            let mut results = Vec::new();
            for status in statuses {
                let rs = match sqlx::query_as::<_, Type>(&stmt)
                    .bind(status.to_string())
                    .bind(stop_type.to_string())
                    .fetch_all(&db.pool)
                    .await
                {
                    sqlx::Result::Ok(val) => val,
                    Err(err) => panic!(
                        "Failed to fetch locker entries from db, closing app, error={}",
                        err
                    ),
                };
                results.extend(rs);
            }
            results
        }

        let columns = vec!["status", "type"];
        let stmt = self
            .db
            .query_builder
            .prepare_fetch_statement("locker", &columns);

        let atr_stops = fetch_stops::<AtrStop>(
            stmt.clone(),
            vec![LockerStatus::Disabled, LockerStatus::Active],
            StopType::Atr,
            &self.db,
        )
        .await;

        let percent_stops = fetch_stops::<TrailingStop>(
            stmt.clone(),
            vec![LockerStatus::Disabled, LockerStatus::Active],
            StopType::Percent,
            &self.db,
        )
        .await;

        let mut rows: Vec<Stop> = atr_stops.iter().map(|f| Stop::Atr(f.clone())).collect();
        rows.extend(
            percent_stops
                .iter()
                .map(|f| Stop::Smart(f.clone()))
                .collect::<Vec<Stop>>(),
        );

        for stop in &rows {
            let local_id = match stop {
                Stop::Atr(stop) => stop.local_id,
                Stop::Smart(stop) => stop.local_id,
            };
            self.stops.insert(local_id, stop.clone());
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
        let stop_type = StopType::from_str(&stop_cfg.locker_type).unwrap();
        let stop = match stop_type {
            StopType::Percent => {
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
                Stop::Smart(stop)
            }
            StopType::Atr => {
                let stop = AtrStop::new(
                    strategy,
                    symbol,
                    entry_price.clone(),
                    to_num!(stop_cfg.multiplier),
                    transact_type,
                    direction,
                    &self.db,
                )
                .await
                .unwrap();
                Stop::Atr(stop)
            }
        };

        let local_id = match &stop {
            Stop::Atr(stop) => stop.local_id,
            Stop::Smart(stop) => stop.local_id,
        };
        info!(
            "Strategy[{}] locker monitoring new symbol: {} entry price: {} transaction: {:?}",
            strategy,
            symbol,
            entry_price.round_with(2),
            transact_type
        );
        self.stops.insert(local_id, stop);
        local_id
    }

    pub async fn update_stop(&mut self, stop_id: Uuid, entry_price: Num) {
        if let Some(stop) = self.stops.get_mut(&stop_id) {
            let stop = match stop {
                Stop::Atr(val) => val,
                Stop::Smart(val) => val,
            };
            stop.into().refresh_entry_price(entry_price);
            stop.persist_to_db(self.db.clone()).await.unwrap();
        } else {
            warn!("Failed to update stop, cannot find stop_id: {} ", stop_id);
        }
    }

    pub async fn complete(&mut self, locker_id: Uuid) {
        if let Some(stop) = self.stops.get_mut(&locker_id) {
            let stop = match stop {
                Stop::Atr(val) => val,
                Stop::Smart(val) => val,
            };
            info!("Locker tracking symbol: {} marked as complete", stop.symbol);
            stop.status = LockerStatus::Finished;
            stop.persist_to_db(self.db.clone()).await.unwrap();
        }
    }

    pub async fn revive(&mut self, locker_id: Uuid) {
        if let Some(stop) = self.stops.get_mut(&locker_id) {
            let stop = match stop {
                Stop::Atr(val) => val,
                Stop::Smart(val) => val,
            };
            info!("Locker tracking symbol: {} being revived", stop.symbol);
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

        async fn check_should_close<ST>(trade_price: &Num, stop: &ST) -> Result<Num> {
            if stop.status.ne(&LockerStatus::Active) {
                bail!("Not active");
            }
            let stop_price = stop
                .price_update(trade_price.clone(), &self.db, &self.mktdata)
                .await;
            let result = match stop.direction {
                Direction::Long => stop_price > trade_price,
                Direction::Short => stop_price < trade_price,
            };

            if !result {
                bail!("Don't close")
            }
            Ok(stop_price)
        }

        if let Some(stop) = self.stops.get_mut(locker_id) {
            let result = match stop {
                Stop::Atr(val) => check_should_close::<AtrStop>(trade_price, stop).await,
                Stop::Smart(val) => check_should_close(trade_price, stop).await,
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
