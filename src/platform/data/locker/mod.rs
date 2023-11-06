use anyhow::bail;
use anyhow::Ok;
use anyhow::Result;
use num_decimal::Num;
use sqlx::postgres::PgArguments;
use sqlx::postgres::PgRow;
use sqlx::query::Query;
use sqlx::FromRow;
use sqlx::Postgres;
use sqlx::Row;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::error;
use tracing::info;
use uuid::Uuid;

mod atr_stop;
mod smart_trail;

use super::locker::atr_stop::AtrStop;
use super::locker::smart_trail::SmartTrail;
use super::DBClient;
use super::MktData;
use super::Settings;
use crate::events::Direction;
use crate::platform::mktdata::Snapshot;
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
        match val.to_lowercase().as_str() {
            "pc" => std::result::Result::Ok(StopType::Percent),
            "percent" => std::result::Result::Ok(StopType::Percent),
            "atr" => std::result::Result::Ok(StopType::Atr),
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
enum Stop {
    Smart(SmartTrail),
    Atr(AtrStop),
}

impl Stop {
    fn stop_price(&self) -> Num {
        match self {
            Stop::Atr(atr) => atr.stop_price.clone(),
            Stop::Smart(trailing) => trailing.stop_price.clone(),
        }
    }

    fn watermark(&self) -> Num {
        match self {
            Stop::Atr(atr) => atr.stop_price.clone(),
            Stop::Smart(trailing) => trailing.watermark.clone(),
        }
    }

    fn zone(&self) -> i16 {
        match self {
            Stop::Atr(atr) => atr.zone,
            Stop::Smart(trailing) => trailing.zone,
        }
    }

    async fn price_update(
        &mut self,
        strategy: &str,
        symbol: &str,
        entry_price: Num,
        last_price: Num,
        mktdata: &Arc<Mutex<MktData>>,
    ) -> Num {
        match self {
            Stop::Atr(atr) => {
                atr.price_update(symbol, entry_price, last_price, mktdata)
                    .await
            }
            Stop::Smart(trailing) => {
                trailing
                    .price_update(strategy, symbol, entry_price, last_price)
                    .await
            }
        }
    }
}

#[derive(Debug, Clone)]
struct SmartStop {
    pub local_id: Uuid,
    pub strategy: String,
    pub symbol: String,
    pub entry_price: Num,
    pub multiplier: f64,
    pub direction: Direction,
    pub stop_type: StopType,
    pub status: LockerStatus,
    pub transact_type: TransactionType,
    pub stop: Stop,
}

impl fmt::Display for SmartStop {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let status = match &self.stop {
            Stop::Smart(stop) => stop.print_status(),
            Stop::Atr(stop) => stop.print_status(),
        };
        write!(
            f,
            "{} status[{}] direction[{}]",
            status, self.status, self.direction
        )
    }
}

impl FromRow<'_, PgRow> for SmartStop {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        fn sqlx_to_num(row: &PgRow, value: &str) -> sqlx::Result<Num> {
            match row.try_get::<f64, _>(value) {
                std::result::Result::Ok(val) => std::result::Result::Ok(to_num!(val)),
                Err(err) => Err(err),
            }
        }

        let multiplier: f64 = row.try_get("multiplier")?;
        let entry_price = sqlx_to_num(row, "entry_price")?;
        let watermark = sqlx_to_num(row, "watermark")?;
        let stop_price = sqlx_to_num(row, "stop_price")?;
        let zone: i16 = row.try_get("zone")?;
        let direction = Direction::from_str(row.try_get("direction")?).unwrap();
        let stop_type = StopType::from_str(row.try_get("type")?).unwrap();

        let stop = match stop_type {
            StopType::Atr => Stop::Atr(AtrStop::from_db(
                watermark, multiplier, direction, zone, stop_price,
            )),
            StopType::Percent => Stop::Smart(SmartTrail::from_db(
                watermark, multiplier, direction, zone, stop_price,
            )),
        };

        sqlx::Result::Ok(SmartStop {
            local_id: row.try_get("local_id")?,
            strategy: row.try_get("strategy")?,
            symbol: row.try_get("symbol")?,
            entry_price: entry_price.clone(),
            multiplier,
            direction,
            stop_type: StopType::from_str(row.try_get("type")?).unwrap(),
            status: LockerStatus::from_str(row.try_get("status")?).unwrap(),
            transact_type: TransactionType::from_str(row.try_get("transact_type")?).unwrap(),
            stop,
        })
    }
}

impl SmartStop {
    pub async fn new(
        symbol: &str,
        strategy: &str,
        direction: Direction,
        entry_price: Num,
        multiplier: f64,
        stop_type: StopType,
        mktdata: &Arc<Mutex<MktData>>,
    ) -> Self {
        let stop = match stop_type {
            StopType::Percent => Stop::Smart(SmartTrail::new(
                symbol,
                entry_price.clone(),
                multiplier,
                direction,
            )),
            StopType::Atr => {
                let daily_atr = match AtrStop::update_daily_atr(symbol, mktdata).await {
                    anyhow::Result::Ok(atr) => atr,
                    anyhow::Result::Err(err) => {
                        panic!("Failed to calculate daily atr, error={}", err)
                    }
                };
                Stop::Atr(AtrStop::new(
                    symbol,
                    multiplier,
                    direction,
                    entry_price.clone(),
                    daily_atr,
                ))
            }
        };
        SmartStop {
            local_id: Uuid::nil(),
            strategy: strategy.to_string(),
            symbol: symbol.to_string(),
            entry_price,
            multiplier,
            direction,
            stop_type,
            transact_type: TransactionType::Order,
            status: LockerStatus::Active,
            stop,
        }
    }

    fn build_query<'a>(&'a self, stmt: &'a str, stop: &Stop) -> Query<'_, Postgres, PgArguments> {
        sqlx::query(stmt)
            .bind(self.strategy.clone())
            .bind(self.symbol.clone())
            .bind(self.entry_price.round_with(3).to_f64())
            .bind(stop.stop_price().round_with(3).to_f64())
            .bind(self.stop_type.to_string())
            .bind(self.multiplier)
            .bind(self.direction.to_string())
            .bind(stop.watermark().round_with(3).to_f64())
            .bind(stop.zone())
            .bind(self.status.to_string())
            .bind(self.transact_type.to_string())
            .bind(self.local_id)
    }

    pub async fn persist_to_db(&mut self, db: &Arc<DBClient>) -> Result<()> {
        let columns = vec![
            "strategy",
            "symbol",
            "entry_price",
            "stop_price",
            "type",
            "multiplier",
            "direction",
            "watermark",
            "zone",
            "status",
            "transact_type",
            "local_id",
        ];

        let stmt = db.get_sql_stmt("locker", &self.local_id, columns, db);
        if Uuid::is_nil(&self.local_id) {
            self.local_id = Uuid::new_v4();
        }

        if let Err(err) = self.build_query(&stmt, &self.stop).execute(&db.pool).await {
            bail!("Locker failed to publish to db, error={}", err)
        }
        Ok(())
    }
}

pub struct Locker {
    stops: HashMap<Uuid, SmartStop>,
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
            statuses: Vec<LockerStatus>,
            db: &Arc<DBClient>,
        ) -> Vec<SmartStop> {
            let mut results = Vec::new();
            for status in statuses {
                let rs = match sqlx::query_as::<_, SmartStop>(&stmt)
                    .bind(status.to_string())
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

        let columns = vec!["status"];
        let stmt = self
            .db
            .query_builder
            .prepare_fetch_statement("locker", &columns);

        let rows = fetch_stops(
            stmt.clone(),
            vec![LockerStatus::Disabled, LockerStatus::Active],
            &self.db,
        )
        .await;

        for stop in rows {
            self.stops.insert(stop.local_id, stop);
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
        let mut smart = SmartStop::new(
            symbol,
            strategy,
            direction,
            entry_price.clone(),
            stop_cfg.multiplier,
            stop_type,
            &self.mktdata,
        )
        .await;

        if let Err(err) = smart.persist_to_db(&self.db).await {
            error!("Failed to persist stop to db, error={}", err);
        }

        info!(
            "Strategy[{}] locker monitoring new symbol: {} entry price: {} transaction: {:?}",
            strategy,
            symbol,
            entry_price.round_with(2),
            transact_type
        );
        let local_id = smart.local_id;
        self.stops.insert(local_id, smart);
        local_id
    }

    pub async fn start_tracking_position(&mut self, locker_id: Uuid) -> Result<()> {
        if let Some(stop) = self.stops.get_mut(&locker_id) {
            if stop.transact_type != TransactionType::Position {
                stop.transact_type = TransactionType::Position;
                stop.persist_to_db(&self.db).await?
            }
        }
        Ok(())
    }

    pub async fn complete(&mut self, locker_id: Uuid) {
        if let Some(stop) = self.stops.get_mut(&locker_id) {
            stop.status = LockerStatus::Finished;
            stop.persist_to_db(&self.db).await.unwrap();
            info!("Locker tracking symbol: {} marked as complete", stop.symbol);
        }
    }

    pub async fn activate(&mut self, locker_id: Uuid) {
        if let Some(stop) = self.stops.get_mut(&locker_id) {
            stop.status = LockerStatus::Active;
            stop.persist_to_db(&self.db).await.unwrap();
        }
    }

    pub fn print_stop(&mut self, locker_id: &Uuid) -> String {
        match self.stops.get_mut(locker_id) {
            Some(stop) => format!("{}", stop),
            None => panic!("Failed to find stop with locker_id {}", locker_id),
        }
    }

    pub async fn should_close(
        &mut self,
        symbol: &str,
        locker_id: &Uuid,
        snapshot: &Snapshot,
    ) -> Result<bool> {
        if !self.stops.contains_key(locker_id) {
            bail!("Symbol: {symbol} with {locker_id:?} not being tracked in locker");
        }

        async fn check_should_close(
            snapshot: &Snapshot,
            smart: &mut SmartStop,
            mktdata: &Arc<Mutex<MktData>>,
        ) -> Result<Num> {
            if smart.status.ne(&LockerStatus::Active) {
                bail!("Not active");
            }
            let last_price = snapshot.mid_price.clone();
            let stop_price = smart
                .stop
                .price_update(
                    &smart.strategy,
                    &smart.symbol,
                    smart.entry_price.clone(),
                    last_price.clone(),
                    mktdata,
                )
                .await;

            if smart.status == LockerStatus::Disabled {
                info!("Locker status has been set to disabled");
                return Ok(stop_price);
            }
            let result = match smart.direction {
                Direction::Long => stop_price > last_price,
                Direction::Short => stop_price < last_price,
            };

            if !result {
                bail!("Don't close")
            }
            info!("Last price: {} stop: {}", snapshot, stop_price);
            Ok(stop_price)
        }

        if let Some(smart) = self.stops.get_mut(locker_id) {
            if let anyhow::Result::Ok(stop_price) =
                check_should_close(snapshot, smart, &self.mktdata).await
            {
                if smart.transact_type == TransactionType::Position {
                    let _ = smart.persist_to_db(&self.db).await;
                }
                smart.status = LockerStatus::Disabled;
                info!(
                    "Closing transaction: {} as last price: {} has crossed the stop price: {}",
                    smart.symbol, snapshot, stop_price,
                );
                return Ok(true);
            }
        }
        Ok(false)
    }
}
