use anyhow::bail;
use anyhow::Ok;
use anyhow::Result;
use num_decimal::Num;
use sqlx::postgres::PgRow;
use sqlx::FromRow;
use sqlx::Row;
use std::cmp::max;
use std::cmp::min;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::error;
use tracing::info;
use uuid::Uuid;

use super::DBClient;
use super::LockerStatus;
use super::StopType;
use super::TransactionType;
use crate::events::Direction;
use crate::platform::mktdata::MktData;
use crate::platform::technical_signals::TechnnicalSignals;
use crate::to_num;

#[derive(Debug, Clone, Default)]
pub struct AtrStop {
    pub local_id: Uuid,
    pub strategy: String,
    pub symbol: String,
    pub entry_price: Num,
    pub current_price: Num,
    pub stop_price: Option<Num>,
    pub zone: i16,
    pub multiplier: Num,
    pub watermark: Num,
    pub daily_atr: Option<Num>,
    pub stop_type: StopType,
    pub direction: Direction,
    pub status: LockerStatus,
    transact_type: TransactionType,
}

impl fmt::Display for AtrStop {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "symbol[{}], price[{}], stop[{}], zone[{}] status[{}] type[{}]",
            self.symbol,
            self.current_price.round_with(2),
            self.stop_price.as_ref().unwrap().round_with(2),
            self.zone,
            self.status,
            self.transact_type
        )
    }
}

impl FromRow<'_, PgRow> for AtrStop {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        fn sqlx_to_num(row: &PgRow, value: &str) -> sqlx::Result<Num> {
            match row.try_get::<f64, _>(value) {
                std::result::Result::Ok(val) => std::result::Result::Ok(to_num!(val)),
                Err(err) => Err(err),
            }
        }

        let strategy: &str = row.try_get("strategy")?;
        let symbol: &str = row.try_get("symbol")?;
        let multiplier = sqlx_to_num(row, "multiplier")?;
        let entry_price = sqlx_to_num(row, "entry_price")?;
        let watermark = sqlx_to_num(row, "watermark")?;
        let stop_price = sqlx_to_num(row, "stop_price")?;

        sqlx::Result::Ok(Self {
            local_id: row.try_get("local_id")?,
            strategy: strategy.to_string(),
            symbol: symbol.to_string(),
            entry_price: entry_price.clone(),
            current_price: entry_price.clone(),
            watermark,
            stop_price: Some(stop_price),
            zone: 0,
            multiplier,
            daily_atr: None,
            stop_type: StopType::from_str(row.try_get("stop_type")?).unwrap(),
            direction: Direction::from_str(row.try_get("direction")?).unwrap(),
            status: LockerStatus::from_str(row.try_get("status")?).unwrap(),
            transact_type: TransactionType::Position,
        })
    }
}

impl AtrStop {
    pub async fn new(
        strategy: &str,
        symbol: &str,
        entry_price: Num,
        multiplier: Num,
        transact_type: TransactionType,
        direction: Direction,
        db: &Arc<DBClient>,
    ) -> Result<Self> {
        let watermark = entry_price.clone();
        let current_price = entry_price.clone();
        let mut stop = AtrStop {
            strategy: strategy.to_string(),
            symbol: symbol.to_string(),
            entry_price,
            current_price,
            stop_price: None,
            daily_atr: None,
            multiplier,
            watermark,
            direction,
            transact_type,
            ..Default::default()
        };
        if let Err(err) = stop.persist_to_db(db.clone()).await {
            error!("Failed to persist stop to db, error={}", err);
        }
        Ok(stop)
    }

    async fn calculate_atr_stop(
        &mut self,
        watermark: Num,
        current_price: Num,
        stop_price: Num,
        multiplier: Num,
        direction: Direction,
        mktdata: &Arc<Mutex<MktData>>,
    ) -> Result<Num> {
        let daily_atr = TechnnicalSignals::get_atr(&self.symbol, mktdata).await?;
        let stop = match direction {
            Direction::Long => max(stop_price, watermark - (daily_atr * multiplier)),
            Direction::Short => min(stop_price, watermark + (daily_atr * multiplier)),
            _ => bail!("Direction not found calculating atr stop"),
        };

        info!(
            "Strategy[{}] Symbol[{}], adding new stop with last price[{}], atr stop set at[{}]]",
            &self.strategy,
            &self.symbol,
            current_price.clone(),
            stop
        );
        Ok(stop)
    }

    pub async fn price_update(
        &mut self,
        current_price: Num,
        db: &Arc<DBClient>,
        mktdata: &Arc<Mutex<MktData>>,
    ) -> Result<Num> {
        let stop_price = match &self.daily_atr {
            Some(val) => val.clone(),
            None => {
                self.calculate_atr_stop(
                    self.watermark.clone(),
                    current_price.clone(),
                    self.stop_price.clone().unwrap(),
                    self.multiplier.clone(),
                    self.direction,
                    mktdata,
                )
                .await?
            }
        };
        self.stop_price = Some(stop_price);
        if let Some(stop_price) = &self.stop_price {
            self.current_price = current_price.clone();
            let price = current_price;
            let price_change = price - self.watermark.clone();
            if price_change <= to_num!(0.0) || self.status == LockerStatus::Disabled {
                return Ok(stop_price.clone());
            }

            self.watermark += price_change.clone();

            let stop_loss_level = match self.direction {
                Direction::Long => stop_price + price_change,
                Direction::Short => stop_price - price_change,
                _ => panic!("Can't find a set direction"),
            };
            if self.transact_type == TransactionType::Position {
                let _ = self.persist_to_db(db.clone()).await;
            }

            Ok(stop_loss_level)
        } else {
            panic!("No stop price set for symbol: {}", self.symbol)
        }
    }

    pub async fn persist_to_db(&mut self, db: Arc<DBClient>) -> Result<()> {
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
            .bind(
                self.stop_price
                    .as_ref()
                    .unwrap()
                    .round_with(3)
                    .to_f64()
                    .unwrap(),
            )
            .bind(self.stop_type.to_string())
            .bind(self.multiplier.round_with(3).to_f64().unwrap())
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
