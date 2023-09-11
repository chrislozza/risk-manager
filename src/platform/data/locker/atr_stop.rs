use anyhow::bail;
use anyhow::Ok;
use anyhow::Result;
use num_decimal::Num;
use sqlx::postgres::PgRow;
use sqlx::FromRow;
use sqlx::Row;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use tracing::error;
use tracing::info;
use uuid::Uuid;

use super::DBClient;
use super::LockerStatus;
use crate::to_num;

#[derive(Debug, Clone, Default)]
struct AtrStop {
    local_id: Uuid,
    strategy: String,
    symbol: String,
    entry_price: Num,
    current_price: Num,
    pivot_points: [(i8, f64, f64); 2],
    stop_price: Num,
    zone: i8,
    stop_type: StopType,
    multiplier: f64,
    watermark: Num,
    status: LockerStatus,
    transact_type: TransactionType,
}

impl fmt::Display for AtrStop {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "symbol[{}], price[{}], stop[{}], zone[{}] status[{}] type[{}]",
            self.symbol,
            self.current_price.round_with(2),
            self.stop_price.round_with(2),
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

        let strategy = row.try_get("strategy")?;
        let symbol = row.try_get("symbol")?;
        let multiplier = row.try_get("multiplier")?;
        let entry_price = sqlx_to_num(row, "entry_price")?;
        let watermark = sqlx_to_num(row, "watermark")?;
        let stop_price = sqlx_to_num(row, "stop_price")?;
        let pivot_points = Self::calculate_atr_stop(strategy, symbol, &entry_price, multiplier);

        sqlx::Result::Ok(Self {
            local_id: row.try_get("local_id")?,
            strategy: strategy.to_string(),
            symbol: symbol.to_string(),
            entry_price: entry_price.clone(),
            current_price: entry_price.clone(),
            pivot_points,
            watermark,
            stop_price,
            zone: 0,
            multiplier,
            stop_type: StopType::from_str(row.try_get("stop_type")?).unwrap(),
            status: LockerStatus::from_str(row.try_get("status")?).unwrap(),
            transact_type: TransactionType::Position,
        })
    }
}

impl AtrStop {
    async fn new(
        strategy: &str,
        symbol: &str,
        entry_price: Num,
        multiplier: f64,
        transact_type: TransactionType,
        db: &Arc<DBClient>,
    ) -> Self {
        let pivot_points = Self::calculate_atr_stop(strategy, symbol, &entry_price, multiplier);
        let stop_price = entry_price.clone() * to_num!(1.0 - pivot_points[0].1);
        let watermark = entry_price.clone();
        let current_price = entry_price.clone();
        let mut stop = AtrStop {
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
        self.pivot_points =
            Self::calculate_atr_stop(&self.strategy, &self.symbol, &entry_price, self.multiplier);
    }

    fn calculate_atr_stop(
        strategy: &str,
        symbol: &str,
        entry_price: &Num,
        multiplier: f64,
    ) -> [(i8, f64, f64); 2] {
        fn pivot_to_price(
            entry_price: &Num,
            pivot_points: [(i8, f64, f64); 2],
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
        ];
        info!(
            "Strategy[{}] Symbol[{}], adding new stop with entry_price[{}], zones at price targets 1[{}], 2[{}]",
            strategy,
            symbol,
            entry_price.clone(),
            pivot_to_price(&entry_price, pivot_points, 0),
            pivot_to_price(&entry_price, pivot_points, 1),
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
                2 => {
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
