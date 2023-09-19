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
use super::StopType;
use super::TransactionType;
use crate::events::Direction;
use crate::to_num;

#[derive(Debug, Clone, Default)]
pub struct TrailingStop {
    pub local_id: Uuid,
    pub strategy: String,
    pub symbol: String,
    pub entry_price: Num,
    pub current_price: Num,
    pub pivot_points: [(i16, f64, f64); 4],
    pub stop_price: Num,
    pub zone: i16,
    pub multiplier: f64,
    pub watermark: Num,
    pub direction: Direction,
    pub stop_type: StopType,
    pub status: LockerStatus,
    pub transact_type: TransactionType,
}

impl fmt::Display for TrailingStop {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "price[{}], stop[{}], zone[{}] status[{}] direction[{}]",
            self.current_price.round_with(3).to_f64().unwrap(),
            self.stop_price.round_with(3).to_f64().unwrap(),
            self.zone,
            self.status,
            self.direction
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
        let zone = row.try_get("zone")?;
        let direction = Direction::from_str(row.try_get("direction")?).unwrap();
        let pivot_points =
            Self::calculate_pivot_points(strategy, symbol, &entry_price, multiplier, direction);

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
            direction,
            stop_type: StopType::from_str(row.try_get("type")?).unwrap(),
            status: LockerStatus::from_str(row.try_get("status")?).unwrap(),
            transact_type: TransactionType::from_str(row.try_get("transact_type")?).unwrap(),
        })
    }
}

impl TrailingStop {
    pub async fn new(
        strategy: &str,
        symbol: &str,
        entry_price: Num,
        multiplier: f64,
        transact_type: TransactionType,
        direction: Direction,
        db: &Arc<DBClient>,
    ) -> Self {
        let pivot_points =
            Self::calculate_pivot_points(strategy, symbol, &entry_price, multiplier, direction);
        let stop_price = match direction {
            Direction::Long => entry_price.clone() * to_num!(1.0 - pivot_points[0].1),
            Direction::Short => entry_price.clone() * to_num!(1.0 + pivot_points[0].1),
        };
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
            direction,
            transact_type,
            ..Default::default()
        };
        if let Err(err) = stop.persist_to_db(db.clone()).await {
            error!("Failed to persist stop to db, error={}", err);
        }
        stop
    }

    pub fn refresh_entry_price(&mut self, entry_price: Num) {
        self.pivot_points = Self::calculate_pivot_points(
            &self.strategy,
            &self.symbol,
            &entry_price,
            self.multiplier,
            self.direction,
        );
    }

    fn calculate_pivot_points(
        strategy: &str,
        symbol: &str,
        entry_price: &Num,
        multiplier: f64,
        direction: Direction,
    ) -> [(i16, f64, f64); 4] {
        fn pivot_to_price(
            entry_price: &Num,
            pivot_points: [(i16, f64, f64); 4],
            index: usize,
            direction: Direction,
        ) -> f64 {
            match direction {
                Direction::Long => (entry_price.clone() * to_num!(1.0 + pivot_points[index].1))
                    .round_with(3)
                    .to_f64()
                    .unwrap(),
                Direction::Short => (entry_price.clone() * to_num!(1.0 - pivot_points[index].1))
                    .round_with(3)
                    .to_f64()
                    .unwrap(),
            }
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
            pivot_to_price(&entry_price, pivot_points, 0, direction),
            pivot_to_price(&entry_price, pivot_points, 1, direction),
            pivot_to_price(&entry_price, pivot_points, 2, direction),
            pivot_to_price(&entry_price, pivot_points, 3, direction),
        );
        pivot_points
    }

    pub async fn price_update(&mut self, current_price: Num, db: &Arc<DBClient>) -> Num {
        self.current_price = current_price.clone();
        let price = current_price.to_f64().unwrap();
        let price_change = price - self.watermark.to_f64().unwrap();
        if price_change <= 0.0 || self.status == LockerStatus::Disabled {
            return self.stop_price.clone();
        }
        let entry_price = self.entry_price.to_f64().unwrap();
        let mut stop_loss_level = self.stop_price.to_f64().unwrap();
        for (zone, percentage_change, new_trail_factor) in self.pivot_points.iter() {
            match self.direction {
                Direction::Long => {
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
                }
                Direction::Short => {
                    match zone {
                        4 => {
                            if price < (entry_price * (1.0 + percentage_change)) {
                                // final trail at 1%
                                stop_loss_level = price + (entry_price * 0.01)
                            } else {
                                // close distance X% -> 1%
                                stop_loss_level -= price_change * new_trail_factor
                            }
                        }
                        _ => {
                            if price < entry_price * (1.0 + percentage_change) {
                                continue;
                            }
                            // set trail based on zone
                            stop_loss_level -= new_trail_factor * price_change;
                        }
                    }
                }
            }
            if *zone > self.zone {
                info!(
                    "Zone update for stop: strategy: {}, symbol: {}, last price: {} new stop level: {} in zone: {}, direction: {}",
                    self.strategy,
                    self.symbol,
                    current_price.clone().round_with(2),
                    self.stop_price.clone().round_with(2),
                    zone,
                    self.direction
                );
                self.zone = *zone;
            }
            break;
        }

        let stop_loss_level = to_num!(stop_loss_level);

        self.stop_price = stop_loss_level.clone();
        self.watermark = current_price;

        if self.transact_type == TransactionType::Position {
            let _ = self.persist_to_db(db.clone()).await;
        }
        stop_loss_level
    }

    pub async fn persist_to_db(&mut self, db: Arc<DBClient>) -> Result<()> {
        let columns = vec![
            "strategy",
            "symbol",
            "entry_price",
            "stop_price",
            "type",
            "zone",
            "multiplier",
            "direction",
            "watermark",
            "status",
            "transact_type",
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
            .bind(self.zone)
            .bind(self.multiplier)
            .bind(self.direction.to_string())
            .bind(self.watermark.round_with(3).to_f64().unwrap())
            .bind(self.status.to_string())
            .bind(self.transact_type.to_string())
            .bind(self.local_id)
            .execute(&db.pool)
            .await
        {
            bail!("Failed to publish to db, error={}", err)
        }
        Ok(())
    }
}
