use anyhow::bail;
use anyhow::Ok;
use anyhow::Result;
use apca::api::v2::order;
use chrono::DateTime;
use chrono::Utc;
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
use tracing::info;
use uuid::Uuid;

use super::db_client::DBClient;
use crate::events::Direction;
use crate::platform::web_clients::Connectors;
use crate::to_num;

#[derive(Debug, Clone, Copy, Default)]
pub enum OrderStatus {
    #[default]
    Waiting,
    New,
    Filled,
    Cancelled,
}

impl fmt::Display for OrderStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl FromStr for OrderStatus {
    type Err = String;

    fn from_str(val: &str) -> std::result::Result<Self, Self::Err> {
        match val {
            "Waiting" => std::result::Result::Ok(OrderStatus::Waiting),
            "New" => std::result::Result::Ok(OrderStatus::New),
            "Filled" => std::result::Result::Ok(OrderStatus::Filled),
            "Cancelled" => std::result::Result::Ok(OrderStatus::Cancelled),
            _ => Err(format!("Failed to parse order status, unknown: {}", val)),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub enum OrderAction {
    Create,
    #[default]
    Liquidate,
}

impl fmt::Display for OrderAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl FromStr for OrderAction {
    type Err = String;

    fn from_str(val: &str) -> std::result::Result<Self, Self::Err> {
        match val {
            "Create" => std::result::Result::Ok(OrderAction::Create),
            "Liquidate" => std::result::Result::Ok(OrderAction::Liquidate),
            _ => Err(format!("Failed to parse order action, unknown: {}", val)),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MktOrder {
    pub local_id: Uuid,
    pub strategy: String,
    pub symbol: String,
    pub direction: Direction,
    pub action: OrderAction,
    pub entry_price: Num,
    pub fill_price: Num,
    pub entry_time: DateTime<Utc>,
    pub fill_time: DateTime<Utc>,
    pub quantity: Num,
    pub status: OrderStatus,
}

impl FromRow<'_, PgRow> for MktOrder {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        fn sqlx_to_num(row: &PgRow, value: &str) -> sqlx::Result<Num> {
            match row.try_get::<f64, _>(value) {
                std::result::Result::Ok(val) => std::result::Result::Ok(to_num!(val)),
                Err(err) => Err(err),
            }
        }

        sqlx::Result::Ok(Self {
            local_id: row.try_get("local_id")?,
            strategy: row.try_get("strategy")?,
            symbol: row.try_get("symbol")?,
            direction: Direction::from_str(row.try_get("direction")?).unwrap(),
            action: OrderAction::from_str(row.try_get("action")?).unwrap(),
            entry_price: sqlx_to_num(row, "entry_price")?,
            fill_price: sqlx_to_num(row, "fill_price")?,
            entry_time: row.try_get("entry_time")?,
            fill_time: row.try_get("fill_time")?,
            quantity: Num::from(row.try_get::<i64, &str>("quantity")?),
            status: OrderStatus::from_str(row.try_get("status")?).unwrap(),
        })
    }
}

impl MktOrder {
    pub async fn new(
        order_id: Uuid,
        action: OrderAction,
        strategy: &str,
        symbol: &str,
        direction: Direction,
        db: Option<&Arc<DBClient>>,
    ) -> Result<Self> {
        let mut order = MktOrder {
            local_id: Uuid::nil(),
            strategy: strategy.to_string(),
            symbol: symbol.to_string(),
            direction,
            action,
            ..Default::default()
        };
        if let Some(db) = db {
            order.persist_db(db.clone(), Some(order_id)).await?;
        }
        Ok(order)
    }

    fn build_query<'a>(&'a self, stmt: &'a str) -> Query<'_, Postgres, PgArguments> {
        sqlx::query(stmt)
            .bind(self.action.to_string())
            .bind(self.strategy.to_string())
            .bind(self.symbol.to_string())
            .bind(self.direction.to_string())
            .bind(self.entry_price.round_with(3).to_f64())
            .bind(self.fill_price.round_with(3).to_f64())
            .bind(self.entry_time)
            .bind(self.fill_time)
            .bind(self.quantity.to_i64())
            .bind(self.status.to_string())
            .bind(self.local_id)
    }

    async fn persist_db(&mut self, db: Arc<DBClient>, local_id: Option<Uuid>) -> Result<()> {
        let columns = vec![
            "action",
            "strategy",
            "symbol",
            "direction",
            "entry_price",
            "fill_price",
            "entry_time",
            "fill_time",
            "quantity",
            "status",
            "local_id",
        ];

        fn get_sql_stmt(local_id: &Uuid, columns: Vec<&str>, db: &Arc<DBClient>) -> String {
            if Uuid::is_nil(local_id) {
                db.query_builder
                    .prepare_insert_statement("mktorder", &columns)
            } else {
                db.query_builder
                    .prepare_update_statement("mktorder", &columns)
            }
        }

        let stmt = get_sql_stmt(&self.local_id, columns, &db);
        if let Some(local_id) = local_id {
            self.local_id = local_id;
        }

        if let Err(err) = self.build_query(&stmt).execute(&db.pool).await {
            bail!("Locker failed to publish to db, error={}", err)
        }
        Ok(())
    }

    async fn update_inner(&mut self, order: order::Order, db: Arc<DBClient>) -> Result<&Self> {
        if let Some(price) = order.limit_price {
            self.entry_price = price
        }

        if let Some(price) = order.average_fill_price {
            self.fill_price = price;
        }

        if let Some(time) = order.submitted_at {
            self.entry_time = time;
        }

        if let Some(time) = order.filled_at {
            self.fill_time = time;
        }

        if let order::Amount::Quantity { quantity } = order.amount {
            self.quantity = quantity;
        }

        self.status = match order.status {
            order::Status::Accepted => OrderStatus::Waiting,
            order::Status::New => OrderStatus::New,
            order::Status::Filled => OrderStatus::Filled,
            order::Status::Canceled => OrderStatus::Cancelled,
            _ => self.status,
        };

        self.persist_db(db, None).await?;
        info!("Updating mktorder {}", self);
        Ok(self)
    }
}

impl fmt::Display for MktOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Order strategy[{}], symbol[{}], entry_price[{}], size[{}] action[{}]",
            self.strategy,
            self.symbol,
            self.entry_price.round_with(3).to_f64().unwrap(),
            self.quantity.round_with(3).to_i64().unwrap(),
            self.action
        )
    }
}

pub struct MktOrders {
    connectors: Arc<Connectors>,
    mktorders: HashMap<Uuid, MktOrder>,
    db: Arc<DBClient>,
}

impl MktOrders {
    pub fn new(connectors: &Arc<Connectors>, db: &Arc<DBClient>) -> Self {
        MktOrders {
            connectors: Arc::clone(connectors),
            mktorders: HashMap::default(),
            db: Arc::clone(db),
        }
    }

    pub async fn startup(&mut self, order_ids: Vec<Uuid>) -> Result<Vec<&MktOrder>> {
        for order_id in order_ids {
            let columns = vec!["local_id"];
            let stmt = self
                .db
                .query_builder
                .prepare_fetch_statement("mktorder", &columns);
            let mktorder = match sqlx::query_as::<_, MktOrder>(&stmt)
                .bind(order_id)
                .fetch_one(&self.db.pool)
                .await
            {
                sqlx::Result::Ok(val) => val,
                Err(err) => panic!(
                    "Failed to fetch transactions from db, err={}, closing app",
                    err
                ),
            };
            self.mktorders.insert(order_id, mktorder);
        }
        let mktorders = Vec::from_iter(self.mktorders.values());
        Ok(mktorders)
    }

    pub async fn add_order(
        &mut self,
        order_id: Uuid,
        symbol: &str,
        strategy: &str,
        direction: Direction,
        action: OrderAction,
    ) -> Result<MktOrder> {
        let mut mktorder = MktOrder::new(
            order_id,
            action,
            strategy,
            symbol,
            direction,
            Some(&self.db),
        )
        .await?;
        let order = self.connectors.get_order(order_id).await?;
        let _ = mktorder.update_inner(order, self.db.clone()).await?;

        self.mktorders.insert(order_id, mktorder.clone());
        Ok(mktorder)
    }

    pub async fn update_order(&mut self, order_id: &Uuid) -> Result<MktOrder> {
        let order = self.connectors.get_order(*order_id).await?;
        if let Some(mktorder) = self.mktorders.get_mut(order_id) {
            Ok(mktorder.update_inner(order, self.db.clone()).await?.clone())
        } else {
            bail!(
                "MktOrder {} with order_id: {} not found",
                order.symbol,
                order_id
            )
        }
    }

    pub fn get_order(&self, order_id: &Uuid) -> Option<&MktOrder> {
        self.mktorders.get(order_id)
    }

    pub async fn update_orders(&mut self) -> Result<&HashMap<Uuid, MktOrder>> {
        let orders = self.connectors.get_orders().await?;
        for order in &orders {
            if let Some(mktorder) = self.mktorders.get_mut(&order.id.0) {
                let _ = mktorder.update_inner(order.clone(), self.db.clone()).await;
            }
        }
        Ok(&self.mktorders)
    }
}
