use anyhow::Ok;
use apca::api::v2::order;

use anyhow::bail;
use anyhow::Result;
use chrono::DateTime;
use chrono::Utc;
use num_decimal::Num;
use std::collections::HashMap;

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use uuid::Uuid;

use super::db_client::DBClient;
use crate::events::Direction;
use crate::platform::web_clients::Connectors;
use crate::to_num;

#[derive(Debug, Clone, Copy)]
pub enum OrderAction {
    Create,
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
            "Active" => std::result::Result::Ok(OrderAction::Create),
            "Closed" => std::result::Result::Ok(OrderAction::Liquidate),
            _ => Err(format!("Failed to parse order action, unknown: {}", val)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MktOrder {
    pub local_id: Option<Uuid>,
    pub order_id: Uuid,
    pub order: Option<order::Order>,
    pub strategy: String,
    pub symbol: String,
    pub direction: Direction,
    pub action: OrderAction,
    pub fill_price: Num,
    pub fill_time: DateTime<Utc>,
    pub quantity: Num,
}

impl MktOrder {
    pub async fn new(
        action: OrderAction,
        order_id: Uuid,
        strategy: &str,
        symbol: &str,
        direction: Direction,
        db: &Arc<DBClient>,
    ) -> Result<Self> {
        let mut order = MktOrder {
            local_id: None,
            order_id,
            order: None,
            strategy: strategy.to_string(),
            symbol: symbol.to_string(),
            direction,
            action,
            fill_price: to_num!(0.0),
            fill_time: DateTime::<Utc>::default(),
            quantity: Num::from(0_i32),
        };
        order.persist_db(db.clone()).await?;
        Ok(order)
    }

    async fn persist_db(&mut self, db: Arc<DBClient>) -> Result<()> {
        let columns = vec![
            "action",
            "order_id",
            "strategy",
            "symbol",
            "direction",
            "fill_price",
            "fill_time",
            "quantity",
            "local_id",
        ];

        let stmt = match self.local_id {
            Some(_) => db
                .query_builder
                .prepare_update_statement("mktorder", &columns),
            None => {
                self.local_id = Some(Uuid::new_v4());
                db.query_builder
                    .prepare_insert_statement("mktorder", &columns)
            }
        };

        if let Err(err) = sqlx::query(&stmt)
            .bind(self.action.to_string())
            .bind(self.order_id)
            .bind(self.strategy.to_string())
            .bind(self.symbol.to_string())
            .bind(self.direction.to_string())
            .bind(self.fill_price.round_with(3).to_f64())
            .bind(self.fill_time)
            .bind(self.quantity.to_i64())
            .bind(self.local_id)
            .fetch_one(&db.pool)
            .await
        {
            bail!("Failed to publish to db, error={}", err)
        }
        Ok(())
    }

    fn update_inner(&mut self, order: order::Order) -> &Self {
        self.order = Some(order);
        self
    }

    pub fn get_order_id(&self) -> Uuid {
        self.order.as_ref().unwrap().id.0
    }

    pub fn get_action(&self) -> OrderAction {
        self.action
    }

    pub fn get_direction(&self) -> Direction {
        self.direction
    }

    pub fn get_limit_price(&self) -> Num {
        if let Some(order) = &self.order {
            return order.limit_price.as_ref().unwrap().clone();
        }
        panic!("Failed to pull limit price from order")
    }

    pub fn get_fill_price(&self) -> Num {
        if let Some(order) = &self.order {
            return order.average_fill_price.as_ref().unwrap().clone();
        }
        to_num!(0.0)
    }

    pub fn get_quantity(&self) -> Num {
        if let Some(order) = &self.order {
            return order.filled_quantity.clone();
        }
        panic!("Failed to pull limit price from order")
    }

    pub fn get_fill_time(&self) -> DateTime<Utc> {
        if let Some(order) = &self.order {
            return order.filled_at.unwrap();
        }
        panic!("Order fill not complete yet for entry time request")
    }

    pub fn get_symbol(&self) -> &str {
        &self.symbol
    }

    pub fn get_strategy(&self) -> &str {
        &self.strategy
    }

    pub fn market_value(&self) -> Num {
        if let Some(order) = &self.order {
            let market_value = match &order.amount {
                order::Amount::Quantity { quantity } => quantity.clone(),
                _ => Num::from(0),
            };
            return market_value * order.average_fill_price.as_ref().unwrap();
        }
        panic!("Order fill not complete yet for entry time request")
    }
}

impl fmt::Display for MktOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let quantity = match self.order.as_ref().unwrap().amount.clone() {
            order::Amount::Quantity { quantity } => quantity,
            _ => Num::from(0),
        };
        write!(
            f,
            "Order symbol[{}], limitPrice[{}], size[{}] status[{}]",
            self.symbol,
            self.order
                .as_ref()
                .unwrap()
                .limit_price
                .as_ref()
                .unwrap()
                .round_with(3),
            quantity.round(),
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

    pub async fn add_order(&mut self, order_id: Uuid, order: MktOrder) -> Result<()> {
        self.mktorders.insert(order_id, order);
        Ok(())
    }

    pub async fn update_order(&mut self, order_id: &Uuid) -> Result<MktOrder> {
        let order = self.connectors.get_order(*order_id).await?;
        if let Some(mktorder) = self.mktorders.get_mut(&order.id.0) {
            mktorder.update_inner(order);
            Ok(mktorder.clone())
        } else {
            bail!("MktOrder key not found in HashMap")
        }
    }

    pub async fn get_order(&self, order_id: &Uuid) -> Result<&MktOrder> {
        Ok(&self.mktorders[order_id])
    }

    pub async fn get_orders(&self) -> &HashMap<Uuid, MktOrder> {
        &self.mktorders
    }

    pub async fn update_orders(&mut self) -> Result<&HashMap<Uuid, MktOrder>> {
        let orders = self.connectors.get_orders().await?;
        for order in &orders {
            if let Some(mktorder) = self.mktorders.get_mut(&order.id.0) {
                mktorder.update_inner(order.clone());
            }
        }
        Ok(&self.mktorders)
    }
}
