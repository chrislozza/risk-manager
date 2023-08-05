use apca::api::v2::order;

use std::str::FromStr;
use chrono::DateTime;
use chrono::Utc;
use anyhow::Result;
use anyhow::bail;
use num_decimal::Num;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio_postgres::types::ToSql;
use crate::utils::to_sql_type;
use tokio_postgres::Row;
use uuid::Uuid;

use tracing::info;

use super::DBClient;
use crate::platform::web_clients::Connectors;
use crate::to_num;
use crate::events::Direction;

#[derive(Debug, Clone)]
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

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        match val {
            "Active" => Ok(OrderAction::Create),
            "Closed" => Ok(OrderAction::Liquidate),
            _ => Err(format!(
                "Failed to parse order action, unknown: {}",
                val
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MktOrder {
    local_id: Uuid,
    action: OrderAction,
    order: order::Order,
    strategy: String,
    symbol: String,
    direction: Direction,
    fill_price: Option<Num>,
    fill_time: Option<DateTime<Utc>>
}

impl MktOrder {
    pub fn new(action: OrderAction, order: order::Order, strategy: &str, symbol: &str, direction: Direction) -> Self {
        MktOrder {
            local_id: order.id.0,
            action,
            order,
            strategy: strategy.to_string(),
            symbol: symbol.to_string(),
            direction,
            fill_price: None,
            fill_time: None
        }
    }

    fn load_from_db(row: &Row, order: &order::Order) -> Self {
        let local_id = row.get::<&str, Uuid>("local_id");
        let strategy = row.get::<&str, String>("strategy");
        let symbol = row.get::<&str, String>("symbol");
        let action = OrderAction::from_str(row.get::<&str, &str>("action")).unwrap();
        let direction = Direction::from_str(row.get::<&str, &str>("direction")).unwrap();

        MktOrder {
            local_id,
            action,
            order: order.clone(),
            strategy,
            symbol,
            direction,
            fill_price: None,
            fill_time: None
        }
    }

    fn update_inner(&mut self, order: &order::Order)-> &Self {
        self.order = order.clone();
        self
    }

    pub fn get_order_id(&self) -> order::Id {
        self.order.id
    }

    pub fn get_action(&self) -> OrderAction {
        self.action
    }

    pub fn get_direction(&self) -> Direction {
        self.direction
    }

    pub fn get_limit_price(&self) -> Num {
        match self.order.limit_price {
            Some(val) => val,
            None => panic!("Failed to pull limit price from order")
        }
    }

    pub fn get_fill_price(&self) -> Num {
        match self.order.average_fill_price.clone() {
            Some(val) => val,
            None => to_num!(0.0)
        }
    }

    pub fn get_quantity(&self) -> Num {
        self.order.filled_quantity.clone()
    }

    pub fn get_fill_time(&self) -> DateTime<Utc> {
        match self.order.filled_at.clone() {
            Some(val) => val,
            None => panic!("Order fill not complete yet for entry time request")
        }
    }

    pub fn get_symbol(&self) -> &str {
        &self.symbol
    }

    pub fn get_strategy(&self) -> &str {
        &self.strategy
    }

    pub fn market_value(&self) -> Num {
        let market_value = match &self.order.amount {
            order::Amount::Quantity { quantity } => quantity.clone(),
            _ => Num::from(0),
        };
        return market_value * self.order.average_fill_price.as_ref().unwrap();
    }
}

impl fmt::Display for MktOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let quantity = match self.order.amount.clone() {
            order::Amount::Quantity { quantity } => quantity,
            _ => Num::from(0),
        };
        write!(
            f,
            "Order symbol[{}], limitPrice[{}], size[{}] status[{}]",
            self.order.symbol,
            self.order.limit_price.as_ref().unwrap().round_with(3),
            quantity.round(),
            self.action
        )
    }
}

pub struct MktOrders {
    connectors: Arc<Connectors>,
    mktorders: HashMap<String, MktOrder>,
    db: Arc<DBClient>
}

impl MktOrders {
    pub fn new(connectors: &Arc<Connectors>, db: &Arc<DBClient>) -> Self {
        MktOrders {
            connectors: Arc::clone(connectors),
            mktorders: HashMap::default(),
            db: Arc::clone(db),
        }
    }

    pub async fn add_order(&mut self, order: MktOrder) -> Result<()> {
        self.mktorders
            .insert(order.get_symbol().to_string(), order);
        Ok(())
    }

    pub async fn update_order(&mut self, order: &order::Order) -> Result<MktOrder> {
        if let Some(mktorder) = self.mktorders.get_mut(&order.symbol) {
            Ok(mktorder.update_inner(order).clone())
        } else {
            bail!("MktOrder key not found in HashMap")
        }
    }

    pub async fn get_order(&mut self, symbol: &str) -> Result<&MktOrder> {
        Ok(&self.mktorders[symbol])
    }

    pub async fn get_orders(&self) -> &HashMap<String, MktOrder> {
        &self.mktorders
    }

    pub async fn update_orders(&mut self) -> Result<&HashMap<String, MktOrder>> {
        let orders = self.connectors.get_orders().await?;
        for order in &orders {
            let rows = match self.db.fetch("order", vec!["local_id"], vec![to_sql_type((order.id).0)]).await {
                Ok(val) => val,
                Err(err) => panic!("Failed to fetch order from db with id: {}, closing app", order.id.to_string()),
            };
            let mktorder = MktOrder::load_from_db(&rows[0], &order);
            info!("{mktorder}");
            self.mktorders
                .insert(mktorder.get_symbol().to_string(), mktorder);
        }
        Ok(&self.mktorders)
    }
}
