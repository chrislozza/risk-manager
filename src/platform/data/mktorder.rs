use apca::api::v2::order;

use anyhow::Result;
use num_decimal::Num;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use tracing::info;

use super::super::web_clients::Connectors;

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

#[derive(Debug, Clone)]
pub struct MktOrder {
    action: OrderAction,
    order: order::Order,
    strategy: String,
}

impl MktOrder {
    pub fn new(action: OrderAction, order: order::Order, strategy: Option<&str>) -> Self {
        let strategy = if let Some(strategy) = strategy {
            strategy.to_string()
        } else {
            //db lookup
            String::default()
        };

        MktOrder {
            action,
            order,
            strategy,
        }
    }

    pub fn get_order(&self) -> &order::Order {
        &self.order
    }

    pub fn get_action(&self) -> &OrderAction {
        &self.action
    }

    pub fn get_strategy(&self) -> &str {
        //&self.strategy
        "00cl1"
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
}

impl MktOrders {
    pub fn new(connectors: &Arc<Connectors>) -> Self {
        MktOrders {
            connectors: Arc::clone(connectors),
            mktorders: HashMap::default(),
        }
    }

    pub async fn add_order(&mut self, order: MktOrder) -> Result<()> {
        self.mktorders
            .insert(order.get_order().symbol.clone(), order);
        Ok(())
    }

    pub async fn get_order(&mut self, symbol: &str) -> Result<&MktOrder> {
        Ok(&self.mktorders[symbol])
    }

    pub async fn get_orders(&self) -> &HashMap<String, MktOrder> {
        &self.mktorders
    }

    pub async fn update_orders(&mut self) -> Result<()> {
        let orders = self.connectors.get_orders().await?;
        for order in &orders {
            let mktorder = MktOrder::new(OrderAction::Create, order.clone(), Some("00cl1"));
            info!("{mktorder}");
            self.mktorders
                .insert(mktorder.get_order().symbol.clone(), mktorder);
        }
        Ok(())
    }
}
