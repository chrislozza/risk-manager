use apca::api::v2::order;

use num_decimal::Num;
use std::fmt;

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
            self.order.limit_price.as_ref().unwrap(),
            quantity,
            self.action
        )
    }
}
