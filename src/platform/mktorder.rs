use apca::api::v2::order;

use num_decimal::Num;

#[derive(Debug)]
pub struct MktOrder {
    order: order::Order,
}

impl MktOrder {
    pub fn new(order: order::Order) -> Self {
        MktOrder { order }
    }

    pub fn get_symbol(&self) -> &String {
        &self.order.symbol
    }

    pub fn market_value(&self) -> Num {
        let market_value = match &self.order.amount {
            order::Amount::Quantity { quantity } => quantity.clone(),
            _ => Num::from(0)
        };
        return market_value * self.order.average_fill_price.as_ref().unwrap()
    }
}
