use apca::api::v2::{asset, order, orders, position, positions};
use num_decimal::Num;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::thread;
use tokio::sync::Mutex;

use tracing::debug; 
use tracing::info; 
use tracing::warn;
use tracing::error; 

use anyhow::Result;
use anyhow::bail;
use tokio::sync::broadcast;

use super::mktorder::OrderAction;
use super::mktorder::MktOrder;
use super::mktposition::MktPosition;
use super::stream_handler::WebSocket;
use super::mktdata::MktData;
use super::Event;
use crate::float_to_num;
use tokio_util::sync::CancellationToken;

pub struct OrderHandler {
    connectors: Arc<Connectors>,
}

impl OrderHandler {
    pub fn new(connectors: &Arc<Client>) -> Self {
        OrderHandler {
            connectors: Arc::clone(connectors)
        }
    }

//    pub async fn startup(&mut self) -> (HashMap<String, MktPosition>, HashMap<String, MktOrder>) {
//        let orders = match self.get_orders().await {
//            Ok(val) => val,
//            Err(err) => panic!("{err:?}"),
//        };
//        let positions = match self.get_positions().await {
//            Ok(val) => val,
//            Err(err) => panic!("{err:?}"),
//        };
//        if let Err(err) = self.stream_handler.subscribe_to_order_updates().await {
//            error!("Failed to subscribe to stream, error: {err:?}");
//            panic!("{:?}", err);
//        };
//        (positions, orders)
//    }
//
//    pub async fn shutdown(&self) {
//        info!("Shutdown initiated");
//    }

    pub async fn create_position(
        &mut self,
        symbol: &str,
        strategy: &str,
        target_price: Num,
        position_size: Num,
        side: Side,
    ) -> Result<MktOrder> {
        let limit_price = target_price.clone() * float_to_num!(1.07);
        let stop_price = target_price * float_to_num!(1.01);
        let amount = order::Amount::quantity(position_size.round());
        let side = Self::convert_side(&side);
        info!(
            "Placing order for fields limit_price: {}, stop_price: {}, amount: {:?}, side: {:?}",
            limit_price, stop_price, position_size, side
        );

        let request = order::OrderReqInit {
            type_: order::Type::StopLimit,
            limit_price: Some(limit_price.round_with(2)),
            stop_price: Some(stop_price.round_with(2)),
            ..Default::default()
        }
        .init(symbol, side, amount);
        self.connectors.place_order(request).await?
    }

    pub async fn liquidate_position(&self, position: &MktPosition) -> Result<()> {
        let symbol = asset::Symbol::Sym(position.get_position().symbol);
        self.connectors.close_position(request).await?
    }

    pub async fn cancel_order(&self, order: &MktOrder) -> Result<()> {
        let id = order.get_order().id;
        self.connectors.cancel_order(id).await?
    }

    fn convert_side(side: &Side) -> apca::api::v2::order::Side {
        match side {
            Side::Buy => apca::api::v2::order::Side::Buy,
            Side::Sell => apca::api::v2::order::Side::Sell,
        }
    }
}
