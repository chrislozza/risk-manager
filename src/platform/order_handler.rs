use apca::api::v2::{asset, order};
use num_decimal::Num;

use std::sync::Arc;

use tracing::info;

use anyhow::bail;
use anyhow::Result;

use super::super::events::Side;
use super::data::mktorder::MktOrder;
use super::data::mktorder::OrderAction;
use super::data::mktposition::MktPosition;

use super::web_clients::Connectors;

use crate::to_num;

pub struct OrderHandler {
    connectors: Arc<Connectors>,
}

impl OrderHandler {
    pub fn new(connectors: &Arc<Connectors>) -> Self {
        OrderHandler {
            connectors: Arc::clone(connectors),
        }
    }

    pub async fn subscribe_to_events(&self) -> Result<()> {
        self.connectors.subscibe_to_order_updates().await
    }

    pub async fn create_position(
        &mut self,
        symbol: &str,
        strategy: &str,
        target_price: Num,
        position_size: Num,
        side: &Side,
    ) -> Result<MktOrder> {
        let limit_price = target_price.clone() * to_num!(1.07);
        let stop_price = target_price * to_num!(1.01);
        let amount = order::Amount::quantity(position_size.round());
        let side = Self::convert_side(side);
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
        match self.connectors.place_order(&request).await {
            Err(error) => bail!("Failed to place order for request: {request:?}, error: {error}"),
            Ok(order) => Ok(MktOrder::new(OrderAction::Create, order, Some(strategy))),
        }
    }

    pub async fn liquidate_position(&self, position: &MktPosition) -> Result<MktOrder> {
        let symbol = asset::Symbol::Sym(position.get_position().symbol.to_string());
        match self.connectors.close_position(&symbol).await {
            Err(error) => {
                bail!("Failed to liquidate position for symbol {symbol}, error={error}")
            }
            Ok(order) => Ok(MktOrder::new(
                OrderAction::Liquidate,
                order,
                Some(position.get_strategy()),
            )),
        }
    }

    pub async fn cancel_order(&self, order: &MktOrder) -> Result<()> {
        let id = order.get_order().id;
        let symbol = &order.get_order().symbol;
        if let Err(error) = self.connectors.cancel_order(&id).await {
            bail!("Failed to cancel order for symbol {symbol}, error={error}")
        }
        Ok(())
    }

    fn convert_side(side: &Side) -> apca::api::v2::order::Side {
        match side {
            Side::Buy => apca::api::v2::order::Side::Buy,
            Side::Sell => apca::api::v2::order::Side::Sell,
        }
    }
}
