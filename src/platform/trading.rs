use apca::api::v2::{asset, order, orders, position, positions};
use apca::Client;
use log::{error, info, warn};
use num_decimal::Num;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::{thread, time::Duration};

use tokio::sync::broadcast;

use super::mktorder::{MktOrder, OrderAction};
use super::mktposition::MktPosition;
use super::stream_handler::StreamHandler;
use super::Event;
use crate::utils::round_to;

const DENOM: f32 = 100.00;

pub struct Trading {
    client: Arc<Mutex<Client>>,
    is_alive: Arc<Mutex<bool>>,
    stream_handler: StreamHandler,
    receiver: broadcast::Receiver<Event>,
    sender: broadcast::Sender<Event>,
}

impl Trading {
    pub fn new(client: Arc<Mutex<Client>>) -> Self {
        let (sender, receiver) = broadcast::channel(2);
        let stream_handler = StreamHandler::new(Arc::clone(&client), sender.clone());
        Trading {
            client,
            is_alive: Arc::new(Mutex::new(false)),
            stream_handler,
            sender,
            receiver,
        }
    }

    pub fn stream_reader(&self) -> broadcast::Receiver<Event> {
        self.sender.subscribe()
    }

    pub async fn startup(&mut self) -> (HashMap<String, MktPosition>, HashMap<String, MktOrder>) {
        let orders = match self.get_orders().await {
            Ok(val) => val,
            Err(err) => panic!("{err:?}"),
        };
        let positions = match self.get_positions().await {
            Ok(val) => val,
            Err(err) => panic!("{err:?}"),
        };
        match self.stream_handler.subscribe_to_order_updates().await {
            Err(err) => {
                error!("Failed to subscribe to stream, error: {err:?}");
                panic!("{:?}", err);
            }
            _ => (),
        };
        (positions, orders)
    }

    pub async fn shutdown(&self) {
        info!("Shutdown initiated");
    }

    pub async fn create_position(
        &mut self,
        symbol: &String,
        target_price: Num,
        position_size: Num,
        side: order::Side,
    ) -> Result<MktOrder, ()> {

        let limit_price = target_price.clone() * Num::new((1.07 * DENOM) as i32, DENOM as i32);
        let stop_price = target_price * Num::new((1.01 * DENOM) as i32, DENOM as i32);
        let amount = order::Amount::quantity(position_size.to_u64().unwrap());
        info!("Placing order for fields limit_price: {}, stop_price: {}, amount: {:?}, side: {:?}", limit_price, stop_price, amount, side);

        let request = order::OrderReqInit {
            type_: order::Type::StopLimit,
            limit_price: Some(round_to(limit_price, 2)),
            stop_price: Some(round_to(stop_price, 2)),
            ..Default::default()
        }
        .init(symbol, side, order::Amount::quantity(position_size.to_u64().unwrap()));
        let mut retry = 5;
        loop {
            info!("Before posting the order");
            match self
                .client
                .lock()
                .await
                .issue::<order::Post>(&request)
                .await
            {
                Ok(val) => {
                    info!("Placed order {val:?}");
                    return Ok(MktOrder::new(OrderAction::Create, val));
                }
                Err(apca::RequestError::Endpoint(order::PostError::NotPermitted(err))) => {
                    if retry == 0 {
                        error!("Failed to post order");
                        return Err(());
                    }
                    warn!("Retry order posting retries left: {retry}, err: {err:?}");
                }
                Err(err) => {
                    error!("Unknown error: {err:?}");
                    return Err(());
                }
            }
            retry -= 1;
            thread::sleep(Duration::from_secs(1));
        }
    }

    pub async fn liquidate_position(&self, position: &MktPosition) -> bool {
        let mut retry = 5;
        loop {
            info!("Before the liquidate position");
            let result = self.client.lock().await
                .issue::<position::Delete>(&asset::Symbol::Sym(
                    position.get_position().symbol.to_string(),
                )).await;
            if let Ok(val) = result
            {
                info!("Placed order {:?}", val);
                return true;
            }
            else if let Err(err) = result {
                if retry == 0 {
                    error!("Failed to liquidate position");
                    break;
                }
                warn!("Retry liquidating position retries left: {retry}, err: {err:?}");
            }
            retry -= 1;
            thread::sleep(Duration::from_secs(1));
        }
        false
    }

    pub async fn cancel_order(&self, order: &MktOrder) -> bool {
        let mut retry = 5;
        loop {
            info!("Before the liquidate position");
            match self
                .client
                .lock()
                .await
                .issue::<order::Delete>(&order.get_order().id)
                .await
            {
                Ok(val) => {
                    info!("Placed order {:?}", val);
                    return true;
                }
                Err(err) => {
                    if retry == 0 {
                        error!("Failed to post order, not permitted");
                        break;
                    }
                    warn!("Retry order cancelling retries left: {retry}, err: {err:?}");
                }
            }
            retry -= 1;
            thread::sleep(Duration::from_secs(1));
        }
        false
    }

    async fn get_orders(
        &self,
    ) -> Result<HashMap<String, MktOrder>, apca::RequestError<orders::GetError>> {
        let mut retry = 5;
        loop {
            let request = orders::OrdersReq::default();
            match self
                .client
                .lock()
                .await
                .issue::<orders::Get>(&request)
                .await
            {
                Ok(val) => {
                    info!("Downloaded orders");
                    let mut orders = HashMap::default();
                    for v in val {
                        info!("Order download {v:?}");
                        orders.insert(v.symbol.to_string(), MktOrder::new(OrderAction::Create, v));
                    }
                    return Ok(orders);
                }
                Err(err) => {
                    retry -= 1;
                    if retry == 0 {
                        error!("Failed to retrieve orders {}", err);
                        return Err(err);
                    }
                }
            }
            thread::sleep(Duration::from_secs(1));
        }
    }

    pub async fn get_positions(
        &self,
    ) -> Result<HashMap<String, MktPosition>, apca::RequestError<positions::GetError>> {
        let mut retry = 5;
        loop {
            match self
                .client
                .lock()
                .await
                .issue::<positions::Get>(&())
                .await
            {
                Ok(val) => {
                    let mut positions = HashMap::default();
                    for v in val {
                        info!("Position download {v:?}");
                        positions.insert(v.symbol.to_string(), MktPosition::new(v));
                    }
                    return Ok(positions);
                }
                Err(err) => {
                    retry -= 1;
                    if retry == 0 {
                        error!("Failed to retrieve positions {}", err);
                        return Err(err);
                    }
                }
            }
            thread::sleep(Duration::from_secs(1));
        }
    }
}
