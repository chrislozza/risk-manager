use apca::api::v2::{asset, order, orders, position, positions, updates};
use apca::Client;
use log::{error, info, warn};
use num_decimal::Num;
use std::sync::{Arc, Mutex};
use std::{thread, time::Duration};

use futures::StreamExt as _;
use futures::TryStreamExt as _;

use tokio::time;

use super::mktorder;
use super::mktposition;

const DENOM: f32 = 100.00;

pub struct Trading {
    client: Arc<Mutex<Client>>,
    positions: Vec<mktposition::MktPosition>,
    orders: Vec<mktorder::MktOrder>,
    is_alive: Arc<Mutex<bool>>,
}

impl Trading {
    pub fn new(client: Arc<Mutex<Client>>) -> Self {
        Trading {
            client,
            positions: Vec::default(),
            orders: Vec::default(),
            is_alive: Arc::new(Mutex::new(false)),
        }
    }

    pub fn get_mktorders(&self) -> &Vec<mktorder::MktOrder> {
        return &self.orders;
    }

    pub fn get_mktpositions(&self) -> &Vec<mktposition::MktPosition> {
        return &self.positions;
    }

    pub async fn startup(&mut self) {
        self.orders = match self.get_orders().await {
            Ok(val) => val,
            Err(err) => panic!("{:?}", err),
        };
        self.positions = match self.get_positions().await {
            Ok(val) => val,
            Err(err) => panic!("{:?}", err),
        };
        match self.subscribe_to_stream().await {
            Err(err) => {
                error!("Failed to subscribe to stream, error: {err}");
                panic!("{:?}", err);
            }
            _ => (),
        };
    }

    async fn subscribe_to_stream(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if *self.is_alive.lock().unwrap() {
            return Ok(());
        }
        let (mut stream, mut _subscription) = self
            .client
            .lock()
            .unwrap()
            .subscribe::<updates::OrderUpdates>()
            .await?;

        *self.is_alive.lock().unwrap() = true;
        info!("Before the stream is off");

        let is_alive = self.is_alive.clone();
        tokio::spawn(async move {
            while *is_alive.lock().unwrap() {
                match stream
                    .by_ref()
                    .take_until(time::sleep(time::Duration::from_secs(30)))
                    .map_err(apca::Error::WebSocket)
                    .try_for_each(|result| async {
                        info!("Checking map home");
                        result
                            .map(|data| info!("Chris {data:?}"))
                            .map_err(apca::Error::Json)
                    })
                    .await
                {
                    Err(err) => error!("Error thrown in websocket {}", err),
                    _ => (),
                };
            }
            info!("Trading updates ended");
        });
        Ok(())
    }

    pub async fn create_position(
        &mut self,
        symbol: String,
        target_price: Num,
        position_size: Num,
        side: order::Side,
    ) -> Result<(), ()> {
        let request = order::OrderReqInit {
            type_: order::Type::StopLimit,
            limit_price: Some(target_price.clone()),
            stop_price: Some(target_price * Num::new((1.12 * DENOM) as i32, DENOM as i32)),
            ..Default::default()
        }
        .init(symbol, side, order::Amount::quantity(position_size));
        let mut retry = 5;
        loop {
            info!("Before posting the order");
            match self
                .client
                .lock()
                .unwrap()
                .issue::<order::Post>(&request)
                .await
            {
                Ok(val) => {
                    info!("Placed order {val:?}");
                    self.orders.push(mktorder::MktOrder::new(val));
                    return Ok(());
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

    pub async fn liquidate_position(&mut self, position: &position::Position) -> bool {
        let mut retry = 5;
        loop {
            info!("Before the liquidate position");
            match self
                .client
                .lock()
                .unwrap()
                .issue::<position::Delete>(&asset::Symbol::Sym(position.symbol.to_string()))
                .await
            {
                Ok(val) => {
                    info!("Placed order {:?}", val);
                    return true;
                }
                Err(err) => {
                    if retry == 0 {
                        error!("Failed to liquidate position");
                        break;
                    }
                    warn!("Retry liquidating position retries left: {retry}, err: {err:?}");
                }
                Err(err) => panic!("Unknown error: {err:?}"),
            }
            retry -= 1;
            thread::sleep(Duration::from_secs(1));
        }
        false
    }

    pub async fn cancel_order(&self, order: &mktorder::MktOrder) -> bool {
        let mut retry = 5;
        loop {
            info!("Before the liquidate position");
            match self
                .client
                .lock()
                .unwrap()
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
                Err(err) => panic!("Unknown error: {err:?}"),
            }
            retry -= 1;
            thread::sleep(Duration::from_secs(1));
        }
        false
    }

    async fn get_orders(
        &self,
    ) -> Result<Vec<mktorder::MktOrder>, apca::RequestError<orders::GetError>> {
        let mut retry = 5;
        loop {
            let request = orders::OrdersReq::default();
            match self
                .client
                .lock()
                .unwrap()
                .issue::<orders::Get>(&request)
                .await
            {
                Ok(val) => {
                    info!("Downloaded orders");
                    let mut orders = Vec::default();
                    for v in val {
                        info!("Order download {v:?}");
                        orders.push(mktorder::MktOrder::new(v));
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
                Err(err) => panic!("Unknown error: {err:?}"),
            }
            thread::sleep(Duration::from_secs(1));
        }
    }

    async fn get_positions(
        &self,
    ) -> Result<Vec<mktposition::MktPosition>, apca::RequestError<positions::GetError>> {
        let mut retry = 5;
        loop {
            match self
                .client
                .lock()
                .unwrap()
                .issue::<positions::Get>(&())
                .await
            {
                Ok(val) => {
                    let mut positions = Vec::default();
                    for v in val {
                        info!("Position download {v:?}");
                        positions.push(mktposition::MktPosition::new(v));
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
                Err(err) => panic!("Unknown error: {err:?}"),
            }
            thread::sleep(Duration::from_secs(1));
        }
    }
}
