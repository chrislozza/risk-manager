use apca::api::v2::{order, orders, position, positions, updates, asset};
use apca::Client;
use log::{error, info, warn};
use num_decimal::Num;
use std::{thread, time::Duration};

use futures::FutureExt as _;
use futures::StreamExt as _;
use futures::TryStreamExt as _;
use futures::stream::Map;


use crate::platform::mktorder;
use crate::platform::mktposition;

const DENOM: f32 = 100.00;

#[derive(Debug)]
pub struct Trading {
    positions: Vec<mktposition::MktPosition>,
    orders: Vec<mktorder::MktOrder>,
}

impl Trading {
    pub fn new() -> Self {
        Trading {
            positions: Vec::default(),
            orders: Vec::default(),
        }
    }

    pub fn get_mktorders(&self) -> &Vec<mktorder::MktOrder> {
        return &self.orders;
    }

    pub fn get_mktpositions(&self) -> &Vec<mktposition::MktPosition> {
        return &self.positions;
    }

    pub async fn startup(&mut self, client: &Client) {
        self.orders = match Self::get_orders(client).await {
            Ok(val) => val,
            Err(err) => panic!("{:?}", err),
        };
        self.positions = match Self::get_positions(client).await {
            Ok(val) => val,
            Err(err) => panic!("{:?}", err),
        };
        match self.subscribe_to_stream(client).await {
            Err(err) => {
                error!("Failed to subscribe to stream, error: {err}");
                panic!("{:?}", err);
            }
            _ => (),
        };
    }

    async fn subscribe_to_stream(&mut self, _client: &Client) -> Result<(), Box<dyn std::error::Error>> {
//        let (mut stream, mut _subscription) = client
//            .subscribe::<updates::OrderUpdates>().await?;
        //
        //        info!("Type is {:?}", Self::print_type(&stream));
        //
//        let () = match stream
//            .try_for_each(|result| async {
//                result
//                    .map(|data| println!("{data:?}"))
//                    .map_err(apca::Error::Json).unwrap()
//            })
//        .await.unwrap().unwrap() {
//            Err(err) => panic!("Panic {:?}", err);
//            _ => () 
//
//        };


        //        let update = stream
        //            .try_filter_map(|result| {
        //                let update = result.unwrap();
        //                Ok(Some(update))
        //            })
        //            .await
        //            .unwrap()
        //            .unwrap();
        Ok(())
    }

    pub async fn create_position(
        &mut self,
        client: &Client,
        symbol: String,
        target_price: Num,
        position_size: Num,
        side: order::Side,
    ) -> bool {
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
            match client.issue::<order::Post>(&request).await {
                Ok(val) => {
                    info!("Placed order {val:?}");
                    self.orders.push(mktorder::MktOrder::new(val));
                    return true
                },
                Err(apca::RequestError::Endpoint(order::PostError::NotPermitted(err))) => {
                    if retry == 0 {
                        error!("Failed to post order");
                        break;
                    }
                    warn!("Retry order posting retries left: {retry}, err: {err:?}");
                },
                Err(err) => panic!("Unknown error: {err:?}")
            }
            retry -= 1;
            thread::sleep(Duration::from_secs(1));
        }
        false
    }

    pub async fn liquidate_position(&mut self, client: &Client, position: &position::Position) -> bool {
        let mut retry = 5;
        loop {
            info!("Before the liquidate position");
            match client.issue::<position::Delete>(&asset::Symbol::Sym(position.symbol.to_string())).await {
                Ok(val) => {
                    info!("Placed order {:?}", val);
                    return true
                },
                Err(err) => {
                    if retry == 0 {
                        error!("Failed to liquidate position");
                        break;
                    }
                    warn!("Retry liquidating position retries left: {retry}, err: {err:?}");
                },
                Err(err) => panic!("Unknown error: {err:?}")
            }
            retry -= 1;
            thread::sleep(Duration::from_secs(1));
        }
        false
    }

    pub async fn cancel_position(&self, client: &Client, order: &order::Order) -> bool {
        let mut retry = 5;
        loop {
            info!("Before the liquidate position");
            match client.issue::<order::Delete>(&order.id).await {
                Ok(val) => {
                    info!("Placed order {:?}", val);
                    return true
                },
                Err(err) => {
                    if retry == 0 {
                        error!("Failed to post order, not permitted");
                        break;
                    }
                    warn!("Retry order cancelling retries left: {retry}, err: {err:?}");
                },
                Err(err) => panic!("Unknown error: {err:?}")
            }
            retry -= 1;
            thread::sleep(Duration::from_secs(1));
        }
        false
    }

    async fn get_orders(client: &Client) -> Result<Vec<mktorder::MktOrder>, apca::RequestError<orders::GetError>> {
        let mut retry = 5;
        loop {
            let request = orders::OrdersReq::default();
            match client.issue::<orders::Get>(&request).await {
                Ok(val) => {
                    info!("Downloaded orders");
                    let mut orders = Vec::default();
                    for v in val {
                        info!("Order download {v:?}");
                        orders.push(mktorder::MktOrder::new(v));
                    }
                    return Ok(orders);
                },
                Err(err) => {
                    retry -= 1;
                    if retry == 0 {
                        error!("Failed to retrieve orders {}", err);
                        return Err(err);
                    }
                },
                Err(err) => panic!("Unknown error: {err:?}")
            }
            thread::sleep(Duration::from_secs(1));
        }
    }

    async fn get_positions(client: &Client) -> Result<Vec<mktposition::MktPosition>, apca::RequestError<positions::GetError>> {
        let mut retry = 5;
        loop {
            match client.issue::<positions::Get>(&()).await {
                Ok(val) => {
                    let mut positions = Vec::default();
                    for v in val {
                        info!("Position download {v:?}");
                        positions.push(mktposition::MktPosition::new(v));
                    }
                    return Ok(positions);
                },
                Err(err) => {
                    retry -= 1;
                    if retry == 0 {
                        error!("Failed to retrieve positions {}", err);
                        return Err(err);
                    }
                },
                Err(err) => panic!("Unknown error: {err:?}")
            }
            thread::sleep(Duration::from_secs(1));
        }
    }
}
