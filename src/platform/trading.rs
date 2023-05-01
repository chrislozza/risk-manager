use apca::api::v2::{order, orders, position, positions, updates, asset};
use apca::Client;
use log::{error, info};
use num_decimal::Num;
use std::{thread, time::Duration};

use futures::FutureExt as _;
use futures::StreamExt as _;
use futures::TryStreamExt as _;
use futures::stream::Map;

use std::any::type_name;

const DENOM: f32 = 100.00;

#[derive(Debug)]
pub struct Trading {
    positions: Vec<position::Position>,
    orders: Vec<order::Order>,
}

impl Trading {
    pub fn new() -> Self {
        Trading {
            positions: Vec::default(),
            orders: Vec::default(),
        }
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

    async fn subscribe_to_stream(
        &mut self,
        _client: &Client,
        ) -> Result<(), Box<dyn std::error::Error>> {
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

    fn print_type<T>(_:&T) -> &'static str {
        type_name::<T>()
    }

    pub async fn create_position(
        &mut self,
        client: &Client,
        symbol: String,
        target_price: f32,
        position_size: u16,
        side: order::Side,
    ) -> Result<(), apca::RequestError<order::PostError>> {
        let request = order::OrderReqInit {
            type_: order::Type::StopLimit,
            limit_price: Some(Num::new((target_price * DENOM) as i32, DENOM as i32)),
            stop_price: Some(Num::new((target_price * 1.12 * DENOM) as i32, DENOM as i32)),
            ..Default::default()
        }
        .init(symbol, side, order::Amount::quantity(position_size));
        let mut retry = 5;
        loop {
            info!("Before posting the order");
            match client.issue::<order::Post>(&request).await {
                Err(err) => {
                    if retry == 0 {
                        error!("Failed to post order, not permitted");
                        return Err(err);
                    }
                    info!("Retry order posting retries left: {retry}, err: {err}");
                }
                Ok(val) => {
                    info!("Placed order {:?}", val);
                    self.orders.push(val);
                    break
                }
            }
            retry -= 1;
            thread::sleep(Duration::from_secs(1));
        }
        Ok(())
    }

    pub async fn liquidate_position(&mut self, client: &Client, position: &position::Position) -> Result<(), apca::RequestError<position::DeleteError>> {
        let mut retry = 5;
        loop {
            info!("Before the liquidate position");
            match client.issue::<position::Delete>(&asset::Symbol::Sym(position.symbol.to_string())).await {
                Err(err) => {
                    if retry == 0 {
                        error!("Failed to post order, not permitted");
                        return Err(err);
                    }
                    info!("Retry order posting retries left: {retry}, err: {err}");
                }
                Ok(val) => {
                    info!("Placed order {:?}", val);
                    break
                }
            }
            retry -= 1;
            thread::sleep(Duration::from_secs(1));
        }
        Ok(())
    }

    pub async fn cancel_position(&self, client: &Client, order: &order::Order) -> Result<(), apca::RequestError<order::DeleteError>> {
        let mut retry = 5;
        loop {
            info!("Before the liquidate position");
            match client.issue::<order::Delete>(&order.id).await {
                Err(err) => {
                    if retry == 0 {
                        error!("Failed to post order, not permitted");
                        return Err(err);
                    }
                    info!("Retry order posting retries left: {retry}, err: {err}");
                }
                Ok(val) => {
                    info!("Placed order {:?}", val);
                    break
                }
            }
            retry -= 1;
            thread::sleep(Duration::from_secs(1));
        }
        Ok(())
    }

    async fn get_orders(
        client: &Client,
    ) -> Result<Vec<order::Order>, apca::RequestError<orders::GetError>> {
        let mut retry = 5;
        loop {
            let request = orders::OrdersReq::default();
            match client.issue::<orders::Get>(&request).await {
                Ok(val) => {
                    info!("Downloaded ");
                    return Ok(val);
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

    async fn get_positions(
        client: &Client,
    ) -> Result<Vec<position::Position>, apca::RequestError<positions::GetError>> {
        loop {
            let mut retry = 5;
            match client.issue::<positions::Get>(&()).await {
                Ok(val) => {
                    info!("Downloaded ");
                    return Ok(val);
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
