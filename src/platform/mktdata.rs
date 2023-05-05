use apca::data::v2::{bars, quotes, stream, trades};
use apca::Client;
use log::{error, info};
use std::vec::Vec;
use std::sync::atomic;

use futures::FutureExt as _;
use futures::StreamExt as _;
use futures::TryStreamExt as _;

use crate::platform::mktorder;
use crate::platform::mktposition;


#[derive(Debug)]
pub struct MktData {
    symbols: Vec<String>,
    is_alive: atomic::AtomicBool
}

impl MktData {
    pub fn new() -> Self {
        MktData {
            symbols: Vec::<String>::default(),
            is_alive: atomic::AtomicBool::new(false),
        }
    }

    pub async fn startup(&mut self, client: &Client, orders: &Vec<mktorder::MktOrder>, positions: &Vec<mktposition::MktPosition>) {
        for mktorder in orders.iter() {
            self.symbols.push(mktorder.get_symbol().clone());
        }

        for mktposition in positions.iter() {
            self.symbols.push(mktposition.get_symbol().clone());
        }
        
        if self.symbols.len() == 0 {
            info!("No symbols to subscribe to");
        }

        match self.subscribe_to_stream(client, self.symbols.clone().into()).await {
            Err(err) => {
                error!("Failed to subscribe to stream, error: {err}");
                panic!("{:?}", err);
            }
            _ => (),
        };
    }

    pub fn subscribe(&mut self, symbol: String) {
        self.symbols.push(symbol);
    }

    pub fn unsubscribe(&mut self, symbol: String) {
//        self.unsubscribe_from_stream(vec!(symbol).into());
        self.symbols.retain(|element| {
            let is_match = element.eq(&symbol);
            !is_match
        });
    }

    pub fn unsubscribe_all(&mut self) {
    }

//    async fn unsubscribe_from_stream(
//        &mut self,
//        client: &Client,
//        symbols: stream::SymbolList,
//    ) -> Result<(), Box<dyn std::error::Error>> {
//
//        if !self.is_alive.load(atomic::Ordering::Acquire) {
//            return Ok(())
//        }
//        let mut data = stream::MarketData::default();
//        data.set_bars(symbols.clone());
//        data.set_trades(symbols);
//
//        Ok(())
//    }

    async fn subscribe_to_stream(
        &mut self,
        client: &Client,
        symbols: stream::SymbolList,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (mut stream, mut subscription) = client
            .subscribe::<stream::RealtimeData<stream::IEX>>().await?;
        let mut data = stream::MarketData::default();
        data.set_bars(symbols.clone());
        data.set_trades(symbols);

        let subscribe = subscription.subscribe(&data).boxed();
        // Actually subscribe with the websocket server.
        let () = stream::drive(subscribe, &mut stream)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        if self.is_alive.load(atomic::Ordering::Acquire) {
            return Ok(())
        }
        self.is_alive.store(true, atomic::Ordering::Release);
        info!("Before the stream is off");
        tokio::spawn(async move {
            let () = match stream
                .take(50)
                .map_err(apca::Error::WebSocket)
                .try_for_each(|result| async {
                    result
                        .map(|data| println!("{data:?}"))
                        .map_err(apca::Error::Json)
                })
                .await
            {
                Err(err) => error!("Error thrown in websocket {}", err),
                _ => (),
            };
        });
        Ok(())
    }

    //    async fn request_account_details() -> Self {
    //        //        BarsReqInit::init
    //        //        let request = Request::<'a>::Authenticate { client.api_info().api_key, client.api_info().secret };
    //        //
    //        //        Client::issue<Get>(&()).await.unwrap();
    //
    //    }
}
