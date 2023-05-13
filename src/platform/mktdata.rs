use apca::data::v2::{bars, stream, trades};
use apca::Client;
use log::{error, info};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::vec::Vec;

use futures::FutureExt as _;
use futures::StreamExt as _;
use futures::TryStreamExt as _;

use tokio::time;

use super::mktorder;
use super::mktposition;

#[derive(Clone)]
pub struct MktData {
    client: Arc<Mutex<Client>>,
    symbols: Vec<String>,
    is_alive: Arc<Mutex<bool>>,
    callbacks: Vec<fn(stream::Trade)>,
    subscription: Option<Arc<Mutex<StreamHandler>>>,
}

impl MktData {
    pub fn new(client: Arc<Mutex<Client>>) -> Self {
        MktData {
            client,
            symbols: Vec::<String>::default(),
            is_alive: Arc::new(Mutex::new(false)),
            callbacks: Vec::<fn(stream::Trade)>::default(),
            subscription: None
        }
    }

    fn create_callback(&mut self) -> Arc<Mutex<dyn FnOnce(stream::Trade) + Send>>
    {
        let callback = Arc::new(Mutex::new(|trade: stream::Trade| { self.callback(trade); }));
        return callback
    }

    pub async fn startup(
        &mut self,
        orders: &Vec<mktorder::MktOrder>,
        positions: &Vec<mktposition::MktPosition>,
    ) {
        for mktorder in orders.iter() {
            self.symbols.push(mktorder.get_order().symbol.clone());
        }

        for mktposition in positions.iter() {
            self.symbols.push(mktposition.get_position().symbol.clone());
        }

        if self.symbols.len() == 0 {
            info!("No symbols to subscribe to");
            return;
        }

        match self.subscription.as_ref() {
            Some(_) => (),
            None => {
                let callback = self.create_callback(); 
                let subscription = Arc::new(Mutex::new(StreamHandler::new(self.client.clone(), callback)));
                self.subscription = Some(subscription);
            }
        };

        match self
            .subscription.as_mut().unwrap().lock().unwrap()
            .subscribe_to_stream(self.symbols.clone().into())
            .await
        {
            Err(err) => {
                error!("Failed to subscribe to stream, error: {err}");
                panic!("{:?}", err);
            }
            _ => (),
        };
    }

    pub async fn subscribe(&mut self, symbol: String) {
            self.subscription.as_mut().unwrap().lock().unwrap()
            .subscribe_to_stream(vec![symbol.clone()].into())
            .await;
        self.symbols.push(symbol);
    }

    pub fn unsubscribe(&mut self, symbol: String) {
            self.subscription.as_mut().unwrap().lock().unwrap()
            .unsubscribe_from_stream(vec![symbol.clone()].into());
        self.symbols.retain(|element| {
            let is_match = element.eq(&symbol);
            !is_match
        });
    }

    pub async fn unsubscribe_all(&mut self) {
            self.subscription.as_mut().unwrap().lock().unwrap()
            .unsubscribe_from_stream(self.symbols.clone().into())
            .await;
    }

    fn callback(&self, trade: stream::Trade) {
        info!("Trade {trade:?}");
    }

}
#[derive(Clone)]
struct StreamHandler {
    client: Arc<Mutex<Client>>,
    callback: Arc<Mutex<dyn FnOnce(stream::Trade) + Send> >,
}

impl StreamHandler {
    fn new(client: Arc<Mutex<Client>>, callback: Arc<Mutex<dyn FnOnce(stream::Trade) + Send>>) -> Self {
        StreamHandler { client, callback }
    }

    pub fn call(&mut self, trade: stream::Trade) {
        (self.callback.lock().unwrap())(trade)
    }

    async fn subscribe_to_stream(
        &self,
        _: stream::SymbolList,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (mut stream, mut subscription) = self
            .client
            .lock()
            .unwrap()
            .subscribe::<stream::RealtimeData<stream::IEX>>()
            .await?;
        let data = stream::MarketData {
            trades: stream::Symbols::List(stream::SymbolList::from(vec!["MSFT".to_string()])),
            bars: stream::Symbols::List(stream::SymbolList::from(vec!["MSFT".to_string()])),
            ..Default::default()
        };
        let subscribe = subscription.subscribe(&data).boxed_local().fuse();
        // Actually subscribe with the websocket server.
        info!("Before subscribe");
        let error = stream::drive(subscribe, &mut stream)
            .await
            .unwrap()
            .unwrap();

        match error {
            Err(apca::Error::Str(ref e)) if e == "failed to subscribe: invalid syntax (400)" => {}
            Err(e) => panic!("received unexpected error: {e:?}"),
            _ => info!("Subcribed to mktdata trades"),
        }

        let callback = Arc::new(Mutex::new(self.clone()));
        tokio::spawn(async move {
            //        let mut data = stream::MarketData::default();
            //        data.set_bars(symbols.clone());
            //        data.set_trades(symbols);
            match stream
                .by_ref()
                .take_until(time::sleep(time::Duration::from_secs(30)))
                .map_err(apca::Error::WebSocket)
                .try_for_each(|result| async {
                    info!("Checking map home");
                    result
                        .map(|data| match data {
                            stream::Data::Trade(val) => {
                                callback.lock().unwrap().call(val);
                            }
                            _ => info!("Unknown"),
                        })
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

    async fn unsubscribe_from_stream(
        &mut self,
        symbols: stream::SymbolList,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (mut stream, mut subscription) = self
            .client
            .lock()
            .unwrap()
            .subscribe::<stream::RealtimeData<stream::IEX>>()
            .await?;

        let mut data = stream::MarketData::default();
        data.set_bars(symbols.clone());
        data.set_trades(symbols);

        let unsubscribe = subscription.unsubscribe(&data).boxed_local().fuse();
        let () = stream::drive(unsubscribe, &mut stream)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        Ok(())
    }
}
