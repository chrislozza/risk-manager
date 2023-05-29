use apca::data::v2::stream;
use apca::Client;
use log::{error, info};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;

use super::mktorder;
use super::mktposition;
use super::stream_handler::StreamHandler;
use super::Event;

pub struct MktData {
    client: Arc<Mutex<Client>>,
    symbols: stream::Symbols,
    is_alive: Arc<Mutex<bool>>,
    stream_handler: StreamHandler,
    receiver: broadcast::Receiver<Event>,
    sender: broadcast::Sender<Event>,
}

impl MktData {
    pub fn new(client: Arc<Mutex<Client>>) -> Self {
        let (sender, receiver) = broadcast::channel(100);
        let stream_handler = StreamHandler::new(Arc::clone(&client), sender.clone());
        MktData {
            client,
            symbols: stream::Symbols::default(),
            is_alive: Arc::new(Mutex::new(false)),
            stream_handler,
            receiver,
            sender,
        }
    }

    fn build_symbol_list(
        &self,
        orders: &HashMap<String, mktorder::MktOrder>,
        positions: &HashMap<String, mktposition::MktPosition>,
    ) -> Vec<String> {
        let mut symbols = Vec::<String>::default();
        for mktorder in orders.values() {
            symbols.push(mktorder.get_order().symbol.clone());
        }

        for mktposition in positions.values() {
            symbols.push(mktposition.get_position().symbol.clone());
        }
        return symbols;
    }

    pub fn stream_reader(&self) -> broadcast::Receiver<Event> {
        self.sender.subscribe()
    }

    pub async fn startup(
        &mut self,
        orders: &HashMap<String, mktorder::MktOrder>,
        positions: &HashMap<String, mktposition::MktPosition>,
    ) {
        let symbols = self.build_symbol_list(orders, positions);
        if symbols.len() == 0 {
            info!("No symbols to subscribe to");
            return;
        }

        let symbols = match self
            .stream_handler
            .subscribe_to_mktdata(symbols.into())
            .await
        {
            Err(err) => {
                error!("Failed to subscribe to stream, error: {err:?}");
                panic!("{:?}", err);
            }
            Ok(val) => val,
        };
        self.symbols = symbols;
    }

    pub async fn shutdown(&self) {
        info!("Shutdown initiated");
    }

    pub async fn subscribe(&mut self, symbol: String) {
        let symbols = match self
            .stream_handler
            .subscribe_to_mktdata(vec![symbol.clone()].into())
            .await
        {
            Err(val) => {
                error!("Failed to subscribe {val:?}");
                return;
            }
            Ok(val) => val,
        };
        self.symbols = symbols;
    }

    pub async fn unsubscribe(&mut self, symbol: String) {
        let symbols = match self
            .stream_handler
            .unsubscribe_from_stream(vec![symbol.clone()].into())
            .await
        {
            Err(val) => {
                error!("Failed to unsubscribe {val:?}");
                return;
            }
            Ok(val) => val,
        };
        self.symbols = symbols;
    }

    pub async fn unsubscribe_all(&mut self) {
        let symbol_list = match self.symbols.clone() {
            stream::Symbols::List(val) => val,
            _ => stream::SymbolList::default(),
        };
        if symbol_list.len() == 0 {
            return;
        }
        let symbols = match self
            .stream_handler
            .unsubscribe_from_stream(symbol_list)
            .await
        {
            Err(val) => {
                error!("Failed to unsubscribe {val:?}");
                return;
            }
            Ok(val) => val,
        };
        self.symbols = symbols;
    }
}
//#[derive(Clone)]
//struct StreamHandler {
//    client: Arc<Mutex<Client>>,
//    callbacks: Vec<Arc<Mutex<dyn Fn(stream::Trade) + Send + Sync>>>,
//}
//
//impl StreamHandler {
//    fn new(
//        client: Arc<Mutex<Client>>,
//        callbacks: Vec<Arc<Mutex<dyn Fn(stream::Trade) + Send + Sync>>>,
//    ) -> Self {
//        StreamHandler { client, callbacks }
//    }
//
//    pub fn call(&mut self, trade: stream::Trade) {
//        for callback in &self.callbacks {
//            (callback.lock().unwrap())(trade.clone())
//        }
//    }
//
//    async fn subscribe_to_stream(
//        &self,
//        symbols: stream::SymbolList,
//    ) -> Result<stream::Symbols, Box<dyn std::error::Error>> {
//        let (mut stream, mut subscription) = self
//            .client
//            .lock()
//            .unwrap()
//            .subscribe::<stream::RealtimeData<stream::IEX>>()
//            .await?;
//        let mut data = stream::MarketData::default();
//        data.set_trades(symbols);
//        let subscribe = subscription.subscribe(&data).boxed_local().fuse();
//        // Actually subscribe with the websocket server.
//        info!("Before subscribe");
//        let error = stream::drive(subscribe, &mut stream)
//            .await
//            .unwrap()
//            .unwrap();
//
//        match error {
//            Err(apca::Error::Str(ref e)) if e == "failed to subscribe: invalid syntax (400)" => {}
//            Err(e) => panic!("received unexpected error: {e:?}"),
//            _ => info!("Subcribed to mktdata trades"),
//        }
//
//        let callback = Arc::new(Mutex::new(self.clone()));
//        tokio::spawn(async move {
//            match stream
//                .by_ref()
//                .take_until(time::sleep(time::Duration::from_secs(30)))
//                .map_err(apca::Error::WebSocket)
//                .try_for_each(|result| async {
//                    info!("Checking map home");
//                    result
//                        .map(|data| match data {
//                            stream::Data::Trade(val) => {
//                                callback.lock().unwrap().call(val);
//                            }
//                            _ => info!("Unknown"),
//                        })
//                        .map_err(apca::Error::Json)
//                })
//                .await
//            {
//                Err(err) => error!("Error thrown in websocket {}", err),
//                _ => (),
//            };
//        });
//        Ok(subscription.subscriptions().trades.clone())
//    }
//
//    async fn unsubscribe_from_stream(
//        &self,
//        symbols: stream::SymbolList,
//    ) -> Result<stream::Symbols, Box<dyn std::error::Error>> {
//        let (mut stream, mut subscription) = self
//            .client
//            .lock()
//            .unwrap()
//            .subscribe::<stream::RealtimeData<stream::IEX>>()
//            .await?;
//
//        let mut data = stream::MarketData::default();
//        data.set_bars(symbols.clone());
//        data.set_trades(symbols);
//
//        let unsubscribe = subscription.unsubscribe(&data).boxed_local().fuse();
//        let () = stream::drive(unsubscribe, &mut stream)
//            .await
//            .unwrap()
//            .unwrap()
//            .unwrap();
//
//        let callback = Arc::new(Mutex::new(self.clone()));
//        tokio::spawn(async move {
//            match stream
//                .by_ref()
//                .take_until(time::sleep(time::Duration::from_secs(30)))
//                .map_err(apca::Error::WebSocket)
//                .try_for_each(|result| async {
//                    info!("Checking map home");
//                    result
//                        .map(|data| match data {
//                            stream::Data::Trade(val) => {
//                                callback.lock().unwrap().call(val);
//                            }
//                            _ => info!("Unknown"),
//                        })
//                        .map_err(apca::Error::Json)
//                })
//                .await
//            {
//                Err(err) => error!("Error thrown in websocket {}", err),
//                _ => (),
//            };
//        });
//        Ok(subscription.subscriptions().trades.clone())
//    }
