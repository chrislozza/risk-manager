use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use apca::api::v2::updates;
use apca::data::v2::stream;
use apca::Client;

use log::{error, info};

use tokio::sync::broadcast;
use tokio::time;

use futures::FutureExt as _;
use futures::StreamExt as _;
use futures::TryStreamExt as _;

#[derive(Debug, Clone)]
pub enum Event {
    Trade(stream::Trade),
    OrderUpdate(updates::OrderUpdate),
}

pub struct StreamHandler {
    client: Arc<Mutex<Client>>,
    subscriber: broadcast::Sender<Event>,
    is_alive: Arc<Mutex<bool>>,
}

impl Clone for StreamHandler {
    fn clone(&self) -> Self {
        StreamHandler {
            client: Arc::clone(&self.client),
            subscriber: self.subscriber.clone(),
            is_alive: Arc::clone(&self.is_alive),
        }
    }
    fn clone_from(&mut self, source: &Self) {
        *self = source.clone()
    }
}

impl StreamHandler {
    pub fn new(client: Arc<Mutex<Client>>, subscriber: broadcast::Sender<Event>) -> Self {
        StreamHandler {
            client,
            subscriber,
            is_alive: Arc::new(Mutex::new(false)),
        }
    }

    pub async fn subscribe_to_order_updates(&self) -> Result<(), ()> {
        let (mut stream, subscription) = self
            .client
            .lock()
            .unwrap()
            .subscribe::<updates::OrderUpdates>()
            .await
            .unwrap();

        info!("Before subscribe");

        let subscriber = self.subscriber.clone();
        let is_alive = Arc::clone(&self.is_alive);
        tokio::spawn(async move {
            while *is_alive.lock().unwrap() {
                match stream
                    .by_ref()
                    .take_until(time::sleep(time::Duration::from_secs(30)))
                    .map_err(apca::Error::WebSocket)
                    .try_for_each(|result| async {
                        info!("Checking map home");
                        result
                            .map(|data| {
                                let event = match data {
                                    updates::OrderUpdate { event, order } => {
                                        Event::OrderUpdate(updates::OrderUpdate { event, order })
                                    }
                                    _ => {
                                        error!("Unknown error");
                                        return ();
                                    }
                                };
                                match subscriber.send(event) {
                                    Err(val) => {
                                        error!("Sending error {val:?}");
                                        return ();
                                    }
                                    Err(broadcast::error::SendError(data)) => error!("{data:?}"),
                                    Ok(_) => (),
                                };
                            })
                            .map_err(apca::Error::Json)
                    })
                    .await
                {
                    Err(err) => error!("Error thrown in websocket {}", err),
                    _ => return (),
                };
            }
            info!("Trading updates ended");
        });
        Ok(())
    }

    pub async fn subscribe_to_mktdata(
        &mut self,
        symbols: stream::SymbolList,
    ) -> Result<stream::Symbols, ()> {
        let (mut stream, mut subscription) = self
            .client
            .lock()
            .unwrap()
            .subscribe::<stream::RealtimeData<stream::IEX>>()
            .await
            .unwrap();
        let mut data = stream::MarketData::default();
        data.set_trades(symbols);
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

        let subscriber = self.subscriber.clone();
        let is_alive = Arc::clone(&self.is_alive);
        tokio::spawn(async move {
            while *is_alive.lock().unwrap() {
                match stream
                    .by_ref()
                    .take_until(time::sleep(time::Duration::from_secs(30)))
                    .map_err(apca::Error::WebSocket)
                    .try_for_each(|result| async {
                        info!("Checking map home");
                        result
                            .map(|data| {
                                let event = match data {
                                    stream::Data::Trade(data) => Event::Trade(data),
                                    _ => {
                                        error!("Unknown error");
                                        return ();
                                    }
                                };
                                match subscriber.send(event) {
                                    Err(val) => {
                                        error!("Sending error {val:?}");
                                        return ();
                                    }
                                    Err(broadcast::error::SendError(data)) => error!("{data:?}"),
                                    Ok(_) => (),
                                };
                            })
                            .map_err(apca::Error::Json)
                    })
                    .await
                {
                    Err(err) => {
                        error!("Error thrown in websocket {err:?}");
                        return ();
                    }
                    _ => (),
                };
            }
        });
        Ok(subscription.subscriptions().trades.clone())
    }

    pub async fn unsubscribe_from_stream(
        &self,
        symbols: stream::SymbolList,
    ) -> Result<stream::Symbols, ()> {
        let (mut stream, mut subscription) = self
            .client
            .lock()
            .unwrap()
            .subscribe::<stream::RealtimeData<stream::IEX>>()
            .await
            .unwrap();

        let mut data = stream::MarketData::default();
        data.set_bars(symbols.clone());
        data.set_trades(symbols);

        let unsubscribe = subscription.unsubscribe(&data).boxed_local().fuse();
        let () = stream::drive(unsubscribe, &mut stream)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        let subscriber = self.subscriber.clone();
        tokio::spawn(async move {
            match stream
                .by_ref()
                .take_until(time::sleep(time::Duration::from_secs(30)))
                .map_err(apca::Error::WebSocket)
                .try_for_each(|result| async {
                    info!("Checking map home");
                    result
                        .map(|data| {
                            let event = match data {
                                stream::Data::Trade(data) => Event::Trade(data),
                                data => {
                                    error!("Got data type {data:?}");
                                    return ();
                                }
                            };
                            match subscriber.send(event) {
                                Err(broadcast::error::SendError(val)) => {
                                    error!("Sending error {val:?}");
                                    return ();
                                }
                                Ok(_) => (),
                            };
                        })
                        .map_err(apca::Error::Json)
                })
                .await
            {
                Err(err) => error!("Error thrown in websocket {}", err),
                _ => (),
            };
        });
        Ok(subscription.subscriptions().trades.clone())
    }
}
