use apca::api::v2::updates;
use apca::data::v2::stream;
use apca::Client;

use log::{error, info};

use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::Mutex;
use tokio::time;

use futures::FutureExt;
use futures::StreamExt;
use futures::TryStreamExt;

use super::Event;

use anyhow::Result;

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
            is_alive: Arc::new(Mutex::new(true)),
        }
    }

    //    async fn run_stream<Strm, Sub>(
    //        mut stream: Strm,
    //        subscription: Sub,
    //        subscriber: broadcast::Sender<Event>,
    //        )
    //        -> Result<()>
    //        where
    //            Strm: StreamExt + TryStreamExt + FutureExt
    //        {
    //            tokio::spawn(async move {
    //                info!("In task listening for order updates");
    //                let mut retries = 3;
    //                loop {
    //                    match stream
    //                        .take_until(time::sleep(time::Duration::from_secs(30)))
    //                        .map_err(apca::Error::WebSocket)
    //                        .try_for_each(|result| async {
    //                            info!("Order Updates {result:?}");
    //                            result
    //                                .map(|data| {
    //                                    let event = match data {
    //                                        updates::OrderUpdate { event, order } => {
    //                                            Event::OrderUpdate(updates::OrderUpdate { event, order })
    //                                        }
    //                                    };
    //                                    if let Err(broadcast::error::SendError(val)) = subscriber.send(event) {
    //                                        error!("Sending error {val:?}");
    //                                    }
    //                                })
    //                            .map_err(apca::Error::Json)
    //                        })
    //                    .await
    //                    {
    //                        Err(apca::Error::WebSocket(err)) => {
    //                            error!("Error thrown in websocket {err:?}");
    //                            if stream.is_done() {
    //                                error!("websocket is done, should restart?");
    //                            }
    //                            if retries == 0 {
    //                                anyhow::bail!("Websocket retries used up, restarting app");
    //                            }
    //
    //                            retries -= 1;
    //                        }
    //                        Err(err) => {
    //                            error!("Error thrown in websocket {err:?}");
    //                            retries -= 1;
    //                        }
    //                        _ => ()
    //                    };
    //                }
    //                info!("Trading updates ended");
    //            });
    //            Ok(())
    //        }

    pub async fn subscribe_to_order_updates(&self) -> Result<(), ()> {
        let (mut stream, _subscription) = self
            .client
            .lock()
            .await
            .subscribe::<updates::OrderUpdates>()
            .await
            .unwrap();

        info!("Before subscribe");

        let subscriber = self.subscriber.clone();
        let is_alive = Arc::clone(&self.is_alive);
        tokio::spawn(async move {
            info!("In task listening for order updates");
            while *is_alive.lock().await {
                match stream
                    .by_ref()
                    .take_until(time::sleep(time::Duration::from_secs(30)))
                    .map_err(apca::Error::WebSocket)
                    .try_for_each(|result| async {
                        info!("Order Updates {result:?}");
                        result
                            .map(|data| {
                                let updates::OrderUpdate { event, order } = data;
                                let event =
                                    Event::OrderUpdate(updates::OrderUpdate { event, order });
                                if let Err(broadcast::error::SendError(val)) =
                                    subscriber.send(event)
                                {
                                    error!("Sending error {val:?}");
                                }
                            })
                            .map_err(apca::Error::Json)
                    })
                    .await
                {
                    Err(err) => error!("Error thrown in websocket {}", err),
                    _ => return,
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
            .await
            .subscribe::<stream::RealtimeData<stream::IEX>>()
            .await
            .unwrap();
        let mut data = stream::MarketData::default();
        data.set_trades(symbols);
        let subscribe = subscription.subscribe(&data).boxed_local().fuse();
        // Actually subscribe with the websocket server.

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
            info!("In task listening for mktdata updates");
            while *is_alive.lock().await {
                match stream
                    .by_ref()
                    .take_until(time::sleep(time::Duration::from_secs(30)))
                    .map_err(apca::Error::WebSocket)
                    .try_for_each(|result| async {
                        result
                            .map(|data| {
                                let event = match data {
                                    stream::Data::Trade(data) => Event::Trade(data),
                                    _ => {
                                        error!("Unknown error");
                                        return;
                                    }
                                };
                                match subscriber.send(event) {
                                    Err(broadcast::error::SendError(data)) => error!("{data:?}"),
                                    Ok(_) => (),
                                }
                            })
                            .map_err(apca::Error::Json)
                    })
                    .await
                {
                    Err(apca::Error::WebSocket(err)) => {
                        error!("Error thrown in websocket {err:?}");
                        if stream.is_done() {
                            error!("websocket is done, should restart?");
                        }
                    }
                    Err(err) => {
                        error!("Error thrown in websocket {err:?}");
                        return;
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
            .await
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
            if let Err(err) = stream
                .by_ref()
                .take_until(time::sleep(time::Duration::from_secs(30)))
                .map_err(apca::Error::WebSocket)
                .try_for_each(|result| async {
                    result
                        .map(|data| {
                            let event = match data {
                                stream::Data::Trade(data) => Event::Trade(data),
                                data => {
                                    error!("Got data type {data:?}");
                                    return;
                                }
                            };

                            if let Err(broadcast::error::SendError(val)) = subscriber.send(event) {
                                error!("Sending error {val:?}");
                            }
                        })
                        .map_err(apca::Error::Json)
                })
                .await
            {
                error!("Error thrown in websocket {}", err)
            };
        });
        Ok(subscription.subscriptions().trades.clone())
    }
}
