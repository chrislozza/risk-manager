use apca::api::v2::updates;
use apca::data::v2::stream;
use apca::Client;

use tracing::{error, info, warn};

use anyhow::{bail, Result};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::Mutex;

use tokio_util::sync::CancellationToken;

use futures::FutureExt as _;
use futures::StreamExt as _;
use futures::TryStreamExt as _;

use super::Event;

pub struct WebSocket {
    client: Arc<Mutex<Client>>,
    event_publisher: broadcast::Sender<Event>,
    shutdown_signal: CancellationToken,
}

impl Clone for WebSocket {
    fn clone(&self) -> Self {
        WebSocket {
            client: Arc::clone(&self.client),
            event_publisher: self.event_publisher.clone(),
            shutdown_signal: self.shutdown_signal.clone(),
        }
    }
    fn clone_from(&mut self, source: &Self) {
        *self = source.clone()
    }
}

impl WebSocket {
    pub fn new(
        client: Arc<Mutex<Client>>,
        event_publisher: broadcast::Sender<Event>,
        shutdown_signal: CancellationToken,
    ) -> Self {
        WebSocket {
            client,
            event_publisher,
            shutdown_signal,
        }
    }

    pub async fn subscribe_to_order_updates(&self) -> Result<()> {
        let (mut stream, _subscription) = self
            .client
            .lock()
            .await
            .subscribe::<updates::OrderUpdates>()
            .await
            .unwrap();

        let subscriber = self.event_publisher.clone();
        let shutdown_signal = self.shutdown_signal.clone();
        tokio::spawn(async move {
            info!("In task listening for order updates");
            let mut retries = 5;
            loop {
                match stream
                    .by_ref()
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
                    Err(apca::Error::WebSocket(err)) => {
                        error!("Error thrown in websocket {err:?}");
                    }
                    Err(err) => {
                        error!("Error thrown in websocket {err:?}");
                    }
                    _ => {
                        retries = 5;
                    }
                };
                if stream.is_done() {
                    error!("websocket is done, should restart?");
                }
                retries -= 1;
                warn!("Number of retries left in order updates {retries}");
                if retries == 0 {
                    shutdown_signal.cancel();
                    break;
                }
            }
            info!("Trading updates ended");
        });
        Ok(())
    }

    pub async fn subscribe_to_mktdata(
        &mut self,
        symbols: stream::SymbolList,
    ) -> Result<stream::Symbols> {
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

        if let Err(error) = stream::drive(subscribe, &mut stream)
            .await
            .unwrap()
            .unwrap()
        {
            self.shutdown_signal.cancel();
            bail!("Subscribe error in the stream drive: {error:?}");
        }

        let subscriber = self.event_publisher.clone();
        let shutdown_signal = self.shutdown_signal.clone();
        tokio::spawn(async move {
            info!("In task listening for mktdata updates");
            let mut retries = 5;
            loop {
                match stream
                    .by_ref()
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
                    }
                    Err(err) => {
                        error!("Error thrown in websocket {err:?}");
                        return;
                    }
                    _ => {
                        retries = 5;
                    }
                };
                retries -= 1;
                if stream.is_done() {
                    error!("websocket is done, should restart?");
                }
                warn!("Number of retries left in mktdata updates {retries}");
                if retries == 0 {
                    shutdown_signal.cancel();
                    break;
                }
            }
        });
        Ok(subscription.subscriptions().trades.clone())
    }

    pub async fn unsubscribe_from_stream(
        &self,
        symbols: stream::SymbolList,
    ) -> Result<stream::Symbols> {
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
        if let Err(error) = stream::drive(unsubscribe, &mut stream)
            .await
            .unwrap()
            .unwrap()
        {
            error!("Unsubscribe error in the stream drive: {error:?}");
            self.shutdown_signal.cancel();
            bail!("Unsubscribe error in the stream drive: {error:?}");
        }

        Ok(subscription.subscriptions().trades.clone())
    }
}
