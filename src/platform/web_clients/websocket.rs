use apca::api::v2::updates;
use apca::data::v2::stream;
use apca::data::v2::stream::MarketData;
use apca::Client;
use std::sync::Arc;
use tokio::sync::broadcast::error::RecvError;

use tracing::error;
use tracing::info;
use tracing::warn;

use anyhow::Result;

use tokio::sync::broadcast;

use tokio_util::sync::CancellationToken;

use futures::FutureExt as _;
use futures::StreamExt as _;
use futures::TryStreamExt as _;

use super::Event;

#[derive(Debug, Clone)]
pub enum SubscriptType {
    Subscribe,
    Unsubscribe,
}

#[derive(Debug, Clone)]
pub struct SubscriptPayload {
    pub action: SubscriptType,
    pub data: MarketData,
}

#[derive(Debug)]
pub(crate) struct WebSocket {
    event_publisher: broadcast::Sender<Event>,
    subscript_publisher: broadcast::Sender<SubscriptPayload>,
    not_initialised: bool,
    shutdown_signal: CancellationToken,
}

impl WebSocket {
    pub fn new(
        event_publisher: broadcast::Sender<Event>,
        shutdown_signal: CancellationToken,
    ) -> Self {
        let (publisher, _) = broadcast::channel(100);
        WebSocket {
            event_publisher,
            subscript_publisher: publisher,
            not_initialised: true,
            shutdown_signal,
        }
    }

    pub async fn subscribe_to_mktdata(
        &self,
        client: &Client,
        symbols: stream::SymbolList,
        subscript_type: SubscriptType,
    ) -> Result<()> {
        let mut data = stream::MarketData::default();
        data.set_trades(symbols);

        if !self.not_initialised {
            let _ = match subscript_type {
                SubscriptType::Subscribe => self
                    .subscript_publisher
                    .send(SubscriptPayload {
                        action: SubscriptType::Subscribe,
                        data,
                    })
                    .unwrap(),
                SubscriptType::Unsubscribe => self
                    .subscript_publisher
                    .send(SubscriptPayload {
                        action: SubscriptType::Unsubscribe,
                        data,
                    })
                    .unwrap(),
            };
        } else {
            let subscript_publisher = self.subscript_publisher.clone();
            let mut subscript_subscriber = subscript_publisher.subscribe();
            let event_publisher = self.event_publisher.clone();
            let shutdown_signal = self.shutdown_signal.clone();

            let (mut stream, mut subscription) = client
                .subscribe::<stream::RealtimeData<stream::IEX>>()
                .await
                .unwrap();

            tokio::spawn(async move {
                let subscribe = subscription.subscribe(&data).boxed().fuse();

                if let Err(error) = stream::drive(subscribe, &mut stream)
                    .await
                    .unwrap()
                    .unwrap()
                {
                    error!("Subscribe error in the stream drive: {error:?}");
                    shutdown_signal.cancel();
                    return;
                }
                let mut retries = 5;
                loop {
                    tokio::select! {
                        event = subscript_subscriber.recv() => {
                            match event {
                                Ok(SubscriptPayload { action, data }) => {
                                    let subscribe = match action {
                                        SubscriptType::Subscribe => subscription.subscribe(&data).boxed().fuse(),
                                        SubscriptType::Unsubscribe => subscription.unsubscribe(&data).boxed().fuse()

                                    };
                                    match stream::drive(subscribe, &mut stream).await.unwrap().unwrap() {
                                        Err(err) =>
                                        {
                                            error!("Subscribe error in the stream drive: {err:?}");
                                            shutdown_signal.cancel();
                                            break
                                        }
                                        _ => ()
                                    }
                                }
                                Err(RecvError::Lagged(err)) => warn!("Publisher channel skipping a number of messages: {}", err),
                                Err(RecvError::Closed) => {
                                    error!("Publisher channel closed");
                                    shutdown_signal.cancel();
                                    break
                                }
                            }
                        },
                        data = stream.next() => {
                            let event = match data.unwrap().unwrap().unwrap() {
                                stream::Data::Trade(data) => Event::Trade(data),
                                _ => return,
                            };
                            match event_publisher.send(event) {
                                Err(broadcast::error::SendError(data)) => {
                                    error!("{data:?}");
                                    match retries {
                                        0 => shutdown_signal.cancel(),
                                        _ => retries -= 1
                                    }
                                }
                                Ok(_) => retries = 5,
                            };
                        }
                        _ = shutdown_signal.cancelled() => {
                            break
                        }
                    }
                }
            });
        }
        Ok(())
    }

    pub async fn subscribe_to_order_updates(&self, client: &Client) -> Result<()> {
        let (mut stream, _subscription) =
            client.subscribe::<updates::OrderUpdates>().await.unwrap();

        let subscriber = self.event_publisher.clone();
        let shutdown_signal = self.shutdown_signal.clone();
        tokio::spawn(async move {
            info!("In task listening for order updates");
            let mut retries = Arc::new(5);
            loop {
                match stream
                    .by_ref()
                    .map_err(apca::Error::WebSocket)
                    .try_for_each(|result| async {
                        info!("Order Updates {result:?}");
                        result
                            .map(|data| {
                                let mut retry_count = Arc::clone(&retries);
                                let updates::OrderUpdate { event, order } = data;
                                let event =
                                    Event::OrderUpdate(updates::OrderUpdate { event, order });
                                match subscriber.send(event) {
                                    Err(broadcast::error::SendError(error)) => {
                                        error!("Sending error {error:?}");
                                    }
                                    Ok(_) => {
                                        retry_count = 5.into();
                                    }
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
                        retries = 5.into();
                        continue;
                    }
                };
                if stream.is_done() {
                    error!("websocket is done, should restart?");
                    shutdown_signal.cancel();
                    break;
                }
                retries = (*retries - 1).into();
                warn!("Number of retries left in order updates {retries}");
                if *retries == 0 {
                    shutdown_signal.cancel();
                    break;
                }
            }
            info!("Trading updates ended");
        });
        Ok(())
    }
}
