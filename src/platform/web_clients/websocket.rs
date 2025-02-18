use anyhow::bail;
use anyhow::Ok;
use anyhow::Result;
use apca::api::v2::updates;
use apca::data::v2::stream;
use apca::data::v2::stream::MarketData;
use apca::Client;
use futures::FutureExt as _;
use futures::StreamExt as _;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

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
            shutdown_signal,
        }
    }

    pub async fn startup(&self, client: &Client) -> Result<()> {
        if let Err(err) = self.subscribe_to_data_stream(client).await {
            bail!("{:?}", err)
        }

        if let Err(err) = self.subscribe_to_order_updates(client).await {
            bail!("{:?}", err)
        }
        Ok(())
    }

    pub async fn subscribe_to_mktdata(&self, symbols: stream::SymbolList) -> Result<()> {
        let mut data = stream::MarketData::default();
        data.set_quotes(symbols);

        let _ = self
            .subscript_publisher
            .send(SubscriptPayload {
                action: SubscriptType::Subscribe,
                data,
            })
            .unwrap();
        Ok(())
    }

    pub async fn unsubscribe_from_mktdata(&self, symbols: stream::SymbolList) -> Result<()> {
        let mut data = stream::MarketData::default();
        data.set_quotes(symbols.clone());
        data.set_bars(symbols);

        let _ = self
            .subscript_publisher
            .send(SubscriptPayload {
                action: SubscriptType::Unsubscribe,
                data,
            })
            .unwrap();
        Ok(())
    }

    async fn subscribe_to_data_stream(&self, client: &Client) -> Result<()> {
        let mut subscript_subscriber = self.subscript_publisher.subscribe();
        let event_publisher = self.event_publisher.clone();
        let shutdown_signal = self.shutdown_signal.clone();

        let (mut stream, mut subscription) = client
            .subscribe::<stream::RealtimeData<stream::IEX>>()
            .await?;

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    event = subscript_subscriber.recv() => {
                        match event {
                            std::result::Result::Ok(SubscriptPayload { action, data }) => {
                                let subscribe = match action {
                                    SubscriptType::Subscribe => {
                                        debug!("Received subscribed for symbol list: {:?}", data);
                                        subscription.subscribe(&data).boxed().fuse()
                                    },
                                    SubscriptType::Unsubscribe => {
                                        debug!("Received unsubscribed for symbol list: {:?}", data);
                                        subscription.unsubscribe(&data).boxed().fuse()
                                    }

                                };
                                if let Err(err) = stream::drive(subscribe, &mut stream).await.unwrap().unwrap() {
                                        error!("Subscribe error in the stream drive: {err:?}");
                                        shutdown_signal.cancel();
                                        break
                                };
                            }
                            Err(RecvError::Lagged(err)) => warn!("Publisher channel skipping a number of messages: {}", err),
                            Err(RecvError::Closed) => {
                                error!("Publisher channel closed");
                                shutdown_signal.cancel();
                                break
                            }
                        }
                    },
                    payload = stream.next() => {
                        let publisher = event_publisher.clone();
                        let shutdown = shutdown_signal.clone();
                        tokio::spawn(async move {
                            if let Some(data) = payload {
                                let data = match data {
                                    std::result::Result::Ok(val) => val,
                                    Err(err) => {
                                        shutdown.cancel();
                                        return warn!("Failed to parse data, error={}", err);
                                    }
                                };
                                let data = match data {
                                    std::result::Result::Ok(val) => val,
                                    Err(err) => {
                                        shutdown.cancel();
                                        return warn!("Failed to parse data, error={}", err);
                                    }
                                };
                                let event = match data {
                                    stream::Data::Trade(data) => Event::Trade(data),
                                    stream::Data::Quote(data) => Event::Quote(data),
                                    stream::Data::Bar(data) => Event::Bar(data),
                                    _ => return,
                                };
                                let mut retries = 5;
                                while let Err(broadcast::error::SendError(data)) = publisher.send(event.clone()) {
                                    error!("{data:?}");
                                    match retries {
                                        0 => {
                                            error!("Max retries reached, closing app");
                                            shutdown.cancel();
                                            break
                                        },
                                        _ => retries -= 1
                                    }
                                }
                            };
                        });
                    }
                    _ = shutdown_signal.cancelled() => {
                        break
                    }
                }
            }
        });
        Ok(())
    }

    pub async fn subscribe_to_order_updates(&self, client: &Client) -> Result<()> {
        let (mut stream, _subscription) =
            client.subscribe::<updates::OrderUpdates>().await.unwrap();

        let event_publisher = self.event_publisher.clone();
        let shutdown_signal = self.shutdown_signal.clone();
        tokio::spawn(async move {
            info!("In task listening for order updates");
            loop {
                tokio::select! {
                    data = stream.next() => {
                        let publisher = event_publisher.clone();
                        let shutdown = shutdown_signal.clone();
                        tokio::spawn(async move {
                            if let Some(payload) = data {
                                let data = match payload.unwrap() {
                                    std::result::Result::Ok(val) => val,
                                    Err(err) => {
                                        shutdown.cancel();
                                        return warn!("Failed to parse data, error={}", err);
                                    }
                                };
                            let updates::OrderUpdate { event, order } = data;
                            let event =
                                Event::OrderUpdate(updates::OrderUpdate { event, order });
                            let mut retries = 5;
                            while let Err(broadcast::error::SendError(data)) = publisher.send(event.clone()) {
                                error!("{data:?}");
                                match retries {
                                    0 => {
                                        error!("Max retries reached, closing app");
                                        shutdown.cancel();
                                        break
                                    },
                                    _ => retries -= 1
                                }
                            }
                        }
                        });
                    },
                        _ = shutdown_signal.cancelled() => {
                            break
                    }
                }
            }
        });
        Ok(())
    }
}
