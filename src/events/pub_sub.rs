use google_cloud_default::WithAuthExt;

use google_cloud_pubsub::client::{Client, ClientConfig};

use log::{info, warn};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use std::collections::HashMap;

use super::Event;
use super::MktSignal;
use crate::Settings;

#[derive(Debug, Clone)]
pub struct GcpPubSub {
    client: Client,
    shutdown_signal: CancellationToken,
    subscription_name: String,
}

impl GcpPubSub {
    pub async fn new(shutdown_signal: CancellationToken, settings: Settings) -> Self {
        let config = ClientConfig::default().with_auth().await.unwrap();
        GcpPubSub {
            client: Client::new(config).await.unwrap(),
            shutdown_signal,
            subscription_name: settings.gcp_subscription,
        }
    }

    pub fn startup(&self) {}

    pub async fn run(&self, send_mkt_signals: mpsc::UnboundedSender<Event>) -> Result<(), ()> {
        info!("PubSub subscribing to {}", &self.subscription_name);
        let subscriber = self.client.subscription(&self.subscription_name);
        //subscribe
        let cancel_receiver = self.shutdown_signal.clone();
        let _handle = tokio::spawn(async move {
            subscriber
                .receive(
                    move |message, _ctx| {
                        let sender = send_mkt_signals.clone();
                        async move {
                            let _ = message.ack().await;
                            let data = std::str::from_utf8(&message.message.data)
                                .unwrap()
                                .to_string();
                            let package: HashMap<String, String> =
                                serde_json::from_str(&data).unwrap();
                            let payload = &package["payload"];

                            if let Ok(event) = serde_json::from_str::<MktSignal>(payload) {
                                info!("Data pulled from pubsub {event:?}");
                                let _ = sender.send(Event::MktSignal(event));
                            } else {
                                warn!("Failed to parse unknown message");
                            }
                        }
                    },
                    cancel_receiver,
                    None,
                )
                .await
                .unwrap();
        });
        info!("After task spawned pubsub");
        Ok(())
    }
}
