use google_cloud_default::WithAuthExt;

use google_cloud_pubsub::client::Client;
use google_cloud_pubsub::client::ClientConfig;

use tracing::info;
use tracing::warn;

use tokio::sync::broadcast::Sender;
use tokio_util::sync::CancellationToken;

use anyhow::Result;
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
    pub async fn new(shutdown_signal: CancellationToken, settings: Settings) -> Result<Self> {
        let config = ClientConfig::default().with_auth().await?;
        Ok(GcpPubSub {
            client: Client::new(config).await.unwrap(),
            shutdown_signal,
            subscription_name: settings.gcp_subscription,
        })
    }

    pub async fn run(&self, event_publisher: Sender<Event>) -> Result<()> {
        info!("PubSub subscribing to {}", &self.subscription_name);
        let subscriber = self.client.subscription(&self.subscription_name);
        //subscribe
        let shutdown_signal = self.shutdown_signal.clone();
        let _ = tokio::spawn(async move {
            let _ = subscriber
                .receive(
                    move |message, _ctx| {
                        let sender = event_publisher.clone();
                        async move {
                            if let Err(err) = message.ack().await {
                                warn!("Failed to ack gcp message, error: {err}");
                            }
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
                    shutdown_signal,
                    None,
                )
                .await;
        });
        Ok(())
    }
}
