use google_cloud_default::WithAuthExt;

use google_cloud_pubsub::client::{Client, ClientConfig};

use log::info;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::Event;
use super::MktSignal;
use crate::Settings;

pub struct GcpPubSub {
    client: Client,
    cancel_token: CancellationToken,
    subscription_name: String,
}

impl GcpPubSub {
    pub async fn new(settings: Settings) -> Self {
        let config = ClientConfig::default().with_auth().await.unwrap();
        GcpPubSub {
            client: Client::new(config).await.unwrap(),
            cancel_token: CancellationToken::new(),
            subscription_name: settings.gcp_subscription,
        }
    }

    pub fn shutdown(&self) {
        self.cancel_token.cancel()
    }

    pub async fn run(&self, send_mkt_signals: mpsc::UnboundedSender<Event>) -> Result<(), ()> {
        info!("PubSub subscribing to {}", &self.subscription_name);
        let subscriber = self.client.subscription(&self.subscription_name);
        //subscribe
        let cancel_receiver = self.cancel_token.clone();
        let _handle = tokio::spawn(async move {
            subscriber
                .receive(
                    move |message, _ctx| {
                        info!("Reieved a message pubsub");
                        let s2 = send_mkt_signals.clone();
                        async move {
                            let _ = message.ack().await;
                            let data = std::str::from_utf8(&message.message.data)
                                .unwrap()
                                .to_string();
                            let package: MktSignal = serde_json::from_str(&data).unwrap();
                            info!("Data pulled from pubsub {package:?}");
                            let _ = s2.send(Event::MktSignal(package));
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
