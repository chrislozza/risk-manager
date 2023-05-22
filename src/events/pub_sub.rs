use google_cloud_default::WithAuthExt;
use google_cloud_gax::grpc::Status;
use google_cloud_pubsub::subscription::ReceiveConfig;
use google_cloud_pubsub::subscription::SubscriptionConfig;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::client::{Client, ClientConfig};

use log::info;

use std::time::Duration;
use tokio_util::sync::CancellationToken;
use futures_util::StreamExt as _;


pub struct GcpPubSub {
    client: Client,
    cancel_token: CancellationToken
}

impl GcpPubSub { 
    pub async fn new() -> Self {
        let config = ClientConfig::default().with_auth().await.unwrap();
        GcpPubSub {
            client: Client::new(config).await.unwrap(),
            cancel_token: CancellationToken::new()
        }
    }

    pub fn shutdown(&self) {
        self.cancel_token.cancel()
    }

    pub async fn run(&self, subscription: &str) -> Result<(), ()> {
        let subscriber = self.client.subscription(subscription);
        //subscribe
        let cancel_receiver = self.cancel_token.clone();
        let (sender, mut reader) = tokio::sync::mpsc::channel(100);
        let handle = tokio::spawn(async move {
            let _ = subscriber 
                .receive(
                    move |message, _ctx| {
                        let s2 = sender.clone();
                        async move {
                            let _ = message.ack().await;
                            let data = std::str::from_utf8(&message.message.data).unwrap().to_string();
                            let _ = s2.send(data).await;
                        }
                    },
                    cancel_receiver,
                    None,
                )
                .await.unwrap();
        }).await;
        Ok(())
    }
}
