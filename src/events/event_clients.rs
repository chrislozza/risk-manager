use apca::api::v2::updates;
use apca::data::v2::stream;
use serde::{Deserialize, Deserializer};
use tracing::info;

use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::Sender;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use anyhow::Result;
use tokio::time::sleep;
use tokio::time::Duration;

use super::Event;
use super::Settings;
use super::pub_sub::GcpPubSub;
use super::web_hook::WebHook;

pub struct EventClients {
    pubsub: GcpPubSub,
    webhook: WebHook,
    subscriber: Receiver<Event>,
    publisher: Sender<Event>,
}

impl EventClients {
    pub async fn new(
        shutdown_signal: CancellationToken,
        settings: Settings,
    ) -> Result<Arc<Mutex<Self>>> {
        let (publisher, subscriber) = broadcast::channel(32);
        let pubsub = GcpPubSub::new(shutdown_signal.clone(), settings.clone()).await?;
        let webhook = WebHook::new(shutdown_signal).await;
        Ok(Arc::new(Mutex::new(EventClients {
            pubsub,
            webhook,
            subscriber,
            publisher,
        })))
    }

    pub async fn startup(&self) -> Result<Receiver<Event>> {
        Ok(self.subscribe_to_events())
    }

    pub fn subscribe_to_events(&self) -> Receiver<Event> {
        self.publisher.subscribe()
    }

    pub async fn run(&mut self) -> Result<()> {
        self.pubsub.run(self.publisher.clone()).await;
        self.webhook.run(self.publisher.clone()).await
    }
}
