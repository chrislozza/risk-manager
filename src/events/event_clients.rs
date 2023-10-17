use anyhow::Result;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::Sender;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use super::pub_sub::GcpPubSub;
use super::web_hook::WebHook;
use super::Event;
use super::Settings;

pub struct EventClients {
    pubsub: GcpPubSub,
    webhook: WebHook,
    publisher: Sender<Event>,
}

impl EventClients {
    pub async fn new(
        shutdown_signal: CancellationToken,
        settings: Settings,
    ) -> Result<Arc<Mutex<Self>>> {
        let (publisher, _) = broadcast::channel(32);
        let pubsub = GcpPubSub::new(shutdown_signal.clone(), settings.clone()).await?;
        let webhook = WebHook::new(shutdown_signal).await;
        Ok(Arc::new(Mutex::new(EventClients {
            pubsub,
            webhook,
            publisher,
        })))
    }

    pub fn subscribe_to_events(&self) -> Receiver<Event> {
        self.publisher.subscribe()
    }

    pub async fn run(&mut self) -> Result<()> {
        self.pubsub.run(self.publisher.clone()).await;
        self.webhook.run(self.publisher.clone()).await
    }
}
