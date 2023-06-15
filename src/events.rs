use apca::api::v2::updates;
use apca::data::v2::stream;
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use tokio::sync::mpsc;
use std::sync::Arc;
use tokio::sync::Mutex;
use log::{info};

mod pub_sub;
mod web_hook;

use web_hook::WebHook;
use pub_sub::GcpPubSub;
use super::Settings;

use tokio::time::{sleep, Duration};

#[derive(Debug, Clone)]
pub enum Shutdown {
    Good,
    Bad,
}

#[derive(Debug, Clone)]
pub enum Event {
    Trade(stream::Trade),
    OrderUpdate(updates::OrderUpdate),
    MktSignal(MktSignal),
    Shutdown(Shutdown)
}

#[derive(Debug, Clone)]
pub enum PortAction {
    Create,
    Liquidate,
}

impl<'de> serde::Deserialize<'de> for PortAction {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value: u8 = Deserialize::deserialize(deserializer)?;
        match value {
            1 => Ok(PortAction::Create),
            2 => Ok(PortAction::Liquidate),
            _ => Err(serde::de::Error::custom("Invalid PortAction value")),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Direction {
    Long,
    Short,
}

impl<'de> serde::Deserialize<'de> for Direction {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value: u8 = Deserialize::deserialize(deserializer)?;
        match value {
            1 => Ok(Direction::Long),
            2 => Ok(Direction::Short),
            _ => Err(serde::de::Error::custom("Invalid Direction value")),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Side {
    Buy,
    Sell,
}

impl<'de> serde::Deserialize<'de> for Side {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value: u8 = Deserialize::deserialize(deserializer)?;
        match value {
            1 => Ok(Side::Buy),
            2 => Ok(Side::Sell),
            _ => Err(serde::de::Error::custom("Invalid Side value")),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Source {
    Email,
}


impl<'de> serde::Deserialize<'de> for Source {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value: u8 = Deserialize::deserialize(deserializer)?;
        match value {
            1 => Ok(Source::Email),
            _ => Err(serde::de::Error::custom("Invalid Source value")),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MktSignal {
    pub strategy: String,
    pub symbol: String,
    pub side: Side,
    pub action: PortAction,
    pub direction: Direction,
    pub source: Source,
    pub price: Option<f64>,
    pub primary_exchange: Option<String>,
    pub is_dirty: Option<bool>,
    pub amount: Option<f64>,
}

struct EventClients {
    pubsub: GcpPubSub,
    webhook: WebHook,
}

pub struct EventPublisher {
    event_clients: Arc<Mutex<EventClients>>,
    shutdown_signal: Option<mpsc::UnboundedSender<Event>>,
    settings: Settings
}

impl EventClients {
    pub fn new(
        pubsub: GcpPubSub,
        webhook: WebHook,
        ) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(EventClients {
            pubsub,
            webhook,
        }))
    }

    pub async fn shutdown(&self) {
        self.pubsub.shutdown();
        self.webhook.shutdown();
    }
}

impl EventPublisher {
    pub async fn new(settings: Settings) -> Self {
        info!("Initialised publisher components");
        EventPublisher {
            event_clients: EventClients::new(GcpPubSub::new(settings.clone()).await, WebHook::new(settings.clone()).await),
            shutdown_signal: None,
            settings,
        }
    }

    async fn startup(&self, send_mkt_signals: &mpsc::UnboundedSender<Event>) -> Result<(), ()> {
        let event_clients = self.event_clients.lock().await;
        let _ = event_clients.webhook.run(&send_mkt_signals.clone());
        let _ = event_clients.pubsub.run(send_mkt_signals.clone()).await;
        Ok(())
    }

    pub async fn shutdown(&self) {
        self.event_clients.lock().await.shutdown();
        match self.shutdown_signal.as_ref() {
            Some(val) => val.clone().send(Event::Shutdown(Shutdown::Good)).unwrap(),
            _ => ()
        }
    }

    pub async fn run(&mut self, send_mkt_signals: &mpsc::UnboundedSender<Event>) -> Result<(), ()> {
        let (shutdown_sender, mut shutdown_reader) = mpsc::unbounded_channel();
        self.startup(send_mkt_signals).await.unwrap();
        info!("Startup completed in event publisher");

        self.shutdown_signal = Some(shutdown_sender);
        let event_clients_cpy = Arc::clone(&self.event_clients);
        tokio::spawn(async move {
            info!("Taking a loop in the event publisher");
            loop {
                tokio::select!(
                    _event = shutdown_reader.recv() => {
                        info!("Shutdown reader in the event publisher");
                        break;
                    }
                    _ = sleep(Duration::from_millis(10)) => {
                    }
                );
            }
            info!("Returning from the loop in the event publisher");
            let _ = event_clients_cpy.lock().await.shutdown().await;
        });
        Ok(())
    }
}
