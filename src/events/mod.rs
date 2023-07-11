use apca::api::v2::updates;
use apca::data::v2::stream;
use serde::{Deserialize, Deserializer};
use tracing::info;

use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::Sender;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use anyhow::Result;

mod event_clients;
mod pub_sub;
mod web_hook;

use super::Settings;
use event_clients::EventClients;

use tokio::time::sleep;
use tokio::time::Duration;

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
    pub price: f64,
    pub primary_exchange: Option<String>,
    pub is_dirty: Option<bool>,
    pub amount: Option<f64>,
}

pub struct EventPublisher {
    event_clients: Arc<Mutex<EventClients>>,
}

impl EventPublisher {
    pub async fn new(shutdown_signal: CancellationToken, settings: Settings) -> Result<Self> {
        info!("Initialised publisher components");
        Ok(EventPublisher {
            event_clients: EventClients::new(shutdown_signal, settings).await?,
        })
    }

    pub async fn startup(&self) -> Result<Receiver<Event>> {
        self.event_clients.lock().await.startup().await
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("Startup completed in event publisher");
        self.event_clients.lock().await.run().await
    }
}
