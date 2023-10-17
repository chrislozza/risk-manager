use anyhow::Result;
use serde::Deserialize;
use serde::Deserializer;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info;

mod event_clients;
mod pub_sub;
mod web_hook;

use super::Event;
use super::Settings;
use event_clients::EventClients;

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

#[derive(Debug, Clone, Copy, Default)]
pub enum Direction {
    #[default]
    Long,
    Short,
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
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

impl FromStr for Direction {
    type Err = String;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        match val {
            "Long" => Ok(Direction::Long),
            "Short" => Ok(Direction::Short),
            _ => Err(format!("Failed to parse direction, unknown: {}", val)),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub enum Side {
    #[default]
    Buy,
    Sell,
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl FromStr for Side {
    type Err = String;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        match val {
            "Buy" => Ok(Side::Buy),
            "Sell" => Ok(Side::Sell),
            _ => Err(format!("Failed to parse direction, unknown: {}", val)),
        }
    }
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
    PubSub,
    WebHook,
}

impl<'de> serde::Deserialize<'de> for Source {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value: u8 = Deserialize::deserialize(deserializer)?;
        match value {
            1 => Ok(Source::PubSub),
            2 => Ok(Source::WebHook),
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

    pub async fn startup(&self) -> Receiver<Event> {
        self.event_clients.lock().await.subscribe_to_events()
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("Startup completed in event publisher");
        self.event_clients.lock().await.run().await
    }
}
