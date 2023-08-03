use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use anyhow::Result;

mod data;
mod engine;
mod locker;
mod mktdata;
mod order_handler;
mod risk_sizing;
mod web_clients;

use engine::Engine;

use super::events::MktSignal;
use super::Event;
use crate::Settings;
use tokio_util::sync::CancellationToken;

pub struct Platform {
    engine: Arc<Mutex<Engine>>,
    shutdown_signal: CancellationToken,
}

impl Platform {
    pub async fn new(
        settings: Settings,
        key: &str,
        secret: &str,
        is_live: bool,
        shutdown_signal: CancellationToken,
    ) -> Result<Self> {
        let engine = Engine::new(settings, key, secret, is_live, shutdown_signal.clone()).await?;

        info!("Initialised platform components");
        Ok(Platform {
            engine,
            shutdown_signal,
        })
    }

    pub async fn startup(&self) -> Result<()> {
        let result = self.engine.lock().await.startup().await;
        info!("Startup completed in the platform");
        result
    }

    pub async fn run(&mut self) -> Result<()> {
        let engine = Arc::clone(&self.engine);
        Engine::run(engine, self.shutdown_signal.clone()).await
    }

    pub async fn create_position(&mut self, mkt_signal: &MktSignal) -> Result<()> {
        self.engine.lock().await.create_position(mkt_signal).await
    }

    pub async fn print_status(&self) {
        self.engine.lock().await.update_status().await
    }
}
