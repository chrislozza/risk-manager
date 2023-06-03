use apca::{ApiInfo, Client};
use log::{info, debug};
use std::sync::Arc;
use tokio::sync::Mutex;
use url::Url;

use num_decimal::Num;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{sleep, Duration};

mod account;
mod engine;
mod locker;
mod mktdata;
mod mktorder;
mod mktposition;
mod risk_sizing;
mod stream_handler;
mod trading;

use account::AccountDetails;
use engine::Engine;
use locker::Locker;
use mktdata::MktData;
use mktorder::MktOrder;
use mktposition::MktPosition;

use super::events::{Event, Shutdown};
use crate::events::MktSignal;
use crate::Settings;
use trading::Trading;

pub struct Platform {
    engine: Arc<Mutex<Engine>>,
    shutdown_signal: Option<mpsc::UnboundedSender<Event>>,
}

impl Platform {
    pub fn new(settings: Settings, key: &str, secret: &str, is_live: bool) -> Self {
        let api_base_url = match is_live {
            true => Url::parse("https://api.alpaca.markets").unwrap(),
            false => Url::parse("https://paper-api.alpaca.markets").unwrap(),
        };
        info!("Using url {api_base_url}");
        let api_info = ApiInfo::from_parts(api_base_url, key, secret).unwrap();

        let client = Arc::new(Mutex::new(Client::new(api_info)));
        let account = AccountDetails::new(Arc::clone(&client));
        let trading = Trading::new(Arc::clone(&client));
        let mktdata = MktData::new(Arc::clone(&client));
        let locker = Locker::new();

        let engine = Engine::new(settings, account, mktdata, trading, locker);

        info!("Initialised platform components");
        Platform {
            engine,
            shutdown_signal: None,
        }
    }

    async fn startup(
        &self,
    ) -> Result<(broadcast::Receiver<Event>, broadcast::Receiver<Event>), ()> {
        Ok(self.engine.lock().await.startup().await)
    }

    pub async fn shutdown(&self) {
        self.engine.lock().await.shutdown().await;
        match self.shutdown_signal.as_ref() {
            Some(val) => val.clone().send(Event::Shutdown(Shutdown::Good)).unwrap(),
            _ => (),
        }
    }

    pub async fn run(&mut self) -> Result<(), ()> {
        info!("Sending order");
        let (shutdown_sender, mut shutdown_reader) = mpsc::unbounded_channel();
        let (mut trading_reader, mut mktdata_reader) = self.startup().await.unwrap();
        info!("Startup completed in the platform");
        
        self.shutdown_signal = Some(shutdown_sender);
        let engine_clone = Arc::clone(&self.engine);
        tokio::spawn(async move {
            info!("Taking a loop in the platform");
            loop {
                tokio::select!(
                event = trading_reader.recv() => {
                    if let Ok(Event::OrderUpdate(event)) = event {
                        info!("Found a trade event: {event:?}");
                        //engine_clone.lock().unwrap().create_position(&event);
                    };
                }
                event = mktdata_reader.recv() => {
                    if let Ok(Event::Trade(event)) = event {
                        debug!("Found a mkdata event: {event:?}");

                        engine_clone.lock().await.mktdata_update(&event).await;
                    };
                }
                _event = shutdown_reader.recv() => {
                    break;
                }
                _ = sleep(Duration::from_millis(10)) => {
                });
            }
            info!("Shutting down event loop in platform");
        });
        Ok(())
    }

    pub async fn create_position(&mut self, mkt_signal: &MktSignal) {
        self.engine.lock().await.create_position(mkt_signal).await;
    }

    async fn get_gross_position_value(&self) -> Num {
        let mut gross_position_value = Num::from(0);
        let engine = self.engine.lock().await;
        for order in engine.get_mktorders().values() {
            gross_position_value += order.market_value();
        }
        gross_position_value
    }
}
