use apca::api::v2::order;
use apca::{ApiInfo, Client};
use log::{info, error};
use std::sync::{Arc, Mutex};
use url::Url;
use std::collections::HashMap;

use num_decimal::Num;
use tokio::sync::mpsc;
use tokio::sync::broadcast;
use tokio::time;

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
use locker::Locker;
use engine::Engine;
use mktdata::MktData;
use mktorder::MktOrder;
use mktposition::MktPosition;
use risk_sizing::MaxLeverage;
use trading::Trading;
use super::events::{Event::*, Shutdown, Event};
use crate::Settings;
use crate::events::MktSignal;

pub struct Platform {
    engine: Arc<Mutex<Engine>>,
    shutdown_signal: Option<mpsc::UnboundedSender<Event>>
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
        Platform { engine, shutdown_signal: None }
    }

    async fn startup(&self) -> Result<(broadcast::Receiver<Event>, broadcast::Receiver<Event>), ()> {
        let mut engine = self.engine.lock().unwrap();
        engine.account.startup().await;
        let (positions, orders) = engine.trading.startup().await?;
        engine.mktdata.startup(&orders, &positions).await;
        engine.orders = orders;
        engine.positions = positions;
        Ok((
            engine.trading.stream_reader(),
            engine.mktdata.stream_reader(),
        ))
    }

    pub async fn shutdown(&self) {
        self.engine.lock().unwrap().shutdown();
        match self.shutdown_signal.as_ref() {
            Some(val) => val.clone().send(Event::Shutdown(Shutdown::Good)).unwrap(),
            _ => ()
        }
    }

    async fn handle_event(event: &Event, engine: &Arc<Mutex<Engine>>) {
        match event {
            Event::Trade(data) => {
                info!("Trade received {data:?}");
                engine.lock().unwrap().mktdata_update(&data);
            },
            Event::OrderUpdate(data) => {
                engine.lock().unwrap().order_update(&data);
                info!("Orderupdate received {data:?}");
            },
            Event::MktSignal(data) => {
                info!("MarketSignal received {data:?}");
                engine.lock().unwrap().create_position(&data);
            },
            Event::Shutdown(data) => {
                info!("Shutdown received {data:?}");
                engine.lock().unwrap().shutdown();
            },
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
            loop {
                info!("Taking a loop in the platform");
                tokio::select!(
                    event = trading_reader.recv() => {
                        match event {
                            Ok(event) => {
                                info!("Found a trade {event:?}");
                                Self::handle_event(&event, &engine_clone).await;
                            },
                            _ => (),
                        }
                    }
                    event = mktdata_reader.recv() => {
                        match event {
                            Ok(event) => {
                                info!("Found a mktdata {event:?}");
                                Self::handle_event(&event, &engine_clone).await;
                            },
                            _ => (),
                        }
                    }
                    _event = shutdown_reader.recv() => {
                        break;
                    },
                    );
            }
        }).await;
        info!("Returning from the loop in the platform");
        Ok(())
    }

    pub async fn create_position(&mut self, mkt_signal: &MktSignal) {
        self.engine.lock().unwrap().create_position(mkt_signal);
    }

    fn get_gross_position_value(&self) -> Num {
        let mut gross_position_value = Num::from(0);
        let engine = self.engine.lock().unwrap();
        for order in engine.get_mktorders().values() {
            gross_position_value += order.market_value();
        }
        return gross_position_value;
    }
}
