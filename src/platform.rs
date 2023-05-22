use apca::api::v2::order;
use apca::{ApiInfo, Client};
use log::info;
use std::sync::Arc;
use std::sync::Mutex;
use url::Url;
use std::collections::HashMap;

use num_decimal::Num;
use tokio::sync::mpsc;
use tokio::sync::broadcast;
use tokio::time;

mod account;
mod locker;
mod mktdata;
mod mktorder;
mod mktposition;
mod risk_sizing;
mod stream_handler;
mod trading;

use account::AccountDetails;
use locker::Locker;
use mktdata::MktData;
use mktorder::MktOrder;
use mktposition::MktPosition;
use risk_sizing::MaxLeverage;
use trading::Trading;
use super::events::{Event::*, Shutdown, Event};

struct Engine {
    account: AccountDetails,
    mktdata: MktData,
    trading: Trading,
    locker: Locker,
    positions: HashMap<String, MktPosition>,
    orders: HashMap<String, MktOrder>,
}

pub struct Platform {
    engine: Arc<Mutex<Engine>>,
    shutdown_signal: Option<mpsc::UnboundedSender<Event>>
}

impl Engine {
    pub fn new(
        account: AccountDetails,
        mktdata: MktData,
        trading: Trading,
        locker: Locker,
    ) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Engine {
            account,
            mktdata,
            trading,
            locker,
            positions: HashMap::default(),
            orders: HashMap::default(),
        }))
    }

    pub async fn shutdown(&self) {
        self.trading.shutdown();
        self.mktdata.shutdown();
    }

    pub fn get_mktorders(&self) -> &HashMap<String, MktOrder> {
        return &self.orders;
    }

    pub fn get_mktpositions(&self) -> &HashMap<String, MktPosition> {
        return &self.positions;
    }
}

impl Platform {
    pub fn new(key: &str, secret: &str, is_live: bool) -> Self {
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

        let engine = Engine::new(account, mktdata, trading, locker);

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

    async fn handle_event(event: &Event, engine_clone: Arc<Mutex<Engine>>) {
        let mut engine = engine_clone.lock().unwrap();
        match event {
            Event::Trade(data) => {
                if engine.locker.should_close(&data) {
                    let position = engine.positions.get(&data.symbol).unwrap();
                    engine.trading.liquidate_position(position).await;
                }
            },
            Event::OrderUpdate(data) => {
                info!("Orderupdate received {data:?}");
            }
            Event::Shutdown(data) => {
                info!("Orderupdate received {data:?}");
            }
        }
    }

    pub async fn run(&mut self) -> Result<(), ()> {
        info!("Sending order");

        let (shutdown_sender, mut shutdown_reader) = mpsc::unbounded_channel();
        let (mut trading_reader, mut mktdata_reader) = self.startup().await.unwrap();

        self.shutdown_signal = Some(shutdown_sender);
        let engine_cpy = Arc::clone(&self.engine);
        loop {
            tokio::select!(
                event = trading_reader.recv() => {
                    match event {
                        Ok(event) => {
                            info!("Found a trade {event:?}");
                            Self::handle_event(&event, Arc::clone(&engine_cpy)).await;
                        },
                        _ => (),
                    }
                }
                event = mktdata_reader.recv() => {
                    match event {
                        Ok(event) => {
                            info!("Found a mktdata {event:?}");
                            Self::handle_event(&event, Arc::clone(&engine_cpy));
                        },
                        _ => (),
                    }
                }
                _event = shutdown_reader.recv() => {
                        break;
                },
            );
        }
        Ok(())
    }

    //    pub fn create_position(&self, symbol: String, price: Num, side: order::Side) {
    //        let account = &self.services.unwrap().account;
    //        let port_weight = MaxLeverage::get_port_weight(account.get_buying_power(), self.get_gross_position_value(), account.get_equity_with_loan());
    //        self.services.unwrap().trading.create_position(symbol,price, Num::from(10), side);
    //    }
    //
    //    pub fn cancel_position(&self, symbol: String) -> Result<(), ()> {
    //        self.services.unwrap().trading.cancel_position();
    //        Ok(())
    //    }

    pub async fn create_position(&mut self) {
        self.engine.lock().unwrap().trading.create_position(
            "MSFT".to_string(),
            Num::from(120 as i8),
            Num::from(10 as i8),
            order::Side::Buy,
        );
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
