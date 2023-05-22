use apca::api::v2::order;
use apca::{ApiInfo, Client};
use log::info;
use std::error::Error;
use std::sync::Arc;
use std::sync::Mutex;
use url::Url;

use num_decimal::Num;
use tokio::sync::broadcast::Receiver;
use tokio::time;

mod account;
mod locker;
mod mktdata;
mod mktorder;
mod mktposition;
mod risk_sizing;
mod stream_handler;
mod trading;

use crate::platform::account::AccountDetails;
use crate::platform::locker::Locker;
use crate::platform::mktdata::MktData;
use crate::platform::mktorder::MktOrder;
use crate::platform::mktposition::MktPosition;
use crate::platform::risk_sizing::MaxLeverage;
use crate::platform::stream_handler::Event;
use crate::platform::trading::Trading;

struct Engine {
    account: AccountDetails,
    mktdata: MktData,
    trading: Trading,
    locker: Locker,
    positions: Vec<MktPosition>,
    orders: Vec<MktOrder>,
}

pub struct Platform {
    engine: Arc<Mutex<Engine>>,
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
            positions: Vec::default(),
            orders: Vec::default(),
        }))
    }

    pub fn get_mktorders(&self) -> &Vec<MktOrder> {
        return &self.orders;
    }

    pub fn get_mktpositions(&self) -> &Vec<MktPosition> {
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

        Platform { engine }
    }

    async fn startup(&self) -> Result<(Receiver<Event>, Receiver<Event>), ()> {
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

    pub async fn run(&mut self) -> Result<(), ()> {
        info!("Sending order");

        let _engine = Arc::clone(&self.engine);
        let (mut trading_reader, mut mktdata_reader) = self.startup().await.unwrap();
        loop {
            tokio::select!(
                val = trading_reader.recv() => {info!("Found a trade {val:?}")}
                val = mktdata_reader.recv() => {info!("Found a mktdata {val:?}")}
            );
        }
        //        self.services.as_mut().unwrap().mktdata.subscribe("BTCUSD".to_string());
        //        match self
        //            .services
        //            .as_mut()
        //            .unwrap()
        //            .trading
        //            .create_position(
        //                "BTC/USD".to_string(),
        //                Num::from(170),
        //                Num::from(10),
        //                order::Side::Buy,
        //                )
        //            .await {
        //                _ => {
        //                    let mut _mktorders = &self.services.as_ref().unwrap().trading.get_mktorders();
        ////                    trading.cancel_position();
        //                }
        //            };

        time::sleep(time::Duration::from(time::Duration::from_secs(45))).await;
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
        for order in engine.get_mktorders() {
            gross_position_value += order.market_value();
        }
        return gross_position_value;
    }
}
