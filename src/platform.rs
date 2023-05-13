use apca::api::v2::order;
use apca::{ApiInfo, Client};
use log::{error, info, warn};
use std::error::Error;
use std::sync::Arc;
use std::sync::Mutex;
use url::Url;

use num_decimal::Num;
use tokio::time;

mod account;
mod locker;
mod mktdata;
mod mktorder;
mod mktposition;
mod risk_sizing;
mod trading;

use crate::platform::account::AccountDetails;
use crate::platform::locker::TrailingStop;
use crate::platform::mktdata::MktData;
use crate::platform::risk_sizing::MaxLeverage;
use crate::platform::trading::Trading;

#[derive(Debug)]
struct Services {
    account: AccountDetails,
    mktdata: MktData,
    trading: Trading,
}

#[derive(Debug)]
struct RiskManagement {
    locker: TrailingStop,
    risk: MaxLeverage,
}

#[derive(Debug)]
pub struct Platform {
    client: Arc<Mutex<Client>>,
    services: Option<Services>,
}

impl Platform {
    pub fn new(key: &str, secret: &str, is_live: bool) -> Self {
        let api_base_url = match is_live {
            true => Url::parse("https://api.alpaca.markets").unwrap(),
            false => Url::parse("https://paper-api.alpaca.markets").unwrap(),
        };
        info!("Using url {api_base_url}");
        let api_info = ApiInfo::from_parts(api_base_url, key, secret).unwrap();
        Platform {
            client: Arc::new(Mutex::new(Client::new(api_info))),
            services: None,
        }
    }

    pub async fn startup(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut account = AccountDetails::new(Arc::clone(&self.client));
        let mut mktdata = MktData::new(Arc::clone(&self.client));
        let mut trading = Trading::new(Arc::clone(&self.client));

        account.startup().await;
        trading.startup().await;
        mktdata
            .startup(trading.get_mktorders(), trading.get_mktpositions())
            .await;

        Ok(self.services = Some(Services {
            account,
            mktdata,
            trading,
        }))
    }

    pub async fn poll(&mut self) -> Result<(), ()> {
        if self.services.is_none() {
            info!("Startup not complete");
            return Ok(());
        }
        info!("Sending order");
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

    fn get_gross_position_value(&self) -> Num {
        let mut gross_position_value = Num::from(0);
        for order in self.services.as_ref().unwrap().trading.get_mktorders() {
            gross_position_value += order.market_value();
        }
        return gross_position_value;
    }
}
