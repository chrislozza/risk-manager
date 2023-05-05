use apca::{ApiInfo, Client};
use apca::api::v2::order;
use log::{error, info, warn};
use std::error::Error;
use url::Url;

use num_decimal::Num;

mod account;
mod mktdata;
mod trading;
mod mktorder;
mod mktposition;
mod risk_sizing;

use crate::platform::account::AccountDetails;
use crate::platform::mktdata::MktData;
use crate::platform::trading::Trading;
use crate::platform::risk_sizing::MaxLeverage;

#[derive(Debug)]
struct Services {
    account: AccountDetails,
    mktdata: MktData,
    trading: Trading,
}

//#[derive(Debug)]
//struct RiskManagement {
//    locker: Locker,
//    risk: RiskCalculator
//}

#[derive(Debug)]
pub struct Platform {
    client: Client,
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
            client: Client::new(api_info),
            services: None,
        }
    }

    pub async fn startup(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut account = AccountDetails::new();
        let mut mktdata = MktData::new();
        let mut trading = Trading::new();

        account.startup(&self.client).await;
        trading.startup(&self.client).await;
        mktdata
            .startup(&self.client, trading.get_mktorders(), trading.get_mktpositions())
            .await;

        Ok(self.services = Some(Services {
            account,
            mktdata,
            trading,
        }))
    }

    pub async fn poll(&mut self) -> bool {
        if self.services.is_none() {
            info!("Startup not complete");
            return false;
        }
        info!("Sending order");
        let success = true;
//        let success = self
//            .services
//            .as_mut()
//            .unwrap()
//            .trading
//            .create_position(
//                &self.client,
//                "MSFT".to_string(),
//                Num::from(170),
//                Num::from(10),
//                order::Side::Buy,
//            )
//            .await;
        info!("Sending order success {success:?}");
        return success;
    }

//    pub fn create_position(&self, symbol: String, price: Num, side: order::Side) -> Result<(), Box<dyn std::error::Error>> {
//        let account = &self.services.unwrap().account;
//        let port_weight = MaxLeverage::get_port_weight(account.get_buying_power(), self.get_gross_position_value(), account.get_equity_with_loan());
//        self.services.unwrap().trading.create_position(&self.client,symbol,price, size, side); 
//        return true
//    }

    fn get_gross_position_value(&self) -> Num {
        let mut gross_position_value = Num::from(0);
        for order in self.services.as_ref().unwrap().trading.get_mktorders() {
            gross_position_value += order.market_value();
        }
        return gross_position_value;
    }
}
