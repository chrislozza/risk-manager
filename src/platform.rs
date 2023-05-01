use apca::api::v2::{order, position};
use apca::{ApiInfo, Client};
use log::{error, info, warn};
use std::error::Error;
use url::Url;

mod account;
pub mod mktdata;
pub mod trading;

use crate::platform::account::AccountDetails;
use crate::platform::mktdata::MktData;
use crate::platform::trading::Trading;

struct Services {
    account: AccountDetails,
    mktdata: MktData,
    trading: Trading,
}

//struct RiskManagement {
//    locker: Locker,
//    risk: RiskCalculator
//}

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
        let mktdata = MktData::new();
        let mut trading = Trading::new();

        account.startup(&self.client).await;
        trading.startup(&self.client).await;
        info!("To loop Here");
        mktdata
            .startup(&self.client, vec!["APPL".to_string()])
            .await;
        info!("To loop Here");

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
        match self
            .services
            .as_mut()
            .unwrap()
            .trading
            .create_position(
                &self.client,
                "APPL".to_string(),
                170.00,
                10,
                order::Side::Buy,
            )
            .await
        {
            Err(err) => {
                error!("Failed to place first order {}", err);
                return false;
            }
            _ => (),
        };
        info!("Polled");
        return true;
    }
}
