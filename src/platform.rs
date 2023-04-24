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

    pub async fn startup(&mut self, is_live: bool) -> Result<(), Box<dyn Error + Send + Sync>> {
        let account = AccountDetails::new(&self.client).await;
        info!("Account deets: {:?}", account);
        let mktdata = MktData::new(&self.client, is_live).await;
        info!("MktData deets: {:?}", mktdata);
        let trading = Trading::new(&self.client, is_live).await;
        info!("Trading deets: {:?}", trading);

        Ok(self.services = Some(Services {
            account,
            mktdata,
            trading
        }))
    }

    pub async fn poll(&self) -> bool {
        if self.services.is_none() {
            info!("Startup not complete");
            return false;
        }
        return true;
    }
}
