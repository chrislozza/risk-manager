use apca::api::v2::account;
use apca::Client;
use num_decimal::Num;
use std::fmt;
use std::sync::Arc;
use std::{thread, time::Duration};
use tokio::sync::Mutex;
use tracing::{error, info};

use anyhow::{bail, Result};

#[derive(Debug)]
pub struct AccountDetails {
    client: Arc<Mutex<Client>>,
    account_details: Option<account::Account>,
}

impl fmt::Display for AccountDetails {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(account) = &self.account_details {
            write!(
                f,
                "Account id[{}], equity[{}], cash[{}], buying_power[{}]",
                *account.id, account.equity, account.cash, account.buying_power
            )
        } else {
            write!(f, "No details available")
        }
    }
}

impl AccountDetails {
    pub fn new(client: Arc<Mutex<Client>>) -> AccountDetails {
        AccountDetails {
            client,
            account_details: None,
        }
    }

    pub async fn startup(&mut self) -> Result<()> {
        self.refresh_account_details().await
    }

    pub fn equity(&self) -> Num {
        self.account_details.clone().unwrap().equity
    }

    pub fn buying_power(&self) -> Num {
        self.account_details.clone().unwrap().buying_power
    }

    pub async fn refresh_account_details(&mut self) -> Result<()> {
        let account_details = match self.request_account_details().await {
            Ok(account) => account,
            Err(err) => {
                bail!("{:?}", err);
            }
        };
        self.account_details = Some(account_details);
        info!("{self}");
        Ok(())
    }

    async fn request_account_details(
        &self,
    ) -> Result<account::Account, apca::RequestError<account::GetError>> {
        let mut retry = 5;
        loop {
            match self.client.lock().await.issue::<account::Get>(&()).await {
                Ok(val) => {
                    return Ok(val);
                }
                Err(err) => {
                    retry -= 1;
                    if retry == 0 {
                        error!("Get request failed, error: {err}");
                        return Err(err);
                    }
                }
            }
            thread::sleep(Duration::from_secs(1));
        }
    }
}
