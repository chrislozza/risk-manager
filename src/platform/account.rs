use apca::api::v2::account;
use apca::Client;
use log::{error, info};
use num_decimal::Num;
use std::sync::Arc;
use std::{thread, time::Duration};
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct AccountDetails {
    client: Arc<Mutex<Client>>,
    account_details: Option<account::Account>,
}

impl AccountDetails {
    pub fn new(client: Arc<Mutex<Client>>) -> AccountDetails {
        AccountDetails {
            client,
            account_details: None,
        }
    }

    pub async fn startup(&mut self) {
        self.refresh_account_details().await
    }

    pub fn equity(&self) -> Num {
        self.account_details.clone().unwrap().equity
    }

    pub fn buying_power(&self) -> Num {
        self.account_details.clone().unwrap().buying_power
    }

    pub async fn refresh_account_details(&mut self) {
        let account_details = match self.request_account_details().await {
            Ok(account) => account,
            Err(err) => {
                panic!("{:?}", err)
            }
        };
        self.account_details = Some(account_details);
    }

    async fn request_account_details(
        &self,
    ) -> Result<account::Account, apca::RequestError<account::GetError>> {
        let mut retry = 5;
        loop {
            match self.client.lock().await.issue::<account::Get>(&()).await {
                Ok(val) => {
                    info!("Account Downloaded {:?}", val);
                    return Ok(val);
                }
                Err(err) => {
                    retry -= 1;
                    if retry == 0 {
                        error!("Failed to post order {}", err);
                        return Err(err);
                    }
                }
            }
            thread::sleep(Duration::from_secs(1));
        }
    }
}
