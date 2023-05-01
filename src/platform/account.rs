use apca::api::v2::account;
use apca::Client;
use log::{error, info};
use std::{thread, time::Duration};

#[derive(Debug)]
pub struct AccountDetails {
    account_details: Option<account::Account>,
}

impl AccountDetails {
    pub fn new() -> AccountDetails {
        AccountDetails {
            account_details: None,
        }
    }

    pub async fn startup(&mut self, client: &Client) {
        let account_details = match self.request_account_details(client).await {
            Ok(account) => account,
            Err(err) => {
                panic!("{:?}", err)
            }
        };
        self.account_details = Some(account_details);
    }

    pub async fn request_account_details(
        &self,
        client: &Client,
    ) -> Result<account::Account, apca::RequestError<account::GetError>> {
        loop {
            let mut retry = 5;
            match client.issue::<account::Get>(&()).await {
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
