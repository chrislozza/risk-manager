use apca::api::v2::account::{Account, Get};
use apca::ApiError;
use apca::Client;
use log::{error, info};

#[derive(Debug)]
pub struct AccountDetails {
    account_details: Account,
}

impl AccountDetails {
    pub async fn new(client: &Client) -> AccountDetails {
        let account = match Self::request_account_details(client).await {
            Ok(account) => account,
            Err(err) => { error!("Failed to authenticate, error: {err}");
                panic!("Failed to startup, shutting down")
            }
        };
        return account
    }

    async fn request_account_details(client: &Client) -> Result<Self, ApiError> {
        let account_details = client.issue::<Get>(&()).await.unwrap();
        Ok(AccountDetails { account_details })
    }
}
