use apca::api::v2::account;
use num_decimal::Num;
use std::fmt;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::error;
use tracing::info;
use tracing::warn;

use anyhow::bail;
use anyhow::Result;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct AccountDetails {
    connectors: Arc<Connectors>,
    account: RwLock<account::Account>,
}

impl fmt::Display for AccountDetails {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let account = &self.account.read().await;
        write!(
            f,
            "Account id[{}], equity[{}], cash[{}], buying_power[{}] is_margin[{}]",
            account.id.to_string(),
            account.equity,
            account.cash,
            account.buying_power,
            account.shorting_enabled
        )
    }
}

impl AccountDetails {
    pub fn new(connectors: &Arc<Connectors>) -> Result<Self> {
        let account_details = connector.get_account_details().await?;
        Ok(AccountDetails {
            connectors: Arc::clone(connectors),
            account: account_details,
        })
    }

    pub async fn equity(&self) -> Num {
        self.account.read().await.equity
    }

    pub async fn update_account(&mut self) {
        let details = self.connector.get_account_details().await?;
        let account = self.account.write().await;
        account = account_details;
        info!("{self}");
    }

    async fn pull_account_details(connector: &Connectors) -> Result<account::Account> {}
}
