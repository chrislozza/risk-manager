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
    account: account::Account,
}

impl fmt::Display for AccountDetails {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let account = &self.account;
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
    pub async fn new(connectors: &Arc<Connectors>) -> Result<Self> {
        let account_details = connector.get_account_details().await?;
        Ok(AccountDetails {
            connectors: Arc::clone(connectors),
            account: account_details,
        })
    }

    pub async fn equity(&self) -> Num {
        self.account.equity.clone()
    }

    pub async fn update_account(&mut self) -> Result<()> {
        let details = self.connectors.get_account_details().await?;
        self.account = account_details;
        info!("{self}");
        Ok(())
    }
}
