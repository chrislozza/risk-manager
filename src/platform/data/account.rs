use super::super::web_clients::Connectors;
use anyhow::Result;
use apca::api::v2::account;
use num_decimal::Num;
use std::fmt;
use std::sync::Arc;
use tracing::info;

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
            "Account id[{}], equity[{}], cash[{}], buying power[{}] is margin[{}] pdt count[{}]",
            *account.id,
            account.equity,
            account.cash,
            account.buying_power,
            account.shorting_enabled,
            account.daytrade_count
        )
    }
}

impl AccountDetails {
    pub async fn new(connectors: &Arc<Connectors>) -> Result<Self> {
        let account_details = connectors.get_account_details().await?;
        Ok(AccountDetails {
            connectors: Arc::clone(connectors),
            account: account_details,
        })
    }

    pub async fn equity(&self) -> Num {
        self.account.equity.clone()
    }

    pub async fn update_account(&mut self) -> Result<()> {
        let account_details = self.connectors.get_account_details().await?;
        self.account = account_details;
        info!("{self}");
        Ok(())
    }
}
