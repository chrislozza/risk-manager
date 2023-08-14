use tracing::info;

use std::time::SystemTime;
use crate::to_num;
use crate::utils::to_sql_type;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use tokio_postgres::types::ToSql;
use tokio_postgres::Row;
use uuid::Uuid;

use num_decimal::Num;

use super::Settings;
use crate::events::Direction;
use anyhow::bail;
use anyhow::Result;

use chrono::DateTime;
use chrono::Utc;

pub mod account;
pub mod mktorder;
pub mod mktposition;

use super::data::mktorder::MktOrder;
use super::data::mktposition::MktPosition;
use super::db_client::DBClient;
use super::locker::Locker;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TransactionStatus {
    Active,
    Cancelled,
    Closed,
}

impl fmt::Display for TransactionStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl FromStr for TransactionStatus {
    type Err = String;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        match val {
            "Active" => Ok(TransactionStatus::Active),
            "Cancelled" => Ok(TransactionStatus::Cancelled),
            "Closed" => Ok(TransactionStatus::Closed),
            _ => Err(format!(
                "Failed to parse transaction status, unknown: {}",
                val
            )),
        }
    }
}

#[derive(Debug)]
struct Transaction {
    local_id: Uuid,
    strategy: String,
    symbol: String,
    locker: Uuid,
    entry_time: DateTime<Utc>,
    exit_time: Option<DateTime<Utc>>,
    entry_price: Num,
    exit_price: Option<Num>,
    quantity: Num,
    pnl: Num,
    roi: Num,
    cost_basis: Num,
    direction: Direction,
    status: TransactionStatus,
}

impl Transaction {
    fn new(locker: Uuid, order: &MktOrder, db: &Arc<DBClient>) -> Self {
        let transaction = Transaction {
            local_id: Uuid::new_v4(),
            strategy: order.get_strategy().to_string(),
            symbol: order.get_symbol().to_string(),
            locker,
            entry_time: order.get_fill_time(),
            exit_time: None,
            entry_price: order.get_fill_price(),
            exit_price: None,
            quantity: order.get_quantity(),
            pnl: to_num!(0.0),
            roi: to_num!(0.0),
            cost_basis: to_num!(0.0),
            direction: order.get_direction().clone(),
            status: TransactionStatus::Active,
        };
        transaction.write_to_db(db);
        transaction
    }

    fn load_from_db(row: &Row) -> Self {
        let local_id = row.get::<&str, Uuid>("local_id");
        let strategy = row.get::<&str, String>("strategy");
        let symbol = row.get::<&str, String>("symbol");
        let locker = row.get::<&str, Uuid>("locker");
        let entry_time = row.get::<&str, DateTime<Utc>>("entry_time");
        let entry_price = to_num!(row.get::<&str, f64>("entry_price"));
        let quantity = Num::from(row.get::<&str, i64>("quantity"));
        let pnl = to_num!(row.get::<&str, f64>("pnl"));
        let roi = to_num!(row.get::<&str, f64>("roi"));
        let cost_basis = to_num!(row.get::<&str, f64>("cost_basis"));
        let direction = Direction::from_str(row.get::<&str, &str>("direction")).unwrap();
        Transaction {
            local_id,
            strategy,
            symbol,
            locker,
            entry_time,
            exit_time: None,
            entry_price,
            exit_price: None,
            quantity,
            pnl,
            roi,
            cost_basis,
            direction,
            status: TransactionStatus::Active,
        }
    }

    async fn transaction_complete(&mut self, order: MktOrder, position: MktPosition, status: TransactionStatus, db: &Arc<DBClient>) -> Result<()> {
        self.exit_price = Some(order.get_fill_price());
        self.exit_time = Some(order.get_fill_time());
        self.cost_basis = position.get_cost_basis();
        self.pnl = position.get_pnl();
        self.status = status;
        self.write_to_db(db).await
    }

    async fn write_to_db(&self, db: &Arc<DBClient>) -> Result<()> {
        let (columns, values) = self.serialise()?;
        tokio::spawn(async move {
            if let Err(err) = db.clone().update("transaction", columns, values).await {
                bail!("Failed to write to db in transaction");
            }
            Ok(())
        });
        Ok(())
    }

    fn serialise(&self) -> Result<(Vec<&str>, Vec<Box<dyn ToSql + Sync>>)> {
        let exit_time = match self.exit_time {
            Some(val) => val,
            None => SystemTime::now().into()
        };
        let exit_price = match &self.exit_price {
            Some(val) => val.round_with(3).to_f64().unwrap(),
            None => 0.0,
        };
        Ok((vec![
            "strategy",
            "symbol",
            "locker",
            "entry_time",
            "exit_time",
            "entry_price",
            "exit_price",
            "quantity",
            "pnl",
            "rol",
            "cost_basis",
            "direction",
            "status",
            "local_id",
        ],
        vec![
            to_sql_type(self.strategy.clone()),
            to_sql_type(self.symbol.clone()),
            to_sql_type(self.locker),
            to_sql_type(self.entry_time),
            to_sql_type(exit_time),
            to_sql_type(self.entry_price.round_with(3).to_f64()),
            to_sql_type(exit_price),
            to_sql_type(self.quantity.to_i64()),
            to_sql_type(self.pnl.round_with(3).to_f64()),
            to_sql_type(self.roi.round_with(3).to_f64()),
            to_sql_type(self.cost_basis.round_with(3).to_f64()),
            to_sql_type(self.direction.to_string()),
            to_sql_type(self.status.to_string()),
            to_sql_type(self.local_id),
        ]))
    }
}

pub struct Transactions {
    transactions: HashMap<String, Transaction>,
}

impl Transactions {
    pub async fn new(db: &Arc<DBClient>, locker: &mut Locker) -> Self {
        let rows = match db.fetch("transaction", vec!["status"], vec![to_sql_type(TransactionStatus::Active.to_string())]).await {
            Ok(val) => val,
            Err(err) => panic!("Failed to fetch transactions from db, closing app"),
        };
        let mut transactions = HashMap::new();

        for row in &rows {
            let transaction = Transaction::load_from_db(row);
            let symbol = transaction.symbol.clone();
            locker.load_stop_from_db(&transaction.symbol, transaction.locker);
            transactions.insert(symbol, transaction);
        }

        Transactions {
            transactions
        }
    }

    pub async fn print_active_transactions(&self, positions: Vec<MktPosition>, locker: &mut Locker) -> Result<()> {
        for position in positions {
            let symbol = position.get_symbol();
            let transaction = match self.get_transaction(symbol) {
                Some(val) => val,
                None => bail!("Failed to match a transaction for position with symbol"),
            };
            info!("{} {}", position, locker.print_stop(symbol));
        }
        Ok(())
    }

    pub async fn add_transaction(&mut self, locker: Uuid, order: MktOrder, db: &Arc<DBClient>) -> Result<()> {
        let transaction = Transaction::new(locker, &order, db);
        self.transactions.insert(order.get_symbol().to_string(), transaction);
        Ok(())
    }

    pub async fn close_transaction(&mut self, order: MktOrder, position: MktPosition, db: &Arc<DBClient>) -> Result<()> {
        let symbol = order.get_symbol();
        if let Some(transaction) = self.transactions.get_mut(symbol) {
            transaction.transaction_complete(order, position, TransactionStatus::Closed, db).await
        } else {
            bail!("Transaction key not found in HashMap")
        }
    }

    pub async fn cancel_transaction(&mut self, order: MktOrder, position: MktPosition, db: &Arc<DBClient>) -> Result<()> {
        let symbol = order.get_symbol();
        if let Some(transaction) = self.transactions.get_mut(symbol) {
            transaction.transaction_complete(order, position, TransactionStatus::Cancelled, db).await
        } else {
            bail!("Transaction key not found in HashMap")
        }
    }

    pub fn get_transactions(&self) -> &HashMap<String, Transaction> {
        &self.transactions
    }

    pub fn get_transaction(&self, symbol: &str) -> Option<&Transaction> {
        self.transactions.get(symbol)
    }
}
