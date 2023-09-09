use anyhow::Ok;

use apca::api::v2::positions;
use tracing::error;
use tracing::info;
use tracing::warn;
use tracing_subscriber::filter::FilterExt;

use crate::to_num;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

use num_decimal::Num;

use crate::events::Direction;
use anyhow::bail;
use anyhow::Result;

use sqlx::postgres::PgRow;
use sqlx::FromRow;
use sqlx::Row;

use chrono::DateTime;
use chrono::Utc;

pub mod account;
pub mod assets;
mod db_client;
pub mod locker;
pub mod mktorder;
pub mod mktposition;

use super::data::db_client::DBClient;
use super::data::locker::Locker;
use super::data::locker::LockerStatus;
use super::data::locker::TransactionType;
use super::data::mktorder::MktOrder;
use super::data::mktorder::MktOrders;
use super::data::mktorder::OrderAction;
use super::data::mktposition::MktPosition;
use super::data::mktposition::MktPositions;
use super::web_clients::Connectors;

use crate::Settings;

#[derive(Debug, PartialEq, Clone, Copy, Default)]
pub enum TransactionStatus {
    #[default]
    Waiting,
    Confirmed,
    Cancelled,
    Complete,
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
            "Waiting" => std::result::Result::Ok(TransactionStatus::Waiting),
            "Active" => std::result::Result::Ok(TransactionStatus::Confirmed),
            "Cancelled" => std::result::Result::Ok(TransactionStatus::Cancelled),
            "Complete" => std::result::Result::Ok(TransactionStatus::Complete),
            _ => Err(format!(
                "Failed to parse transaction status, unknown: {}",
                val
            )),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Transaction {
    pub local_id: Uuid,
    pub orders: Vec<Uuid>,
    pub strategy: String,
    pub symbol: String,
    pub locker: Uuid,
    pub entry_time: DateTime<Utc>,
    pub exit_time: DateTime<Utc>,
    pub entry_price: Num,
    pub exit_price: Num,
    pub quantity: Num,
    pub pnl: Num,
    pub roi: Num,
    pub cost_basis: Num,
    pub direction: Direction,
    pub status: TransactionStatus,
}

impl FromRow<'_, PgRow> for Transaction {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        let quantity: i64 = row.try_get("quantity")?;
        let entry_price: f64 = row.try_get("entry_price")?;
        let exit_price: f64 = row.try_get("exit_price")?;
        let pnl: f64 = row.try_get("pnl")?;
        let roi: f64 = row.try_get("roi")?;
        let cost_basis: f64 = row.try_get("cost_basis")?;
        let order_str: &str = row.try_get("orders")?;
        let orders = order_str
            .split(',')
            .map(|x| Uuid::from_str(x).unwrap())
            .collect();

        sqlx::Result::Ok(Self {
            local_id: row.try_get("local_id")?,
            strategy: row.try_get("strategy")?,
            symbol: row.try_get("symbol")?,
            locker: row.try_get("locker")?,
            orders,
            entry_time: row.try_get("entry_time")?,
            exit_time: row.try_get("exit_time")?,
            entry_price: to_num!(entry_price),
            exit_price: to_num!(exit_price),
            quantity: Num::from(quantity),
            pnl: to_num!(pnl),
            roi: to_num!(roi),
            cost_basis: to_num!(cost_basis),
            direction: Direction::from_str(row.try_get("direction")?).unwrap(),
            status: TransactionStatus::from_str(row.try_get("status")?).unwrap(),
        })
    }
}

impl Transaction {
    async fn new(
        symbol: &str,
        strategy: &str,
        direction: Direction,
        entry_price: Num,
        db: &Arc<DBClient>,
    ) -> Result<Self> {
        let mut transaction = Transaction {
            strategy: strategy.to_string(),
            symbol: symbol.to_string(),
            entry_price,
            direction,
            status: TransactionStatus::Waiting,
            ..Default::default()
        };
        transaction.persist_db(db.clone()).await?;
        Ok(transaction)
    }

    fn calculate_roi(&self) -> Num {
        self.pnl.clone() / self.cost_basis.clone() * to_num!(100.00)
    }

    fn get_order_ids(&self) -> Vec<Uuid> {
        self.orders.clone()
    }

    fn update_from_position(&mut self, position: &MktPosition) {
        self.cost_basis = position.cost_basis.clone();
        self.pnl = position.pnl.clone();
        self.roi = self.calculate_roi();
    }

    fn update_from_order(&mut self, order: &MktOrder) {
        match order.action {
            OrderAction::Create => {
                self.entry_time = order.fill_time;
                self.entry_price = order.fill_price.clone();
            }
            OrderAction::Liquidate => {
                self.exit_time = order.fill_time;
                self.exit_price = order.fill_price.clone();
            }
        };
        if !self
            .orders
            .iter()
            .any(|order_id| *order_id == order.local_id)
        {
            info!(
                "Found local ID: {} adding to transactions orders",
                order.local_id
            );
            self.orders.push(order.local_id)
        }
    }

    async fn transaction_complete(
        &mut self,
        order: &MktOrder,
        mktposition: Option<MktPosition>,
        status: TransactionStatus,
        db: &Arc<DBClient>,
    ) {
        self.status = status;
        match status {
            TransactionStatus::Complete => {
                self.exit_price = order.fill_price.clone();
                self.exit_time = order.fill_time;
                if let Some(position) = mktposition {
                    self.cost_basis = position.cost_basis;
                }
            }
            _ => (),
        };
        let _ = self.persist_db(db.clone()).await;
    }

    pub async fn persist_db(&mut self, db: Arc<DBClient>) -> Result<()> {
        let columns = vec![
            "strategy",
            "symbol",
            "locker",
            "orders",
            "entry_time",
            "exit_time",
            "entry_price",
            "exit_price",
            "quantity",
            "pnl",
            "roi",
            "cost_basis",
            "direction",
            "status",
            "local_id",
        ];

        let mut stmt = String::default();
        if Uuid::is_nil(&self.local_id) {
            self.local_id = Uuid::new_v4();
            stmt = db
                .query_builder
                .prepare_insert_statement("transaction", &columns);
        } else {
            stmt = db
                .query_builder
                .prepare_update_statement("transaction", &columns);
        }

        let mut order_string = self
            .orders
            .iter()
            .map(|id| id.to_string() + ",")
            .collect::<String>();

        let _ = order_string.pop();

        if let Err(err) = sqlx::query(&stmt)
            .bind(self.strategy.clone())
            .bind(self.symbol.clone())
            .bind(self.locker)
            .bind(order_string)
            .bind(self.entry_time)
            .bind(self.exit_time)
            .bind(self.entry_price.round_with(3).to_f64())
            .bind(self.exit_price.round_with(3).to_f64())
            .bind(self.quantity.to_i64())
            .bind(self.pnl.round_with(3).to_f64())
            .bind(self.roi.round_with(3).to_f64())
            .bind(self.cost_basis.round_with(3).to_f64())
            .bind(self.direction.to_string())
            .bind(self.status.to_string())
            .bind(self.local_id)
            .execute(&db.pool)
            .await
        {
            error!("Failed to publish to db, error={}", err);
            anyhow::bail!(err)
        }
        Ok(())
    }
}

pub struct Transactions {
    transactions: HashMap<String, Transaction>,
    locker: Locker,
    db: Arc<DBClient>,
    mktorders: MktOrders,
    mktpositions: MktPositions,
}

impl Transactions {
    pub async fn new(settings: &Settings, connectors: &Arc<Connectors>) -> Result<Self> {
        let db = DBClient::new(settings).await?;
        let locker = Locker::new(settings, db.clone());

        let transactions = HashMap::new();
        let mktorders = MktOrders::new(connectors, &db);
        let mktpositions = MktPositions::new(connectors);

        Ok(Transactions {
            transactions,
            locker,
            db,
            mktorders,
            mktpositions,
        })
    }

    pub async fn startup(&mut self) -> Result<()> {
        let columns = vec!["status"];

        async fn fetch_with_status(
            columns: Vec<&str>,
            status: TransactionStatus,
            db: &Arc<DBClient>,
        ) -> Vec<Transaction> {
            let stmt = db
                .query_builder
                .prepare_fetch_statement("transaction", &columns);
            match sqlx::query_as::<_, Transaction>(&stmt)
                .bind(status.to_string())
                .fetch_all(&db.pool)
                .await
            {
                sqlx::Result::Ok(val) => val,
                Err(err) => panic!(
                    "Failed to fetch transactions from db, err={}, closing app",
                    err
                ),
            }
        }

        self.locker.startup().await?;

        let mut db_transact =
            fetch_with_status(columns.clone(), TransactionStatus::Waiting, &self.db).await;
        info!("Pulled {} waiting transactions from db", db_transact.len());

        let orders = Vec::from_iter(db_transact.iter().map(|t| t.orders[0]));
        let orders_count = orders.len();
        let _ = self.mktorders.startup(orders).await?;

        db_transact
            .extend(fetch_with_status(columns, TransactionStatus::Confirmed, &self.db).await);
        let _ = self.mktpositions.update_positions().await?;
        let position_count = db_transact.len() - orders_count;
        info!("Pulled {} confirmed transactions from db", position_count,);

        for transaction in db_transact {
            self.transactions
                .insert(transaction.symbol.clone(), transaction.clone());
        }
        Ok(())
    }

    pub async fn get_subscribed_symbols(&mut self) -> Result<Vec<String>> {
        let mktorders = self.mktorders.update_orders().await?;
        let mut orders: Vec<String> = mktorders.values().map(|o| o.symbol.clone()).collect();

        let mktpositions = self.mktpositions.update_positions().await?;
        let positions: Vec<String> = mktpositions.values().map(|p| p.symbol.clone()).collect();
        orders.extend(positions);
        Ok(orders)
    }

    pub async fn print_active_transactions(&mut self) -> Result<()> {
        info!("Printing active orders");
        let _ = self.mktpositions.update_positions().await?;
        let orders = self.mktorders.update_orders().await?;
        for order in orders.values() {
            info!("{:?}", order);
        }
        info!("Printing confirmed transactions");
        for transaction in self.transactions.values() {
            if let Some(position) = self.mktpositions.get_position(&transaction.symbol) {
                let stop = self.locker.print_stop(&transaction.locker);
                info!("{} {}", position, stop);
            }
        }
        Ok(())
    }

    async fn update_order(&mut self, order_id: Uuid) -> Result<MktOrder> {
        self.mktorders.update_order(&order_id).await
    }

    async fn update_position(&mut self, symbol: &str) -> Result<MktPosition> {
        self.mktpositions.update_position(symbol).await
    }

    pub async fn add_stop(&mut self, symbol: &str, strategy: &str, entry_price: Num) -> Result<()> {
        if let Some(transaction) = self.transactions.get_mut(symbol) {
            info!(
                "Strategy[{}] locker tracking {} at entry_price: {}",
                strategy, symbol, entry_price
            );
            let locker_id = self
                .locker
                .create_new_stop(symbol, strategy, entry_price, TransactionType::Order)
                .await;
            transaction.locker = locker_id;
            transaction.persist_db(self.db.clone()).await?
        } else {
            warn!(
                "Unable to activate locker, transaction not found for symbol: {}",
                symbol
            );
        };
        Ok(())
    }

    pub async fn activate_stop(&mut self, symbol: &str) {
        if let Some(transaction) = self.transactions.get_mut(symbol) {
            self.locker.revive(transaction.locker);
        }
    }

    pub async fn reactivate_stop(&mut self, symbol: &str) {
        if let Some(transaction) = self.transactions.get_mut(symbol) {
            self.locker.revive(transaction.locker);
        }
    }
    pub async fn update_stop_entry_price(&mut self, symbol: &str, entry_price: Num) -> Result<()> {
        if let Some(transaction) = self.transactions.get(symbol) {
            self.locker.update_stop(transaction.locker, entry_price);
        } else {
            warn!(
                "Unable to update locker, transaction not found for symbol: {}",
                symbol
            );
        };
        Ok(())
    }

    pub async fn update_stop_status(&mut self, symbol: &str, status: LockerStatus) -> Result<()> {
        if let Some(transaction) = self.transactions.get(symbol) {
            let locker_id = transaction.locker;
            self.locker.update_status(&locker_id, status)
        } else {
            warn!(
                "Unable to update locker, transaction not found for symbol: {}",
                symbol
            );
        };
        Ok(())
    }

    pub async fn stop_complete(&mut self, symbol: &str) -> Result<()> {
        if let Some(transaction) = self.transactions.get("symbol") {
            self.locker.complete(transaction.locker).await;
        } else {
            warn!(
                "Unable to close locker, transaction not found for symbol: {}",
                symbol
            );
        };
        Ok(())
    }

    pub async fn add_waiting_transaction(
        &mut self,
        symbol: &str,
        strategy: &str,
        direction: Direction,
        entry_price: Num,
    ) -> Result<()> {
        let transaction =
            Transaction::new(symbol, strategy, direction, entry_price, &self.db).await?;
        self.transactions.insert(symbol.to_string(), transaction);
        Ok(())
    }

    pub async fn update_transaction(&mut self, order_id: Uuid) -> Result<()> {
        let order = self.update_order(order_id).await?;
        let symbol = &order.symbol;
        let mktposition = self.update_position(symbol).await;
        if let Some(transaction) = self.transactions.get_mut(symbol) {
            transaction.update_from_order(&order);
            if let anyhow::Result::Ok(position) = mktposition {
                transaction.update_from_position(&position);
            }
            transaction.persist_db(self.db.clone()).await?
        }
        Ok(())
    }

    pub async fn close_transaction(&mut self, order_id: Uuid) -> Result<()> {
        let order = self.mktorders.update_order(&order_id).await?;
        let symbol = &order.symbol;
        if let Some(transaction) = self.transactions.get_mut(symbol) {
            let orders = &transaction.orders;
            assert!(orders
                .into_iter()
                .any(|order_local_id| order.local_id.eq(order_local_id)));
            let position = self.mktpositions.update_position(symbol).await?;
            transaction
                .transaction_complete(
                    &order,
                    Some(position),
                    TransactionStatus::Complete,
                    &self.db,
                )
                .await
        } else {
            bail!(
                "Unable to close transaction, not found for symbol: {}",
                symbol
            );
        }
        Ok(())
    }

    pub async fn cancel_transaction(&mut self, order_id: Uuid) -> Result<()> {
        let order = self.update_order(order_id).await?;
        let symbol = order.symbol.clone();
        if let Some(transaction) = self.transactions.get_mut(&symbol) {
            transaction
                .transaction_complete(&order, None, TransactionStatus::Cancelled, &self.db)
                .await;
            self.locker.complete(transaction.locker).await;
        } else {
            bail!(
                "Unable to cancel transaction, not found for symbol: {}",
                symbol
            );
        }
        Ok(())
    }

    pub async fn get_order(&self, order_id: &Uuid) -> Option<&MktOrder> {
        self.mktorders.get_order(order_id).await
    }

    pub fn get_transaction(&self, symbol: &str) -> Option<&Transaction> {
        self.transactions.get(symbol)
    }

    pub async fn add_order(
        &mut self,
        symbol: &str,
        order_id: Uuid,
        direction: Direction,
        action: OrderAction,
    ) -> Result<()> {
        if let Some(transaction) = self.transactions.get_mut(symbol) {
            let _ = self
                .mktorders
                .add_order(order_id, symbol, &transaction.strategy, direction, action)
                .await?;
            transaction.orders.push(order_id);
            transaction.persist_db(self.db.clone()).await?;
        } else {
            bail!(
                "Could not find transaction for new order with symbol: {}",
                symbol
            )
        }
        info!("New order added for symbol: {}", symbol);
        Ok(())
    }

    pub async fn has_stop_crossed(
        &mut self,
        symbol: &str,
        last_price: &Num,
    ) -> (bool, TransactionType) {
        if let Some(transaction) = self.transactions.get_mut(symbol) {
            let locker_id = transaction.locker;
            let transaction_type = self.locker.get_transaction_type(&locker_id);
            let should_close = self.locker.should_close(&locker_id, last_price).await;
            return (should_close, transaction_type);
        }
        (false, TransactionType::Order)
    }
}
