use anyhow::Ok;

use tracing::info;
use tracing::warn;

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

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TransactionStatus {
    Waiting,
    Active,
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
            "Active" => std::result::Result::Ok(TransactionStatus::Active),
            "Cancelled" => std::result::Result::Ok(TransactionStatus::Cancelled),
            "Complete" => std::result::Result::Ok(TransactionStatus::Complete),
            _ => Err(format!(
                "Failed to parse transaction status, unknown: {}",
                val
            )),
        }
    }
}

#[derive(Debug)]
pub struct Transaction {
    pub local_id: Option<Uuid>,
    pub position: Option<Uuid>,
    pub orders: Vec<Uuid>,
    pub strategy: String,
    pub symbol: String,
    pub locker: Option<Uuid>,
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
        let position: Option<Uuid> = row.try_get("position")?;

        sqlx::Result::Ok(Self {
            local_id: row.try_get("local_id")?,
            strategy: row.try_get("strategy")?,
            symbol: row.try_get("symbol")?,
            locker: row.try_get("locker")?,
            position,
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
    fn new(symbol: &str, strategy: &str, direction: Direction) -> Self {
        Transaction {
            local_id: None,
            locker: None,
            position: None,
            orders: Vec::default(),
            strategy: strategy.to_string(),
            symbol: symbol.to_string(),
            entry_time: DateTime::<Utc>::default(),
            exit_time: DateTime::<Utc>::default(),
            entry_price: to_num!(0.0),
            exit_price: to_num!(0.0),
            quantity: to_num!(0.0),
            pnl: to_num!(0.0),
            roi: to_num!(0.0),
            cost_basis: to_num!(0.0),
            direction,
            status: TransactionStatus::Waiting,
        }
    }

    fn calculate_roi(&self) -> Num {
        self.pnl.clone() / self.cost_basis.clone() * to_num!(100.00)
    }

    fn get_order_ids(&self) -> Vec<Uuid> {
        self.orders.clone()
    }

    fn update_from_position(&mut self, position: &MktPosition) {
        self.cost_basis = position.get_cost_basis();
        self.pnl = position.get_pnl();
        self.roi = self.calculate_roi();
    }

    fn update_from_order(&mut self, order: &MktOrder) {
        match order.get_action() {
            OrderAction::Create => {
                self.entry_time = order.get_fill_time();
                self.entry_price = order.get_fill_price();
            }
            OrderAction::Liquidate => {
                self.exit_time = order.get_fill_time();
                self.exit_price = order.get_fill_price();
            }
        };
    }

    async fn load_from_db(local_id: Uuid, db: Arc<DBClient>) -> Result<Self> {
        let columns = vec!["local_id"];
        let stmt = db
            .query_builder
            .prepare_fetch_statement("transaction", &columns);

        match sqlx::query_as::<_, Transaction>(&stmt)
            .bind(local_id)
            .fetch_one(&db.pool)
            .await
        {
            Err(err) => bail!("Failed to pull data from db, error={}", err),
            core::result::Result::Ok(val) => Ok(val),
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
                self.exit_price = order.get_fill_price();
                self.exit_time = order.get_fill_time();
                if let Some(position) = mktposition {
                    self.cost_basis = position.get_cost_basis();
                }
            }
            _ => (),
        };
        self.persist_db(db.clone());
    }

    async fn persist_db(&mut self, db: Arc<DBClient>) -> Result<()> {
        let columns = vec![
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
        ];

        let stmt = match self.local_id {
            Some(_) => db
                .query_builder
                .prepare_update_statement("transaction", &columns),
            None => {
                self.local_id = Some(Uuid::new_v4());
                db.query_builder
                    .prepare_insert_statement("transaction", &columns)
            }
        };

        if let Err(err) = sqlx::query(&stmt)
            .bind(self.strategy.clone())
            .bind(self.symbol.clone())
            .bind(self.locker)
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
            .fetch_one(&db.pool)
            .await
        {
            bail!("Failed to publish to db, error={}", err)
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
        info!("Loading transactions from db");

        let columns = vec!["status"];
        let stmt = self
            .db
            .query_builder
            .prepare_fetch_statement("transaction", &columns);
        let rows = match sqlx::query(&stmt)
            .bind(TransactionStatus::Active.to_string())
            .fetch_all(&self.db.pool)
            .await
        {
            sqlx::Result::Ok(val) => val,
            Err(err) => panic!(
                "Failed to fetch transactions from db, err={}, closing app",
                err
            ),
        };

        for row in &rows {
            let local_id = row.try_get("local_id")?;
            let transaction = Transaction::load_from_db(local_id, self.db.clone()).await?;
            let symbol = transaction.symbol.clone();
            self.transactions.insert(symbol, transaction);
        }

        info!("Downloading orders and positions");

        self.locker.startup();
        let mktorders = self.mktorders.update_orders().await?;
        for mktorder in mktorders.values() {
            self.locker.create_new_stop(
                mktorder.get_symbol(),
                mktorder.get_strategy(),
                mktorder.get_limit_price(),
                TransactionType::Order,
            );
        }

        let mktpositions = self.mktpositions.update_positions().await?;
        for mktposition in mktpositions.values() {
            self.locker.create_new_stop(
                mktposition.get_symbol(),
                mktposition.get_strategy(),
                mktposition.get_entry_price(),
                TransactionType::Position,
            );
        }
        Ok(())
    }

    pub async fn get_subscribed_symbols(&mut self) -> Result<Vec<String>> {
        let mktorders = self.mktorders.update_orders().await?;
        let mut orders: Vec<String> = mktorders
            .values()
            .map(|o| o.get_symbol().to_string())
            .collect();

        let mktpositions = self.mktpositions.update_positions().await?;
        let positions: Vec<String> = mktpositions
            .values()
            .map(|p| p.get_symbol().to_string())
            .collect();
        orders.extend(positions);
        Ok(orders)
    }

    pub async fn print_active_transactions(&mut self) -> Result<()> {
        // let _ = self.mktorders.update_orders().await;
        // let positions = self
        //     .mktpositions
        //     .update_positions()
        //     .await?;
        // for (symbol, position) in positions {
        //     let symbol = position.get_symbol();
        //     let transaction = match self.get_transaction(symbol) {
        //         Some(val) => val,
        //         None => bail!("Failed to match a transaction for position with symbol"),
        //     };
        //     info!("{} {}", position, self.locker.print_stop(symbol));
        // }
        Ok(())
    }

    async fn update_order(&mut self, order_id: Uuid) -> Result<MktOrder> {
        self.mktorders.update_order(&order_id).await
    }

    async fn update_position(&mut self, symbol: &str) -> Result<MktPosition> {
        self.mktpositions.update_position(symbol).await
    }

    pub async fn activate_stop(
        &mut self,
        symbol: &str,
        strategy: &str,
        entry_price: Num,
    ) -> Result<()> {
        if let Some(transaction) = self.transactions.get_mut(symbol) {
            let locker_id =
                self.locker
                    .create_new_stop(symbol, strategy, entry_price, TransactionType::Order);
            transaction.locker = Some(locker_id);
        } else {
            warn!(
                "Unable to activate locker, transaction not found for symbol: {}",
                symbol
            );
        };

        Ok(())
    }
    pub async fn reactivate_stop(&mut self, symbol: &str) {
        if let Some(transaction) = self.transactions.get_mut(symbol) {
            self.locker.revive(transaction.locker.unwrap());
        }
    }
    pub async fn update_stop_entry_price(&mut self, symbol: &str, entry_price: Num) -> Result<()> {
        if let Some(transaction) = self.transactions.get(symbol) {
            self.locker
                .update_stop(transaction.locker.unwrap(), entry_price);
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
            let locker_id = transaction.locker.unwrap();
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
            self.locker.complete(transaction.locker.unwrap()).await;
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
    ) -> Result<()> {
        let mut transaction = Transaction::new(symbol, strategy, direction);
        transaction.persist_db(self.db.clone());
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
        }
        Ok(())
    }

    pub async fn close_transaction(&mut self, order_id: Uuid) -> Result<()> {
        let order = self.mktorders.update_order(&order_id).await?;
        let symbol = &order.symbol;
        if let Some(transaction) = self.transactions.get_mut(symbol) {
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
        let symbol = order.get_symbol().clone();
        if let Some(transaction) = self.transactions.get_mut(symbol) {
            let _symbol = order.get_symbol();
            transaction
                .transaction_complete(&order, None, TransactionStatus::Cancelled, &self.db)
                .await;
            self.locker.complete(transaction.locker.unwrap()).await;
        } else {
            bail!(
                "Unable to cancel transaction, not found for symbol: {}",
                symbol
            );
        }
        Ok(())
    }

    pub async fn get_order(&self, order_id: &Uuid) -> Result<&MktOrder> {
        self.mktorders.get_order(order_id).await
    }

    pub fn get_transaction(&self, symbol: &str) -> Option<&Transaction> {
        self.transactions.get(symbol)
    }

    pub async fn add_order(
        &mut self,
        strategy: &str,
        symbol: &str,
        order_id: Uuid,
        action: OrderAction,
    ) -> Result<()> {
        if let Some(transaction) = self.transactions.get(symbol) {
            self.mktorders
                .add_order(
                    order_id,
                    MktOrder::new(
                        action,
                        order_id,
                        strategy,
                        symbol,
                        transaction.direction,
                        &self.db,
                    )
                    .await?,
                )
                .await
        } else {
            bail!(
                "Could not find transaction for new order with symbol: {}",
                symbol
            )
        }
    }

    pub async fn has_stop_crossed(
        &mut self,
        symbol: &str,
        last_price: &Num,
    ) -> (bool, TransactionType) {
        if let Some(transaction) = self.transactions.get_mut(symbol) {
            let locker_id = transaction.locker.unwrap();
            let transaction_type = self.locker.get_transaction_type(&locker_id);
            let should_close = self.locker.should_close(&locker_id, last_price);
            return (should_close, transaction_type);
        }
        (false, TransactionType::Order)
    }
}
