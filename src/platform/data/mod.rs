use anyhow::bail;
use anyhow::Ok;
use anyhow::Result;
use chrono::DateTime;
use chrono::Utc;
use num_decimal::Num;
use sqlx::postgres::PgArguments;
use sqlx::postgres::PgRow;
use sqlx::query::Query;
use sqlx::FromRow;
use sqlx::Postgres;
use sqlx::Row;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::debug;
use tracing::info;
use tracing::warn;
use uuid::Uuid;

pub mod account;
pub mod assets;
mod db_client;
mod locker;
pub mod mktorder;
pub mod mktposition;

use super::mktdata::MktData;
use super::mktdata::Snapshot;
use super::web_clients::Connectors;
use crate::events::Direction;
use crate::events::Side;
use crate::to_num;
use assets::Assets;
use db_client::DBClient;
use locker::Locker;
use locker::TransactionType;
use mktorder::MktOrder;
use mktorder::MktOrders;
use mktorder::OrderAction;
use mktorder::OrderStatus;
use mktposition::MktPosition;
use mktposition::MktPositions;

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
            "Confirmed" => std::result::Result::Ok(TransactionStatus::Confirmed),
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
        fn sqlx_to_num(row: &PgRow, value: &str) -> sqlx::Result<Num> {
            match row.try_get::<f64, _>(value) {
                std::result::Result::Ok(val) => std::result::Result::Ok(to_num!(val)),
                Err(err) => Err(err),
            }
        }

        fn get_orders_from_row(order_str: &str) -> Result<Vec<Uuid>> {
            if order_str.is_empty() {
                bail!("Failed to pull orders from DB")
            }
            Ok(order_str
                .split(',')
                .map(|x| Uuid::from_str(x).unwrap())
                .collect())
        }

        sqlx::Result::Ok(Self {
            local_id: row.try_get("local_id")?,
            strategy: row.try_get("strategy")?,
            symbol: row.try_get("symbol")?,
            locker: row.try_get("locker")?,
            orders: get_orders_from_row(row.try_get("orders")?).unwrap(),
            entry_time: row.try_get("entry_time")?,
            exit_time: row.try_get("exit_time")?,
            entry_price: sqlx_to_num(row, "entry_price")?,
            exit_price: sqlx_to_num(row, "exit_price")?,
            quantity: Num::from(row.try_get::<i64, &str>("quantity")?),
            pnl: sqlx_to_num(row, "pnl")?,
            roi: sqlx_to_num(row, "roi")?,
            cost_basis: sqlx_to_num(row, "cost_basis")?,
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

    async fn update_from_position(&mut self, position: &MktPosition, db: &Arc<DBClient>) {
        self.cost_basis = position.cost_basis.clone();
        self.pnl = position.pnl.clone();
        self.roi = self.calculate_roi();
        let _ = self.persist_db(db.clone()).await;
    }

    async fn update_from_order(&mut self, order: &MktOrder, db: &Arc<DBClient>) {
        match order.action {
            OrderAction::Create => {
                if order.status.eq(&OrderStatus::Filled) {
                    self.entry_time = order.fill_time;
                    self.entry_price = order.fill_price.clone();
                    self.quantity = order.quantity.clone();
                    if self.status.eq(&TransactionStatus::Waiting) {
                        self.status = TransactionStatus::Confirmed;
                    }
                }
            }
            OrderAction::Liquidate => {
                if order.status.eq(&OrderStatus::Filled) {
                    self.exit_time = order.fill_time;
                    self.exit_price = order.fill_price.clone();
                }
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
        let _ = self.persist_db(db.clone()).await;
    }

    async fn cancel(&mut self, order: &MktOrder, db: &Arc<DBClient>) {
        self.status = TransactionStatus::Cancelled;
        self.update_from_order(order, db).await;
    }

    async fn zombie(&mut self, db: &Arc<DBClient>) {
        self.status = TransactionStatus::Cancelled;
        let _ = self.persist_db(db.clone()).await;
    }

    async fn complete(
        &mut self,
        order: &MktOrder,
        position: Option<&MktPosition>,
        db: &Arc<DBClient>,
    ) {
        self.status = TransactionStatus::Complete;
        self.update_from_order(order, db).await;
        if let Some(position) = position {
            self.update_from_position(position, db).await;
        }
    }

    fn build_query<'a>(
        &'a self,
        stmt: &'a str,
        order_string: &'a str,
    ) -> Query<'_, Postgres, PgArguments> {
        sqlx::query(stmt)
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

        fn get_sql_stmt(local_id: &Uuid, columns: Vec<&str>, db: &Arc<DBClient>) -> String {
            if Uuid::is_nil(local_id) {
                db.query_builder
                    .prepare_insert_statement("transaction", &columns)
            } else {
                db.query_builder
                    .prepare_update_statement("transaction", &columns)
            }
        }

        let stmt = get_sql_stmt(&self.local_id, columns, &db);
        if Uuid::is_nil(&self.local_id) {
            self.local_id = Uuid::new_v4();
        }

        let mut order_string = self
            .orders
            .iter()
            .map(|id| id.to_string() + ",")
            .collect::<String>();

        let _ = order_string.pop();

        if let Err(err) = self
            .build_query(&stmt, &order_string)
            .execute(&db.pool)
            .await
        {
            bail!("Locker failed to publish to db, error={}", err)
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
    assets: Assets,
}

impl Transactions {
    pub async fn new(
        settings: &Settings,
        connectors: &Arc<Connectors>,
        mktdata: &Arc<Mutex<MktData>>,
    ) -> Result<Self> {
        let db = DBClient::new(settings).await?;
        let locker = Locker::new(settings, db.clone(), mktdata);

        let transactions = HashMap::new();
        let mktorders = MktOrders::new(connectors, &db);
        let mktpositions = MktPositions::new(connectors);
        let assets = Assets::new(connectors).await;

        Ok(Transactions {
            transactions,
            locker,
            db,
            mktorders,
            mktpositions,
            assets,
        })
    }

    pub async fn startup(&mut self) -> Result<()> {
        self.assets.startup().await?;
        let columns = vec!["status"];

        async fn fetch_with_status(
            columns: Vec<&str>,
            statuses: Vec<TransactionStatus>,
            db: &Arc<DBClient>,
        ) -> Vec<Transaction> {
            let mut results = Vec::new();
            for status in statuses {
                let stmt = db
                    .query_builder
                    .prepare_fetch_statement("transaction", &columns);
                let rs = match sqlx::query_as::<_, Transaction>(&stmt)
                    .bind(status.to_string())
                    .fetch_all(&db.pool)
                    .await
                {
                    sqlx::Result::Ok(val) => val,
                    Err(err) => panic!(
                        "Failed to fetch transactions from db, err={}, closing app",
                        err
                    ),
                };
                results.extend(rs);
            }
            results
        }

        let transactions = fetch_with_status(
            columns.clone(),
            vec![TransactionStatus::Waiting, TransactionStatus::Confirmed],
            &self.db,
        )
        .await;

        let mut orders = 0;
        let mut positions = 0;
        for mut transaction in transactions {
            match transaction.status {
                TransactionStatus::Waiting => {
                    let mktorder = &self
                        .mktorders
                        .load_from_db(&[transaction.orders[0]])
                        .await?[0];
                    match mktorder.status {
                        OrderStatus::Cancelled => {
                            transaction.cancel(mktorder, &self.db).await;
                            self.locker.complete(transaction.locker).await;
                            continue;
                        }
                        OrderStatus::Filled => {
                            transaction.update_from_order(mktorder, &self.db).await;
                            self.mktpositions.add_position(
                                &transaction.strategy,
                                &transaction.symbol,
                                transaction.direction,
                            )
                        }
                        _ => (),
                    }
                    orders += 1;
                }
                TransactionStatus::Confirmed => {
                    let symbol = &transaction.symbol;
                    self.mktpositions.add_position(
                        &transaction.strategy,
                        symbol,
                        transaction.direction,
                    );
                    let order_ids = &transaction.orders[1..];
                    let orders = self.mktorders.load_from_db(order_ids).await?;
                    if !order_ids.is_empty() && orders.is_empty() {
                        transaction.zombie(&self.db).await;
                        continue;
                    }

                    let filled_quantity: i64 = orders
                        .iter()
                        .map(|order| {
                            if order.status.eq(&OrderStatus::Filled) {
                                order.quantity.to_i64().unwrap()
                            } else {
                                0_i64
                            }
                        })
                        .sum();
                    if transaction.quantity == Num::from(filled_quantity) {
                        transaction
                            .complete(orders.last().unwrap(), None, &self.db)
                            .await;
                        self.locker.complete(transaction.locker).await;
                        continue;
                    }
                    positions += 1;
                }
                _ => (),
            }
            self.transactions
                .insert(transaction.symbol.clone(), transaction);
        }
        info!(
            "Loaded {} positions and {} orders from db",
            positions, orders
        );
        self.locker.startup().await?;

        Ok(())
    }

    pub async fn get_subscribed_symbols(&mut self) -> Result<Vec<String>> {
        let mktorders = self.mktorders.update_orders().await?;
        let mut orders: Vec<String> = mktorders.values().map(|o| o.symbol.clone()).collect();

        let mktpositions = self.mktpositions.update_positions().await?;
        let positions: Vec<String> = mktpositions.into_iter().map(|p| p.symbol.clone()).collect();
        orders.extend(positions);
        Ok(orders)
    }

    pub async fn print_active_transactions(&mut self) -> Result<()> {
        let _ = self.mktorders.update_orders().await?;

        for transaction in &mut self.transactions.values_mut() {
            match transaction.status {
                TransactionStatus::Cancelled | TransactionStatus::Waiting => {
                    if let Some(order) = self
                        .mktorders
                        .get_order(transaction.orders.first().unwrap())
                    {
                        if order.status == OrderStatus::New {
                            let stop = self.locker.print_stop(&transaction.locker);
                            info!("{} {}", order, stop);
                        }
                    }
                }
                _ => {
                    let symbol = transaction.symbol.as_str();
                    if let anyhow::Result::Ok(position) = self
                        .mktpositions
                        .update_position(symbol, self.assets.get_exchange(symbol))
                        .await
                    {
                        let stop = self.locker.print_stop(&transaction.locker);
                        info!("{} {}", position, stop);
                        transaction.update_from_position(&position, &self.db).await;
                        self.locker
                            .start_tracking_position(transaction.locker)
                            .await?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn count_capacity(&self, strategy: &str) -> usize {
        self.transactions
            .values()
            .filter(|transaction| {
                transaction.strategy == strategy
                    && (transaction.status == TransactionStatus::Waiting
                        || transaction.status == TransactionStatus::Confirmed)
            })
            .count()
    }

    async fn update_order(&mut self, order_id: Uuid) -> Result<MktOrder> {
        self.mktorders.update_order(&order_id).await
    }

    pub async fn add_stop(
        &mut self,
        symbol: &str,
        strategy: &str,
        entry_price: Num,
        direction: Direction,
    ) -> Result<()> {
        if let Some(transaction) = self.transactions.get_mut(symbol) {
            info!(
                "Strategy[{}] locker tracking {} at entry_price: {}",
                strategy, symbol, entry_price
            );
            let locker_id = self
                .locker
                .create_new_stop(
                    symbol,
                    strategy,
                    entry_price,
                    TransactionType::Order,
                    direction,
                )
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
            self.locker.activate(transaction.locker).await;
            info!("Locker tracking symbol: {} activated", symbol);
        } else {
            warn!(
                "Unable to update locker, transaction not found for symbol: {}",
                symbol
            );
        }
    }

    pub async fn stop_complete(&mut self, symbol: &str) {
        if let Some(transaction) = self.transactions.get(symbol) {
            self.locker.complete(transaction.locker).await;
        } else {
            warn!(
                "Unable to close locker, transaction not found for symbol: {}",
                symbol
            );
        };
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
        info!(
            "Strategy[{}] symbol[{}] added a waiting transaction",
            strategy, symbol
        );
        Ok(())
    }

    pub fn get_assets(&self) -> &Assets {
        &self.assets
    }

    pub fn get_transaction(&self, symbol: &str) -> Option<&Transaction> {
        self.transactions.get(symbol)
    }

    pub async fn confirm_transaction(&mut self, order_id: Uuid) -> Result<()> {
        let order = self.update_order(order_id).await?;
        let symbol = order.symbol.clone();
        if let Some(transaction) = self.transactions.get_mut(&symbol) {
            transaction.update_from_order(&order, &self.db).await;
            info!(
                "Strategy[{}] symbol[{}], position confirmed",
                transaction.strategy, transaction.symbol
            );
            self.locker
                .start_tracking_position(transaction.locker)
                .await?;
            self.mktpositions.add_position(
                &transaction.strategy,
                &transaction.symbol,
                transaction.direction,
            );
        }
        Ok(())
    }

    pub async fn close_transaction(&mut self, order_id: Uuid) -> Result<()> {
        let order = self.mktorders.update_order(&order_id).await?;
        let symbol = &order.symbol;
        if let Some(transaction) = self.transactions.get_mut(symbol) {
            let orders = &transaction.orders;
            assert!(orders
                .iter()
                .any(|order_local_id| order.local_id.eq(order_local_id)));
            info!("Closing transaction, before getting position from broker");
            let position = self
                .mktpositions
                .update_position(symbol, self.assets.get_exchange(symbol))
                .await?;
            transaction
                .complete(&order, Some(&position), &self.db)
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
        info!("Transaction cancelled for symbol: {}", symbol);
        if let Some(transaction) = self.transactions.get_mut(&symbol) {
            transaction.cancel(&order, &self.db).await;
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
        self.mktorders.get_order(order_id)
    }

    pub async fn add_order(
        &mut self,
        symbol: &str,
        order_id: Uuid,
        side: Side,
        direction: Direction,
        action: OrderAction,
    ) -> Result<()> {
        if let Some(transaction) = self.transactions.get_mut(symbol) {
            let _ = self
                .mktorders
                .add_order(
                    order_id,
                    symbol,
                    &transaction.strategy,
                    side,
                    direction,
                    action,
                )
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

    pub async fn find_transactions_to_close(
        &mut self,
        snapshots: &HashMap<String, Snapshot>,
    ) -> Vec<Transaction> {
        let mut to_close: Vec<Transaction> = Vec::new();
        for (_, transaction) in self.transactions.clone() {
            let symbol = &transaction.symbol;
            debug!(
                "Checking has stop crossed before has transaction type symbol: {}",
                symbol
            );
            if let Some(snapshot) = snapshots.get(symbol) {
                match self
                    .locker
                    .should_close(symbol, &transaction.locker, snapshot)
                    .await
                {
                    anyhow::Result::Ok(result) => {
                        if result {
                            to_close.push(transaction.clone());
                        }
                    }
                    anyhow::Result::Err(err) => {
                        warn!("Locker check failed, err={err}");
                        let _ = self
                            .cancel_transaction(*transaction.orders.first().unwrap())
                            .await;
                    }
                }
            }
        }
        to_close
    }
}
