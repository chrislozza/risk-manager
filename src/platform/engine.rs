use anyhow::Ok;

use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::interval;
use tokio::time::Duration;

use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use apca::api::v2::updates;
use apca::data::v2::stream;

use num_decimal::Num;

use anyhow::bail;
use anyhow::Result;

use tokio::sync::broadcast::Receiver;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::super::events::MktSignal;
use super::data::account::AccountDetails;
use super::data::assets::Assets;
use super::data::locker::LockerStatus;
use super::data::locker::TransactionType;

use super::data::mktorder::OrderAction;

use super::data::Transactions;
use super::mktdata::MktData;
use super::order_handler::OrderHandler;
use super::risk_sizing::RiskManagement;
use super::web_clients::Connectors;
use super::Event;
use super::Settings;

use crate::to_num;

pub struct Engine {
    settings: Settings,
    account: AccountDetails,
    assets: Assets,
    mktdata: MktData,
    order_handler: OrderHandler,
    transactions: Transactions,
    connectors: Arc<Connectors>,
}

impl Engine {
    pub async fn new(
        settings: Settings,
        key: &str,
        secret: &str,
        is_live: bool,
        shutdown_signal: CancellationToken,
    ) -> Result<Arc<Mutex<Self>>> {
        let connectors = Connectors::new(key, secret, is_live, shutdown_signal)?;
        let account = AccountDetails::new(&connectors).await?;
        let assets = Assets::new(&connectors).await?;
        let order_handler = OrderHandler::new(&connectors);
        let mktdata = MktData::new(&connectors);
        let transactions = Transactions::new(&settings, &connectors).await?;
        Ok(Arc::new(Mutex::new(Engine {
            settings,
            account,
            assets,
            mktdata,
            order_handler,
            transactions,
            connectors,
        })))
    }

    pub async fn startup(&mut self) -> Result<()> {
        info!("Downloading orders and positions in engine startup");
        self.transactions.startup().await?;
        self.assets.startup().await
    }

    pub async fn create_position(&mut self, mkt_signal: &MktSignal) -> Result<()> {
        if !self
            .assets
            .check_if_assest_is_tradable(&mkt_signal.symbol, mkt_signal.direction)
        {
            warn!(
                "Mktsignal with symbol: {} ignored, failed tradable check",
                mkt_signal.symbol
            );
            return Ok(());
        }
        let strategy_cfg = match self
            .settings
            .strategies
            .configuration
            .get(&mkt_signal.strategy)
        {
            Some(strategy) => strategy,
            _ => {
                info!("Not subscribed to strategy: {}", mkt_signal.strategy);
                return Ok(());
            }
        };
        let target_price = to_num!(mkt_signal.price);
        let size = Self::size_position(
            &mkt_signal.symbol,
            &self.account.equity().await,
            strategy_cfg.trailing_size,
            self.settings.strategies.configuration.len(),
            &self.mktdata,
        )
        .await?;
        let symbol = &mkt_signal.symbol;
        let strategy = &mkt_signal.strategy;
        let direction = mkt_signal.direction;
        info!(
            "Stragegy[{}], Symbol[{}], create a waiting transaction",
            strategy, symbol
        );
        if let Err(err) = self
            .transactions
            .add_waiting_transaction(symbol, strategy, direction)
            .await
        {
            bail!("Failed to add waiting transaction, error={}", err)
        };
        if let anyhow::Result::Ok(order_id) = self
            .order_handler
            .create_position(&mkt_signal.symbol, target_price, size, mkt_signal.side)
            .await
        {
            return self
                .transactions
                .add_order(&symbol, order_id, direction, OrderAction::Create)
                .await;
        }
        bail!(
            "Failed to create new position for symbol: {}",
            mkt_signal.symbol,
        )
    }

    pub async fn update_status(&mut self) -> Result<()> {
        let _ = self.account.update_account().await;
        self.transactions.print_active_transactions().await
    }

    async fn size_position(
        symbol: &str,
        total_equity: &Num,
        multiplier: f64,
        number_of_strategies: usize,
        mktdata: &MktData,
    ) -> Result<Num> {
        let risk_tolerance = to_num!(0.02);
        let total_equity_per_strategy = total_equity / number_of_strategies;
        let risk_per_trade = total_equity_per_strategy * risk_tolerance;
        let atr = RiskManagement::get_atr(symbol, mktdata).await?;
        let atr_stop = atr.clone() * to_num!(multiplier);
        let position_size = risk_per_trade / atr_stop.clone();
        info!("Position size: {position_size} from equity: {total_equity} with atr: {atr}, atr_stop: {atr_stop}");
        Ok(position_size)
    }

    pub async fn mktdata_update(&mut self, mktdata_update: &stream::Trade) {
        self.mktdata.capture_data(mktdata_update);
    }

    pub async fn mktdata_publish(&mut self) {
        let snapshots = self.mktdata.get_snapshots();
        for (symbol, last_price) in &snapshots {
            let (has_crossed, transaction_type) =
                self.transactions.has_stop_crossed(symbol, last_price).await;
            if !has_crossed {
                continue;
            }
            if let Some(transaction) = self.transactions.get_transaction(symbol) {
                match transaction_type {
                    TransactionType::Order => {
                        let order_id = transaction.orders[0];
                        let stop_status = self.handle_cancel(symbol, order_id).await.unwrap();
                        if let Err(err) = self
                            .transactions
                            .update_stop_status(symbol, stop_status)
                            .await
                        {
                            warn!("Failed to update stop status, error={}", err);
                        }
                    }
                    TransactionType::Position => {
                        match self.handle_liquidate(symbol).await {
                            Some(order_id) => {
                                let strategy = transaction.strategy.clone();
                                let direction = transaction.direction.clone();
                                if let Err(err) = self
                                    .transactions
                                    .add_order(symbol, order_id, direction, OrderAction::Liquidate)
                                    .await
                                {
                                    warn!("Failed to add stop order, error={}", err);
                                }
                                if let Err(err) = self
                                    .transactions
                                    .update_stop_status(symbol, LockerStatus::Finished)
                                    .await
                                {
                                    warn!("Failed to update stop status, error={}", err);
                                }
                            }
                            _ => {
                                if let Err(err) = self
                                    .transactions
                                    .update_stop_status(symbol, LockerStatus::Active)
                                    .await
                                {
                                    warn!("Failed to update stop status, error={}", err);
                                }
                            }
                        };
                    }
                };
            }
        }
    }

    async fn handle_cancel(&self, symbol: &str, order_id: Uuid) -> Result<LockerStatus> {
        info!("In handle new for symbol: {symbol}");
        match self.order_handler.cancel_order(&order_id).await {
            Err(error) => {
                error!("Dropping order cancel, failed to send to server, error={error}");
                Ok(LockerStatus::Active)
            }
            _ => Ok(LockerStatus::Finished),
        }
    }

    async fn handle_liquidate(&self, symbol: &str) -> Option<Uuid> {
        info!("In handle new for symbol: {symbol}");
        match self.order_handler.liquidate_position(symbol).await {
            Err(error) => {
                error!("Dropping liquidate, failed to send to server, error={error}");
                None
            }
            std::result::Result::Ok(order_id) => Some(order_id),
        }
    }

    pub async fn order_update(&mut self, order_update: &updates::OrderUpdate) -> Result<()> {
        let order_id = order_update.order.id.0;
        match order_update.event {
            updates::OrderStatus::New => {
                self.handle_new(order_id).await?;
            }
            updates::OrderStatus::Filled => {
                self.handle_fill(order_id).await?;
            }
            updates::OrderStatus::Canceled => {
                self.handle_cancel_reject(order_id).await?;
            }
            _ => info!("Not listening to event {0:?}", order_update.event),
        }
        Ok(())
    }

    async fn handle_cancel_reject(&mut self, order_id: Uuid) -> Result<()> {
        let order = self.transactions.get_order(&order_id).await?;
        let symbol = order.get_symbol().to_string();
        info!("In handle cancel reject for symbol: {}", symbol);

        let action = order.get_action();

        match action {
            OrderAction::Create => {
                self.transactions.cancel_transaction(order_id).await?;
            }
            OrderAction::Liquidate => self.transactions.reactivate_stop(&symbol).await,
        };
        Ok(())
    }

    async fn handle_new(&mut self, order_id: Uuid) -> Result<()> {
        let order = self.transactions.get_order(&order_id).await?;
        let symbol = order.get_symbol().to_string();
        info!("In handle new for symbol: {}", symbol);

        if let OrderAction::Create = order.get_action() {
            let strategy = order.get_strategy().to_string();
            let entry_price = order.get_limit_price();
            self.transactions
                .activate_stop(&symbol, &strategy, entry_price)
                .await?;
            self.mktdata.subscribe(&symbol).await?;
        } else {
            warn!("Didn't add order to locker action: {}", order.get_action());
        };
        Ok(())
    }

    async fn handle_fill(&mut self, order_id: Uuid) -> Result<()> {
        let order = self.transactions.get_order(&order_id).await?;
        let symbol = order.get_symbol().to_string();
        info!("In handle fill for symbol: : {}", symbol);

        let fill_price = order.get_fill_price();
        let action = order.get_action();

        match action {
            OrderAction::Create => {
                self.transactions
                    .update_stop_entry_price(&symbol, fill_price)
                    .await?;
                self.transactions.update_transaction(order_id).await?;
            }
            OrderAction::Liquidate => {
                self.mktdata.unsubscribe(&symbol).await?;
                self.transactions.stop_complete(&symbol).await?;

                self.transactions.close_transaction(order_id).await?
            }
        };
        Ok(())
    }

    pub async fn subscribe_to_events(&mut self) -> Result<()> {
        self.order_handler.subscribe_to_events().await?;

        let symbols = self.transactions.get_subscribed_symbols().await?;
        self.mktdata.startup(symbols).await?;
        Ok(())
    }

    pub fn get_event_subscriber(&self) -> Result<Receiver<Event>> {
        Ok(self.connectors.get_subscriber())
    }

    pub async fn run(engine: Arc<Mutex<Engine>>, shutdown_signal: CancellationToken) -> Result<()> {
        let mut event_subscriber = engine.lock().await.get_event_subscriber()?;
        let mut mktdata_publish_interval = interval(Duration::from_secs(5));
        tokio::spawn(async move {
            let _ = engine.lock().await.subscribe_to_events().await;
            loop {
                tokio::select!(
                    event = event_subscriber.recv() => {
                        match event {
                            anyhow::Result::Ok(Event::OrderUpdate(event)) => {
                                info!("Found a trade event: {event:?}");
                                let _ = engine.lock().await.order_update(&event).await;
                            },
                            anyhow::Result::Ok(Event::Trade(event)) => {
                                debug!("Found a mkdata event: {event:?}");
                                engine.lock().await.mktdata_update(&event).await;
                            }
                            anyhow::Result::Err(err) => {
                                error!("Unknown error: {err}");
                                shutdown_signal.cancel();
                            }
                            _ => ()
                        }
                    }
                    //smooth market data updates to avoid reacting to spikes
                    _ = mktdata_publish_interval.tick() => {
                        debug!("Publish mktdata snapshots");
                        let _ = engine.lock().await.mktdata_publish().await;
                    }
                    _ = shutdown_signal.cancelled() => {
                        break;
                    }
                );
            }
            info!("Shutting down event loop in platform");
        });
        Ok(())
    }
}
