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
use super::data::TransactionStatus;

use super::data::mktorder::OrderAction;

use super::data::Transactions;
use super::mktdata::MktData;
use super::order_handler::OrderHandler;
use super::technical_signals::TechnnicalSignals;
use super::web_clients::Connectors;
use super::Event;
use super::Settings;
use crate::events::Direction;

use crate::settings::PositionSizing;
use crate::to_num;

pub struct Engine {
    settings: Settings,
    account: AccountDetails,
    assets: Assets,
    mktdata: Arc<Mutex<MktData>>,
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
        let transactions = Transactions::new(&settings, &connectors, &mktdata).await?;
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
        self.assets.startup().await?;
        self.connectors.startup().await?;
        Ok(())
    }

    pub async fn create_position(&mut self, mkt_signal: &MktSignal) -> Result<()> {
        if let Some(transaction) = self.transactions.get_transaction(&mkt_signal.symbol) {
            info!(
                "Already has an open transaction for strategy: {} symbol: {}",
                transaction.strategy, transaction.strategy
            );
            return Ok(());
        }
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
        let position_sizing = self.settings.sizing.clone();
        let entry_price = to_num!(mkt_signal.price);
        let size = Self::size_position(
            &mkt_signal.symbol,
            &self.account.equity().await,
            position_sizing,
            self.settings.strategies.len(),
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
            .add_waiting_transaction(symbol, strategy, direction, entry_price.clone())
            .await
        {
            bail!("Failed to add waiting transaction, error={}", err)
        };
        match self
            .order_handler
            .create_position(
                &mkt_signal.symbol,
                entry_price.clone(),
                size,
                mkt_signal.side,
            )
            .await
        {
            anyhow::Result::Ok(order_id) => {
                self.transactions
                    .add_order(symbol, order_id, direction, OrderAction::Create)
                    .await?;
                info!(
                    "Strategy[{}] symbol[{}] added a waiting order",
                    strategy, symbol
                );
                self.transactions
                    .add_stop(symbol, strategy, entry_price, direction)
                    .await
            }
            Err(err) => bail!(
                "Failed to create new position for symbol: {}, error={}",
                mkt_signal.symbol,
                err
            ),
        }
    }

    pub async fn update_status(&mut self) -> Result<()> {
        let _ = self.account.update_account().await;
        self.transactions.print_active_transactions().await
    }

    async fn size_position(
        symbol: &str,
        total_equity: &Num,
        sizing: PositionSizing,
        number_of_strategies: usize,
        mktdata: &Arc<Mutex<MktData>>,
    ) -> Result<Num> {
        let risk_tolerance = to_num!(sizing.risk_tolerance);
        let total_equity_per_strategy = total_equity / number_of_strategies;
        let risk_per_trade = total_equity_per_strategy * risk_tolerance;
        let atr = TechnnicalSignals::get_atr(symbol, mktdata).await?;
        let atr_stop = atr.clone() * to_num!(sizing.multiplier);
        let position_size = risk_per_trade.clone() / atr_stop.clone();
        info!(
            "Position size: {} total risk per position: {} with atr: {}, atr_stop: {}",
            position_size,
            risk_per_trade.round_with(3).to_string(),
            atr,
            atr_stop
        );
        Ok(position_size)
    }

    async fn handle_closing_order(&mut self, symbol: &str, order_id: Uuid) {
        self.handle_cancel(symbol, order_id).await;
        self.transactions.stop_complete(symbol).await
    }

    async fn handle_closing_position(
        &mut self,
        symbol: &str,
        order_id: Uuid,
        direction: Direction,
    ) {
        if let Err(err) = self
            .transactions
            .add_order(symbol, order_id, direction, OrderAction::Liquidate)
            .await
        {
            warn!("Failed to add stop order, error={}", err);
        }
        self.transactions.stop_complete(symbol).await
    }

    pub async fn mktdata_update(&mut self, mktdata_update: &stream::Quote) {
        self.mktdata.lock().await.capture_data(mktdata_update)
    }

    pub async fn mktdata_publish(&mut self) {
        let snapshots = self.mktdata.lock().await.get_snapshots();
        let to_close = self
            .transactions
            .find_transactions_to_close(&snapshots)
            .await;
        for transaction in &to_close {
            let symbol = transaction.symbol.clone();
            match transaction.status {
                TransactionStatus::Waiting => {
                    let order_id = transaction.orders.first().unwrap();
                    self.handle_closing_order(&symbol, *order_id).await
                }
                TransactionStatus::Confirmed => match self.handle_liquidate(&symbol).await {
                    Some(order_id) => {
                        let direction = transaction.direction;
                        self.handle_closing_position(&symbol, order_id, direction)
                            .await
                    }
                    None => self.transactions.reactivate_stop(&symbol).await,
                },
                TransactionStatus::Cancelled => {
                    warn!("Ignoring mktdata update for cancelled transaction")
                }
                TransactionStatus::Complete => {
                    warn!("Ignoring mktdata update for complete transaction")
                }
            };
        }
    }

    async fn handle_cancel(&mut self, symbol: &str, order_id: Uuid) {
        info!("In handle new for symbol: {symbol}");
        match self.order_handler.cancel_order(&order_id).await {
            Err(error) => {
                error!("Dropping order cancel, failed to send to server, error={error}");
                self.transactions.reactivate_stop(symbol).await
            }
            _ => self.transactions.stop_complete(symbol).await,
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
        info!("{:?}", order_update.order);
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
        if let Some(order) = self.transactions.get_order(&order_id).await {
            let symbol = order.symbol.clone();
            info!("In handle cancel reject for symbol: {}", symbol);

            let action = order.action;

            match action {
                OrderAction::Create => match self.transactions.cancel_transaction(order_id).await {
                    Err(err) => {
                        error!("Failed to cancel transaction, error={}", err);
                        bail!("{}", err)
                    }
                    _ => self.mktdata.lock().await.unsubscribe(&symbol).await?,
                },
                OrderAction::Liquidate => self.transactions.reactivate_stop(&symbol).await,
            }
        } else {
            warn!("Order with Id: {}, not found in db", order_id);
        }
        Ok(())
    }

    async fn handle_new(&mut self, order_id: Uuid) -> Result<()> {
        if let Some(order) = self.transactions.get_order(&order_id).await {
            let symbol = order.symbol.clone();
            info!("In handle new for symbol: {}", symbol);

            if let OrderAction::Create = order.action {
                self.transactions.activate_stop(&symbol).await;
                self.mktdata.lock().await.subscribe(&symbol).await?;
            } else {
                warn!("Didn't add order to locker action: {}", order.action);
            };
        } else {
            warn!("Order with Id: {}, not found in db", order_id);
        }
        Ok(())
    }

    async fn handle_fill(&mut self, order_id: Uuid) -> Result<()> {
        if let Some(order) = self.transactions.get_order(&order_id).await {
            let symbol = order.symbol.clone();
            info!("In handle fill for symbol: : {}", symbol);

            let fill_price = order.fill_price.clone();
            let action = order.action;

            match action {
                OrderAction::Create => {
                    self.transactions
                        .update_stop_entry_price(&symbol, fill_price)
                        .await?;
                    self.transactions.update_transaction(order_id).await?;
                }
                OrderAction::Liquidate => {
                    self.mktdata.lock().await.unsubscribe(&symbol).await?;
                    self.transactions.stop_complete(&symbol).await;

                    self.transactions.close_transaction(order_id).await?
                }
            };
        } else {
            warn!("Order with Id: {}, not found in db", order_id);
        }
        Ok(())
    }

    pub async fn subscribe_to_mktdata(&mut self) -> Result<()> {
        let symbols = self.transactions.get_subscribed_symbols().await?;
        self.mktdata.lock().await.startup(symbols).await?;
        Ok(())
    }

    pub fn get_event_subscriber(&self) -> Result<Receiver<Event>> {
        Ok(self.connectors.get_subscriber())
    }

    pub async fn run(engine: Arc<Mutex<Engine>>, shutdown_signal: CancellationToken) -> Result<()> {
        let mut event_subscriber = engine.lock().await.get_event_subscriber()?;
        let mut mktdata_publish_interval = interval(Duration::from_millis(100));
        tokio::spawn(async move {
            let _ = engine.lock().await.subscribe_to_mktdata().await;
            loop {
                tokio::select!(
                    event = event_subscriber.recv() => {
                        match event {
                            anyhow::Result::Ok(Event::OrderUpdate(event)) => {
                                debug!("Found a trade event: {event:?}");
                                let _ = engine.lock().await.order_update(&event).await;
                            },
                            anyhow::Result::Ok(Event::Quote(event)) => {
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
