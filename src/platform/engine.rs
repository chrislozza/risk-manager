use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::interval;
use tokio::time::Duration;

use tracing::debug;
use tracing::error;
use tracing::info;

use apca::api::v2::updates;
use apca::data::v2::stream;

use num_decimal::Num;

use anyhow::bail;
use anyhow::Result;

use tokio::sync::broadcast::Receiver;
use tokio_util::sync::CancellationToken;

use super::super::events::MktSignal;
use super::data::account::AccountDetails;
use super::data::mktorder::MktOrder;
use super::data::mktorder::MktOrders;
use super::data::mktorder::OrderAction;
use super::data::mktposition::MktPosition;
use super::data::mktposition::MktPositions;
use super::locker::Locker;
use super::locker::LockerStatus;
use super::locker::TransactionType;
use super::mktdata::MktData;
use super::order_handler::OrderHandler;
use super::risk_sizing::RiskManagement;
use super::web_clients::Connectors;
use super::Event;
use super::Settings;
use crate::to_num;

pub struct Engine {
    account: AccountDetails,
    mktdata: MktData,
    order_handler: OrderHandler,
    locker: Locker,
    mktpositions: MktPositions,
    mktorders: MktOrders,
    connectors: Arc<Connectors>,
    settings: Settings,
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
        let order_handler = OrderHandler::new(&connectors);
        let mktdata = MktData::new(&connectors);
        let mktorders = MktOrders::new(&connectors);
        let mktpositions = MktPositions::new(&connectors);
        let locker = Locker::new(settings.clone());
        Ok(Arc::new(Mutex::new(Engine {
            account,
            mktdata,
            order_handler,
            locker,
            mktpositions,
            mktorders,
            connectors,
            settings,
        })))
    }

    pub async fn startup(&mut self) -> Result<()> {
        info!("Downloading orders and positions in engine startup");
        self.mktorders.update_orders().await?;
        self.mktpositions.update_positions().await?;

        let mktorders = self.mktorders.get_orders().await;
        for mktorder in mktorders.values() {
            let order = mktorder.get_order();
            self.locker.monitor_trade(
                &order.symbol,
                &order.limit_price.clone().unwrap(),
                mktorder.get_strategy(),
                TransactionType::Order,
            );
        }
        let mktpositions = self.mktpositions.get_positions().await;
        for mktposition in mktpositions.values() {
            let position = mktposition.get_position();
            self.locker.monitor_trade(
                &position.symbol,
                &position.average_entry_price,
                mktposition.get_strategy(),
                TransactionType::Position,
            );
        }
        Ok(())
    }

    pub async fn create_position(&mut self, mkt_signal: &MktSignal) -> Result<()> {
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
        let target_price = Num::new((mkt_signal.price * 100.0) as i32, 100);
        let size = Self::size_position(
            &mkt_signal.symbol,
            &self.account.equity().await,
            strategy_cfg.trailing_size,
            self.settings.strategies.configuration.len(),
            &self.mktdata,
        )
        .await?;
        let order = self
            .order_handler
            .create_position(
                &mkt_signal.symbol,
                &mkt_signal.strategy,
                target_price,
                size,
                &mkt_signal.side,
            )
            .await?;
        self.mktorders.add_order(order).await
    }

    pub async fn update_status(&mut self) {
        let _ = self.account.update_account().await;
        let _ = self.mktorders.update_orders().await;
        let _ = self.mktpositions.update_positions().await;
        let _ = self.locker.print_snapshot();
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

    pub async fn mktdata_publish(&mut self) -> Result<()> {
        let snapshots = self.mktdata.get_snapshots();
        for (symbol, last_price) in snapshots {
            if !self.locker.should_close(&symbol, &last_price)
                || self.locker.get_status(&symbol) != LockerStatus::Active
            {
                return Ok(());
            }
            match self.locker.get_transaction_type(&symbol) {
                TransactionType::Order => self.handle_cancel(&symbol).await?,
                TransactionType::Position => {
                    let order = self.handle_liquidate(&symbol).await?;
                    self.mktorders.add_order(order);
                }
            }
        }
        Ok(())
    }

    pub async fn order_update(&mut self, order_update: &updates::OrderUpdate) -> Result<()> {
        match order_update.event {
            updates::OrderStatus::New => {
                self.handle_new(order_update);
            }
            updates::OrderStatus::Filled => {
                self.handle_fill(order_update);
                self.mktpositions.update_positions().await?;
            }
            updates::OrderStatus::Canceled => {
                self.handle_cancel_reject(order_update);
            }
            _ => info!("Not listening to event {0:?}", order_update.event),
        }
        Ok(())
    }

    async fn handle_cancel_reject(&mut self, order_update: &updates::OrderUpdate) -> Result<()> {
        let symbol = &order_update.order.symbol;
        let mktorder = match self.mktorders.get_order(symbol).await {
            Ok(order) => order,
            Err(err) => bail!("Failed to find order for symbol: {symbol}, error: {err}"),
        };

        match mktorder.get_action() {
            OrderAction::Create => {
                self.locker.complete(symbol);
            }
            OrderAction::Liquidate => self.locker.revive(symbol),
        };
        Ok(())
    }

    async fn handle_new(&mut self, order_update: &updates::OrderUpdate) -> Result<()> {
        let symbol = &order_update.order.symbol.clone();
        let mktorder = match self.mktorders.get_order(symbol).await {
            Ok(order) => order,
            Err(err) => bail!("Failed to find order for symbol: {symbol}, error: {err}"),
        };

        if let OrderAction::Create = mktorder.get_action() {
            self.locker.monitor_trade(
                symbol,
                order_update.order.limit_price.as_ref().unwrap(),
                mktorder.get_strategy(),
                TransactionType::Order,
            );
        };
        Ok(())
    }

    async fn handle_fill(&mut self, order_update: &updates::OrderUpdate) -> Result<()> {
        let symbol = &order_update.order.symbol;
        let mktorder = match self.mktorders.get_order(symbol).await {
            Ok(order) => order,
            Err(err) => bail!("Failed to find order for symbol: {symbol}, error: {err}"),
        };

        match mktorder.get_action() {
            OrderAction::Create => {
                self.locker.monitor_trade(
                    &symbol.clone(),
                    &order_update.order.average_fill_price.clone().unwrap(),
                    mktorder.get_strategy(),
                    TransactionType::Position,
                );
            }
            OrderAction::Liquidate => {
                self.locker.complete(symbol);
                self.mktdata.unsubscribe(symbol).await;
            }
        };
        Ok(())
    }

    async fn handle_cancel(&mut self, symbol: &str) -> Result<()> {
        let order = self.mktorders.get_order(symbol).await?;
        match self.order_handler.cancel_order(order).await {
            Err(error) => {
                error!("Dropping order cancel, failed to send to server, error={error}");
                self.locker.update_status(symbol, LockerStatus::Active);
                bail!("{error}")
            }
            _ => Ok(()),
        }
    }

    async fn handle_liquidate(&mut self, symbol: &str) -> Result<MktOrder> {
        let position = self.mktpositions.get_position(symbol).await?;
        match self.order_handler.liquidate_position(position).await {
            Err(error) => {
                error!("Dropping liquidate, failed to send to server, error={error}");
                self.locker.update_status(symbol, LockerStatus::Active);
                bail!("{error}")
            }
            Ok(order) => Ok(order),
        }
    }

    pub async fn subscribe_to_events(&mut self) -> Result<()> {
        self.order_handler.subscribe_to_events().await?;

        let mktorders = self.mktorders.get_orders().await;
        let orders = mktorders
            .values()
            .map(|o| o.get_order().symbol.clone())
            .collect();

        let mktpositions: &HashMap<String, MktPosition> = self.mktpositions.get_positions().await;
        let positions = mktpositions
            .values()
            .map(|p| p.get_position().symbol.clone())
            .collect();
        self.mktdata.startup(positions, orders).await?;
        Ok(())
    }

    pub fn get_event_subscriber(&self) -> Result<Receiver<Event>> {
        Ok(self.connectors.get_subscriber())
    }

    pub async fn run(engine: Arc<Mutex<Engine>>, shutdown_signal: CancellationToken) -> Result<()> {
        let mut event_subscriber = engine.lock().await.get_event_subscriber()?;
        let mut mktdata_publish_interval = interval(Duration::from_secs(5)); 
        tokio::spawn(async move {
            loop {
                tokio::select!(
                    event = event_subscriber.recv() => {
                        match event {
                            Ok(Event::OrderUpdate(event)) => {
                                debug!("Found a trade event: {event:?}");
                                engine.lock().await.order_update(&event).await;
                            },
                            Ok(Event::Trade(event)) => {
                                debug!("Found a mkdata event: {event:?}");
                                engine.lock().await.mktdata_update(&event).await;
                            }
                            Err(err) => {
                                error!("Unknown error: {err}");
                                shutdown_signal.cancel();
                            }
                            _ => ()
                        }
                    }
                    //smooth market data updates to avoid reacting to spikes
                    _ = mktdata_publish_interval.tick() => {
                        debug!("Publish mktdata snapshots");
                        engine.lock().await.mktdata_publish().await;
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
