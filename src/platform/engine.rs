use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use tracing::debug; 
use tracing::info; 
use tracing::warn;
use tracing::error; 

use apca::api::v2::updates;
use apca::data::v2::stream;

use num_decimal::Num;

use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use anyhow::Result;

use crate::float_to_num;
use super::Settings;

use super::AccountDetails;
use super::locker::Locker;
use super::order_handler::OrderHandler;
use super::Event;

use super::mktdata::MktData;
use super::web_clients::Connectors;

use super::data::mktorder::MktOrder;
use super::data::mktorder::MktOrders;
use super::data::mktposition::MktPosition;
use super::data::mktposition::MktPositions;
use super::data::account::AccountDetails;

pub struct Engine {
    account: AccountDetails,
    mktdata: MktData,
    order_handler: OrderHandler,
    locker: Locker,
    mktpositions: MktPositions,
    mktorders: MktOrders,
    connectors : Connectors,
    settings: Settings,
}

impl Engine {
    pub async fn new(
        settings: Settings,
        key: &str,
        secret: &str,
        is_live: bool,
        shutdown_signal: CancellationToken,
    ) -> Arc<Mutex<Self>> {
        let connectors = Connectors::new(key, secret, is_live, shutdown_signal); 
        let account = AccountDetails::new(&connectors).await;
        let order_handler = OrderHandler::new(&connectors);
        let mktdata = MktData::new(&connectors);
        let mktorders = MktOrders::new(&connectors);
        let mktpositions = MktPositions::new(&connectors);
        let locker = Locker::new(settings.clone());
        Arc::new(Mutex::new(Engine {
            account,
            mktdata,
            order_handler,
            locker,
            mktpositions,
            mktorders,
            connectors,  
            settings,
        }))
    }

    pub async fn startup(&mut self) -> Result<()> {

        self.mktorders.update_orders()?;
        self.mktpositions.update_positions()?;

        let mktorders = self.mktorders.get_orders().await?;
        for mktorder in &mktorders {
            let order = mktorder.get_order();
            self.locker.monitor_trade(
                &order.symbol,
                &order.limit_price.clone().unwrap(),
                &mktorder.get_strategy(),
                TransactionType::Order,
            );
        }
        for mktposition in &positions {
            let position = mktposition.get_position();
            self.locker.monitor_trade(
                &position.symbol,
                &position.average_entry_price,
                &mktposition.get_strategy(),
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
            &self.account.equity(),
            strategy_cfg.trailing_size,
            self.settings.strategies.configuration.len(),
            &self.mktdata,
        )
        .await?;
        self.order_handler.create_position(
                &mkt_signal.symbol,
                &mkt_signal.strategy,
                target_price,
                size,
                side,
            ).await?
    }

    pub async fn update_status(&mut self) {
        let _ = self.account.update_account().await;
        let _ = self.mktpositions.update_positions().await;
        let _ = self.mktorders.update_positions().await;
    }

    async fn size_position(
        symbol: &str,
        total_equity: &Num,
        multiplier: f64,
        number_of_strategies: usize,
        mktdata: &MktData,
    ) -> Result<Num> {
        let risk_tolerance = float_to_num!(0.02);
        let total_equity_per_strategy = total_equity / number_of_strategies;
        let risk_per_trade = total_equity_per_strategy  * risk_tolerance;
        let atr = RiskManagement::get_atr(symbol, mktdata).await?;
        let atr_stop = atr.clone() * float_to_num!(multiplier);
        let position_size = risk_per_trade / atr_stop.clone();
        info!("Position size: {position_size} from equity: {total_equity} with atr: {atr}, atr_stop: {atr_stop}");
        Ok(position_size)
    }

    pub async fn mktdata_update(&mut self, mktdata_update: &stream::Trade) {
        self.mktdata.capture_data(mktdata_update);
    }

    pub async fn mktdata_publish(&mut self) -> Result<()> {
        let snapshot = self.mktdata.get_snapshot();
        if !self.locker.should_close(mktdata_update)
            || self.locker.get_status(&mktdata_update.symbol) != LockerStatus::Active
        {
            return Ok(())
        }
        match self.locker.get_transaction_type(&mktdata_update.symbol) {
            TransactionType::Order => {
                let order = self.mktorders.get_order(&mktdata_update.symbol).await?;
                match self.order_handler.cancel_order(order).await {
                    Err(error) => {
                        error!("Dropping order cancel, failed to send to server, error: {val}");
                        self.locker
                            .update_status(&mktdata_update.symbol, LockerStatus::Active);
                        bail!("{val}")
                    },
                    _ => ()
                };
            }
            TransactionType::Position => {
                let position = self.mktpositions.get_position(&mktdata_update.symbol).await?;
                match self.order_handler.liquidate_position(position).await {
                    Err(error) => {
                        error!("Dropping liquidate, failed to send to server, error: {val:?}");
                        self.locker
                            .update_status(&mktdata_update.symbol, LockerStatus::Active);
                        bail!("{val}")
                    },
                    _ => ()
                };
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
        let mktorder = match &self.mktorders.get_order(&symbol.clone()).await? {
            Ok(order) => order,
            Err(err) => bail!("Failed to find order for symbol: {symbol.clone()}, error: {err}")
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
        let symbol = &order_update.order.symbol;
        let mktorder = match &self.mktorders.get_order(&symbol).await? {
            Ok(order) => order,
            Err(err) => bail!("Failed to find order for symbol: {symbol.clone()}, error: {err}")
        };

        if let OrderAction::Create = mktorder.get_action() {
            let strategy_cfg = &self.settings.strategies.configuration[mktorder.get_strategy()];
            self.locker.monitor_trade(
                &symbol,
                order_update.order.limit_price.as_ref().unwrap(),
                strategy_cfg,
                TransactionType::Order,
            );
        };
        Ok(())
    }

    async fn handle_fill(&mut self, order_update: &updates::OrderUpdate) -> Result<()> {
        let symbol = &order_update.order.symbol;
        let mktorder = match &self.mktorders.get_order(&symbol.clone()).await? {
            Ok(order) => order,
            Err(err) => bail!("Failed to find order for symbol: {symbol.clone()}, error: {err}");,
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
            OrderAction::Liquidate => self.locker.complete(&order_update.order.symbol),
        };
        Ok(())
    }

    pub async fn subscribe_to_events(&self) -> Receiver<Event> {
        self.order_handler.subscribe_to_events().await?;

        let mktorders = self.mktorders.get_orders().await?;
        let orders = mktorders.iter().map(|s| s.get_order().symbol).collect();
        self.mktdata.batch_subscribe(orders).await?;

        let mktpositions = self.mktpositions.get_positions().await?;
        let positions = mktpositions.iter().map(|s| s.get_position().symbol).collect();
        self.mktdata.batch_subscribe(positions).await?;

        self.connectors.get_subscriber()
    }

    pub async fn run(engine: Arc<Mutex<Engine>>, shutdown_signal: CancellationToken) -> Result<()> {
        tokio::spawn(async move {
            let event_subscriber = engine.lock().await.subscribe_to_events().await;
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
                        }
                    }
                    //smooth market data updates to avoid reacting to spikes
                    _ = sleep(Duration::from_secs(5)) => {
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
