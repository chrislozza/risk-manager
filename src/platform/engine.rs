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
    trading: Trading,
    locker: Locker,
    mktpositions: MktPositions,
    mktorders: MktOrders,
    connectors : Connectors,
    settings: Settings,
}

impl Engine {
    pub fn new(
        settings: Settings,
        key: &str,
        secret: &str,
        is_live: bool,
        shutdown_signal: CancellationToken,
    ) -> Arc<Mutex<Self>> {
        let connectors = Connectors::new(key, secret, is_live, shutdown_signal); 
        let account = AccountDetails::new(&connectors);
        let trading = OrderHandler::new(&connectors);
        let mktdata = MktData::new(&connectors);
        let mktorders = MktOrders::new(&connectors);
        let mktpositions = MktPositions::new(&connectors);
        let locker = Locker::new();
        Arc::new(Mutex::new(Engine {
            account,
            mktdata,
            trading,
            locker,
            mktpositions,
            mktorders,
            connectors,  
            settings,
        }))
    }

    pub async fn startup(&mut self) -> Result<broadcast::Receiver<Event>> {
        let _ = self.account.startup().await;
        let (positions, orders) = self.trading.startup().await;
        
        let 


        self.mktdata.startup(&orders, &positions).await;
        for mktorder in &orders {
            let order = mktorder.1.get_order();
            self.locker.monitor_trade(
                &order.symbol,
                &order.limit_price.clone().unwrap(),
                &self.settings.strategies.configuration[mktorder.1.get_strategy()],
                TransactionType::Order,
            );
        }
        for mktposition in &positions {
            let position = mktposition.1.get_position();
            self.locker.monitor_trade(
                &position.symbol,
                &position.average_entry_price,
                &self.settings.strategies.configuration[mktposition.1.get_strategy()],
                TransactionType::Position,
            );
        }
        self.orders = orders;
        self.positions = positions;
        self.connectors.startup(&self.orders, &self.positions);
        Ok(self.connectors.subscribe_to_streams())
    }

    pub async fn shutdown(&self) {
        self.trading.shutdown().await;
        self.mktdata.shutdown().await;
    }

    async fn update_mktpositions(&mut self) {
        let positions = self.trading.get_positions().await;
        self.positions = positions.unwrap();
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
        let mktorder = self
            .trading
            .create_position(
                &mkt_signal.symbol,
                &mkt_signal.strategy,
                target_price,
                size,
                side,
            )
            .await?;
        self.orders.insert(mkt_signal.symbol.clone(), mktorder);
        Ok(())
    }

    pub async fn update_status(&mut self) {
        let _ = self.account.refresh_account_details().await;
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
        if !self.locker.should_close(mktdata_update)
            || self.locker.get_status(&mktdata_update.symbol) != LockerStatus::Active
        {
            return;
        }
        match self.locker.get_transaction_type(&mktdata_update.symbol) {
            TransactionType::Order => {
                let order = &self.orders[&mktdata_update.symbol];
                if let Err(val) = self.trading.cancel_order(order).await {
                    error!("Dropping order cancel, failed to send to server, error: {val}");
                    self.locker
                        .update_status(&mktdata_update.symbol, LockerStatus::Active);
                }
            }
            TransactionType::Position => {
                let position = &self.positions[&mktdata_update.symbol];
                if let Err(val) = self.trading.liquidate_position(position).await {
                    error!("Dropping liquidate, failed to send to server, error: {val}");
                    self.locker
                        .update_status(&mktdata_update.symbol, LockerStatus::Active);
                }
            }
        }
    }

    pub async fn order_update(&mut self, order_update: &updates::OrderUpdate) -> Result<()> {
        match order_update.event {
            updates::OrderStatus::New => {
                self.handle_new(order_update);
            }
            updates::OrderStatus::Filled => {
                self.handle_fill(order_update);
                self.update_mktpositions().await;
            }
            updates::OrderStatus::Canceled => {
                self.handle_cancel_reject(order_update);
            }
            _ => info!("Not listening to event {0:?}", order_update.event),
        }
        Ok(())
    }

    fn handle_cancel_reject(&mut self, order_update: &updates::OrderUpdate) -> Result<()> {
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

    fn handle_new(&mut self, order_update: &updates::OrderUpdate) {
        let symbol = &order_update.order.symbol;
        let mktorder = &self.mktorders.get_order(&symbol.clone()).await? {
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
    }

    fn handle_fill(&mut self, order_update: &updates::OrderUpdate) -> Result<()> {
        let symbol = &order_update.order.symbol;
        let mktorder = &self.mktorders.get_order(&symbol.clone()).await? {
            Ok(order) => order,
            Err(err) => bail!("Failed to find order for symbol: {symbol.clone()}, error: {err}")
        };

        match mktorder.get_action() {
            OrderAction::Create => {
                let strategy_cfg = &self.settings.strategies.configuration[mktorder.get_strategy()];
                self.locker.monitor_trade(
                    &symbol.clone(),
                    &order_update.order.average_fill_price.clone().unwrap(),
                    strategy_cfg,
                    TransactionType::Position,
                );
            }
            OrderAction::Liquidate => self.locker.complete(&order_update.order.symbol),
        };
        Ok(())
    }

    pub fn subscribe_to_events(&self) -> Receiver<Event> {
        self.connectors.subscribe_to_streams()
    }

    pub async fn run(engine: Arc<Mutex<Engine>>, shutdown_signal: CancellationToken) -> Result<()> {
        tokio::spawn(async move {
            let event_subscriber = engine.lock().await.subscribe_to_events();
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
                _ = shutdown_signal.cancelled() => {
                    break;
                });
            }
            info!("Shutting down event loop in platform");
        });
        Ok(())
    }
}
