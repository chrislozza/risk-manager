use log::{error, info};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use apca::api::v2::updates;
use apca::data::v2::stream;

use super::mktorder::OrderAction;
use super::AccountDetails;
use super::Locker;

use super::MktData;
use super::MktOrder;
use super::Trading;

use super::Event;
use super::MktPosition;

use anyhow::{Result};

use crate::events::MktSignal;
use crate::events::Side;
use crate::platform::locker::{LockerStatus, TransactionType};
use crate::Settings;
use crate::platform::risk_sizing::MaxLeverage;

use num_decimal::Num;

use tokio::sync::broadcast;

pub struct Engine {
    pub account: AccountDetails,
    pub mktdata: MktData,
    pub trading: Trading,
    pub locker: Locker,
    pub positions: HashMap<String, MktPosition>,
    pub orders: HashMap<String, MktOrder>,
    settings: Settings,
}

impl Engine {
    pub fn new(
        settings: Settings,
        account: AccountDetails,
        mktdata: MktData,
        trading: Trading,
        locker: Locker,
    ) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Engine {
            settings,
            account,
            mktdata,
            trading,
            locker,
            positions: HashMap::default(),
            orders: HashMap::default(),
        }))
    }

    pub async fn startup(&mut self) -> (broadcast::Receiver<Event>, broadcast::Receiver<Event>) {
        self.account.startup().await;
        let (positions, orders) = self.trading.startup().await;
        self.mktdata.startup(&orders, &positions).await;
        for mktorders in &orders {
            let order = mktorders.1.get_order();
            self.locker.monitor_trade(
                &order.symbol,
                &order.limit_price.clone().unwrap(),
                TransactionType::Order,
            );
        }
        for mktposition in &positions {
            let position = mktposition.1.get_position();
            self.locker.monitor_trade(
                &position.symbol,
                &position.average_entry_price,
                TransactionType::Position,
            );
        }
        self.orders = orders;
        self.positions = positions;
        (self.trading.stream_reader(), self.mktdata.stream_reader())
    }

    pub async fn shutdown(&self) {
        self.trading.shutdown().await;
        self.mktdata.shutdown().await;
    }

    pub fn get_mktorders(&self) -> &HashMap<String, MktOrder> {
        &self.orders
    }

    pub fn get_mktpositions(&self) -> &HashMap<String, MktPosition> {
        &self.positions
    }

    async fn update_mktpositions(&mut self) {
        let positions = self.trading.get_positions().await;
        self.positions = positions.unwrap();
    }

    pub async fn order_update(&mut self, order_update: &updates::OrderUpdate) {
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
    }

    fn handle_cancel_reject(&mut self, order_update: &updates::OrderUpdate) {
        let mktorder = &self.orders[&order_update.order.symbol];
        match mktorder.get_action() {
            OrderAction::Create => {
                self.locker.complete(&order_update.order.symbol);
            }
            OrderAction::Liquidate => self.locker.revive(&order_update.order.symbol),
        };
    }

    fn handle_new(&mut self, order_update: &updates::OrderUpdate) {
        let mktorder = &self.orders[&order_update.order.symbol];
        if let OrderAction::Create = mktorder.get_action() {
            self.locker.monitor_trade(
                &order_update.order.symbol,
                order_update.order.limit_price.as_ref().unwrap(),
                TransactionType::Order,
            );
        };
    }

    fn handle_fill(&mut self, order_update: &updates::OrderUpdate) {
        let mktorder = &self.orders[&order_update.order.symbol];
        match mktorder.get_action() {
            OrderAction::Create => {
                self.locker.monitor_trade(
                    &order_update.order.symbol,
                    &order_update.order.average_fill_price.clone().unwrap(),
                    TransactionType::Position,
                );
            }
            OrderAction::Liquidate => self.locker.complete(&order_update.order.symbol),
        };
    }

    pub async fn mktdata_update(&mut self, mktdata_update: &stream::Trade) {
        if !self.locker.should_close(mktdata_update) {
            return;
        }
        if *self.locker.get_status(&mktdata_update.symbol) != LockerStatus::Active {
            return;
        }
        match self.locker.get_transaction_type(&mktdata_update.symbol) {
            TransactionType::Order => {
                let order = &self.orders[&mktdata_update.symbol];
                if !(self.trading.cancel_order(order).await) {
                    error!("Dropping order cancel, failed to send to server");
                }
            }
            TransactionType::Position => {
                let position = &self.positions[&mktdata_update.symbol];
                if !(self.trading.liquidate_position(position).await) {
                    error!("Dropping liquidate, failed to send to server");
                }
            }
        }
    }

    pub async fn create_position(&mut self, mkt_signal: &MktSignal) -> Result<()> {
        let strategy_cfg = match self.settings.strategies.configuration.get(&mkt_signal.strategy) {
            Some(strategy) => strategy,
            _ => {
                info!("Not subscribed to strategy: {}", mkt_signal.strategy);
                return Ok(());
            }
        };
        let target_price = Num::new((mkt_signal.price.unwrap() * 100.0) as i32, 100);
        let size = Self::size_position(
            &mkt_signal.symbol,
            &self.account.equity(),
            strategy_cfg.multiplier,
            &self.mktdata,
        ).await?;
        let side = Self::convert_side(&mkt_signal.side);
        let mktorder = self
            .trading
            .create_position(&mkt_signal.symbol, target_price, size, side)
            .await?;
        self.orders.insert(mkt_signal.symbol.clone(), mktorder);
        Ok(())
    }

    pub async fn refresh_data(&mut self) {
        self.account.refresh_account_details().await;
        if let Ok(positions) = self.trading.get_positions().await {
            self.positions = positions;
        }
        if let Ok(orders) = self.trading.get_orders().await {
            self.orders = orders;
        }
    }

    fn get_gross_position_value(&self) -> Num {
        let mut gross_position_value = Num::from(0);
        for order in self.get_mktorders().values() {
            gross_position_value += order.market_value();
        }
        gross_position_value
    }

    async fn size_position(symbol: &str, total_equity: &Num, multiplier: i8, mktdata: &MktData) -> Result<Num> {
        let risk_tolerance = Num::new((0.02 * 100.0) as i64, 100);
        let risk_per_trade = total_equity * risk_tolerance;
        let atr = MaxLeverage::get_atr(symbol, mktdata).await?;
        let atr_stop = atr * Num::from(multiplier);
        Ok(risk_per_trade / atr_stop)
    }

    fn convert_side(side: &Side) -> apca::api::v2::order::Side {
        match side {
            Side::Buy => apca::api::v2::order::Side::Buy,
            Side::Sell => apca::api::v2::order::Side::Sell,
        }
    }
}
