use log::{error, info};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use apca::api::v2::updates;
use apca::data::v2::stream;

use super::AccountDetails;
use super::Locker;
use super::MktData;
use super::MktOrder;
use super::Trading;

use super::Event;
use super::MktPosition;

use crate::events::MktSignal;
use crate::Settings;

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
            self.locker.monitor_trade(&order.symbol, &order.limit_price.clone().unwrap());
        }
        for mktposition in &positions {
            let position = mktposition.1.get_position();
            self.locker.monitor_trade(&position.symbol, &position.average_entry_price);
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

    pub async fn order_update(&mut self, order_update: &updates::OrderUpdate) {
        match order_update.event {
            updates::OrderStatus::Filled => self.locker.monitor_trade(
                &order_update.order.symbol,
                &order_update.order.average_fill_price.clone().unwrap(),
            ),
            _ => info!("Not listening to event {0:?}", order_update.event),
        }
    }

    pub async fn mktdata_update(&mut self, mktdata_update: &stream::Trade) {
        if self.locker.should_close(mktdata_update) {
            let position = &self.positions[&mktdata_update.symbol];
            if !(self.trading.liquidate_position(position).await) {
                error!("Dropping liquidate, failed to send to server");
            }
        }
    }

    pub async fn cancel_order(&mut self) {
    }

    pub async fn create_position(&mut self, mkt_signal: &MktSignal) {
        let price = Num::new((mkt_signal.price * 100.0) as i32, 100);
        let buying_power = self.account.buying_power();
        let capacity = self.settings.strategies.get(&mkt_signal.strategy).unwrap();
        let size = buying_power / Num::from(capacity.max_positions);
        let _ = self.trading
            .create_position(&mkt_signal.symbol, price, size, mkt_signal.side)
            .await;
    }

    pub async fn liquidate_position(&mut self) {
    }
}
