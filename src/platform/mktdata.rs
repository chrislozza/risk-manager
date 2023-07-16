use apca::data::v2::bars;
use apca::data::v2::stream;
use apca::Client;
use chrono::Utc;
use chrono::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use anyhow::bail;
use anyhow::Result;

use num_decimal::Num;
use std::vec::Vec;

use tracing::debug; 
use tracing::info; 
use tracing::warn;
use tracing::error; 

use tokio::sync::broadcast;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use super::data::mktorder::MktOrders;
use super::data::mktposition::MktPositions;
use super::Event;
use super::web_clients::Connectors;
use crate::to_num;

pub struct MktData {
    connectors: Arc<Connectors>,
    snapshots: HashMap<String, Num>
}

impl MktData {
    pub fn new(connectors: &Arc<Connectors>) -> Self {
        MktData {
            connectors: Arc::clone(connectors),
            snapshots: HashMap::default()
        }
    }

    pub async fn get_historical_bars(&self, symbol: &str, days_to_lookback: i64) -> Result<Vec<bars::Bar>> {
        let today = Utc::now();
        let start_date = today - Duration::days(days_to_lookback);
        let end_date = today - Duration::days(1);
        let request = bars::BarsReqInit {
            limit: Some(days_to_lookback as usize),
            ..Default::default()
        }
        .init(symbol, start_date, end_date, bars::TimeFrame::OneDay);

        let result = self.connectors.get_historical_bars(&request).await?;
        Ok(result.bars)
    }

    pub async fn startup(&mut self, positions: Vec<String>, orders: Vec<String>) -> Result<()> {
        let position_sym = self.batch_subscribe(positions).await?;
        let order_sym = self.batch_subscribe(orders).await?;

        let symbol_list = vec![position_sym, order_sym];
        for symbols in symbol_list.iter() {
            match symbols {
                stream::Symbols::List(list) => {
                    for symbol in list.to_vec() {
                        if !self.snapshots.contains_key(&symbol.to_string()) {
                            self.snapshots.insert(symbol.to_string(), to_num!(0.0));
                        }
                    }
                    ()
                },
                _ => (),
            }
        }
        Ok(())
    }

    async fn batch_subscribe(&self, symbols: Vec<String>) -> Result<stream::Symbols> {
        self.connectors
            .subscribe_to_symbols(symbols.into())
            .await
    }

    pub async fn subscribe(&mut self, symbol: &str) -> Result<()> {
        let symbols = vec![symbol.to_string()];
        let _ = self.batch_subscribe(symbols).await?;
        self.snapshots.insert(symbol.to_string(), to_num!(0.0));
        Ok(())
    }

    pub async fn unsubscribe(&mut self, symbol: &str) -> Result<()> {
        let symbols = self
            .connectors
            .unsubscribe_from_symbols(vec![symbol.to_string()].into())
            .await?;
        self.snapshots.remove(symbol);
        Ok(())
    }

    pub fn get_snapshots(&self) -> HashMap<String, Num> {
        self.snapshots.clone()
    }

    pub fn capture_data(&self, mktdata_update: &stream::Trade) {
        let symbol = mktdata_update.symbol;
        self.snapshots[&symbol] = mktdata_update.trade_price
    }
}
