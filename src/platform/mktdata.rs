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

use std::vec::Vec;

use tracing::debug; 
use tracing::info; 
use tracing::warn;
use tracing::error; 

use tokio::sync::broadcast;
use tokio::sync::RwLock;
use super::Event;
use tokio_util::sync::CancellationToken;

use super::data::mktorder::MktOrders;
use super::data::mktposition::MktPositions;

pub struct MktData {
    connectors: Arc<Connectors>,
    snapshots: HashMap<&str, Num>>
}

impl MktData {
    pub fn new(connectors: &Arc<Connectors>) -> Self {
        MktData {
            connectors: Arc::clone(connectors),
            snapshots: HashSet::default()
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

        let result = self.connectors.get_historical_bars(request).await?;
        result.bars
    }

    pub async fn batch_subscribe(&mut self, symbols: Vec<&str>) -> Result<()> {
        let symbols = match self
            .connectors
            .subscribe_to_mktdata(symbols)
            .await?;
        let subscribed = self.subscribed_symbols.write().await;
        subscribed = symbols;
    }

    pub async fn subscribe(&mut self, symbol: &str) -> Result<()> {
        self.batch_subscribe(vec![symbol.clone()]).await
    }

    pub async fn unsubscribe(&mut self, symbol: &str) -> Result<()> {
        let symbols = match self
            .connectors
            .unsubscribe_from_stream(vec![symbol.clone()].into())
            .await?;
        let subscribed = self.subscribed_symbols.write().await;
        subscribed = symbols;
    }

    pub fn get_snapshot(&self) -> Vec<Bar> {
        return Vec::default()
    }

    pub fn capture_data(&self, mktdata_update: &stream::Trade) {
        let symbol = mktdata_update.symbol;
        self.snapshots[symbol] = mktdata_update.trade_price
    }

    fn build_symbol_list(
        &self,
        orders: &MktOrders,
    ) -> Vec<String> {
        let mut symbols = Vec::<String>::default();
        for mktorder in orders.get_orders() {
            symbols.push(mktorder.get_order().symbol.clone());
        }

        for mktposition in positions.get_positions() {
            symbols.push(mktposition.get_position().symbol.clone());
        }
        symbols
    }
}
