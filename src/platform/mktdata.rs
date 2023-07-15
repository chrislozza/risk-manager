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

pub struct MktData {
    connectors: Arc<Connectors>,
    snapshots: HashMap<String, Num>>
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

    pub async fn batch_subscribe(&mut self, symbols: Vec<&str>) -> Result<()> {
        let symbols = match self
            .connectors
            .subscribe_to_mktdata(symbols)
            .await?;
        let subscribed = self.subscribed_symbols.write().await;
        subscribed = symbols;
    }

    pub async fn subscribe(&mut self, symbol: &str) -> Result<()> {
        self.batch_subscribe(vec![symbol]).await
    }

    pub async fn unsubscribe(&mut self, symbol: &str) -> Result<()> {
        let symbols = match self
            .connectors
            .unsubscribe_from_stream(vec![symbol.clone()].into())
            .await?;
        self.snapshots.remove(symbol);
    }

    pub fn get_snapshots(&self) -> &HashMap<String, Num> {
        &self.snapshots
    }

    pub fn capture_data(&self, mktdata_update: &stream::Trade) {
        let symbol = mktdata_update.symbol;
        self.snapshots[&symbol] = mktdata_update.trade_price
    }
}
