use super::web_clients::Connectors;
use anyhow::Result;
use apca::data::v2::bars;
use apca::data::v2::stream;
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use num_decimal::Num;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::vec::Vec;
use tokio::sync::Mutex;
use tracing::info;
use tracing::warn;

#[derive(Default, Debug, Clone)]
pub struct Snapshot {
    pub bid_price: Num,
    pub sell_price: Num,
    pub mid_price: Num,
    pub last_seen: DateTime<Utc>,
}

impl fmt::Display for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "bid[{}], ask[{}], mid[{}]",
            self.bid_price.round_with(2),
            self.sell_price.round_with(2),
            self.mid_price.round_with(2),
        )
    }
}

impl Snapshot {
    pub fn new(bid: Num, ask: Num) -> Self {
        let mid = (ask.clone() - bid.clone()) / 2 + bid.clone();
        Snapshot {
            bid_price: bid,
            sell_price: ask,
            mid_price: mid,
            last_seen: Utc::now(),
        }
    }

    pub fn update(&mut self, bid: Num, ask: Num) {
        let last_seen = self.last_seen;
        *self = Self::new(bid, ask);
        self.last_seen = last_seen;
    }

    pub fn is_periodic_check(&mut self) -> bool {
        let now = Utc::now();
        if now < self.last_seen + Duration::seconds(5) {
            self.last_seen = now;
            return true;
        }
        false
    }
}

#[derive(Debug, Clone)]
pub struct MktData {
    connectors: Arc<Connectors>,
    snapshots: HashMap<String, Option<Snapshot>>,
}

impl MktData {
    pub fn new(connectors: &Arc<Connectors>) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(MktData {
            connectors: Arc::clone(connectors),
            snapshots: HashMap::default(),
        }))
    }

    pub async fn get_historical_bars(
        &self,
        symbol: &str,
        days_to_lookback: i64,
    ) -> Result<Vec<bars::Bar>> {
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

    pub async fn startup(&mut self, symbols: Vec<String>) -> Result<()> {
        if !symbols.is_empty() {
            for symbol in &symbols {
                self.snapshots
                    .entry(symbol.to_string())
                    .or_insert_with(|| None);
            }
            self.batch_subscribe(symbols).await?
        }
        info!("Mktdata startup complete");
        Ok(())
    }

    async fn batch_subscribe(&self, symbols: Vec<String>) -> Result<()> {
        info!("Batch subscribing to symbols {symbols:?}");
        self.connectors.subscribe_to_symbols(symbols.into()).await
    }

    pub async fn subscribe(&mut self, symbol: &str) -> Result<()> {
        let symbols = vec![symbol.to_string()];
        self.batch_subscribe(symbols).await?;
        self.snapshots.insert(symbol.to_string(), None);
        Ok(())
    }

    pub async fn unsubscribe(&mut self, symbol: &str) -> Result<()> {
        info!("Unsubscribing from market data for symbol: {}", symbol);
        self.connectors
            .unsubscribe_from_symbols(vec![symbol.to_string()].into())
            .await?;
        self.snapshots.remove(symbol);
        Ok(())
    }

    pub fn get_snapshots(&mut self) -> HashMap<String, Snapshot> {
        let mut to_check = HashMap::default();
        for (symbol, snapshot) in &mut self.snapshots {
            if let Some(snapshot) = snapshot {
                if snapshot.is_periodic_check() {
                    to_check.insert(symbol.clone(), snapshot.clone());
                }
            }
        }
        to_check
    }

    pub fn capture_data(&mut self, mktdata_update: &stream::Quote) {
        let symbol = &mktdata_update.symbol;
        let bid = &mktdata_update.ask_price;
        let ask = &mktdata_update.bid_price;
        if let Some(wrapped_snapshot) = &mut self.snapshots.get_mut(symbol) {
            match wrapped_snapshot {
                Some(snapshot) => {
                    snapshot.update(bid.clone(), ask.clone());
                }
                None => {
                    let snapshot = Snapshot::new(bid.clone(), ask.clone());
                    self.snapshots.insert(symbol.clone(), Some(snapshot));
                }
            }
        } else {
            warn!("Symbol[{}] not found in mktdata update", symbol);
        }
    }
}
