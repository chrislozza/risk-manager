use apca::data::v2::bars;
use apca::data::v2::stream;

use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;

use tracing::info;

use anyhow::Result;

use num_decimal::Num;
use std::vec::Vec;

use super::web_clients::Connectors;

#[derive(Default, Debug, Clone)]
pub struct Snapshot {
    pub last_price: Num,
    pub last_seen: DateTime<Utc>,
}

impl Snapshot {
    pub fn new(last_price: Num) -> Self {
        Snapshot {
            last_price,
            last_seen: Utc::now(),
        }
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

pub struct MktData {
    connectors: Arc<Connectors>,
    snapshots: HashMap<String, Option<Snapshot>>,
}

impl MktData {
    pub fn new(connectors: &Arc<Connectors>) -> Self {
        MktData {
            connectors: Arc::clone(connectors),
            snapshots: HashMap::default(),
        }
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
            let _ = self.batch_subscribe(symbols).await?;
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
        let _ = self.batch_subscribe(symbols).await?;
        self.snapshots.insert(symbol.to_string(), None);
        Ok(())
    }

    pub async fn unsubscribe(&mut self, symbol: &str) -> Result<()> {
        info!("Unsubscribing from market data for symbol: {}", symbol);
        let _ = self
            .connectors
            .unsubscribe_from_symbols(vec![symbol.to_string()].into())
            .await?;
        self.snapshots.remove(symbol);
        std::result::Result::Ok(())
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

    pub fn capture_data(&mut self, mktdata_update: &stream::Trade) {
        let symbol = &mktdata_update.symbol;
        let last_price = mktdata_update.trade_price.clone();
        match &mut self.snapshots.get_mut(symbol).unwrap() {
            Some(snapshot) => snapshot.last_price = last_price,
            None => {
                let snapshot = Snapshot::new(last_price);
                self.snapshots.insert(symbol.clone(), Some(snapshot));
            }
        }
    }
}
