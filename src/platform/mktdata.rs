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

use tracing::debug; 
use tracing::info; 
use tracing::warn;
use tracing::error; 

use tokio::sync::broadcast;
use super::Event;
use tokio_util::sync::CancellationToken;

use super::data::mktorder::MktOrders;
use super::data::mktposition::MktPositions;

pub struct MktData {
    connectors: Arc<Connectors>,
}

impl MktData {
    pub fn new(connectors: &Arc<Connectors>) -> Self {
        MktData {
            connectors: Arc::clone(connectors)
        }
    }

    fn build_symbol_list(
        &self,
        orders: &MktOrders,
        positions: &MktPositions,
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

    pub async fn startup(
        &mut self,
        orders: &MktOrders,
        positions: &MktPositions,
    ) -> Result<()> {
        let symbols = self.build_symbol_list(orders, positions);
        if symbols.is_empty() {
            info!("No symbols to subscribe to");
            return;
        }

        let symbols = match self
            .connectors
            .subscribe_to_mktdata(symbols.into())
            .await?
        {
            Err(err) => bail!("Failed to subscribe to stream, error: {err:?}"),
            Ok(val) => val,
        };
        self.symbols = symbols;
        Ok(())
    }

    pub async fn shutdown(&self) {
        info!("Shutdown initiated");
    }

    pub async fn get_historical_bars(&self, symbol: &str, days_to_lookback: i64) -> Vec<bars::Bar> {
        let client = self.client.lock().await;
        {
            let today = Utc::now();
            let start_date = today - Duration::days(days_to_lookback);
            let end_date = today - Duration::days(1);
            let request = bars::BarsReqInit {
                limit: Some(days_to_lookback as usize),
                ..Default::default()
            }
            .init(symbol, start_date, end_date, bars::TimeFrame::OneDay);

            let result = self.connectors.get_historical_bars(request).await?
            result.bars
        }
    }

    pub async fn subscribe(&mut self, symbol: String) {
        let symbols = match self
            .connectors
            .subscribe_to_mktdata(vec![symbol.clone()].into())
            .await
        {
            Err(val) => {
                error!("Failed to subscribe {val:?}");
                return;
            }
            Ok(val) => val,
        };
        self.symbols = symbols;
    }

    pub async fn unsubscribe(&mut self, symbol: String) {
        let symbols = match self
            .connectors
            .unsubscribe_from_stream(vec![symbol.clone()].into())
            .await
        {
            Err(val) => {
                error!("Failed to unsubscribe {val:?}");
                return;
            }
            Ok(val) => val,
        };
        self.symbols = symbols;
    }

    pub async fn unsubscribe_all(&mut self) {
        let symbol_list = match self.symbols.clone() {
            stream::Symbols::List(val) => val,
            _ => stream::SymbolList::default(),
        };
        if symbol_list.len() == 0 {
            return;
        }
        let symbols = match self
            .connectors
            .unsubscribe_from_stream(symbol_list)
            .await
        {
            Err(val) => {
                error!("Failed to unsubscribe {val:?}");
                return;
            }
            Ok(val) => val,
        };
        self.symbols = symbols;
    }
}
