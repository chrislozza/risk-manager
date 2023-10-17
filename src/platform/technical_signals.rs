use anyhow::Result;
use num_decimal::Num;
use std::sync::Arc;
use ta::indicators::AverageTrueRange;
use ta::DataItem;
use ta::Next;
use tokio::sync::Mutex;
use tracing::info;

use super::mktdata::MktData;
use crate::to_num;

pub struct TechnnicalSignals {}

impl TechnnicalSignals {
    pub async fn get_atr(symbol: &str, mktdata: &Arc<Mutex<MktData>>) -> Result<Num> {
        let mut indicator = AverageTrueRange::new(14).unwrap();

        let bars = mktdata.lock().await.get_historical_bars(symbol, 60).await?;
        let mut atr: f64 = 0.0;
        for data in &bars {
            if let Ok(data_item) = DataItem::builder()
                .high(data.high.to_f64().unwrap())
                .low(data.low.to_f64().unwrap())
                .close(data.close.to_f64().unwrap())
                .open(data.open.to_f64().unwrap())
                .volume(data.volume as f64)
                .build()
            {
                atr = indicator.next(&data_item);
            }
        }
        let atr = to_num!(atr);
        info!("Symbol [{}] todays atr: {}", symbol, atr);
        Ok(atr)
    }
}
