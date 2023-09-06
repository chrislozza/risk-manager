use std::cmp;

use ta::indicators::AverageTrueRange;
use ta::{DataItem, Next};

use num_decimal::Num;

use super::mktdata::MktData;
use crate::to_num;

use anyhow::Result;

pub struct RiskManagement {}

impl RiskManagement {
    pub fn get_port_weight(
        buying_power: Num,
        _portfolio_size: Num,
        gross_position: Num,
        equity_with_loan: Num,
    ) -> Num {
        let max_port_weight = ((buying_power + gross_position) / equity_with_loan)
            / Num::new((1.05 * 100.00) as i32, 100);
        cmp::min(max_port_weight, Num::from(2))
    }

    pub async fn get_atr(symbol: &str, mktdata: &MktData) -> Result<Num> {
        let mut indicator = AverageTrueRange::new(14).unwrap();

        let bars = mktdata.get_historical_bars(symbol, 60).await?;
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
        Ok(to_num!(atr))
    }
}
