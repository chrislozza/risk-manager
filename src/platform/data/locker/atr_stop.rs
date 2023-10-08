use crate::events::Direction;
use crate::platform::mktdata::MktData;
use crate::platform::technical_signals::TechnnicalSignals;
use crate::to_num;
use anyhow::Ok;
use anyhow::Result;
use num_decimal::Num;
use std::cmp::max;
use std::cmp::min;
use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct AtrStop {
    pub last_price: Num,
    pub stop_price: Num,
    pub daily_atr: Num,
    pub zone: i16,
    pub multiplier: Num,
    pub watermark: Num,
    pub direction: Direction,
}

impl fmt::Display for AtrStop {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "price[{}], stop[{}], zone[{}]",
            self.last_price.round_with(2),
            self.stop_price.round_with(2),
            self.zone,
        )
    }
}

impl AtrStop {
    pub fn new(
        watermark: Num,
        multiplier: f64,
        direction: Direction,
        zone: i16,
        stop_price: Num,
    ) -> Self {
        let multiplier = to_num!(multiplier);
        AtrStop {
            last_price: stop_price.clone(),
            stop_price,
            daily_atr: to_num!(0.0),
            zone,
            multiplier,
            watermark,
            direction,
        }
    }

    pub fn print_status(&self) -> String {
        format!("{}", self)
    }

    fn calculate_atr_stop(&self, last_price: Num) -> Num {
        match self.direction {
            Direction::Long => max(
                self.stop_price.clone(),
                last_price - (self.daily_atr.clone() * self.multiplier.clone()),
            ),
            Direction::Short => min(
                self.stop_price.clone(),
                last_price + (self.daily_atr.clone() * self.multiplier.clone()),
            ),
        }
    }

    fn get_water_mark(&self, current_price: Num) -> Num {
        match self.direction {
            Direction::Long => max(self.watermark.clone(), current_price),
            Direction::Short => min(self.watermark.clone(), current_price),
        }
    }

    pub async fn price_update(
        &mut self,
        symbol: &str,
        last_price: Num,
        mktdata: &Arc<Mutex<MktData>>,
    ) -> Result<Num> {
        if self.daily_atr.eq(&Num::default()) {
            self.daily_atr = TechnnicalSignals::get_atr(symbol, mktdata).await?;
        }
        self.last_price = last_price.clone();
        self.stop_price = self.calculate_atr_stop(last_price.clone());
        self.watermark = self.get_water_mark(last_price);
        Ok(self.stop_price.clone())
    }
}
