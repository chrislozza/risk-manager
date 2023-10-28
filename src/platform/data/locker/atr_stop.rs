use crate::events::Direction;
use crate::platform::mktdata::MktData;
use crate::platform::technical_signals::TechnnicalSignals;
use crate::to_num;
use anyhow::Result;
use num_decimal::Num;
use std::cmp::max;
use std::cmp::min;
use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

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
        symbol: &str,
        multiplier: f64,
        direction: Direction,
        entry_price: Num,
        daily_atr: Num,
    ) -> Self {
        let multiplier = to_num!(multiplier);
        let stop_price = match direction {
            Direction::Long => entry_price - (daily_atr.clone() * multiplier.clone()),
            Direction::Short => entry_price + (daily_atr.clone() * &multiplier.clone()),
        };
        info!(
            "For {}, adding new stop_price: {} from atr: {}",
            symbol, stop_price, daily_atr
        );
        AtrStop {
            last_price: stop_price.clone(),
            stop_price: stop_price.clone(),
            daily_atr,
            zone: 0,
            multiplier,
            watermark: stop_price,
            direction,
        }
    }

    pub fn from_db(
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
            Direction::Long => {
                let calculated_stop =
                    last_price - (self.daily_atr.clone() * self.multiplier.clone());
                max(calculated_stop, self.stop_price.clone())
            }
            Direction::Short => {
                let calculated_stop =
                    last_price + (self.daily_atr.clone() * self.multiplier.clone());
                min(calculated_stop, self.stop_price.clone())
            }
        }
    }

    fn get_water_mark(&self, current_price: Num) -> Num {
        match self.direction {
            Direction::Long => max(self.watermark.clone(), current_price),
            Direction::Short => min(self.watermark.clone(), current_price),
        }
    }

    pub async fn update_daily_atr(symbol: &str, mktdata: &Arc<Mutex<MktData>>) -> Result<Num> {
        TechnnicalSignals::get_atr(symbol, mktdata).await
    }

    pub async fn price_update(
        &mut self,
        symbol: &str,
        last_price: Num,
        mktdata: &Arc<Mutex<MktData>>,
    ) -> Num {
        if self.daily_atr.is_zero() {
            self.daily_atr = match Self::update_daily_atr(symbol, mktdata).await {
                Ok(atr) => atr,
                Err(err) => panic!("Failed to update atr for {}, errer={}", symbol, err),
            };
        }
        self.last_price = last_price.clone();
        self.stop_price = self.calculate_atr_stop(last_price.clone());
        self.watermark = self.get_water_mark(last_price);
        self.stop_price.clone()
    }
}
