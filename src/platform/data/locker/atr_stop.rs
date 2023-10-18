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
    pub last_price: Option<Num>,
    pub stop_price: Option<Num>,
    pub daily_atr: Num,
    pub zone: i16,
    pub multiplier: Num,
    pub watermark: Num,
    pub direction: Direction,
}

impl fmt::Display for AtrStop {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(stop_price) = &self.stop_price {
            write!(
                f,
                "price[{}], stop[{}], zone[{}]",
                self.last_price.as_ref().unwrap().round_with(2),
                stop_price.round_with(2),
                self.zone,
            )
        } else {
            write!(f, "zone[{}]", self.zone)
        }
    }
}

impl AtrStop {
    pub fn new(
        watermark: Num,
        multiplier: f64,
        direction: Direction,
        zone: i16,
        stop_price: Option<Num>,
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
                match &self.stop_price {
                    Some(last_stop) => max(calculated_stop, last_stop.clone()),
                    None => calculated_stop,
                }
            }
            Direction::Short => {
                let calculated_stop =
                    last_price + (self.daily_atr.clone() * self.multiplier.clone());
                match &self.stop_price {
                    Some(last_stop) => min(calculated_stop, last_stop.clone()),
                    None => calculated_stop,
                }
            }
        }
    }

    fn get_water_mark(&self, current_price: Num) -> Num {
        match self.direction {
            Direction::Long => max(self.watermark.clone(), current_price),
            Direction::Short => min(self.watermark.clone(), current_price),
        }
    }

    pub async fn update_daily_atr(
        &mut self,
        symbol: &str,
        mktdata: &Arc<Mutex<MktData>>,
    ) -> Result<()> {
        if self.daily_atr.eq(&Num::default()) {
            self.daily_atr = TechnnicalSignals::get_atr(symbol, mktdata).await?;
        }
        Ok(())
    }

    pub async fn price_update(&mut self, _symbol: &str, last_price: Num) -> Option<Num> {
        self.last_price = Some(last_price.clone());
        let stop_price = self.calculate_atr_stop(last_price.clone());
        self.stop_price = Some(stop_price.clone());
        self.watermark = self.get_water_mark(last_price);
        Some(stop_price)
    }
}
