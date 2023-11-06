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
use tracing::debug;
use tracing::info;

#[derive(Debug, Clone)]
pub struct AtrStop {
    pub last_price: Num,
    pub stop_price: Num,
    pub pivot_points: [(i16, f64, f64); 4],
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
        let atr_stop = daily_atr.clone() * multiplier.clone();
        let stop_price = match direction {
            Direction::Long => entry_price - atr_stop.clone(),
            Direction::Short => entry_price + atr_stop.clone(),
        };
        let pivot_points = Self::calculate_pivot_points(atr_stop.to_f64().unwrap());
        info!(
            "For {}, adding new stop_price: {} from atr: {}",
            symbol, stop_price, daily_atr
        );
        AtrStop {
            last_price: stop_price.clone(),
            stop_price: stop_price.clone(),
            pivot_points,
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
        let pivot_points = Self::calculate_pivot_points(1.0);
        AtrStop {
            last_price: stop_price.clone(),
            stop_price,
            pivot_points,
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

    fn calculate_pivot_points(atr_stop: f64) -> [(i16, f64, f64); 4] {
        [
            (1, (atr_stop / 100.0), 1.0),
            (2, (atr_stop * 2.0 / 100.0), 2.0 - (1.0 / atr_stop)),
            (3, (atr_stop * 3.0 / 100.0), 2.0 - (1.0 / atr_stop)),
            (4, (atr_stop * 4.0 / 100.0), 2.0 - (1.0 / atr_stop)),
        ]
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
        entry_price: Num,
        last_price: Num,
        mktdata: &Arc<Mutex<MktData>>,
    ) -> Num {
        if self.daily_atr.is_zero() {
            self.daily_atr = match Self::update_daily_atr(symbol, mktdata).await {
                Ok(atr) => atr,
                Err(err) => panic!("Failed to update atr for {}, errer={}", symbol, err),
            };
            let atr_stop = self.daily_atr.clone() * self.multiplier.clone();
            self.pivot_points = Self::calculate_pivot_points(atr_stop.to_f64().unwrap());
        }
        let daily_atr = self.daily_atr.to_f64().unwrap();
        let price = last_price.to_f64().unwrap();
        let price_change = price - self.watermark.to_f64().unwrap();
        if price_change <= 0.0 {
            return self.stop_price.clone();
        }
        let price = last_price.to_f64().unwrap();
        let entry_price = entry_price.to_f64().unwrap();
        let mut stop_loss_level = self.stop_price.to_f64().unwrap();
        for (zone, zone_watermark, new_trail_factor) in self.pivot_points.iter() {
            match self.direction {
                Direction::Long => {
                    match zone {
                        4 => {
                            if price > (entry_price * (1.0 + zone_watermark)) {
                                // final trail at 1%
                                stop_loss_level = price - (daily_atr * 2.0)
                            } else {
                                // close distance X% -> 1%
                                stop_loss_level += price_change * new_trail_factor
                            }
                        }
                        _ => {
                            if price > entry_price * (1.0 + zone_watermark) {
                                continue;
                            }
                            // set trail based on zone
                            stop_loss_level += new_trail_factor * price_change;
                        }
                    }
                }
                Direction::Short => {
                    match zone {
                        4 => {
                            if price < (entry_price * (1.0 - zone_watermark)) {
                                // final trail at 1%
                                stop_loss_level = price + (daily_atr * 2.0)
                            } else {
                                // close distance X% -> 1%
                                stop_loss_level -= price_change * new_trail_factor
                            }
                        }
                        _ => {
                            if price < entry_price * (1.0 - zone_watermark) {
                                continue;
                            }
                            // set trail based on zone
                            stop_loss_level -= new_trail_factor * price_change;
                        }
                    }
                    debug!(
                        "price {}, entry {}, % {}, stop_loss_level {}, stop_level {}, change {},",
                        price,
                        entry_price,
                        zone_watermark,
                        stop_loss_level,
                        self.stop_price,
                        price_change
                    );
                }
            }
            self.stop_price = self.calculate_atr_stop(last_price.clone());
            self.watermark = self.get_water_mark(last_price.clone());
        }
        self.last_price = last_price.clone();
        self.stop_price.clone()
    }
}
