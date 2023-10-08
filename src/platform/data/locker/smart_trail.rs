use anyhow::Result;
use num_decimal::Num;
use std::fmt;
use tracing::debug;
use tracing::info;

use crate::events::Direction;
use crate::to_num;

#[derive(Debug)]
pub struct SmartTrail {
    pub current_price: Num,
    pub stop_price: Num,
    pub pivot_points: [(i16, f64, f64); 4],
    pub watermark: Num,
    pub zone: i16,
    pub multiplier: f64,
    pub direction: Direction,
}

impl fmt::Display for SmartTrail {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "price[{}], stop[{}], zone[{}]",
            self.current_price.round_with(3).to_f64().unwrap(),
            self.stop_price.round_with(3).to_f64().unwrap(),
            self.zone,
        )
    }
}

impl SmartTrail {
    pub fn new(
        entry_price: Num,
        watermark: Num,
        multiplier: f64,
        direction: Direction,
        zone: i16,
        mut stop_price: Num,
    ) -> Self {
        let pivot_points = Self::calculate_pivot_points(multiplier);
        if stop_price.eq(&Num::default()) {
            stop_price = match direction {
                Direction::Long => entry_price.clone() * to_num!(1.0 - pivot_points[0].1),
                Direction::Short => entry_price.clone() * to_num!(1.0 + pivot_points[0].1),
            };
        }
        let current_price = stop_price.clone();
        SmartTrail {
            current_price,
            pivot_points,
            stop_price,
            watermark,
            zone,
            multiplier,
            direction,
        }
    }

    pub fn print_status(&self) -> String {
        format!("{}", self)
    }

    fn calculate_pivot_points(multiplier: f64) -> [(i16, f64, f64); 4] {
        [
            (1, (multiplier / 100.0), 1.0),
            (2, (multiplier * 2.0 / 100.0), 0.0),
            (3, (multiplier * 3.0 / 100.0), 2.0),
            (4, (multiplier * 4.0 / 100.0), 2.0 - (1.0 / multiplier)),
        ]
    }

    pub async fn price_update(&mut self, entry_price: Num, last_price: Num) -> Result<Num> {
        self.current_price = last_price.clone();
        let price = last_price.to_f64().unwrap();
        let price_change = price - self.watermark.to_f64().unwrap();
        if price_change <= 0.0 {
            return Ok(self.stop_price.clone());
        }
        let entry_price = entry_price.to_f64().unwrap();
        let mut stop_loss_level = self.stop_price.to_f64().unwrap();
        for (zone, percentage_change, new_trail_factor) in self.pivot_points.iter() {
            match self.direction {
                Direction::Long => {
                    match zone {
                        4 => {
                            if price > (entry_price * (1.0 + percentage_change)) {
                                // final trail at 1%
                                stop_loss_level = price - (entry_price * 0.01)
                            } else {
                                // close distance X% -> 1%
                                stop_loss_level += price_change * new_trail_factor
                            }
                        }
                        _ => {
                            if price > entry_price * (1.0 + percentage_change) {
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
                            if price < (entry_price * (1.0 - percentage_change)) {
                                // final trail at 1%
                                stop_loss_level = price + (entry_price * 0.01)
                            } else {
                                // close distance X% -> 1%
                                stop_loss_level -= price_change * new_trail_factor
                            }
                        }
                        _ => {
                            if price < entry_price * (1.0 - percentage_change) {
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
                        percentage_change,
                        stop_loss_level,
                        self.stop_price,
                        price_change
                    );
                }
            }
            if *zone > self.zone {
                info!(
                    "Zone update for stop: last price: {} new stop level: {} in zone: {}, direction: {}",
                    last_price.clone().round_with(2),
                    self.stop_price.clone().round_with(2),
                    zone,
                    self.direction
                );
                self.zone = *zone;
            }
            break;
        }

        let stop_loss_level = to_num!(stop_loss_level);

        self.stop_price = stop_loss_level.clone();
        self.watermark = last_price;
        Ok(stop_loss_level)
    }
}
