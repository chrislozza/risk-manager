use super::mktdata::MktData;
use super::mktorder::MktOrder;
use super::mktposition::MktPosition;
use log::info;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use num_decimal::Num;

#[derive(Debug)]
struct Locker {
    mktdata: Arc<Mutex<MktData>>,
    liqudate: Option<fn()>,
    cancel: Option<fn()>,
    stops: HashMap<String, TrailingStop>,
}

#[derive(Debug)]
pub struct TrailingStop {
    symbol: String,
    entry_price: f64,
    trail_pc: f64,
    pivot_points: [(i8, f64, f64); 4],
    high_low: f64,
    stop_loss_level: f64,
}

impl Locker {
    pub fn new(mktdata: Arc<Mutex<MktData>>) -> Self {
        Locker {
            mktdata,
            liqudate: None,
            cancel: None,
            stops: HashMap::new(),
        }
    }

    pub async fn monitor_trade(&mut self, symbol: &String, entry_price: Num) {
        let stop = TrailingStop::new(symbol.clone(), entry_price.to_f64().unwrap(), 7.0);
        if self.stops.contains_key(symbol) {
            *self.stops.get_mut(symbol).unwrap() = stop;
        } else {
            self.stops.insert(symbol.clone(), stop);
            self.mktdata.lock().unwrap().subscribe(symbol.clone()).await;
        }
    }

    pub fn register_cancel_order(&mut self, cancel_order: fn()) {
        self.cancel = Some(cancel_order)
    }

    pub fn register_liquidate_position(&mut self, liquidate_position: fn()) {
        self.liqudate = Some(liquidate_position)
    }
}

impl TrailingStop {
    fn new(symbol: String, entry_price: f64, trail_pc: f64) -> Self {
        let pivot_points = [
            (1, (trail_pc / 100.0), 1.0),
            (2, (trail_pc * 2.0 / 100.0), 0.0),
            (3, (trail_pc * 3.0 / 100.0), 2.0),
            (4, (trail_pc * 4.0 / 100.0), 2.0 - (1.0 / trail_pc)),
        ];
        let stop_loss_level = entry_price * (1.0 - pivot_points[0].1);
        TrailingStop {
            symbol,
            entry_price,
            trail_pc,
            pivot_points,
            high_low: entry_price,
            stop_loss_level,
        }
    }

    fn price_update(&mut self, current_price: f64) -> f64 {
        let price_change = current_price - self.high_low;
        if price_change <= 0.0 {
            return self.stop_loss_level;
        }
        for pivot in self.pivot_points.iter() {
            let (zone, percentage_change, new_trail_factor) = pivot;
            match zone {
                4 => {
                    // final trail at 1%
                    if current_price > (self.entry_price * (1.0 + percentage_change)) {
                        self.stop_loss_level = current_price - (self.entry_price * 0.01)
                    } else {
                        // close distance X% -> 1%
                        self.stop_loss_level += price_change * new_trail_factor
                    }
                }
                _ => {
                    if current_price > self.entry_price * (1.0 + percentage_change) {
                        continue;
                    }
                    // set trail based on zone
                    self.stop_loss_level = self.stop_loss_level + (new_trail_factor * price_change);
                }
            }
            info!(
                "Price update for symbol: {}, new stop level: {} in zone: {}",
                self.symbol, self.stop_loss_level, zone
            );
            break;
        }
        self.high_low = current_price;
        return self.stop_loss_level;
    }
}
