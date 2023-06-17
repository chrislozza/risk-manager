use apca::data::v2::stream;
use log::info;
use std::collections::HashMap;

use num_decimal::Num;

#[derive(PartialEq)]
pub enum LockerStatus {
    Active,
    Disabled,
    Finished,
}

#[derive(Debug, Clone, Copy)]
pub enum TransactionType {
    Order,
    Position,
}

pub struct Locker {
    stops: HashMap<String, TrailingStop>,
}

struct TrailingStop {
    symbol: String,
    entry_price: f64,
    trail_pc: f64,
    pivot_points: [(i8, f64, f64); 4],
    high_low: f64,
    stop_loss_level: f64,
    zone: i8,
    status: LockerStatus,
    t_type: TransactionType,
}

impl Locker {
    pub fn new() -> Self {
        Locker {
            stops: HashMap::new(),
        }
    }

    pub fn monitor_trade(&mut self, symbol: &String, entry_price: &Num, t_type: TransactionType) {
        let stop = TrailingStop::new(symbol.clone(), entry_price.to_f64().unwrap(), 7.0, t_type);
        if self.stops.contains_key(symbol) {
            info!("Locker monitoring update symbol: {symbol} entry price: {entry_price} transaction: {t_type:?}");
            *self.stops.get_mut(symbol).unwrap() = stop;
        } else {
            info!("Locker monitoring new symbol: {symbol} entry price: {entry_price} transaction: {t_type:?}");
            self.stops.insert(symbol.clone(), stop);
        }
    }

    pub fn complete(&mut self, symbol: &str) {
        if let Some(mut stop) = self.stops.remove(symbol) {
            info!("Locker tracking symbol: {symbol} marked as complete");
            stop.status = LockerStatus::Finished;
            //write to db
        }
    }

    pub fn revive(&mut self, symbol: &str) {
        if let Some(stop) = self.stops.get_mut(symbol) {
            info!("Locker tracking symbol: {symbol} re-enabled");
            stop.status = LockerStatus::Active;
            //write to db
        }
    }

    pub fn get_transaction_type(&mut self, symbol: &str) -> &TransactionType {
        &self.stops[symbol].t_type
    }

    pub fn get_status(&mut self, symbol: &str) -> &LockerStatus {
        &self.stops[symbol].status
    }

    pub fn should_close(&mut self, last_trade: &stream::Trade) -> bool {
        let symbol = last_trade.symbol.as_str();
        if !self.stops.contains_key(symbol) {
            info!("Symbol: {symbol:?} not being tracked in locker");
            return false;
        }
        let trade_price = last_trade.trade_price.to_f64().unwrap();
        if let Some(stop) = &mut self.stops.get_mut(symbol) {
            let stop_price = stop.price_update(trade_price);
            if stop_price > trade_price {
                stop.status = LockerStatus::Disabled;
                return true;
            }
        }
        return false;
    }
}

impl TrailingStop {
    fn new(symbol: String, entry_price: f64, trail_pc: f64, t_type: TransactionType) -> Self {
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
            zone: 0,
            status: LockerStatus::Active,
            t_type,
        }
    }

    fn price_update(&mut self, current_price: f64) -> f64 {
        let price_change = current_price - self.high_low;
        if price_change <= 0.0 || self.status == LockerStatus::Disabled {
            return self.stop_loss_level;
        }
        for pivot in self.pivot_points.iter() {
            let (zone, percentage_change, new_trail_factor) = pivot;
            match zone {
                4 => {
                    if current_price > (self.entry_price * (1.0 + percentage_change)) {
                        // final trail at 1%
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
                    self.stop_loss_level += new_trail_factor * price_change;
                }
            }
            if *zone > self.zone {
                info!(
                    "Price update for symbol: {}, new stop level: {} in zone: {}",
                    self.symbol, self.stop_loss_level, zone
                );
                self.zone = *zone;
            }
            break;
        }
        self.high_low = current_price;
        //write to db
        self.stop_loss_level
    }
}
