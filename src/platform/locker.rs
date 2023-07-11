use std::collections::HashMap;
use tracing::info;

use crate::to_num;

use num_decimal::Num;

use super::Settings;

#[derive(PartialEq, Clone, Copy)]
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
    settings: Settings,
}

impl Locker {
    pub fn new(settings: Settings) -> Self {
        Locker {
            stops: HashMap::new(),
            settings,
        }
    }

    pub fn monitor_trade(
        &mut self,
        symbol: &String,
        entry_price: &Num,
        strategy: &str,
        t_type: TransactionType,
    ) {
        let strategy_cfg = &self.settings.strategies.configuration[strategy];
        let multiplier = to_num!(strategy_cfg.trailing_size);
        let stop = TrailingStop::new(symbol.clone(), entry_price.clone(), multiplier, t_type);
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

    pub fn get_status(&self, symbol: &str) -> LockerStatus {
        self.stops[symbol].status
    }

    pub fn update_status(&mut self, symbol: &str, status: LockerStatus) {
        if let Some(locker) = self.stops.get_mut(symbol) {
            locker.status = status
        }
    }

    pub fn should_close(&mut self, symbol: &str, trade_price: &Num) -> bool {
        if !self.stops.contains_key(symbol) {
            info!("Symbol: {symbol:?} not being tracked in locker");
            return false;
        }
        if let Some(stop) = &mut self.stops.get_mut(symbol) {
            let stop_price = stop.price_update(trade_price.clone());
            if stop_price > *trade_price {
                stop.status = LockerStatus::Disabled;
                return true;
            }
        }
        false
    }
}

struct TrailingStop {
    symbol: String,
    entry_price: Num,
    trail_pc: Num,
    pivot_points: [(i8, f64, f64); 4],
    high_low: Num,
    stop_loss_level: Num,
    zone: i8,
    status: LockerStatus,
    t_type: TransactionType,
}

impl TrailingStop {
    fn new(symbol: String, entry_price: Num, trail_pc: Num, t_type: TransactionType) -> Self {
        let stop_trail = trail_pc.to_f64().unwrap();
        let pivot_points = [
            (1, (stop_trail / 100.0), 1.0),
            (2, (stop_trail * 2.0 / 100.0), 0.0),
            (3, (stop_trail * 3.0 / 100.0), 2.0),
            (4, (stop_trail * 4.0 / 100.0), 2.0 - (1.0 / stop_trail)),
        ];
        let stop_loss_level = entry_price.clone() * to_num!(1.0 - pivot_points[0].1);
        let high_low = entry_price.clone();
        TrailingStop {
            symbol,
            entry_price,
            trail_pc,
            pivot_points,
            high_low,
            stop_loss_level,
            zone: 0,
            status: LockerStatus::Active,
            t_type,
        }
    }

    fn price_update(&mut self, current_price: Num) -> Num {
        let price = current_price.to_f64().unwrap();
        let price_change = price - self.high_low.to_f64().unwrap();
        if price_change <= 0.0 || self.status == LockerStatus::Disabled {
            return self.stop_loss_level.clone();
        }
        let entry_price = self.entry_price.to_f64().unwrap();
        let mut stop_loss_level = self.stop_loss_level.to_f64().unwrap();
        for pivot in self.pivot_points.iter() {
            let (zone, percentage_change, new_trail_factor) = pivot;
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
            if *zone > self.zone {
                info!(
                    "Price update for symbol: {}, new stop level: {} in zone: {}",
                    self.symbol,
                    self.stop_loss_level.clone().round_with(2),
                    zone
                );
                self.zone = *zone;
            }
            break;
        }
        if self.stop_loss_level != to_num!(stop_loss_level) {
            self.stop_loss_level = Num::new((stop_loss_level * 100.0) as i64, 100);
        }
        if current_price > self.high_low {
            self.high_low = current_price;
        }
        //write to db
        self.stop_loss_level.clone()
    }
}
