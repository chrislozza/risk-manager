
use crate::platform::mktdata::MktData;


pub struct Locker {
    mktdata: Option<&MktData>,
    liqudateFn: Option<impl Fn>,
    cancelFn: Option<impl Fn>
}

impl Locker {

    pub fn monitor_order(order: MktOrder) {
    }

    pub fn monitor_position(position: MktPosition) {
    }

    pub fn register_cancel_order(&mut self, cancel_order: impl Fn) {
        self.cancelFn = Some(cancel_order)
    }

    pub fn register_liquidate_position(&mut self, liquidate_position: impl Fn) {
        self.liquidateFn = Some(liquidate_position)
    }

    fn subscribe_to_mktdata(&self, symbol: String) {
        self.mktdata.subscribe_to_stream(symbol)
    }
}
//struct TrailingStop {
//    entry_price: f64,
//    pivot_points: [(f64, f64); 5], // 5 pivot points as tuples of (percentage change, trailing factor)
//    trailing_factor: f64,
//    stop_loss_level: f64,
//}
//
//impl TrailingStop {
//    fn new(entry_price: f64) -> Self {
//        let pivot_points = [
//            (0.07, 1.0), // pivot point 1: 7% profit, trail factor 1.0
//            (0.14, 1.0), // pivot point 2: 14% profit, trail factor 1.0
//            (0.21, 0.93), // pivot point 3: 21% profit, trail factor 0.93 (7% below current price)
//            (0.28, 0.99), // pivot point 4: 28% profit, trail factor 0.99 (1% below current price)
//            (0.35, 0.99), // pivot point 5: 35% profit, trail factor 0.99 (1% below current price)
//        ];
//        let trailing_factor = 1.0;
//        let stop_loss_level = entry_price * (1.0 - trailing_factor);
//        TrailingStop {
//            entry_price,
//            pivot_points,
//            trailing_factor,
//            stop_loss_level,
//        }
//    }
//    
//    fn update_stop_loss(&mut self, current_price: f64) {
//        let price_change = (current_price - self.entry_price) / self.entry_price;
//        let mut trail_factor = self.trailing_factor;
//        let mut stop_loss_level = self.stop_loss_level;
//        for (percentage_change, new_trail_factor) in &self.pivot_points {
//            if price_change >= *percentage_change {
//                let new_stop_loss_level = current_price * (1.0 - new_trail_factor);
//                if new_stop_loss_level > stop_loss_level {
//                    stop_loss_level = new_stop_loss_level;
//                    trail_factor = *new_trail_factor;
//                }
//            } else {
//                break;
//            }
//        }
//        self.stop_loss_level = stop_loss_level;
//        self.trailing_factor = trail_factor;
//    }
//}
//
//
//
//let entry_price = ...; // Enter the position price
//let pivot_points = [7, 14, 21, 28, 35]; // Pivot points at 7% intervals
//let trailing_factors = [1.0, 1.0, 1.0, 0.5, 0.25]; // Trailing factors for each pivot point
//let trailing_percents = [7.0, 7.0, 7.0, 14.0, 7.0]; // Trailing percents for each pivot point
//let mut stop_loss = entry_price * (1.0 - trailing_percents[0] / 100.0); // Initial stop loss
//
//for (i, price) in stock_prices.iter().enumerate() {
//    let profit = (price - entry_price) / entry_price * 100.0; // Profit percentage
//    let trailing_factor = trailing_factors[3] + (trailing_factors[4] - trailing_factors[3]) * ((profit - pivot_points[3]) / (pivot_points[4] - pivot_points[3])); // Calculate dynamic trailing factor between pivot points 3 and 4
//    let trailing_percent = if profit <= pivot_points[0] {
//        trailing_percents[0]
//    } else if profit <= pivot_points[1] {
//        entry_price * (1.0 - trailing_percents[1] / 100.0) / entry_price * (1.0 + profit / 100.0) - 1.0
//    } else if profit <= pivot_points[2] {
//        entry_price * (1.0 - trailing_percents[2] / 100.0) / entry_price * (1.0 + profit / 100.0) - 1.0
//    } else if profit <= pivot_points[3] {
//        entry_price * (1.0 - trailing_percents[3] / 100.0) / entry_price * (1.0 + profit / 100.0) - 1.0
//    } else {
//        entry_price * (1.0 - trailing_percents[4] / 100.0) / entry_price * (1.0 + profit / 100.0) - 1.0
//    };
//    stop_loss = price * (1.0 - trailing_factor * trailing_percent / 100.0); // Update stop loss level
//}

