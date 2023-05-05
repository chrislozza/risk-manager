use std::cmp;
use log::{error, info, warn};

use num_decimal::Num;

pub struct MaxLeverage {}

impl MaxLeverage {
    pub fn get_port_weight(buying_power: Num, portfolio_size: Num, gross_position: Num, equity_with_loan: Num) -> Num {
        let max_port_weight = ((buying_power + gross_position) / equity_with_loan) / Num::new((1.05 * 100.00) as i32, 100);
        return cmp::min(max_port_weight, Num::from(2));
    }
}
