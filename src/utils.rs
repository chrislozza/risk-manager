use num_decimal::Num;
use std::any::type_name;

pub fn print_type<T>(_: &T) -> &'static str {
    type_name::<T>()
}

pub fn round_to(value: Num, decimal_places: u32) -> Num {
    let multiplier = Num::from(decimal_places);
    (value * multiplier.clone()).round() / multiplier
}

#[macro_export]
macro_rules! float_to_num {
    ($value: expr, $denom: expr) => {
        Num::new($value, $denom)
    };
    ($value: expr) => {
        Num::new(($value * 1000.0) as i64, 1000)
    };
}
