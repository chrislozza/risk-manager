use std::any::type_name;

use num_decimal::Num;

pub fn print_type<T>(_: &T) -> &'static str {
    type_name::<T>()
}


macro_rules! float_to_num {
    ($value: expr, $denom: ) => {
        Num::new($value, $denom)
    };
    ($value: expr) => {
        Num::new($value * 100, 100)
    };
}
