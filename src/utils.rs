use num_decimal::Num;
use std::any::type_name;

pub fn print_type<T>(_: &T) -> &'static str {
    type_name::<T>()
}

pub fn round_to(value: Num, decimal_places: u32) -> Num {
    let multiplier = Num::from(decimal_places);
    (value * multiplier.clone()).round() / multiplier
}

//macro_rules! float_to_num {
//    ($value: expr, $denom: ) => {
//        Num::new($value, $denom)
//    };
//    ($value: expr) => {
//        Num::new($value * 100, 100)
//    };
//}
