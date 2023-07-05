use std::any::type_name;

pub fn print_type<T>(_: &T) -> &'static str {
    type_name::<T>()
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
