use std::any::type_name;
extern crate chrono;
use chrono::offset::Utc;
use chrono::DateTime;
use std::time::SystemTime;

pub fn print_type<T>(_: &T) -> &'static str {
    type_name::<T>()
}

pub fn get_utc_now_str(system_time: SystemTime) -> String {
    let datetime: DateTime<Utc> = system_time.into();
    format!("{}", datetime.format("%Y/%m/%d-%H:%M:%S.%f"))
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
