use std::any::type_name;
extern crate chrono;
use chrono::offset::Utc;
use chrono::DateTime;
use std::boxed::Box;
use std::time::SystemTime;
use tokio_postgres::types::ToSql;

pub fn print_type<T>(_: &T) -> &'static str {
    type_name::<T>()
}

pub fn get_utc_now_str(system_time: SystemTime) -> String {
    let datetime: DateTime<Utc> = system_time.into();
    format!("{}", datetime.format("%Y/%m/%d-%H:%M:%S.%f"))
}

pub fn to_sql_type<T>(t: T) -> Box<dyn ToSql + Sync>
where
    T: ToSql + Sync + 'static,
{
    Box::new(t)
}

#[macro_export]
macro_rules! to_num {
    ($value: expr, $denom: expr) => {
        Num::new($value, $denom)
    };
    ($value: expr) => {
        Num::new(($value * 1000.0) as i64, 1000)
    };
}
