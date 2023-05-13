use std::any::type_name;

pub fn print_type<T>(_: &T) -> &'static str {
    type_name::<T>()
}
