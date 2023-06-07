use chrono::prelude::*;
use log::{Level, Metadata, Record};

pub struct SimpleLogger;

impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Info
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            let current_time = Local::now();
            let timestamp = current_time.format("%Y-%m-%d %H:%M:%S").to_string();
            let location = format!("{}:{}", record.file().unwrap(), record.line().unwrap());
            println!(
                "{:<30} {:<10} {:<40.40}  -  {}",
                timestamp,
                record.level(),
                location,
                record.args()
            );
        }
    }

    fn flush(&self) {}
}

