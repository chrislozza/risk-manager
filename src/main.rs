use clap::{App, Arg};
use log::{error, info};

extern crate libc;

mod logging;
mod platform;
mod settings;
mod events;

use crate::logging::SimpleLogger;
use crate::events::EventPublisher;
use crate::platform::Platform;
use crate::settings::Settings;

use tokio::signal;

use log::{LevelFilter, SetLoggerError};

static LOGGER: SimpleLogger = SimpleLogger;

fn log_init() -> Result<(), SetLoggerError> {
    log::set_logger(&LOGGER).map(|()| log::set_max_level(LevelFilter::Info))
}

#[tokio::main]
async fn main() {
    let cmdline_args = App::new("trading-app")
        .version("0.1")
        .author("yours truely")
        .about("learning some rust")
        .arg(
            Arg::with_name("TYPE")
                .takes_value(true)
                .required(true)
                .long("type")
                .short('a')
                .possible_values(&["paper", "live"]),
        )
        .arg(
            Arg::with_name("KEY")
                .takes_value(true)
                .required(true)
                .long("key")
                .short('k'),
        )
        .arg(
            Arg::with_name("SECRET")
                .takes_value(true)
                .required(true)
                .long("secret")
                .short('s'),
        )
        .arg(
            Arg::with_name("CONFIG")
                .takes_value(true)
                .required(true)
                .long("config")
                .short('c'),
        )
        .get_matches();

    match log_init() {
        Err(err) => {
            println!("Failed to start logging, error: {err}");
            std::process::exit(1);
        }
        _ => (),
    };
    let key = cmdline_args.value_of("KEY").unwrap();
    let secret = cmdline_args.value_of("SECRET").unwrap();
    let config = cmdline_args.value_of("CONFIG").unwrap();
    let is_live = match cmdline_args.value_of("TYPE").unwrap() {
        "live" => true,
        "paper" => false,
        _ => {
            error!("Couldn't determine cmdline type");
            std::process::exit(1);
        }
    };

    let _settings = Settings::read_config_file(config);
    let platform = Platform::new(key, secret, is_live);
    let publisher = EventPublisher::new();

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                publisher.shutdown();
                platform.shutdown().await;
                std::process::exit(0)
            },
        }
    }
}
