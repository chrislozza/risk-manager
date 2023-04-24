use clap::{App, Arg};
use log::{error, info};
use tokio::time::{sleep, Duration};

mod platform;
mod logging;

use crate::platform::Platform;
use crate::logging::SimpleLogger;

use log::{SetLoggerError, LevelFilter};

static LOGGER: SimpleLogger = SimpleLogger;

pub fn log_init() -> Result<(), SetLoggerError> {
    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(LevelFilter::Info))
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
        .get_matches();

    match log_init() {
        Err(err) => {
            println!("Failed to start logging, error: {err}");
            std::process::exit(1);
        },
        _ => ()
    };
    let key = cmdline_args.value_of("KEY").unwrap();
    let secret = cmdline_args.value_of("SECRET").unwrap();
    let is_live = match cmdline_args.value_of("TYPE").unwrap() {
        "live" => true,
        "paper" => false,
        _ => panic!("Couldn't determine cmdline type"),
    };
    let mut platform = Platform::new(key, secret, is_live);
    match platform.startup(is_live).await {
        Ok(_) => info!("Startup complete"),
        Err(err) => {
            error!("Failed to startup, error: {err}");
            std::process::exit(1);
        }
    };
    loop {
        if platform.poll().await {
            info!("Startup complete");
            break;
        }
        sleep(Duration::from_millis(5000)).await;
    }
}
