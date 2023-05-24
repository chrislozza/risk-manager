use clap::{App, Arg};
use log::{error, info};

extern crate libc;

mod logging;
mod platform;
mod settings;
mod events;

use logging::SimpleLogger;
use events::EventPublisher;
use platform::Platform;
use settings::{Settings, Config};
use events::Event;

use tokio::signal;
use tokio::sync::{broadcast, mpsc};

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

    let (send_mkt_signals, mut receive_mkt_signals) = mpsc::unbounded_channel();

    let settings = Config::read_config_file(config).unwrap();
    let mut platform = Platform::new(settings.clone(), key, secret, is_live);
    let mut publisher = EventPublisher::new(settings).await;
    info!("Initialised components");

    platform.run().await;
    publisher.run(&send_mkt_signals).await;
    loop {
        info!("Taking a loop in the app");
        tokio::select! {
            event = receive_mkt_signals.recv() => {
                match event.unwrap() {
                    Event::MktSignal(event) => {
                        info!("Recieved an event {event:?}, creating new position");
                        platform.create_position(&event).await;
                    },
                    _ => (),
                }
            }
            _ = signal::ctrl_c() => {
                info!("Keyboard shutdown detected");
                publisher.shutdown().await;
                platform.shutdown().await;
                break
            },
        }
    }
    std::process::exit(0);
}
