use clap::Parser;
use log::{error, info};
use tokio_util::sync::CancellationToken;

mod db_client;
mod events;
mod gcp_client;
mod logging;
mod platform;
mod settings;
mod utils;
//mod passwords;

use events::Event;
use events::EventPublisher;
use logging::SimpleLogger;
use platform::Platform;
use settings::{Config, Settings};

use tokio::signal;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

use std::env;

use log::{LevelFilter, SetLoggerError};

static LOGGER: SimpleLogger = SimpleLogger;

fn log_init() -> Result<(), SetLoggerError> {
    log::set_logger(&LOGGER).map(|()| log::set_max_level(LevelFilter::Info))
}

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    settings: String,
}

#[tokio::main]
async fn main() {
    if let Err(err) = log_init() {
        println!("Failed to start logging, error: {err}");
        std::process::exit(1);
    }
    let cmdline_args = Args::parse();
    let settings = match Config::read_config_file(&cmdline_args.settings.as_str()) {
        Err(val) => {
            error!("Settings file not found: {val}");
            std::process::exit(1);
        },
        Ok(val) => val,
    };
    let is_live = match settings.account_type.as_str() {
        "live" => true,
        "paper" => false,
        val => {
            error!("Couldn't determine cmdline type [{val:?}] account [{}]", settings.account_type);
            std::process::exit(1);
        }
    };

    let (send_mkt_signals, mut receive_mkt_signals) = mpsc::unbounded_channel();
    let shutdown_signal = CancellationToken::new();

    let key = env::var("KEY").expect("Failed to read the 'key' environment variable.");
    let secret = env::var("SECRET").expect("Failed to read the 'secret' environment variable.");

    info!("Found key: {key}, secret: {secret}");

    let mut platform = Platform::new(
        shutdown_signal.clone(),
        settings.clone(),
        &key,
        &secret,
        is_live,
    );
    let mut publisher = EventPublisher::new(shutdown_signal.clone(), settings).await;
    info!("Initialised components");

    let _ = platform.run().await;
    let _ = publisher.run(&send_mkt_signals).await;
    info!("Taking a loop in the app");
    loop {
        tokio::select! {
            event = receive_mkt_signals.recv() => {
                if let Event::MktSignal(event) = event.unwrap() {
                    info!("Recieved an event {event:?}, creating new position");
                    let _ = platform.create_position(&event).await;
                }
            }
            _ = shutdown_signal.cancelled() => {
                let _ = publisher.shutdown().await;
                let _ = platform.shutdown().await;
                break
            }
            _ = signal::ctrl_c() => {
                info!("Keyboard shutdown detected");
                shutdown_signal.cancel();
            }
            _ = sleep(Duration::from_millis(10)) => {
            }
        }
    }
    std::process::exit(0);
}
