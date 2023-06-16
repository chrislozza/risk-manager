use clap::Parser;
use log::{error, info};

mod events;
mod logging;
mod platform;
mod settings;
mod db_client;
mod gcp_client;
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
    type_: String,

    #[arg(short, long)]
    key: String,

    #[arg(short, long)]
    secret: String,

    #[arg(short, long)]
    config_path: String,
}


#[tokio::main]
async fn main() {
    if let Err(err) = log_init() {
        println!("Failed to start logging, error: {err}");
        std::process::exit(1);
    }
    let cmdline_args = Args::parse();
    let is_live = match cmdline_args.type_.as_str() {
        "live" => true,
        "paper" => false,
        _ => {
            error!("Couldn't determine cmdline type");
            std::process::exit(1);
        }
    };

    let (send_mkt_signals, mut receive_mkt_signals) = mpsc::unbounded_channel();

    let settings = Config::read_config_file(cmdline_args.config_path.as_str()).unwrap();
    let mut platform = Platform::new(settings.clone(), cmdline_args.key.as_str(), cmdline_args.secret.as_str(), is_live);
    let mut publisher = EventPublisher::new(settings).await;
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
            _ = signal::ctrl_c() => {
                info!("Keyboard shutdown detected");
                let _ = publisher.shutdown().await;
                let _ = platform.shutdown().await;
                break
            }
            _ = sleep(Duration::from_millis(10)) => {
            }
        }
    }
    std::process::exit(0);
}
