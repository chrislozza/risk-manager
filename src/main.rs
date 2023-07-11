use clap::Parser;

use apca::api::v2::updates::OrderUpdate;
use apca::data::v2::stream::Trade;

use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

mod db_client;
mod events;
mod gcp_client;
mod logging;
mod platform;
mod settings;
mod utils;
//mod passwords;

use events::EventPublisher;
use events::MktSignal;
use logging::CloudLogging;
use platform::Platform;
use settings::Config;
use settings::Settings;

use tokio::signal;

use tokio::time::sleep;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

use std::env;

#[derive(Debug, Clone)]
pub enum Event {
    Trade(Trade),
    OrderUpdate(OrderUpdate),
    MktSignal(MktSignal),
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    settings: String,
}

#[tokio::main]
async fn main() {
    let cmdline_args = Args::parse();
    let settings = match Config::read_config_file(cmdline_args.settings.as_str()) {
        Err(val) => {
            println!("Settings file error: {val}");
            std::process::exit(1);
        }
        Ok(val) => val,
    };

    let shutdown_signal = CancellationToken::new();
    let _logger = CloudLogging::new(
        settings.gcp_log_name.clone(),
        settings.gcp_project_id.clone(),
        shutdown_signal.clone(),
    )
    .await
    .unwrap();
    let is_live = match settings.account_type.as_str() {
        "live" => true,
        "paper" => false,
        val => {
            error!(
                "Couldn't determine cmdline type [{val:?}] account [{}]",
                settings.account_type
            );
            std::process::exit(1);
        }
    };

    info!("Starting trading app");

    let key = env::var("KEY").expect("Failed to read the 'key' environment variable.");
    let secret = env::var("SECRET").expect("Failed to read the 'secret' environment variable.");

    info!("Found key: {key}, secret: {secret}");

    let mut platform = Platform::new(
        settings.clone(),
        &key,
        &secret,
        is_live,
        shutdown_signal.clone(),
    )
    .await
    .unwrap();
    let mut publisher = EventPublisher::new(shutdown_signal.clone(), settings)
        .await
        .unwrap();
    info!("Initialised components");

    match platform.startup().await {
        Ok(event_handler) => event_handler,
        _ => {
            error!("Platform failed in startup, exiting app...");
            std::process::exit(1);
        }
    };
    let mut publisher_events = match publisher.startup().await {
        Ok(event_handler) => event_handler,
        _ => {
            error!("Publisher failed in startup, exiting app...");
            std::process::exit(1);
        }
    };
    let mut is_graceful_shutdown = false;
    let _ = platform.run().await;
    let _ = publisher.run().await;
    loop {
        tokio::select! {
            event = publisher_events.recv() => {
                if let Event::MktSignal(event) = event.unwrap() {
                    info!("Recieved an event {event:?}, creating new position");
                    if let Err(err) = platform.create_position(&event).await {
                        warn!("Signal dropped {event:?}, error: {err}");
                    }
                }
            }
            _ = shutdown_signal.cancelled() => {
                if is_graceful_shutdown {
                    std::process::exit(0);
                }
                else {
                    warn!("exiting early");
                    std::process::exit(1)
                }
            }
            _ = signal::ctrl_c() => {
                is_graceful_shutdown = true;
                info!("Graceful shutdown initiated");
                shutdown_signal.cancel();
            }
            _ = sleep(Duration::from_secs(180)) => {
                info!("Printing status updates");
                platform.print_status().await;
            }
        }
    }
}
