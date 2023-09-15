use clap::Parser;

use apca::api::v2::updates::OrderUpdate;
use apca::data::v2::stream::Bar;
use apca::data::v2::stream::Quote;
use apca::data::v2::stream::Trade;

use tokio::sync::broadcast::error::RecvError;

use tracing::error;
use tracing::info;
use tracing::warn;

mod events;
mod gcp_client;
mod logging;
mod platform;
mod settings;
mod utils;

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
    Quote(Quote),
    Bar(Bar),
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
        settings.log_level.clone(),
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

    info!("**************** Let the trading begin! ****************\n");

    let key = env::var("API_KEY").expect("Failed to read the 'key' environment variable.");
    let secret = env::var("API_SECRET").expect("Failed to read the 'secret' environment variable.");

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
                match event {
                    Ok(Event::MktSignal(event)) => {
                        info!("Recieved an event {event:?}, creating new position");
                        if let Err(err) = platform.create_position(&event).await {
                            warn!("Signal dropped {event:?}, error: {err}");
                        }
                    },
                    Ok(_) => (),
                    Err(RecvError::Lagged(err)) => warn!("Publisher channel skipping a number of messages: {}", err),
                    Err(RecvError::Closed) => {
                        error!("Publisher channel closed");
                        shutdown_signal.cancel()
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
            _ = sleep(Duration::from_secs(120)) => {
                info!("Printing status updates");
                platform.print_status().await;
            }
        }
    }
}
