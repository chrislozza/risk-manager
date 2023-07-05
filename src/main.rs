use clap::Parser;
use tracing::{debug, error, info, warn};

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
use logging::CloudLogging;
use platform::Platform;
use settings::{Config, Settings};

use tokio::signal;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

use std::env;

//async fn log_init(shutdown_signal: CancellationToken, project_id: &str, logging_name: Option<&str>) -> Result<()> {
//    let gcp_logger = match logging_name {
//        Some(name) => Some(CloudLogging::new(name, project_id, shutdown_signal).await?),
//        _ => None,
//    };
//
//    if let Err(err) = log::set_boxed_logger(Box::new(SimpleLogger::new(gcp_logger)))
//        .map(|()| log::set_max_level(LevelFilter::Info))
//    {
//        bail!("Error caught in logging setup, error={err}")
//    }
//
//    Ok(())
//}
//
/// Simple program to greet a person
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

    let (send_mkt_signals, mut receive_mkt_signals) = mpsc::unbounded_channel();

    let key = env::var("KEY").expect("Failed to read the 'key' environment variable.");
    let secret = env::var("SECRET").expect("Failed to read the 'secret' environment variable.");

    debug!("Found key: {key}, secret: {secret}");

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
    loop {
        tokio::select! {
            event = receive_mkt_signals.recv() => {
                if let Event::MktSignal(event) = event.unwrap() {
                    info!("Recieved an event {event:?}, creating new position");
                    if let Err(err) = platform.create_position(&event).await {
                        warn!("Signal dropped {event:?}, error: {err}");
                    }
                }
            }
            _ = shutdown_signal.cancelled() => {
                warn!("exiting early");
                std::process::exit(1)
            }
            _ = signal::ctrl_c() => {
                info!("Keyboard shutdown detected");
                let _ = publisher.shutdown().await;
                let _ = platform.shutdown().await;
                break
            }
            _ = sleep(Duration::from_secs(180)) => {
                platform.print_status().await;
            }
        }
    }
    std::process::exit(0);
}
