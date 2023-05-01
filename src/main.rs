use clap::{App, Arg};
use log::{error, info};
use tokio::time::{sleep, Duration};

extern crate libc;

mod logging;
mod platform;
mod settings;

use crate::logging::SimpleLogger;
use crate::platform::Platform;
use crate::settings::Settings;

use log::{LevelFilter, SetLoggerError};

use std::sync::atomic::{AtomicBool, Ordering};

static LOGGER: SimpleLogger = SimpleLogger;
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

fn log_init() -> Result<(), SetLoggerError> {
    log::set_logger(&LOGGER).map(|()| log::set_max_level(LevelFilter::Info))
}

fn register_signal_handler(signal: libc::c_int) {
    unsafe {
        let mut sigaction: libc::sigaction = std::mem::zeroed();
        sigaction.sa_sigaction = signal_handler as usize;
        sigaction.sa_flags = libc::SA_SIGINFO;

        libc::sigemptyset(&mut sigaction.sa_mask as *mut libc::sigset_t);
        libc::sigaddset(&mut sigaction.sa_mask as *mut libc::sigset_t, signal);

        libc::sigaction(signal, &sigaction, std::ptr::null_mut());
    }

    extern "C" fn signal_handler(_: libc::c_int, _: *mut libc::siginfo_t, _: *mut libc::c_void) {
        info!("In c signal handler");
        SHUTDOWN_REQUESTED.store(true, Ordering::Relaxed);
    }
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
        _ => panic!("Couldn't determine cmdline type"),
    };

    register_signal_handler(libc::SIGINT);
    register_signal_handler(libc::SIGTERM);

    let _settings = Settings::read_config_file(config);
    let mut platform = Platform::new(key, secret, is_live);
    match platform.startup().await {
        Ok(_) => info!("Startup complete"),
        Err(err) => {
            error!("Failed to startup, error: {err}");
            std::process::exit(1);
        }
    };
    info!("To loop Here");
    while !SHUTDOWN_REQUESTED.load(Ordering::Relaxed) {
        info!("in the while loop");
        if platform.poll().await {
            info!("Startup complete");
            break;
        }
        sleep(Duration::from_secs(5)).await;
    }
}
