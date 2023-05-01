use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::prelude::*;
use std::vec::Vec;

#[derive(Debug, Deserialize, Serialize)]
struct StrategyCfg {
    name: String,
    stop: String,
    max_positions: i8,
    port_weight: i8,
}

#[derive(Debug, Deserialize, Serialize)]
struct DatabaseCfg {
    host: String,
    port: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    gcp_sub: String,
    service_client: String,
    database: Option<DatabaseCfg>,
    strategy: Vec<StrategyCfg>,
}

pub struct Settings {}

impl Settings {
    pub fn read_config_file(path: &str) -> Result<Config, Box<dyn std::error::Error>> {
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let config: Config = serde_json::from_str(&contents)?;
        Ok(config)
    }
}
