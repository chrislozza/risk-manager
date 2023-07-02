use serde::Deserialize;
use std::fs::File;
use std::io::prelude::*;

use std::collections::HashMap;

use anyhow::Result;

#[derive(Clone, Debug, Deserialize)]
pub struct Settings {
    pub gcp_subscription: String,
    pub gcp_project_id: String,
    pub service_client: String,
    pub gcp_log_name: Option<String>,
    pub account_type: String,
    pub database: DatabaseConfig,
    pub strategies: StrategyConfigWrapper,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DatabaseConfig {
    pub name: String,
    pub port: u16,
    pub host: String,
    pub secret_id: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct StrategyConfigWrapper {
    pub risk_tolerance: f64,
    pub configuration: HashMap<String, StrategyConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct StrategyConfig {
    pub max_positions: i8,
    pub locker_type: String,
    pub trailing_size: f64,
}

#[derive(Debug)]
pub struct Config {}

impl Config {
    pub fn read_config_file(path: &str) -> Result<Settings> {
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let settings: Settings = serde_json::from_str(&contents)?;
        Ok(settings)
    }
}
