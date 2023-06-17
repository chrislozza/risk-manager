use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::prelude::*;

use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize)]
pub struct StrategyCfg {
    pub max_positions: i8,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseCfg {
    pub host: String,
    pub port: String,
    pub name: String,
    pub secret_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Settings {
    pub gcp_subscription: String,
    pub service_client: String,
    pub gcp_project_id: String,
    pub database: Option<DatabaseCfg>,
    pub strategies: HashMap<String, StrategyCfg>,
}

#[derive(Debug)]
pub struct Config {}

impl Config {
    pub fn read_config_file(path: &str) -> Result<Settings, Box<dyn std::error::Error>> {
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let settings: Settings = serde_json::from_str(&contents)?;
        Ok(settings)
    }
}
