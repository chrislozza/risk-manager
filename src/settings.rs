
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::prelude::*;

use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StrategyCfg {
    pub name: String,
    pub max_positions: i8,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DatabaseCfg {
    pub host: String,
    pub port: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Settings {
    pub gcp_sub: String,
    pub service_client: String,
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
