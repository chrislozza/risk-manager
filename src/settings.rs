use serde::Deserialize;
use std::fs::File;
use std::io::prelude::*;

use std::collections::HashMap;

use anyhow::Result;

#[derive(Default, Clone, Debug, Deserialize)]
pub struct Settings {
    pub gcp_subscription: String,
    pub service_client: String,
    pub gcp_project_id: Option<String>,
    pub gcp_log_name: Option<String>,
    pub log_level: String,
    pub account_type: String,
    pub launch_process: Option<ProcessLaunchSettings>,
    pub database: DatabaseConfig,
    pub sizing: PositionSizing,
    pub strategies: HashMap<String, StrategyConfig>,
    pub stops: HashMap<String, Stop>,
}

#[derive(Default, Clone, Debug, Deserialize)]
pub struct DatabaseConfig {
    pub name: String,
    pub port: u16,
    pub host: String,
    pub user: String,
    pub password: Option<String>,
}

#[derive(Default, Clone, Debug, Deserialize)]
pub struct ProcessLaunchSettings {
    pub name: String,
    pub args: Vec<String>,
}

#[derive(Default, Clone, Debug, Deserialize)]
pub struct PositionSizing {
    pub risk_tolerance: f32,
    pub multiplier: f32,
}

#[derive(Default, Clone, Debug, Deserialize)]
pub struct StrategyConfig {
    pub max_positions: i8,
    pub locker: String,
}

#[derive(Default, Clone, Debug, Deserialize)]
pub struct Stop {
    pub locker_type: String,
    pub multiplier: f64,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_stops() -> Result<()> {
        let settings = r#"{
  "stops": [
    {
      "name": "smart_01",
      "locker_type": "pc",
      "multiplier": 7
    },
    {
      "name": "atr_01",
      "locker_type": "atr",
      "multiplier": 5.5
    }
  ],
            }"#;
        let stops: Vec<Stop> = serde_json::from_str(settings)?;
        assert_eq!(stops.len(), 2);
        Ok(())
    }

    #[test]
    fn test_build_strategy_with_stop() -> Result<()> {
        let settings = r#"
            {
  "stops": [
    {
      "name": "smart_01",
      "locker_type": "pc",
      "multiplier": 7
    },
    {
      "name": "atr_01",
      "locker_type": "atr",
      "multiplier": 5.5
    }
  ],
  "strategies": {
    "risk_tolerance": 0.02,
    "configuration": {
      "auto01": {
        "max_positions": 10,
        "locker": "atr_01"
      },
      "manual01": {
        "max_positions": 10,
        "locker": "smart_01"
      }
    }
  }
            }
        "#;
        let stops: Vec<Stop> = serde_json::from_str(settings)?;
        assert_eq!(stops.len(), 2);
        Ok(())
    }

    #[test]
    fn test_build_from_json_no_errors() {}
}
