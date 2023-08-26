use apca::api::v2::asset::Asset;
use apca::api::v2::asset::Class;
use apca::api::v2::asset::Status;
use apca::api::v2::assets::AssetsReq;


use std::sync::Arc;

use std::collections::HashMap;



use anyhow::Result;

use super::Direction;
use crate::platform::web_clients::Connectors;

#[derive(Debug)]
pub struct Assets {
    connectors: Arc<Connectors>,
    assets: HashMap<String, Asset>,
}

impl Assets {
    pub async fn new(connectors: &Arc<Connectors>) -> Result<Self> {
        Ok(Assets {
            connectors: Arc::clone(connectors),
            assets: HashMap::default(),
        })
    }

    pub async fn fetch_asset_list(&self) -> HashMap<String, Asset> {
        let request = AssetsReq {
            status: Status::Active,
            class: Class::UsEquity,
        };
        let assets = self.connectors.get_assets(&request).await.unwrap();
        assets
            .into_iter()
            .map(|data| (data.symbol.clone(), data))
            .collect()
    }

    pub async fn startup(&mut self) -> Result<()> {
        self.assets = self.fetch_asset_list().await;
        Ok(())
    }

    pub fn check_if_assest_is_tradable(&self, symbol: &str, direction: Direction) -> bool {
        if let Some(asset) = self.assets.get(symbol) {
            let is_tradable = match direction {
                Direction::Short => asset.shortable && asset.marginable,
                _ => true,
            };
            return is_tradable;
        }
        false
    }
}
