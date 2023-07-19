use apca::api::v2::position;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use std::fmt;

use tracing::info;

use super::super::web_clients::Connectors;

#[derive(Debug, Clone)]
pub struct MktPosition {
    position: position::Position,
    strategy: String,
}

impl MktPosition {
    pub fn new(position: position::Position, strategy: Option<&str>) -> Self {
        let strategy = if let Some(strategy) = strategy {
            strategy.to_string()
        } else {
            //db lookup
            String::default()
        };

        MktPosition { position, strategy }
    }

    pub fn get_position(&self) -> &position::Position {
        &self.position
    }

    pub fn get_strategy(&self) -> &str {
        //&self.strategy
        "00cl1"
    }
}

impl fmt::Display for MktPosition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Position symbol[{}], strategy[{}] avgPrice[{}], size[{}], pnl[{}]",
            self.position.symbol,
            self.strategy,
            self.position.current_price.as_ref().unwrap().round_with(2),
            self.position.quantity.to_string(),
            self.position.unrealized_gain_total.as_ref().unwrap()
        )
    }
}

pub struct MktPositions {
    connectors: Arc<Connectors>,
    mktpositions: HashMap<String, MktPosition>,
}

impl MktPositions {
    pub fn new(connectors: &Arc<Connectors>) -> Self {
        MktPositions {
            connectors: Arc::clone(connectors),
            mktpositions: HashMap::default(),
        }
    }

    pub async fn get_position(&mut self, symbol: &str) -> Result<&MktPosition> {
        Ok(&self.mktpositions[symbol])
    }

    pub async fn get_positions(&self) -> &HashMap<String, MktPosition> {
        &self.mktpositions
    }

    pub async fn update_positions(&mut self) -> Result<()> {
        let positions = self.connectors.get_positions().await?;
        for position in &positions {
            let mktposition = MktPosition::new(position.clone(), Some("00cl1"));
            info!("{mktposition}");
            self.mktpositions
                .insert(mktposition.get_position().symbol.clone(), mktposition);
        }
        Ok(())
    }
}
