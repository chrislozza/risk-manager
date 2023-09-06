use apca::api::v2::position::Position;
use num_decimal::Num;
use uuid::Uuid;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::bail;
use anyhow::Result;
use std::fmt;

use crate::events::Direction;
use crate::to_num;

use super::super::web_clients::Connectors;

#[derive(Debug, Clone)]
pub struct MktPosition {
    pub local_id: Uuid,
    pub position: Option<Position>,
    pub symbol: String,
    pub strategy: String,
    pub direction: Direction,
}

impl MktPosition {
    pub fn new(symbol: &str, strategy: &str, direction: Direction) -> Self {
        MktPosition {
            local_id: Uuid::new_v4(),
            position: None,
            symbol: symbol.into(),
            strategy: strategy.into(),
            direction,
        }
    }

    pub fn update_inner(&mut self, position: Position) -> &Self {
        self.position = Some(position);
        self
    }

    pub fn get_strategy(&self) -> &str {
        &self.strategy
    }

    pub fn get_direction(&self) -> Direction {
        self.direction
    }

    pub fn get_entry_price(&self) -> Num {
        self.position.as_ref().unwrap().average_entry_price.clone()
    }

    pub fn get_symbol(&self) -> &str {
        &self.position.as_ref().unwrap().symbol
    }

    pub fn get_cost_basis(&self) -> Num {
        self.position.as_ref().unwrap().cost_basis.clone()
    }

    pub fn get_pnl(&self) -> Num {
        match self
            .position
            .as_ref()
            .unwrap()
            .unrealized_gain_total
            .clone()
        {
            Some(val) => val,
            None => to_num!(0.0),
        }
    }
}

impl fmt::Display for MktPosition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let position = self.position.as_ref().unwrap();
        write!(
            f,
            "Position symbol[{}], strategy[{}] avgPrice[{}], size[{}], pnl[{}]",
            position.symbol,
            self.strategy,
            position.current_price.as_ref().unwrap().round_with(2),
            position.quantity,
            position
                .unrealized_gain_total
                .as_ref()
                .unwrap()
                .round_with(2)
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

    pub async fn get_position(&self, symbol: &str) -> Result<&MktPosition> {
        Ok(&self.mktpositions[symbol])
    }

    pub fn insert(&mut self, symbol: &str, strategy: &str, direction: Direction) -> Uuid {
        let mktpostion = MktPosition::new(symbol, strategy, direction);
        let local_id = mktpostion.local_id;
        self.mktpositions.insert(symbol.to_string(), mktpostion);
        local_id
    }

    pub async fn update_position(&mut self, symbol: &str) -> Result<MktPosition> {
        let position = self.connectors.get_position(symbol).await?;
        if let Some(mktposition) = self.mktpositions.get_mut(symbol) {
            Ok(mktposition.update_inner(position).clone())
        } else {
            bail!("MktPosition key not found in HashMap")
        }
    }

    pub async fn get_positions(&self) -> &HashMap<String, MktPosition> {
        &self.mktpositions
    }

    pub async fn update_positions(&mut self) -> Result<&HashMap<String, MktPosition>> {
        let positions = self.connectors.get_positions().await?;
        for position in &positions {
            if let Some(mktposition) = self.mktpositions.get_mut(&position.symbol) {
                mktposition.update_inner(position.clone());
            }
        }
        Ok(&self.mktpositions)
    }
}
