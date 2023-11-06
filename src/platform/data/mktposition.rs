use anyhow::Result;
use apca::api::v2::asset::Exchange;
use apca::api::v2::position::Position;
use num_decimal::Num;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use uuid::Uuid;

use super::super::web_clients::Connectors;
use crate::events::Direction;
use crate::to_num;

#[derive(Debug, Clone, Default)]
pub struct MktPosition {
    pub local_id: Uuid,
    pub symbol: String,
    pub strategy: String,
    pub avg_price: Num,
    pub quantity: Num,
    pub cost_basis: Num,
    pub pnl: Num,
    pub direction: Direction,
}

impl MktPosition {
    pub fn new(strategy: &str, symbol: &str, direction: Direction) -> Self {
        MktPosition {
            strategy: strategy.to_string(),
            symbol: symbol.to_string(),
            direction,
            ..Default::default()
        }
    }

    pub fn update_inner(&mut self, position: Position) -> &Self {
        let entry_price = position.average_entry_price.clone();
        self.avg_price = match &position.current_price {
            Some(price) => price.clone(),
            None => entry_price,
        };
        self.quantity = position.quantity.clone();
        self.cost_basis = position.cost_basis.clone();
        self.pnl = self.get_pnl(&position);
        self
    }

    fn get_pnl(&self, position: &Position) -> Num {
        match &position.unrealized_gain_total {
            Some(val) => val.clone(),
            None => to_num!(0.0),
        }
    }
}

impl fmt::Display for MktPosition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Position strategy[{}], symbol[{}] avgPrice[{}], size[{}], pnl[{}]",
            self.strategy,
            self.symbol,
            self.avg_price.round_with(3),
            self.quantity,
            self.pnl.round_with(3)
        )
    }
}

pub struct MktPositions {
    connectors: Arc<Connectors>,
    positions: HashMap<String, MktPosition>,
}

impl MktPositions {
    pub fn new(connectors: &Arc<Connectors>) -> Self {
        MktPositions {
            connectors: Arc::clone(connectors),
            positions: HashMap::default(),
        }
    }

    pub fn add_position(&mut self, strategy: &str, symbol: &str, direction: Direction) {
        let position = MktPosition::new(strategy, symbol, direction);
        self.positions.insert(symbol.to_string(), position);
    }

    pub async fn update_position(
        &mut self,
        symbol: &str,
        exchange: Exchange,
    ) -> Result<MktPosition> {
        let position = self.connectors.get_position(symbol, exchange).await?;
        if let Some(mktposition) = self.positions.get_mut(symbol) {
            Ok(mktposition.update_inner(position).clone())
        } else {
            panic!("MktPosition key not found in collection")
        }
    }

    pub async fn update_positions(&mut self) -> Result<Vec<MktPosition>> {
        let positions = self.connectors.get_positions().await?;
        for position in &positions {
            if let Some(mktposition) = self.positions.get_mut(&position.symbol) {
                mktposition.update_inner(position.clone());
            }
        }
        Ok(self.positions.values().cloned().collect())
    }
}
