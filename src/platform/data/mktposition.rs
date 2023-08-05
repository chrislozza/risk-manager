use apca::api::v2::position;
use num_decimal::Num;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use anyhow::bail;
use std::fmt;

use tracing::info;

use super::Transaction;
use crate::events::Direction;
use crate::to_num;

use super::super::web_clients::Connectors;

#[derive(Debug, Clone)]
pub struct MktPosition {
    position: position::Position,
    strategy: String,
    direction: Direction
}

impl MktPosition {
    pub fn new(position: position::Position, strategy: &str, direction: Direction) -> Self {
        MktPosition { position, strategy: strategy.into(), direction}
    }

    pub fn update_inner(&mut self, position: position::Position) -> &Self {
        self.position = position;
        self
    }

    pub fn get_strategy(&self) -> &str {
        &self.strategy
    }

    pub fn get_direction(&self) -> Direction {
        self.direction
    }

    pub fn get_entry_price(&self) -> Num {
        self.position.average_entry_price
    }

    pub fn get_symbol(&self) -> &str {
        &self.position.symbol
    }

    pub fn get_cost_basis(&self) -> Num {
        self.position.cost_basis
    }

    pub fn get_pnl(&self) -> Num {
        match self.position.unrealized_gain_total {
            Some(val) => val,
            None => to_num!(0.0)
        }
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
            self.position.quantity,
            self.position
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


    pub async fn update_positions(&mut self, transactions: &HashMap<String, Transaction>) -> Result<HashMap<String, MktPosition>> {
        let positions = self.connectors.get_positions().await?;
        for position in &positions {
            let transaction = &transactions[&position.symbol];
            let mktposition = MktPosition::new(position.clone(), &transaction.strategy, transaction.direction);
            self.mktpositions
                .insert(mktposition.get_symbol().to_string(), mktposition);
        }
        Ok(self.mktpositions.clone())
    }
}
