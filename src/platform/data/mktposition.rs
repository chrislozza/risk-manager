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
            "Position symbol[{}], strategy[{}] avgPrice[{}], size[{}], pnl[{}]",
            self.symbol, self.strategy, self.avg_price, self.quantity, self.pnl
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

    pub fn get_position(&self, symbol: &str) -> Option<&MktPosition> {
        self.mktpositions.get(symbol)
    }

    pub async fn update_position(&mut self, symbol: &str) -> Result<MktPosition> {
        let position = self.connectors.get_position(symbol).await?;
        if let Some(mktposition) = self.mktpositions.get_mut(symbol) {
            Ok(mktposition.update_inner(position).clone())
        } else {
            bail!("MktPosition key not found in HashMap")
        }
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
