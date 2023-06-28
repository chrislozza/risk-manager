use apca::api::v2::position;

use std::fmt;

#[derive(Debug, Clone)]
pub struct MktPosition {
    position: position::Position,
}

impl MktPosition {
    pub fn new(position: position::Position) -> Self {
        MktPosition { position }
    }

    pub fn get_position(&self) -> &position::Position {
        &self.position
    }
}

impl fmt::Display for MktPosition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Position symbol[{}], avgPrice[{}], size[{}], pnl[{}]", self.position.symbol, self.position.current_price.as_ref().unwrap(), self.position.current_price.as_ref().unwrap(), self.position.unrealized_gain_total.as_ref().unwrap())
    }
}
