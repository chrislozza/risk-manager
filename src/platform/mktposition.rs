use apca::api::v2::position;

use std::fmt;

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
            self.position.current_price.as_ref().unwrap(),
            self.position.current_price.as_ref().unwrap(),
            self.position.unrealized_gain_total.as_ref().unwrap()
        )
    }
}
