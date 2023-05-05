use apca::api::v2::position;

use num_decimal::Num;

#[derive(Debug)]
pub struct MktPosition {
    position: position::Position,
}

impl MktPosition {
    pub fn new(position: position::Position) -> Self {
        MktPosition { position }
    }

    pub fn get_symbol(&self) -> &String {
        &self.position.symbol
    }

}
