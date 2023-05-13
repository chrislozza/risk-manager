use apca::api::v2::position;

#[derive(Debug)]
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
