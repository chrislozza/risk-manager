use apca::api::v2::updates;
use apca::data::v2::stream;

#[derive(Debug, Clone)]
pub enum Shutdown {
    Good,
    Bad,
}

#[derive(Debug, Clone)]
pub enum Event {
    Trade(stream::Trade),
    OrderUpdate(updates::OrderUpdate),
    Shutdown(Shutdown)
}
