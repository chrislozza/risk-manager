use apca::api::v2::updates;
use apca::data::v2::stream;

mod pub_sub;
mod web_hook;

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


pub struct EventPublisher {
}

impl EventPublisher {
    pub fn new() -> Self {
        EventPublisher {
        }
    }

    pub fn shutdown(&self) {
    }

    pub fn run(&self) {
    }
}
