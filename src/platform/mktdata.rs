use apca::data::v2::{bars, stream};
use apca::Client;
use chrono::{Duration, Utc};
use log::{error, info};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use tokio::sync::broadcast;

use super::mktorder;
use super::mktposition;
use super::stream_handler::StreamHandler;
use super::Event;

pub struct MktData {
    client: Arc<Mutex<Client>>,
    symbols: stream::Symbols,
    is_alive: Arc<Mutex<bool>>,
    stream_handler: StreamHandler,
    receiver: broadcast::Receiver<Event>,
    sender: broadcast::Sender<Event>,
}

impl MktData {
    pub fn new(client: Arc<Mutex<Client>>) -> Self {
        let (sender, receiver) = broadcast::channel(100);
        let stream_handler = StreamHandler::new(Arc::clone(&client), sender.clone());
        MktData {
            client,
            symbols: stream::Symbols::default(),
            is_alive: Arc::new(Mutex::new(false)),
            stream_handler,
            receiver,
            sender,
        }
    }

    fn build_symbol_list(
        &self,
        orders: &HashMap<String, mktorder::MktOrder>,
        positions: &HashMap<String, mktposition::MktPosition>,
    ) -> Vec<String> {
        let mut symbols = Vec::<String>::default();
        for mktorder in orders.values() {
            symbols.push(mktorder.get_order().symbol.clone());
        }

        for mktposition in positions.values() {
            symbols.push(mktposition.get_position().symbol.clone());
        }
        symbols
    }

    pub fn stream_reader(&self) -> broadcast::Receiver<Event> {
        self.sender.subscribe()
    }

    pub async fn startup(
        &mut self,
        orders: &HashMap<String, mktorder::MktOrder>,
        positions: &HashMap<String, mktposition::MktPosition>,
    ) {
        let symbols = self.build_symbol_list(orders, positions);
        if symbols.is_empty() {
            info!("No symbols to subscribe to");
            return;
        }

        let symbols = match self
            .stream_handler
            .subscribe_to_mktdata(symbols.into())
            .await
        {
            Err(err) => {
                error!("Failed to subscribe to stream, error: {err:?}");
                panic!("{:?}", err);
            }
            Ok(val) => val,
        };
        self.symbols = symbols;
    }

    pub async fn shutdown(&self) {
        info!("Shutdown initiated");
    }

    pub async fn get_historical_bars(&self, symbol: &str, days_to_lookback: i64) -> Vec<bars::Bar> {
        let client = self.client.lock().await;
        {
            let today = Utc::now();
            let start_date = today - Duration::days(days_to_lookback);
            let end_date = today;
            let request = bars::BarsReqInit {
                limit: Some(days_to_lookback as usize),
                ..Default::default()
            }
            .init(symbol, start_date, end_date, bars::TimeFrame::OneDay);

            let res = client.issue::<bars::Get>(&request).await.unwrap();
            res.bars
        }
    }

    pub async fn subscribe(&mut self, symbol: String) {
        let symbols = match self
            .stream_handler
            .subscribe_to_mktdata(vec![symbol.clone()].into())
            .await
        {
            Err(val) => {
                error!("Failed to subscribe {val:?}");
                return;
            }
            Ok(val) => val,
        };
        self.symbols = symbols;
    }

    pub async fn unsubscribe(&mut self, symbol: String) {
        let symbols = match self
            .stream_handler
            .unsubscribe_from_stream(vec![symbol.clone()].into())
            .await
        {
            Err(val) => {
                error!("Failed to unsubscribe {val:?}");
                return;
            }
            Ok(val) => val,
        };
        self.symbols = symbols;
    }

    pub async fn unsubscribe_all(&mut self) {
        let symbol_list = match self.symbols.clone() {
            stream::Symbols::List(val) => val,
            _ => stream::SymbolList::default(),
        };
        if symbol_list.len() == 0 {
            return;
        }
        let symbols = match self
            .stream_handler
            .unsubscribe_from_stream(symbol_list)
            .await
        {
            Err(val) => {
                error!("Failed to unsubscribe {val:?}");
                return;
            }
            Ok(val) => val,
        };
        self.symbols = symbols;
    }
}
