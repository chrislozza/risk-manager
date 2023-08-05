use apca::api::v2::account;
use apca::api::v2::asset;
use apca::api::v2::assets;
use apca::api::v2::order;
use apca::api::v2::orders;
use apca::api::v2::position;
use apca::api::v2::positions;
use apca::data::v2::bars;
use apca::data::v2::stream;
use apca::ApiInfo;
use apca::Client;

use anyhow::Result;
use std::sync::Arc;
use url::Url;

use tokio::sync::broadcast;

use tokio_util::sync::CancellationToken;

use tracing::info;

mod http_client;
mod websocket;

use super::web_clients::http_client::HttpClient;
use super::web_clients::websocket::WebSocket;
use super::Event;

#[derive(Debug)]
pub struct Connectors {
    client: Client,
    publisher: broadcast::Sender<Event>,
    http_client: HttpClient,
    websocket: WebSocket,
    shutdown_signal: CancellationToken,
}

impl Connectors {
    pub fn new(
        key: &str,
        secret: &str,
        is_live: bool,
        shutdown_signal: CancellationToken,
    ) -> Result<Arc<Self>> {
        let api_base_url = match is_live {
            true => Url::parse("https://api.alpaca.markets")?,
            false => Url::parse("https://paper-api.alpaca.markets")?,
        };
        info!("Connector starting with url: {api_base_url}");
        let api_info = ApiInfo::from_parts(api_base_url, key, secret)?;
        let client = Client::new(api_info);
        let (publisher, _subscriber) = broadcast::channel(32);
        let http_client = HttpClient::new(shutdown_signal.clone());
        let websocket = WebSocket::new(publisher.clone(), shutdown_signal.clone());
        Ok(Arc::new(Connectors {
            client,
            publisher,
            http_client,
            websocket,
            shutdown_signal,
        }))
    }

    pub fn get_subscriber(&self) -> broadcast::Receiver<Event> {
        self.publisher.subscribe()
    }

    pub async fn get_assets(&self, request: &assets::AssetsReq) -> Result<Vec<asset::Asset>> {
        self.http_client
            .send_request::<assets::Get>(&self.client, request)
            .await
    }

    pub async fn get_account_details(&self) -> Result<account::Account> {
        self.http_client
            .send_request::<account::Get>(&self.client, &())
            .await
    }

    pub async fn get_orders(&self) -> Result<Vec<order::Order>> {
        let request = orders::OrdersReq::default();
        self.http_client
            .send_request::<orders::Get>(&self.client, &request)
            .await
    }

    pub async fn get_position(&self, symbol: &str) -> Result<position::Position> {
        let symbol_exchange: asset::Symbol = asset::Symbol::SymExchg(symbol.to_string(), asset::Exchange::Amex);
        self.http_client
            .send_request::<position::Get>(&self.client, &symbol_exchange)
            .await
    }

    pub async fn get_positions(&self) -> Result<Vec<position::Position>> {
        self.http_client
            .send_request::<positions::Get>(&self.client, &())
            .await
    }

    pub async fn place_order(&self, request: &order::OrderReq) -> Result<order::Order> {
        self.http_client
            .send_request::<order::Post>(&self.client, request)
            .await
    }

    pub async fn cancel_order(&self, id: &order::Id) -> Result<()> {
        self.http_client
            .send_request::<order::Delete>(&self.client, id)
            .await
    }

    pub async fn close_position(&self, symbol: &asset::Symbol) -> Result<order::Order> {
        self.http_client
            .send_request::<position::Delete>(&self.client, symbol)
            .await
    }

    pub async fn get_historical_bars(&self, request: &bars::BarsReq) -> Result<bars::Bars> {
        self.http_client
            .send_request::<bars::Get>(&self.client, request)
            .await
    }

    pub async fn subscribe_to_symbols(
        &self,
        symbol: stream::SymbolList,
    ) -> Result<stream::Symbols> {
        self.websocket
            .subscribe_to_mktdata(&self.client, symbol)
            .await
    }

    pub async fn unsubscribe_from_symbols(
        &self,
        symbols: stream::SymbolList,
    ) -> Result<stream::Symbols> {
        self.websocket
            .unsubscribe_from_mktdata(&self.client, symbols)
            .await
    }

    pub async fn subscibe_to_order_updates(&self) -> Result<()> {
        self.websocket
            .subscribe_to_order_updates(&self.client)
            .await
    }
}
