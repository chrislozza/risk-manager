use anyhow::bail;
use anyhow::Result;
use apca::api::v2::account;
use apca::api::v2::asset;
use apca::api::v2::assets;
use apca::api::v2::order;
use apca::api::v2::order::Id;
use apca::api::v2::orders;
use apca::api::v2::position;
use apca::api::v2::positions;
use apca::data::v2::bars;
use apca::data::v2::stream;
use apca::ApiInfo;
use apca::Client;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::info;
use url::Url;
use uuid::Uuid;

mod http_client;
mod websocket;

use super::Event;
use http_client::HttpClient;
use websocket::WebSocket;

#[derive(Debug)]
pub struct Connectors {
    client: Client,
    publisher: broadcast::Sender<Event>,
    http_client: HttpClient,
    websocket: WebSocket,
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
        let (publisher, _subscriber) = broadcast::channel(150);
        let http_client = HttpClient::new(shutdown_signal.clone());
        let websocket = WebSocket::new(publisher.clone(), shutdown_signal.clone());
        Ok(Arc::new(Connectors {
            client,
            publisher,
            http_client,
            websocket,
        }))
    }

    pub async fn startup(&self) -> Result<()> {
        self.websocket.startup(&self.client).await
    }

    pub fn get_subscriber(&self) -> broadcast::Receiver<Event> {
        self.publisher.subscribe()
    }

    pub async fn get_assets(&self, request: &assets::AssetsReq) -> Result<Vec<asset::Asset>> {
        info!("Request get_assets");
        match self
            .http_client
            .send_request::<assets::Get>(&self.client, request)
            .await
        {
            anyhow::Result::Err(err) => bail!("Call to get_assets failed, error={}", err),
            val => val,
        }
    }

    pub async fn get_account_details(&self) -> Result<account::Account> {
        info!("Request get_account_details");
        match self
            .http_client
            .send_request::<account::Get>(&self.client, &())
            .await
        {
            anyhow::Result::Err(err) => bail!("Call to get_account_details failed, error={}", err),
            val => val,
        }
    }

    pub async fn get_order(&self, order_id: Uuid) -> Result<order::Order> {
        let _request = orders::OrdersReq::default();
        info!("Request get_order");
        match self
            .http_client
            .send_request::<order::Get>(&self.client, &Id(order_id))
            .await
        {
            anyhow::Result::Err(err) => bail!("Call to get_order failed, error={}", err),
            val => val,
        }
    }

    pub async fn get_orders(&self) -> Result<Vec<order::Order>> {
        let request = orders::OrdersReq {
            status: orders::Status::All,
            ..Default::default()
        };
        info!("Request get_orders");
        match self
            .http_client
            .send_request::<orders::Get>(&self.client, &request)
            .await
        {
            anyhow::Result::Err(err) => bail!("Call to get_orders failed, error={}", err),
            val => val,
        }
    }

    pub async fn get_position(
        &self,
        symbol: &str,
        exchange: asset::Exchange,
    ) -> Result<position::Position> {
        let symbol_exchange: asset::Symbol = asset::Symbol::SymExchg(symbol.to_string(), exchange);
        info!("Request get_position");
        match self
            .http_client
            .send_request::<position::Get>(&self.client, &symbol_exchange)
            .await
        {
            anyhow::Result::Err(err) => bail!("Call to get_position failed, error={}", err),
            val => val,
        }
    }

    pub async fn get_positions(&self) -> Result<Vec<position::Position>> {
        info!("Request get_positions");
        match self
            .http_client
            .send_request::<positions::Get>(&self.client, &())
            .await
        {
            anyhow::Result::Err(err) => bail!("Call to get_positions failed, error={}", err),
            val => val,
        }
    }

    pub async fn place_order(&self, request: &order::OrderReq) -> Result<order::Order> {
        info!("Request place_order");
        match self
            .http_client
            .send_request::<order::Post>(&self.client, request)
            .await
        {
            anyhow::Result::Err(err) => bail!("Call to place_order failed, error={}", err),
            val => val,
        }
    }

    pub async fn cancel_order(&self, id: &order::Id) -> Result<()> {
        info!("Request cancel_order");
        match self
            .http_client
            .send_request::<order::Delete>(&self.client, id)
            .await
        {
            anyhow::Result::Err(err) => bail!("Call to cancel_order failed, error={}", err),
            _ => Ok(()),
        }
    }

    pub async fn close_position(&self, symbol: &asset::Symbol) -> Result<order::Order> {
        info!("Request close_position");
        match self
            .http_client
            .send_request::<position::Delete>(&self.client, symbol)
            .await
        {
            anyhow::Result::Err(err) => bail!("Call to close_position failed, error={}", err),
            val => val,
        }
    }

    pub async fn get_historical_bars(&self, request: &bars::BarsReq) -> Result<bars::Bars> {
        info!("Request get_historical_bars");
        match self
            .http_client
            .send_request::<bars::Get>(&self.client, request)
            .await
        {
            anyhow::Result::Err(err) => bail!("Call to get_historical_bars failed, error={}", err),
            val => val,
        }
    }

    pub async fn subscribe_to_symbols(&self, symbols: stream::SymbolList) -> Result<()> {
        info!("Request subscribe_to_symbols");
        match self.websocket.subscribe_to_mktdata(symbols).await {
            anyhow::Result::Err(err) => bail!("Call to subscribe_to_symbols failed, error={}", err),
            _ => Ok(()),
        }
    }

    pub async fn unsubscribe_from_symbols(&self, symbols: stream::SymbolList) -> Result<()> {
        info!("Request unsubscribe_from_symbols");
        match self.websocket.unsubscribe_from_mktdata(symbols).await {
            anyhow::Result::Err(err) => {
                bail!("Call to unsubscribe_from_symbols failed, error={}", err)
            }
            _ => Ok(()),
        }
    }
}
