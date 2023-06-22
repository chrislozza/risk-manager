use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use axum::{response, routing, Router};
use tower_http::cors::CorsLayer;

use log::{error, info};

use serde_json::{json, Value};

use anyhow::Result;

use super::Direction;
use super::Event;
use super::PortAction;
use super::Side;
use super::Source;
use crate::events::MktSignal;

async fn post_event(
    sender: mpsc::UnboundedSender<Event>,
    response::Json(payload): response::Json<HashMap<String, String>>,
) -> response::Json<Value> {
    info!("Received post from webhook, payload: {payload:?}");

    let price = match str::parse::<f64>(&payload["price"]) {
        Ok(price) => price,
        Err(err) => {
            error!("Failed to parse value: price");
            return response::Json(json!({"response" : 400, "msg": "{err:?}"}));
        }
    };

    let mktsignal = MktSignal {
        strategy: payload["strategy"].clone(),
        symbol: payload["symbol"].clone(),
        side: Side::Buy,
        action: PortAction::Create,
        direction: Direction::Long,
        source: Source::Email,
        price: Some(price),
        primary_exchange: None,
        is_dirty: None,
        amount: None,
    };

    let event = Event::MktSignal(mktsignal);
    match sender.send(event) {
        Err(err) => {
            error!("{err:?}");
            response::Json(json!({"response" : 400, "msg": "{err}"}))
        }
        Ok(_) => response::Json(json!({"response" : 200, "msg": "success"})),
    }
}

#[derive(Debug, Clone)]
pub struct WebHook {
    shutdown_signal: CancellationToken,
}

impl WebHook {
    pub async fn new(shutdown_signal: CancellationToken) -> Self {
        WebHook { shutdown_signal }
    }

    pub async fn run(&mut self, sender: mpsc::UnboundedSender<Event>) -> Result<()> {
        let app = Router::new()
            .route(
                "/v1/mktsignal",
                routing::post(move |body| post_event(sender.clone(), body)),
            )
            .layer(CorsLayer::permissive());

        let server =
            axum::Server::bind(&"0.0.0.0:496".parse().unwrap()).serve(app.into_make_service());

        let cancel_request = self.shutdown_signal.clone();
        server.with_graceful_shutdown(async {
            cancel_request.cancelled().await;
        });

        Ok(())
    }
}
