use apca::api::v2::{asset, order, orders, position, positions};
use apca::Client;
use num_decimal::Num;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::sync::Mutex;

use anyhow::bail;
use anyhow::Result;
use tokio::sync::broadcast;
use http_endpoint::Endpoint;

use tracing::info; 
use tracing::warn;
use tracing::error; 

use crate::float_to_num;
use tokio_util::sync::CancellationToken;

pub(crate) struct HttpClient {
    shutdown_signal: CancellationToken,
}

impl HttpClient {
    pub fn new(shutdown_signal: CancellationToken) -> Self {
        HttpClient {
            shutdown_signal
        }
    }

    pub async fn send_request<E>(&self, client: &Client, input: &E::Input) -> Result<E::Output>
        where
            E: Endpoint,
        {
        let mut retry = 5;
        loop {
            let _ = match client.issue::<E>(input).await {
                Err(apca::RequestError::Endpoint(err)) => {
                    warn!("Request failed, error: {err}");
                    anyhow::anyhow!("request failed, trying again...")
                }
                Err(err) => 
                {
                    self.shutdown_signal.cancel();
                    bail!("Unknown error: {err}, exiting");
                }
                Ok(payload) => return Ok(payload),
            };
            if retry == 0 {
                self.shutdown_signal.cancel();
                bail!("Failed to post order")
            }
            retry -= 1;
            warn!("Retry order posting retries left: {retry}");
            thread::sleep(Duration::from_secs(1));
        }
    }

//    async fn send_post_request<T: http_endpoint::Endpoint<Output = (), Input = T>>(&self, payload: &T) -> Result<T::Output> {
//        let mut retry = 5;
//        loop {
//            match self.client.issue::<T>(payload).await {
//                Ok(payload) => payload,
//                Err(apca::RequestError::Endpoint(err)) => warn!("Request failed, error: {err}"),
//                Err(err) => 
//                {
//                    self.shutdown_signal.cancel();
//                    bail!("Unknown error: {err}, exiting");
//                }
//
//            }
//            if retry == 0 {
//                bail!("Failed to post order")
//            }
//            retry -= 1;
//            warn!("Retry order posting retries left: {retry}");
//            thread::sleep(Duration::from_secs(1));
//        }
//    }
}
