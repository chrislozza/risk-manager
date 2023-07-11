use apca::Client;

use std::thread;
use std::time::Duration;

use anyhow::bail;
use anyhow::Result;
use http_endpoint::Endpoint;

use tracing::warn;

use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub(crate) struct HttpClient {
    shutdown_signal: CancellationToken,
}

impl HttpClient {
    pub fn new(shutdown_signal: CancellationToken) -> Self {
        HttpClient { shutdown_signal }
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
                Err(err) => {
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
}
