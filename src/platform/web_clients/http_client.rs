use anyhow::bail;
use anyhow::Result;
use apca::Client;
use http_endpoint::Endpoint;
use std::thread;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::warn;

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
            match client.issue::<E>(input).await {
                Err(apca::RequestError::Endpoint(err)) => {
                    warn!("Request failed, endpoint error: {err}");
                }
                Err(apca::RequestError::Hyper(err)) => {
                    warn!("Request failed, hyper error: {err}");
                }
                Err(apca::RequestError::Io(err)) => {
                    warn!("Request failed, io error: {err}");
                }
                Ok(payload) => return Ok(payload),
            };
            if retry == 0 {
                self.shutdown_signal.cancel();
                bail!("No retry attempts left, exiting app")
            }
            retry -= 1;
            warn!("Retry order posting retries left: {retry}");
            thread::sleep(Duration::from_secs(1));
        }
    }
}
