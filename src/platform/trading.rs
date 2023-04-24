use apca::Client;
use apca::data::v2::stream::{StreamApiError};
use log::{error, info};

#[derive(Debug)]
pub struct Trading {
}

impl Trading {
    pub async fn new(client: &Client, is_live: bool) -> Self {
        let trading = match Self::subscribe_to_stream(client).await {
            Ok(data) => data,
            Err(err) => {
                error!("Failed to subscribe to stream, error: {err}");
                panic!("{:?}", err);
            }
        };
        return trading
    }

    pub async fn subscribe_to_stream(client: &Client) -> Result<Self, StreamApiError> {
        let (mut stream, mut subscription) = client
            .subscribe::<RealtimeData<IEX>>()
            .await.unwrap();

        Ok(Trading{})
    }
}
