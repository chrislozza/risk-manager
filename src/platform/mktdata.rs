use apca::data::v2::{bars, quotes, stream, trades};
use apca::Client;
use log::{error, info};
use std::vec::Vec;

use futures::FutureExt as _;
use futures::StreamExt as _;
use futures::TryStreamExt as _;

#[derive(Debug)]
pub struct MktData {
    subscribed: Vec<String>,
}

impl MktData {
    pub fn new() -> Self {
        MktData {
            subscribed: Vec::default(),
        }
    }

    pub async fn startup(&self, client: &Client, symbols: Vec<String>) {
        match self.subscribe_to_stream(client, symbols).await {
            Err(err) => {
                error!("Failed to subscribe to stream, error: {err}");
                panic!("{:?}", err);
            }
            _ => (),
        };
    }

    async fn subscribe_to_stream(
        &self,
        client: &Client,
        symbols: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (mut stream, mut subscription) = client
            .subscribe::<stream::RealtimeData<stream::IEX>>().await?;
        let mut data = stream::MarketData::default();
        data.set_bars(symbols.clone());
        data.set_trades(symbols);

        let subscribe = subscription.subscribe(&data).boxed();
        // Actually subscribe with the websocket server.
        let () = stream::drive(subscribe, &mut stream)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        info!("Before the stream is off");
        tokio::spawn(async move {
            let () = match stream
                .take(50)
                .map_err(apca::Error::WebSocket)
                .try_for_each(|result| async {
                    result
                        .map(|data| println!("{data:?}"))
                        .map_err(apca::Error::Json)
                })
                .await
            {
                Err(err) => error!("Error thrown in websocket {}", err),
                _ => (),
            };
        });
        info!("Returning");
        Ok(())
    }

    //    async fn request_account_details() -> Self {
    //        //        BarsReqInit::init
    //        //        let request = Request::<'a>::Authenticate { client.api_info().api_key, client.api_info().secret };
    //        //
    //        //        Client::issue<Get>(&()).await.unwrap();
    //
    //    }
}
