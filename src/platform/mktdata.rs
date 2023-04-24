use apca::data::v2::stream::{Request, RealtimeData, StreamApiError, Subscription, IEX};
use apca::{ApiInfo, Client};
use url::Url;
use log::{error, info};

#[derive(Debug)]
pub struct MktData {

//    auth_token: String,
}

impl MktData {
    pub async fn new(client: &Client, is_live: bool) -> Self {

        let mktdata = match Self::subscribe_to_stream(client).await {
            Ok(data) => data,
            Err(err) => {
                error!("Failed to subscribe to stream, error: {err}");
                panic!("{:?}", err);
            }
        };
        return mktdata
    }

    pub async fn subscribe_to_stream(client: &Client) -> Result<Self, StreamApiError> {
        let (mut stream, mut subscription) = client
            .subscribe::<RealtimeData<IEX>>()
            .await.unwrap();

        Ok(MktData{})
    }

//    async fn request_account_details() -> Self {
//        //        BarsReqInit::init
//        //        let request = Request::<'a>::Authenticate { client.api_info().api_key, client.api_info().secret };
//        //
//        //        Client::issue<Get>(&()).await.unwrap();
//
//    }
}
