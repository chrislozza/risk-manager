use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use axum::Server;

use super::Event;
use crate::Settings;

pub struct WebHook {
    cancel_token: CancellationToken,
    server: Option<Server>,
}

impl WebHook {
    pub async fn new(_settings: Settings) -> Self {
        WebHook {
            cancel_token: CancellationToken::new(),
            server: None,
        }
    }

    pub fn startup(&self) {}

    pub fn shutdown(&self) {
        self.cancel_token.cancel()
    }

    pub fn run(&mut self, _sender: &mpsc::UnboundedSender<Event>) -> Result<(), ()> {
        let app = Router::new()
            .route("/v1/send-order", post(move |body| post_event(body)))
            .layer(CorsLayer::permissive());

        let server = axum::Server::bind(&"0.0.0.0:496".parse().unwrap())
            .serve(app.into_make_service())
            .await
            .unwrap();

        info!("Webhook started");
        self.server = Some(server);
        Ok(())
    }

    //    async fn post_order(Json(payload): Json<HashMap<String, String>>) -> Json<Value> {
    //
    //        let config = ClientConfig::default().with_auth().await.unwrap();
    //        let client = Client::new(config).await.unwrap();
    //
    //        println!(
    //            "Json payload received {:?}",
    //            serde_json::to_string(&payload).unwrap()
    //            );
    //
    //        let topic = client.topic("vibgo-alerts");
    //        let mut publisher = topic.new_publisher(None);
    //
    //        let pub_client = publisher.clone();
    //
    //        let task: JoinHandle<Result<String, Status>> = tokio::spawn(async move {
    //            let mut msg = PubsubMessage::default();
    //            let order_details = match OrderDetails::new(&payload) {
    //                Ok(var) => var,
    //                Err(err) => {return Err(Status::failed_precondition(err))}
    //            };
    //            let serialised = serde_json::to_string(&order_details).unwrap();
    //            let data = HashMap::from([("type", "alert"), ("payload", serialised.as_str())]);
    //            msg.data = serde_json::to_string(&data).unwrap().into();
    //            let awaiter = pub_client.publish(msg).await;
    //            awaiter.get(None).await
    //        });
    //
    //        let pub_result: Json<Value> = match task.await.unwrap() {
    //            Ok(res) => Json(json!({"response" : 200, "msg": format!("Id:{}", res)})),
    //            Err(err) => Json(json!({"response" : 400, "msg": err.message()})),
    //        };
    //
    //        publisher.shutdown().await;
    //        return pub_result;
    //    }
}
