use gcloud_sdk::google::logging::v2::logging_service_v2_client::LoggingServiceV2Client;
use gcloud_sdk::google::logging::v2::{log_entry, LogEntry, WriteLogEntriesRequest};
use gcloud_sdk::*;

use anyhow::Result;


struct CloudLogging {
    name: String,
    client: GoogleApi<LoggingServiceV2Client<GoogleAuthMiddleware>>
}
impl CloudLogging {

    pub async fn new(logging_name: &str, google_project_id: &str) -> Result<Self> {
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter("gcloud_sdk=info")
            .finish();
        tracing::subscriber::set_global_default(subscriber)?;

        let name = format!("projects/{}/logs/{}", google_project_id, logging_name);
        let client = GoogleApi::from_function(
            LoggingServiceV2Client::new,
            "https://logging.googleapis.com",
            None,
            )
            .await?;

        Ok(CloudLogging {
            name,
            client,
        })
    }

    async fn send_log(&self, message: &str) -> Result<()> {
        let response = self.client
            .get()
            .write_log_entries(tonic::Request::new(WriteLogEntriesRequest {
                log_name: self.name.clone(),
                entries: vec![LogEntry {
                    log_name: self.name.clone(),
                    resource: Some(gcloud_sdk::google::api::MonitoredResource {
                        r#type: "global".to_string(),
                        ..Default::default()
                    }),
                    payload: Some(log_entry::Payload::TextPayload(
                                     message.to_string(),
                                     )),
                                     ..Default::default()
                }],
                ..Default::default()
            }))
        .await?;

        println!("Response: {:?}", response);

        Ok(())
    }
}
