use anyhow::Result;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct CloudLogging {
    name: Option<String>,
    google_project_id: String,
    shutdown_signal: CancellationToken,
}

impl CloudLogging {
    pub async fn new(
        logging_name: Option<String>,
        google_project_id: String,
        shutdown_signal: CancellationToken,
    ) -> Result<Self> {
        let subscriber = tracing_subscriber::fmt()
            // Use a more compact, abbreviated log format
            .compact()
            // Display source code file paths
            .with_file(true)
            // Display source code line numbers
            .with_line_number(true)
            // Display the thread ID an event was recorded on
            .with_thread_ids(true)
            // Don't display the event's target (module path)
            .with_target(false)
            // Build the subscriber
            .finish();

        tracing::subscriber::set_global_default(subscriber)?;

        Ok(CloudLogging {
            name: logging_name,
            google_project_id,
            shutdown_signal,
        })
    }

    pub fn get_message_publisher(&mut self) -> Result<mpsc::Sender<&'static str>> {
        let (tx, _rx) = mpsc::channel(32);

        //        let logging_name = String::from(self.name.unwrap());
        //        let google_project_id = self.google_project_id;
        //        let shutdown_signal = self.shutdown_signal.clone();
        //        tokio::spawn(async move {
        //            let name = format!(
        //                "projects/{}/logs/{}",
        //                google_project_id,
        //                logging_name.clone()
        //            );
        //            let client = GoogleApi::from_function(
        //                LoggingServiceV2Client::new,
        //                "https://logging.googleapis.com",
        //                None,
        //            ).await.unwrap();
        //
        //            loop {
        //                tokio::select! {
        //                    Some(msg) = rx.recv().await => {
        //                            client
        //                            .get()
        //                            .write_log_entries(tonic::Request::new(WriteLogEntriesRequest {
        //                                log_name: name.clone(),
        //                                entries: vec![LogEntry {
        //                                    log_name: name.clone(),
        //                                    resource: Some(gcloud_sdk::google::api::MonitoredResource {
        //                                        r#type: "global".to_string(),
        //                                        ..Default::default()
        //                                    }),
        //                                    payload: Some(log_entry::Payload::TextPayload(String::from(msg))),
        //                                    ..Default::default()
        //                                }],
        //                                ..Default::default()
        //                            }))
        //                        .await.unwrap();
        //                    }
        //                    _ = shutdown_signal.cancelled() => {
        //                        break;
        //                    }
        //                }
        //            }
        //        });

        Ok(tx)
    }
}
