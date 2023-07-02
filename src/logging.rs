use gcloud_sdk::google::logging::v2::logging_service_v2_client::LoggingServiceV2Client;
use gcloud_sdk::google::logging::v2::{log_entry, LogEntry, WriteLogEntriesRequest};
use gcloud_sdk::*;

use anyhow::Result;
use chrono::prelude::*;
use log::{Level, Metadata, Record};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

pub struct SimpleLogger {
    cloud_log: Option<CloudLogging>,
    publish_to_cloud: Option<oneshot::Sender<&'static str>>,
}

impl SimpleLogger {
    pub fn new(cloud_log: Option<CloudLogging>) -> Self {
        let publish_to_cloud = match cloud_log.clone() {
            Some(mut logger) => Some(logger.get_message_publisher().unwrap()),
            _ => None,
        };

        SimpleLogger {
            cloud_log,
            publish_to_cloud,
        }
    }
}

impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Info
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            let current_time = Local::now();
            let timestamp = current_time.format("%Y-%m-%d %H:%M:%S").to_string();
            let location = format!("{}:{}", record.file().unwrap(), record.line().unwrap());
            let message = format!(
                "{:<30} {:<10} {:<40.40}  -  {}",
                timestamp,
                record.level(),
                location,
                record.args()
            );
            if let Some(publish_to_cloud) = &self.publish_to_cloud {
                let publish = publish_to_cloud.clone();
                publish.clone().send(&message);
            }
            println!("{message}");
        }
    }

    fn flush(&self) {}
}

#[derive(Debug, Clone)]
pub struct CloudLogging {
    name: String,
    google_project_id: String,
    shutdown_signal: CancellationToken,
}

impl CloudLogging {
    pub async fn new(logging_name: &str, google_project_id: &str, shutdown_signal: CancellationToken) -> Result<Self> {
        let subscriber = tracing_subscriber::fmt()
            .compact()
            // Display source code file paths
            .with_file(true)
            // Display source code line numbers
            .with_line_number(true)
            // Display the thread ID an event was recorded on
            .with_thread_ids(true)
            // Don't display the event's target (module path)
            .with_target(true).finish();
        tracing::subscriber::set_global_default(subscriber)?;

        Ok(CloudLogging {
            name: String::from(logging_name),
            google_project_id: String::from(google_project_id),
            shutdown_signal,
        })
    }

    pub fn get_message_publisher(&mut self) -> Result<oneshot::Sender<&'static str>> {
        let (tx, mut rx) = oneshot::channel();

        let logging_name = String::from(&self.name);
        let google_project_id = String::from(&self.google_project_id);
        let shutdown_signal = self.shutdown_signal.clone();
        tokio::spawn(async move {
            let name = format!(
                "projects/{}/logs/{}",
                google_project_id,
                logging_name.clone()
            );
            let client = GoogleApi::from_function(
                LoggingServiceV2Client::new,
                "https://logging.googleapis.com",
                None,
            ).await.unwrap();

            loop {
                tokio::select! {
                    msg = &mut rx => {
                        let response = client
                            .get()
                            .write_log_entries(tonic::Request::new(WriteLogEntriesRequest {
                                log_name: name.clone(),
                                entries: vec![LogEntry {
                                    log_name: name.clone(),
                                    resource: Some(gcloud_sdk::google::api::MonitoredResource {
                                        r#type: "global".to_string(),
                                        ..Default::default()
                                    }),
                                    payload: Some(log_entry::Payload::TextPayload(String::from(msg.unwrap()))),
                                    ..Default::default()
                                }],
                                ..Default::default()
                            }))
                        .await.unwrap();
                    }
                    _ = shutdown_signal.cancelled() => {
                        break;
                    }
                }
            }
        });

        Ok(tx)
    }
}
