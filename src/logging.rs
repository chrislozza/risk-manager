use anyhow::Result;
use gcloud_sdk::google::api::MonitoredResource;
use gcloud_sdk::google::logging::r#type::LogSeverity;
use gcloud_sdk::google::logging::v2::log_entry::Payload;
use gcloud_sdk::google::logging::v2::logging_service_v2_client::LoggingServiceV2Client;
use gcloud_sdk::google::logging::v2::LogEntry;
use gcloud_sdk::google::logging::v2::WriteLogEntriesRequest;
use gcloud_sdk::GoogleApi;
use std::str::FromStr;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing::Event;
use tracing::Level;
use tracing::Subscriber;
use tracing_subscriber::filter;
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use tracing_subscriber::Layer;

#[derive(Debug, Clone)]
struct CloudLogPayload {
    severity: LogSeverity,
    message: String,
}

struct LogVisitor {
    message: Option<String>,
}

impl tracing::field::Visit for LogVisitor {
    fn record_debug(&mut self, _field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.message = Some(format!("{:?}", value))
    }
}

pub struct GcpLayer {
    publisher: broadcast::Sender<CloudLogPayload>,
}

impl<S: Subscriber + std::fmt::Debug> Layer<S> for GcpLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = LogVisitor { message: None };
        event.record(&mut visitor);
        if let Some(message) = visitor.message {
            let severity = match *event.metadata().level() {
                Level::TRACE => LogSeverity::Default,
                Level::DEBUG => LogSeverity::Debug,
                Level::INFO => LogSeverity::Info,
                Level::WARN => LogSeverity::Warning,
                Level::ERROR => LogSeverity::Error,
            };
            let _ = self.publisher.send(CloudLogPayload { severity, message });
        }
    }
}

#[derive(Debug, Clone)]
pub struct CloudLogging;

impl CloudLogging {
    pub async fn new(
        log_level: String,
        logging_name: Option<String>,
        google_project_id: Option<String>,
        shutdown_signal: CancellationToken,
    ) -> Result<Self> {
        let level = Level::from_str(&log_level).unwrap();

        if logging_name.is_some() && google_project_id.is_some() {
            let stdout_layer = tracing_subscriber::fmt::layer()
                // Display source code file paths
                .with_file(true)
                // Display source code line numbers
                .with_line_number(true)
                // Display the thread ID an event was recorded on
                .with_thread_ids(true)
                // Don't display the event's target (module path)
                .with_target(false)
                // Use a more compact, abbreviated log format
                .compact();

            let publisher = Self::get_message_publisher(
                shutdown_signal,
                logging_name.unwrap(),
                google_project_id.unwrap(),
            )?;
            let gcp_layer = GcpLayer { publisher };
            let subscriber = tracing_subscriber::registry()
                .with(gcp_layer.with_filter(filter::LevelFilter::from_level(level)))
                .with(stdout_layer.with_filter(filter::LevelFilter::from_level(level)));
            tracing::subscriber::set_global_default(subscriber)?;
        } else {
            let subscriber = tracing_subscriber::fmt()
                // Display source code file paths
                .with_file(true)
                // Display source code line numbers
                .with_line_number(true)
                // Display the thread ID an event was recorded on
                .with_thread_ids(true)
                // Don't display the event's target (module path)
                .with_target(false)
                // Assign a log-level
                .with_max_level(level)
                // Use a more compact, abbreviated log format
                .compact()
                .finish();
            tracing::subscriber::set_global_default(subscriber)?;
        }
        Ok(CloudLogging {})
    }

    fn get_message_publisher(
        shutdown_signal: CancellationToken,
        log_name: String,
        google_project_id: String,
    ) -> Result<broadcast::Sender<CloudLogPayload>> {
        let (publisher, mut subscriber) = broadcast::channel(100);

        let name = format!("projects/{}/logs/{}", google_project_id, log_name);

        let client = GoogleApi::from_function(
            LoggingServiceV2Client::new,
            "https://logging.googleapis.com",
            None,
        );

        tokio::spawn(async move {
            let gcp = client.await.unwrap().clone();
            loop {
                tokio::select! {
                    payload = subscriber.recv() => {
                        let (severity, message) = match payload {
                            Ok(CloudLogPayload{ severity, message }) => (severity, message),
                            _ => continue
                        };
                        let resource = Some(MonitoredResource {
                            r#type: "global".to_string(),
                            ..Default::default()
                        });
                        let payload = Some(Payload::TextPayload(message));
                        let log_entry = LogEntry {
                            log_name: name.to_string(),
                            resource,
                            payload,
                            severity: severity.into(),
                            ..Default::default()
                        };
                        if let Err(err) = gcp
                            .get()
                            .write_log_entries(tonic::Request::new(WriteLogEntriesRequest {
                                log_name: name.to_string(),
                                entries: vec![ log_entry ],
                                ..Default::default()
                            }))
                        .await {
                            error!("Failed to write log entries to gcp, error={}", err);
                            shutdown_signal.cancel()
                        }
                    }
                    _ = shutdown_signal.cancelled() => {
                        break;
                    }
                }
            }
        });

        Ok(publisher)
    }
}
