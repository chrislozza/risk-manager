
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::Settings;
use super::Event;

pub struct WebHook {
    cancel_token: CancellationToken
}

impl WebHook { 
    pub async fn new(settings: Settings) -> Self {
        WebHook {
            cancel_token: CancellationToken::new()
        }
    }

    pub fn startup(&self) {
    }

    pub fn shutdown(&self) {
        self.cancel_token.cancel()
    }

    pub fn run(&self, _sender: &mpsc::UnboundedSender<Event>) -> Result<(), ()> {
        Ok(())
    }
}
