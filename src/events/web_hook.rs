
use tokio_util::sync::CancellationToken;

pub struct WebHook {
    cancel_token: CancellationToken
}

impl WebHook { 
    pub async fn new() -> Self {
        WebHook {
            cancel_token: CancellationToken::new()
        }
    }

    pub fn shutdown(&self) {
        self.cancel_token.cancel()
    }

    pub fn run(&self) -> Result<(), ()> {
        Ok(())
    }
}
