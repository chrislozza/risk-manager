pub struct WebSocketClient {
    client: Arc<Mutex<Client>>,
    subscriber: broadcast::Sender<Event>,
    is_alive: Arc<Mutex<bool>>,
}

impl Clone for WebSocketClient {
    fn clone(&self) -> Self {
        WebSocketClient {
            client: Arc::clone(&self.client),
            subscriber: self.subscriber.clone(),
            is_alive: Arc::clone(&self.is_alive),
        }
    }
    fn clone_from(&mut self, source: &Self) {
        *self = source.clone()
    }
}

impl WebSocketClient {
    pub fn new(client: Arc<Mutex<Client>>, subscriber: broadcast::Sender<Event>) -> Self {
        WebSocketClient {
            client,
            subscriber,
            is_alive: Arc::new(Mutex::new(true)),
        }
    }
}
