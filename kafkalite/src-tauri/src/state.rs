use std::sync::{Arc, Mutex};

use crate::kafka::connection_manager::ConnectionManager;
use crate::kafka::consumer_session::ConsumerSessionManager;
use crate::storage::Database;

pub struct AppState {
    pub db: Mutex<Database>,
    pub connections: Mutex<ConnectionManager>,
    pub sessions: Arc<ConsumerSessionManager>,
}

impl AppState {
    pub fn new(db: Database) -> Self {
        Self {
            db: Mutex::new(db),
            connections: Mutex::new(ConnectionManager::new()),
            sessions: Arc::new(ConsumerSessionManager::new()),
        }
    }
}
