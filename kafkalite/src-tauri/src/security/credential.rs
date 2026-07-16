use std::collections::HashMap;
use std::sync::Mutex;

use uuid::Uuid;

use crate::error::AppError;

static SECRETS: Mutex<Option<HashMap<String, String>>> = Mutex::new(None);

fn store() -> Result<std::sync::MutexGuard<'static, Option<HashMap<String, String>>>, AppError> {
    SECRETS
        .lock()
        .map_err(|_| AppError::Storage("Credential store lock poisoned".into()))
}

pub struct CredentialService;

impl CredentialService {
    pub fn save_secret(connection_id: &str, password: &str) -> Result<String, AppError> {
        let credential_ref = format!("kafkalite:{connection_id}");
        let mut guard = store()?;
        if guard.is_none() {
            *guard = Some(HashMap::new());
        }
        if let Some(map) = guard.as_mut() {
            map.insert(credential_ref.clone(), password.to_string());
        }
        Ok(credential_ref)
    }

    pub fn get_secret(credential_ref: &str) -> Result<Option<String>, AppError> {
        let guard = store()?;
        Ok(guard
            .as_ref()
            .and_then(|map| map.get(credential_ref).cloned()))
    }

    pub fn delete_secret(credential_ref: &str) -> Result<(), AppError> {
        let mut guard = store()?;
        if let Some(map) = guard.as_mut() {
            map.remove(credential_ref);
        }
        Ok(())
    }

    pub fn new_credential_ref() -> String {
        format!("kafkalite:{}", Uuid::new_v4())
    }
}
