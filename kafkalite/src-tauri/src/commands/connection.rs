use tauri::{AppHandle, Manager, State};
use uuid::Uuid;

use crate::error::{wrap, ApiResult};
use crate::kafka::config_builder::{ConnectionProfile, SaveConnectionRequest, TestConnectionResult};
use crate::kafka::connection_manager::ConnectionManager;
use crate::security::credential::CredentialService;
use crate::state::AppState;

#[tauri::command]
pub fn list_connections(state: State<'_, AppState>) -> ApiResult<Vec<ConnectionProfile>> {
    let operation_id = Uuid::new_v4().to_string();
    wrap(operation_id, || {
        let db = state.db.lock().map_err(|_| {
            crate::error::AppError::Storage("Database lock poisoned".into())
        })?;
        db.list_connections()
    })
}

#[tauri::command]
pub fn save_connection(
    state: State<'_, AppState>,
    request: SaveConnectionRequest,
) -> ApiResult<ConnectionProfile> {
    let operation_id = Uuid::new_v4().to_string();
    wrap(operation_id, || {
        let mut credential_ref = None;
        if let Some(password) = request.password.as_deref() {
            if !password.is_empty() {
                let connection_id = request
                    .id
                    .clone()
                    .unwrap_or_else(|| Uuid::new_v4().to_string());
                credential_ref = Some(CredentialService::save_secret(&connection_id, password)?);
            }
        }

        let db = state.db.lock().map_err(|_| {
            crate::error::AppError::Storage("Database lock poisoned".into())
        })?;
        db.save_connection(request, credential_ref)
    })
}

#[tauri::command]
pub fn delete_connection(state: State<'_, AppState>, id: String) -> ApiResult<()> {
    let operation_id = Uuid::new_v4().to_string();
    wrap(operation_id, || {
        let db = state.db.lock().map_err(|_| {
            crate::error::AppError::Storage("Database lock poisoned".into())
        })?;
        if let Ok(profile) = db.get_connection(&id) {
            if let Some(credential_ref) = profile.credential_ref {
                CredentialService::delete_secret(&credential_ref)?;
            }
        }
        db.delete_connection(&id)
    })
}

#[tauri::command]
pub fn test_connection(
    state: State<'_, AppState>,
    profile_id: Option<String>,
    request: Option<SaveConnectionRequest>,
) -> ApiResult<TestConnectionResult> {
    let operation_id = Uuid::new_v4().to_string();
    wrap(operation_id, || {
        let profile = resolve_profile(&state, profile_id, request)?;
        ConnectionManager::test_connection(&profile)
    })
}

#[tauri::command]
pub fn connect_cluster(state: State<'_, AppState>, profile_id: String) -> ApiResult<ConnectionProfile> {
    let operation_id = Uuid::new_v4().to_string();
    wrap(operation_id, || {
        let profile = {
            let db = state.db.lock().map_err(|_| {
                crate::error::AppError::Storage("Database lock poisoned".into())
            })?;
            db.get_connection(&profile_id)?
        };

        let mut manager = state.connections.lock().map_err(|_| {
            crate::error::AppError::Storage("Connection manager lock poisoned".into())
        })?;
        manager.connect(profile.clone())?;
        Ok(profile)
    })
}

#[tauri::command]
pub async fn disconnect_cluster(app: AppHandle, profile_id: String) -> ApiResult<()> {
    let operation_id = Uuid::new_v4().to_string();
    let state = app.state::<AppState>();
    state.sessions.stop_all().await;
    wrap(operation_id, || {
        let mut manager = state.connections.lock().map_err(|_| {
            crate::error::AppError::Storage("Connection manager lock poisoned".into())
        })?;
        manager.disconnect(&profile_id)
    })
}

#[tauri::command]
pub fn get_active_connection(state: State<'_, AppState>) -> ApiResult<Option<ConnectionProfile>> {
    let operation_id = Uuid::new_v4().to_string();
    wrap(operation_id, || {
        let manager = state.connections.lock().map_err(|_| {
            crate::error::AppError::Storage("Connection manager lock poisoned".into())
        })?;
        Ok(manager.active_profile().cloned())
    })
}

fn resolve_profile(
    state: &AppState,
    profile_id: Option<String>,
    request: Option<SaveConnectionRequest>,
) -> Result<ConnectionProfile, crate::error::AppError> {
    if let Some(id) = profile_id {
        let db = state.db.lock().map_err(|_| {
            crate::error::AppError::Storage("Database lock poisoned".into())
        })?;
        return db.get_connection(&id);
    }

    let request = request.ok_or_else(|| {
        crate::error::AppError::InvalidRequest("Connection profile is required".into())
    })?;

    let id = request
        .id
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let credential_ref = request.password.as_ref().and_then(|password| {
        if password.is_empty() {
            None
        } else {
            CredentialService::save_secret(&id, password).ok()
        }
    });

    Ok(ConnectionProfile {
        id,
        name: request.name,
        bootstrap_servers: crate::kafka::config_builder::parse_bootstrap_servers(
            &request.bootstrap_servers,
        )?,
        security_protocol: request.security_protocol,
        sasl_mechanism: request.sasl_mechanism,
        username: request.username,
        credential_ref,
        ssl_ca_path: request.ssl_ca_path,
        ssl_certificate_path: request.ssl_certificate_path,
        ssl_key_path: request.ssl_key_path,
        client_id: request
            .client_id
            .unwrap_or_else(|| "kafkalite-test".to_string()),
        request_timeout_ms: request.request_timeout_ms.unwrap_or(10_000),
    })
}
