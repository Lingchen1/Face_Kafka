use serde::Serialize;
use tauri::ipc::Channel;
use tauri::{AppHandle, Manager};
use uuid::Uuid;

use crate::error::{wrap_async, ApiResult};
use crate::kafka::connection_manager::create_producer;
use crate::kafka::consumer_session::{ConsumeRequest, SessionEvent};
use crate::kafka::producer_service::{self, ProduceResult, ProduceTombstoneRequest};
use crate::state::AppState;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StartConsumeResult {
    pub session_id: String,
}

#[tauri::command]
pub async fn start_consume(
    app: AppHandle,
    request: ConsumeRequest,
    on_event: Channel<SessionEvent>,
) -> ApiResult<StartConsumeResult> {
    let operation_id = Uuid::new_v4().to_string();
    let state = app.state::<AppState>();
    wrap_async(operation_id, || async move {
        let profile = {
            let manager = state.connections.lock().map_err(|_| {
                crate::error::AppError::Storage("Connection manager lock poisoned".into())
            })?;
            manager.get_profile(&request.connection_id)?
        };
        let session_id = state.sessions.start(profile, request, on_event).await?;
        Ok(StartConsumeResult { session_id })
    })
    .await
}

#[tauri::command]
pub async fn stop_consume(app: AppHandle, session_id: String) -> ApiResult<()> {
    let operation_id = Uuid::new_v4().to_string();
    let state = app.state::<AppState>();
    wrap_async(operation_id, || async move {
        state.sessions.stop(&session_id).await
    })
    .await
}

#[tauri::command]
pub async fn produce_tombstone(
    app: AppHandle,
    request: ProduceTombstoneRequest,
) -> ApiResult<ProduceResult> {
    let operation_id = Uuid::new_v4().to_string();
    let state = app.state::<AppState>();
    wrap_async(operation_id, || async move {
        let profile = {
            let manager = state.connections.lock().map_err(|_| {
                crate::error::AppError::Storage("Connection manager lock poisoned".into())
            })?;
            manager.get_profile(&request.connection_id)?
        };
        let producer = create_producer(&profile)?;
        let result = producer_service::produce_tombstone(&producer, &request).await?;
        if let Ok(db) = state.db.lock() {
            let _ = db.append_audit(
                Some(&request.connection_id),
                "produce_tombstone",
                Some(&request.topic),
                "ok",
                Some(&request.key),
            );
        }
        Ok(result)
    })
    .await
}
