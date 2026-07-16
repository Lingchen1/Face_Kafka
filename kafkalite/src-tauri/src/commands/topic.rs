use tauri::{AppHandle, Manager, State};
use uuid::Uuid;

use crate::error::{wrap, wrap_async, ApiResult};
use crate::kafka::admin_service::{
    self, CreateTopicRequest, DeleteRecordsRequest, DeleteRecordsResultItem, TopicDetail,
    TopicSummary,
};
use crate::kafka::connection_manager::{create_admin_client, create_base_consumer};
use crate::state::AppState;

#[tauri::command]
pub fn list_topics(
    state: State<'_, AppState>,
    connection_id: String,
) -> ApiResult<Vec<TopicSummary>> {
    let operation_id = Uuid::new_v4().to_string();
    wrap(operation_id, || {
        let manager = state.connections.lock().map_err(|_| {
            crate::error::AppError::Storage("Connection manager lock poisoned".into())
        })?;
        manager.list_topics(&connection_id)
    })
}

#[tauri::command]
pub fn describe_topic(
    state: State<'_, AppState>,
    connection_id: String,
    topic_name: String,
) -> ApiResult<TopicDetail> {
    let operation_id = Uuid::new_v4().to_string();
    wrap(operation_id, || {
        let manager = state.connections.lock().map_err(|_| {
            crate::error::AppError::Storage("Connection manager lock poisoned".into())
        })?;
        manager.describe_topic(&connection_id, &topic_name)
    })
}

#[tauri::command]
pub async fn create_topic(
    app: AppHandle,
    request: CreateTopicRequest,
) -> ApiResult<TopicSummary> {
    let operation_id = Uuid::new_v4().to_string();
    let state = app.state::<AppState>();
    wrap_async(operation_id, || async move {
        let profile = {
            let manager = state.connections.lock().map_err(|_| {
                crate::error::AppError::Storage("Connection manager lock poisoned".into())
            })?;
            manager.get_profile(&request.connection_id)?
        };
        let admin = create_admin_client(&profile)?;
        let summary = admin_service::create_topic(&admin, &request).await?;
        if let Ok(db) = state.db.lock() {
            let _ = db.append_audit(
                Some(&request.connection_id),
                "create_topic",
                Some(&request.name),
                "ok",
                None,
            );
        }
        Ok(summary)
    })
    .await
}

#[tauri::command]
pub async fn delete_topic(
    app: AppHandle,
    connection_id: String,
    topic_name: String,
    confirmation_text: String,
) -> ApiResult<()> {
    let operation_id = Uuid::new_v4().to_string();
    let state = app.state::<AppState>();
    wrap_async(operation_id, || async move {
        let profile = {
            let manager = state.connections.lock().map_err(|_| {
                crate::error::AppError::Storage("Connection manager lock poisoned".into())
            })?;
            manager.get_profile(&connection_id)?
        };
        let admin = create_admin_client(&profile)?;
        admin_service::delete_topic(&admin, &topic_name, &confirmation_text).await?;
        if let Ok(db) = state.db.lock() {
            let _ = db.append_audit(
                Some(&connection_id),
                "delete_topic",
                Some(&topic_name),
                "ok",
                None,
            );
        }
        Ok(())
    })
    .await
}

#[tauri::command]
pub async fn delete_records_before(
    app: AppHandle,
    request: DeleteRecordsRequest,
) -> ApiResult<Vec<DeleteRecordsResultItem>> {
    let operation_id = Uuid::new_v4().to_string();
    let state = app.state::<AppState>();
    wrap_async(operation_id, || async move {
        let profile = {
            let manager = state.connections.lock().map_err(|_| {
                crate::error::AppError::Storage("Connection manager lock poisoned".into())
            })?;
            manager.get_profile(&request.connection_id)?
        };

        let expected = format!(
            "{}/{}/{}",
            profile.name,
            request.topic,
            request
                .targets
                .iter()
                .map(|t| format!("{}:{}", t.partition, t.before_offset))
                .collect::<Vec<_>>()
                .join(",")
        );

        let admin = create_admin_client(&profile)?;
        let consumer = create_base_consumer(&profile)?;
        let results =
            admin_service::delete_records_before(&admin, &consumer, &request, &expected).await?;

        if let Ok(db) = state.db.lock() {
            let _ = db.append_audit(
                Some(&request.connection_id),
                "delete_records_before",
                Some(&request.topic),
                "ok",
                Some(&serde_json::to_string(&results).unwrap_or_default()),
            );
        }
        Ok(results)
    })
    .await
}
