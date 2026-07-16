use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("Kafka connection failed: {0}")]
    KafkaConnection(String),
    #[error("Kafka authentication failed: {0}")]
    KafkaAuth(String),
    #[error("Kafka operation timed out")]
    KafkaTimeout,
    #[error("Invalid request: {0}")]
    InvalidRequest(String),
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Not connected")]
    NotConnected,
    #[error("Operation cancelled")]
    Cancelled,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiError {
    pub code: String,
    pub message: String,
    pub detail: Option<String>,
    pub retryable: bool,
    pub operation_id: String,
}

impl ApiError {
    pub fn from_app_error(error: AppError, operation_id: Option<String>) -> Self {
        let operation_id = operation_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        match &error {
            AppError::KafkaConnection(_) => Self {
                code: "KAFKA_CONNECTION_FAILED".into(),
                message: error.to_string(),
                detail: None,
                retryable: true,
                operation_id,
            },
            AppError::KafkaAuth(_) => Self {
                code: "KAFKA_AUTH_FAILED".into(),
                message: error.to_string(),
                detail: None,
                retryable: false,
                operation_id,
            },
            AppError::KafkaTimeout => Self {
                code: "KAFKA_TIMEOUT".into(),
                message: error.to_string(),
                detail: None,
                retryable: true,
                operation_id,
            },
            AppError::InvalidRequest(message) => {
                let code = if message.contains("DELETE_NOT_CONFIRMED") {
                    "DELETE_NOT_CONFIRMED"
                } else if message.contains("INVALID_OFFSET") {
                    "INVALID_OFFSET"
                } else if message.to_lowercase().contains("not found") {
                    "TOPIC_NOT_FOUND"
                } else if message.to_lowercase().contains("already exists") {
                    "TOPIC_ALREADY_EXISTS"
                } else {
                    "INVALID_REQUEST"
                };
                Self {
                    code: code.into(),
                    message: error.to_string(),
                    detail: None,
                    retryable: false,
                    operation_id,
                }
            }
            AppError::Storage(_) => Self {
                code: "STORAGE_ERROR".into(),
                message: error.to_string(),
                detail: None,
                retryable: false,
                operation_id,
            },
            AppError::NotConnected => Self {
                code: "NOT_CONNECTED".into(),
                message: error.to_string(),
                detail: None,
                retryable: false,
                operation_id,
            },
            AppError::Cancelled => Self {
                code: "SESSION_CANCELLED".into(),
                message: error.to_string(),
                detail: None,
                retryable: false,
                operation_id,
            },
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiResult<T> {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ApiError>,
}

impl<T> ApiResult<T> {
    pub fn success(data: T) -> Self {
        Self {
            ok: true,
            data: Some(data),
            error: None,
        }
    }

    pub fn failure(error: ApiError) -> Self {
        Self {
            ok: false,
            data: None,
            error: Some(error),
        }
    }
}

pub fn wrap<T, F>(operation_id: String, f: F) -> ApiResult<T>
where
    F: FnOnce() -> Result<T, AppError>,
{
    match f() {
        Ok(data) => ApiResult::success(data),
        Err(err) => ApiResult::failure(ApiError::from_app_error(err, Some(operation_id))),
    }
}

pub async fn wrap_async<T, F, Fut>(operation_id: String, f: F) -> ApiResult<T>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, AppError>>,
{
    match f().await {
        Ok(data) => ApiResult::success(data),
        Err(err) => ApiResult::failure(ApiError::from_app_error(err, Some(operation_id))),
    }
}
