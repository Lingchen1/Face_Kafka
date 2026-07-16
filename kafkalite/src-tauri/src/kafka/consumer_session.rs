use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use tauri::ipc::Channel;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::error::{ApiError, AppError};
use crate::kafka::config_builder::{build_client_config, ConnectionProfile};
use crate::kafka::message_decoder::{decode_message, MessageDto};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumeStart {
    #[serde(rename = "type")]
    pub kind: String,
    pub value: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumeRequest {
    pub connection_id: String,
    pub topic: String,
    pub partitions: PartitionSelector,
    pub start: ConsumeStart,
    pub limit: u32,
    pub batch_size: Option<u32>,
    pub idle_timeout_ms: Option<u64>,
    pub max_preview_bytes: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum PartitionSelector {
    All(String),
    List(Vec<i32>),
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "type")]
pub enum SessionEvent {
    #[serde(rename = "started")]
    Started { session_id: String },
    #[serde(rename = "batch")]
    Batch {
        session_id: String,
        messages: Vec<MessageDto>,
    },
    #[serde(rename = "progress")]
    Progress { session_id: String, count: u32 },
    #[serde(rename = "finished")]
    Finished { session_id: String, reason: String },
    #[serde(rename = "error")]
    Error {
        session_id: String,
        error: ApiError,
    },
}

struct SessionHandle {
    cancel: Arc<AtomicBool>,
}

pub struct ConsumerSessionManager {
    sessions: Mutex<HashMap<String, SessionHandle>>,
}

impl ConsumerSessionManager {
    pub fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
        }
    }

    pub async fn stop(&self, session_id: &str) -> Result<(), AppError> {
        let sessions = self.sessions.lock().await;
        if let Some(handle) = sessions.get(session_id) {
            handle.cancel.store(true, Ordering::SeqCst);
            Ok(())
        } else {
            Err(AppError::InvalidRequest(format!(
                "Unknown session: {session_id}"
            )))
        }
    }

    pub async fn stop_all(&self) {
        let sessions = self.sessions.lock().await;
        for handle in sessions.values() {
            handle.cancel.store(true, Ordering::SeqCst);
        }
    }

    pub async fn start(
        self: &Arc<Self>,
        profile: ConnectionProfile,
        request: ConsumeRequest,
        channel: Channel<SessionEvent>,
    ) -> Result<String, AppError> {
        let session_id = Uuid::new_v4().to_string();
        let cancel = Arc::new(AtomicBool::new(false));
        {
            let mut sessions = self.sessions.lock().await;
            sessions.insert(
                session_id.clone(),
                SessionHandle {
                    cancel: cancel.clone(),
                },
            );
        }

        let manager = Arc::clone(self);
        let sid = session_id.clone();
        let event_channel = channel.clone();
        let _ = channel.send(SessionEvent::Started {
            session_id: session_id.clone(),
        });
        tauri::async_runtime::spawn(async move {
            let result = run_consume_session(
                profile,
                request,
                sid.clone(),
                cancel,
                event_channel.clone(),
            )
            .await;
            if let Err(err) = result {
                let _ = event_channel.send(SessionEvent::Error {
                    session_id: sid.clone(),
                    error: ApiError::from_app_error(err, None),
                });
            }
            let mut sessions = manager.sessions.lock().await;
            sessions.remove(&sid);
        });
        Ok(session_id)
    }
}

async fn run_consume_session(
    profile: ConnectionProfile,
    request: ConsumeRequest,
    session_id: String,
    cancel: Arc<AtomicBool>,
    channel: Channel<SessionEvent>,
) -> Result<(), AppError> {
    let limit = request.limit.clamp(1, 10_000);
    let batch_size = request.batch_size.unwrap_or(200).clamp(1, 1000) as usize;
    let idle_timeout = Duration::from_millis(request.idle_timeout_ms.unwrap_or(4000));
    let max_preview_bytes = request.max_preview_bytes.unwrap_or(512 * 1024);

    let mut consumer_config = build_client_config(&profile)?;
    consumer_config.set(
        "group.id",
        format!("kafkalite-session-{}", &session_id[..8]),
    );
    consumer_config.set("enable.auto.commit", "false");
    consumer_config.set("enable.partition.eof", "true");
    // StreamConsumer requires an active Tokio runtime (available inside this spawned task).
    let consumer: StreamConsumer = consumer_config
        .create()
        .map_err(|err| AppError::KafkaConnection(err.to_string()))?;

    let partitions = resolve_partitions(&consumer, &request)?;
    if partitions.is_empty() {
        let _ = channel.send(SessionEvent::Finished {
            session_id: session_id.clone(),
            reason: "idle".into(),
        });
        return Ok(());
    }

    let mut tpl = TopicPartitionList::new();
    match request.start.kind.as_str() {
        "earliest" => {
            for partition in &partitions {
                tpl.add_partition_offset(&request.topic, *partition, Offset::Beginning)
                    .map_err(|err| AppError::InvalidRequest(err.to_string()))?;
            }
        }
        "latest" => {
            for partition in &partitions {
                tpl.add_partition_offset(&request.topic, *partition, Offset::End)
                    .map_err(|err| AppError::InvalidRequest(err.to_string()))?;
            }
        }
        "offset" => {
            let offset = request.start.value.ok_or_else(|| {
                AppError::InvalidRequest("start.value is required for offset mode".into())
            })?;
            for partition in &partitions {
                tpl.add_partition_offset(&request.topic, *partition, Offset::Offset(offset))
                    .map_err(|err| AppError::InvalidRequest(err.to_string()))?;
            }
        }
        "timestamp" => {
            let timestamp = request.start.value.ok_or_else(|| {
                AppError::InvalidRequest("start.value is required for timestamp mode".into())
            })?;
            let mut query = TopicPartitionList::new();
            for partition in &partitions {
                query
                    .add_partition_offset(&request.topic, *partition, Offset::Offset(timestamp))
                    .map_err(|err| AppError::InvalidRequest(err.to_string()))?;
            }
            let offsets = consumer
                .offsets_for_times(query, Duration::from_secs(10))
                .map_err(|err| AppError::KafkaConnection(err.to_string()))?;
            for element in offsets.elements() {
                match element.offset() {
                    Offset::Offset(value) if value >= 0 => {
                        tpl.add_partition_offset(
                            element.topic(),
                            element.partition(),
                            Offset::Offset(value),
                        )
                        .map_err(|err| AppError::InvalidRequest(err.to_string()))?;
                    }
                    _ => {}
                }
            }
            if tpl.count() == 0 {
                let _ = channel.send(SessionEvent::Finished {
                    session_id: session_id.clone(),
                    reason: "idle".into(),
                });
                return Ok(());
            }
        }
        other => {
            return Err(AppError::InvalidRequest(format!(
                "Unsupported start type: {other}"
            )));
        }
    }

    consumer
        .assign(&tpl)
        .map_err(|err| AppError::KafkaConnection(err.to_string()))?;

    let mut batch = Vec::with_capacity(batch_size);
    let mut count = 0u32;
    let mut last_message_at = Instant::now();
    let flush_interval = Duration::from_millis(150);
    let mut last_flush = Instant::now();

    loop {
        if cancel.load(Ordering::SeqCst) {
            flush_batch(&channel, &session_id, &mut batch);
            let _ = channel.send(SessionEvent::Finished {
                session_id: session_id.clone(),
                reason: "cancelled".into(),
            });
            break;
        }

        if count >= limit {
            flush_batch(&channel, &session_id, &mut batch);
            let _ = channel.send(SessionEvent::Finished {
                session_id: session_id.clone(),
                reason: "limit".into(),
            });
            break;
        }

        if last_message_at.elapsed() >= idle_timeout {
            flush_batch(&channel, &session_id, &mut batch);
            let _ = channel.send(SessionEvent::Finished {
                session_id: session_id.clone(),
                reason: "idle".into(),
            });
            break;
        }

        let recv_timeout = tokio::time::timeout(Duration::from_millis(200), consumer.recv()).await;
        match recv_timeout {
            Ok(Ok(message)) => {
                let dto = decode_message(&message, max_preview_bytes);
                batch.push(dto);
                count += 1;
                last_message_at = Instant::now();
                let _ = channel.send(SessionEvent::Progress {
                    session_id: session_id.clone(),
                    count,
                });
                if batch.len() >= batch_size || last_flush.elapsed() >= flush_interval {
                    flush_batch(&channel, &session_id, &mut batch);
                    last_flush = Instant::now();
                }
            }
            Ok(Err(err)) => {
                let message = err.to_string();
                if message.to_lowercase().contains("partition eof") {
                    continue;
                }
                flush_batch(&channel, &session_id, &mut batch);
                return Err(AppError::KafkaConnection(message));
            }
            Err(_) => {
                if !batch.is_empty() && last_flush.elapsed() >= flush_interval {
                    flush_batch(&channel, &session_id, &mut batch);
                    last_flush = Instant::now();
                }
            }
        }
    }

    Ok(())
}

fn flush_batch(channel: &Channel<SessionEvent>, session_id: &str, batch: &mut Vec<MessageDto>) {
    if batch.is_empty() {
        return;
    }
    let messages = std::mem::take(batch);
    let _ = channel.send(SessionEvent::Batch {
        session_id: session_id.to_string(),
        messages,
    });
}

fn resolve_partitions(
    consumer: &StreamConsumer,
    request: &ConsumeRequest,
) -> Result<Vec<i32>, AppError> {
    match &request.partitions {
        PartitionSelector::List(list) => Ok(list.clone()),
        PartitionSelector::All(value) if value == "all" => {
            let metadata = consumer
                .fetch_metadata(Some(&request.topic), Duration::from_secs(10))
                .map_err(|err| AppError::KafkaConnection(err.to_string()))?;
            let topic = metadata
                .topics()
                .iter()
                .find(|item| item.name() == request.topic)
                .ok_or_else(|| {
                    AppError::InvalidRequest(format!("Topic not found: {}", request.topic))
                })?;
            Ok(topic.partitions().iter().map(|p| p.id()).collect())
        }
        PartitionSelector::All(other) => Err(AppError::InvalidRequest(format!(
            "Invalid partitions selector: {other}"
        ))),
    }
}
