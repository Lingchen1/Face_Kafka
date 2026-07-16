use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::metadata::Metadata;
use rdkafka::{Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::error::AppError;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicSummary {
    pub name: String,
    pub partition_count: usize,
    pub internal: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicDetail {
    pub name: String,
    pub partition_count: usize,
    pub internal: bool,
    pub partitions: Vec<PartitionInfo>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionInfo {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub beginning_offset: Option<i64>,
    pub end_offset: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateTopicRequest {
    pub connection_id: String,
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i32,
    pub cleanup_policy: Option<String>,
    pub retention_ms: Option<i64>,
    pub retention_bytes: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteRecordsTarget {
    pub partition: i32,
    pub before_offset: i64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteRecordsRequest {
    pub connection_id: String,
    pub topic: String,
    pub targets: Vec<DeleteRecordsTarget>,
    pub confirmation_text: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteRecordsResultItem {
    pub partition: i32,
    pub before_offset: i64,
    pub new_beginning_offset: Option<i64>,
    pub success: bool,
    pub error: Option<String>,
}

pub fn list_topics(
    _admin: &AdminClient<DefaultClientContext>,
    consumer: &BaseConsumer,
) -> Result<Vec<TopicSummary>, AppError> {
    let metadata = fetch_all_metadata(consumer)?;
    Ok(metadata
        .topics()
        .iter()
        .map(|topic| TopicSummary {
            name: topic.name().to_string(),
            partition_count: topic.partitions().len(),
            internal: topic.name().starts_with("__"),
        })
        .collect())
}

pub fn describe_topic(
    consumer: &BaseConsumer,
    topic_name: &str,
) -> Result<TopicDetail, AppError> {
    let metadata = consumer
        .fetch_metadata(Some(topic_name), Duration::from_secs(10))
        .map_err(|err| AppError::KafkaConnection(err.to_string()))?;

    let topic = metadata
        .topics()
        .iter()
        .find(|item| item.name() == topic_name)
        .ok_or_else(|| AppError::InvalidRequest(format!("Topic not found: {topic_name}")))?;

    let partitions = topic
        .partitions()
        .iter()
        .map(|partition| {
            let watermarks = consumer
                .fetch_watermarks(topic_name, partition.id(), Duration::from_secs(5))
                .ok();
            PartitionInfo {
                id: partition.id(),
                leader: partition.leader(),
                replicas: partition.replicas().to_vec(),
                isr: partition.isr().to_vec(),
                beginning_offset: watermarks.map(|(lo, _)| lo),
                end_offset: watermarks.map(|(_, hi)| hi),
            }
        })
        .collect();

    Ok(TopicDetail {
        name: topic.name().to_string(),
        partition_count: topic.partitions().len(),
        internal: topic.name().starts_with("__"),
        partitions,
    })
}

pub async fn create_topic(
    admin: &AdminClient<DefaultClientContext>,
    request: &CreateTopicRequest,
) -> Result<TopicSummary, AppError> {
    validate_topic_name(&request.name)?;
    if request.partitions <= 0 {
        return Err(AppError::InvalidRequest(
            "partitions must be greater than 0".into(),
        ));
    }
    if request.replication_factor <= 0 {
        return Err(AppError::InvalidRequest(
            "replicationFactor must be greater than 0".into(),
        ));
    }

    let cleanup = request.cleanup_policy.clone();
    let retention_ms = request.retention_ms.map(|v| v.to_string());
    let retention_bytes = request.retention_bytes.map(|v| v.to_string());

    let mut topic = NewTopic::new(
        &request.name,
        request.partitions,
        TopicReplication::Fixed(request.replication_factor),
    );
    if let Some(ref policy) = cleanup {
        topic = topic.set("cleanup.policy", policy);
    }
    if let Some(ref value) = retention_ms {
        topic = topic.set("retention.ms", value);
    }
    if let Some(ref value) = retention_bytes {
        topic = topic.set("retention.bytes", value);
    }

    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(15)));
    let results = admin
        .create_topics(&[topic], &opts)
        .await
        .map_err(|err| AppError::KafkaConnection(err.to_string()))?;

    match results.first() {
        Some(Ok(_)) => Ok(TopicSummary {
            name: request.name.clone(),
            partition_count: request.partitions as usize,
            internal: false,
        }),
        Some(Err((_, code))) => Err(AppError::InvalidRequest(format!(
            "Failed to create topic: {code:?}"
        ))),
        None => Err(AppError::KafkaConnection(
            "Empty create topic response".into(),
        )),
    }
}

pub async fn delete_topic(
    admin: &AdminClient<DefaultClientContext>,
    topic_name: &str,
    confirmation_text: &str,
) -> Result<(), AppError> {
    if confirmation_text != topic_name {
        return Err(AppError::InvalidRequest(
            "DELETE_NOT_CONFIRMED: confirmation text does not match topic name".into(),
        ));
    }

    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(15)));
    let results = admin
        .delete_topics(&[topic_name], &opts)
        .await
        .map_err(|err| AppError::KafkaConnection(err.to_string()))?;

    match results.first() {
        Some(Ok(_)) => Ok(()),
        Some(Err((_, code))) => Err(AppError::InvalidRequest(format!(
            "Failed to delete topic: {code:?}"
        ))),
        None => Err(AppError::KafkaConnection(
            "Empty delete topic response".into(),
        )),
    }
}

pub async fn delete_records_before(
    admin: &AdminClient<DefaultClientContext>,
    consumer: &BaseConsumer,
    request: &DeleteRecordsRequest,
    expected_confirmation: &str,
) -> Result<Vec<DeleteRecordsResultItem>, AppError> {
    if request.confirmation_text != expected_confirmation {
        return Err(AppError::InvalidRequest(
            "DELETE_NOT_CONFIRMED: confirmation text mismatch".into(),
        ));
    }
    if request.targets.is_empty() {
        return Err(AppError::InvalidRequest(
            "At least one partition target is required".into(),
        ));
    }

    let mut tpl = TopicPartitionList::new();
    for target in &request.targets {
        let (beginning, end) = consumer
            .fetch_watermarks(
                &request.topic,
                target.partition,
                Duration::from_secs(5),
            )
            .map_err(|err| AppError::KafkaConnection(err.to_string()))?;

        if target.before_offset <= beginning || target.before_offset > end {
            return Err(AppError::InvalidRequest(format!(
                "INVALID_OFFSET: partition {} beforeOffset {} is outside ({}, {}]",
                target.partition, target.before_offset, beginning, end
            )));
        }

        tpl.add_partition_offset(
            &request.topic,
            target.partition,
            Offset::Offset(target.before_offset),
        )
        .map_err(|err| AppError::InvalidRequest(err.to_string()))?;
    }

    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(20)));
    let result_tpl = admin
        .delete_records(&tpl, &opts)
        .await
        .map_err(|err| AppError::KafkaConnection(err.to_string()))?;

    let mut results = Vec::new();
    for element in result_tpl.elements() {
        let success = element.error().is_ok();
        let new_beginning = if success {
            consumer
                .fetch_watermarks(element.topic(), element.partition(), Duration::from_secs(5))
                .ok()
                .map(|(lo, _)| lo)
        } else {
            None
        };

        results.push(DeleteRecordsResultItem {
            partition: element.partition(),
            before_offset: match element.offset() {
                Offset::Offset(value) => value,
                _ => -1,
            },
            new_beginning_offset: new_beginning,
            success,
            error: element.error().err().map(|err| err.to_string()),
        });
    }

    Ok(results)
}

fn fetch_all_metadata(consumer: &BaseConsumer) -> Result<Metadata, AppError> {
    consumer
        .fetch_metadata(None, Duration::from_secs(10))
        .map_err(|err| AppError::KafkaConnection(err.to_string()))
}

fn validate_topic_name(name: &str) -> Result<(), AppError> {
    if name.is_empty() || name.len() > 249 {
        return Err(AppError::InvalidRequest(
            "Topic name length must be 1-249".into(),
        ));
    }
    if !name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
    {
        return Err(AppError::InvalidRequest(
            "Topic name contains invalid characters".into(),
        ));
    }
    Ok(())
}
