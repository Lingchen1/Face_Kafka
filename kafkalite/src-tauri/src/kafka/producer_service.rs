use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::error::AppError;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProduceTombstoneRequest {
    pub connection_id: String,
    pub topic: String,
    pub key: String,
    pub partition: Option<i32>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProduceResult {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

pub async fn produce_tombstone(
    producer: &FutureProducer,
    request: &ProduceTombstoneRequest,
) -> Result<ProduceResult, AppError> {
    if request.key.is_empty() {
        return Err(AppError::InvalidRequest(
            "Tombstone requires a non-empty key".into(),
        ));
    }

    let mut record = FutureRecord::<str, str>::to(&request.topic).key(&request.key);
    if let Some(partition) = request.partition {
        record = record.partition(partition);
    }

    let delivery = producer
        .send(record, Timeout::After(Duration::from_secs(10)))
        .await
        .map_err(|(err, _)| AppError::KafkaConnection(err.to_string()))?;

    Ok(ProduceResult {
        topic: request.topic.clone(),
        partition: delivery.partition,
        offset: delivery.offset,
    })
}
