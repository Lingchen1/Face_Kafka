use std::collections::HashMap;
use std::time::Instant;

use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::FutureProducer;

use crate::error::AppError;
use crate::kafka::admin_service;
use crate::kafka::config_builder::{
    build_client_config, ConnectionProfile, TestConnectionResult,
};

pub struct ClientBundle {
    pub profile: ConnectionProfile,
    pub admin: AdminClient<DefaultClientContext>,
    pub consumer: BaseConsumer,
    pub producer: FutureProducer,
}

pub struct ConnectionManager {
    active_id: Option<String>,
    bundles: HashMap<String, ClientBundle>,
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self {
            active_id: None,
            bundles: HashMap::new(),
        }
    }

    pub fn active_profile(&self) -> Option<&ConnectionProfile> {
        self.active_id
            .as_ref()
            .and_then(|id| self.bundles.get(id))
            .map(|bundle| &bundle.profile)
    }

    pub fn get_profile(&self, connection_id: &str) -> Result<ConnectionProfile, AppError> {
        self.bundles
            .get(connection_id)
            .map(|bundle| bundle.profile.clone())
            .ok_or(AppError::NotConnected)
    }

    pub fn connect(&mut self, profile: ConnectionProfile) -> Result<(), AppError> {
        let id = profile.id.clone();
        let bundle = create_client_bundle(&profile)?;
        self.bundles.insert(id.clone(), bundle);
        self.active_id = Some(id);
        Ok(())
    }

    pub fn disconnect(&mut self, connection_id: &str) -> Result<(), AppError> {
        self.bundles.remove(connection_id);
        if self.active_id.as_deref() == Some(connection_id) {
            self.active_id = None;
        }
        Ok(())
    }

    pub fn test_connection(profile: &ConnectionProfile) -> Result<TestConnectionResult, AppError> {
        let start = Instant::now();
        let bundle = create_client_bundle(profile)?;
        let metadata = bundle
            .consumer
            .fetch_metadata(
                None,
                std::time::Duration::from_millis(profile.request_timeout_ms),
            )
            .map_err(|err| AppError::KafkaConnection(err.to_string()))?;

        Ok(TestConnectionResult {
            success: true,
            latency_ms: start.elapsed().as_millis() as u64,
            broker_count: metadata.brokers().len(),
            cluster_id: None,
            warnings: Vec::new(),
        })
    }

    pub fn list_topics(
        &self,
        connection_id: &str,
    ) -> Result<Vec<admin_service::TopicSummary>, AppError> {
        let bundle = self
            .bundles
            .get(connection_id)
            .ok_or(AppError::NotConnected)?;
        admin_service::list_topics(&bundle.admin, &bundle.consumer)
    }

    pub fn describe_topic(
        &self,
        connection_id: &str,
        topic_name: &str,
    ) -> Result<admin_service::TopicDetail, AppError> {
        let bundle = self
            .bundles
            .get(connection_id)
            .ok_or(AppError::NotConnected)?;
        admin_service::describe_topic(&bundle.consumer, topic_name)
    }
}

pub fn create_admin_client(
    profile: &ConnectionProfile,
) -> Result<AdminClient<DefaultClientContext>, AppError> {
    build_client_config(profile)?
        .create()
        .map_err(|err| map_kafka_error(err.to_string()))
}

pub fn create_base_consumer(profile: &ConnectionProfile) -> Result<BaseConsumer, AppError> {
    let mut consumer_config = build_client_config(profile)?;
    consumer_config.set("group.id", format!("kafkalite-admin-{}", profile.id));
    consumer_config.set("enable.auto.commit", "false");
    consumer_config
        .create()
        .map_err(|err| map_kafka_error(err.to_string()))
}

pub fn create_producer(profile: &ConnectionProfile) -> Result<FutureProducer, AppError> {
    build_client_config(profile)?
        .create()
        .map_err(|err| map_kafka_error(err.to_string()))
}

fn create_client_bundle(profile: &ConnectionProfile) -> Result<ClientBundle, AppError> {
    Ok(ClientBundle {
        profile: profile.clone(),
        admin: create_admin_client(profile)?,
        consumer: create_base_consumer(profile)?,
        producer: create_producer(profile)?,
    })
}

fn map_kafka_error(message: String) -> AppError {
    let lower = message.to_lowercase();
    if lower.contains("authentication") || lower.contains("sasl") {
        AppError::KafkaAuth(message)
    } else if lower.contains("timed out") || lower.contains("timeout") {
        AppError::KafkaTimeout
    } else {
        AppError::KafkaConnection(message)
    }
}
