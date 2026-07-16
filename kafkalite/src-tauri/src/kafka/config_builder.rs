use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};

use crate::error::AppError;
use crate::security::credential::CredentialService;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionProfile {
    pub id: String,
    pub name: String,
    pub bootstrap_servers: Vec<String>,
    pub security_protocol: SecurityProtocol,
    pub sasl_mechanism: Option<SaslMechanism>,
    pub username: Option<String>,
    pub credential_ref: Option<String>,
    pub ssl_ca_path: Option<String>,
    pub ssl_certificate_path: Option<String>,
    pub ssl_key_path: Option<String>,
    pub client_id: String,
    pub request_timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SecurityProtocol {
    #[serde(rename = "PLAINTEXT")]
    Plaintext,
    #[serde(rename = "SSL")]
    Ssl,
    #[serde(rename = "SASL_PLAINTEXT")]
    SaslPlaintext,
    #[serde(rename = "SASL_SSL")]
    SaslSsl,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SaslMechanism {
    #[serde(rename = "PLAIN")]
    Plain,
    #[serde(rename = "SCRAM-SHA-256")]
    ScramSha256,
    #[serde(rename = "SCRAM-SHA-512")]
    ScramSha512,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveConnectionRequest {
    pub id: Option<String>,
    pub name: String,
    pub bootstrap_servers: String,
    pub security_protocol: SecurityProtocol,
    pub sasl_mechanism: Option<SaslMechanism>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub ssl_ca_path: Option<String>,
    pub ssl_certificate_path: Option<String>,
    pub ssl_key_path: Option<String>,
    pub client_id: Option<String>,
    pub request_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestConnectionResult {
    pub success: bool,
    pub latency_ms: u64,
    pub broker_count: usize,
    pub cluster_id: Option<String>,
    pub warnings: Vec<String>,
}

pub fn build_client_config(profile: &ConnectionProfile) -> Result<ClientConfig, AppError> {
    let mut config = ClientConfig::new();
    let bootstrap = profile.bootstrap_servers.join(",");
    config.set("bootstrap.servers", bootstrap);
    config.set("client.id", &profile.client_id);
    config.set(
        "socket.timeout.ms",
        profile.request_timeout_ms.to_string(),
    );

    let protocol = match profile.security_protocol {
        SecurityProtocol::Plaintext => "PLAINTEXT",
        SecurityProtocol::Ssl => "SSL",
        SecurityProtocol::SaslPlaintext => "SASL_PLAINTEXT",
        SecurityProtocol::SaslSsl => "SASL_SSL",
    };
    config.set("security.protocol", protocol);

    if let Some(mechanism) = &profile.sasl_mechanism {
        let mech = match mechanism {
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
        };
        config.set("sasl.mechanism", mech);
    }

    if let Some(username) = &profile.username {
        config.set("sasl.username", username);
    }

    if let Some(credential_ref) = &profile.credential_ref {
        if let Some(password) = CredentialService::get_secret(credential_ref)? {
            config.set("sasl.password", password);
        }
    }

    if let Some(path) = &profile.ssl_ca_path {
        config.set("ssl.ca.location", path);
    }
    if let Some(path) = &profile.ssl_certificate_path {
        config.set("ssl.certificate.location", path);
    }
    if let Some(path) = &profile.ssl_key_path {
        config.set("ssl.key.location", path);
    }

    Ok(config)
}

pub fn parse_bootstrap_servers(raw: &str) -> Result<Vec<String>, AppError> {
    let servers: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect();
    if servers.is_empty() {
        return Err(AppError::InvalidRequest(
            "Bootstrap servers cannot be empty".into(),
        ));
    }
    Ok(servers)
}
