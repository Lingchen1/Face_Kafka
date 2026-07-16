use std::path::PathBuf;

use rusqlite::{params, Connection};
use uuid::Uuid;

use crate::error::AppError;
use crate::kafka::config_builder::{
    parse_bootstrap_servers, ConnectionProfile, SaveConnectionRequest, SecurityProtocol,
};

pub struct Database {
    conn: Connection,
}

impl Database {
    pub fn open(path: PathBuf) -> Result<Self, AppError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|err| AppError::Storage(err.to_string()))?;
        }
        let conn = Connection::open(path).map_err(|err| AppError::Storage(err.to_string()))?;
        let db = Self { conn };
        db.migrate()?;
        Ok(db)
    }

    fn migrate(&self) -> Result<(), AppError> {
        self.conn
            .execute_batch(
                "
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS connections (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    bootstrap_servers TEXT NOT NULL,
                    security_protocol TEXT NOT NULL,
                    sasl_mechanism TEXT,
                    username TEXT,
                    credential_ref TEXT,
                    ssl_ca_path TEXT,
                    ssl_certificate_path TEXT,
                    ssl_key_path TEXT,
                    client_id TEXT NOT NULL,
                    timeout_ms INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS audit_logs (
                    id TEXT PRIMARY KEY,
                    timestamp INTEGER NOT NULL,
                    connection_id TEXT,
                    operation TEXT NOT NULL,
                    resource TEXT,
                    result TEXT NOT NULL,
                    detail_json TEXT
                );
                ",
            )
            .map_err(|err| AppError::Storage(err.to_string()))?;

        let version: i64 = self
            .conn
            .query_row(
                "SELECT COALESCE((SELECT version FROM schema_version LIMIT 1), 0)",
                [],
                |row| row.get(0),
            )
            .map_err(|err| AppError::Storage(err.to_string()))?;

        if version == 0 {
            self.conn
                .execute("INSERT INTO schema_version(version) VALUES (1)", [])
                .map_err(|err| AppError::Storage(err.to_string()))?;
        }

        Ok(())
    }

    pub fn list_connections(&self) -> Result<Vec<ConnectionProfile>, AppError> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, name, bootstrap_servers, security_protocol, sasl_mechanism,
                        username, credential_ref, ssl_ca_path, ssl_certificate_path,
                        ssl_key_path, client_id, timeout_ms
                 FROM connections ORDER BY updated_at DESC",
            )
            .map_err(|err| AppError::Storage(err.to_string()))?;

        let rows = stmt
            .query_map([], map_connection_row)
            .map_err(|err| AppError::Storage(err.to_string()))?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|err| AppError::Storage(err.to_string()))
    }

    pub fn get_connection(&self, id: &str) -> Result<ConnectionProfile, AppError> {
        self.conn
            .query_row(
                "SELECT id, name, bootstrap_servers, security_protocol, sasl_mechanism,
                        username, credential_ref, ssl_ca_path, ssl_certificate_path,
                        ssl_key_path, client_id, timeout_ms
                 FROM connections WHERE id = ?1",
                params![id],
                map_connection_row,
            )
            .map_err(|err| AppError::Storage(err.to_string()))
    }

    pub fn save_connection(
        &self,
        request: SaveConnectionRequest,
        credential_ref: Option<String>,
    ) -> Result<ConnectionProfile, AppError> {
        let id = request.id.unwrap_or_else(|| Uuid::new_v4().to_string());
        let now = chrono_now();
        let bootstrap_servers = parse_bootstrap_servers(&request.bootstrap_servers)?;
        let bootstrap_raw = bootstrap_servers.join(",");
        let security_protocol = security_protocol_str(&request.security_protocol);
        let sasl_mechanism = request
            .sasl_mechanism
            .as_ref()
            .map(sasl_mechanism_str);
        let client_id = request
            .client_id
            .unwrap_or_else(|| format!("kafkalite-{}", &id[..8.min(id.len())]));
        let timeout_ms = request.request_timeout_ms.unwrap_or(10_000) as i64;

        self.conn
            .execute(
                "INSERT INTO connections (
                    id, name, bootstrap_servers, security_protocol, sasl_mechanism,
                    username, credential_ref, ssl_ca_path, ssl_certificate_path,
                    ssl_key_path, client_id, timeout_ms, created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    bootstrap_servers = excluded.bootstrap_servers,
                    security_protocol = excluded.security_protocol,
                    sasl_mechanism = excluded.sasl_mechanism,
                    username = excluded.username,
                    credential_ref = COALESCE(excluded.credential_ref, connections.credential_ref),
                    ssl_ca_path = excluded.ssl_ca_path,
                    ssl_certificate_path = excluded.ssl_certificate_path,
                    ssl_key_path = excluded.ssl_key_path,
                    client_id = excluded.client_id,
                    timeout_ms = excluded.timeout_ms,
                    updated_at = excluded.updated_at",
                params![
                    id,
                    request.name,
                    bootstrap_raw,
                    security_protocol,
                    sasl_mechanism,
                    request.username,
                    credential_ref,
                    request.ssl_ca_path,
                    request.ssl_certificate_path,
                    request.ssl_key_path,
                    client_id,
                    timeout_ms,
                    now,
                    now,
                ],
            )
            .map_err(|err| AppError::Storage(err.to_string()))?;

        self.get_connection(&id)
    }

    pub fn delete_connection(&self, id: &str) -> Result<(), AppError> {
        self.conn
            .execute("DELETE FROM connections WHERE id = ?1", params![id])
            .map_err(|err| AppError::Storage(err.to_string()))?;
        Ok(())
    }

    pub fn append_audit(
        &self,
        connection_id: Option<&str>,
        operation: &str,
        resource: Option<&str>,
        result: &str,
        detail_json: Option<&str>,
    ) -> Result<(), AppError> {
        self.conn
            .execute(
                "INSERT INTO audit_logs(id, timestamp, connection_id, operation, resource, result, detail_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    Uuid::new_v4().to_string(),
                    chrono_now(),
                    connection_id,
                    operation,
                    resource,
                    result,
                    detail_json,
                ],
            )
            .map_err(|err| AppError::Storage(err.to_string()))?;
        Ok(())
    }
}

fn map_connection_row(row: &rusqlite::Row<'_>) -> Result<ConnectionProfile, rusqlite::Error> {
    let bootstrap_raw: String = row.get(2)?;
    let security_protocol_raw: String = row.get(3)?;
    let sasl_mechanism_raw: Option<String> = row.get(4)?;

    Ok(ConnectionProfile {
        id: row.get(0)?,
        name: row.get(1)?,
        bootstrap_servers: bootstrap_raw
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect(),
        security_protocol: parse_security_protocol(&security_protocol_raw),
        sasl_mechanism: sasl_mechanism_raw.as_deref().map(parse_sasl_mechanism),
        username: row.get(5)?,
        credential_ref: row.get(6)?,
        ssl_ca_path: row.get(7)?,
        ssl_certificate_path: row.get(8)?,
        ssl_key_path: row.get(9)?,
        client_id: row.get(10)?,
        request_timeout_ms: row.get::<_, i64>(11)? as u64,
    })
}

fn chrono_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn security_protocol_str(protocol: &SecurityProtocol) -> &'static str {
    match protocol {
        SecurityProtocol::Plaintext => "PLAINTEXT",
        SecurityProtocol::Ssl => "SSL",
        SecurityProtocol::SaslPlaintext => "SASL_PLAINTEXT",
        SecurityProtocol::SaslSsl => "SASL_SSL",
    }
}

fn parse_security_protocol(raw: &str) -> SecurityProtocol {
    match raw {
        "SSL" => SecurityProtocol::Ssl,
        "SASL_PLAINTEXT" => SecurityProtocol::SaslPlaintext,
        "SASL_SSL" => SecurityProtocol::SaslSsl,
        _ => SecurityProtocol::Plaintext,
    }
}

fn sasl_mechanism_str(mechanism: &crate::kafka::config_builder::SaslMechanism) -> &'static str {
    use crate::kafka::config_builder::SaslMechanism;
    match mechanism {
        SaslMechanism::Plain => "PLAIN",
        SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
        SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
    }
}

fn parse_sasl_mechanism(raw: &str) -> crate::kafka::config_builder::SaslMechanism {
    use crate::kafka::config_builder::SaslMechanism;
    match raw {
        "SCRAM-SHA-256" => SaslMechanism::ScramSha256,
        "SCRAM-SHA-512" => SaslMechanism::ScramSha512,
        _ => SaslMechanism::Plain,
    }
}
