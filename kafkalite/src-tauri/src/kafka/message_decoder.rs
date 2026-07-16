use base64::{engine::general_purpose::STANDARD, Engine};
use rdkafka::message::{BorrowedMessage, Headers, Message};
use serde::Serialize;

const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageDto {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
    pub key: Option<String>,
    pub value_preview: String,
    pub value_encoding: String,
    pub value_size: usize,
    pub truncated: bool,
    pub headers: Vec<HeaderDto>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HeaderDto {
    pub key: String,
    pub value: Option<String>,
}

pub fn decode_message(message: &BorrowedMessage<'_>, max_preview_bytes: usize) -> MessageDto {
    let key = message
        .key()
        .map(|bytes| decode_bytes_preview(bytes, max_preview_bytes).0);

    let payload = message.payload().unwrap_or_default();
    let (value_preview, value_encoding, truncated) =
        decode_bytes_preview(payload, max_preview_bytes);

    let headers = match message.headers() {
        Some(headers) => (0..headers.count())
            .filter_map(|index| {
                let header = headers.get(index);
                Some(HeaderDto {
                    key: header.key.to_string(),
                    value: header.value.map(|bytes| {
                        decode_bytes_preview(bytes, max_preview_bytes.min(256)).0
                    }),
                })
            })
            .collect(),
        None => Vec::new(),
    };

    let timestamp = match message.timestamp() {
        rdkafka::Timestamp::NotAvailable => None,
        rdkafka::Timestamp::CreateTime(ts) | rdkafka::Timestamp::LogAppendTime(ts) => Some(ts),
    };

    MessageDto {
        topic: message.topic().to_string(),
        partition: message.partition(),
        offset: message.offset(),
        timestamp,
        key,
        value_preview,
        value_encoding,
        value_size: payload.len(),
        truncated,
        headers,
    }
}

fn decode_bytes_preview(bytes: &[u8], max_preview_bytes: usize) -> (String, String, bool) {
    let truncated = bytes.len() > max_preview_bytes;
    let slice = if truncated {
        &bytes[..max_preview_bytes]
    } else {
        bytes
    };

    if slice.is_empty() {
        return (String::new(), "utf8".into(), truncated);
    }

    if let Ok(text) = std::str::from_utf8(slice) {
        if text
            .chars()
            .all(|ch| !ch.is_control() || matches!(ch, '\n' | '\r' | '\t'))
        {
            return (text.to_string(), "utf8".into(), truncated);
        }
    }

    // Prefer hex for short binary payloads, base64 for larger ones.
    if slice.len() <= 64 {
        return (to_hex(slice), "hex".into(), truncated);
    }
    (STANDARD.encode(slice), "base64".into(), truncated)
}

pub fn to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX_CHARS[(byte >> 4) as usize] as char);
        out.push(HEX_CHARS[(byte & 0xf) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_utf8_text() {
        let (preview, encoding, truncated) = decode_bytes_preview(b"hello", 512);
        assert_eq!(preview, "hello");
        assert_eq!(encoding, "utf8");
        assert!(!truncated);
    }

    #[test]
    fn truncates_large_payload() {
        let bytes = vec![b'a'; 20];
        let (preview, _, truncated) = decode_bytes_preview(&bytes, 10);
        assert_eq!(preview.len(), 10);
        assert!(truncated);
    }
}
