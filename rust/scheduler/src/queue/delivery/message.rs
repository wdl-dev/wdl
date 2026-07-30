use redis::Value;
use redis::streams::StreamId;

use super::super::{QueueMessage, StreamEntry};

fn value_into_string(value: Value) -> String {
    match value {
        Value::BulkString(bytes) => match String::from_utf8(bytes) {
            Ok(value) => value,
            Err(error) => String::from_utf8_lossy(error.as_bytes()).into_owned(),
        },
        Value::SimpleString(value) => value,
        Value::Okay => "OK".to_string(),
        Value::Int(n) => n.to_string(),
        Value::Double(n) => n.to_string(),
        Value::Boolean(v) => v.to_string(),
        Value::Nil => String::new(),
        _ => String::new(),
    }
}

pub(crate) fn stream_id_to_entry(id: StreamId) -> StreamEntry {
    let StreamId { id, map, .. } = id;
    let fields = map
        .into_iter()
        .map(|(key, value)| (key, value_into_string(value)))
        .collect();
    StreamEntry { id, fields }
}

pub(crate) fn entries_to_messages(entries: Vec<StreamEntry>, now: i64) -> Vec<QueueMessage> {
    let now = now.to_string();
    entries
        .into_iter()
        .map(|mut entry| {
            let id = entry
                .fields
                .remove("id")
                .unwrap_or_else(|| entry.id.clone());
            QueueMessage {
                stream_id: entry.id,
                id,
                body_b64: entry.fields.remove("body_b64").unwrap_or_default(),
                content_type: entry
                    .fields
                    .remove("content_type")
                    .unwrap_or_else(|| "json".to_string()),
                attempts: entry
                    .fields
                    .remove("attempts")
                    .unwrap_or_else(|| "0".to_string()),
                first_seen_ms: entry
                    .fields
                    .remove("first_seen_ms")
                    .unwrap_or_else(|| now.clone()),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn str_map(items: &[(&str, &str)]) -> HashMap<String, String> {
        items
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    fn msg(id: &str, stream_id: &str, attempts: &str) -> QueueMessage {
        QueueMessage {
            stream_id: stream_id.to_string(),
            id: id.to_string(),
            body_b64: "aGVsbG8=".to_string(),
            content_type: "text".to_string(),
            attempts: attempts.to_string(),
            first_seen_ms: "1699999999999".to_string(),
        }
    }

    #[test]
    fn entries_to_messages_preserves_stream_fields_and_defaults() {
        let entries = vec![
            StreamEntry {
                id: "1700000000000-0".to_string(),
                fields: str_map(&[
                    ("id", "user-chosen"),
                    ("body_b64", "aGVsbG8="),
                    ("content_type", "text"),
                    ("attempts", "2"),
                    ("first_seen_ms", "1699999999999"),
                ]),
            },
            StreamEntry {
                id: "1700000000001-0".to_string(),
                fields: str_map(&[("body_b64", "")]),
            },
        ];
        let messages = entries_to_messages(entries, 1_234_567_890_000);
        assert_eq!(messages[0].stream_id, "1700000000000-0");
        assert_eq!(messages[0].id, "user-chosen");
        assert_eq!(messages[0].body_b64, "aGVsbG8=");
        assert_eq!(messages[0].content_type, "text");
        assert_eq!(messages[0].attempts, "2");
        assert_eq!(messages[0].first_seen_ms, "1699999999999");
        assert_eq!(messages[1].stream_id, "1700000000001-0");
        assert_eq!(messages[1].id, "1700000000001-0");
        assert_eq!(messages[1].content_type, "json");
        assert_eq!(messages[1].attempts, "0");
        assert_eq!(messages[1].first_seen_ms, "1234567890000");
    }

    #[test]
    fn queue_message_serialization_drops_stream_id() {
        assert_eq!(
            serde_json::to_value(msg("m1", "1700000000000-0", "1")).unwrap(),
            serde_json::json!({
                "id": "m1",
                "body_b64": "aGVsbG8=",
                "content_type": "text",
                "attempts": "1",
                "first_seen_ms": "1699999999999",
            })
        );
    }
}
