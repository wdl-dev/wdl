//! Queue Redis key helpers shared by producer and scheduler services.

use crate::identity::{is_valid_route_ns, is_valid_runtime_load_ns};

pub const QUEUE_CONSUMER_INDEX_KEY: &str = "queue:index:consumers";
pub const QUEUE_STREAM_INDEX_KEY: &str = "queue:index:streams";
pub const QUEUE_DELAYED_INDEX_KEY: &str = "queue:index:delayed";
pub const QUEUE_CONSUMER_SCAN_PATTERN: &str = "queue-consumer:*:*";
pub const QUEUE_STREAM_SCAN_PATTERN: &str = "queue:*:*:s";
pub const QUEUE_DELAYED_SCAN_PATTERN: &str = "queue-delayed:*:*";
pub const QUEUE_DELAYED_WAKE_STREAM: &str = "queue-delayed-wake";
pub const QUEUE_DELAYED_WAKE_KEY_FIELD: &str = "delayed_key";
pub const QUEUE_DELAYED_WAKE_VISIBLE_AT_FIELD: &str = "visible_at";

pub fn is_valid_queue_name(queue: &str) -> bool {
    let bytes = queue.as_bytes();
    if bytes.is_empty() || bytes.len() > 63 {
        return false;
    }
    if !bytes[0].is_ascii_lowercase() && !bytes[0].is_ascii_digit() {
        return false;
    }
    bytes
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
}

pub fn queue_stream_key(ns: &str, queue: &str) -> String {
    format!("queue:{ns}:{queue}:s")
}

pub fn queue_delayed_key(ns: &str, queue: &str) -> String {
    format!("queue-delayed:{ns}:{queue}")
}

pub fn queue_dlq_key(ns: &str, queue: &str) -> String {
    format!("queue:{ns}:{queue}:dlq")
}

pub fn queue_orphaned_key(ns: &str, queue: &str) -> String {
    format!("queue-orphaned:{ns}:{queue}")
}

pub fn queue_consumer_key(ns: &str, queue: &str) -> String {
    format!("queue-consumer:{ns}:{queue}")
}

pub fn parse_stream_key(key: &str) -> Option<(String, String)> {
    let rest = key.strip_prefix("queue:")?;
    let rest = rest.strip_suffix(":s")?;
    split_queue_key_rest(rest, is_valid_runtime_load_ns)
}

pub fn parse_delayed_key(key: &str) -> Option<(String, String)> {
    let rest = key.strip_prefix("queue-delayed:")?;
    split_queue_key_rest(rest, is_valid_runtime_load_ns)
}

pub fn parse_consumer_key(key: &str) -> Option<(String, String)> {
    let rest = key.strip_prefix("queue-consumer:")?;
    split_queue_key_rest(rest, is_valid_route_ns)
}

fn split_queue_key_rest(rest: &str, is_valid_ns: fn(&str) -> bool) -> Option<(String, String)> {
    let (ns, queue) = rest.split_once(':')?;
    if !is_valid_ns(ns) || !is_valid_queue_name(queue) {
        return None;
    }
    Some((ns.to_string(), queue.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::identity_cases;
    use serde_json::{Value as JsonValue, json};

    fn assert_queue_key_parse_contract(kind: &str, parse: fn(&str) -> Option<(String, String)>) {
        let fixture: JsonValue =
            serde_json::from_str(include_str!("../../../tests/fixtures/queue-key-parse.json"))
                .expect("queue key parse fixture parses");
        for entry in fixture[kind]
            .as_array()
            .expect("queue key parse fixture field is an array")
        {
            let key = entry["key"]
                .as_str()
                .expect("queue key parse fixture key is a string");
            let actual = parse(key)
                .map(|(ns, queue)| json!({ "ns": ns, "queue": queue }))
                .unwrap_or(JsonValue::Null);
            assert_eq!(actual, entry["parsed"], "{kind}:{key:?}");
        }
    }

    #[test]
    fn queue_name_grammar_matches_cross_language_fixture() {
        for (value, valid) in identity_cases("queueNames") {
            assert_eq!(is_valid_queue_name(&value), valid, "queueNames:{value:?}");
        }
    }

    #[test]
    fn queue_key_builders_compose_redis_key_shapes() {
        assert_eq!(QUEUE_CONSUMER_INDEX_KEY, "queue:index:consumers");
        assert_eq!(QUEUE_STREAM_INDEX_KEY, "queue:index:streams");
        assert_eq!(QUEUE_DELAYED_INDEX_KEY, "queue:index:delayed");
        assert_eq!(QUEUE_CONSUMER_SCAN_PATTERN, "queue-consumer:*:*");
        assert_eq!(QUEUE_STREAM_SCAN_PATTERN, "queue:*:*:s");
        assert_eq!(QUEUE_DELAYED_SCAN_PATTERN, "queue-delayed:*:*");
        assert_eq!(QUEUE_DELAYED_WAKE_STREAM, "queue-delayed-wake");
        assert_eq!(QUEUE_DELAYED_WAKE_KEY_FIELD, "delayed_key");
        assert_eq!(QUEUE_DELAYED_WAKE_VISIBLE_AT_FIELD, "visible_at");
        assert_eq!(queue_stream_key("demo", "jobs"), "queue:demo:jobs:s");
        assert_eq!(queue_delayed_key("demo", "jobs"), "queue-delayed:demo:jobs");
        assert_eq!(queue_dlq_key("demo", "jobs"), "queue:demo:jobs:dlq");
        assert_eq!(
            queue_orphaned_key("demo", "jobs"),
            "queue-orphaned:demo:jobs"
        );
        assert_eq!(
            queue_consumer_key("demo", "jobs"),
            "queue-consumer:demo:jobs"
        );
        assert_eq!(queue_stream_key("t", "my-queue-1"), "queue:t:my-queue-1:s");
    }

    #[test]
    fn queue_name_grammar_matches_control_contract() {
        assert!(is_valid_queue_name("jobs"));
        assert!(is_valid_queue_name("jobs-1"));
        assert!(is_valid_queue_name(&"a".repeat(63)));
        for queue in ["", "Jobs", "-jobs", "jobs_", "jobs:tail"] {
            assert!(!is_valid_queue_name(queue), "{queue}");
        }
        assert!(!is_valid_queue_name(&"a".repeat(64)));
    }

    #[test]
    fn queue_key_parsers_match_cross_language_fixture() {
        assert_queue_key_parse_contract("stream", parse_stream_key);
        assert_queue_key_parse_contract("delayed", parse_delayed_key);
        assert_queue_key_parse_contract("consumer", parse_consumer_key);
    }
}
