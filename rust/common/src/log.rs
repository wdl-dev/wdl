use chrono::{SecondsFormat, Utc};
use serde_json::Value as JsonValue;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[repr(u8)]
pub enum LogLevel {
    Debug = 10,
    Info = 20,
    Warn = 30,
    Error = 40,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LogStream {
    Stdout,
    Stderr,
}

fn should_emit(level: LogLevel, min_level: LogLevel) -> bool {
    level >= min_level
}

fn stream_for_level(level: LogLevel) -> LogStream {
    if level == LogLevel::Error {
        LogStream::Stderr
    } else {
        LogStream::Stdout
    }
}

impl LogLevel {
    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "debug" => Some(Self::Debug),
            "info" => Some(Self::Info),
            "warn" => Some(Self::Warn),
            "error" => Some(Self::Error),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

pub fn log_level_from_env() -> LogLevel {
    std::env::var("LOG_LEVEL")
        .ok()
        .as_deref()
        .and_then(LogLevel::parse)
        .unwrap_or(LogLevel::Info)
}

pub fn emit_log_line(
    service: &str,
    level: LogLevel,
    min_level: LogLevel,
    event: &str,
    fields: JsonValue,
) {
    if !should_emit(level, min_level) {
        return;
    }
    let line = log_payload_line(service, level, event, fields);
    match stream_for_level(level) {
        LogStream::Stdout => println!("{line}"),
        LogStream::Stderr => eprintln!("{line}"),
    }
}

fn log_payload_line(service: &str, level: LogLevel, event: &str, fields: JsonValue) -> String {
    let payload = log_payload(service, level, event, fields);
    let mut line = String::from("{");
    let mut first = true;
    for key in ["ts", "service", "level", "event"] {
        if let Some(value) = payload.get(key) {
            push_json_entry(&mut line, &mut first, key, value);
        }
    }
    for (key, value) in &payload {
        if !matches!(key.as_str(), "ts" | "service" | "level" | "event") {
            push_json_entry(&mut line, &mut first, key, value);
        }
    }
    line.push('}');
    line
}

fn push_json_entry(line: &mut String, first: &mut bool, key: &str, value: &JsonValue) {
    if *first {
        *first = false;
    } else {
        line.push(',');
    }
    line.push_str(&serde_json::to_string(key).expect("serializing log key should not fail"));
    line.push(':');
    line.push_str(&serde_json::to_string(value).expect("serializing log value should not fail"));
}

fn log_payload(
    service: &str,
    level: LogLevel,
    event: &str,
    fields: JsonValue,
) -> serde_json::Map<String, JsonValue> {
    let mut payload = serde_json::Map::new();
    payload.insert("ts".to_string(), JsonValue::String(now_log_ts()));
    payload.insert(
        "service".to_string(),
        JsonValue::String(service.to_string()),
    );
    payload.insert(
        "level".to_string(),
        JsonValue::String(level.as_str().to_string()),
    );
    payload.insert("event".to_string(), JsonValue::String(event.to_string()));
    if let JsonValue::Object(extra) = fields {
        for (key, value) in extra {
            if !value.is_null() {
                payload.insert(key, value);
            }
        }
    }
    payload
}

fn now_log_ts() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::observability_contract;
    use serde_json::json;

    fn timestamp_shape(value: &str) -> String {
        value
            .chars()
            .map(|ch| if ch.is_ascii_digit() { '0' } else { ch })
            .collect()
    }

    #[test]
    fn level_parse_is_case_insensitive() {
        assert_eq!(LogLevel::parse("debug"), Some(LogLevel::Debug));
        assert_eq!(LogLevel::parse("INFO"), Some(LogLevel::Info));
        assert_eq!(LogLevel::parse("Warn"), Some(LogLevel::Warn));
        assert_eq!(LogLevel::parse("error"), Some(LogLevel::Error));
        assert_eq!(LogLevel::parse("trace"), None);
    }

    #[test]
    fn log_levels_gating_and_streams_match_cross_language_fixture() {
        let contract = observability_contract();
        let levels = contract["logEnvelope"]["levels"]
            .as_array()
            .expect("logEnvelope.levels is an array");

        for entry in levels {
            let name = entry["name"].as_str().expect("log level name is a string");
            let priority = entry["priority"]
                .as_u64()
                .expect("log level priority is a number");
            let stream = entry["stream"]
                .as_str()
                .expect("log level stream is a string");
            let level = LogLevel::parse(name).expect("fixture log level is supported");
            let actual_stream = match stream_for_level(level) {
                LogStream::Stdout => "stdout",
                LogStream::Stderr => "stderr",
            };
            assert_eq!(u64::from(level as u8), priority, "{name} priority");
            assert_eq!(actual_stream, stream, "{name} stream");
        }

        for min_entry in levels {
            let min_name = min_entry["name"]
                .as_str()
                .expect("minimum log level name is a string");
            let min_priority = min_entry["priority"]
                .as_u64()
                .expect("minimum log level priority is a number");
            let min_level = LogLevel::parse(min_name).expect("minimum log level is supported");
            for entry in levels {
                let name = entry["name"].as_str().expect("log level name is a string");
                let priority = entry["priority"]
                    .as_u64()
                    .expect("log level priority is a number");
                let level = LogLevel::parse(name).expect("fixture log level is supported");
                assert_eq!(
                    should_emit(level, min_level),
                    priority >= min_priority,
                    "{name} at minimum {min_name}"
                );
            }
        }
    }

    #[test]
    fn log_payload_keeps_stable_fields_and_skips_null_extras() {
        let payload = log_payload(
            "scheduler",
            LogLevel::Warn,
            "cron_tick",
            json!({
                "namespace": "demo",
                "ignored": null
            }),
        );

        assert!(payload.get("ts").and_then(JsonValue::as_str).is_some());
        assert_eq!(payload.get("service"), Some(&json!("scheduler")));
        assert_eq!(payload.get("level"), Some(&json!("warn")));
        assert_eq!(payload.get("event"), Some(&json!("cron_tick")));
        assert_eq!(payload.get("namespace"), Some(&json!("demo")));
        assert!(!payload.contains_key("ignored"));
    }

    #[test]
    fn log_payload_line_matches_js_envelope_order() {
        let contract = observability_contract();
        let ordered_keys = contract["logEnvelope"]["orderedKeys"]
            .as_array()
            .expect("logEnvelope.orderedKeys is an array");
        let line = log_payload_line(
            "scheduler",
            LogLevel::Info,
            "started",
            json!({ "port": 7070 }),
        );

        let mut position = 0;
        for (index, key) in ordered_keys.iter().enumerate() {
            let key = key.as_str().expect("ordered log key is a string");
            let marker = format!("{}\"{key}\":", if index == 0 { "{" } else { "," });
            let offset = line[position..]
                .find(&marker)
                .unwrap_or_else(|| panic!("missing ordered marker {marker:?} in {line}"));
            position += offset + marker.len();
        }
    }

    #[test]
    fn log_timestamp_matches_js_iso_millisecond_shape() {
        let contract = observability_contract();
        let expected = contract["logEnvelope"]["timestampShape"]
            .as_str()
            .expect("logEnvelope.timestampShape is a string");
        let ts = now_log_ts();
        assert_eq!(timestamp_shape(&ts), expected);
    }
}
