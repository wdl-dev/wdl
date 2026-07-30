use std::collections::{HashMap, HashSet};

use serde_json::Value as JsonValue;

use crate::RuntimeResponse;

use super::super::{OutcomePlan, QueueMessage};

pub(crate) fn decide_outcome(res: &RuntimeResponse, messages: Vec<QueueMessage>) -> OutcomePlan {
    if let Some((kind, reason)) = terminal_failure(res) {
        return OutcomePlan::TerminalAll {
            kind,
            reason,
            messages,
        };
    }

    if res.error.is_some() || res.json.is_none() {
        return OutcomePlan::RetryAll {
            kind: "transport_error",
            reason: res
                .error
                .clone()
                .or_else(|| res.text.clone())
                .unwrap_or_else(|| "no response body".to_string()),
            messages,
        };
    }

    let Some(json) = res.json.as_ref() else {
        unreachable!("json checked above");
    };
    if json.get("outcome").and_then(JsonValue::as_str) == Some("error") {
        return OutcomePlan::RetryAll {
            kind: "handler_error",
            reason: "outer_outcome_error".to_string(),
            messages,
        };
    }
    // Runtime always wraps the handler return in `{outcome, result, ...}`
    // (runtime/index.js#/_queued). Anything outside that envelope is a
    // protocol violation — fall through to retry-all rather than guess.
    let Some(result) = json.get("result") else {
        return OutcomePlan::RetryAll {
            kind: "handler_error",
            reason: "missing_result_envelope".to_string(),
            messages,
        };
    };
    if result.get("outcome").and_then(JsonValue::as_str) == Some("exception") {
        return OutcomePlan::RetryAll {
            kind: "handler_error",
            reason: "inner_outcome_exception".to_string(),
            messages,
        };
    }

    let explicit_acks = result
        .get("explicitAcks")
        .and_then(JsonValue::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(JsonValue::as_str)
                .map(str::to_string)
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();
    let mut retry_map = HashMap::new();
    if let Some(items) = result.get("retryMessages").and_then(JsonValue::as_array) {
        for item in items {
            if let Some(id) = item.get("msgId").and_then(JsonValue::as_str) {
                let delay = item.get("delaySeconds").and_then(JsonValue::as_i64);
                retry_map.insert(id.to_string(), delay);
            }
        }
    }
    // result.ackAll has the same effect as implicit-ack (handler returns nothing)
    // — both fall through the bottom branch. No need to read it separately.
    let batch_retry = result
        .get("retryBatch")
        .and_then(|v| v.get("retry"))
        .and_then(JsonValue::as_bool)
        == Some(true);
    let batch_retry_delay = result
        .get("retryBatch")
        .and_then(|v| v.get("delaySeconds"))
        .and_then(JsonValue::as_i64);

    // Precedence per CF QueueResponse: per-message decisions (explicitAcks,
    // retryMessages) override batch-level ones (retryBatch, ackAll);
    // implicit-ack is the final fallthrough so handlers that just `return`
    // don't duplicate. Reordering these branches changes semantics.
    let mut to_ack = Vec::new();
    let mut to_retry = Vec::new();
    for msg in messages {
        if explicit_acks.contains(&msg.id) {
            to_ack.push(msg);
        } else if let Some(delay) = retry_map.get(&msg.id) {
            to_retry.push((msg, *delay));
        } else if batch_retry {
            to_retry.push((msg, batch_retry_delay));
        } else {
            to_ack.push(msg);
        }
    }
    OutcomePlan::Normal { to_ack, to_retry }
}

fn terminal_failure(res: &RuntimeResponse) -> Option<(&'static str, String)> {
    let status = res.status?;
    if status == 413 {
        return Some(("permanent_http_error", runtime_failure_reason(res, status)));
    }
    if status == 400 {
        let error = res
            .json
            .as_ref()
            .and_then(|json| json.get("error"))
            .and_then(JsonValue::as_str);
        if matches!(
            error,
            Some("queue_message_decode_failed" | "invalid_queue_body")
        ) {
            return Some(("permanent_queue_error", runtime_failure_reason(res, status)));
        }
    }
    None
}

fn runtime_failure_reason(res: &RuntimeResponse, status: u16) -> String {
    res.json
        .as_ref()
        .and_then(|json| json.get("error"))
        .and_then(JsonValue::as_str)
        .map(str::to_string)
        .or_else(|| res.text.clone())
        .unwrap_or_else(|| format!("http_status_{status}"))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

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
    fn decide_outcome_retries_all_on_transport_and_handler_errors() {
        let messages = vec![msg("a", "1-0", "0"), msg("b", "2-0", "0")];
        let transport = decide_outcome(
            &RuntimeResponse {
                status: None,
                json: None,
                text: None,
                error: Some("ECONNREFUSED".to_string()),
            },
            messages.clone(),
        );
        match transport {
            OutcomePlan::RetryAll {
                kind,
                reason,
                messages,
            } => {
                assert_eq!(kind, "transport_error");
                assert_eq!(reason, "ECONNREFUSED");
                assert_eq!(messages.len(), 2);
            }
            _ => panic!("expected transport retry-all"),
        }

        let outer = decide_outcome(
            &RuntimeResponse {
                status: Some(200),
                json: Some(json!({ "outcome": "error" })),
                text: None,
                error: None,
            },
            messages.clone(),
        );
        assert!(matches!(
            outer,
            OutcomePlan::RetryAll {
                kind: "handler_error",
                reason,
                ..
            } if reason == "outer_outcome_error"
        ));

        let inner = decide_outcome(
            &RuntimeResponse {
                status: Some(200),
                json: Some(json!({ "outcome": "ok", "result": { "outcome": "exception" } })),
                text: None,
                error: None,
            },
            messages.clone(),
        );
        assert!(matches!(
            inner,
            OutcomePlan::RetryAll {
                kind: "handler_error",
                reason,
                ..
            } if reason == "inner_outcome_exception"
        ));
    }

    #[test]
    fn decide_outcome_dlqs_permanent_platform_failures_without_retry_budget_burn() {
        let messages = vec![msg("a", "1-0", "0"), msg("b", "2-0", "0")];

        for response in [
            RuntimeResponse {
                status: Some(400),
                json: Some(json!({ "error": "queue_message_decode_failed" })),
                text: None,
                error: None,
            },
            RuntimeResponse {
                status: Some(413),
                json: None,
                text: Some("payload too large".to_string()),
                error: None,
            },
        ] {
            match decide_outcome(&response, messages.clone()) {
                OutcomePlan::TerminalAll {
                    messages: planned, ..
                } => assert_eq!(planned.len(), 2),
                _ => panic!("expected terminal DLQ outcome"),
            }
        }
    }

    #[test]
    fn decide_outcome_retries_auth_and_unknown_4xx_errors() {
        let messages = vec![msg("a", "1-0", "0")];
        for response in [
            RuntimeResponse {
                status: Some(401),
                json: None,
                text: Some("unauthorized".to_string()),
                error: None,
            },
            RuntimeResponse {
                status: Some(403),
                json: None,
                text: Some("forbidden".to_string()),
                error: None,
            },
            RuntimeResponse {
                status: Some(409),
                json: None,
                text: Some("application conflict".to_string()),
                error: None,
            },
        ] {
            match decide_outcome(&response, messages.clone()) {
                OutcomePlan::RetryAll { messages, .. } => assert_eq!(messages.len(), 1),
                _ => panic!("expected auth and unknown 4xx to keep existing retry behavior"),
            }
        }
    }

    #[test]
    fn decide_outcome_maps_ack_retry_precedence_like_workerd_queue_response() {
        let messages = vec![msg("a", "1-0", "0"), msg("b", "2-0", "0")];

        match decide_outcome(
            &RuntimeResponse {
                status: Some(200),
                json: Some(json!({ "result": {} })),
                text: None,
                error: None,
            },
            messages.clone(),
        ) {
            OutcomePlan::Normal { to_ack, to_retry } => {
                assert_eq!(
                    to_ack.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
                    ["a", "b"]
                );
                assert!(to_retry.is_empty());
            }
            _ => panic!("expected normal outcome"),
        }

        match decide_outcome(
            &RuntimeResponse {
                status: Some(200),
                json: Some(
                    json!({ "result": { "retryBatch": { "retry": true, "delaySeconds": 30 } } }),
                ),
                text: None,
                error: None,
            },
            messages.clone(),
        ) {
            OutcomePlan::Normal { to_ack, to_retry } => {
                assert!(to_ack.is_empty());
                assert_eq!(to_retry.len(), 2);
                assert!(to_retry.iter().all(|(_, delay)| *delay == Some(30)));
            }
            _ => panic!("expected normal outcome"),
        }

        match decide_outcome(
            &RuntimeResponse {
                status: Some(200),
                json: Some(json!({
                    "result": {
                        "explicitAcks": ["a"],
                        "retryBatch": { "retry": true, "delaySeconds": 10 }
                    }
                })),
                text: None,
                error: None,
            },
            messages.clone(),
        ) {
            OutcomePlan::Normal { to_ack, to_retry } => {
                assert_eq!(
                    to_ack.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
                    ["a"]
                );
                assert_eq!(to_retry.len(), 1);
                assert_eq!(to_retry[0].0.id, "b");
                assert_eq!(to_retry[0].1, Some(10));
            }
            _ => panic!("expected normal outcome"),
        }

        match decide_outcome(
            &RuntimeResponse {
                status: Some(200),
                json: Some(json!({
                    "result": {
                        "ackAll": true,
                        "retryMessages": [{ "msgId": "a", "delaySeconds": 5 }]
                    }
                })),
                text: None,
                error: None,
            },
            messages.clone(),
        ) {
            OutcomePlan::Normal { to_ack, to_retry } => {
                assert_eq!(
                    to_retry
                        .iter()
                        .map(|(m, _)| m.id.as_str())
                        .collect::<Vec<_>>(),
                    ["a"]
                );
                assert_eq!(to_retry[0].1, Some(5));
                assert_eq!(
                    to_ack.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
                    ["b"]
                );
            }
            _ => panic!("expected normal outcome"),
        }

        match decide_outcome(
            &RuntimeResponse {
                status: Some(200),
                json: Some(json!({ "result": { "retryMessages": [{ "msgId": "a" }] } })),
                text: None,
                error: None,
            },
            messages.clone(),
        ) {
            OutcomePlan::Normal { to_retry, .. } => {
                assert_eq!(to_retry.len(), 1);
                assert_eq!(to_retry[0].0.id, "a");
                assert_eq!(to_retry[0].1, None);
            }
            _ => panic!("expected normal outcome"),
        }
    }

    #[test]
    fn decide_outcome_retries_when_result_envelope_missing() {
        let messages = vec![msg("a", "1-0", "0")];
        match decide_outcome(
            &RuntimeResponse {
                status: Some(200),
                json: Some(json!({ "ackAll": true })),
                text: None,
                error: None,
            },
            messages,
        ) {
            OutcomePlan::RetryAll {
                kind: "handler_error",
                reason,
                ..
            } => {
                assert_eq!(reason, "missing_result_envelope");
            }
            _ => panic!("expected retry-all on missing result envelope"),
        }
    }

    #[test]
    fn decide_outcome_treats_unknown_msg_ids_in_explicit_acks_as_implicit_ack() {
        let messages = vec![msg("a", "1-0", "0")];
        match decide_outcome(
            &RuntimeResponse {
                status: Some(200),
                json: Some(json!({ "result": { "explicitAcks": ["ghost"] } })),
                text: None,
                error: None,
            },
            messages,
        ) {
            OutcomePlan::Normal { to_ack, to_retry } => {
                assert_eq!(to_ack[0].id, "a");
                assert!(to_retry.is_empty());
            }
            _ => panic!("expected normal outcome"),
        }
    }
}
