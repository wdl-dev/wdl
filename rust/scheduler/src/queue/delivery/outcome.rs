use std::collections::{HashMap, HashSet};

use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::RuntimeResponse;

use super::super::{OutcomePlan, QueueMessage};

struct QueueDecision {
    explicit_acks: HashSet<String>,
    retry_map: HashMap<String, Option<i64>>,
    batch_retry: bool,
    batch_retry_delay: Option<i64>,
}

fn parse_delay_seconds(value: Option<&JsonValue>) -> Result<Option<i64>, ()> {
    let Some(value) = value else {
        return Ok(None);
    };
    let Some(delay) = value.as_i64() else {
        return Err(());
    };
    if i32::try_from(delay).is_err() {
        return Err(());
    }
    Ok(Some(delay))
}

fn parse_queue_decision(
    result: &JsonMap<String, JsonValue>,
    message_ids: &HashSet<&str>,
) -> Result<QueueDecision, &'static str> {
    if !result.get("ackAll").is_some_and(JsonValue::is_boolean) {
        return Err("invalid_ack_all");
    }

    let explicit_ack_items = result
        .get("explicitAcks")
        .and_then(JsonValue::as_array)
        .ok_or("invalid_explicit_acks")?;
    let mut explicit_acks = HashSet::with_capacity(explicit_ack_items.len());
    for item in explicit_ack_items {
        let id = item.as_str().ok_or("invalid_explicit_ack")?;
        if !message_ids.contains(id) {
            return Err("unknown_explicit_ack_id");
        }
        explicit_acks.insert(id.to_string());
    }

    let retry_items = result
        .get("retryMessages")
        .and_then(JsonValue::as_array)
        .ok_or("invalid_retry_messages")?;
    let mut retry_map = HashMap::with_capacity(retry_items.len());
    for item in retry_items {
        let item = item.as_object().ok_or("invalid_retry_message")?;
        let id = item
            .get("msgId")
            .and_then(JsonValue::as_str)
            .ok_or("invalid_retry_message_id")?;
        if !message_ids.contains(id) {
            return Err("unknown_retry_message_id");
        }
        let delay = parse_delay_seconds(item.get("delaySeconds"))
            .map_err(|()| "invalid_retry_message_delay")?;
        retry_map.insert(id.to_string(), delay);
    }
    if retry_map
        .keys()
        .any(|message_id| explicit_acks.contains(message_id))
    {
        return Err("conflicting_message_decision");
    }

    let batch = result
        .get("retryBatch")
        .and_then(JsonValue::as_object)
        .ok_or("invalid_retry_batch")?;
    let batch_retry = batch
        .get("retry")
        .and_then(JsonValue::as_bool)
        .ok_or("invalid_retry_batch")?;
    let batch_retry_delay =
        parse_delay_seconds(batch.get("delaySeconds")).map_err(|()| "invalid_retry_batch_delay")?;

    Ok(QueueDecision {
        explicit_acks,
        retry_map,
        batch_retry,
        batch_retry_delay,
    })
}

pub(crate) fn decide_outcome(res: &RuntimeResponse, messages: Vec<QueueMessage>) -> OutcomePlan {
    if res.error.is_some() {
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

    if let Some((kind, reason)) = terminal_failure(res) {
        return OutcomePlan::TerminalAll {
            kind,
            reason,
            messages,
        };
    }

    if res.json.is_none() || res.status.is_none() {
        return OutcomePlan::RetryAll {
            kind: "transport_error",
            reason: res
                .text
                .clone()
                .unwrap_or_else(|| "no response body".to_string()),
            messages,
        };
    }

    let Some(json) = res.json.as_ref() else {
        unreachable!("json checked above");
    };
    let status = res.status.expect("status checked above");
    if !(200..300).contains(&status) {
        return OutcomePlan::RetryAll {
            kind: "handler_error",
            reason: format!("http_status_{status}"),
            messages,
        };
    }
    match json.get("outcome").and_then(JsonValue::as_str) {
        Some("ok") => {}
        Some("error") => {
            return OutcomePlan::RetryAll {
                kind: "handler_error",
                reason: "outer_outcome_error".to_string(),
                messages,
            };
        }
        _ => {
            return OutcomePlan::RetryAll {
                kind: "handler_error",
                reason: "invalid_outer_outcome".to_string(),
                messages,
            };
        }
    }
    // Runtime always wraps the handler return in `{outcome, result, ...}`
    // (runtime/index.js#/_queued). Anything outside that envelope is a
    // protocol violation — fall through to retry-all rather than guess.
    let Some(result_value) = json.get("result") else {
        return OutcomePlan::RetryAll {
            kind: "handler_error",
            reason: "missing_result_envelope".to_string(),
            messages,
        };
    };
    let Some(result) = result_value.as_object() else {
        return OutcomePlan::RetryAll {
            kind: "handler_error",
            reason: "invalid_result_envelope".to_string(),
            messages,
        };
    };
    match result.get("outcome") {
        None => {
            return OutcomePlan::RetryAll {
                kind: "handler_error",
                reason: "missing_inner_outcome".to_string(),
                messages,
            };
        }
        Some(outcome) if outcome.as_str() == Some("ok") => {}
        Some(outcome) if outcome.as_str() == Some("exception") => {
            return OutcomePlan::RetryAll {
                kind: "handler_error",
                reason: "inner_outcome_exception".to_string(),
                messages,
            };
        }
        Some(_) => {
            return OutcomePlan::RetryAll {
                kind: "handler_error",
                reason: "invalid_inner_outcome".to_string(),
                messages,
            };
        }
    }

    let message_ids = messages
        .iter()
        .map(|message| message.id.as_str())
        .collect::<HashSet<_>>();
    let QueueDecision {
        explicit_acks,
        retry_map,
        batch_retry,
        batch_retry_delay,
    } = match parse_queue_decision(result, &message_ids) {
        Ok(decision) => decision,
        Err(reason) => {
            return OutcomePlan::RetryAll {
                kind: "handler_error",
                reason: reason.to_string(),
                messages,
            };
        }
    };
    // result.ackAll has the same effect as implicit-ack (handler returns nothing)
    // — both fall through the bottom branch after its type is validated.

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

    fn complete_queue_result() -> JsonValue {
        json!({
            "outcome": "ok",
            "ackAll": false,
            "retryBatch": { "retry": false },
            "explicitAcks": [],
            "retryMessages": []
        })
    }

    fn queue_response_with(field: &str, value: JsonValue) -> RuntimeResponse {
        let mut result = complete_queue_result();
        result
            .as_object_mut()
            .expect("queue result must be an object")
            .insert(field.to_string(), value);
        RuntimeResponse {
            status: Some(200),
            json: Some(json!({ "outcome": "ok", "result": result })),
            text: None,
            error: None,
        }
    }

    fn queue_response_without(field: &str) -> RuntimeResponse {
        let mut result = complete_queue_result();
        result
            .as_object_mut()
            .expect("queue result must be an object")
            .remove(field);
        RuntimeResponse {
            status: Some(200),
            json: Some(json!({ "outcome": "ok", "result": result })),
            text: None,
            error: None,
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

        assert!(matches!(
            decide_outcome(
                &RuntimeResponse {
                    status: Some(413),
                    json: None,
                    text: None,
                    error: Some("runtime response body exceeds limit".to_string()),
                },
                messages.clone(),
            ),
            OutcomePlan::RetryAll {
                kind: "transport_error",
                ..
            }
        ));

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
                json: Some(json!({
                    "outcome": "ok",
                    "result": {
                        "outcome": "exception",
                        "ackAll": false,
                        "retryBatch": { "retry": false },
                        "explicitAcks": [],
                        "retryMessages": []
                    }
                })),
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
                json: Some(json!({
                    "outcome": "ok",
                    "result": {
                        "outcome": "ok",
                        "ackAll": false,
                        "retryBatch": { "retry": false },
                        "explicitAcks": [],
                        "retryMessages": []
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
                    ["a", "b"]
                );
                assert!(to_retry.is_empty());
            }
            _ => panic!("expected normal outcome"),
        }

        match decide_outcome(
            &RuntimeResponse {
                status: Some(200),
                json: Some(json!({
                    "outcome": "ok",
                    "result": {
                        "outcome": "ok",
                        "ackAll": false,
                        "retryBatch": { "retry": true, "delaySeconds": 30 },
                        "explicitAcks": [],
                        "retryMessages": []
                    }
                })),
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
                    "outcome": "ok",
                    "result": {
                        "outcome": "ok",
                        "ackAll": false,
                        "explicitAcks": ["a"],
                        "retryBatch": { "retry": true, "delaySeconds": 10 },
                        "retryMessages": []
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
                    "outcome": "ok",
                    "result": {
                        "outcome": "ok",
                        "ackAll": true,
                        "explicitAcks": [],
                        "retryMessages": [{ "msgId": "a", "delaySeconds": 5 }],
                        "retryBatch": { "retry": false }
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
                json: Some(json!({
                    "outcome": "ok",
                    "result": {
                        "outcome": "ok",
                        "ackAll": false,
                        "retryBatch": { "retry": false },
                        "explicitAcks": [],
                        "retryMessages": [{ "msgId": "a" }]
                    }
                })),
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
                json: Some(json!({ "outcome": "ok", "ackAll": true })),
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
    fn decide_outcome_retries_malformed_runtime_envelopes() {
        let messages = vec![msg("a", "1-0", "0")];
        for (name, response, expected_reason) in [
            (
                "non-success status",
                RuntimeResponse {
                    status: Some(500),
                    json: Some(json!({ "outcome": "ok", "result": {} })),
                    text: None,
                    error: None,
                },
                "http_status_500",
            ),
            (
                "missing outer outcome",
                RuntimeResponse {
                    status: Some(200),
                    json: Some(json!({ "result": {} })),
                    text: None,
                    error: None,
                },
                "invalid_outer_outcome",
            ),
            (
                "unknown outer outcome",
                RuntimeResponse {
                    status: Some(200),
                    json: Some(json!({ "outcome": "future", "result": {} })),
                    text: None,
                    error: None,
                },
                "invalid_outer_outcome",
            ),
            (
                "null result",
                RuntimeResponse {
                    status: Some(200),
                    json: Some(json!({ "outcome": "ok", "result": null })),
                    text: None,
                    error: None,
                },
                "invalid_result_envelope",
            ),
            (
                "array result",
                RuntimeResponse {
                    status: Some(200),
                    json: Some(json!({ "outcome": "ok", "result": [] })),
                    text: None,
                    error: None,
                },
                "invalid_result_envelope",
            ),
            (
                "unknown inner outcome",
                queue_response_with("outcome", json!("future")),
                "invalid_inner_outcome",
            ),
            (
                "non-string inner outcome",
                queue_response_with("outcome", json!(1)),
                "invalid_inner_outcome",
            ),
            (
                "non-boolean ackAll",
                queue_response_with("ackAll", json!("true")),
                "invalid_ack_all",
            ),
            (
                "invalid explicit ack",
                queue_response_with("explicitAcks", json!(["a", 1])),
                "invalid_explicit_ack",
            ),
            (
                "unknown explicit ack message id",
                queue_response_with("explicitAcks", json!(["ghost"])),
                "unknown_explicit_ack_id",
            ),
            (
                "non-string retry message id",
                queue_response_with("retryMessages", json!([{ "msgId": 1 }])),
                "invalid_retry_message_id",
            ),
            (
                "unknown retry message id",
                queue_response_with("retryMessages", json!([{ "msgId": "ghost" }])),
                "unknown_retry_message_id",
            ),
            (
                "conflicting per-message decisions",
                RuntimeResponse {
                    status: Some(200),
                    json: Some(json!({
                        "outcome": "ok",
                        "result": {
                            "outcome": "ok",
                            "ackAll": false,
                            "explicitAcks": ["a"],
                            "retryMessages": [{ "msgId": "a" }],
                            "retryBatch": { "retry": false }
                        }
                    })),
                    text: None,
                    error: None,
                },
                "conflicting_message_decision",
            ),
            (
                "invalid retry message delay",
                queue_response_with(
                    "retryMessages",
                    json!([{ "msgId": "a", "delaySeconds": "5" }]),
                ),
                "invalid_retry_message_delay",
            ),
            (
                "non-boolean batch retry",
                queue_response_with("retryBatch", json!({ "retry": "true" })),
                "invalid_retry_batch",
            ),
            (
                "missing batch retry decision",
                queue_response_with("retryBatch", json!({})),
                "invalid_retry_batch",
            ),
            (
                "invalid batch retry delay",
                queue_response_with("retryBatch", json!({ "retry": true, "delaySeconds": 1.5 })),
                "invalid_retry_batch_delay",
            ),
        ] {
            match decide_outcome(&response, messages.clone()) {
                OutcomePlan::RetryAll {
                    kind,
                    reason,
                    messages: planned,
                } => {
                    assert_eq!(kind, "handler_error", "{name}");
                    assert_eq!(reason, expected_reason, "{name}");
                    assert_eq!(planned.len(), 1, "{name}");
                }
                _ => panic!("expected malformed {name} response to retry all messages"),
            }
        }
    }

    #[test]
    fn decide_outcome_uses_the_shared_runtime_response_contract() {
        let contract: JsonValue = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/queue-runtime-response.json"
        ))
        .expect("queue runtime response fixture must be valid JSON");
        let outer_ok = contract["outerOutcomes"]["ok"]
            .as_str()
            .expect("outer ok outcome must be a string");
        let inner_ok = contract["innerOutcomes"]["ok"]
            .as_str()
            .expect("inner ok outcome must be a string");
        let inner_exception = contract["innerOutcomes"]["exception"]
            .as_str()
            .expect("inner exception outcome must be a string");
        let required_result_fields = contract["requiredResultFields"]
            .as_array()
            .expect("required result fields must be an array");
        let messages = vec![msg("a", "1-0", "0")];
        let mut ok_result = complete_queue_result();
        ok_result
            .as_object_mut()
            .expect("queue result must be an object")
            .insert("outcome".to_string(), json!(inner_ok));
        for field in required_result_fields {
            let field = field
                .as_str()
                .expect("required result field must be a string");
            assert!(ok_result.get(field).is_some(), "missing {field}");
        }

        assert!(matches!(
            decide_outcome(
                &RuntimeResponse {
                    status: Some(200),
                    json: Some(json!({
                        "outcome": outer_ok,
                        "result": ok_result
                    })),
                    text: None,
                    error: None,
                },
                messages.clone(),
            ),
            OutcomePlan::Normal { .. }
        ));
        for field in required_result_fields {
            let field = field
                .as_str()
                .expect("required result field must be a string");
            assert!(matches!(
                decide_outcome(&queue_response_without(field), messages.clone()),
                OutcomePlan::RetryAll {
                    kind: "handler_error",
                    ..
                }
            ));
        }
        let mut exception_result = complete_queue_result();
        exception_result
            .as_object_mut()
            .expect("queue result must be an object")
            .insert("outcome".to_string(), json!(inner_exception));
        assert!(matches!(
            decide_outcome(
                &RuntimeResponse {
                    status: Some(200),
                    json: Some(json!({
                        "outcome": outer_ok,
                        "result": exception_result
                    })),
                    text: None,
                    error: None,
                },
                messages,
            ),
            OutcomePlan::RetryAll {
                kind: "handler_error",
                ..
            }
        ));
    }
}
