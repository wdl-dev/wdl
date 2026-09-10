use std::collections::HashMap;

use serde_json::Value as JsonValue;

use crate::{AppState, WorkflowError, WorkflowResult};

use super::{
    InstanceRouteKeys, MAX_WORKFLOW_EVENT_BYTES, MAX_WORKFLOW_EVENT_TYPE_BYTES,
    MAX_WORKFLOW_INSTANCE_PAYLOAD_BYTES, MAX_WORKFLOW_PARAMS_BYTES, MAX_WORKFLOW_RESULT_BYTES,
};

pub(crate) fn params_json(params: &JsonValue) -> WorkflowResult<String> {
    let json = serde_json::to_string(params)
        .map_err(|err| WorkflowError::invalid_request(format!("Invalid workflow params: {err}")))?;
    if json.len() > MAX_WORKFLOW_PARAMS_BYTES {
        return Err(WorkflowError::request_too_large(
            "Workflow params exceed the 1048576 byte limit",
        ));
    }
    Ok(json)
}

pub(super) fn result_json(value: &JsonValue, kind: &str) -> WorkflowResult<String> {
    let json = serde_json::to_string(value).map_err(|err| {
        WorkflowError::internal_error(format!("Workflow {kind} serialization failed: {err}"))
    })?;
    if json.len() > MAX_WORKFLOW_RESULT_BYTES {
        return Err(WorkflowError::request_too_large(format!(
            "Workflow {kind} exceeds the {MAX_WORKFLOW_RESULT_BYTES} byte limit"
        )));
    }
    Ok(json)
}

pub(super) fn instance_payload_limit_arg() -> String {
    MAX_WORKFLOW_INSTANCE_PAYLOAD_BYTES.to_string()
}

pub(super) fn payload_bytes_arg(json: &str) -> String {
    json.len().to_string()
}

pub(super) fn aggregate_payload_error() -> WorkflowError {
    WorkflowError::request_too_large(format!(
        "Workflow instance payloads exceed the {MAX_WORKFLOW_INSTANCE_PAYLOAD_BYTES} byte limit"
    ))
}

pub(super) fn event_type_from_value(value: &JsonValue) -> WorkflowResult<&str> {
    let event_type = value
        .get("type")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| WorkflowError::invalid_request("Workflow event type is required"))?;
    if event_type.is_empty() {
        return Err(WorkflowError::invalid_request(
            "Workflow event type must be non-empty",
        ));
    }
    if event_type.len() > MAX_WORKFLOW_EVENT_TYPE_BYTES {
        return Err(WorkflowError::request_too_large(format!(
            "Workflow event type exceeds the {MAX_WORKFLOW_EVENT_TYPE_BYTES} byte limit"
        )));
    }
    Ok(event_type)
}

pub(super) fn event_payload_json(value: &JsonValue) -> WorkflowResult<String> {
    let payload = value.get("payload").cloned().unwrap_or(JsonValue::Null);
    let json = serde_json::to_string(&payload).map_err(|err| {
        WorkflowError::invalid_request(format!("Invalid workflow event payload: {err}"))
    })?;
    if json.len() > MAX_WORKFLOW_EVENT_BYTES {
        return Err(WorkflowError::request_too_large(format!(
            "Workflow event payload exceeds the {MAX_WORKFLOW_EVENT_BYTES} byte limit"
        )));
    }
    Ok(json)
}

pub(super) async fn read_payload_ref(
    app: &AppState,
    state: &HashMap<String, String>,
    payloads_key: &str,
    field: &str,
) -> WorkflowResult<Option<JsonValue>> {
    let Some(payload_ref) = state.get(field).cloned() else {
        return Ok(None);
    };
    let payload_ref_for_query = payload_ref.clone();
    let raw: Option<String> = app
        .redis
        .with_conn(async |mut conn| {
            redis::cmd("HGET")
                .arg(payloads_key)
                .arg(payload_ref_for_query)
                .query_async(&mut conn)
                .await
        })
        .await?;
    parse_payload_ref(raw, &payload_ref, MAX_WORKFLOW_RESULT_BYTES)
}

pub(super) fn canonical_instance_payloads_key(
    state: &HashMap<String, String>,
    ns: &str,
    workflow_key: &str,
    instance_id: &str,
) -> WorkflowResult<String> {
    for (field, expected) in [
        ("ns", ns),
        ("workflowKey", workflow_key),
        ("instanceId", instance_id),
    ] {
        if state.get(field).map(String::as_str) != Some(expected) {
            return Err(WorkflowError::invalid_state(
                "Workflow state identity does not match the requested instance",
            ));
        }
    }
    let expected = InstanceRouteKeys::new(ns, workflow_key, instance_id).payloads();
    if state.get("payloadsKey") != Some(&expected) {
        return Err(WorkflowError::invalid_state(
            "Workflow payload storage key is invalid",
        ));
    }
    Ok(expected)
}

pub(super) fn parse_payload_ref(
    raw: Option<String>,
    payload_ref: &str,
    max_bytes: usize,
) -> WorkflowResult<Option<JsonValue>> {
    let Some(raw) = raw else {
        return Err(WorkflowError::payload_missing(format!(
            "Workflow payload reference {payload_ref} is missing"
        )));
    };
    parse_payload_json(&raw, max_bytes).map(Some)
}

pub(super) fn parse_payload_json(raw: &str, max_bytes: usize) -> WorkflowResult<JsonValue> {
    if raw.len() > max_bytes {
        return Err(WorkflowError::invalid_state(format!(
            "Workflow payload exceeds the {max_bytes} byte limit"
        )));
    }
    serde_json::from_str(raw)
        .map_err(|err| WorkflowError::invalid_state(format!("Workflow payload is corrupt: {err}")))
}

pub(super) fn payload_storage_key_for_ref(
    state: &HashMap<String, String>,
    payload_ref: &str,
) -> WorkflowResult<String> {
    state.get("payloadsKey").cloned().ok_or_else(|| {
        WorkflowError::payload_missing(format!(
            "Workflow payload reference {payload_ref} has no payload storage key"
        ))
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::*;

    #[test]
    fn payload_ref_missing_fails_closed() {
        let err = parse_payload_ref(None, "output", MAX_WORKFLOW_RESULT_BYTES)
            .expect_err("dangling payload ref should fail closed");
        assert_eq!(err.code, "workflow_payload_missing");
        assert!(err.message.contains("output"));
    }

    #[test]
    fn payload_ref_byte_limit_is_shared_by_result_and_params_readers() {
        for (field, limit) in [
            ("output", MAX_WORKFLOW_RESULT_BYTES),
            ("error", MAX_WORKFLOW_RESULT_BYTES),
            ("params", MAX_WORKFLOW_PARAMS_BYTES),
        ] {
            let value = "x".repeat(limit - 2);
            let exact = serde_json::to_string(&value).unwrap();
            assert_eq!(exact.len(), limit);
            assert_eq!(
                parse_payload_ref(Some(exact), field, limit).unwrap(),
                Some(JsonValue::String(value))
            );
            let oversized = serde_json::to_string(&"x".repeat(limit - 1)).unwrap();
            let error = parse_payload_ref(Some(oversized), field, limit).unwrap_err();
            assert_eq!(error.code, "workflow_invalid_state");
            assert_eq!(
                error.message,
                format!("Workflow payload exceeds the {limit} byte limit")
            );
        }
        let error = parse_payload_ref(Some("not-json".to_string()), "output", 1).unwrap_err();
        assert_eq!(error.message, "Workflow payload exceeds the 1 byte limit");
    }

    #[test]
    fn persisted_payload_json_rejects_corruption_before_reuse() {
        for raw in ["", "{broken", "null trailing", r#""\ud800""#, "NaN"] {
            let error = parse_payload_json(raw, MAX_WORKFLOW_PARAMS_BYTES).unwrap_err();
            assert_eq!(error.code, "workflow_invalid_state");
            assert_eq!(
                parse_payload_ref(Some(raw.to_string()), "params", MAX_WORKFLOW_PARAMS_BYTES)
                    .unwrap_err()
                    .message,
                error.message,
            );
        }
    }

    #[test]
    fn payload_ref_requires_payloads_key() {
        let mut state = HashMap::new();
        state.insert("outputRef".to_string(), "output".to_string());
        let err = payload_storage_key_for_ref(&state, "output")
            .expect_err("payload ref without payloadsKey should fail closed");
        assert_eq!(err.code, "workflow_payload_missing");
    }

    #[test]
    fn canonical_payload_key_binds_all_persisted_instance_identity() {
        let ns = "demo";
        let workflow_key = "wf_aaaaaaaaaaaaaaaa";
        let instance_id = "wfi_bbbbbbbbbbbbbbbb";
        let expected = InstanceRouteKeys::new(ns, workflow_key, instance_id).payloads();
        let mut state = HashMap::from([
            ("ns".to_string(), ns.to_string()),
            ("workflowKey".to_string(), workflow_key.to_string()),
            ("instanceId".to_string(), instance_id.to_string()),
            ("payloadsKey".to_string(), expected.clone()),
        ]);

        assert_eq!(
            canonical_instance_payloads_key(&state, ns, workflow_key, instance_id).unwrap(),
            expected
        );

        for (field, invalid) in [
            ("ns", "other"),
            ("workflowKey", "wf_cccccccccccccccc"),
            ("instanceId", "wfi_dddddddddddddddd"),
        ] {
            let original = state
                .insert(field.to_string(), invalid.to_string())
                .unwrap();
            let err = canonical_instance_payloads_key(&state, ns, workflow_key, instance_id)
                .expect_err("persisted identity drift must fail closed");
            assert_eq!(err.code, "workflow_invalid_state");
            state.insert(field.to_string(), original);
        }

        state.insert("payloadsKey".to_string(), "wf:payloads:other".to_string());
        let err = canonical_instance_payloads_key(&state, ns, workflow_key, instance_id)
            .expect_err("noncanonical payload storage must fail closed");
        assert_eq!(err.code, "workflow_invalid_state");
    }

    #[test]
    fn event_type_has_ingress_cap() {
        let value = json!({ "type": "x".repeat(MAX_WORKFLOW_EVENT_TYPE_BYTES + 1) });
        let err = event_type_from_value(&value).expect_err("oversized event type should fail");
        assert_eq!(err.code, "request_too_large");
    }
}
