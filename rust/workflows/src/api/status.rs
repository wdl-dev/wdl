use std::collections::HashMap;

use serde_json::{Value as JsonValue, json};

use crate::{AppState, WorkflowError, WorkflowResult, by_workflow_key, instance_state_key};

use super::payload::{canonical_instance_payloads_key, parse_payload_ref};
use super::{
    InstanceResponse, ListInstancesResponse, MAX_WORKFLOW_INSTANCES_RESPONSE_BYTES,
    MAX_WORKFLOW_RESULT_BYTES, WorkflowRequest, instance_id, public_state_or_empty,
    read_payload_ref, read_step_history, validate_identity, verify_workflow_def,
    workflow_step_options,
};

const DEFAULT_INSTANCES_LIMIT: usize = 100;
const MAX_INSTANCES_LIMIT: usize = 1000;
const LIST_PAYLOAD_READ_BATCH_SIZE: usize = 32;

#[derive(Clone, Copy)]
enum InstancePayloadField {
    Output,
    Error,
}

struct InstancePayloadRead {
    instance_index: usize,
    field: InstancePayloadField,
    payloads_key: String,
    payload_ref: String,
}

fn workflow_instances_options(options: &JsonValue) -> WorkflowResult<(usize, u64)> {
    let limit = match options.get("limit") {
        Some(raw) => {
            let Some(value) = raw.as_u64() else {
                return Err(WorkflowError::invalid_request(
                    "instances limit must be an integer",
                ));
            };
            let Ok(value) = usize::try_from(value) else {
                return Err(WorkflowError::request_too_large(
                    "instances limit exceeds the 1000 item limit",
                ));
            };
            if !(1..=MAX_INSTANCES_LIMIT).contains(&value) {
                return Err(WorkflowError::request_too_large(
                    "instances limit must be in [1, 1000]",
                ));
            }
            value
        }
        None => DEFAULT_INSTANCES_LIMIT,
    };
    let cursor = match options.get("cursor") {
        Some(raw) => {
            let Some(raw) = raw.as_str() else {
                return Err(WorkflowError::invalid_request(
                    "instances cursor must be a string",
                ));
            };
            if raw.is_empty() {
                0
            } else {
                raw.parse::<u64>()
                    .map_err(|_| WorkflowError::invalid_request("instances cursor is invalid"))?
            }
        }
        None => 0,
    };
    Ok((limit, cursor))
}

fn inline_error_from_state(state: &HashMap<String, String>) -> Option<JsonValue> {
    let code = state.get("errorCode")?;
    let message = state
        .get("errorMessage")
        .cloned()
        .unwrap_or_else(|| code.clone());
    Some(json!({ "name": "WorkflowError", "code": code, "message": message }))
}

pub(super) async fn read_state(
    state: &AppState,
    req: &WorkflowRequest,
) -> WorkflowResult<HashMap<String, String>> {
    let id = instance_id(req)?.to_string();
    read_state_by_id(state, &req.ns, &req.workflow_key, &id).await
}

pub(super) async fn read_state_by_id(
    state: &AppState,
    ns: &str,
    workflow_key: &str,
    instance_id: &str,
) -> WorkflowResult<HashMap<String, String>> {
    let key = instance_state_key(ns, workflow_key, instance_id);
    state
        .redis
        .with_conn(async |mut conn| redis::cmd("HGETALL").arg(key).query_async(&mut conn).await)
        .await
        .map_err(WorkflowError::from)
}

pub(super) async fn response_from_state(
    app: &AppState,
    ns: &str,
    workflow_key: &str,
    id: &str,
    state: &HashMap<String, String>,
) -> WorkflowResult<InstanceResponse> {
    let status = state
        .get("status")
        .ok_or_else(|| WorkflowError::not_found("Workflow instance not found"))?;
    let payloads_key = canonical_instance_payloads_key(state, ns, workflow_key, id)?;
    let output = read_payload_ref(app, state, &payloads_key, "outputRef").await?;
    let error = match read_payload_ref(app, state, &payloads_key, "errorRef").await? {
        Some(error) => Some(error),
        None => inline_error_from_state(state),
    };
    Ok(InstanceResponse {
        id: id.to_string(),
        status: status.to_string(),
        output,
        error,
        steps: None,
    })
}

async fn response_with_steps(
    app: &AppState,
    ns: &str,
    workflow_key: &str,
    id: &str,
    state: &HashMap<String, String>,
    limit: usize,
) -> WorkflowResult<InstanceResponse> {
    let mut response = response_from_state(app, ns, workflow_key, id, state).await?;
    response.steps = Some(read_step_history(app, ns, workflow_key, id, limit).await?);
    Ok(response)
}

fn prepare_list_instance(
    ns: &str,
    workflow_key: &str,
    id: &str,
    state: &HashMap<String, String>,
    instance_index: usize,
    payload_reads: &mut Vec<InstancePayloadRead>,
) -> WorkflowResult<InstanceResponse> {
    let status = state
        .get("status")
        .ok_or_else(|| WorkflowError::not_found("Workflow instance not found"))?;
    let payloads_key = canonical_instance_payloads_key(state, ns, workflow_key, id)?;
    for (field_name, field) in [
        ("outputRef", InstancePayloadField::Output),
        ("errorRef", InstancePayloadField::Error),
    ] {
        let Some(payload_ref) = state.get(field_name) else {
            continue;
        };
        payload_reads.push(InstancePayloadRead {
            instance_index,
            field,
            payloads_key: payloads_key.clone(),
            payload_ref: payload_ref.clone(),
        });
    }
    Ok(InstanceResponse {
        id: id.to_string(),
        status: status.clone(),
        output: None,
        error: (!state.contains_key("errorRef"))
            .then(|| inline_error_from_state(state))
            .flatten(),
        steps: None,
    })
}

fn apply_list_payload_reply(
    instance: &mut InstanceResponse,
    read: &InstancePayloadRead,
    raw: Option<String>,
) -> WorkflowResult<()> {
    let value = parse_payload_ref(raw, &read.payload_ref, MAX_WORKFLOW_RESULT_BYTES)?;
    match read.field {
        InstancePayloadField::Output => instance.output = value,
        InstancePayloadField::Error => instance.error = value,
    }
    Ok(())
}

async fn read_list_payload_batch(
    state: &AppState,
    reads: &[InstancePayloadRead],
) -> WorkflowResult<std::vec::IntoIter<Option<String>>> {
    let raw_values: Vec<Option<String>> = state
        .redis
        .with_conn(async |mut conn| {
            let mut pipe = redis::pipe();
            for read in reads {
                pipe.cmd("HGET")
                    .arg(&read.payloads_key)
                    .arg(&read.payload_ref);
            }
            pipe.query_async(&mut conn).await
        })
        .await?;
    if raw_values.len() != reads.len() {
        return Err(WorkflowError::internal_error(
            "workflow list payload reply count mismatch",
        ));
    }
    Ok(raw_values.into_iter())
}

fn list_response_overhead() -> WorkflowResult<usize> {
    // Reserve the envelope and longest cursor before admitting any instance.
    serde_json::to_vec(&ListInstancesResponse {
        instances: Vec::new(),
        cursor: Some(u64::MAX.to_string()),
    })
    .map(|body| body.len())
    .map_err(|err| WorkflowError::internal_error(format!("workflow list envelope: {err}")))
}

fn append_list_instance(
    page: &mut ListInstancesResponse,
    response_bytes: &mut usize,
    max_bytes: usize,
    instance: &InstanceResponse,
) -> WorkflowResult<bool> {
    let raw = serde_json::value::to_raw_value(instance)
        .map_err(|err| WorkflowError::internal_error(format!("workflow list instance: {err}")))?;
    let next_bytes = response_bytes
        .saturating_add(raw.get().len())
        .saturating_add(usize::from(!page.instances.is_empty()));
    if next_bytes > max_bytes {
        if page.instances.is_empty() {
            return Err(WorkflowError::invalid_state(
                "Workflow instance exceeds the list response byte limit",
            ));
        }
        return Ok(false);
    }
    *response_bytes = next_bytes;
    page.instances.push(raw);
    Ok(true)
}

pub(super) async fn read_public_state(
    state: &AppState,
    req: &WorkflowRequest,
) -> WorkflowResult<HashMap<String, String>> {
    public_state_or_empty(state, read_state(state, req).await?).await
}

pub(super) async fn read_public_state_by_id(
    state: &AppState,
    ns: &str,
    workflow_key: &str,
    instance_id: &str,
) -> WorkflowResult<HashMap<String, String>> {
    public_state_or_empty(
        state,
        read_state_by_id(state, ns, workflow_key, instance_id).await?,
    )
    .await
}

pub(crate) async fn get_instance(
    state: &AppState,
    req: WorkflowRequest,
) -> WorkflowResult<InstanceResponse> {
    validate_identity(&req)?;
    let id = instance_id(&req)?.to_string();
    let existing = read_public_state(state, &req).await?;
    if existing.is_empty() {
        return Err(WorkflowError::not_found("Workflow instance not found"));
    }
    response_from_state(state, &req.ns, &req.workflow_key, &id, &existing).await
}

pub(crate) async fn status_instance(
    state: &AppState,
    req: WorkflowRequest,
) -> WorkflowResult<InstanceResponse> {
    let step_limit = workflow_step_options(&req.options)?;
    validate_identity(&req)?;
    let id = instance_id(&req)?.to_string();
    let existing = read_public_state(state, &req).await?;
    if existing.is_empty() {
        return Err(WorkflowError::not_found("Workflow instance not found"));
    }
    match step_limit {
        Some(limit) => {
            response_with_steps(state, &req.ns, &req.workflow_key, &id, &existing, limit).await
        }
        None => response_from_state(state, &req.ns, &req.workflow_key, &id, &existing).await,
    }
}

pub(crate) async fn list_instances(
    state: &AppState,
    req: WorkflowRequest,
) -> WorkflowResult<ListInstancesResponse> {
    let (limit, cursor) = workflow_instances_options(&req.options)?;
    validate_identity(&req)?;
    verify_workflow_def(state, &req).await?;
    let by_workflow = by_workflow_key(&req.ns, &req.worker, &req.workflow_key);
    let start = i64::try_from(cursor)
        .map_err(|_| WorkflowError::invalid_request("instances cursor is invalid"))?;
    let limit_u64 = u64::try_from(limit).unwrap_or(u64::MAX);
    let max_cursor = u64::try_from(i64::MAX).unwrap_or(u64::MAX);
    let stop = cursor.saturating_add(limit_u64).min(max_cursor);
    let stop = i64::try_from(stop)
        .map_err(|_| WorkflowError::invalid_request("instances cursor is invalid"))?;
    let mut members: Vec<String> = state
        .redis
        .with_conn(async |mut conn| {
            redis::cmd("ZRANGE")
                .arg(by_workflow)
                .arg(start)
                .arg(stop)
                .query_async(&mut conn)
                .await
        })
        .await?;
    let has_more = members.len() > limit;
    members.truncate(limit);
    let raw_states: Vec<HashMap<String, String>> = if members.is_empty() {
        Vec::new()
    } else {
        let state_keys = members
            .iter()
            .map(|instance_id| instance_state_key(&req.ns, &req.workflow_key, instance_id))
            .collect::<Vec<_>>();
        state
            .redis
            .with_conn(async |mut conn| {
                let mut pipe = redis::pipe();
                for key in &state_keys {
                    pipe.cmd("HGETALL").arg(key);
                }
                pipe.query_async(&mut conn).await
            })
            .await?
    };
    if raw_states.len() != members.len() {
        return Err(WorkflowError::internal_error(
            "workflow list state reply count mismatch",
        ));
    }
    let mut instances = Vec::new();
    let mut payload_reads = Vec::new();
    for (member_index, (instance_id, raw_state)) in members.iter().zip(raw_states).enumerate() {
        let existing = public_state_or_empty(state, raw_state).await?;
        if existing.is_empty() {
            continue;
        }
        let instance_index = instances.len();
        let instance = prepare_list_instance(
            &req.ns,
            &req.workflow_key,
            instance_id,
            &existing,
            instance_index,
            &mut payload_reads,
        )?;
        instances.push((member_index, instance));
    }
    let next = cursor.saturating_add(u64::try_from(members.len()).unwrap_or(u64::MAX));
    let mut page = ListInstancesResponse {
        instances: Vec::new(),
        cursor: has_more.then(|| next.to_string()),
    };
    let mut response_bytes = list_response_overhead()?;
    let mut read_index = 0;
    let mut raw_values = Vec::new().into_iter();
    for (instance_index, (member_index, mut instance)) in instances.into_iter().enumerate() {
        while let Some(read) = payload_reads
            .get(read_index)
            .filter(|read| read.instance_index == instance_index)
        {
            if raw_values.len() == 0 {
                let end = (read_index + LIST_PAYLOAD_READ_BATCH_SIZE).min(payload_reads.len());
                raw_values =
                    read_list_payload_batch(state, &payload_reads[read_index..end]).await?;
            }
            let raw = raw_values.next().ok_or_else(|| {
                WorkflowError::internal_error("workflow list payload reply count mismatch")
            })?;
            apply_list_payload_reply(&mut instance, read, raw)?;
            read_index += 1;
        }
        // Retain serialized rows, not every payload's parsed JSON tree.
        if !append_list_instance(
            &mut page,
            &mut response_bytes,
            MAX_WORKFLOW_INSTANCES_RESPONSE_BYTES,
            &instance,
        )? {
            page.cursor = Some((cursor + member_index as u64).to_string());
            break;
        }
    }
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::InstanceRouteKeys;

    #[test]
    fn list_options_bound_count_and_preserve_cursor() {
        assert_eq!(
            workflow_instances_options(&JsonValue::Null).unwrap(),
            (100, 0)
        );
        assert_eq!(
            workflow_instances_options(&json!({"limit": 1000, "cursor": "27"})).unwrap(),
            (1000, 27)
        );
        for limit in [0, 1001] {
            assert_eq!(
                workflow_instances_options(&json!({"limit": limit}))
                    .unwrap_err()
                    .code,
                "request_too_large"
            );
        }
        assert_eq!(
            workflow_instances_options(&json!({"cursor": "invalid"}))
                .unwrap_err()
                .code,
            "invalid_request"
        );
    }

    fn list_instance(output: JsonValue, error: Option<JsonValue>) -> InstanceResponse {
        InstanceResponse {
            id: "order-1".to_string(),
            status: "completed".to_string(),
            output: Some(output),
            error,
            steps: None,
        }
    }

    #[test]
    fn list_response_budget_counts_utf8_escaping_commas_and_cursor() {
        let instance = list_instance(json!({"value": "\u{4e2d}\u{1f600}\n\"\\"}), None);
        let row_bytes = serde_json::to_vec(&instance).unwrap().len();
        let mut page = ListInstancesResponse {
            instances: Vec::new(),
            cursor: Some(u64::MAX.to_string()),
        };
        let mut bytes = list_response_overhead().unwrap();
        let limit = bytes + 2 * row_bytes + 1;
        assert!(append_list_instance(&mut page, &mut bytes, limit, &instance).unwrap());
        assert!(append_list_instance(&mut page, &mut bytes, limit, &instance).unwrap());
        assert!(!append_list_instance(&mut page, &mut bytes, limit, &instance).unwrap());
        assert_eq!(bytes, limit);
        let serialized = serde_json::to_vec(&page).unwrap();
        assert_eq!(serialized.len(), limit);
        let body: JsonValue = serde_json::from_slice(&serialized).unwrap();
        assert_eq!(body["instances"].as_array().unwrap().len(), 2);
        assert_eq!(
            body["instances"][0],
            serde_json::to_value(&instance).unwrap()
        );
        assert!(body["instances"][0].get("steps").is_none());

        let mut shorter = ListInstancesResponse {
            instances: Vec::new(),
            cursor: None,
        };
        let mut bytes = list_response_overhead().unwrap();
        assert!(append_list_instance(&mut shorter, &mut bytes, limit - 1, &instance).unwrap());
        assert!(!append_list_instance(&mut shorter, &mut bytes, limit - 1, &instance).unwrap());
    }

    #[test]
    fn list_response_byte_limit_shortens_a_legal_large_payload_page() {
        let instance = list_instance(
            json!("x".repeat(MAX_WORKFLOW_RESULT_BYTES - 2)),
            Some(JsonValue::Null),
        );
        let mut page = ListInstancesResponse {
            instances: Vec::new(),
            cursor: None,
        };
        let mut bytes = list_response_overhead().unwrap();
        let mut admitted = 0;
        for _ in 0..MAX_INSTANCES_LIMIT {
            if !append_list_instance(
                &mut page,
                &mut bytes,
                MAX_WORKFLOW_INSTANCES_RESPONSE_BYTES,
                &instance,
            )
            .unwrap()
            {
                break;
            }
            admitted += 1;
        }
        assert_eq!(admitted, 7);
        assert!(serde_json::to_vec(&page).unwrap().len() <= MAX_WORKFLOW_INSTANCES_RESPONSE_BYTES);
        assert_eq!(page.instances.len(), admitted);
    }

    #[test]
    fn list_response_rejects_an_unpageable_instance_instead_of_stalling_cursor() {
        let instance = list_instance(JsonValue::Null, None);
        let mut page = ListInstancesResponse {
            instances: Vec::new(),
            cursor: None,
        };
        let mut bytes = list_response_overhead().unwrap();
        let limit = bytes + serde_json::to_vec(&instance).unwrap().len() - 1;
        let error = append_list_instance(&mut page, &mut bytes, limit, &instance).unwrap_err();
        assert_eq!(error.code, "workflow_invalid_state");
        assert!(page.instances.is_empty());
    }

    #[test]
    fn list_payload_rejects_oversized_persisted_json_before_parsing() {
        let mut instance = list_instance(JsonValue::Null, None);
        let read = InstancePayloadRead {
            instance_index: 0,
            field: InstancePayloadField::Output,
            payloads_key: "payloads".to_string(),
            payload_ref: "output".to_string(),
        };
        let error = apply_list_payload_reply(
            &mut instance,
            &read,
            Some("x".repeat(MAX_WORKFLOW_RESULT_BYTES + 1)),
        )
        .unwrap_err();
        assert_eq!(error.code, "workflow_invalid_state");
        assert_eq!(
            error.message,
            format!("Workflow payload exceeds the {MAX_WORKFLOW_RESULT_BYTES} byte limit")
        );
    }

    #[test]
    fn inline_error_from_state_uses_stored_code_and_message() {
        let mut state = HashMap::new();
        state.insert(
            "errorCode".to_string(),
            "workflow_payload_too_large".to_string(),
        );
        state.insert(
            "errorMessage".to_string(),
            "payload budget exceeded".to_string(),
        );

        assert_eq!(
            inline_error_from_state(&state),
            Some(json!({
                "name": "WorkflowError",
                "code": "workflow_payload_too_large",
                "message": "payload budget exceeded",
            }))
        );
    }

    #[test]
    fn list_payload_reads_preserve_duplicate_refs_and_response_slots() {
        let state = HashMap::from([
            ("ns".to_string(), "demo".to_string()),
            ("workflowKey".to_string(), "wf_test".to_string()),
            ("instanceId".to_string(), "inst-1".to_string()),
            ("status".to_string(), "failed".to_string()),
            (
                "payloadsKey".to_string(),
                InstanceRouteKeys::new("demo", "wf_test", "inst-1").payloads(),
            ),
            ("outputRef".to_string(), "shared-ref".to_string()),
            ("errorRef".to_string(), "shared-ref".to_string()),
        ]);
        let mut reads = Vec::new();
        let response = prepare_list_instance("demo", "wf_test", "inst-1", &state, 0, &mut reads)
            .expect("valid list instance");
        let mut instances = [response];

        assert_eq!(reads.len(), 2);
        assert!(matches!(reads[0].field, InstancePayloadField::Output));
        assert!(matches!(reads[1].field, InstancePayloadField::Error));
        assert_eq!(reads[0].payload_ref, "shared-ref");
        assert_eq!(reads[1].payload_ref, "shared-ref");

        apply_list_payload_reply(
            &mut instances[0],
            &reads[0],
            Some(r#"{"kind":"output"}"#.to_string()),
        )
        .expect("output payload");
        apply_list_payload_reply(
            &mut instances[0],
            &reads[1],
            Some(r#"{"kind":"error"}"#.to_string()),
        )
        .expect("error payload");
        assert_eq!(instances[0].output, Some(json!({ "kind": "output" })));
        assert_eq!(instances[0].error, Some(json!({ "kind": "error" })));
    }

    #[test]
    fn list_payload_read_fails_closed_on_missing_ref() {
        let state = HashMap::from([
            ("ns".to_string(), "demo".to_string()),
            ("workflowKey".to_string(), "wf_test".to_string()),
            ("instanceId".to_string(), "inst-1".to_string()),
            ("status".to_string(), "completed".to_string()),
            (
                "payloadsKey".to_string(),
                InstanceRouteKeys::new("demo", "wf_test", "inst-1").payloads(),
            ),
            ("outputRef".to_string(), "missing-ref".to_string()),
        ]);
        let mut reads = Vec::new();
        let response = prepare_list_instance("demo", "wf_test", "inst-1", &state, 0, &mut reads)
            .expect("valid list instance");
        let mut instances = [response];

        let err = apply_list_payload_reply(&mut instances[0], &reads[0], None)
            .expect_err("missing list payload must fail closed");
        assert_eq!(err.code, "workflow_payload_missing");
    }
}
