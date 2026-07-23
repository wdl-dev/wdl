use std::collections::HashMap;

use serde_json::{Value as JsonValue, json};

use crate::{AppState, WorkflowError, WorkflowResult, by_workflow_key, instance_state_key};

use super::payload::{canonical_instance_payloads_key, parse_payload_ref};
use super::{
    InstanceResponse, ListInstancesResponse, WorkflowRequest, instance_id, public_state_or_empty,
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
    instances: &mut [InstanceResponse],
    read: &InstancePayloadRead,
    raw: Option<String>,
) -> WorkflowResult<()> {
    let value = parse_payload_ref(raw, &read.payload_ref)?;
    let instance = instances.get_mut(read.instance_index).ok_or_else(|| {
        WorkflowError::internal_error("workflow list payload response index mismatch")
    })?;
    match read.field {
        InstancePayloadField::Output => instance.output = value,
        InstancePayloadField::Error => instance.error = value,
    }
    Ok(())
}

async fn read_list_payloads(
    state: &AppState,
    instances: &mut [InstanceResponse],
    reads: &[InstancePayloadRead],
) -> WorkflowResult<()> {
    for batch in reads.chunks(LIST_PAYLOAD_READ_BATCH_SIZE) {
        let raw_values: Vec<Option<String>> = state
            .redis
            .with_conn(async |mut conn| {
                let mut pipe = redis::pipe();
                for read in batch {
                    pipe.cmd("HGET")
                        .arg(&read.payloads_key)
                        .arg(&read.payload_ref);
                }
                pipe.query_async(&mut conn).await
            })
            .await?;
        if raw_values.len() != batch.len() {
            return Err(WorkflowError::internal_error(
                "workflow list payload reply count mismatch",
            ));
        }
        for (read, raw) in batch.iter().zip(raw_values) {
            apply_list_payload_reply(instances, read, raw)?;
        }
    }
    Ok(())
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
    let mut instances = Vec::new();
    let mut payload_reads = Vec::new();
    for (instance_id, raw_state) in members.iter().zip(raw_states) {
        let existing = public_state_or_empty(state, raw_state).await?;
        if existing.is_empty() {
            continue;
        }
        let instance_index = instances.len();
        instances.push(prepare_list_instance(
            &req.ns,
            &req.workflow_key,
            instance_id,
            &existing,
            instance_index,
            &mut payload_reads,
        )?);
    }
    read_list_payloads(state, &mut instances, &payload_reads).await?;
    let next = cursor.saturating_add(u64::try_from(members.len()).unwrap_or(u64::MAX));

    Ok(ListInstancesResponse {
        instances,
        cursor: has_more.then(|| next.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::InstanceRouteKeys;

    #[test]
    fn list_instances_uses_bounded_workflow_index_page() {
        let source = include_str!("status.rs");
        assert!(source.contains(r#"redis::cmd("ZRANGE")"#));
        assert!(!source.contains(concat!("SM", "EMBERS")));
        assert!(source.contains("members.truncate(limit)"));
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
        let mut instances = vec![response];

        assert_eq!(reads.len(), 2);
        assert!(matches!(reads[0].field, InstancePayloadField::Output));
        assert!(matches!(reads[1].field, InstancePayloadField::Error));
        assert_eq!(reads[0].payload_ref, "shared-ref");
        assert_eq!(reads[1].payload_ref, "shared-ref");

        apply_list_payload_reply(
            &mut instances,
            &reads[0],
            Some(r#"{"kind":"output"}"#.to_string()),
        )
        .expect("output payload");
        apply_list_payload_reply(
            &mut instances,
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
        let mut instances = vec![response];

        let err = apply_list_payload_reply(&mut instances, &reads[0], None)
            .expect_err("missing list payload must fail closed");
        assert_eq!(err.code, "workflow_payload_missing");
    }
}
