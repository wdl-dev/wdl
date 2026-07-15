use std::collections::HashSet;

use serde_json::Value as JsonValue;
use wdl_rust_common::time::now_ms;

use crate::{AppState, WorkflowError, WorkflowResult, by_version_key, by_worker_key};

use super::{
    CreateBatchResponse, InstanceResponse, InstanceRouteKeys, MAX_CREATE_BATCH_SIZE,
    PENDING_CREATE_TTL_MS, WorkflowRequest, cleanup_created_instance, ensure_worker_not_deleting,
    eval_script, finalize_created_instance, instance_id, log_instance_event_from_request,
    params_json, payload_bytes_arg, pending_create_token, read_public_state,
    request_with_active_version, response_from_state, retention_policy,
    spawn_progress_from_request, validate_identity, verify_active_workflow_current,
    verify_workflow_def, wait_for_public_create_state, workflow_referrer_member,
};

const CREATE_INSTANCE_SCRIPT: &str = r#"
if redis.call("EXISTS", KEYS[1]) ~= 0 then
  return 0
end
redis.call("HSET", KEYS[1],
  "ns", ARGV[1],
  "worker", ARGV[2],
  "frozenVersion", ARGV[3],
  "workflowName", ARGV[4],
  "workflowKey", ARGV[5],
  "className", ARGV[6],
  "instanceId", ARGV[7],
  "status", "pending_create",
  "payloadsKey", KEYS[2],
  "generation", "1",
  "createdAtMs", ARGV[8],
  "updatedAtMs", ARGV[8],
  "pendingExpiresAtMs", ARGV[12],
  "pendingCreateToken", ARGV[13],
  "paramsRef", "params",
  "payloadBytes", ARGV[15],
  "successRetentionMs", ARGV[9],
  "errorRetentionMs", ARGV[10],
  "eventSeq", "0")
if ARGV[16] ~= "" then
  redis.call("HSET", KEYS[1], "callback", ARGV[16])
end
redis.call("HSET", KEYS[2], "params", ARGV[11])
redis.call("SADD", KEYS[3], ARGV[14])
redis.call("SADD", KEYS[4], ARGV[14])
return 1
"#;

fn callback_json(value: &JsonValue) -> WorkflowResult<String> {
    if value.is_null() {
        return Ok(String::new());
    }
    let Some(obj) = value.as_object() else {
        return Err(WorkflowError::invalid_request(
            "Workflow callback must be an object",
        ));
    };
    if obj.get("kind").and_then(JsonValue::as_str) != Some("do") {
        return Err(WorkflowError::invalid_request(
            "Workflow callback kind must be \"do\"",
        ));
    }
    for field in ["binding", "idFromName"] {
        let Some(value) = obj.get(field).and_then(JsonValue::as_str) else {
            return Err(WorkflowError::invalid_request(format!(
                "Workflow callback {field} must be a non-empty string"
            )));
        };
        if value.is_empty()
            || value.len() > 256
            || value.contains('\t')
            || value.contains('\n')
            || value.contains('\r')
        {
            return Err(WorkflowError::invalid_request(format!(
                "Workflow callback {field} is invalid"
            )));
        }
    }
    if let Some(path) = obj.get("path") {
        let Some(path) = path.as_str() else {
            return Err(WorkflowError::invalid_request(
                "Workflow callback path must be a string",
            ));
        };
        if !path.starts_with('/')
            || path.len() > 512
            || path.contains('\t')
            || path.contains('\n')
            || path.contains('\r')
        {
            return Err(WorkflowError::invalid_request(
                "Workflow callback path is invalid",
            ));
        }
    }
    let json = serde_json::to_string(value).map_err(|err| {
        WorkflowError::invalid_request(format!("Workflow callback JSON is invalid: {err}"))
    })?;
    if json.len() > 8192 {
        return Err(WorkflowError::request_too_large(
            "Workflow callback descriptor exceeds the 8 KiB limit",
        ));
    }
    Ok(json)
}

pub(crate) async fn create_instance(
    state: &AppState,
    req: WorkflowRequest,
) -> WorkflowResult<InstanceResponse> {
    let req = request_with_active_version(state, req).await?;
    validate_identity(&req)?;
    verify_workflow_def(state, &req).await?;
    create_instance_prevalidated(state, req).await
}

async fn create_instance_prevalidated(
    state: &AppState,
    req: WorkflowRequest,
) -> WorkflowResult<InstanceResponse> {
    let id = instance_id(&req)?.to_string();
    ensure_worker_not_deleting(state, &req.ns, &req.worker).await?;

    let params = params_json(&req.params)?;
    let retention = retention_policy(&req.retention)?;
    let callback = callback_json(&req.callback)?;
    let keys = InstanceRouteKeys::new(&req.ns, &req.workflow_key, &id);
    let state_key = keys.state();
    let payloads_key = keys.payloads();
    let by_worker = by_worker_key(&req.ns, &req.worker);
    let by_version = by_version_key(&req.ns, &req.worker, &req.frozen_version);
    let referrer_member = workflow_referrer_member(&req.workflow_key, &id);
    let redis_id = id.clone();
    let now_ms = now_ms();
    let now = now_ms.to_string();
    let pending_expires_at = now_ms.saturating_add(PENDING_CREATE_TTL_MS).to_string();
    let pending_create_token = pending_create_token(state, &req, &id);
    let success_retention_ms = retention.success_ms.to_string();
    let error_retention_ms = retention.error_ms.to_string();
    let params_bytes = payload_bytes_arg(&params);
    let create = async || {
        eval_script(
            state,
            CREATE_INSTANCE_SCRIPT,
            &[&state_key, &payloads_key, &by_worker, &by_version],
            &[
                &req.ns,
                &req.worker,
                &req.frozen_version,
                &req.workflow_name,
                &req.workflow_key,
                &req.class_name,
                &redis_id,
                &now,
                &success_retention_ms,
                &error_retention_ms,
                &params,
                &pending_expires_at,
                &pending_create_token,
                &referrer_member,
                &params_bytes,
                &callback,
            ],
        )
        .await
    };
    let mut created: i64 = create().await?;
    if created != 1 {
        let mut existing = wait_for_public_create_state(state, &req).await?;
        if existing.is_empty() {
            created = create().await?;
            if created == 1 {
                existing.clear();
            } else {
                existing = wait_for_public_create_state(state, &req).await?;
                if existing.is_empty() {
                    return Err(WorkflowError::invalid_state(
                        "Workflow instance create raced and did not persist state",
                    ));
                }
            }
        }
        if created != 1 {
            state
                .metrics
                .increment("workflow_instances", &[("outcome", "existing")], 1.0);
            return response_from_state(state, &id, &existing).await;
        }
    }

    if let Err(err) = ensure_worker_not_deleting(state, &req.ns, &req.worker).await {
        cleanup_created_instance(state, &req, &id, &pending_create_token).await?;
        return Err(err);
    }
    if let Err(err) = verify_active_workflow_current(state, &req).await {
        cleanup_created_instance(state, &req, &id, &pending_create_token).await?;
        return Err(err);
    }
    if let Err(err) = verify_workflow_def(state, &req).await {
        cleanup_created_instance(state, &req, &id, &pending_create_token).await?;
        return Err(err);
    }
    finalize_created_instance(state, &req, &id, &pending_create_token).await?;

    state
        .metrics
        .increment("workflow_instances", &[("outcome", "created")], 1.0);
    log_instance_event_from_request(state, "workflow_instance_created", &req, &id);
    if !callback.is_empty() {
        spawn_progress_from_request(
            state,
            &req,
            &id,
            now_ms,
            "workflow_instance_created",
            "queued",
            None,
        );
    }
    Ok(InstanceResponse {
        id,
        status: "queued".to_string(),
        output: None,
        error: None,
        steps: None,
    })
}

pub(crate) async fn create_batch(
    state: &AppState,
    req: WorkflowRequest,
) -> WorkflowResult<CreateBatchResponse> {
    validate_identity(&req)?;
    if req.entries.is_empty() {
        return Err(WorkflowError::invalid_request(
            "createBatch entries must be a non-empty array",
        ));
    }
    if req.entries.len() > MAX_CREATE_BATCH_SIZE {
        return Err(WorkflowError::request_too_large(format!(
            "createBatch exceeds {MAX_CREATE_BATCH_SIZE} instance limit"
        )));
    }
    let req = request_with_active_version(state, req).await?;
    validate_identity(&req)?;
    verify_workflow_def(state, &req).await?;
    let mut instances = Vec::with_capacity(req.entries.len());
    let mut seen = HashSet::new();
    for entry in &req.entries {
        if !seen.insert(entry.instance_id.clone()) {
            continue;
        }
        let child = WorkflowRequest {
            ns: req.ns.clone(),
            worker: req.worker.clone(),
            frozen_version: req.frozen_version.clone(),
            workflow_name: req.workflow_name.clone(),
            workflow_key: req.workflow_key.clone(),
            class_name: req.class_name.clone(),
            instance_id: Some(entry.instance_id.clone()),
            params: entry.params.clone(),
            event: JsonValue::Null,
            options: JsonValue::Null,
            retention: if entry.retention.is_null() {
                req.retention.clone()
            } else {
                entry.retention.clone()
            },
            callback: if entry.callback.is_null() {
                req.callback.clone()
            } else {
                entry.callback.clone()
            },
            entries: Vec::new(),
            request_id: req.request_id.clone(),
        };
        if !read_public_state(state, &child).await?.is_empty() {
            continue;
        }
        instances.push(create_instance_prevalidated(state, child).await?);
    }
    Ok(CreateBatchResponse { instances })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::pending_create::FINALIZE_CREATE_INSTANCE_SCRIPT;
    use super::*;

    #[test]
    fn create_instance_does_not_expose_ready_token_before_finalize() {
        assert!(CREATE_INSTANCE_SCRIPT.contains(r#""status", "pending_create""#));
        assert!(CREATE_INSTANCE_SCRIPT.contains(r#""pendingCreateToken", ARGV[13]"#));
        assert!(!CREATE_INSTANCE_SCRIPT.contains("wf:ready"));
        assert!(!CREATE_INSTANCE_SCRIPT.contains(r#"redis.call("SADD", KEYS[5]"#));
        assert!(FINALIZE_CREATE_INSTANCE_SCRIPT.contains(r#"status ~= "pending_create""#));
        assert!(FINALIZE_CREATE_INSTANCE_SCRIPT.contains("token ~= ARGV[3]"));
        assert!(
            FINALIZE_CREATE_INSTANCE_SCRIPT.contains(r#"redis.call("SADD", KEYS[2], ARGV[1])"#)
        );
        assert!(
            FINALIZE_CREATE_INSTANCE_SCRIPT.contains(r#"redis.call("SADD", KEYS[3], ARGV[4])"#)
        );
    }

    #[test]
    fn create_revalidates_active_export_before_finalize() {
        let active_export_source = include_str!("active_export.rs");
        let create_source = include_str!("create.rs");
        assert!(active_export_source.contains("async fn verify_active_workflow_current"));
        assert!(active_export_source.contains("active.version != req.frozen_version"));
        assert!(active_export_source.contains("active.class_name != req.class_name"));
        assert!(
            create_source
                .contains("if let Err(err) = verify_active_workflow_current(state, &req).await")
        );
        assert!(
            create_source.contains(
                "finalize_created_instance(state, &req, &id, &pending_create_token).await?"
            )
        );
    }

    #[test]
    fn create_initializes_payload_counter_from_params() {
        assert!(CREATE_INSTANCE_SCRIPT.contains(r#""paramsRef", "params""#));
        assert!(CREATE_INSTANCE_SCRIPT.contains(r#""payloadBytes", ARGV[15]"#));
    }

    #[test]
    fn create_persists_callback_only_when_present() {
        assert!(CREATE_INSTANCE_SCRIPT.contains(r#"if ARGV[16] ~= "" then"#));
        assert!(CREATE_INSTANCE_SCRIPT.contains(r#""callback", ARGV[16]"#));
    }

    #[test]
    fn callback_descriptor_accepts_only_same_worker_do_shape() {
        assert!(
            callback_json(
                &json!({"kind":"do","binding":"ROOMS","idFromName":"room-a","path":"/progress"})
            )
            .is_ok()
        );
        assert!(
            callback_json(&json!({"kind":"service","binding":"ROOMS","idFromName":"room-a"}))
                .is_err()
        );
        assert!(
            callback_json(&json!({"kind":"do","binding":"ROOMS\n","idFromName":"room-a"})).is_err()
        );
        assert!(
            callback_json(
                &json!({"kind":"do","binding":"ROOMS","idFromName":"room-a","path":"relative"})
            )
            .is_err()
        );
    }
}
