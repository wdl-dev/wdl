use std::collections::HashMap;

use axum::body::{Body, to_bytes};
use serde::de::DeserializeOwned;

use crate::{AppState, WorkflowError, WorkflowResult, workflow_defs_key};

mod active_export;
mod create;
mod do_alarms;
mod execution;
mod identity;
mod lease;
mod lifecycle;
mod limits;
mod model;
mod payload;
mod pending_create;
mod pending_restart;
mod progress;
mod redis_script;
mod retention;
mod routing;
mod sharded_dispatch;
mod status;
mod tick;
use active_export::{
    ensure_worker_not_deleting, request_with_active_version, verify_active_workflow_current,
    verify_workflow_def,
};
pub(crate) use create::{create_batch, create_instance};
pub(crate) use do_alarms::{
    DoAlarmCleanupRequest, DoAlarmDeleteRequest, DoAlarmSetRequest, cleanup_do_alarms_for_worker,
    delete_do_alarm, dispatch_ready_do_alarms, set_do_alarm,
};
use execution::{StepHistory, read_step_history, workflow_step_options};
pub(crate) use execution::{
    claim_step, commit_step_error, commit_step_success, read_replay_step_page,
    read_workflow_replay_request, read_workflow_step_request, register_sleep, register_wait,
    send_event,
};
#[cfg(test)]
pub(crate) use identity::validate_instance_id_value;
use identity::{
    identity_from_state, instance_id, log_instance_event, log_instance_event_from_request,
    parse_positive_identity_i64, require_non_empty, validate_identity,
};
use lease::{
    RunClaim, claim_run, clear_suspended_run_claim, release_run_claim, requeue_expired_run_claim,
};
pub(crate) use lifecycle::{
    check_delete_lifecycle, pause_instance, restart_instance, resume_instance, terminate_instance,
};
use limits::{
    LIFECYCLE_BLOCKER_LIMIT, MAX_CREATE_BATCH_SIZE, MAX_WORKFLOW_EVENT_BYTES,
    MAX_WORKFLOW_EVENT_TYPE_BYTES, MAX_WORKFLOW_INSTANCE_PAYLOAD_BYTES,
    MAX_WORKFLOW_JSON_BODY_BYTES, MAX_WORKFLOW_PARAMS_BYTES, MAX_WORKFLOW_RESULT_BYTES,
    MAX_WORKFLOW_RUNTIME_RESPONSE_BYTES, MAX_WORKFLOW_STEP_CONFIG_BYTES,
    MAX_WORKFLOW_STEP_NAME_BYTES, READY_SHARDS, WORKFLOW_PAYLOAD_TOO_LARGE_CODE,
};
pub(crate) use model::{
    CreateBatchResponse, EventRecord, InstanceIdentity, InstanceResponse, LifecycleBlocker,
    LifecycleCheckRequest, LifecycleCheckResponse, ListInstancesResponse, WorkflowDef,
    WorkflowRequest,
};
use payload::{
    aggregate_payload_error, event_payload_json, event_type_from_value, instance_payload_limit_arg,
    params_json, payload_bytes_arg, read_payload_ref, result_json,
};
use pending_create::{
    PENDING_CREATE_TTL_MS, PendingCreateCleanup, cleanup_created_instance,
    cleanup_pending_create_identity, finalize_created_instance, is_pending_create,
    pending_create_cleanup_from_state, pending_create_expired, pending_create_token,
    public_state_or_empty, wait_for_public_create_state,
};
use pending_restart::{
    PendingRestartMarker, active_pending_restart_blockers, create_pending_restart,
    pending_restart_marker, remove_pending_restart,
};
use progress::{
    spawn_progress_from_identity, spawn_progress_from_request, spawn_progress_from_step,
};
use redis_script::eval_script;
use retention::{retention_policy, terminal_retention_ms};
use routing::{
    InstanceRouteKeys, bundle_key, parse_ready_token, parse_workflow_referrer_member,
    workflow_referrer_member,
};
use sharded_dispatch::{
    DuePromotionConfig, DuePromotionMember, ReadyDispatchConfig, ReadyMemberOutcome,
    dispatch_ready_members, due_shards_with_due_members, promote_due_members,
    remove_ready_member_if_state_missing,
};
pub(crate) use status::{get_instance, list_instances, status_instance};
use status::{
    read_public_state, read_public_state_by_id, read_state, read_state_by_id, response_from_state,
};
pub(crate) use tick::tick_workflows;

fn runtime_endpoint(app: &AppState, ns: &str, path: &str) -> String {
    let (host, port) = if ns == "__system__" {
        (
            &app.config.system_runtime_host,
            app.config.system_runtime_port,
        )
    } else {
        (&app.config.runtime_host, app.config.runtime_port)
    };
    format!("http://{host}:{port}{path}")
}

#[cfg(test)]
pub(crate) use execution::{
    COMMIT_STEP_ERROR_SCRIPT, COMMIT_STEP_RECORD_SCRIPT, COMMIT_STEP_SUCCESS_SCRIPT,
    SEND_EVENT_SCRIPT, canonical_json, retry_due_at_ms, retry_policy,
};
#[cfg(test)]
pub(crate) use tick::COMMIT_RUNTIME_TERMINAL_SCRIPT;

fn observe_instance_duration(state: &AppState, record: &HashMap<String, String>, ended_at_ms: i64) {
    if let Some(created_at_ms) = record
        .get("createdAtMs")
        .and_then(|raw| raw.parse::<i64>().ok())
    {
        let duration_ms = ended_at_ms.saturating_sub(created_at_ms).max(0) as f64;
        state
            .metrics
            .observe("workflow_instance_duration_ms", &[], duration_ms);
    }
}

fn emit_outcome_counts(app: &AppState, metric: &'static str, outcomes: &[(&'static str, usize)]) {
    for (outcome, count) in outcomes {
        if *count > 0 {
            app.metrics
                .increment(metric, &[("outcome", *outcome)], *count as f64);
        }
    }
}

async fn read_json_request<T: DeserializeOwned>(
    body: Body,
    invalid_json_message: &'static str,
) -> WorkflowResult<T> {
    let bytes = to_bytes(body, MAX_WORKFLOW_JSON_BODY_BYTES)
        .await
        .map_err(|_| WorkflowError::request_too_large("Workflow request body is too large"))?;
    serde_json::from_slice(&bytes)
        .map_err(|err| WorkflowError::invalid_request(format!("{invalid_json_message}: {err}")))
}

pub(crate) async fn read_workflow_request(body: Body) -> WorkflowResult<WorkflowRequest> {
    read_json_request(body, "Invalid workflow request JSON").await
}

pub(crate) async fn read_do_alarm_set_request(body: Body) -> WorkflowResult<DoAlarmSetRequest> {
    read_json_request(body, "Invalid DO alarm set JSON").await
}

pub(crate) async fn read_do_alarm_delete_request(
    body: Body,
) -> WorkflowResult<DoAlarmDeleteRequest> {
    read_json_request(body, "Invalid DO alarm delete JSON").await
}

pub(crate) async fn read_do_alarm_cleanup_request(
    body: Body,
) -> WorkflowResult<DoAlarmCleanupRequest> {
    read_json_request(body, "Invalid DO alarm cleanup JSON").await
}

pub(crate) async fn read_lifecycle_check_request(
    body: Body,
) -> WorkflowResult<LifecycleCheckRequest> {
    read_json_request(body, "Invalid lifecycle check JSON").await
}
