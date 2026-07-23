use serde::Serialize;
use serde_json::{Value as JsonValue, json};
use std::collections::HashMap;
use wdl_rust_common::redis_eval::StaticRedisScript;

use crate::{
    AppState, DispatchTaskUnavailable, InstanceIdentity, LogLevel, Metrics,
    WORKFLOW_READY_BATCH_SIZE, WorkflowError, WorkflowResult, fields_with_error, log,
    workflow_shard_queue_keys,
};

use super::payload::{parse_payload_ref, payload_storage_key_for_ref};
use super::{
    InstanceRouteKeys, ReadyAdmissionConfig, ReadyAdmissionOutcome, RunClaim,
    admit_ready_do_alarms, admit_ready_members, claim_run, eval_script, identity_from_state,
    log_instance_event, parse_ready_token, release_run_claim, requeue_expired_run_claim,
    spawn_progress_from_identity,
};

mod dispatch;
mod ready;
mod retention;
#[cfg(test)]
pub(crate) use dispatch::COMMIT_RUNTIME_TERMINAL_SCRIPT;
use dispatch::{RuntimeCommitOutcome, commit_runtime_result, dispatch_runtime};
use ready::{
    ReadyTokenGuard, ReadyTokenIdentity, move_due_tokens, remove_ready_token,
    remove_ready_token_if_state_missing, remove_ready_token_if_terminal,
};
use retention::cleanup_retention;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TickResponse {
    pub(crate) workflow_admitted: usize,
    pub(crate) workflow_capacity_blocked: bool,
    pub(crate) due_moved: usize,
    pub(crate) retention_cleaned: usize,
    pub(crate) do_alarm_due_moved: usize,
    pub(crate) do_alarm_admitted: usize,
    pub(crate) do_alarm_capacity_blocked: bool,
}

struct ClaimedWorkflowRun {
    identity: InstanceIdentity,
    params: JsonValue,
    previous_status: String,
    claim: RunClaim,
    request_id: Option<String>,
}

struct WorkflowDispatchGaugeGuard<'a> {
    metrics: &'a Metrics,
}

impl<'a> WorkflowDispatchGaugeGuard<'a> {
    fn begin(metrics: &'a Metrics) -> Self {
        metrics.add_gauge("workflow_dispatch_in_flight", &[], 1.0);
        Self { metrics }
    }
}

impl Drop for WorkflowDispatchGaugeGuard<'_> {
    fn drop(&mut self) {
        self.metrics
            .add_gauge("workflow_dispatch_in_flight", &[], -1.0);
    }
}

enum ReadyTokenAdmission {
    Dispatch(Box<ClaimedWorkflowRun>),
    Immediate(ReadyTokenResult),
}

enum ReadyTokenResult {
    Completed,
    Failed,
    DispatchError,
    Suspended,
    Fenced,
    RemoveMalformed,
    RemoveIfStateMissing(ReadyTokenIdentity),
    RemoveIfTerminal(ReadyTokenGuard),
    Keep,
}

const READ_READY_STATE_AND_PARAMS_SCRIPT: &str = r#"
local state = redis.call("HGETALL", KEYS[1])
local status = redis.call("HGET", KEYS[1], "status")
if status ~= "queued" and status ~= "waiting" then
  return {state, false}
end
local params_ref = redis.call("HGET", KEYS[1], "paramsRef")
if not params_ref then
  return {state, false}
end
local payloads_key = redis.call("HGET", KEYS[1], "payloadsKey")
if payloads_key ~= KEYS[2] then
  return {state, false}
end
return {state, redis.call("HGET", KEYS[2], params_ref)}
"#;

static READ_READY_STATE_AND_PARAMS: StaticRedisScript =
    StaticRedisScript::new(READ_READY_STATE_AND_PARAMS_SCRIPT);

fn params_from_ready_snapshot(
    state: &HashMap<String, String>,
    expected_payloads_key: &str,
    raw_params: Option<String>,
) -> WorkflowResult<JsonValue> {
    let Some(params_ref) = state.get("paramsRef") else {
        return Ok(JsonValue::Null);
    };
    let payloads_key = payload_storage_key_for_ref(state, params_ref)?;
    if payloads_key != expected_payloads_key {
        return Err(WorkflowError::invalid_state(
            "Workflow payload storage key is invalid",
        ));
    }
    Ok(parse_payload_ref(raw_params, params_ref)?.unwrap_or(JsonValue::Null))
}

async fn read_ready_state_and_params(
    app: &AppState,
    ns: &str,
    workflow_key: &str,
    instance_id: &str,
) -> WorkflowResult<(HashMap<String, String>, JsonValue)> {
    let keys = InstanceRouteKeys::new(ns, workflow_key, instance_id);
    let state_key = keys.state();
    let payloads_key = keys.payloads();
    let (state, raw_params): (HashMap<String, String>, Option<String>) = eval_script(
        app,
        &READ_READY_STATE_AND_PARAMS,
        &[&state_key, &payloads_key],
        &[],
    )
    .await?;
    let params = match state.get("status").map(String::as_str) {
        Some("queued" | "waiting") => {
            params_from_ready_snapshot(&state, &payloads_key, raw_params)?
        }
        _ => JsonValue::Null,
    };
    Ok((state, params))
}

async fn prepare_ready_token(
    app: &AppState,
    token: String,
    request_id: Option<&str>,
) -> WorkflowResult<ReadyTokenAdmission> {
    let Some((ns, workflow_key, instance_id)) = parse_ready_token(&token) else {
        return Ok(ReadyTokenAdmission::Immediate(
            ReadyTokenResult::RemoveMalformed,
        ));
    };
    let (state, params) =
        read_ready_state_and_params(app, &ns, &workflow_key, &instance_id).await?;
    let Some(status) = state.get("status").map(String::as_str) else {
        return Ok(ReadyTokenAdmission::Immediate(
            ReadyTokenResult::RemoveIfStateMissing(ReadyTokenIdentity {
                ns,
                workflow_key,
                instance_id,
            }),
        ));
    };
    if status == "running" {
        let identity = identity_from_state(&ns, &workflow_key, &instance_id, &state)?;
        requeue_expired_run_claim(app, &identity).await?;
        return Ok(ReadyTokenAdmission::Immediate(ReadyTokenResult::Keep));
    }
    if status != "queued" && status != "waiting" {
        return Ok(ReadyTokenAdmission::Immediate(match status {
            "completed" | "failed" | "terminated" => {
                let identity = identity_from_state(&ns, &workflow_key, &instance_id, &state)?;
                ReadyTokenResult::RemoveIfTerminal(ReadyTokenGuard {
                    ns,
                    workflow_key,
                    instance_id,
                    generation: identity.generation,
                })
            }
            _ => ReadyTokenResult::Keep,
        }));
    }
    let identity = identity_from_state(&ns, &workflow_key, &instance_id, &state)?;
    let previous_status = status.to_string();
    let Some(claim) = claim_run(app, &identity, &previous_status).await? else {
        return Ok(ReadyTokenAdmission::Immediate(ReadyTokenResult::Keep));
    };
    log_instance_event(app, "workflow_instance_started", &identity);
    spawn_progress_from_identity(app, &identity, "workflow_instance_started", "running", None);
    Ok(ReadyTokenAdmission::Dispatch(Box::new(
        ClaimedWorkflowRun {
            identity,
            params,
            previous_status,
            claim,
            request_id: request_id.map(str::to_string),
        },
    )))
}

async fn finish_claimed_workflow(
    app: &AppState,
    run: ClaimedWorkflowRun,
) -> WorkflowResult<ReadyTokenResult> {
    let ClaimedWorkflowRun {
        identity,
        params,
        previous_status,
        claim,
        request_id,
    } = run;
    let _gauge = WorkflowDispatchGaugeGuard::begin(&app.metrics);
    let response =
        match dispatch_runtime(app, &identity, &claim.token, params, request_id.as_deref()).await {
            Ok(response) => response,
            Err(err) => {
                app.metrics
                    .increment("workflow_dispatches", &[("outcome", "error")], 1.0);
                release_run_claim(app, &identity, &claim, &previous_status).await?;
                log_dispatch_error(app, &identity, &err);
                return Ok(ReadyTokenResult::DispatchError);
            }
        };
    let outcome = match commit_runtime_result(app, &identity, &claim, response).await {
        Ok(outcome) => outcome,
        Err(err) => {
            app.metrics
                .increment("workflow_dispatches", &[("outcome", "error")], 1.0);
            if err.code == "redis_error" {
                return Err(err);
            }
            release_run_claim(app, &identity, &claim, &previous_status).await?;
            log_dispatch_error(app, &identity, &err);
            return Ok(ReadyTokenResult::DispatchError);
        }
    };
    Ok(match outcome {
        RuntimeCommitOutcome::Completed => ReadyTokenResult::Completed,
        RuntimeCommitOutcome::Failed => ReadyTokenResult::Failed,
        RuntimeCommitOutcome::Suspended => ReadyTokenResult::Suspended,
        RuntimeCommitOutcome::Fenced => ReadyTokenResult::Fenced,
    })
}

async fn process_ready_workflow_token(
    app: &AppState,
    shard: usize,
    token: String,
    request_id: Option<&str>,
) -> WorkflowResult<ReadyAdmissionOutcome<usize>> {
    let guard = match app.begin_dispatch_task(&app.dispatch.workflow) {
        Ok(guard) => guard,
        Err(DispatchTaskUnavailable::Stopping) => {
            return Ok(ReadyAdmissionOutcome::stop_after_current_batch(0));
        }
        Err(DispatchTaskUnavailable::AtCapacity) => {
            return Ok(ReadyAdmissionOutcome::capacity_unavailable(0));
        }
    };
    match prepare_ready_token(app, token.clone(), request_id).await? {
        ReadyTokenAdmission::Dispatch(run) => {
            let identity = run.identity.clone();
            let panic_fields = json!({
                "namespace": identity.ns,
                "worker": identity.worker,
                "workflow_name": identity.workflow_name,
                "workflow_key": identity.workflow_key,
                "instance_id": identity.instance_id,
                "generation": identity.generation,
            });
            let state = app.clone();
            app.spawn_tracked(
                guard,
                "workflow_dispatch_task_panicked",
                panic_fields,
                async move {
                    match finish_claimed_workflow(&state, *run).await {
                        Ok(result) => {
                            if let Err(err) =
                                apply_ready_token_result(&state, shard, token, result).await
                            {
                                log_dispatch_error(&state, &identity, &err);
                            }
                        }
                        Err(err) => log_dispatch_error(&state, &identity, &err),
                    }
                },
            );
            Ok(ReadyAdmissionOutcome::admitted(1))
        }
        ReadyTokenAdmission::Immediate(result) => {
            drop(guard);
            apply_ready_token_result(app, shard, token, result).await?;
            Ok(ReadyAdmissionOutcome::capacity_released(0))
        }
    }
}

fn merge_admitted(target: &mut usize, delta: usize) {
    *target += delta;
}

fn log_dispatch_error(app: &AppState, identity: &InstanceIdentity, err: &WorkflowError) {
    log(
        app,
        LogLevel::Warn,
        "workflow_dispatch_error",
        fields_with_error(
            json!({
                "namespace": identity.ns,
                "worker": identity.worker,
                "workflow_name": identity.workflow_name,
                "workflow_key": identity.workflow_key,
                "workflow_class": identity.class_name,
                "instance_id": identity.instance_id,
                "frozen_version": identity.frozen_version,
                "generation": identity.generation,
                "error_code": err.code,
            }),
            "Error",
            &err.message,
        ),
    );
}

pub(crate) async fn tick_workflows(
    app: &AppState,
    request_id: Option<&str>,
) -> WorkflowResult<TickResponse> {
    let due_moved = move_due_tokens(app).await?;
    let retention_cleaned = cleanup_retention(app).await?;
    let workflow_admission = admit_ready_members(
        app,
        workflow_shard_queue_keys(),
        ReadyAdmissionConfig {
            batch_size: WORKFLOW_READY_BATCH_SIZE,
            concurrency: app.config.ready_dispatch_concurrency,
            prune_on_error: false,
        },
        0,
        |shard, token| process_ready_workflow_token(app, shard, token, request_id),
        merge_admitted,
    );
    let (result, do_alarm_result) = tokio::join!(workflow_admission, admit_ready_do_alarms(app));
    let do_alarm_counters = match do_alarm_result {
        Ok(result) => {
            if let Some(err) = &result.error {
                log(
                    app,
                    LogLevel::Warn,
                    "do_alarm_tick_error",
                    fields_with_error(json!({ "error_code": err.code }), "Error", &err.message),
                );
            }
            result
        }
        Err(err) => {
            log(
                app,
                LogLevel::Warn,
                "do_alarm_tick_error",
                fields_with_error(json!({ "error_code": err.code }), "Error", &err.message),
            );
            Default::default()
        }
    };
    let result = result?;
    if let Some(err) = result.error {
        return Err(err);
    }
    if due_moved > 0 {
        app.metrics.increment(
            "workflow_due_claims",
            &[("outcome", "moved")],
            due_moved as f64,
        );
    }
    Ok(TickResponse {
        workflow_admitted: result.counters,
        workflow_capacity_blocked: result.capacity_blocked,
        due_moved,
        retention_cleaned,
        do_alarm_due_moved: do_alarm_counters.due_moved,
        do_alarm_admitted: do_alarm_counters.admitted,
        do_alarm_capacity_blocked: do_alarm_counters.capacity_blocked,
    })
}

fn workflow_dispatch_metric_outcome(result: &ReadyTokenResult) -> Option<&'static str> {
    match result {
        ReadyTokenResult::Completed => Some("completed"),
        ReadyTokenResult::Failed => Some("failed"),
        ReadyTokenResult::Suspended => Some("suspended"),
        ReadyTokenResult::Fenced => Some("fenced"),
        _ => None,
    }
}

async fn apply_ready_token_result(
    app: &AppState,
    shard: usize,
    token: String,
    result: ReadyTokenResult,
) -> WorkflowResult<()> {
    if let Some(outcome) = workflow_dispatch_metric_outcome(&result) {
        app.metrics
            .increment("workflow_dispatches", &[("outcome", outcome)], 1.0);
    }
    match result {
        ReadyTokenResult::Completed
        | ReadyTokenResult::Failed
        | ReadyTokenResult::DispatchError
        | ReadyTokenResult::Suspended
        | ReadyTokenResult::Fenced => {}
        ReadyTokenResult::RemoveMalformed => {
            remove_ready_token(app, shard, token).await?;
        }
        ReadyTokenResult::RemoveIfStateMissing(identity) => {
            remove_ready_token_if_state_missing(app, shard, token, identity).await?;
        }
        ReadyTokenResult::RemoveIfTerminal(guard) => {
            remove_ready_token_if_terminal(app, shard, token, guard).await?;
        }
        ReadyTokenResult::Keep => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::InstanceKeys;

    #[test]
    fn ready_snapshot_validates_params_before_claim() {
        let payloads_key = InstanceKeys::new("ns", "key", "inst").payloads();
        let state = HashMap::from([
            ("paramsRef".to_string(), "params-1".to_string()),
            ("payloadsKey".to_string(), payloads_key.clone()),
        ]);

        assert_eq!(
            params_from_ready_snapshot(&state, &payloads_key, Some(r#"{"ok":true}"#.to_string()))
                .expect("valid params"),
            json!({ "ok": true })
        );
        let err = params_from_ready_snapshot(&state, &payloads_key, None)
            .expect_err("missing params must fail before claim");
        assert_eq!(err.code, "workflow_payload_missing");
    }

    #[test]
    fn ready_snapshot_preserves_null_params_and_rejects_wrong_payload_key() {
        assert_eq!(
            params_from_ready_snapshot(&HashMap::new(), "payloads", None)
                .expect("params are optional"),
            JsonValue::Null
        );

        let state = HashMap::from([
            ("paramsRef".to_string(), "params-1".to_string()),
            ("payloadsKey".to_string(), "other-payloads".to_string()),
        ]);
        let err = params_from_ready_snapshot(&state, "expected-payloads", Some("null".to_string()))
            .expect_err("noncanonical payload storage must fail closed");
        assert_eq!(err.code, "workflow_invalid_state");
    }

    #[test]
    fn ready_snapshot_script_is_read_only() {
        assert!(READ_READY_STATE_AND_PARAMS_SCRIPT.contains(r#"redis.call("HGETALL", KEYS[1])"#));
        assert!(READ_READY_STATE_AND_PARAMS_SCRIPT.contains(r#"redis.call("HGET", KEYS[2]"#));
        for write in ["HSET", "HDEL", "SADD", "SREM", "ZADD", "ZREM", "DEL"] {
            assert!(
                !READ_READY_STATE_AND_PARAMS_SCRIPT.contains(&format!(r#"redis.call("{write}""#)),
                "ready snapshot must not execute {write}"
            );
        }
    }

    #[test]
    fn workflow_dispatch_gauge_releases_when_dispatch_unwinds() {
        let metrics = Metrics::default();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = WorkflowDispatchGaugeGuard::begin(&metrics);
            panic!("simulated workflow dispatch panic");
        }));

        assert!(result.is_err());
        assert!(
            metrics
                .render_prometheus()
                .contains("wdl_workflow_dispatch_in_flight 0")
        );
    }

    #[test]
    fn tick_response_serializes_admission_maintenance_and_capacity_pressure() {
        assert_eq!(
            serde_json::to_value(TickResponse {
                workflow_admitted: 2,
                workflow_capacity_blocked: true,
                due_moved: 3,
                retention_cleaned: 4,
                do_alarm_due_moved: 5,
                do_alarm_admitted: 6,
                do_alarm_capacity_blocked: false,
            })
            .unwrap(),
            json!({
                "workflowAdmitted": 2,
                "workflowCapacityBlocked": true,
                "dueMoved": 3,
                "retentionCleaned": 4,
                "doAlarmDueMoved": 5,
                "doAlarmAdmitted": 6,
                "doAlarmCapacityBlocked": false,
            })
        );
    }

    #[test]
    fn workflow_dispatch_metrics_distinguish_suspension_from_fenced_noop() {
        assert_eq!(
            workflow_dispatch_metric_outcome(&ReadyTokenResult::Suspended),
            Some("suspended")
        );
        assert_eq!(
            workflow_dispatch_metric_outcome(&ReadyTokenResult::Fenced),
            Some("fenced")
        );
    }

    #[test]
    fn terminal_commit_removes_ready_token_inside_fenced_script() {
        assert!(COMMIT_RUNTIME_TERMINAL_SCRIPT.contains(r#"redis.call("SREM", KEYS[4], ARGV[7])"#));
    }

    #[test]
    fn terminal_commit_removes_due_token_inside_fenced_script() {
        assert!(COMMIT_RUNTIME_TERMINAL_SCRIPT.contains(r#"redis.call("ZREM", KEYS[5], ARGV[7])"#));
    }

    #[test]
    fn terminal_payload_cap_failure_is_terminal_not_retried() {
        assert!(
            COMMIT_RUNTIME_TERMINAL_SCRIPT.contains(r#""errorCode", "workflow_payload_too_large""#)
        );
        assert!(
            COMMIT_RUNTIME_TERMINAL_SCRIPT
                .contains(r#"redis.call("HDEL", KEYS[1], "runToken", "runLeaseExpiresAtMs", "waitingEventIndexPrefix")"#)
        );
        assert!(COMMIT_RUNTIME_TERMINAL_SCRIPT.contains("return 2"));
    }

    #[test]
    fn terminal_commit_rejects_expired_run_claims() {
        assert!(COMMIT_RUNTIME_TERMINAL_SCRIPT.contains(r#""runLeaseExpiresAtMs""#));
        assert!(COMMIT_RUNTIME_TERMINAL_SCRIPT.contains(r#"if lease <= tonumber(ARGV[10]) then"#));
        assert!(COMMIT_RUNTIME_TERMINAL_SCRIPT.contains(r#"if waiting_failed then"#));
        assert!(COMMIT_RUNTIME_TERMINAL_SCRIPT.contains(r#"redis.call("SADD", KEYS[4], ARGV[7])"#));
        assert!(
            COMMIT_RUNTIME_TERMINAL_SCRIPT.contains(r#"redis.call("SADD", KEYS[6], ARGV[11])"#)
        );
        assert!(COMMIT_RUNTIME_TERMINAL_SCRIPT.contains(r#"return 3"#));
    }

    #[test]
    fn ready_admission_uses_shared_runner_without_error_prune() {
        let source = include_str!("tick.rs");
        let implementation = source
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .expect("tick implementation should precede tests");

        assert!(implementation.contains("admit_ready_members("));
        assert!(implementation.contains("workflow_shard_queue_keys()"));
        assert!(implementation.contains("prune_on_error: false"));
    }

    #[test]
    fn running_ready_tokens_requeue_expired_claims_before_runtime_dispatch() {
        let source = include_str!("tick.rs");
        let implementation = source
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .expect("tick implementation should precede tests");
        let running_branch = implementation
            .find(r#"if status == "running" {"#)
            .expect("ready-token dispatch must branch on running instances");
        let branch_source = &implementation[running_branch..];
        let requeue_pos = branch_source
            .find("requeue_expired_run_claim(app, &identity).await?")
            .expect("running instances must first try expired-claim requeue");
        let keep_pos = branch_source
            .find("return Ok(ReadyTokenAdmission::Immediate(ReadyTokenResult::Keep));")
            .expect("running branch must stop before runtime dispatch");
        let claim_pos = branch_source
            .find("claim_run(app, &identity, &previous_status)")
            .expect("queued/waiting instances still claim after the running branch");

        assert!(requeue_pos < keep_pos);
        assert!(keep_pos < claim_pos);
        let finish_source = implementation
            .split("async fn finish_claimed_workflow")
            .nth(1)
            .expect("claimed workflow execution must have a completion phase");
        assert!(finish_source.contains("match dispatch_runtime("));
    }

    #[test]
    fn failed_terminal_commit_can_override_same_run_waiting_state() {
        assert!(
            COMMIT_RUNTIME_TERMINAL_SCRIPT
                .contains(r#"local waiting_failed = ARGV[3] == "failed" and status == "waiting""#)
        );
        assert!(
            COMMIT_RUNTIME_TERMINAL_SCRIPT
                .contains(r#"if status ~= "running" and not waiting_failed then"#)
        );
    }
}
