use std::collections::HashMap;
use std::time::Duration;

use serde_json::{Value as JsonValue, json};
use wdl_rust_common::internal_auth::INTERNAL_AUTH_HEADER;
use wdl_rust_common::redis_eval::StaticRedisScript;
use wdl_rust_common::time::now_ms;

use crate::{
    AppState, InstanceIdentity, WorkflowError, WorkflowResult, ready_active_key, retention_key,
};

use super::super::{
    InstanceRouteKeys, MAX_WORKFLOW_RUNTIME_RESPONSE_BYTES, RunClaim,
    WORKFLOW_PAYLOAD_TOO_LARGE_CODE, clear_suspended_run_claim, eval_script,
    instance_payload_limit_arg, log_instance_event, observe_instance_duration,
    parse_positive_identity_i64, read_state_by_id, result_json, runtime_endpoint,
    spawn_progress_from_identity, terminal_retention_ms,
};

pub(crate) const COMMIT_RUNTIME_TERMINAL_SCRIPT: &str = r#"
local generation = redis.call("HGET", KEYS[1], "generation")
local token = redis.call("HGET", KEYS[1], "runToken")
local status = redis.call("HGET", KEYS[1], "status")
if generation ~= ARGV[1] or token ~= ARGV[2] then
  return 0
end
local waiting_failed = ARGV[3] == "failed" and status == "waiting"
if status ~= "running" and not waiting_failed then
  return 0
end
local lease = tonumber(redis.call("HGET", KEYS[1], "runLeaseExpiresAtMs") or "")
if not lease then
  return 0
end
if lease <= tonumber(ARGV[10]) then
  if waiting_failed then
    redis.call("SADD", KEYS[4], ARGV[7])
    redis.call("SADD", KEYS[6], ARGV[11])
    return 3
  end
  return 0
end
local payload_bytes = tonumber(redis.call("HGET", KEYS[1], "payloadBytes") or "0")
local next_payload_bytes = payload_bytes + string.len(ARGV[4])
if next_payload_bytes > tonumber(ARGV[8]) then
  redis.call("HSET", KEYS[1],
    "status", "failed",
    "failedAtMs", ARGV[5],
    "updatedAtMs", ARGV[5],
    "retentionExpiresAtMs", ARGV[9],
    "errorCode", "workflow_payload_too_large",
    "errorMessage", "Workflow instance payload aggregate exceeds the configured limit")
  redis.call("HDEL", KEYS[1], "runToken", "runLeaseExpiresAtMs", "waitingEventIndexPrefix")
  redis.call("ZADD", KEYS[3], ARGV[9], ARGV[7])
  redis.call("SREM", KEYS[4], ARGV[7])
  redis.call("ZREM", KEYS[5], ARGV[7])
  return 2
end
if ARGV[3] == "completed" then
  redis.call("HSET", KEYS[2], "output", ARGV[4])
  redis.call("HSET", KEYS[1],
    "status", "completed",
    "outputRef", "output",
    "completedAtMs", ARGV[5],
    "updatedAtMs", ARGV[5],
    "retentionExpiresAtMs", ARGV[6],
    "payloadBytes", tostring(next_payload_bytes))
else
  redis.call("HSET", KEYS[2], "error", ARGV[4])
  redis.call("HSET", KEYS[1],
    "status", "failed",
    "errorRef", "error",
    "failedAtMs", ARGV[5],
    "updatedAtMs", ARGV[5],
    "retentionExpiresAtMs", ARGV[6],
    "payloadBytes", tostring(next_payload_bytes))
end
redis.call("HDEL", KEYS[1], "runToken", "runLeaseExpiresAtMs", "waitingEventIndexPrefix")
redis.call("ZADD", KEYS[3], ARGV[6], ARGV[7])
redis.call("SREM", KEYS[4], ARGV[7])
redis.call("ZREM", KEYS[5], ARGV[7])
return 1
"#;

static COMMIT_RUNTIME_TERMINAL: StaticRedisScript =
    StaticRedisScript::new(COMMIT_RUNTIME_TERMINAL_SCRIPT);

pub(super) enum RuntimeCommitOutcome {
    Completed,
    Failed,
    Suspended,
    Fenced,
}

#[derive(Debug, Eq, PartialEq)]
enum RuntimeResponseOutcome {
    Completed(JsonValue),
    Failed(JsonValue),
    Suspended,
}

fn runtime_response_outcome(response: &JsonValue) -> WorkflowResult<RuntimeResponseOutcome> {
    match response.get("outcome").and_then(JsonValue::as_str) {
        Some("completed") => response
            .get("output")
            .cloned()
            .map(RuntimeResponseOutcome::Completed)
            .ok_or_else(|| {
                WorkflowError::internal_error(
                    "Workflow completed runtime response is missing output",
                )
            }),
        Some("failed") => response
            .get("error")
            .cloned()
            .map(RuntimeResponseOutcome::Failed)
            .ok_or_else(|| {
                WorkflowError::internal_error("Workflow failed runtime response is missing error")
            }),
        Some("suspended") => Ok(RuntimeResponseOutcome::Suspended),
        Some(outcome) => Err(WorkflowError::internal_error(format!(
            "Workflow runtime response has unknown outcome {outcome:?}"
        ))),
        None => Err(WorkflowError::internal_error(
            "Workflow runtime response is missing outcome",
        )),
    }
}

fn current_runtime_response_outcome(
    current: &HashMap<String, String>,
    generation: &str,
    run_token: &str,
    response: &JsonValue,
) -> WorkflowResult<Option<RuntimeResponseOutcome>> {
    let current_status = current.get("status").map(String::as_str);
    if current.get("generation").map(String::as_str) != Some(generation)
        || current.get("runToken").map(String::as_str) != Some(run_token)
        || matches!(
            current_status,
            Some("paused" | "completed" | "failed" | "terminated")
        )
    {
        return Ok(None);
    }
    let outcome = runtime_response_outcome(response)?;
    if outcome == RuntimeResponseOutcome::Suspended && current_status != Some("waiting") {
        return Ok(None);
    }
    Ok(Some(outcome))
}

struct RuntimeTerminalCommit {
    state_key: String,
    payloads_key: String,
    ready_key: String,
    due_key: String,
    generation: String,
    run_token: String,
    status: &'static str,
    result_json: String,
    now: String,
    retention_expires_at: String,
    ready_token: String,
    payload_limit: String,
    overflow_retention_expires_at: String,
    ready_shard: String,
}

struct RuntimeTerminalCommitSpec {
    status: &'static str,
    result_json: String,
    retention_expires_at: String,
    overflow_retention_expires_at: String,
}

impl RuntimeTerminalCommitSpec {
    fn completed(
        result_json: String,
        retention_expires_at: &str,
        overflow_retention_expires_at: &str,
    ) -> Self {
        Self {
            status: "completed",
            result_json,
            retention_expires_at: retention_expires_at.to_string(),
            overflow_retention_expires_at: overflow_retention_expires_at.to_string(),
        }
    }

    fn failed(result_json: String, retention_expires_at: &str) -> Self {
        Self {
            status: "failed",
            result_json,
            retention_expires_at: retention_expires_at.to_string(),
            overflow_retention_expires_at: retention_expires_at.to_string(),
        }
    }
}

impl RuntimeTerminalCommit {
    fn new(
        identity: &InstanceIdentity,
        claim: &RunClaim,
        keys: &InstanceRouteKeys<'_>,
        now: &str,
        spec: RuntimeTerminalCommitSpec,
    ) -> Self {
        Self {
            state_key: keys.state(),
            payloads_key: keys.payloads(),
            ready_key: keys.ready(),
            due_key: keys.due(),
            generation: identity.generation.clone(),
            run_token: claim.token.clone(),
            status: spec.status,
            result_json: spec.result_json,
            now: now.to_string(),
            retention_expires_at: spec.retention_expires_at,
            ready_token: keys.token(),
            payload_limit: instance_payload_limit_arg(),
            overflow_retention_expires_at: spec.overflow_retention_expires_at,
            ready_shard: keys.shard().to_string(),
        }
    }

    fn keys(&self) -> [&str; 6] {
        [
            &self.state_key,
            &self.payloads_key,
            retention_key(),
            &self.ready_key,
            &self.due_key,
            ready_active_key(),
        ]
    }

    fn args(&self) -> [&str; 11] {
        [
            &self.generation,
            &self.run_token,
            self.status,
            &self.result_json,
            &self.now,
            &self.retention_expires_at,
            &self.ready_token,
            &self.payload_limit,
            &self.overflow_retention_expires_at,
            &self.now,
            &self.ready_shard,
        ]
    }
}

async fn read_runtime_response_text(
    mut response: reqwest::Response,
) -> WorkflowResult<(reqwest::StatusCode, String)> {
    let status = response.status();
    if response
        .content_length()
        .is_some_and(|len| len > MAX_WORKFLOW_RUNTIME_RESPONSE_BYTES as u64)
    {
        return Err(WorkflowError::request_too_large(
            "Workflow runtime response is too large",
        ));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|err| {
        WorkflowError::internal_error(format!("Workflow runtime response read failed: {err}"))
    })? {
        if bytes.len().saturating_add(chunk.len()) > MAX_WORKFLOW_RUNTIME_RESPONSE_BYTES {
            return Err(WorkflowError::request_too_large(
                "Workflow runtime response is too large",
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    let text = String::from_utf8(bytes).map_err(|err| {
        WorkflowError::internal_error(format!("Workflow runtime response is not UTF-8: {err}"))
    })?;
    Ok((status, text))
}

pub(super) async fn dispatch_runtime(
    app: &AppState,
    identity: &InstanceIdentity,
    run_token: &str,
    params: JsonValue,
    request_id: Option<&str>,
) -> WorkflowResult<JsonValue> {
    let url = runtime_endpoint(app, &identity.ns, "/internal/workflows/run");
    let generation = parse_positive_identity_i64(&identity.generation, "generation")?;
    let created_at_ms = parse_positive_identity_i64(&identity.created_at_ms, "createdAtMs")?;
    let mut request = app
        .http
        .post(url)
        .header(
            INTERNAL_AUTH_HEADER,
            app.config.internal_auth_tokens.current.as_str(),
        )
        .timeout(Duration::from_millis(app.config.dispatch_timeout_ms));
    if let Some(request_id) = request_id {
        request = request.header("x-request-id", request_id);
    }
    let response = request
        .json(&json!({
            "ns": identity.ns,
            "worker": identity.worker,
            "frozenVersion": identity.frozen_version,
            "workflowName": identity.workflow_name,
            "workflowKey": identity.workflow_key,
            "className": identity.class_name,
            "instanceId": identity.instance_id,
            "generation": generation,
            "createdAtMs": created_at_ms,
            "runToken": run_token,
            "params": params,
        }))
        .send()
        .await
        .map_err(|err| {
            WorkflowError::internal_error(format!("Workflow runtime dispatch failed: {err}"))
        })?;
    let (status, text) = read_runtime_response_text(response).await?;
    if !status.is_success() {
        return Err(WorkflowError::internal_error(format!(
            "Workflow runtime dispatch returned HTTP {}",
            status.as_u16()
        )));
    }
    let body: JsonValue = serde_json::from_str(&text).map_err(|err| {
        WorkflowError::internal_error(format!("Workflow runtime response is not JSON: {err}"))
    })?;
    Ok(body)
}

// Runs COMMIT_RUNTIME_TERMINAL_SCRIPT for a prepared terminal commit and returns
// its numeric verdict (1 = committed, 2 = lost-race-to-failed, other = no-op).
async fn run_terminal_commit(
    app: &AppState,
    commit: &RuntimeTerminalCommit,
) -> WorkflowResult<i64> {
    eval_script(
        app,
        &COMMIT_RUNTIME_TERMINAL,
        &commit.keys(),
        &commit.args(),
    )
    .await
}

async fn commit_runtime_failed_payload(
    app: &AppState,
    identity: &InstanceIdentity,
    claim: &RunClaim,
    current: &HashMap<String, String>,
    now_ms: i64,
    error_json: String,
) -> WorkflowResult<RuntimeCommitOutcome> {
    let keys = InstanceRouteKeys::new(&identity.ns, &identity.workflow_key, &identity.instance_id);
    let retention_ms = terminal_retention_ms(current, "failed")?;
    let retention_expires_at = now_ms.saturating_add(retention_ms).to_string();
    let now = now_ms.to_string();
    let commit = RuntimeTerminalCommit::new(
        identity,
        claim,
        &keys,
        &now,
        RuntimeTerminalCommitSpec::failed(error_json, &retention_expires_at),
    );
    let committed: i64 = run_terminal_commit(app, &commit).await?;
    if committed != 1 && committed != 2 {
        return Ok(RuntimeCommitOutcome::Fenced);
    }
    observe_instance_duration(app, current, now_ms);
    log_instance_event(app, "workflow_instance_failed", identity);
    spawn_progress_from_identity(app, identity, "workflow_instance_failed", "failed", None);
    Ok(RuntimeCommitOutcome::Failed)
}

fn runtime_payload_too_large_error(err: &WorkflowError) -> JsonValue {
    json!({
        "name": "WorkflowError",
        "code": WORKFLOW_PAYLOAD_TOO_LARGE_CODE,
        "message": err.message,
    })
}

pub(super) async fn commit_runtime_result(
    app: &AppState,
    identity: &InstanceIdentity,
    claim: &RunClaim,
    response: JsonValue,
) -> WorkflowResult<RuntimeCommitOutcome> {
    let current = read_state_by_id(
        app,
        &identity.ns,
        &identity.workflow_key,
        &identity.instance_id,
    )
    .await?;
    let Some(outcome) =
        current_runtime_response_outcome(&current, &identity.generation, &claim.token, &response)?
    else {
        return Ok(RuntimeCommitOutcome::Fenced);
    };
    let keys = InstanceRouteKeys::new(&identity.ns, &identity.workflow_key, &identity.instance_id);
    let now_ms = now_ms();
    let now = now_ms.to_string();
    if outcome == RuntimeResponseOutcome::Suspended {
        return Ok(if clear_suspended_run_claim(app, identity, claim).await? {
            RuntimeCommitOutcome::Suspended
        } else {
            RuntimeCommitOutcome::Fenced
        });
    }
    if let RuntimeResponseOutcome::Completed(output) = outcome {
        let retention_ms = terminal_retention_ms(&current, "completed")?;
        let retention_expires_at = now_ms.saturating_add(retention_ms).to_string();
        let error_retention_ms = terminal_retention_ms(&current, "failed")?;
        let error_retention_expires_at = now_ms.saturating_add(error_retention_ms).to_string();
        let output_json = match result_json(&output, "output") {
            Ok(output_json) => output_json,
            Err(err) if err.code == "request_too_large" => {
                let error_json = result_json(&runtime_payload_too_large_error(&err), "error")?;
                return commit_runtime_failed_payload(
                    app, identity, claim, &current, now_ms, error_json,
                )
                .await;
            }
            Err(err) => return Err(err),
        };
        let commit = RuntimeTerminalCommit::new(
            identity,
            claim,
            &keys,
            &now,
            RuntimeTerminalCommitSpec::completed(
                output_json,
                &retention_expires_at,
                &error_retention_expires_at,
            ),
        );
        let committed: i64 = run_terminal_commit(app, &commit).await?;
        if committed == 2 {
            observe_instance_duration(app, &current, now_ms);
            log_instance_event(app, "workflow_instance_failed", identity);
            spawn_progress_from_identity(app, identity, "workflow_instance_failed", "failed", None);
            return Ok(RuntimeCommitOutcome::Failed);
        }
        if committed != 1 {
            return Ok(RuntimeCommitOutcome::Fenced);
        }
        observe_instance_duration(app, &current, now_ms);
        log_instance_event(app, "workflow_instance_completed", identity);
        spawn_progress_from_identity(
            app,
            identity,
            "workflow_instance_completed",
            "completed",
            None,
        );
        return Ok(RuntimeCommitOutcome::Completed);
    }

    let retention_ms = terminal_retention_ms(&current, "failed")?;
    let retention_expires_at = now_ms.saturating_add(retention_ms).to_string();
    let RuntimeResponseOutcome::Failed(error) = outcome else {
        unreachable!("suspended and completed runtime outcomes returned above");
    };
    let error_json = match result_json(&error, "error") {
        Ok(error_json) => error_json,
        Err(err) if err.code == "request_too_large" => {
            let error_json = result_json(&runtime_payload_too_large_error(&err), "error")?;
            return commit_runtime_failed_payload(
                app, identity, claim, &current, now_ms, error_json,
            )
            .await;
        }
        Err(err) => return Err(err),
    };
    let commit = RuntimeTerminalCommit::new(
        identity,
        claim,
        &keys,
        &now,
        RuntimeTerminalCommitSpec::failed(error_json, &retention_expires_at),
    );
    let committed: i64 = run_terminal_commit(app, &commit).await?;
    if committed == 2 {
        observe_instance_duration(app, &current, now_ms);
        log_instance_event(app, "workflow_instance_failed", identity);
        spawn_progress_from_identity(app, identity, "workflow_instance_failed", "failed", None);
        return Ok(RuntimeCommitOutcome::Failed);
    }
    if committed != 1 {
        return Ok(RuntimeCommitOutcome::Fenced);
    }
    observe_instance_duration(app, &current, now_ms);
    log_instance_event(app, "workflow_instance_failed", identity);
    spawn_progress_from_identity(app, identity, "workflow_instance_failed", "failed", None);
    Ok(RuntimeCommitOutcome::Failed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_response_outcome_accepts_only_the_terminal_protocol_values() {
        let fixture: JsonValue = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/workflow-runtime-response.json"
        ))
        .expect("workflow runtime response fixture parses");
        let outcomes = &fixture["runtimeOutcomes"];
        let payload_fields = &fixture["terminalPayloadFields"];
        let completed_field = payload_fields["completed"]
            .as_str()
            .expect("completed payload field is a string");
        let failed_field = payload_fields["failed"]
            .as_str()
            .expect("failed payload field is a string");
        let mut completed = json!({ "outcome": outcomes["completed"] });
        completed
            .as_object_mut()
            .expect("completed response is an object")
            .insert(completed_field.to_string(), JsonValue::Null);
        assert_eq!(
            runtime_response_outcome(&completed).unwrap(),
            RuntimeResponseOutcome::Completed(JsonValue::Null)
        );
        let mut failed = json!({ "outcome": outcomes["failed"] });
        failed
            .as_object_mut()
            .expect("failed response is an object")
            .insert(failed_field.to_string(), JsonValue::Null);
        assert_eq!(
            runtime_response_outcome(&failed).unwrap(),
            RuntimeResponseOutcome::Failed(JsonValue::Null)
        );
        assert_eq!(
            runtime_response_outcome(&json!({ "outcome": outcomes["suspended"] })).unwrap(),
            RuntimeResponseOutcome::Suspended
        );

        for response in [
            json!({}),
            json!({ "outcome": null }),
            json!({ "outcome": "error" }),
            json!({ "outcome": 1 }),
            json!({ "outcome": outcomes["completed"] }),
            json!({ "outcome": outcomes["failed"] }),
        ] {
            let err = runtime_response_outcome(&response)
                .expect_err("missing or unknown runtime outcomes must fail closed");
            assert_eq!(err.code, "internal_error");
        }

        let retryable = &fixture["retryableBackendErrors"];
        let redis_error = WorkflowError::from(redis::RedisError::from((
            redis::ErrorKind::Io,
            "test Redis failure",
        )));
        for (name, error) in [
            (
                "internal",
                WorkflowError::internal_error("test internal failure"),
            ),
            ("redis", redis_error),
            (
                "unavailable",
                WorkflowError::unavailable("test unavailable failure"),
            ),
        ] {
            assert_eq!(retryable[name]["code"].as_str(), Some(error.code));
            assert_eq!(
                retryable[name]["status"].as_u64(),
                Some(u64::from(error.status.as_u16()))
            );
        }
    }

    #[test]
    fn runtime_response_outcome_is_parsed_only_for_the_current_run() {
        let malformed = json!({});
        let current = HashMap::from([
            ("generation".to_string(), "generation-1".to_string()),
            ("runToken".to_string(), "run-token-1".to_string()),
            ("status".to_string(), "running".to_string()),
        ]);

        assert!(
            current_runtime_response_outcome(&current, "generation-1", "run-token-1", &malformed,)
                .is_err()
        );
        for stale in [
            HashMap::from([
                ("generation".to_string(), "generation-2".to_string()),
                ("runToken".to_string(), "run-token-1".to_string()),
                ("status".to_string(), "running".to_string()),
            ]),
            HashMap::from([
                ("generation".to_string(), "generation-1".to_string()),
                ("runToken".to_string(), "run-token-2".to_string()),
                ("status".to_string(), "running".to_string()),
            ]),
            HashMap::from([
                ("generation".to_string(), "generation-1".to_string()),
                ("runToken".to_string(), "run-token-1".to_string()),
                ("status".to_string(), "paused".to_string()),
            ]),
        ] {
            assert_eq!(
                current_runtime_response_outcome(
                    &stale,
                    "generation-1",
                    "run-token-1",
                    &malformed,
                )
                .unwrap(),
                None
            );
        }
    }

    #[test]
    fn suspended_runtime_response_requires_authoritative_waiting_state() {
        let response = json!({ "outcome": "suspended" });
        let mut current = HashMap::from([
            ("generation".to_string(), "generation-1".to_string()),
            ("runToken".to_string(), "run-token-1".to_string()),
            (
                "runLeaseExpiresAtMs".to_string(),
                "9999999999999".to_string(),
            ),
            ("status".to_string(), "running".to_string()),
        ]);

        assert_eq!(
            current_runtime_response_outcome(&current, "generation-1", "run-token-1", &response,)
                .unwrap(),
            None
        );
        assert_eq!(current.get("status").map(String::as_str), Some("running"));
        assert_eq!(
            current.get("runToken").map(String::as_str),
            Some("run-token-1")
        );
        assert_eq!(
            current.get("runLeaseExpiresAtMs").map(String::as_str),
            Some("9999999999999")
        );

        current.insert("status".to_string(), "waiting".to_string());
        assert_eq!(
            current_runtime_response_outcome(&current, "generation-1", "run-token-1", &response,)
                .unwrap(),
            Some(RuntimeResponseOutcome::Suspended)
        );
    }
}
