mod events;
mod history;
mod model;
mod retry;
mod sleep;
use serde_json::{Value as JsonValue, json};
use wdl_rust_common::{redis_eval::StaticRedisScript, time::now_ms};

use crate::{AppState, LogLevel, WorkflowError, WorkflowResult, log};

use super::{
    InstanceRouteKeys, aggregate_payload_error, eval_script, instance_payload_limit_arg,
    result_json, spawn_progress_from_step,
};
#[cfg(test)]
pub(crate) use events::SEND_EVENT_SCRIPT;
pub(crate) use events::{register_wait, send_event};
pub(crate) use history::StepHistory;
pub(crate) use history::{read_replay_step_page, read_workflow_replay_request};
pub(super) use history::{read_step_history, workflow_step_options};
#[cfg(test)]
pub(crate) use model::canonical_json;
use model::{
    StepRecord, StepSummary, inline_step_payload, log_step_event, observe_step_duration,
    request_attempt, step_error_ref, step_event_ref, step_field, step_output_ref, step_record_json,
    step_summary_json, validate_step_request, verify_step_record,
};
pub(crate) use model::{WorkflowStepRequest, read_workflow_step_request};
pub(crate) use retry::{retry_due_at_ms, retry_policy};
pub(crate) use sleep::register_sleep;

const STEP_SCRIPT_STALE_CLAIM: i64 = 0;
const STEP_SCRIPT_OK: i64 = 1;
const STEP_SCRIPT_PAYLOAD_LIMIT: i64 = -1;
const STEP_SCRIPT_EXPIRED_CLAIM: i64 = -2;
const STEP_SCRIPT_INACTIVE_INSTANCE: i64 = -3;
const STEP_SCRIPT_CORRUPT_CLAIM: i64 = -4;

static READ_STEP_RECORD: StaticRedisScript = StaticRedisScript::new(READ_STEP_RECORD_SCRIPT);
static COMMIT_STEP_SUCCESS: StaticRedisScript = StaticRedisScript::new(COMMIT_STEP_SUCCESS_SCRIPT);
static COMMIT_STEP_ERROR: StaticRedisScript = StaticRedisScript::new(COMMIT_STEP_ERROR_SCRIPT);
static COMMIT_STEP_RECORD: StaticRedisScript = StaticRedisScript::new(COMMIT_STEP_RECORD_SCRIPT);
static RESTORE_WAITING_DUE: StaticRedisScript = StaticRedisScript::new(RESTORE_WAITING_DUE_SCRIPT);

const READ_STEP_RECORD_SCRIPT: &str = r#"
local generation = redis.call("HGET", KEYS[1], "generation")
local token = redis.call("HGET", KEYS[1], "runToken")
if generation ~= ARGV[1] or token ~= ARGV[2] then
  return {0, ""}
end
local lease = tonumber(redis.call("HGET", KEYS[1], "runLeaseExpiresAtMs") or "")
if not lease then
  return {-4, ""}
end
if lease <= tonumber(ARGV[3]) then
  return {-2, ""}
end
local status = redis.call("HGET", KEYS[1], "status")
if status ~= "running" and status ~= "waiting" then
  return {-3, ""}
end
local raw = redis.call("HGET", KEYS[2], ARGV[4])
if not raw then
  return {1, ""}
end
return {1, raw}
"#;

pub(crate) const COMMIT_STEP_SUCCESS_SCRIPT: &str = r#"
local generation = redis.call("HGET", KEYS[1], "generation")
local token = redis.call("HGET", KEYS[1], "runToken")
if generation ~= ARGV[1] or token ~= ARGV[2] then
  return 0
end
local lease = tonumber(redis.call("HGET", KEYS[1], "runLeaseExpiresAtMs") or "")
if not lease then
  return -4
end
if lease <= tonumber(ARGV[9]) then
  return -2
end
local status = redis.call("HGET", KEYS[1], "status")
if status ~= "running" and status ~= "waiting" then
  return -3
end
local payload_bytes = tonumber(redis.call("HGET", KEYS[1], "payloadBytes") or "0")
local old_payload_bytes = 0
local new_payload_bytes = 0
if ARGV[3] ~= "" then
  old_payload_bytes = string.len(redis.call("HGET", KEYS[2], ARGV[3]) or "")
  new_payload_bytes = string.len(ARGV[4])
end
local old_record_bytes = string.len(redis.call("HGET", KEYS[3], ARGV[5]) or "")
local old_summary_bytes = string.len(redis.call("HGET", KEYS[4], ARGV[5]) or "")
local next_payload_bytes = payload_bytes - old_payload_bytes - old_record_bytes - old_summary_bytes + new_payload_bytes + string.len(ARGV[6]) + string.len(ARGV[8])
if next_payload_bytes > tonumber(ARGV[7]) then
  return -1
end
if ARGV[3] ~= "" then
  redis.call("HSET", KEYS[2], ARGV[3], ARGV[4])
end
redis.call("HSET", KEYS[3], ARGV[5], ARGV[6])
redis.call("HSET", KEYS[4], ARGV[5], ARGV[8])
redis.call("ZADD", KEYS[5], tonumber(ARGV[5]), ARGV[5])
redis.call("HSET", KEYS[1], "payloadBytes", tostring(next_payload_bytes))
return 1
"#;
pub(crate) const COMMIT_STEP_ERROR_SCRIPT: &str = r#"
local generation = redis.call("HGET", KEYS[1], "generation")
local token = redis.call("HGET", KEYS[1], "runToken")
if generation ~= ARGV[1] or token ~= ARGV[2] then
  return 0
end
local lease = tonumber(redis.call("HGET", KEYS[1], "runLeaseExpiresAtMs") or "")
if not lease then
  return -4
end
if lease <= tonumber(ARGV[13]) then
  return -2
end
local status = redis.call("HGET", KEYS[1], "status")
if status ~= "running" and status ~= "waiting" then
  return -3
end
local payload_bytes = tonumber(redis.call("HGET", KEYS[1], "payloadBytes") or "0")
local old_payload_bytes = 0
local new_payload_bytes = 0
if ARGV[3] ~= "" then
  old_payload_bytes = string.len(redis.call("HGET", KEYS[2], ARGV[3]) or "")
  new_payload_bytes = string.len(ARGV[4])
end
local old_record_bytes = string.len(redis.call("HGET", KEYS[3], ARGV[5]) or "")
local old_summary_bytes = string.len(redis.call("HGET", KEYS[4], ARGV[5]) or "")
local next_payload_bytes = payload_bytes - old_payload_bytes - old_record_bytes - old_summary_bytes + new_payload_bytes + string.len(ARGV[6]) + string.len(ARGV[12])
if next_payload_bytes > tonumber(ARGV[11]) then
  return -1
end
if ARGV[3] ~= "" then
  redis.call("HSET", KEYS[2], ARGV[3], ARGV[4])
end
redis.call("HSET", KEYS[3], ARGV[5], ARGV[6])
redis.call("HSET", KEYS[4], ARGV[5], ARGV[12])
redis.call("ZADD", KEYS[5], tonumber(ARGV[5]), ARGV[5])
redis.call("HSET", KEYS[1], "payloadBytes", tostring(next_payload_bytes))
if ARGV[7] == "waiting" then
  redis.call("HSET", KEYS[1], "status", "waiting", "updatedAtMs", ARGV[8])
  redis.call("ZADD", KEYS[6], ARGV[9], ARGV[10])
  redis.call("SREM", KEYS[7], ARGV[10])
end
return 1
"#;
pub(crate) const COMMIT_STEP_RECORD_SCRIPT: &str = r#"
local generation = redis.call("HGET", KEYS[1], "generation")
local token = redis.call("HGET", KEYS[1], "runToken")
if generation ~= ARGV[1] or token ~= ARGV[2] then
  return 0
end
local lease = tonumber(redis.call("HGET", KEYS[1], "runLeaseExpiresAtMs") or "")
if not lease then
  return -4
end
if lease <= tonumber(ARGV[17]) then
  return -2
end
local status = redis.call("HGET", KEYS[1], "status")
if status ~= "running" and status ~= "waiting" then
  return -3
end
local payload_bytes = tonumber(redis.call("HGET", KEYS[1], "payloadBytes") or "0")
local old_payload_bytes = 0
local new_payload_bytes = 0
if ARGV[3] ~= "" then
  old_payload_bytes = string.len(redis.call("HGET", KEYS[2], ARGV[3]) or "")
  new_payload_bytes = string.len(ARGV[4])
end
local old_record_bytes = string.len(redis.call("HGET", KEYS[3], ARGV[5]) or "")
local old_summary_bytes = string.len(redis.call("HGET", KEYS[4], ARGV[5]) or "")
local old_event_bytes = 0
local new_event_bytes = 0
if ARGV[11] ~= "" then
  old_event_bytes = string.len(redis.call("HGET", KEYS[8], ARGV[11]) or "")
  new_event_bytes = string.len(ARGV[12])
end
local next_payload_bytes = payload_bytes - old_payload_bytes - old_record_bytes - old_summary_bytes - old_event_bytes + new_payload_bytes + string.len(ARGV[6]) + string.len(ARGV[14]) + new_event_bytes
if next_payload_bytes > tonumber(ARGV[13]) then
  return -1
end
if ARGV[4] ~= "" then
  if ARGV[3] ~= "" then
    redis.call("HSET", KEYS[2], ARGV[3], ARGV[4])
  end
end
redis.call("HSET", KEYS[1], "payloadBytes", tostring(next_payload_bytes))
redis.call("HSET", KEYS[3], ARGV[5], ARGV[6])
redis.call("HSET", KEYS[4], ARGV[5], ARGV[14])
redis.call("ZADD", KEYS[5], tonumber(ARGV[5]), ARGV[5])
if ARGV[7] ~= "" then
  redis.call("HSET", KEYS[1], "status", ARGV[7], "updatedAtMs", ARGV[8])
end
if ARGV[16] ~= "" then
  redis.call("HSET", KEYS[1], "waitingEventIndexPrefix", ARGV[16])
else
  redis.call("HDEL", KEYS[1], "waitingEventIndexPrefix")
end
if ARGV[9] ~= "" then
  redis.call("ZADD", KEYS[6], ARGV[9], ARGV[10])
else
  redis.call("ZREM", KEYS[6], ARGV[10])
end
redis.call("SREM", KEYS[7], ARGV[10])
if ARGV[11] ~= "" then
  redis.call("HSET", KEYS[8], ARGV[11], ARGV[12])
end
if ARGV[15] ~= "" then
  redis.call("ZREM", KEYS[9], ARGV[15])
end
return 1
"#;
const RESTORE_WAITING_DUE_SCRIPT: &str = r#"
local generation = redis.call("HGET", KEYS[1], "generation")
local token = redis.call("HGET", KEYS[1], "runToken")
if generation ~= ARGV[1] or token ~= ARGV[2] then
  return 0
end
local lease = tonumber(redis.call("HGET", KEYS[1], "runLeaseExpiresAtMs") or "")
if not lease then
  return -4
end
if lease <= tonumber(ARGV[6]) then
  return -2
end
local status = redis.call("HGET", KEYS[1], "status")
if status ~= "running" then
  return -3
end
redis.call("HSET", KEYS[1], "status", "waiting", "updatedAtMs", ARGV[3])
redis.call("ZADD", KEYS[2], ARGV[4], ARGV[5])
redis.call("SREM", KEYS[3], ARGV[5])
return 1
"#;

fn active_claim_error(code: i64) -> WorkflowError {
    match code {
        STEP_SCRIPT_EXPIRED_CLAIM => {
            WorkflowError::invalid_state("Workflow run claim lease has expired")
        }
        STEP_SCRIPT_INACTIVE_INSTANCE => {
            WorkflowError::invalid_state("Workflow instance is not active")
        }
        STEP_SCRIPT_CORRUPT_CLAIM => {
            WorkflowError::invalid_state("Workflow run claim lease is corrupt")
        }
        _ => WorkflowError::invalid_state("Workflow run claim does not match instance state"),
    }
}

fn ensure_step_script_ok(code: i64) -> WorkflowResult<()> {
    match code {
        STEP_SCRIPT_OK => Ok(()),
        STEP_SCRIPT_PAYLOAD_LIMIT => Err(aggregate_payload_error()),
        other => Err(active_claim_error(other)),
    }
}

async fn read_active_claim_field(
    state: &AppState,
    req: &WorkflowStepRequest,
    data_key: &str,
    field: &str,
) -> WorkflowResult<Option<String>> {
    let keys = InstanceRouteKeys::new(&req.ns, &req.workflow_key, &req.instance_id);
    let state_key = keys.state();
    let now = now_ms().to_string();
    let generation = req.generation.to_string();
    let (code, raw): (i64, String) = eval_script(
        state,
        &READ_STEP_RECORD,
        &[&state_key, data_key],
        &[&generation, &req.run_token, &now, field],
    )
    .await?;
    match code {
        STEP_SCRIPT_OK if raw.is_empty() => Ok(None),
        STEP_SCRIPT_OK => Ok(Some(raw)),
        other => Err(active_claim_error(other)),
    }
}

pub(super) async fn read_step_record_for_claim(
    state: &AppState,
    req: &WorkflowStepRequest,
) -> WorkflowResult<Option<String>> {
    let keys = InstanceRouteKeys::new(&req.ns, &req.workflow_key, &req.instance_id);
    read_active_claim_field(state, req, &keys.steps(), &step_field(req.ordinal)).await
}

pub(super) async fn read_step_payload_for_claim(
    state: &AppState,
    req: &WorkflowStepRequest,
    payload_ref: &str,
) -> WorkflowResult<Option<String>> {
    let keys = InstanceRouteKeys::new(&req.ns, &req.workflow_key, &req.instance_id);
    read_active_claim_field(state, req, &keys.payloads(), payload_ref).await
}
async fn restore_waiting_due_index(
    state: &AppState,
    req: &WorkflowStepRequest,
    due_at_ms: i64,
) -> WorkflowResult<bool> {
    let keys = InstanceRouteKeys::new(&req.ns, &req.workflow_key, &req.instance_id);
    let state_key = keys.state();
    let ready = keys.ready();
    let due = keys.due();
    let token = keys.token();
    let updated_at = now_ms().to_string();
    let generation = req.generation.to_string();
    let due_at_ms = due_at_ms.to_string();
    let restored: i64 = eval_script(
        state,
        &RESTORE_WAITING_DUE,
        &[&state_key, &due, &ready],
        &[
            &generation,
            &req.run_token,
            &updated_at,
            &due_at_ms,
            &token,
            &updated_at,
        ],
    )
    .await?;
    match restored {
        STEP_SCRIPT_OK => Ok(true),
        STEP_SCRIPT_STALE_CLAIM => Ok(false),
        other => Err(active_claim_error(other)),
    }
}

pub(crate) async fn claim_step(
    state: &AppState,
    req: WorkflowStepRequest,
) -> WorkflowResult<JsonValue> {
    let config = validate_step_request(&req)?;
    let raw = read_step_record_for_claim(state, &req).await?;
    let Some(raw) = raw else {
        log_step_event(state, "workflow_step_started", &req, 1);
        spawn_progress_from_step(state, &req, "workflow_step_started", "running", 1);
        return Ok(json!({ "state": "run" }));
    };
    let record: StepRecord = serde_json::from_str(&raw).map_err(|err| {
        WorkflowError::invalid_state(format!("Workflow step record is corrupt: {err}"))
    })?;
    verify_step_record(&req, &config, &record)?;
    match record.status.as_str() {
        "completed" => {
            let output = if let Some(output) = record.output {
                output
            } else {
                let Some(output_ref) = record.output_ref else {
                    return Err(WorkflowError::invalid_state(
                        "Workflow completed step is missing output ref",
                    ));
                };
                let output_raw = read_step_payload_for_claim(state, &req, &output_ref).await?;
                let Some(output_raw) = output_raw else {
                    return Err(WorkflowError::payload_missing(format!(
                        "Workflow step output payload {output_ref} is missing"
                    )));
                };
                serde_json::from_str(&output_raw).map_err(|err| {
                    WorkflowError::invalid_state(format!("Workflow step output is corrupt: {err}"))
                })?
            };
            Ok(json!({ "state": "complete", "output": output }))
        }
        "waiting" => {
            let due_at_ms = record.due_at_ms.ok_or_else(|| {
                WorkflowError::invalid_state("Workflow waiting step is missing dueAtMs")
            })?;
            if due_at_ms > now_ms() {
                let _ = restore_waiting_due_index(state, &req, due_at_ms).await?;
                return Ok(json!({ "state": "waiting" }));
            }
            let attempt = record.attempt.saturating_add(1).max(1);
            log_step_event(state, "workflow_step_started", &req, attempt);
            spawn_progress_from_step(state, &req, "workflow_step_started", "running", attempt);
            Ok(json!({ "state": "run", "attempt": attempt }))
        }
        "failed" => {
            let error = if let Some(error) = record.error {
                error
            } else {
                let Some(error_ref) = record.error_ref else {
                    return Err(WorkflowError::invalid_state(
                        "Workflow failed step is missing error ref",
                    ));
                };
                let error_raw = read_step_payload_for_claim(state, &req, &error_ref).await?;
                let Some(error_raw) = error_raw else {
                    return Err(WorkflowError::payload_missing(format!(
                        "Workflow step error payload {error_ref} is missing"
                    )));
                };
                serde_json::from_str(&error_raw).map_err(|err| {
                    WorkflowError::invalid_state(format!("Workflow step error is corrupt: {err}"))
                })?
            };
            Ok(json!({ "state": "failed", "error": error }))
        }
        _ => Err(WorkflowError::invalid_state(
            "Workflow step status is invalid",
        )),
    }
}

pub(crate) async fn commit_step_success(
    state: &AppState,
    req: WorkflowStepRequest,
) -> WorkflowResult<JsonValue> {
    let config = validate_step_request(&req)?;
    let attempt = request_attempt(&req)?;
    let keys = InstanceRouteKeys::new(&req.ns, &req.workflow_key, &req.instance_id);
    let steps_key = keys.steps();
    let summaries_key = keys.step_summaries();
    let summary_index_key = keys.step_summary_index();
    let payloads_key = keys.payloads();
    let field = step_field(req.ordinal);
    let output_json = result_json(&req.output, "step output")?;
    let output_inline = inline_step_payload(&req.output, &output_json);
    let output_ref = output_inline
        .is_none()
        .then(|| step_output_ref(req.ordinal));
    let completed_at_ms = now_ms();
    let record = StepRecord {
        ordinal: req.ordinal,
        step_name: req.step_name.clone(),
        name_count: req.name_count,
        dependencies: req.dependencies.clone(),
        config,
        status: "completed".to_string(),
        attempt,
        output_ref: output_ref.clone(),
        error_ref: None,
        output: output_inline,
        error: None,
        completed_at_ms: Some(completed_at_ms),
        failed_at_ms: None,
        due_at_ms: None,
    };
    let record_json = step_record_json(&record)?;
    let summary_json = step_summary_json(&record)?;
    let state_key = keys.state();
    let generation = req.generation.to_string();
    let completed_at = completed_at_ms.to_string();
    let payload_limit = instance_payload_limit_arg();
    let committed: i64 = eval_script(
        state,
        &COMMIT_STEP_SUCCESS,
        &[
            &state_key,
            &payloads_key,
            &steps_key,
            &summaries_key,
            &summary_index_key,
        ],
        &[
            &generation,
            &req.run_token,
            output_ref.as_deref().unwrap_or(""),
            &output_json,
            &field,
            &record_json,
            &payload_limit,
            &summary_json,
            &completed_at,
        ],
    )
    .await?;
    ensure_step_script_ok(committed)?;
    state
        .metrics
        .increment("workflow_steps", &[("outcome", "completed")], 1.0);
    observe_step_duration(state, &req, completed_at_ms);
    log_step_event(state, "workflow_step_completed", &req, attempt);
    spawn_progress_from_step(state, &req, "workflow_step_completed", "completed", attempt);
    Ok(json!({ "state": "complete" }))
}

pub(crate) async fn commit_step_error(
    state: &AppState,
    req: WorkflowStepRequest,
) -> WorkflowResult<JsonValue> {
    let config = validate_step_request(&req)?;
    let attempt = request_attempt(&req)?;
    let policy = retry_policy(&req.config)?;
    let keys = InstanceRouteKeys::new(&req.ns, &req.workflow_key, &req.instance_id);
    let steps_key = keys.steps();
    let summaries_key = keys.step_summaries();
    let summary_index_key = keys.step_summary_index();
    let payloads_key = keys.payloads();
    let state_key = keys.state();
    let ready = keys.ready();
    let due = keys.due();
    let token = keys.token();
    let field = step_field(req.ordinal);
    let error_json = result_json(&req.error, "step error")?;
    let error_inline = inline_step_payload(&req.error, &error_json);
    let error_ref = error_inline.is_none().then(|| step_error_ref(req.ordinal));
    let terminal = req.non_retryable || attempt >= policy.limit;
    let now = now_ms();
    let due_at_ms = if terminal {
        None
    } else {
        Some(retry_due_at_ms(now, &policy, attempt))
    };
    let record = StepRecord {
        ordinal: req.ordinal,
        step_name: req.step_name.clone(),
        name_count: req.name_count,
        dependencies: req.dependencies.clone(),
        config,
        status: if terminal { "failed" } else { "waiting" }.to_string(),
        attempt,
        output_ref: None,
        error_ref: error_ref.clone(),
        output: None,
        error: error_inline,
        completed_at_ms: None,
        failed_at_ms: if terminal { Some(now) } else { None },
        due_at_ms,
    };
    let record_json = step_record_json(&record)?;
    let summary_json = step_summary_json(&record)?;
    let updated_at = now.to_string();
    let due_arg = due_at_ms.unwrap_or(0).to_string();
    let mode = if due_at_ms.is_some() {
        "waiting"
    } else {
        "terminal"
    };
    let generation = req.generation.to_string();
    let now_arg = now.to_string();
    let payload_limit = instance_payload_limit_arg();
    let committed: i64 = eval_script(
        state,
        &COMMIT_STEP_ERROR,
        &[
            &state_key,
            &payloads_key,
            &steps_key,
            &summaries_key,
            &summary_index_key,
            &due,
            &ready,
        ],
        &[
            &generation,
            &req.run_token,
            error_ref.as_deref().unwrap_or(""),
            &error_json,
            &field,
            &record_json,
            mode,
            &updated_at,
            &due_arg,
            &token,
            &payload_limit,
            &summary_json,
            &now_arg,
        ],
    )
    .await?;
    ensure_step_script_ok(committed)?;
    if terminal {
        state
            .metrics
            .increment("workflow_steps", &[("outcome", "failed")], 1.0);
        observe_step_duration(state, &req, now);
        spawn_progress_from_step(state, &req, "workflow_step_failed", "failed", attempt);
        Ok(json!({ "state": "failed" }))
    } else {
        state
            .metrics
            .increment("workflow_steps", &[("outcome", "waiting")], 1.0);
        observe_step_duration(state, &req, now);
        log_step_event(state, "workflow_step_retry_scheduled", &req, attempt);
        spawn_progress_from_step(
            state,
            &req,
            "workflow_step_retry_scheduled",
            "waiting",
            attempt,
        );
        Ok(json!({ "state": "waiting", "dueAtMs": due_at_ms }))
    }
}

struct StepRecordCommit<'a> {
    record_json: String,
    summary_json: String,
    state_status: &'a str,
    due_at_ms: Option<i64>,
    payload_ref: Option<&'a str>,
    payload_json: Option<String>,
    event_record: Option<(&'a str, String)>,
    event_index_member: Option<String>,
    waiting_event_index_prefix: Option<String>,
}

async fn commit_step_record(
    state: &AppState,
    req: &WorkflowStepRequest,
    commit: StepRecordCommit<'_>,
) -> WorkflowResult<()> {
    let keys = InstanceRouteKeys::new(&req.ns, &req.workflow_key, &req.instance_id);
    let state_key = keys.state();
    let payloads_key = keys.payloads();
    let steps_key = keys.steps();
    let summaries_key = keys.step_summaries();
    let summary_index_key = keys.step_summary_index();
    let due = keys.due();
    let ready = keys.ready();
    let events_key = keys.events();
    let event_index_key = keys.event_type_index();
    let token = keys.token();
    let field = step_field(req.ordinal);
    let now = now_ms().to_string();
    let due_arg = commit
        .due_at_ms
        .map(|value| value.to_string())
        .unwrap_or_default();
    let payload_ref = commit.payload_ref.unwrap_or("");
    let payload_json = commit.payload_json.unwrap_or_default();
    let (event_field, event_json) = commit.event_record.unwrap_or(("", String::new()));
    let event_index_member = commit.event_index_member.unwrap_or_default();
    let waiting_event_index_prefix = commit.waiting_event_index_prefix.unwrap_or_default();
    let generation = req.generation.to_string();
    let payload_limit = instance_payload_limit_arg();
    let updated: i64 = eval_script(
        state,
        &COMMIT_STEP_RECORD,
        &[
            &state_key,
            &payloads_key,
            &steps_key,
            &summaries_key,
            &summary_index_key,
            &due,
            &ready,
            &events_key,
            &event_index_key,
        ],
        &[
            &generation,
            &req.run_token,
            payload_ref,
            &payload_json,
            &field,
            &commit.record_json,
            commit.state_status,
            &now,
            &due_arg,
            &token,
            event_field,
            &event_json,
            &payload_limit,
            &commit.summary_json,
            &event_index_member,
            &waiting_event_index_prefix,
            &now,
        ],
    )
    .await?;
    ensure_step_script_ok(updated)
}

async fn write_waiting_record(
    state: &AppState,
    req: &WorkflowStepRequest,
    record: StepRecord,
    due_at_ms: Option<i64>,
    waiting_event_index_prefix: Option<String>,
) -> WorkflowResult<()> {
    let record_json = step_record_json(&record)?;
    let summary_json = step_summary_json(&record)?;
    commit_step_record(
        state,
        req,
        StepRecordCommit {
            record_json,
            summary_json,
            state_status: "waiting",
            due_at_ms,
            payload_ref: None,
            payload_json: None,
            event_record: None,
            event_index_member: None,
            waiting_event_index_prefix,
        },
    )
    .await?;
    state
        .metrics
        .increment("workflow_steps", &[("outcome", "waiting")], 1.0);
    log(
        state,
        LogLevel::Info,
        "workflow_instance_waiting",
        json!({
            "namespace": req.ns,
            "worker": req.worker,
            "workflow_name": req.workflow_name,
            "workflow_key": req.workflow_key,
            "workflow_class": req.class_name,
            "instance_id": req.instance_id,
            "frozen_version": req.frozen_version,
            "generation": req.generation,
            "step_name": req.step_name,
            "ordinal": req.ordinal,
        }),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_claim_field_script_checks_the_full_fence_before_reading() {
        let generation = READ_STEP_RECORD_SCRIPT
            .find(r#"redis.call("HGET", KEYS[1], "generation")"#)
            .expect("claim snapshot must read generation");
        let token = READ_STEP_RECORD_SCRIPT
            .find(r#"redis.call("HGET", KEYS[1], "runToken")"#)
            .expect("claim snapshot must read run token");
        let lease = READ_STEP_RECORD_SCRIPT
            .find(r#"redis.call("HGET", KEYS[1], "runLeaseExpiresAtMs")"#)
            .expect("claim snapshot must read lease");
        let status = READ_STEP_RECORD_SCRIPT
            .find(r#"redis.call("HGET", KEYS[1], "status")"#)
            .expect("claim snapshot must read status");
        let field = READ_STEP_RECORD_SCRIPT
            .find(r#"redis.call("HGET", KEYS[2], ARGV[4])"#)
            .expect("claim snapshot must read the requested field");
        assert!(generation < token && token < lease && lease < status && status < field);
    }
}
