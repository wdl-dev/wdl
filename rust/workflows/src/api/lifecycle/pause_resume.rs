use wdl_rust_common::{redis_eval::StaticRedisScript, time::now_ms};

use crate::{AppState, WorkflowError, WorkflowResult, ready_active_key};

use super::super::{
    InstanceResponse, InstanceRouteKeys, WorkflowRequest, eval_script, identity_from_state,
    instance_id, read_public_state, response_from_state, validate_identity,
};
use super::common::{
    TransitionSignal, next_generation, stale_transition_response, successful_transition_response,
};

const PAUSE_RESUME_SCRIPT: &str = r#"
local status = redis.call("HGET", KEYS[1], "status")
if not status then
  return 0
end
local generation = redis.call("HGET", KEYS[1], "generation")
local created_at_ms = redis.call("HGET", KEYS[1], "createdAtMs")
if generation ~= ARGV[1] or created_at_ms ~= ARGV[7] then
  return 0
end
if ARGV[4] == "pause" then
  if status == "completed" or status == "failed" or status == "terminated" or status == "paused" then
    return 0
  end
  redis.call("HSET", KEYS[1], "status", "paused", "generation", ARGV[2], "updatedAtMs", ARGV[3], "pausedAtMs", ARGV[3])
  redis.call("HDEL", KEYS[1], "runToken", "runLeaseExpiresAtMs", "waitingEventIndexPrefix")
  redis.call("SREM", KEYS[2], ARGV[5])
  redis.call("ZREM", KEYS[3], ARGV[5])
  return 1
end
if status ~= "paused" then
  return 0
end
redis.call("HSET", KEYS[1], "status", "queued", "generation", ARGV[2], "updatedAtMs", ARGV[3], "resumedAtMs", ARGV[3])
redis.call("HDEL", KEYS[1], "runToken", "runLeaseExpiresAtMs", "waitingEventIndexPrefix")
redis.call("SADD", KEYS[2], ARGV[5])
redis.call("SADD", KEYS[4], ARGV[6])
return 1
"#;

static PAUSE_RESUME: StaticRedisScript = StaticRedisScript::new(PAUSE_RESUME_SCRIPT);

async fn transition_paused(
    state: &AppState,
    req: WorkflowRequest,
    paused: bool,
) -> WorkflowResult<InstanceResponse> {
    validate_identity(&req)?;
    let id = instance_id(&req)?.to_string();
    let mut existing = read_public_state(state, &req).await?;
    if existing.is_empty() {
        return Err(WorkflowError::not_found("Workflow instance not found"));
    }
    let status = existing
        .get("status")
        .map(String::as_str)
        .ok_or_else(|| WorkflowError::invalid_state("Workflow state missing status"))?;
    if matches!(status, "completed" | "failed" | "terminated") || (paused && status == "paused") {
        return response_from_state(state, &req.ns, &req.workflow_key, &id, &existing).await;
    }
    if !paused && status != "paused" {
        return response_from_state(state, &req.ns, &req.workflow_key, &id, &existing).await;
    }
    let identity = identity_from_state(&req.ns, &req.workflow_key, &id, &existing)?;
    let next_generation = next_generation(&identity)?;
    let keys = InstanceRouteKeys::new(&identity.ns, &identity.workflow_key, &identity.instance_id);
    let state_key = keys.state();
    let shard = keys.shard();
    let ready = keys.ready();
    let due = keys.due();
    let token = keys.token();
    let shard_arg = shard.to_string();
    let now = now_ms().to_string();
    let expected_generation = identity.generation.clone();
    let op = if paused { "pause" } else { "resume" };
    let stored_generation = next_generation.clone();
    let stored_now = now.clone();
    let updated: i64 = eval_script(
        state,
        &PAUSE_RESUME,
        &[&state_key, &ready, &due, ready_active_key()],
        &[
            &expected_generation,
            &next_generation,
            &now,
            op,
            &token,
            &shard_arg,
            &identity.created_at_ms,
        ],
    )
    .await?;
    if updated != 1 {
        return stale_transition_response(state, &identity, &id).await;
    }
    let next_status = if paused { "paused" } else { "queued" };
    let timestamp_field = if paused { "pausedAtMs" } else { "resumedAtMs" };
    existing.insert("status".to_string(), next_status.to_string());
    existing.insert("generation".to_string(), stored_generation);
    existing.insert("updatedAtMs".to_string(), stored_now.clone());
    existing.insert(timestamp_field.to_string(), stored_now);
    successful_transition_response(
        state,
        &identity,
        &id,
        &existing,
        TransitionSignal {
            outcome: if paused { "paused" } else { "resumed" },
            event: if paused {
                "workflow_instance_paused"
            } else {
                "workflow_instance_resumed"
            },
            progress_status: next_status,
        },
    )
    .await
}

pub(crate) async fn pause_instance(
    state: &AppState,
    req: WorkflowRequest,
) -> WorkflowResult<InstanceResponse> {
    transition_paused(state, req, true).await
}

pub(crate) async fn resume_instance(
    state: &AppState,
    req: WorkflowRequest,
) -> WorkflowResult<InstanceResponse> {
    transition_paused(state, req, false).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pause_resume_progress_uses_post_transition_identity() {
        let source = include_str!("pause_resume.rs");
        let implementation = source
            .split("\n#[cfg(test)]")
            .next()
            .expect("pause/resume implementation should precede tests");
        assert!(
            implementation
                .contains("successful_transition_response(\n        state,\n        &identity,")
        );
        assert!(implementation.contains("outcome: if paused { \"paused\" } else { \"resumed\" }"));
        assert!(implementation.contains("progress_status: next_status"));
    }

    #[test]
    fn pause_resume_clears_run_claims_before_generation_requeue() {
        let source = include_str!("pause_resume.rs");
        let script = source
            .split("#[cfg(test)]")
            .next()
            .expect("script source should precede tests");
        assert_eq!(
            script
                .matches(r#"redis.call("HDEL", KEYS[1], "runToken", "runLeaseExpiresAtMs", "waitingEventIndexPrefix")"#)
                .count(),
            2
        );
    }

    #[test]
    fn pause_resume_rejects_a_recreated_instance() {
        let first_write = PAUSE_RESUME_SCRIPT
            .find(r#"redis.call("HSET", KEYS[1]"#)
            .expect("pause/resume state write");
        let incarnation_check = PAUSE_RESUME_SCRIPT
            .find(r#"created_at_ms ~= ARGV[7]"#)
            .expect("pause/resume incarnation fence");
        assert!(incarnation_check < first_write);
    }
}
