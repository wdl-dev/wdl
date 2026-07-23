use wdl_rust_common::{redis_eval::StaticRedisScript, time::now_ms};

use crate::{AppState, WorkflowError, WorkflowResult, by_version_key, retention_key};

use super::super::{
    InstanceResponse, InstanceRouteKeys, WorkflowRequest, eval_script, identity_from_state,
    instance_id, observe_instance_duration, read_public_state, response_from_state,
    terminal_retention_ms, validate_identity, workflow_referrer_member,
};
use super::common::{
    TransitionSignal, next_generation, stale_transition_response, successful_transition_response,
};

const TERMINATE_SCRIPT: &str = r#"
local status = redis.call("HGET", KEYS[1], "status")
if not status then
  return 0
end
local generation = redis.call("HGET", KEYS[1], "generation")
local created_at_ms = redis.call("HGET", KEYS[1], "createdAtMs")
if generation ~= ARGV[1] or created_at_ms ~= ARGV[7] then
  return 0
end
if status == "completed" or status == "failed" or status == "terminated" then
  return 0
end
redis.call("HSET", KEYS[1], "status", "terminated", "generation", ARGV[2], "updatedAtMs", ARGV[3], "terminatedAtMs", ARGV[3])
redis.call("HSET", KEYS[1], "retentionExpiresAtMs", ARGV[6])
redis.call("SREM", KEYS[2], ARGV[4])
redis.call("ZREM", KEYS[3], ARGV[4])
redis.call("SREM", KEYS[4], ARGV[5])
redis.call("ZADD", KEYS[5], ARGV[6], ARGV[4])
redis.call("HDEL", KEYS[1], "runToken", "runLeaseExpiresAtMs", "waitingEventIndexPrefix")
return 1
"#;

static TERMINATE: StaticRedisScript = StaticRedisScript::new(TERMINATE_SCRIPT);

pub(crate) async fn terminate_instance(
    state: &AppState,
    req: WorkflowRequest,
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
    if matches!(status, "completed" | "failed" | "terminated") {
        return response_from_state(state, &req.ns, &req.workflow_key, &id, &existing).await;
    }
    let identity = identity_from_state(&req.ns, &req.workflow_key, &id, &existing)?;
    let next_generation = next_generation(&identity)?;
    let keys = InstanceRouteKeys::new(&identity.ns, &identity.workflow_key, &identity.instance_id);
    let state_key = keys.state();
    let ready = keys.ready();
    let due = keys.due();
    let token = keys.token();
    let referrer_member = workflow_referrer_member(&identity.workflow_key, &identity.instance_id);
    let by_version = by_version_key(&identity.ns, &identity.worker, &identity.frozen_version);
    let ended_at_ms = now_ms();
    let now = ended_at_ms.to_string();
    let retention_ms = terminal_retention_ms(&existing, "failed")?;
    let retention_expires_at = ended_at_ms.saturating_add(retention_ms).to_string();
    let retention = retention_key().to_string();
    let expected_generation = identity.generation.clone();
    let stored_generation = next_generation.clone();
    let stored_now = now.clone();
    let stored_retention_expires_at = retention_expires_at.clone();
    let updated: i64 = eval_script(
        state,
        &TERMINATE,
        &[&state_key, &ready, &due, &by_version, &retention],
        &[
            &expected_generation,
            &next_generation,
            &now,
            &token,
            &referrer_member,
            &retention_expires_at,
            &identity.created_at_ms,
        ],
    )
    .await?;
    if updated != 1 {
        return stale_transition_response(state, &identity, &id).await;
    }
    existing.insert("status".to_string(), "terminated".to_string());
    existing.insert("generation".to_string(), stored_generation);
    existing.insert("updatedAtMs".to_string(), stored_now.clone());
    existing.insert("terminatedAtMs".to_string(), stored_now);
    existing.insert(
        "retentionExpiresAtMs".to_string(),
        stored_retention_expires_at,
    );
    observe_instance_duration(state, &existing, ended_at_ms);
    successful_transition_response(
        state,
        &identity,
        &id,
        &existing,
        TransitionSignal {
            outcome: "terminated",
            event: "workflow_instance_terminated",
            progress_status: "terminated",
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminate_progress_uses_post_transition_identity() {
        let source = include_str!("terminate.rs");
        let implementation = source
            .split("\n#[cfg(test)]")
            .next()
            .expect("terminate implementation should precede tests");
        assert!(
            implementation
                .contains("successful_transition_response(\n        state,\n        &identity,")
        );
        assert!(implementation.contains("event: \"workflow_instance_terminated\""));
        assert!(implementation.contains("progress_status: \"terminated\""));
    }

    #[test]
    fn terminate_rejects_a_recreated_instance() {
        let first_write = TERMINATE_SCRIPT
            .find(r#"redis.call("HSET", KEYS[1]"#)
            .expect("terminate state write");
        let incarnation_check = TERMINATE_SCRIPT
            .find(r#"created_at_ms ~= ARGV[7]"#)
            .expect("terminate incarnation fence");
        assert!(incarnation_check < first_write);
    }
}
