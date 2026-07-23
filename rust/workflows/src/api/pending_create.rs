use std::collections::HashMap;
use std::time::Duration;

use wdl_rust_common::{
    redis_eval::StaticRedisScript,
    time::{now_ms, random_hex_64},
};

use crate::{
    AppState, InstanceIdentity, WorkflowError, WorkflowResult, by_version_key, by_worker_key,
    by_workflow_key, ready_active_key,
};

use super::{
    InstanceRouteKeys, WorkflowRequest, eval_script, identity_from_state, read_state,
    workflow_referrer_member,
};

const PENDING_CREATE_WAIT_ATTEMPTS: usize = 20;
const PENDING_CREATE_WAIT_DELAY_MS: u64 = 25;
pub(super) const PENDING_CREATE_TTL_MS: i64 = 30_000;

pub(super) const FINALIZE_CREATE_INSTANCE_SCRIPT: &str = r#"
local generation = redis.call("HGET", KEYS[1], "generation")
local status = redis.call("HGET", KEYS[1], "status")
local token = redis.call("HGET", KEYS[1], "pendingCreateToken")
if generation ~= "1" or status ~= "pending_create" or token ~= ARGV[3] then
  return 0
end
redis.call("HSET", KEYS[1], "status", "queued", "updatedAtMs", ARGV[2])
redis.call("HDEL", KEYS[1], "pendingExpiresAtMs", "pendingCreateToken")
redis.call("SADD", KEYS[2], ARGV[1])
redis.call("SADD", KEYS[3], ARGV[4])
redis.call("ZADD", KEYS[4], ARGV[2], ARGV[5])
return 1
"#;

const CLEANUP_PENDING_CREATE_SCRIPT: &str = r#"
local generation = redis.call("HGET", KEYS[1], "generation")
local status = redis.call("HGET", KEYS[1], "status")
local token = redis.call("HGET", KEYS[1], "pendingCreateToken")
if generation ~= "1" or status ~= "pending_create" or token ~= ARGV[2] then
  return 0
end
redis.call("DEL", KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[7])
redis.call("SREM", KEYS[8], ARGV[1])
redis.call("SREM", KEYS[9], ARGV[1])
return 1
"#;

static FINALIZE_CREATE_INSTANCE: StaticRedisScript =
    StaticRedisScript::new(FINALIZE_CREATE_INSTANCE_SCRIPT);
static CLEANUP_PENDING_CREATE: StaticRedisScript =
    StaticRedisScript::new(CLEANUP_PENDING_CREATE_SCRIPT);

pub(super) fn pending_create_token(state: &AppState, req: &WorkflowRequest, id: &str) -> String {
    format!(
        "{}:{}:{}:{}",
        state.instance_id,
        req.workflow_key,
        id,
        random_hex_64()
    )
}

pub(super) async fn wait_for_public_create_state(
    state: &AppState,
    req: &WorkflowRequest,
) -> WorkflowResult<HashMap<String, String>> {
    let mut existing = read_state(state, req).await?;
    for _ in 0..PENDING_CREATE_WAIT_ATTEMPTS {
        if !is_pending_create(&existing) {
            return Ok(existing);
        }
        if cleanup_expired_pending_create(state, &existing).await? {
            return Ok(HashMap::new());
        }
        tokio::time::sleep(Duration::from_millis(PENDING_CREATE_WAIT_DELAY_MS)).await;
        existing = read_state(state, req).await?;
    }
    Err(WorkflowError::invalid_state(
        "Workflow instance create is still pending",
    ))
}

pub(super) fn is_pending_create(state: &HashMap<String, String>) -> bool {
    state
        .get("status")
        .is_some_and(|status| status == "pending_create")
}

pub(super) fn pending_create_expired(state: &HashMap<String, String>, now: i64) -> bool {
    state
        .get("pendingExpiresAtMs")
        .and_then(|raw| raw.parse::<i64>().ok())
        .is_none_or(|expires_at| expires_at <= now)
}

pub(super) async fn cleanup_expired_pending_create(
    state: &AppState,
    existing: &HashMap<String, String>,
) -> WorkflowResult<bool> {
    if !is_pending_create(existing) || !pending_create_expired(existing, now_ms()) {
        return Ok(false);
    }
    let pending = pending_create_cleanup_from_state(existing)?;
    cleanup_pending_create_identity(state, &pending.identity, &pending.token).await
}

pub(super) async fn public_state_or_empty(
    state: &AppState,
    existing: HashMap<String, String>,
) -> WorkflowResult<HashMap<String, String>> {
    if !is_pending_create(&existing) {
        return Ok(existing);
    }
    cleanup_expired_pending_create(state, &existing).await?;
    Ok(HashMap::new())
}

pub(super) async fn finalize_created_instance(
    state: &AppState,
    req: &WorkflowRequest,
    id: &str,
    pending_create_token: &str,
) -> WorkflowResult<()> {
    let keys = InstanceRouteKeys::new(&req.ns, &req.workflow_key, id);
    let state_key = keys.state();
    let shard = keys.shard();
    let ready = keys.ready();
    let by_workflow = by_workflow_key(&req.ns, &req.worker, &req.workflow_key);
    let token = keys.token();
    let now = now_ms().to_string();
    let shard_arg = shard.to_string();
    let finalized: i64 = eval_script(
        state,
        &FINALIZE_CREATE_INSTANCE,
        &[&state_key, &ready, ready_active_key(), &by_workflow],
        &[&token, &now, pending_create_token, &shard_arg, id],
    )
    .await?;
    if finalized != 1 {
        return Err(WorkflowError::invalid_state(
            "Workflow instance create did not finalize",
        ));
    }
    Ok(())
}

pub(super) async fn cleanup_created_instance(
    state: &AppState,
    req: &WorkflowRequest,
    id: &str,
    pending_create_token: &str,
) -> WorkflowResult<()> {
    let identity = InstanceIdentity {
        ns: req.ns.clone(),
        worker: req.worker.clone(),
        frozen_version: req.frozen_version.clone(),
        workflow_name: req.workflow_name.clone(),
        workflow_key: req.workflow_key.clone(),
        class_name: req.class_name.clone(),
        instance_id: id.to_string(),
        generation: "1".to_string(),
        created_at_ms: "0".to_string(),
    };
    cleanup_pending_create_identity(state, &identity, pending_create_token).await?;
    Ok(())
}

pub(super) async fn cleanup_pending_create_identity(
    state: &AppState,
    identity: &InstanceIdentity,
    pending_create_token: &str,
) -> WorkflowResult<bool> {
    let keys = InstanceRouteKeys::new(&identity.ns, &identity.workflow_key, &identity.instance_id);
    let state_key = keys.state();
    let payloads_key = keys.payloads();
    let steps_key = keys.steps();
    let summaries_key = keys.step_summaries();
    let summary_index_key = keys.step_summary_index();
    let events_key = keys.events();
    let event_index_key = keys.event_type_index();
    let by_worker = by_worker_key(&identity.ns, &identity.worker);
    let by_version = by_version_key(&identity.ns, &identity.worker, &identity.frozen_version);
    let referrer_member = workflow_referrer_member(&identity.workflow_key, &identity.instance_id);
    let removed: i64 = eval_script(
        state,
        &CLEANUP_PENDING_CREATE,
        &[
            &state_key,
            &payloads_key,
            &steps_key,
            &events_key,
            &summaries_key,
            &summary_index_key,
            &event_index_key,
            &by_worker,
            &by_version,
        ],
        &[&referrer_member, pending_create_token],
    )
    .await?;
    Ok(removed == 1)
}

pub(super) struct PendingCreateCleanup {
    pub(super) identity: InstanceIdentity,
    pub(super) token: String,
}

pub(super) fn pending_create_cleanup_from_state(
    existing: &HashMap<String, String>,
) -> WorkflowResult<PendingCreateCleanup> {
    let identity = identity_from_state(
        existing.get("ns").map(String::as_str).unwrap_or_default(),
        existing
            .get("workflowKey")
            .map(String::as_str)
            .unwrap_or_default(),
        existing
            .get("instanceId")
            .map(String::as_str)
            .unwrap_or_default(),
        existing,
    )?;
    let token = existing
        .get("pendingCreateToken")
        .cloned()
        .ok_or_else(|| WorkflowError::invalid_state("Workflow pending create token is missing"))?;
    Ok(PendingCreateCleanup { identity, token })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_cleanup_only_deletes_pending_create_state() {
        assert!(CLEANUP_PENDING_CREATE_SCRIPT.contains(r#"status ~= "pending_create""#));
        assert!(CLEANUP_PENDING_CREATE_SCRIPT.contains("generation ~= \"1\""));
        assert!(CLEANUP_PENDING_CREATE_SCRIPT.contains("token ~= ARGV[2]"));
        assert!(CLEANUP_PENDING_CREATE_SCRIPT.contains(
            r#"redis.call("DEL", KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[7])"#
        ));
    }

    #[test]
    fn finalize_create_adds_ready_and_workflow_indexes() {
        assert!(
            FINALIZE_CREATE_INSTANCE_SCRIPT.contains(r#"redis.call("SADD", KEYS[2], ARGV[1])"#)
        );
        assert!(
            FINALIZE_CREATE_INSTANCE_SCRIPT.contains(r#"redis.call("SADD", KEYS[3], ARGV[4])"#)
        );
        assert!(
            FINALIZE_CREATE_INSTANCE_SCRIPT
                .contains(r#"redis.call("ZADD", KEYS[4], ARGV[2], ARGV[5])"#)
        );
    }

    #[test]
    fn pending_create_is_not_a_public_status() {
        assert_eq!(PENDING_CREATE_WAIT_ATTEMPTS, 20);
        assert_eq!(PENDING_CREATE_WAIT_DELAY_MS, 25);
        assert_eq!(PENDING_CREATE_TTL_MS, 30_000);
        let mut state = HashMap::new();
        state.insert("status".to_string(), "pending_create".to_string());
        state.insert("pendingExpiresAtMs".to_string(), "100".to_string());
        assert!(is_pending_create(&state));
        assert!(!pending_create_expired(&state, 99));
        assert!(pending_create_expired(&state, 100));
    }
}
