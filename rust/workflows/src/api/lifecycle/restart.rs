use wdl_rust_common::{redis_eval::StaticRedisScript, time::now_ms};

use crate::{
    AppState, LogLevel, WorkflowError, WorkflowResult, by_version_key, by_worker_key,
    by_workflow_key, log, ready_active_key, retention_key, workflow_error_fields,
};

use super::super::{
    InstanceResponse, InstanceRouteKeys, PendingRestartMarker, WorkflowRequest,
    create_pending_restart, ensure_worker_not_deleting, eval_script, identity_from_state,
    instance_id, payload_bytes_arg, pending_restart_marker, read_public_state,
    remove_pending_restart, request_with_active_version, validate_identity,
    verify_active_workflow_current, verify_workflow_def, workflow_referrer_member,
};

use super::common::{
    TransitionSignal, next_generation, stale_transition_response, successful_transition_response,
};

// Marker cleanup is best-effort: the marker TTL expires a leaked member, and
// the caller's original rejection must stay the visible error.
async fn discard_pending_restart(state: &AppState, marker: &PendingRestartMarker) {
    if let Err(err) = remove_pending_restart(state, marker).await {
        log(
            state,
            LogLevel::Warn,
            "pending_restart_cleanup_failed",
            workflow_error_fields(&err),
        );
    }
}

const RESTART_SCRIPT: &str = r#"
local marker_score = redis.call("ZSCORE", KEYS[16], ARGV[12])
local marker_time = redis.call("TIME")
local marker_now = tonumber(marker_time[1]) * 1000 + math.floor(tonumber(marker_time[2]) / 1000)
if not marker_score or tonumber(marker_score) <= marker_now then
  return -1
end
local status = redis.call("HGET", KEYS[1], "status")
if not status then
  return 0
end
local generation = redis.call("HGET", KEYS[1], "generation")
local created_at_ms = redis.call("HGET", KEYS[1], "createdAtMs")
if generation ~= ARGV[1] or created_at_ms ~= ARGV[13] then
  return 0
end
redis.call("DEL", KEYS[2])
redis.call("DEL", KEYS[3])
redis.call("DEL", KEYS[4])
redis.call("DEL", KEYS[5])
redis.call("DEL", KEYS[6])
redis.call("DEL", KEYS[15])
redis.call("HSET", KEYS[6], "params", ARGV[8])
redis.call("HSET", KEYS[1], "status", "queued", "generation", ARGV[2], "updatedAtMs", ARGV[3], "restartedAtMs", ARGV[3], "frozenVersion", ARGV[4], "className", ARGV[5], "payloadsKey", KEYS[6], "paramsRef", "params", "eventSeq", "0", "payloadBytes", ARGV[9])
redis.call("HDEL", KEYS[1], "outputRef", "errorRef", "errorCode", "errorMessage", "completedAtMs", "failedAtMs", "terminatedAtMs", "pausedAtMs", "resumedAtMs", "retentionExpiresAtMs")
redis.call("HDEL", KEYS[1], "runToken", "runLeaseExpiresAtMs", "waitingEventIndexPrefix")
redis.call("SADD", KEYS[7], ARGV[6])
redis.call("SADD", KEYS[13], ARGV[10])
redis.call("ZREM", KEYS[16], ARGV[12])
redis.call("ZREM", KEYS[8], ARGV[6])
redis.call("SREM", KEYS[9], ARGV[7])
redis.call("SADD", KEYS[10], ARGV[7])
redis.call("SADD", KEYS[11], ARGV[7])
redis.call("ZREM", KEYS[12], ARGV[6])
redis.call("ZADD", KEYS[14], ARGV[3], ARGV[11])
return 1
"#;

static RESTART: StaticRedisScript = StaticRedisScript::new(RESTART_SCRIPT);

const RESTART_LEASE_EXPIRED: i64 = -1;

pub(crate) async fn restart_instance(
    state: &AppState,
    req: WorkflowRequest,
) -> WorkflowResult<InstanceResponse> {
    let req = request_with_active_version(state, req).await?;
    validate_identity(&req)?;
    let id = instance_id(&req)?.to_string();
    let mut existing = read_public_state(state, &req).await?;
    if existing.is_empty() {
        return Err(WorkflowError::not_found("Workflow instance not found"));
    }
    let identity = identity_from_state(&req.ns, &req.workflow_key, &id, &existing)?;
    let next_generation = next_generation(&identity)?;
    let keys = InstanceRouteKeys::new(&identity.ns, &identity.workflow_key, &identity.instance_id);
    let state_key = keys.state();
    let steps_key = keys.steps();
    let summaries_key = keys.step_summaries();
    let summary_index_key = keys.step_summary_index();
    let events_key = keys.events();
    let event_index_key = keys.event_type_index();
    let payloads_key = keys.payloads();
    let params_ref = existing
        .get("paramsRef")
        .cloned()
        .ok_or_else(|| WorkflowError::invalid_state("Workflow state missing params ref"))?;
    let params_payloads_key = payloads_key.clone();
    let params_json: String = state
        .redis
        .with_conn(async |mut conn| {
            redis::cmd("HGET")
                .arg(&params_payloads_key)
                .arg(&params_ref)
                .query_async::<Option<String>>(&mut conn)
                .await
        })
        .await?
        .ok_or_else(|| WorkflowError::payload_missing("Workflow params payload is missing"))?;
    ensure_worker_not_deleting(state, &req.ns, &req.worker).await?;
    let pending_restart = pending_restart_marker(state, &req, &id);
    create_pending_restart(state, &pending_restart).await?;
    if let Err(err) = verify_active_workflow_current(state, &req).await {
        discard_pending_restart(state, &pending_restart).await;
        return Err(err);
    }
    if let Err(err) = verify_workflow_def(state, &req).await {
        discard_pending_restart(state, &pending_restart).await;
        return Err(err);
    }
    if let Err(err) = ensure_worker_not_deleting(state, &req.ns, &req.worker).await {
        discard_pending_restart(state, &pending_restart).await;
        return Err(err);
    }
    let shard = keys.shard();
    let ready = keys.ready();
    let due = keys.due();
    let token = keys.token();
    let shard_arg = shard.to_string();
    let referrer_member = workflow_referrer_member(&identity.workflow_key, &identity.instance_id);
    let old_by_version = by_version_key(&identity.ns, &identity.worker, &identity.frozen_version);
    let new_by_version = by_version_key(&req.ns, &req.worker, &req.frozen_version);
    let by_worker = by_worker_key(&req.ns, &req.worker);
    let by_workflow = by_workflow_key(&req.ns, &req.worker, &req.workflow_key);
    let retention = retention_key().to_string();
    let now = now_ms().to_string();
    let expected_generation = identity.generation.clone();
    let stored_generation = next_generation.clone();
    let stored_now = now.clone();
    let stored_payloads_key = payloads_key.clone();
    let request_version = req.frozen_version.clone();
    let params_bytes = payload_bytes_arg(&params_json);
    let updated: i64 = match eval_script(
        state,
        &RESTART,
        &[
            &state_key,
            &steps_key,
            &summaries_key,
            &summary_index_key,
            &events_key,
            &payloads_key,
            &ready,
            &due,
            &old_by_version,
            &new_by_version,
            &by_worker,
            &retention,
            ready_active_key(),
            &by_workflow,
            &event_index_key,
            &pending_restart.key,
        ],
        &[
            &expected_generation,
            &next_generation,
            &now,
            &request_version,
            &req.class_name,
            &token,
            &referrer_member,
            &params_json,
            &params_bytes,
            &shard_arg,
            &identity.instance_id,
            &pending_restart.member,
            &identity.created_at_ms,
        ],
    )
    .await
    {
        Ok(updated) => updated,
        Err(err) => {
            discard_pending_restart(state, &pending_restart).await;
            return Err(err);
        }
    };
    if updated == RESTART_LEASE_EXPIRED {
        return Err(WorkflowError::conflict(
            "Workflow restart target lease expired; retry restart",
        ));
    }
    if updated != 1 {
        discard_pending_restart(state, &pending_restart).await;
        return stale_transition_response(state, &identity, &id).await;
    }
    existing.insert("status".to_string(), "queued".to_string());
    existing.insert("generation".to_string(), stored_generation);
    existing.insert("updatedAtMs".to_string(), stored_now.clone());
    existing.insert("restartedAtMs".to_string(), stored_now);
    existing.insert("frozenVersion".to_string(), req.frozen_version);
    existing.insert("className".to_string(), req.class_name);
    existing.insert("payloadsKey".to_string(), stored_payloads_key);
    existing.insert("paramsRef".to_string(), "params".to_string());
    existing.insert("eventSeq".to_string(), "0".to_string());
    for field in [
        "outputRef",
        "errorRef",
        "errorCode",
        "errorMessage",
        "completedAtMs",
        "failedAtMs",
        "terminatedAtMs",
        "pausedAtMs",
        "resumedAtMs",
        "retentionExpiresAtMs",
    ] {
        existing.remove(field);
    }
    successful_transition_response(
        state,
        &identity,
        &id,
        &existing,
        TransitionSignal {
            outcome: "restarted",
            event: "workflow_instance_restarted",
            progress_status: "queued",
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_initializes_payload_counter_from_params() {
        assert!(RESTART_SCRIPT.contains(r#"redis.call("HSET", KEYS[6], "params", ARGV[8])"#));
        assert!(RESTART_SCRIPT.contains(r#""payloadBytes", ARGV[9]"#));
    }

    #[test]
    fn restart_updates_class_name_for_active_version() {
        assert!(
            RESTART_SCRIPT
                .contains(r#"local marker_score = redis.call("ZSCORE", KEYS[16], ARGV[12])"#)
        );
        assert!(RESTART_SCRIPT.contains(r#"local marker_time = redis.call("TIME")"#));
        assert!(RESTART_SCRIPT.contains(r#""className", ARGV[5]"#));
        assert!(RESTART_SCRIPT.contains(r#"redis.call("SADD", KEYS[7], ARGV[6])"#));
        assert!(RESTART_SCRIPT.contains(r#"redis.call("SADD", KEYS[13], ARGV[10])"#));
        assert!(RESTART_SCRIPT.contains(r#"redis.call("ZREM", KEYS[16], ARGV[12])"#));
        assert!(RESTART_SCRIPT.contains(r#"redis.call("ZADD", KEYS[14], ARGV[3], ARGV[11])"#));
        let lease_check = RESTART_SCRIPT.find("if not marker_score").unwrap();
        let incarnation_check = RESTART_SCRIPT
            .find(r#"created_at_ms ~= ARGV[13]"#)
            .expect("restart must reject a recreated instance");
        let first_write = RESTART_SCRIPT
            .find(r#"redis.call("DEL", KEYS[2])"#)
            .unwrap();
        assert!(lease_check < first_write);
        assert!(incarnation_check < first_write);
    }

    #[test]
    fn restart_progress_uses_post_transition_identity() {
        let source = include_str!("restart.rs");
        let implementation = source
            .split("\n#[cfg(test)]")
            .next()
            .expect("restart implementation should precede tests");
        assert!(
            implementation
                .contains("successful_transition_response(\n        state,\n        &identity,")
        );
        assert!(implementation.contains("event: \"workflow_instance_restarted\""));
        assert!(implementation.contains("progress_status: \"queued\""));
    }
}
