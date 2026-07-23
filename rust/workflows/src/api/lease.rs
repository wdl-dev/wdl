use wdl_rust_common::{redis_eval::StaticRedisScript, time::now_ms};

use crate::{AppState, InstanceIdentity, WorkflowResult, ready_active_key};

use super::{InstanceRouteKeys, eval_script};

const CLAIM_RUN_SCRIPT: &str = r#"
local status = redis.call("HGET", KEYS[1], "status")
if status ~= ARGV[1] then
  return 0
end
local generation = redis.call("HGET", KEYS[1], "generation")
local created_at_ms = redis.call("HGET", KEYS[1], "createdAtMs")
if generation ~= ARGV[2] or created_at_ms ~= ARGV[6] then
  return 0
end
if redis.call("HGET", KEYS[1], "runToken") then
  local lease = tonumber(redis.call("HGET", KEYS[1], "runLeaseExpiresAtMs") or "0")
  if lease > tonumber(ARGV[5]) then
    return 0
  end
  redis.call("HDEL", KEYS[1], "runToken", "runLeaseExpiresAtMs")
end
redis.call("HSET", KEYS[1],
  "status", "running",
  "runToken", ARGV[3],
  "runLeaseExpiresAtMs", ARGV[4],
  "updatedAtMs", ARGV[5])
return 1
"#;

const RELEASE_RUN_SCRIPT: &str = r#"
local generation = redis.call("HGET", KEYS[1], "generation")
local token = redis.call("HGET", KEYS[1], "runToken")
if generation ~= ARGV[1] or token ~= ARGV[2] then
  return 0
end
redis.call("HSET", KEYS[1], "status", ARGV[3], "updatedAtMs", ARGV[4])
redis.call("HDEL", KEYS[1], "runToken", "runLeaseExpiresAtMs")
return 1
"#;

const CLEAR_SUSPENDED_RUN_SCRIPT: &str = r#"
local generation = redis.call("HGET", KEYS[1], "generation")
local token = redis.call("HGET", KEYS[1], "runToken")
if generation ~= ARGV[1] or token ~= ARGV[2] then
  return 0
end
redis.call("HSET", KEYS[1], "status", "waiting", "updatedAtMs", ARGV[4])
redis.call("HDEL", KEYS[1], "runToken", "runLeaseExpiresAtMs")
local waiting_event_prefix = redis.call("HGET", KEYS[1], "waitingEventIndexPrefix")
if waiting_event_prefix then
  local pending_events = redis.call("ZRANGEBYLEX", KEYS[3], "[" .. waiting_event_prefix, "[" .. waiting_event_prefix .. "~", "LIMIT", 0, 1)
  if #pending_events > 0 then
    return 1
  end
end
redis.call("SREM", KEYS[2], ARGV[3])
return 1
"#;

const REQUEUE_EXPIRED_RUN_SCRIPT: &str = r#"
local status = redis.call("HGET", KEYS[1], "status")
if status ~= "running" then
  return 0
end
local lease = tonumber(redis.call("HGET", KEYS[1], "runLeaseExpiresAtMs") or "0")
if lease > tonumber(ARGV[1]) then
  return 0
end
local generation = redis.call("HGET", KEYS[1], "generation")
local created_at_ms = redis.call("HGET", KEYS[1], "createdAtMs")
if generation ~= ARGV[2] or created_at_ms ~= ARGV[5] then
  return 0
end
redis.call("HSET", KEYS[1], "status", "queued", "updatedAtMs", ARGV[1])
redis.call("HDEL", KEYS[1], "runToken", "runLeaseExpiresAtMs")
redis.call("SADD", KEYS[2], ARGV[3])
redis.call("SADD", KEYS[3], ARGV[4])
return 1
"#;

static CLAIM_RUN: StaticRedisScript = StaticRedisScript::new(CLAIM_RUN_SCRIPT);
static RELEASE_RUN: StaticRedisScript = StaticRedisScript::new(RELEASE_RUN_SCRIPT);
static CLEAR_SUSPENDED_RUN: StaticRedisScript = StaticRedisScript::new(CLEAR_SUSPENDED_RUN_SCRIPT);
static REQUEUE_EXPIRED_RUN: StaticRedisScript = StaticRedisScript::new(REQUEUE_EXPIRED_RUN_SCRIPT);

pub(super) struct RunClaim {
    pub(super) token: String,
}

fn run_lease_expires_at_ms(app: &AppState, now: i64) -> i64 {
    let lease_ms = i64::try_from(app.config.run_lease_ms).unwrap_or(i64::MAX);
    now.saturating_add(lease_ms)
}

fn run_claim_token(
    instance_id: &str,
    identity: &InstanceIdentity,
    now: i64,
    sequence: u64,
) -> String {
    format!(
        "{}:{}:{}:{}:{}",
        instance_id, identity.workflow_key, identity.instance_id, now, sequence
    )
}

pub(super) async fn claim_run(
    app: &AppState,
    identity: &InstanceIdentity,
    expected_status: &str,
) -> WorkflowResult<Option<RunClaim>> {
    let keys = InstanceRouteKeys::new(&identity.ns, &identity.workflow_key, &identity.instance_id);
    let state_key = keys.state();
    let now = now_ms();
    let token = run_claim_token(
        &app.instance_id,
        identity,
        now,
        app.next_run_claim_sequence(),
    );
    let redis_token = token.clone();
    let expires_at = run_lease_expires_at_ms(app, now).to_string();
    let now_arg = now.to_string();
    let expected_generation = identity.generation.clone();
    let claimed: i64 = eval_script(
        app,
        &CLAIM_RUN,
        &[&state_key],
        &[
            expected_status,
            &expected_generation,
            &redis_token,
            &expires_at,
            &now_arg,
            &identity.created_at_ms,
        ],
    )
    .await?;
    Ok((claimed == 1).then_some(RunClaim { token }))
}

pub(super) async fn release_run_claim(
    app: &AppState,
    identity: &InstanceIdentity,
    claim: &RunClaim,
    status: &str,
) -> WorkflowResult<bool> {
    let keys = InstanceRouteKeys::new(&identity.ns, &identity.workflow_key, &identity.instance_id);
    let state_key = keys.state();
    let now = now_ms().to_string();
    let generation = identity.generation.clone();
    let token = claim.token.clone();
    let status = status.to_string();
    let released: i64 = eval_script(
        app,
        &RELEASE_RUN,
        &[&state_key],
        &[&generation, &token, &status, &now],
    )
    .await?;
    Ok(released == 1)
}

pub(super) async fn clear_suspended_run_claim(
    app: &AppState,
    identity: &InstanceIdentity,
    claim: &RunClaim,
) -> WorkflowResult<bool> {
    let keys = InstanceRouteKeys::new(&identity.ns, &identity.workflow_key, &identity.instance_id);
    let state_key = keys.state();
    let ready = keys.ready();
    let event_index = keys.event_type_index();
    let ready_token = keys.token();
    let generation = identity.generation.clone();
    let token = claim.token.clone();
    let now = now_ms().to_string();
    let cleared: i64 = eval_script(
        app,
        &CLEAR_SUSPENDED_RUN,
        &[&state_key, &ready, &event_index],
        &[&generation, &token, &ready_token, &now],
    )
    .await?;
    Ok(cleared == 1)
}

pub(super) async fn requeue_expired_run_claim(
    app: &AppState,
    identity: &InstanceIdentity,
) -> WorkflowResult<bool> {
    let keys = InstanceRouteKeys::new(&identity.ns, &identity.workflow_key, &identity.instance_id);
    let state_key = keys.state();
    let shard = keys.shard();
    let ready = keys.ready();
    let token = keys.token();
    let now = now_ms().to_string();
    let generation = identity.generation.clone();
    let shard_arg = shard.to_string();
    let requeued: i64 = eval_script(
        app,
        &REQUEUE_EXPIRED_RUN,
        &[&state_key, &ready, ready_active_key()],
        &[
            &now,
            &generation,
            &token,
            &shard_arg,
            &identity.created_at_ms,
        ],
    )
    .await?;
    Ok(requeued == 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suspended_claim_clear_removes_ready_token_inside_fenced_script() {
        assert!(CLEAR_SUSPENDED_RUN_SCRIPT.contains("generation ~= ARGV[1]"));
        assert!(CLEAR_SUSPENDED_RUN_SCRIPT.contains("token ~= ARGV[2]"));
        assert!(CLEAR_SUSPENDED_RUN_SCRIPT.contains(
            r#"redis.call("HSET", KEYS[1], "status", "waiting", "updatedAtMs", ARGV[4])"#
        ));
        assert!(CLEAR_SUSPENDED_RUN_SCRIPT.contains(r#"redis.call("SREM", KEYS[2], ARGV[3])"#));
    }

    #[test]
    fn suspended_claim_clear_preserves_ready_token_for_pending_wait_event() {
        assert!(
            CLEAR_SUSPENDED_RUN_SCRIPT
                .contains(r#"redis.call("HGET", KEYS[1], "waitingEventIndexPrefix")"#)
        );
        assert!(CLEAR_SUSPENDED_RUN_SCRIPT.contains(r#"redis.call("ZRANGEBYLEX", KEYS[3]"#));
        assert!(CLEAR_SUSPENDED_RUN_SCRIPT.contains("if #pending_events > 0 then"));
        assert!(
            CLEAR_SUSPENDED_RUN_SCRIPT
                .find("if #pending_events > 0 then")
                .expect("pending event branch")
                < CLEAR_SUSPENDED_RUN_SCRIPT
                    .find(r#"redis.call("SREM", KEYS[2], ARGV[3])"#)
                    .expect("ready token removal")
        );
    }

    #[test]
    fn expired_claim_requeue_marks_ready_shard_active() {
        assert!(REQUEUE_EXPIRED_RUN_SCRIPT.contains(r#"redis.call("SADD", KEYS[2], ARGV[3])"#));
        assert!(REQUEUE_EXPIRED_RUN_SCRIPT.contains(r#"redis.call("SADD", KEYS[3], ARGV[4])"#));
    }

    #[test]
    fn run_claim_rejects_existing_run_token() {
        assert!(CLAIM_RUN_SCRIPT.contains(r#"redis.call("HGET", KEYS[1], "runToken")"#));
        assert!(CLAIM_RUN_SCRIPT.contains(r#"lease > tonumber(ARGV[5])"#));
        assert!(
            CLAIM_RUN_SCRIPT
                .contains(r#"redis.call("HDEL", KEYS[1], "runToken", "runLeaseExpiresAtMs")"#)
        );
    }

    #[test]
    fn run_admission_rejects_a_recreated_instance() {
        let claim_write = CLAIM_RUN_SCRIPT
            .find(r#"redis.call("HSET", KEYS[1]"#)
            .expect("claim state write");
        let claim_fence = CLAIM_RUN_SCRIPT
            .find(r#"created_at_ms ~= ARGV[6]"#)
            .expect("claim incarnation fence");
        assert!(claim_fence < claim_write);

        let requeue_write = REQUEUE_EXPIRED_RUN_SCRIPT
            .find(r#"redis.call("HSET", KEYS[1]"#)
            .expect("requeue state write");
        let requeue_fence = REQUEUE_EXPIRED_RUN_SCRIPT
            .find(r#"created_at_ms ~= ARGV[5]"#)
            .expect("requeue incarnation fence");
        assert!(requeue_fence < requeue_write);
    }

    #[test]
    fn run_claim_token_is_unique_within_one_millisecond() {
        let identity = InstanceIdentity {
            ns: "demo".to_string(),
            worker: "shop".to_string(),
            frozen_version: "v1".to_string(),
            workflow_name: "orders".to_string(),
            workflow_key: "wf_orders".to_string(),
            class_name: "OrderWorkflow".to_string(),
            instance_id: "order-1".to_string(),
            generation: "1".to_string(),
            created_at_ms: "123".to_string(),
        };

        assert_ne!(
            run_claim_token("wf-process", &identity, 42, 1),
            run_claim_token("wf-process", &identity, 42, 2)
        );
    }
}
