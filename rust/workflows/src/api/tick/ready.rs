use crate::{
    AppState, WORKFLOW_READY_BATCH_SIZE, WorkflowResult, instance_state_key,
    workflow_shard_queue_keys,
};
use wdl_rust_common::redis_eval::StaticRedisScript;

use super::super::{
    DuePromotionConfig, DuePromotionMember, eval_script, parse_ready_token, promote_due_members,
    remove_ready_member_if_state_missing,
};
pub(super) const REMOVE_READY_TOKEN_IF_TERMINAL_SCRIPT: &str = r#"
local generation = redis.call("HGET", KEYS[1], "generation")
local status = redis.call("HGET", KEYS[1], "status")
if generation ~= ARGV[1] then
  return 0
end
if status ~= "completed" and status ~= "failed" and status ~= "terminated" then
  return 0
end
redis.call("SREM", KEYS[2], ARGV[2])
return 1
"#;

static REMOVE_READY_TOKEN_IF_TERMINAL: StaticRedisScript =
    StaticRedisScript::new(REMOVE_READY_TOKEN_IF_TERMINAL_SCRIPT);

const DUE_SCAN_OVERFETCH_FACTOR: usize = 4;

const MOVE_DUE_TOKEN_SCRIPT: &str = r#"
local score = redis.call("ZSCORE", KEYS[1], ARGV[1])
if not score then
  return 0
end
if tonumber(score) > tonumber(ARGV[2]) then
  return 0
end
local status = redis.call("HGET", KEYS[4], "status")
if status ~= "queued" and status ~= "waiting" then
  redis.call("ZREM", KEYS[1], ARGV[1])
  return 0
end
if redis.call("HGET", KEYS[4], "runToken") then
  local lease = tonumber(redis.call("HGET", KEYS[4], "runLeaseExpiresAtMs") or "0")
  if lease > tonumber(ARGV[2]) then
    return 0
  end
  redis.call("HDEL", KEYS[4], "runToken", "runLeaseExpiresAtMs")
end
redis.call("SADD", KEYS[2], ARGV[1])
redis.call("SADD", KEYS[3], ARGV[3])
redis.call("ZREM", KEYS[1], ARGV[1])
return 1
"#;

pub(super) struct ReadyTokenGuard {
    pub(super) ns: String,
    pub(super) workflow_key: String,
    pub(super) instance_id: String,
    pub(super) generation: String,
}

pub(super) struct ReadyTokenIdentity {
    pub(super) ns: String,
    pub(super) workflow_key: String,
    pub(super) instance_id: String,
}

pub(super) async fn move_due_tokens(app: &AppState) -> WorkflowResult<usize> {
    promote_due_members(
        app,
        workflow_shard_queue_keys(),
        DuePromotionConfig {
            total_limit: WORKFLOW_READY_BATCH_SIZE,
            per_shard_limit: WORKFLOW_READY_BATCH_SIZE,
            scan_overfetch_factor: DUE_SCAN_OVERFETCH_FACTOR,
        },
        MOVE_DUE_TOKEN_SCRIPT,
        |token| {
            parse_ready_token(token).map(|(ns, workflow_key, instance_id)| DuePromotionMember {
                member: token.to_string(),
                extra_keys: vec![instance_state_key(&ns, &workflow_key, &instance_id)],
                extra_args: Vec::new(),
            })
        },
    )
    .await
}

pub(super) async fn remove_ready_token(
    app: &AppState,
    shard: usize,
    token: String,
) -> WorkflowResult<()> {
    let key = workflow_shard_queue_keys().ready(shard);
    app.redis
        .with_conn(async |mut conn| {
            redis::cmd("SREM")
                .arg(key)
                .arg(token)
                .query_async::<()>(&mut conn)
                .await
        })
        .await?;
    Ok(())
}

pub(super) async fn remove_ready_token_if_terminal(
    app: &AppState,
    shard: usize,
    token: String,
    guard: ReadyTokenGuard,
) -> WorkflowResult<()> {
    let state_key = instance_state_key(&guard.ns, &guard.workflow_key, &guard.instance_id);
    let ready = workflow_shard_queue_keys().ready(shard);
    eval_script::<i64>(
        app,
        &REMOVE_READY_TOKEN_IF_TERMINAL,
        &[&state_key, &ready],
        &[&guard.generation, &token],
    )
    .await?;
    Ok(())
}

pub(super) async fn remove_ready_token_if_state_missing(
    app: &AppState,
    shard: usize,
    token: String,
    identity: ReadyTokenIdentity,
) -> WorkflowResult<()> {
    let state_key = instance_state_key(&identity.ns, &identity.workflow_key, &identity.instance_id);
    let ready = workflow_shard_queue_keys().ready(shard);
    remove_ready_member_if_state_missing(app, &state_key, &ready, &token).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_stale_token_cleanup_is_generation_guarded() {
        assert!(REMOVE_READY_TOKEN_IF_TERMINAL_SCRIPT.contains("generation ~= ARGV[1]"));
        assert!(REMOVE_READY_TOKEN_IF_TERMINAL_SCRIPT.contains(r#"status ~= "completed""#));
        assert!(REMOVE_READY_TOKEN_IF_TERMINAL_SCRIPT.contains(r#"status ~= "failed""#));
        assert!(REMOVE_READY_TOKEN_IF_TERMINAL_SCRIPT.contains(r#"status ~= "terminated""#));
    }

    #[test]
    fn due_promotion_rechecks_score_and_instance_status_inside_lua() {
        assert!(MOVE_DUE_TOKEN_SCRIPT.contains(r#"redis.call("ZSCORE", KEYS[1], ARGV[1])"#));
        assert!(MOVE_DUE_TOKEN_SCRIPT.contains(r#"tonumber(score) > tonumber(ARGV[2])"#));
        assert!(MOVE_DUE_TOKEN_SCRIPT.contains(r#"redis.call("HGET", KEYS[4], "status")"#));
        assert!(MOVE_DUE_TOKEN_SCRIPT.contains(r#"status ~= "queued" and status ~= "waiting""#));
        assert!(MOVE_DUE_TOKEN_SCRIPT.contains(r#"redis.call("HGET", KEYS[4], "runToken")"#));
        assert!(
            MOVE_DUE_TOKEN_SCRIPT.contains(r#"redis.call("HGET", KEYS[4], "runLeaseExpiresAtMs")"#)
        );
        assert!(MOVE_DUE_TOKEN_SCRIPT.contains(r#"lease > tonumber(ARGV[2])"#));
        assert!(
            MOVE_DUE_TOKEN_SCRIPT
                .contains(r#"redis.call("HDEL", KEYS[4], "runToken", "runLeaseExpiresAtMs")"#)
        );
        assert!(MOVE_DUE_TOKEN_SCRIPT.contains(r#"redis.call("SADD", KEYS[2], ARGV[1])"#));
        assert!(MOVE_DUE_TOKEN_SCRIPT.contains(r#"redis.call("ZREM", KEYS[1], ARGV[1])"#));
    }

    #[test]
    fn due_sweep_overfetches_but_keeps_move_batch_bounded() {
        let source = include_str!("ready.rs");
        let shared_source = include_str!("../sharded_dispatch.rs");
        assert!(source.contains("DUE_SCAN_OVERFETCH_FACTOR"));
        assert!(source.contains("promote_due_members"));
        assert!(source.contains("total_limit: WORKFLOW_READY_BATCH_SIZE"));
        assert!(source.contains("per_shard_limit: WORKFLOW_READY_BATCH_SIZE"));
        assert!(shared_source.contains("fn due_shards_with_due_members"));
        assert!(shared_source.contains(r#".cmd("ZRANGEBYSCORE")"#));
        assert!(shared_source.contains("config.scan_overfetch_factor"));
        assert!(shared_source.contains("while moved < config.total_limit"));
    }
}
