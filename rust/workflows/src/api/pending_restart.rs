use wdl_rust_common::{redis_eval::StaticRedisScript, time::random_hex_64};

use crate::{AppState, WorkflowError, WorkflowResult, pending_version_key};

use super::{LifecycleBlocker, WorkflowRequest, eval_script};

const PENDING_RESTART_TTL_MS: i64 = 30_000;
const PENDING_RESTART_KEY_TTL_MS: i64 = 60_000;

const CREATE_PENDING_RESTART_SCRIPT: &str = r#"
local time = redis.call("TIME")
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", now)
redis.call("ZADD", KEYS[1], now + tonumber(ARGV[2]), ARGV[1])
redis.call("PEXPIRE", KEYS[1], ARGV[3])
return 1
"#;

const READ_PENDING_RESTART_SCRIPT: &str = r#"
local time = redis.call("TIME")
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
if ARGV[1] == "1" then
  redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", now)
end
local active_min = "(" .. tostring(now)
local count = redis.call("ZCOUNT", KEYS[1], active_min, "+inf")
local members = redis.call("ZRANGEBYSCORE", KEYS[1], active_min, "+inf", "LIMIT", 0, ARGV[2])
return { count, members }
"#;

static CREATE_PENDING_RESTART: StaticRedisScript =
    StaticRedisScript::new(CREATE_PENDING_RESTART_SCRIPT);
static READ_PENDING_RESTART: StaticRedisScript =
    StaticRedisScript::new(READ_PENDING_RESTART_SCRIPT);

pub(super) struct PendingRestartMarker {
    pub(super) key: String,
    pub(super) member: String,
}

pub(super) fn pending_restart_marker(
    state: &AppState,
    req: &WorkflowRequest,
    instance_id: &str,
) -> PendingRestartMarker {
    PendingRestartMarker {
        key: pending_version_key(&req.ns, &req.worker, &req.frozen_version),
        member: format!(
            "{}\t{}\t{}:{}",
            req.workflow_key,
            instance_id,
            state.instance_id,
            random_hex_64()
        ),
    }
}

pub(super) async fn create_pending_restart(
    state: &AppState,
    marker: &PendingRestartMarker,
) -> WorkflowResult<()> {
    let marker_ttl = PENDING_RESTART_TTL_MS.to_string();
    let key_ttl = PENDING_RESTART_KEY_TTL_MS.to_string();
    let _: i64 = eval_script(
        state,
        &CREATE_PENDING_RESTART,
        &[&marker.key],
        &[&marker.member, &marker_ttl, &key_ttl],
    )
    .await?;
    Ok(())
}

pub(super) async fn remove_pending_restart(
    state: &AppState,
    marker: &PendingRestartMarker,
) -> WorkflowResult<()> {
    let key = marker.key.clone();
    let member = marker.member.clone();
    state
        .redis
        .with_conn(async move |mut conn| {
            redis::cmd("ZREM")
                .arg(key)
                .arg(member)
                .query_async::<()>(&mut conn)
                .await
        })
        .await?;
    Ok(())
}

pub(super) async fn active_pending_restart_blockers(
    state: &AppState,
    ns: &str,
    worker: &str,
    version: &str,
    allow_cleanup: bool,
    limit: usize,
) -> WorkflowResult<(usize, Vec<LifecycleBlocker>)> {
    let key = pending_version_key(ns, worker, version);
    let cleanup = if allow_cleanup { "1" } else { "0" };
    let limit = limit.to_string();
    let (count, members): (usize, Vec<String>) =
        eval_script(state, &READ_PENDING_RESTART, &[&key], &[cleanup, &limit]).await?;
    let blockers = members
        .into_iter()
        .map(|member| parse_pending_restart_member(&member))
        .collect::<WorkflowResult<Vec<_>>>()?;
    Ok((count, blockers))
}

fn parse_pending_restart_member(member: &str) -> WorkflowResult<LifecycleBlocker> {
    let mut parts = member.split('\t');
    let workflow_key = parts.next().unwrap_or_default();
    let instance_id = parts.next().unwrap_or_default();
    let token = parts.next().unwrap_or_default();
    if workflow_key.is_empty()
        || instance_id.is_empty()
        || token.is_empty()
        || parts.next().is_some()
    {
        return Err(WorkflowError::invalid_state(
            "Pending workflow restart blocker is corrupt",
        ));
    }
    Ok(LifecycleBlocker {
        workflow_key: workflow_key.to_string(),
        instance_id: instance_id.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_restart_member_preserves_blocker_identity() {
        let blocker = parse_pending_restart_member("wf_key\tinstance-1\ttask:token").unwrap();
        assert_eq!(blocker.workflow_key, "wf_key");
        assert_eq!(blocker.instance_id, "instance-1");
        assert_eq!(PENDING_RESTART_TTL_MS, 30_000);
        assert_eq!(PENDING_RESTART_KEY_TTL_MS, 60_000);
    }

    #[test]
    fn pending_restart_member_rejects_malformed_state() {
        for member in [
            "",
            "wf_key",
            "wf_key\tinstance-1",
            "wf\tinstance\ttoken\textra",
        ] {
            assert!(parse_pending_restart_member(member).is_err());
        }
    }
}
