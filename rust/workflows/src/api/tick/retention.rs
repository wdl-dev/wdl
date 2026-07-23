use std::collections::HashMap;

use wdl_rust_common::{redis_eval::append_eval_cmd, time::now_ms};

use crate::{
    AppState, InstanceIdentity, WorkflowError, WorkflowResult, by_version_key, by_worker_key,
    by_workflow_key, retention_key,
};

use super::super::{
    InstanceRouteKeys, identity_from_state, parse_ready_token, workflow_referrer_member,
};

const RETENTION_BATCH_SIZE: usize = 100;

enum RetentionTokenState {
    RemoveToken,
    Wait,
    Cleanup(Box<InstanceIdentity>),
}

enum RetentionAction {
    RemoveToken {
        token: String,
        score: i64,
        state_key: Option<String>,
        observed_status: Option<String>,
        observed_generation: Option<String>,
        observed_created_at_ms: Option<String>,
    },
    Cleanup {
        token: String,
        identity: Box<InstanceIdentity>,
    },
}

pub(super) const REMOVE_RETENTION_TOKEN_SCRIPT: &str = r#"
local score = redis.call("ZSCORE", KEYS[1], ARGV[1])
if not score or tonumber(score) ~= tonumber(ARGV[2]) then
  return 0
end
if #KEYS == 2 then
  local status = redis.call("HGET", KEYS[2], "status") or ""
  local generation = redis.call("HGET", KEYS[2], "generation") or ""
  local created_at_ms = redis.call("HGET", KEYS[2], "createdAtMs") or ""
  if status ~= ARGV[3] or generation ~= ARGV[4] or created_at_ms ~= ARGV[5] then
    return 0
  end
end
return redis.call("ZREM", KEYS[1], ARGV[1])
"#;

pub(super) const CLEANUP_RETENTION_SCRIPT: &str = r#"
local generation = redis.call("HGET", KEYS[1], "generation")
local created_at_ms = redis.call("HGET", KEYS[1], "createdAtMs")
local status = redis.call("HGET", KEYS[1], "status")
if generation ~= ARGV[1] or created_at_ms ~= ARGV[6] then
  return 0
end
if status ~= "completed" and status ~= "failed" and status ~= "terminated" then
  return 0
end
local expires_at = tonumber(redis.call("HGET", KEYS[1], "retentionExpiresAtMs") or "")
if not expires_at or expires_at > tonumber(ARGV[4]) then
  return 0
end
redis.call("DEL", KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[11])
redis.call("SREM", KEYS[7], ARGV[2])
redis.call("SREM", KEYS[8], ARGV[2])
redis.call("ZREM", KEYS[9], ARGV[3])
redis.call("ZREM", KEYS[10], ARGV[5])
return 1
"#;

pub(super) async fn cleanup_retention(app: &AppState) -> WorkflowResult<usize> {
    let now = now_ms();
    let key = retention_key().to_string();
    let tokens: Vec<(String, i64)> = app
        .redis
        .with_conn(async |mut conn| {
            redis::cmd("ZRANGEBYSCORE")
                .arg(&key)
                .arg("-inf")
                .arg(now)
                .arg("WITHSCORES")
                .arg("LIMIT")
                .arg(0)
                .arg(RETENTION_BATCH_SIZE)
                .query_async(&mut conn)
                .await
        })
        .await?;
    if tokens.is_empty() {
        return Ok(0);
    }
    let state_keys = tokens
        .iter()
        .filter_map(|(token, _)| parse_ready_token(token))
        .map(|(ns, workflow_key, instance_id)| {
            InstanceRouteKeys::new(&ns, &workflow_key, &instance_id).state()
        })
        .collect::<Vec<_>>();
    let states: Vec<HashMap<String, String>> = if state_keys.is_empty() {
        Vec::new()
    } else {
        app.redis
            .with_conn(async |mut conn| {
                let mut pipe = redis::pipe();
                for state_key in &state_keys {
                    pipe.cmd("HGETALL").arg(state_key);
                }
                pipe.query_async(&mut conn).await
            })
            .await?
    };
    let actions = plan_retention_actions(tokens, states, now)?;
    let cleaned = apply_retention_actions(app, &key, &actions, now).await?;
    if cleaned > 0 {
        app.metrics.increment(
            "workflow_retention_cleaned",
            &[("outcome", "cleaned")],
            cleaned as f64,
        );
    }
    Ok(cleaned)
}

fn plan_retention_actions(
    tokens: Vec<(String, i64)>,
    states: Vec<HashMap<String, String>>,
    now: i64,
) -> WorkflowResult<Vec<RetentionAction>> {
    let mut states = states.into_iter();
    let mut actions = Vec::new();
    for (token, score) in tokens {
        let Some((ns, workflow_key, instance_id)) = parse_ready_token(&token) else {
            actions.push(RetentionAction::RemoveToken {
                token,
                score,
                state_key: None,
                observed_status: None,
                observed_generation: None,
                observed_created_at_ms: None,
            });
            continue;
        };
        let state_key = InstanceRouteKeys::new(&ns, &workflow_key, &instance_id).state();
        let state = states.next().ok_or_else(|| {
            WorkflowError::internal_error("retention state pipeline reply count mismatch")
        })?;
        match retention_token_state(&ns, &workflow_key, &instance_id, &state, now) {
            RetentionTokenState::RemoveToken => {
                actions.push(RetentionAction::RemoveToken {
                    token,
                    score,
                    state_key: Some(state_key),
                    observed_status: state.get("status").cloned(),
                    observed_generation: state.get("generation").cloned(),
                    observed_created_at_ms: state.get("createdAtMs").cloned(),
                });
            }
            RetentionTokenState::Wait => {}
            RetentionTokenState::Cleanup(identity) => {
                actions.push(RetentionAction::Cleanup { token, identity });
            }
        }
    }
    if states.next().is_some() {
        return Err(WorkflowError::internal_error(
            "retention state pipeline reply count mismatch",
        ));
    }
    Ok(actions)
}

async fn apply_retention_actions(
    app: &AppState,
    retention: &str,
    actions: &[RetentionAction],
    now: i64,
) -> WorkflowResult<usize> {
    if actions.is_empty() {
        return Ok(0);
    }
    let now_arg = now.to_string();
    let mut cleanup_slots = Vec::with_capacity(actions.len());
    let results: Vec<i64> = app
        .redis
        .with_conn(async |mut conn| {
            let mut pipe = redis::pipe();
            for action in actions {
                match action {
                    RetentionAction::RemoveToken {
                        token,
                        score,
                        state_key,
                        observed_status,
                        observed_generation,
                        observed_created_at_ms,
                    } => {
                        let score_arg = score.to_string();
                        let mut keys = vec![retention];
                        if let Some(state_key) = state_key {
                            keys.push(state_key);
                        }
                        append_eval_cmd(
                            &mut pipe,
                            REMOVE_RETENTION_TOKEN_SCRIPT,
                            &keys,
                            &[
                                token,
                                &score_arg,
                                observed_status.as_deref().unwrap_or(""),
                                observed_generation.as_deref().unwrap_or(""),
                                observed_created_at_ms.as_deref().unwrap_or(""),
                            ],
                        );
                        cleanup_slots.push(false);
                    }
                    RetentionAction::Cleanup { token, identity } => {
                        let keys = InstanceRouteKeys::new(
                            &identity.ns,
                            &identity.workflow_key,
                            &identity.instance_id,
                        );
                        let state_key = keys.state();
                        let payloads_key = keys.payloads();
                        let steps_key = keys.steps();
                        let summaries_key = keys.step_summaries();
                        let summary_index_key = keys.step_summary_index();
                        let events_key = keys.events();
                        let event_index_key = keys.event_type_index();
                        let by_worker = by_worker_key(&identity.ns, &identity.worker);
                        let by_version = by_version_key(
                            &identity.ns,
                            &identity.worker,
                            &identity.frozen_version,
                        );
                        let by_workflow =
                            by_workflow_key(&identity.ns, &identity.worker, &identity.workflow_key);
                        // Cross-workflow indexes store workflow referrers; the
                        // workflow-scoped index stores the bare instance id.
                        let referrer =
                            workflow_referrer_member(&identity.workflow_key, &identity.instance_id);
                        append_eval_cmd(
                            &mut pipe,
                            CLEANUP_RETENTION_SCRIPT,
                            &[
                                &state_key,
                                &payloads_key,
                                &steps_key,
                                &summaries_key,
                                &summary_index_key,
                                &events_key,
                                &by_worker,
                                &by_version,
                                retention,
                                &by_workflow,
                                &event_index_key,
                            ],
                            &[
                                &identity.generation,
                                &referrer,
                                token,
                                &now_arg,
                                &identity.instance_id,
                                &identity.created_at_ms,
                            ],
                        );
                        cleanup_slots.push(true);
                    }
                }
            }
            pipe.query_async(&mut conn).await
        })
        .await?;
    if results.len() != cleanup_slots.len() {
        return Err(WorkflowError::internal_error(
            "retention cleanup pipeline reply count mismatch",
        ));
    }
    Ok(results
        .into_iter()
        .zip(cleanup_slots)
        .filter(|(result, is_cleanup)| *is_cleanup && *result == 1)
        .count())
}

fn retention_token_state(
    ns: &str,
    workflow_key: &str,
    instance_id: &str,
    state: &HashMap<String, String>,
    now: i64,
) -> RetentionTokenState {
    if state.is_empty() {
        return RetentionTokenState::RemoveToken;
    }
    let status = state.get("status").map(String::as_str);
    if !matches!(status, Some("completed" | "failed" | "terminated")) {
        return RetentionTokenState::RemoveToken;
    }
    let expires_at = state
        .get("retentionExpiresAtMs")
        .and_then(|raw| raw.parse::<i64>().ok())
        .unwrap_or(now);
    if expires_at > now {
        return RetentionTokenState::Wait;
    }
    match identity_from_state(ns, workflow_key, instance_id, state) {
        Ok(identity) => RetentionTokenState::Cleanup(Box::new(identity)),
        Err(_) => RetentionTokenState::RemoveToken,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_cleanup_is_generation_and_expiry_guarded() {
        assert!(CLEANUP_RETENTION_SCRIPT.contains("generation ~= ARGV[1]"));
        assert!(CLEANUP_RETENTION_SCRIPT.contains("created_at_ms ~= ARGV[6]"));
        assert!(CLEANUP_RETENTION_SCRIPT.contains(r#"status ~= "completed""#));
        assert!(CLEANUP_RETENTION_SCRIPT.contains(r#"status ~= "failed""#));
        assert!(CLEANUP_RETENTION_SCRIPT.contains(r#"status ~= "terminated""#));
        assert!(CLEANUP_RETENTION_SCRIPT.contains("expires_at > tonumber(ARGV[4])"));
        assert!(CLEANUP_RETENTION_SCRIPT.contains(
            r#"redis.call("DEL", KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[11])"#
        ));
        assert!(CLEANUP_RETENTION_SCRIPT.contains(r#"redis.call("ZREM", KEYS[10], ARGV[5])"#));
    }

    #[test]
    fn retention_token_removal_rechecks_score_and_state_snapshot() {
        assert!(
            REMOVE_RETENTION_TOKEN_SCRIPT.contains(r#"redis.call("ZSCORE", KEYS[1], ARGV[1])"#)
        );
        assert!(REMOVE_RETENTION_TOKEN_SCRIPT.contains(r#"redis.call("HGET", KEYS[2], "status")"#));
        assert!(
            REMOVE_RETENTION_TOKEN_SCRIPT.contains(r#"redis.call("HGET", KEYS[2], "generation")"#)
        );
        assert!(
            REMOVE_RETENTION_TOKEN_SCRIPT.contains(r#"redis.call("HGET", KEYS[2], "createdAtMs")"#)
        );
        assert!(REMOVE_RETENTION_TOKEN_SCRIPT.contains("created_at_ms ~= ARGV[5]"));
        assert!(REMOVE_RETENTION_TOKEN_SCRIPT.contains(r#"redis.call("ZREM", KEYS[1], ARGV[1])"#));
    }

    fn retention_state() -> HashMap<String, String> {
        HashMap::from([
            ("ns".to_string(), "demo".to_string()),
            ("worker".to_string(), "shop".to_string()),
            ("frozenVersion".to_string(), "v1".to_string()),
            ("workflowName".to_string(), "orders".to_string()),
            ("workflowKey".to_string(), "wf_abc".to_string()),
            ("className".to_string(), "OrderWorkflow".to_string()),
            ("instanceId".to_string(), "inst-1".to_string()),
            ("generation".to_string(), "1".to_string()),
            ("createdAtMs".to_string(), "10".to_string()),
            ("status".to_string(), "completed".to_string()),
            ("retentionExpiresAtMs".to_string(), "100".to_string()),
        ])
    }

    #[test]
    fn corrupt_terminal_retention_state_removes_only_retention_token() {
        let mut state = retention_state();
        state.insert("workflowKey".to_string(), "wf_other".to_string());

        assert!(matches!(
            retention_token_state("demo", "wf_abc", "inst-1", &state, 100),
            RetentionTokenState::RemoveToken
        ));
    }

    #[test]
    fn future_terminal_retention_token_waits_until_expiry() {
        let state = retention_state();

        assert!(matches!(
            retention_token_state("demo", "wf_abc", "inst-1", &state, 99),
            RetentionTokenState::Wait
        ));
    }

    #[test]
    fn expired_terminal_retention_token_builds_cleanup_identity() {
        let state = retention_state();
        let RetentionTokenState::Cleanup(identity) =
            retention_token_state("demo", "wf_abc", "inst-1", &state, 100)
        else {
            panic!("expected cleanup identity");
        };
        assert_eq!(identity.ns, "demo");
        assert_eq!(identity.workflow_key, "wf_abc");
        assert_eq!(identity.instance_id, "inst-1");
    }

    #[test]
    fn retention_actions_preserve_state_alignment_around_malformed_tokens() {
        let mut waiting = retention_state();
        waiting.insert("instanceId".to_string(), "inst-wait".to_string());
        waiting.insert("retentionExpiresAtMs".to_string(), "101".to_string());
        let mut cleanup = retention_state();
        cleanup.insert("instanceId".to_string(), "inst-clean".to_string());
        let mut corrupt = retention_state();
        corrupt.insert("instanceId".to_string(), "wrong-instance".to_string());

        let actions = plan_retention_actions(
            vec![
                ("malformed".to_string(), 90),
                ("demo\twf_abc\tinst-wait".to_string(), 91),
                ("demo\twf_abc\tinst-clean".to_string(), 92),
                ("demo\twf_abc\tinst-corrupt".to_string(), 93),
            ],
            vec![waiting, cleanup, corrupt],
            100,
        )
        .expect("aligned retention plan");
        let corrupt_state_key = InstanceRouteKeys::new("demo", "wf_abc", "inst-corrupt").state();

        assert_eq!(actions.len(), 3);
        assert!(matches!(
            &actions[0],
            RetentionAction::RemoveToken { token, score: 90, state_key: None, .. }
                if token == "malformed"
        ));
        assert!(matches!(
            &actions[1],
            RetentionAction::Cleanup { token, identity }
                if token == "demo\twf_abc\tinst-clean"
                    && identity.instance_id == "inst-clean"
        ));
        assert!(matches!(
            &actions[2],
            RetentionAction::RemoveToken {
                token,
                score: 93,
                state_key: Some(state_key),
                observed_status: Some(status),
                observed_generation: Some(generation),
                observed_created_at_ms: Some(created_at_ms),
            } if token == "demo\twf_abc\tinst-corrupt"
                && state_key == &corrupt_state_key
                && status == "completed"
                && generation == "1"
                && created_at_ms == "10"
        ));
    }

    #[test]
    fn retention_actions_reject_missing_pipeline_replies() {
        let Err(err) = plan_retention_actions(
            vec![("demo\twf_abc\tinst-1".to_string(), 100)],
            Vec::new(),
            100,
        ) else {
            panic!("missing state reply must fail closed");
        };

        assert_eq!(err.code, "internal_error");
    }
}
