use redis::AsyncCommands;
use serde_json::{Value as JsonValue, json};
use wdl_rust_common::redis_eval::StaticRedisScript;

use crate::{
    AppState, LogLevel, SERVICE, SchedulerError, SchedulerResult, log, now_ms, post_runtime,
    redis_fields_with_error, scheduler_fields_with_error,
};

use super::reference::{CronEntry, RefVerdict, classify_ref, cron_worker_key, parse_ref};
use super::slot::{lease_key, next_fire_ms, slot_expire_at, slot_key, slot_ms_for};

const CRON_CLAIM_ADVANCE_SCRIPT: &str = r#"
if redis.call('SISMEMBER', KEYS[1], ARGV[3]) ~= 1 then
  return 0
end
local meta = redis.call('HGET', KEYS[4], '__meta__')
local entry = redis.call('HGET', KEYS[4], ARGV[6])
if meta ~= ARGV[5] or entry ~= ARGV[7] then
  return 2
end
if redis.call('SET', KEYS[2], ARGV[1], 'NX', 'EX', ARGV[2]) then
  redis.call('SREM', KEYS[1], ARGV[3])
  redis.call('SADD', KEYS[3], ARGV[3])
  redis.call('EXPIREAT', KEYS[3], ARGV[4])
  return 1
end
return 0
"#;

static CRON_CLAIM_ADVANCE: StaticRedisScript = StaticRedisScript::new(CRON_CLAIM_ADVANCE_SCRIPT);

#[derive(Debug, PartialEq, Eq)]
enum CronClaimOutcome {
    LeaseLost,
    Claimed,
    ConfigChanged,
}

#[derive(Debug, PartialEq, Eq)]
enum CronClaimDisposition {
    LeaseLost,
    Claimed,
    RetryConfig,
    DeferConfig,
}

fn cron_claim_outcome(code: i64) -> SchedulerResult<CronClaimOutcome> {
    match code {
        0 => Ok(CronClaimOutcome::LeaseLost),
        1 => Ok(CronClaimOutcome::Claimed),
        2 => Ok(CronClaimOutcome::ConfigChanged),
        other => Err(SchedulerError::internal_error(format!(
            "unexpected cron claim result {other}"
        ))),
    }
}

fn cron_claim_disposition(
    outcome: CronClaimOutcome,
    config_attempt: usize,
) -> CronClaimDisposition {
    match outcome {
        CronClaimOutcome::LeaseLost => CronClaimDisposition::LeaseLost,
        CronClaimOutcome::Claimed => CronClaimDisposition::Claimed,
        CronClaimOutcome::ConfigChanged if config_attempt == 0 => CronClaimDisposition::RetryConfig,
        CronClaimOutcome::ConfigChanged => CronClaimDisposition::DeferConfig,
    }
}

struct CronClaimSnapshot<'a> {
    hash_key: &'a str,
    cron_id: &'a str,
    meta: &'a str,
    entry: &'a str,
}

async fn claim_and_advance_ref(
    state: &AppState,
    reference: &str,
    from_slot: i64,
    to_slot: i64,
    snapshot: CronClaimSnapshot<'_>,
) -> SchedulerResult<CronClaimOutcome> {
    let reference = reference.to_string();
    let from_key = slot_key(from_slot);
    let lease = lease_key(from_slot, &reference);
    let to_key = slot_key(to_slot);
    let to_expire_at = slot_expire_at(to_slot);
    let instance_id = state.instance_id.clone();
    let lease_ttl_s = state.config.lease_ttl_s;
    let lease_ttl_s_arg = lease_ttl_s.to_string();
    let to_expire_at_arg = to_expire_at.to_string();
    state
        .redis
        .with_conn(async |mut conn| {
            CRON_CLAIM_ADVANCE
                .prepare_invoke(
                    &[
                        from_key.as_str(),
                        lease.as_str(),
                        to_key.as_str(),
                        snapshot.hash_key,
                    ],
                    &[
                        instance_id.as_str(),
                        lease_ttl_s_arg.as_str(),
                        reference.as_str(),
                        to_expire_at_arg.as_str(),
                        snapshot.meta,
                        snapshot.cron_id,
                        snapshot.entry,
                    ],
                )
                .invoke_async::<i64>(&mut conn)
                .await
        })
        .await
        .map_err(SchedulerError::from)
        .and_then(cron_claim_outcome)
}

async fn remove_ref_from_slot(
    state: &AppState,
    slot_ms: i64,
    reference: &str,
) -> SchedulerResult<()> {
    let key = slot_key(slot_ms);
    let reference = reference.to_string();
    let _: i64 = state
        .redis
        .with_conn(async |mut conn| conn.srem(key, reference).await)
        .await?;
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
enum CronDispatchDecision {
    RemoveInvalidRef { error_message: String },
    AdvanceOnly { next_slot: i64 },
    Fire { next_slot: i64 },
}

fn cron_dispatch_decision(entry: &CronEntry, slot_ms: i64, tick_now: i64) -> CronDispatchDecision {
    let next_fire = match next_fire_ms(&entry.cron, &entry.timezone, tick_now) {
        Ok(ms) => ms,
        Err(err) => {
            return CronDispatchDecision::RemoveInvalidRef {
                error_message: err.message,
            };
        }
    };
    let next_slot = slot_ms_for(next_fire);
    if slot_ms < slot_ms_for(tick_now) {
        CronDispatchDecision::AdvanceOnly { next_slot }
    } else {
        CronDispatchDecision::Fire { next_slot }
    }
}

fn cron_ref_lookup_cmd(cron_hash_key: &str, cron_id: &str) -> redis::Cmd {
    let mut command = redis::cmd("HMGET");
    command.arg(cron_hash_key).arg("__meta__").arg(cron_id);
    command
}

async fn process_ref(
    state: AppState,
    reference: String,
    slot_ms: i64,
    tick_now: i64,
) -> SchedulerResult<()> {
    let Some(parts) = parse_ref(&reference) else {
        state
            .metrics
            .increment("cron_stale_refs_cleaned", &[("service", SERVICE)], 1.0);
        remove_ref_from_slot(&state, slot_ms, &reference).await?;
        return Ok(());
    };
    let cron_hash_key = cron_worker_key(&parts.ns, &parts.worker);
    let cron_id = parts.cron_id.clone();
    for config_attempt in 0..=1 {
        let (meta_str, entry_str): (Option<String>, Option<String>) = state
            .redis
            .with_conn(async |mut conn| {
                cron_ref_lookup_cmd(&cron_hash_key, &cron_id)
                    .query_async(&mut conn)
                    .await
            })
            .await?;

        let (entry, active_version) =
            match classify_ref(&parts, meta_str.clone(), entry_str.clone()) {
                RefVerdict::Fire {
                    entry,
                    active_version,
                } => (entry, active_version),
                verdict @ (RefVerdict::Stale(_) | RefVerdict::Corrupt) => {
                    let logged_reason = match verdict {
                        RefVerdict::Stale(reason) => reason,
                        RefVerdict::Corrupt => "corrupt",
                        RefVerdict::Fire { .. } => unreachable!(),
                    };
                    state.metrics.increment(
                        "cron_stale_refs_cleaned",
                        &[("service", SERVICE)],
                        1.0,
                    );
                    log(
                        &state,
                        LogLevel::Info,
                        "cron_ref_stale",
                        json!({
                            "ns": parts.ns,
                            "worker": parts.worker,
                            "cron_id": parts.cron_id,
                            "gen": parts.r#gen,
                            "slot": slot_ms,
                            "reason": logged_reason,
                        }),
                    );
                    remove_ref_from_slot(&state, slot_ms, &reference).await?;
                    return Ok(());
                }
            };
        let expected_meta = meta_str.expect("fireable cron ref must have metadata");
        let expected_entry = entry_str.expect("fireable cron ref must have an entry");

        // Lease-before-advance and advance-before-fire preserve at-most-once
        // delivery per slot. A stranded previous-slot ref advances without
        // firing, matching the no-catch-up contract.
        let decision = cron_dispatch_decision(&entry, slot_ms, tick_now);
        let next_slot = match &decision {
            CronDispatchDecision::RemoveInvalidRef { error_message } => {
                state
                    .metrics
                    .increment("cron_stale_refs_cleaned", &[("service", SERVICE)], 1.0);
                log(
                    &state,
                    LogLevel::Warn,
                    "cron_ref_stale",
                    json!({
                        "ns": parts.ns,
                        "worker": parts.worker,
                        "cron_id": parts.cron_id,
                        "gen": parts.r#gen,
                        "slot": slot_ms,
                        "reason": "next_fire_failed",
                        "error_message": error_message,
                    }),
                );
                remove_ref_from_slot(&state, slot_ms, &reference).await?;
                return Ok(());
            }
            CronDispatchDecision::AdvanceOnly { next_slot }
            | CronDispatchDecision::Fire { next_slot } => *next_slot,
        };
        let claim_outcome = claim_and_advance_ref(
            &state,
            &reference,
            slot_ms,
            next_slot,
            CronClaimSnapshot {
                hash_key: &cron_hash_key,
                cron_id: &cron_id,
                meta: &expected_meta,
                entry: &expected_entry,
            },
        )
        .await?;
        match cron_claim_disposition(claim_outcome, config_attempt) {
            CronClaimDisposition::LeaseLost => {
                state.metrics.increment(
                    "cron_fires",
                    &[("service", SERVICE), ("outcome", "lease_lost")],
                    1.0,
                );
                log(
                    &state,
                    LogLevel::Info,
                    "cron_lease_lost",
                    json!({
                        "ns": parts.ns,
                        "worker": parts.worker,
                        "cron_id": parts.cron_id,
                        "slot": slot_ms,
                    }),
                );
                return Ok(());
            }
            CronClaimDisposition::RetryConfig => continue,
            CronClaimDisposition::DeferConfig => {
                state.metrics.increment(
                    "cron_fires",
                    &[("service", SERVICE), ("outcome", "config_changed_deferred")],
                    1.0,
                );
                log(
                    &state,
                    LogLevel::Info,
                    "cron_config_changed_deferred",
                    json!({
                        "ns": parts.ns,
                        "worker": parts.worker,
                        "cron_id": parts.cron_id,
                        "slot": slot_ms,
                    }),
                );
                return Ok(());
            }
            CronClaimDisposition::Claimed => {}
        }

        if matches!(decision, CronDispatchDecision::AdvanceOnly { .. }) {
            state.metrics.increment(
                "cron_fires",
                &[("service", SERVICE), ("outcome", "skipped_stale")],
                1.0,
            );
            log(
                &state,
                LogLevel::Info,
                "cron_ref_stale_advanced",
                json!({
                    "ns": parts.ns,
                    "worker": parts.worker,
                    "cron_id": parts.cron_id,
                    "from_slot": slot_ms,
                    "to_slot": next_slot,
                    "lag_ms": slot_ms_for(tick_now) - slot_ms,
                }),
            );
            return Ok(());
        }

        let worker_id = format!("{}:{}:{}", parts.ns, parts.worker, active_version);
        let request_id = format!("sched-{}-{}-{}", state.instance_id, slot_ms, parts.cron_id);
        let fired_at = now_ms();
        let res = post_runtime(
            &state,
            "/_scheduled",
            json!({ "scheduledTime": slot_ms, "cron": entry.cron }),
            &worker_id,
            &request_id,
        )
        .await;
        let duration_ms = now_ms() - fired_at;
        let outcome = crate::runtime_outcome_label(&res);
        state.metrics.observe(
            "cron_fire_duration_ms",
            &[("service", SERVICE), ("outcome", outcome)],
            duration_ms as f64,
        );
        state.metrics.observe(
            "cron_queue_lag_ms",
            &[("service", SERVICE), ("outcome", outcome)],
            (fired_at - slot_ms) as f64,
        );
        state.metrics.increment(
            "cron_fires",
            &[("service", SERVICE), ("outcome", outcome)],
            1.0,
        );
        log(
            &state,
            if outcome == "ok" {
                LogLevel::Info
            } else {
                LogLevel::Warn
            },
            "cron_fired",
            json!({
                "request_id": request_id,
                "worker_id": worker_id,
                "cron": entry.cron,
                "scheduled_time": slot_ms,
                "outcome": outcome,
                "duration_ms": duration_ms,
                "status": res.status,
                "error_message": res.error.or_else(|| res.json.as_ref().and_then(|v| v.get("error")).and_then(JsonValue::as_str).map(str::to_string)),
            }),
        );
        return Ok(());
    }
    unreachable!("cron config retry loop always returns")
}

pub(crate) async fn tick(state: AppState) -> SchedulerResult<()> {
    let now = now_ms();
    let current_slot = slot_ms_for(now);
    // Look back one minute as well so refs inserted near a minute rollover are
    // advanced or pruned without waiting for the periodic scan.
    for slot in [current_slot, current_slot - 60_000] {
        let key = slot_key(slot);
        let refs: Vec<String> = match state
            .redis
            .with_conn(async |mut conn| conn.smembers(key).await)
            .await
        {
            Ok(refs) => refs,
            Err(err) => {
                log(
                    &state,
                    LogLevel::Error,
                    "tick_scan_failed",
                    redis_fields_with_error(json!({ "slot": slot }), &err),
                );
                continue;
            }
        };
        state.metrics.observe(
            "cron_bucket_size",
            &[("service", SERVICE)],
            refs.len() as f64,
        );
        for reference in refs {
            let child = state.clone();
            let Ok(permit) = state.dispatch.cron.clone().acquire_owned().await else {
                return Ok(());
            };
            let process_ref_fields = json!({ "ref": reference, "slot": slot });
            state.spawn_tracked(
                "process_ref_failed",
                process_ref_fields.clone(),
                async move {
                    let _permit = permit;
                    if let Err(err) = process_ref(child.clone(), reference.clone(), slot, now).await
                    {
                        log(
                            &child,
                            LogLevel::Error,
                            "process_ref_failed",
                            scheduler_fields_with_error(process_ref_fields, &err),
                        );
                    }
                },
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::parse_packed_commands;

    fn cron_entry(cron: &str) -> CronEntry {
        CronEntry {
            cron: cron.to_string(),
            timezone: "UTC".to_string(),
            r#gen: 1,
        }
    }

    #[test]
    fn invalid_next_fire_returns_remove_ref_decision() {
        let decision = cron_dispatch_decision(
            &cron_entry("not a cron"),
            1_700_000_000_000,
            1_700_000_000_000,
        );

        match decision {
            CronDispatchDecision::RemoveInvalidRef { error_message } => {
                assert!(!error_message.is_empty());
            }
            other => panic!("expected invalid next-fire drop, got {other:?}"),
        }
    }

    #[test]
    fn previous_slot_returns_advance_only_decision() {
        let tick_now = 1_700_000_000_000;
        let current_slot = slot_ms_for(tick_now);
        let entry = cron_entry("*/5 * * * *");

        assert!(matches!(
            cron_dispatch_decision(&entry, current_slot - 60_000, tick_now),
            CronDispatchDecision::AdvanceOnly { .. }
        ));
        assert!(matches!(
            cron_dispatch_decision(&entry, current_slot, tick_now),
            CronDispatchDecision::Fire { .. }
        ));
    }

    #[test]
    fn cron_ref_lookup_reads_meta_and_entry_together() {
        let command = cron_ref_lookup_cmd("cron:demo:worker", "cron-id");
        let commands = parse_packed_commands(&command.get_packed_command());

        assert_eq!(
            commands,
            [["HMGET", "cron:demo:worker", "__meta__", "cron-id"]]
        );
    }

    #[test]
    fn cron_claim_checks_source_membership_before_lease_and_advance() {
        let membership_guard = CRON_CLAIM_ADVANCE_SCRIPT
            .find("if redis.call('SISMEMBER', KEYS[1], ARGV[3]) ~= 1 then\n  return 0\nend")
            .expect("missing source membership must stop the claim");
        let lease = CRON_CLAIM_ADVANCE_SCRIPT
            .find("redis.call('SET', KEYS[2]")
            .expect("cron claim must acquire a lease");
        let remove = CRON_CLAIM_ADVANCE_SCRIPT
            .find("redis.call('SREM', KEYS[1], ARGV[3])")
            .expect("cron claim must remove the source ref");
        let advance = CRON_CLAIM_ADVANCE_SCRIPT
            .find("redis.call('SADD', KEYS[3], ARGV[3])")
            .expect("cron claim must advance the ref");

        assert!(membership_guard < lease);
        assert!(lease < remove);
        assert!(remove < advance);
    }

    #[test]
    fn cron_claim_rechecks_projection_before_lease_and_advance() {
        let membership = CRON_CLAIM_ADVANCE_SCRIPT
            .find("redis.call('SISMEMBER', KEYS[1], ARGV[3])")
            .expect("claim must validate source membership");
        let meta = CRON_CLAIM_ADVANCE_SCRIPT
            .find("redis.call('HGET', KEYS[4], '__meta__')")
            .expect("claim must re-read cron metadata");
        let entry = CRON_CLAIM_ADVANCE_SCRIPT
            .find("redis.call('HGET', KEYS[4], ARGV[6])")
            .expect("claim must re-read the cron entry");
        let config_changed = CRON_CLAIM_ADVANCE_SCRIPT
            .find("return 2")
            .expect("projection mismatch must have a distinct result");
        let lease = CRON_CLAIM_ADVANCE_SCRIPT
            .find("redis.call('SET', KEYS[2]")
            .expect("cron claim must acquire a lease");

        assert!(membership < meta && meta < entry && entry < config_changed);
        assert!(config_changed < lease);
    }

    #[test]
    fn cron_claim_result_codes_preserve_retry_semantics() {
        assert_eq!(cron_claim_outcome(0).unwrap(), CronClaimOutcome::LeaseLost);
        assert_eq!(cron_claim_outcome(1).unwrap(), CronClaimOutcome::Claimed);
        assert_eq!(
            cron_claim_outcome(2).unwrap(),
            CronClaimOutcome::ConfigChanged
        );

        let error = cron_claim_outcome(3).unwrap_err();
        assert_eq!(error.code, "internal_error");
        assert_eq!(error.message, "unexpected cron claim result 3");
    }

    #[test]
    fn cron_config_change_retries_once_then_defers() {
        assert_eq!(
            cron_claim_disposition(CronClaimOutcome::ConfigChanged, 0),
            CronClaimDisposition::RetryConfig
        );
        assert_eq!(
            cron_claim_disposition(CronClaimOutcome::ConfigChanged, 1),
            CronClaimDisposition::DeferConfig
        );
    }
}
