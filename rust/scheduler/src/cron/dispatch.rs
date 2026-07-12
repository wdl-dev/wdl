use redis::AsyncCommands;
use serde_json::{Value as JsonValue, json};
use wdl_rust_common::redis_eval::eval_cmd;

use crate::{
    AppState, LogLevel, SERVICE, SchedulerError, SchedulerResult, log, now_ms, post_runtime,
    redis_fields_with_error, scheduler_fields_with_error,
};

use super::reference::{CronEntry, RefVerdict, classify_ref, cron_worker_key, parse_ref};
use super::slot::{lease_key, next_fire_ms, slot_expire_at, slot_key, slot_ms_for};

const CRON_CLAIM_ADVANCE_SCRIPT: &str = r#"
if redis.call('SET', KEYS[2], ARGV[1], 'NX', 'EX', ARGV[2]) then
  redis.call('SREM', KEYS[1], ARGV[3])
  redis.call('SADD', KEYS[3], ARGV[3])
  redis.call('EXPIREAT', KEYS[3], ARGV[4])
  return 1
end
return 0
"#;

async fn claim_and_advance_ref(
    state: &AppState,
    reference: &str,
    from_slot: i64,
    to_slot: i64,
) -> SchedulerResult<bool> {
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
            eval_cmd(
                CRON_CLAIM_ADVANCE_SCRIPT,
                &[from_key.as_str(), lease.as_str(), to_key.as_str()],
                &[
                    instance_id.as_str(),
                    lease_ttl_s_arg.as_str(),
                    reference.as_str(),
                    to_expire_at_arg.as_str(),
                ],
            )
            .query_async::<i64>(&mut conn)
            .await
        })
        .await
        .map(|claimed| claimed == 1)
        .map_err(SchedulerError::from)
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
    let (meta_str, entry_str): (Option<String>, Option<String>) = state
        .redis
        .with_conn(async |mut conn| {
            redis::pipe()
                .cmd("HGET")
                .arg(&cron_hash_key)
                .arg("__meta__")
                .cmd("HGET")
                .arg(&cron_hash_key)
                .arg(&cron_id)
                .query_async(&mut conn)
                .await
        })
        .await?;

    let (entry, active_version) = match classify_ref(&parts, meta_str, entry_str) {
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
            state
                .metrics
                .increment("cron_stale_refs_cleaned", &[("service", SERVICE)], 1.0);
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

    // CF cron contract is encoded in the order below:
    //   (a) lease before advance — two ticks must not both SADD the next
    //       slot or the ref ends up in an unknown slot.
    //   (b) advance before fire — if the POST fails the ref has already
    //       moved to next_slot, giving at-most-once per slot.
    //   (c) stranded refs (slot_ms < current_slot) advance but do NOT fire
    //       — preserves CF's "skip missed after outage" semantic.
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
    if !claim_and_advance_ref(&state, &reference, slot_ms, next_slot).await? {
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
    Ok(())
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
}
