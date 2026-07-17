use std::collections::{BTreeMap, HashMap};

use serde_json::json;

use crate::{
    AppState, LogLevel, SERVICE, SchedulerResult, indexed_keys_after_backfill, log, now_ms,
};

use super::reference::{
    CRON_WORKER_KEY_SCAN_PATTERN, CronEntry, cron_meta_version, parse_cron_worker_key, ref_for,
};
use super::slot::{next_fire_ms, slot_expire_at, slot_key, slot_ms_for};

const CRON_WORKER_INDEX_KEY: &str = "cron:index:workers";
const CRON_WORKER_INDEX_BACKFILLED_KEY: &str = "cron:index:workers:backfilled";
const CRON_SWEEP_READ_CHUNK_SIZE: usize = 100;
const CRON_SWEEP_WRITE_CHUNK_SIZE: usize = 100;

async fn cron_worker_keys(state: &AppState) -> Result<Vec<String>, redis::RedisError> {
    indexed_keys_after_backfill(
        state,
        CRON_WORKER_INDEX_KEY,
        CRON_WORKER_INDEX_BACKFILLED_KEY,
        CRON_WORKER_KEY_SCAN_PATTERN,
    )
    .await
}

async fn fetch_cron_hash_chunk(
    state: &AppState,
    keys: &[String],
) -> Result<Vec<HashMap<String, String>>, redis::RedisError> {
    let chunk_keys = keys.to_vec();
    state
        .redis
        .with_conn(async |mut conn| {
            let mut pipe = redis::pipe();
            for key in &chunk_keys {
                pipe.cmd("HGETALL").arg(key);
            }
            pipe.query_async(&mut conn).await
        })
        .await
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CronSlotWrite {
    slot: i64,
    key: String,
    refs: Vec<String>,
    expire_at: i64,
}

fn cron_slot_writes(slot_refs: BTreeMap<i64, Vec<String>>) -> Vec<CronSlotWrite> {
    slot_refs
        .into_iter()
        .map(|(slot, refs)| CronSlotWrite {
            slot,
            key: slot_key(slot),
            refs,
            expire_at: slot_expire_at(slot),
        })
        .collect()
}

fn count_sadd_replies(replies: &[i64]) -> u64 {
    replies
        .iter()
        .step_by(2)
        .filter_map(|value| u64::try_from(*value).ok())
        .sum()
}

fn has_dispatchable_cron_meta(hash: &HashMap<String, String>) -> bool {
    hash.get("__meta__")
        .and_then(|raw| cron_meta_version(raw))
        .is_some()
}

fn dispatchable_cron_worker<'a>(
    key: &'a str,
    hash: &HashMap<String, String>,
) -> Result<(&'a str, &'a str), &'static str> {
    let (ns, worker) = parse_cron_worker_key(key).ok_or("invalid_identity")?;
    if !has_dispatchable_cron_meta(hash) {
        return Err("invalid_meta");
    }
    Ok((ns, worker))
}

async fn write_cron_slot_refs(
    state: &AppState,
    slot_refs: BTreeMap<i64, Vec<String>>,
) -> SchedulerResult<u64> {
    let writes = cron_slot_writes(slot_refs);
    let mut added = 0_u64;
    for chunk in writes.chunks(CRON_SWEEP_WRITE_CHUNK_SIZE) {
        let chunk = chunk.to_vec();
        let replies: Vec<i64> = state
            .redis
            .with_conn(async |mut conn| {
                let mut pipe = redis::pipe();
                pipe.atomic();
                for write in &chunk {
                    pipe.cmd("SADD").arg(&write.key).arg(&write.refs);
                    pipe.cmd("EXPIREAT").arg(&write.key).arg(write.expire_at);
                }
                pipe.query_async(&mut conn).await
            })
            .await?;
        added += count_sadd_replies(&replies);
    }
    Ok(added)
}

pub(crate) async fn sweep(state: AppState) -> SchedulerResult<()> {
    let started = now_ms();
    let mut workers = 0_u64;
    let mut skipped = 0_u64;
    let mut skipped_workers = 0_u64;
    let keys = cron_worker_keys(&state).await?;
    let mut slot_refs: BTreeMap<i64, Vec<String>> = BTreeMap::new();
    for chunk in keys.chunks(CRON_SWEEP_READ_CHUNK_SIZE) {
        let hashes = fetch_cron_hash_chunk(&state, chunk).await?;
        for (key, hash) in chunk.iter().zip(hashes) {
            workers += 1;
            let (ns, worker) = match dispatchable_cron_worker(key, &hash) {
                Ok(identity) => identity,
                Err(reason) => {
                    skipped_workers += 1;
                    log(
                        &state,
                        LogLevel::Warn,
                        "cron_sweep_worker_skipped",
                        json!({ "worker_key": key, "reason": reason }),
                    );
                    continue;
                }
            };
            let now = now_ms();
            for (id, raw) in hash {
                if id == "__meta__" {
                    continue;
                }
                // Per-entry tolerance: a single corrupt entry or an unparseable
                // cron expression must not stop the rest of the sweep, otherwise
                // one bad row holds up reconciliation for every other worker.
                let entry = match serde_json::from_str::<CronEntry>(&raw) {
                    Ok(entry) => entry,
                    Err(err) => {
                        skipped += 1;
                        log(
                            &state,
                            LogLevel::Warn,
                            "cron_sweep_entry_skipped",
                            json!({
                                "ns": ns,
                                "worker": worker,
                                "cron_id": id,
                                "reason": "corrupt_json",
                                "error_message": err.to_string(),
                            }),
                        );
                        continue;
                    }
                };
                let next_ms = match next_fire_ms(&entry.cron, &entry.timezone, now) {
                    Ok(ms) => ms,
                    Err(err) => {
                        skipped += 1;
                        log(
                            &state,
                            LogLevel::Warn,
                            "cron_sweep_entry_skipped",
                            json!({
                                "ns": ns,
                                "worker": worker,
                                "cron_id": id,
                                "reason": "next_fire_failed",
                                "error_message": err.message,
                            }),
                        );
                        continue;
                    }
                };
                let slot = slot_ms_for(next_ms);
                let reference = ref_for(ns, worker, &id, entry.r#gen);
                slot_refs.entry(slot).or_default().push(reference);
            }
        }
    }
    let re_added = write_cron_slot_refs(&state, slot_refs).await?;
    state.metrics.increment(
        "cron_sweep_entries_skipped",
        &[("service", SERVICE)],
        skipped as f64,
    );
    state.metrics.increment(
        "cron_sweep_workers_skipped",
        &[("service", SERVICE)],
        skipped_workers as f64,
    );
    log(
        &state,
        LogLevel::Info,
        "cron_reconcile",
        json!({
            "workers": workers,
            "re_added": re_added,
            "entries_skipped": skipped,
            "workers_skipped": skipped_workers,
            "duration_ms": now_ms() - started,
        }),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::scheduler_projection_contract;

    #[test]
    fn cron_worker_index_keys_are_stable() {
        let fixture = scheduler_projection_contract();
        assert_eq!(CRON_WORKER_INDEX_KEY, fixture.cron.worker_index_key);
        assert_eq!(
            CRON_WORKER_INDEX_BACKFILLED_KEY,
            "cron:index:workers:backfilled"
        );
    }

    #[test]
    fn cron_slot_writes_group_refs_by_slot_with_single_expiry() {
        let mut slot_refs = BTreeMap::new();
        slot_refs.insert(
            1_778_856_600_000,
            vec!["demo:worker:a:1".to_string(), "demo:worker:b:2".to_string()],
        );
        slot_refs.insert(1_778_856_660_000, vec!["demo:worker:c:3".to_string()]);

        assert_eq!(
            cron_slot_writes(slot_refs),
            vec![
                CronSlotWrite {
                    slot: 1_778_856_600_000,
                    key: "cron-slot:1778856600000".to_string(),
                    refs: vec!["demo:worker:a:1".to_string(), "demo:worker:b:2".to_string()],
                    expire_at: 1_778_857_200,
                },
                CronSlotWrite {
                    slot: 1_778_856_660_000,
                    key: "cron-slot:1778856660000".to_string(),
                    refs: vec!["demo:worker:c:3".to_string()],
                    expire_at: 1_778_857_260,
                },
            ]
        );
    }

    #[test]
    fn count_sadd_replies_ignores_expireat_replies() {
        assert_eq!(count_sadd_replies(&[2, 1, 0, 1, 4, 1]), 6);
    }

    #[test]
    fn cron_sweep_requires_dispatchable_worker_metadata() {
        let valid = HashMap::from([(
            "__meta__".to_string(),
            json!({ "version": "v1" }).to_string(),
        )]);
        assert_eq!(
            dispatchable_cron_worker("crons:demo:worker", &valid),
            Ok(("demo", "worker"))
        );
        assert_eq!(
            dispatchable_cron_worker("crons:__platform__:worker", &valid),
            Err("invalid_identity")
        );
        assert_eq!(
            dispatchable_cron_worker("crons:demo:worker", &HashMap::new()),
            Err("invalid_meta")
        );
        assert_eq!(
            dispatchable_cron_worker(
                "crons:demo:worker",
                &HashMap::from([("__meta__".to_string(), "{broken".to_string(),)])
            ),
            Err("invalid_meta")
        );
        assert_eq!(
            dispatchable_cron_worker(
                "crons:demo:worker",
                &HashMap::from([(
                    "__meta__".to_string(),
                    json!({ "version": "v9007199254740992" }).to_string(),
                )])
            ),
            Err("invalid_meta")
        );
    }
}
