use std::collections::{HashMap, HashSet};
use std::time::Duration;

use redis::streams::StreamReadReply;
use serde_json::json;
use tokio::time::sleep;
use wdl_rust_common::hash::fnv1a64;
use wdl_rust_common::queue_keys::{QUEUE_DELAYED_WAKE_KEY_FIELD, QUEUE_DELAYED_WAKE_STREAM};
use wdl_rust_common::redis_eval::StaticRedisScript;

use crate::{
    AppState, LogLevel, Metrics, SERVICE, SchedulerError, SchedulerResult, log, now_ms,
    redis_fields_with_error,
};

use super::{
    Consumer, ConsumerLookup, QUEUE_REDIS_READ_BATCH_SIZE, parse_delayed_key, queue_orphaned_key,
    queue_stream_key, resolve_consumer_batch, stream_id_to_entry,
};

const QUEUE_DELAYED_CLAIM_SAFETY_MS: u64 = 5_000;
const QUEUE_DELAYED_NO_PROGRESS_BACKOFF_MS: u64 = 100;
const QUEUE_DELAYED_WAKE_RETRY_BASE_MS: u64 = 1_000;
const QUEUE_DELAYED_WAKE_RETRY_MAX_MS: u64 = 10_000;
const QUEUE_DELAYED_WAKE_RETRY_JITTER_MS: u64 = 250;
const MOVE_CLAIMED_DELAYED_MEMBER_SCRIPT: &str = r#"
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return 0
end
if redis.call("ZSCORE", KEYS[2], ARGV[2]) == false then
  redis.call("DEL", KEYS[1])
  return 0
end
if ARGV[3] ~= "" then
  redis.call("XADD", KEYS[3], "MAXLEN", "~", ARGV[3], "*", unpack(ARGV, 4))
else
  redis.call("XADD", KEYS[3], "*", unpack(ARGV, 4))
end
redis.call("ZREM", KEYS[2], ARGV[2])
redis.call("DEL", KEYS[1])
return 1
"#;
const DROP_CLAIMED_DELAYED_MEMBER_SCRIPT: &str = r#"
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return 0
end
if redis.call("ZREM", KEYS[2], ARGV[2]) ~= 1 then
  redis.call("DEL", KEYS[1])
  return 0
end
redis.call("DEL", KEYS[1])
return 1
"#;

static MOVE_CLAIMED_DELAYED_MEMBER: StaticRedisScript =
    StaticRedisScript::new(MOVE_CLAIMED_DELAYED_MEMBER_SCRIPT);
static DROP_CLAIMED_DELAYED_MEMBER: StaticRedisScript =
    StaticRedisScript::new(DROP_CLAIMED_DELAYED_MEMBER_SCRIPT);

struct DelayedQueueProbe {
    delayed_key: String,
    stream_key: String,
    ns: String,
    queue: String,
}

impl DelayedQueueProbe {
    fn parse(delayed_key: String) -> Result<Self, String> {
        let Some((ns, queue)) = parse_delayed_key(&delayed_key) else {
            return Err(delayed_key);
        };
        Ok(Self {
            stream_key: queue_stream_key(&ns, &queue),
            delayed_key,
            ns,
            queue,
        })
    }

    fn consumer_lookup(&self) -> ConsumerLookup<'_> {
        ConsumerLookup {
            stream_key: &self.stream_key,
            ns: &self.ns,
            queue: &self.queue,
        }
    }
}

pub(crate) fn wait_ms_until_due(now_ms: i64, due_ms: i64) -> u64 {
    due_ms.saturating_sub(now_ms).max(0) as u64
}

pub(crate) fn earliest_due_from_zrange_heads(
    delayed_keys: &[String],
    heads: &[Vec<(String, f64)>],
) -> (Option<i64>, Vec<String>) {
    let mut earliest = None;
    let mut empty_keys = Vec::new();
    for (delayed_key, first) in delayed_keys.iter().zip(heads) {
        let Some((_, score)) = first.first() else {
            empty_keys.push(delayed_key.clone());
            continue;
        };
        let due = *score as i64;
        earliest = Some(earliest.map_or(due, |current: i64| current.min(due)));
    }
    (earliest, empty_keys)
}

fn delayed_due_probe_pipeline(
    probes: &[DelayedQueueProbe],
    consumers: &[Option<Consumer>],
    due_at_ms: i64,
    limit: usize,
) -> redis::Pipeline {
    debug_assert_eq!(probes.len(), consumers.len());
    let mut pipe = redis::pipe();
    for (probe, consumer) in probes.iter().zip(consumers) {
        let command = pipe.cmd("ZRANGEBYSCORE").arg(&probe.delayed_key).arg(0);
        if consumer.is_some() {
            command.arg(due_at_ms);
        } else {
            command.arg("+inf");
        }
        command.arg("LIMIT").arg(0).arg(limit);
    }
    pipe
}

async fn load_delayed_due_members(
    state: &AppState,
    probes: &[DelayedQueueProbe],
    consumers: &[Option<Consumer>],
    due_at_ms: i64,
) -> SchedulerResult<Vec<Vec<String>>> {
    if probes.len() != consumers.len() {
        return Err(SchedulerError::internal_error(
            "delayed queue consumer response count mismatch",
        ));
    }
    let mut members = Vec::with_capacity(probes.len());
    for (probe_chunk, consumer_chunk) in probes
        .chunks(QUEUE_REDIS_READ_BATCH_SIZE)
        .zip(consumers.chunks(QUEUE_REDIS_READ_BATCH_SIZE))
    {
        let chunk_members: Vec<Vec<String>> = state
            .data_redis
            .with_conn(async |mut conn| {
                delayed_due_probe_pipeline(
                    probe_chunk,
                    consumer_chunk,
                    due_at_ms,
                    state.config.queue_sweep_batch_size,
                )
                .query_async(&mut conn)
                .await
            })
            .await?;
        if chunk_members.len() != probe_chunk.len() {
            return Err(SchedulerError::internal_error(
                "delayed queue due probe response count mismatch",
            ));
        }
        members.extend(chunk_members);
    }
    Ok(members)
}

fn delayed_head_pipeline(delayed_keys: &[String]) -> redis::Pipeline {
    let mut pipe = redis::pipe();
    for delayed_key in delayed_keys {
        pipe.cmd("ZRANGE")
            .arg(delayed_key)
            .arg(0)
            .arg(0)
            .arg("WITHSCORES");
    }
    pipe
}

async fn load_delayed_heads(
    state: &AppState,
    delayed_keys: &[String],
) -> SchedulerResult<Vec<Vec<(String, f64)>>> {
    let mut heads = Vec::with_capacity(delayed_keys.len());
    for key_chunk in delayed_keys.chunks(QUEUE_REDIS_READ_BATCH_SIZE) {
        let chunk_heads: Vec<Vec<(String, f64)>> = state
            .data_redis
            .with_conn(async |mut conn| {
                delayed_head_pipeline(key_chunk)
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        if chunk_heads.len() != key_chunk.len() {
            return Err(SchedulerError::internal_error(
                "delayed queue head probe response count mismatch",
            ));
        }
        heads.extend(chunk_heads);
    }
    Ok(heads)
}

pub(crate) fn record_queue_delayed_wake_read_error(metrics: &Metrics) {
    record_queue_delayed_metric(metrics, "queue_delayed_wake_read_errors", 1);
}

pub(crate) fn record_queue_delayed_claim_misses(metrics: &Metrics, count: usize) {
    record_queue_delayed_metric(metrics, "queue_delayed_claim_misses", count);
}

pub(crate) fn record_queue_delayed_move_skips(metrics: &Metrics, count: usize) {
    record_queue_delayed_metric(metrics, "queue_delayed_move_skips", count);
}

pub(crate) fn record_queue_delayed_corrupt_members(metrics: &Metrics, count: usize) {
    record_queue_delayed_metric(metrics, "queue_delayed_corrupt_members", count);
}

fn record_queue_delayed_metric(metrics: &Metrics, name: &'static str, count: usize) {
    if count == 0 {
        return;
    }
    metrics.increment(name, &[("service", SERVICE)], count as f64);
}

// Delayed members are scheduler-owned JSON maps of string fields. Parse failure
// means corrupt Redis state or a non-WDL writer, not a user payload variant.
pub(crate) fn parse_json_entry(member: &str) -> Option<HashMap<String, String>> {
    let entry = serde_json::from_str::<HashMap<String, String>>(member).ok()?;
    if entry.is_empty() {
        return None;
    }
    Some(entry)
}

pub(crate) fn delayed_claim_key(delayed_key: &str, member: &str) -> String {
    let mut bytes = Vec::with_capacity(delayed_key.len() + member.len() + 1);
    bytes.extend_from_slice(delayed_key.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(member.as_bytes());
    format!("queue-delayed-claim:{:016x}", fnv1a64(&bytes))
}

pub(crate) fn delayed_claim_ttl_ms(fire_timeout_ms: u64) -> u64 {
    fire_timeout_ms.saturating_add(QUEUE_DELAYED_CLAIM_SAFETY_MS)
}

pub(crate) fn delayed_wake_retry_delay_ms(consecutive_errors: u32, instance_id: &str) -> u64 {
    let shift = consecutive_errors.saturating_sub(1).min(4);
    let backoff = QUEUE_DELAYED_WAKE_RETRY_BASE_MS
        .saturating_mul(1_u64 << shift)
        .min(QUEUE_DELAYED_WAKE_RETRY_MAX_MS);
    let jitter_seed = format!("{instance_id}\0{consecutive_errors}");
    let jitter = fnv1a64(jitter_seed.as_bytes()) % (QUEUE_DELAYED_WAKE_RETRY_JITTER_MS + 1);
    backoff
        .saturating_add(jitter)
        .min(QUEUE_DELAYED_WAKE_RETRY_MAX_MS)
}

async fn claim_delayed_members(
    state: &AppState,
    delayed_key: &str,
    members: Vec<String>,
) -> SchedulerResult<Vec<(String, String)>> {
    let candidates = members
        .into_iter()
        .map(|member| {
            let claim_key = delayed_claim_key(delayed_key, &member);
            (member, claim_key)
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let results: Vec<Option<String>> = state
        .data_redis
        .with_conn(async |mut conn| {
            let mut pipe = redis::pipe();
            for (_, claim_key) in &candidates {
                pipe.cmd("SET")
                    .arg(claim_key)
                    .arg(&state.instance_id)
                    .arg("NX")
                    .arg("PX")
                    .arg(delayed_claim_ttl_ms(state.config.fire_timeout_ms));
            }
            pipe.query_async(&mut conn).await
        })
        .await?;
    let mut claimed = Vec::with_capacity(candidates.len());
    let mut missed = 0_usize;
    for ((member, claim_key), won) in candidates.into_iter().zip(results) {
        if won.is_some() {
            claimed.push((member, claim_key));
        } else {
            missed += 1;
        }
    }
    record_queue_delayed_claim_misses(&state.metrics, missed);
    Ok(claimed)
}

fn delayed_mutation_pipeline(
    instance_id: &str,
    delayed_key: &str,
    target_key: &str,
    trim: Option<usize>,
    moved: &[(String, String, HashMap<String, String>)],
    corrupt: &[(String, String)],
) -> redis::Pipeline {
    let mut pipe = redis::pipe();
    let move_script = MOVE_CLAIMED_DELAYED_MEMBER.prepare_pipeline(&mut pipe, moved.len());
    let drop_script = DROP_CLAIMED_DELAYED_MEMBER.prepare_pipeline(&mut pipe, corrupt.len());
    let trim_arg = trim.map(|value| value.to_string()).unwrap_or_default();
    for (member, claim_key, entry) in moved {
        move_script.append(
            &mut pipe,
            &[claim_key.as_str(), delayed_key, target_key],
            &[instance_id, member.as_str(), trim_arg.as_str()],
        );
        for (field, value) in entry {
            pipe.arg(field).arg(value);
        }
    }
    for (member, claim_key) in corrupt {
        drop_script.append(
            &mut pipe,
            &[claim_key.as_str(), delayed_key],
            &[instance_id, member.as_str()],
        );
    }
    pipe.cmd("ZCARD").arg(delayed_key);
    pipe
}

struct DelayedMutationResult {
    moved: usize,
    dropped: usize,
    remaining: i64,
}

async fn apply_claimed_delayed_members(
    state: &AppState,
    delayed_key: &str,
    target_key: &str,
    trim: Option<usize>,
    moved: &[(String, String, HashMap<String, String>)],
    corrupt: &[(String, String)],
) -> SchedulerResult<DelayedMutationResult> {
    let mut replies: Vec<i64> = state
        .data_redis
        .with_conn(async |mut conn| {
            let pipe = delayed_mutation_pipeline(
                &state.instance_id,
                delayed_key,
                target_key,
                trim,
                moved,
                corrupt,
            );
            pipe.query_async(&mut conn).await
        })
        .await?;
    let expected_replies = moved.len() + corrupt.len() + 1;
    if replies.len() != expected_replies {
        return Err(SchedulerError::internal_error(
            "invalid delayed mutation pipeline response",
        ));
    }
    let remaining = replies
        .pop()
        .ok_or_else(|| SchedulerError::internal_error("missing delayed queue remaining count"))?;
    let moved_count = replies[..moved.len()]
        .iter()
        .filter(|count| **count == 1)
        .count();
    let dropped = replies[moved.len()..]
        .iter()
        .filter(|count| **count == 1)
        .count();
    record_queue_delayed_move_skips(&state.metrics, moved.len().saturating_sub(moved_count));
    Ok(DelayedMutationResult {
        moved: moved_count,
        dropped,
        remaining,
    })
}

pub(crate) async fn queue_due_sweep(state: AppState) -> SchedulerResult<bool> {
    let mut made_progress = false;
    let probes = state
        .queues
        .known_delayed
        .read()
        .await
        .iter()
        .cloned()
        .filter_map(|key| DelayedQueueProbe::parse(key).ok())
        .collect::<Vec<_>>();
    let lookups = probes
        .iter()
        .map(DelayedQueueProbe::consumer_lookup)
        .collect::<Vec<_>>();
    let consumers = resolve_consumer_batch(&state, &lookups).await?;
    let due_members = load_delayed_due_members(&state, &probes, &consumers, now_ms()).await?;
    for ((probe, consumer), members) in probes.into_iter().zip(consumers).zip(due_members) {
        if members.is_empty() {
            continue;
        }
        let claimed = claim_delayed_members(&state, &probe.delayed_key, members).await?;
        if claimed.is_empty() {
            continue;
        }

        let orphaned = consumer.is_none();
        let mut move_batch = Vec::with_capacity(claimed.len());
        let mut corrupt = Vec::new();
        for (member, claim_key) in &claimed {
            let Some(mut entry) = parse_json_entry(member) else {
                corrupt.push((member.clone(), claim_key.clone()));
                continue;
            };
            if orphaned {
                entry.insert("reason".to_string(), "consumer-removed".to_string());
                entry.insert("source".to_string(), "delayed".to_string());
            }
            move_batch.push((member.clone(), claim_key.clone(), entry));
        }
        let (target_key, trim) = if orphaned {
            (
                queue_orphaned_key(&probe.ns, &probe.queue),
                Some(state.config.max_orphaned_len),
            )
        } else {
            (probe.stream_key, None)
        };
        let result = apply_claimed_delayed_members(
            &state,
            &probe.delayed_key,
            &target_key,
            trim,
            &move_batch,
            &corrupt,
        )
        .await?;
        record_queue_delayed_corrupt_members(&state.metrics, result.dropped);
        if result.dropped > 0 {
            log(
                &state,
                LogLevel::Warn,
                "queue_delayed_corrupt_members_dropped",
                json!({ "ns": probe.ns, "queue": probe.queue, "count": result.dropped }),
            );
        }
        let moved = result.moved + result.dropped;
        made_progress = made_progress || moved > 0;
        if orphaned {
            log(
                &state,
                LogLevel::Info,
                "queue_delayed_orphaned",
                json!({ "ns": probe.ns, "queue": probe.queue, "count": result.moved }),
            );
        }
        if result.remaining == 0 {
            state
                .queues
                .known_delayed
                .write()
                .await
                .remove(&probe.delayed_key);
        }
    }
    Ok(made_progress)
}

pub(crate) async fn queue_next_due_ms(state: &AppState) -> SchedulerResult<Option<i64>> {
    let delayed_keys = state
        .queues
        .known_delayed
        .read()
        .await
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    let mut earliest = None;
    let mut empty_keys = Vec::new();
    let mut probes = Vec::new();
    for delayed_key in delayed_keys {
        match DelayedQueueProbe::parse(delayed_key) {
            Ok(probe) => probes.push(probe),
            Err(invalid_key) => empty_keys.push(invalid_key),
        }
    }
    let lookups = probes
        .iter()
        .map(DelayedQueueProbe::consumer_lookup)
        .collect::<Vec<_>>();
    let consumers = resolve_consumer_batch(state, &lookups).await?;
    if consumers.iter().any(Option::is_none) {
        // A removed consumer makes every delayed member orphan-eligible. Wake
        // the sweep immediately; it drains with +inf and removes the key from
        // known_delayed, so this is a bounded cleanup trigger.
        return Ok(Some(now_ms()));
    }
    let zrange_keys = probes
        .iter()
        .map(|probe| probe.delayed_key.clone())
        .collect::<Vec<_>>();
    if !zrange_keys.is_empty() {
        let heads = load_delayed_heads(state, &zrange_keys).await?;
        let (next_due, mut zrange_empty_keys) =
            earliest_due_from_zrange_heads(&zrange_keys, &heads);
        earliest = next_due;
        empty_keys.append(&mut zrange_empty_keys);
    }
    if !empty_keys.is_empty() {
        let mut known = state.queues.known_delayed.write().await;
        for key in empty_keys {
            known.remove(&key);
        }
    }
    Ok(earliest)
}

pub(crate) async fn queue_delayed_dispatch_loop(state: AppState) -> SchedulerResult<()> {
    loop {
        if state.is_shutting_down() {
            break;
        }
        match queue_next_due_ms(&state).await? {
            Some(due_ms) => {
                let wait_ms = wait_ms_until_due(now_ms(), due_ms);
                if wait_ms == 0 {
                    if !queue_due_sweep(state.clone()).await? {
                        tokio::select! {
                            _ = sleep(Duration::from_millis(QUEUE_DELAYED_NO_PROGRESS_BACKOFF_MS)) => {}
                            _ = state.queues.delayed_changed.notified() => {}
                            _ = state.shutdown.stop_notified() => break,
                        }
                    }
                    continue;
                }
                tokio::select! {
                    _ = sleep(Duration::from_millis(wait_ms)) => {
                        queue_due_sweep(state.clone()).await?;
                    }
                    _ = state.queues.delayed_changed.notified() => {}
                    _ = state.shutdown.stop_notified() => break,
                }
            }
            None => {
                tokio::select! {
                    _ = state.queues.delayed_changed.notified() => {}
                    _ = state.shutdown.stop_notified() => break,
                }
            }
        }
    }
    Ok(())
}

pub(crate) async fn queue_delayed_wake_loop(state: AppState) -> SchedulerResult<()> {
    let mut conn = state
        .data_redis_client
        .get_connection_manager_with_config(crate::blocking_redis_connection_config())
        .await
        .map_err(SchedulerError::from)?;
    // Start from the bounded backlog rather than "$" so delayed writes that
    // land between startup reconcile and listener startup still wake the
    // wall-clock loop.
    let mut last_id = "0-0".to_string();
    let mut consecutive_read_errors = 0_u32;
    while !state.is_shutting_down() {
        let reply: Result<StreamReadReply, redis::RedisError> = redis::cmd("XREAD")
            .arg("BLOCK")
            .arg(0)
            .arg("STREAMS")
            .arg(QUEUE_DELAYED_WAKE_STREAM)
            .arg(&last_id)
            .query_async(&mut conn)
            .await;
        let reply = match reply {
            Ok(reply) => reply,
            Err(err) => {
                consecutive_read_errors = consecutive_read_errors.saturating_add(1);
                let retry_delay_ms =
                    delayed_wake_retry_delay_ms(consecutive_read_errors, &state.instance_id);
                record_queue_delayed_wake_read_error(&state.metrics);
                log(
                    &state,
                    LogLevel::Error,
                    "queue_delayed_wake_read_failed",
                    redis_fields_with_error(json!({ "retry_delay_ms": retry_delay_ms }), &err),
                );
                // ConnectionManager owns reconnects; bound the retry cadence
                // so Redis outages do not synchronize every scheduler replica.
                sleep(Duration::from_millis(retry_delay_ms)).await;
                continue;
            }
        };
        consecutive_read_errors = 0;
        let mut delayed_keys = HashSet::new();
        for key in reply.keys {
            for id in key.ids {
                last_id = id.id.clone();
                let entry = stream_id_to_entry(id);
                let Some(delayed_key) = entry.fields.get(QUEUE_DELAYED_WAKE_KEY_FIELD) else {
                    continue;
                };
                if parse_delayed_key(delayed_key).is_none() {
                    log(
                        &state,
                        LogLevel::Warn,
                        "queue_delayed_wake_invalid_key",
                        json!({ "delayed_key": delayed_key }),
                    );
                    continue;
                }
                delayed_keys.insert(delayed_key.clone());
            }
        }
        if !delayed_keys.is_empty() {
            state
                .queues
                .known_delayed
                .write()
                .await
                .extend(delayed_keys);
            state.queues.delayed_changed.notify_one();
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::parse_packed_commands;

    fn consumer(ns: &str, queue: &str) -> Consumer {
        Consumer {
            ns: ns.to_string(),
            queue: queue.to_string(),
            max_batch_size: 10,
            max_batch_timeout_ms: 5000,
            max_retries: 3,
            retry_delay_secs: 0,
            dead_letter_queue: None,
            worker_id: format!("{ns}:worker:v1"),
        }
    }

    #[test]
    fn delayed_queue_wall_clock_wait_is_zero_once_due() {
        assert_eq!(wait_ms_until_due(10_000, 12_345), 2_345);
        assert_eq!(wait_ms_until_due(10_000, 10_000), 0);
        assert_eq!(wait_ms_until_due(10_000, 9_999), 0);
        assert_eq!(wait_ms_until_due(i64::MAX, i64::MIN), 0);
        assert_eq!(wait_ms_until_due(i64::MIN, i64::MAX), i64::MAX as u64);
    }

    #[test]
    fn delayed_queue_pipeline_heads_preserve_key_alignment() {
        let keys = vec![
            "queue-delayed:demo:a".to_string(),
            "queue-delayed:demo:b".to_string(),
            "queue-delayed:demo:c".to_string(),
        ];
        let heads = vec![
            vec![("m1".to_string(), 30_000.0)],
            vec![],
            vec![("m2".to_string(), 10_000.0)],
        ];
        let (earliest, empty) = earliest_due_from_zrange_heads(&keys, &heads);
        assert_eq!(earliest, Some(10_000));
        assert_eq!(empty, vec!["queue-delayed:demo:b"]);
    }

    #[test]
    fn delayed_due_probe_pipeline_aligns_consumer_state_with_each_key() {
        let probes = vec![
            DelayedQueueProbe::parse("queue-delayed:demo:active".to_string()).unwrap(),
            DelayedQueueProbe::parse("queue-delayed:demo:removed".to_string()).unwrap(),
        ];
        let consumers = vec![Some(consumer("demo", "active")), None];
        let commands = parse_packed_commands(
            &delayed_due_probe_pipeline(&probes, &consumers, 12_345, 100).get_packed_pipeline(),
        );

        assert_eq!(
            commands,
            [
                [
                    "ZRANGEBYSCORE",
                    "queue-delayed:demo:active",
                    "0",
                    "12345",
                    "LIMIT",
                    "0",
                    "100",
                ],
                [
                    "ZRANGEBYSCORE",
                    "queue-delayed:demo:removed",
                    "0",
                    "+inf",
                    "LIMIT",
                    "0",
                    "100",
                ],
            ]
        );
    }

    #[test]
    fn delayed_head_pipeline_preserves_key_order() {
        let keys = vec![
            "queue-delayed:demo:a".to_string(),
            "queue-delayed:demo:b".to_string(),
        ];
        let commands = parse_packed_commands(&delayed_head_pipeline(&keys).get_packed_pipeline());

        assert_eq!(
            commands,
            [
                ["ZRANGE", "queue-delayed:demo:a", "0", "0", "WITHSCORES"],
                ["ZRANGE", "queue-delayed:demo:b", "0", "0", "WITHSCORES"],
            ]
        );
    }

    #[test]
    fn delayed_claim_key_is_stable_and_position_bound() {
        let first = delayed_claim_key("queue-delayed:demo:jobs", r#"{"id":"a"}"#);
        assert_eq!(
            first,
            delayed_claim_key("queue-delayed:demo:jobs", r#"{"id":"a"}"#)
        );
        assert_ne!(
            first,
            delayed_claim_key("queue-delayed:demo:other", r#"{"id":"a"}"#)
        );
        assert_ne!(
            first,
            delayed_claim_key("queue-delayed:demo:jobs", r#"{"id":"b"}"#)
        );
    }

    #[test]
    fn delayed_claim_ttl_keeps_fire_timeout_margin() {
        assert_eq!(delayed_claim_ttl_ms(60_000), 65_000);
        assert_eq!(delayed_claim_ttl_ms(120_000), 125_000);
        assert_eq!(delayed_claim_ttl_ms(u64::MAX), u64::MAX);
    }

    #[test]
    fn delayed_wake_retry_delay_is_bounded_and_jittered() {
        let first = delayed_wake_retry_delay_ms(1, "scheduler-a");
        let second = delayed_wake_retry_delay_ms(2, "scheduler-a");
        let capped = delayed_wake_retry_delay_ms(99, "scheduler-a");
        assert!(
            (QUEUE_DELAYED_WAKE_RETRY_BASE_MS
                ..=QUEUE_DELAYED_WAKE_RETRY_BASE_MS + QUEUE_DELAYED_WAKE_RETRY_JITTER_MS)
                .contains(&first)
        );
        assert!(second >= QUEUE_DELAYED_WAKE_RETRY_BASE_MS * 2);
        assert_eq!(capped, QUEUE_DELAYED_WAKE_RETRY_MAX_MS);
        assert_ne!(
            delayed_wake_retry_delay_ms(1, "scheduler-a"),
            delayed_wake_retry_delay_ms(1, "scheduler-b")
        );
    }

    #[test]
    fn delayed_json_entry_rejects_empty_maps_as_corrupt() {
        assert!(parse_json_entry("{}").is_none());
        assert!(parse_json_entry("not-json").is_none());
        assert_eq!(
            parse_json_entry(r#"{"id":"a"}"#).and_then(|entry| entry.get("id").cloned()),
            Some("a".to_string())
        );
    }

    #[test]
    fn delayed_move_script_writes_stream_before_removing_delayed_member() {
        let score_pos = MOVE_CLAIMED_DELAYED_MEMBER_SCRIPT
            .find("ZSCORE")
            .expect("script checks delayed membership");
        let xadd_pos = MOVE_CLAIMED_DELAYED_MEMBER_SCRIPT
            .find("XADD")
            .expect("script writes target stream");
        let zrem_pos = MOVE_CLAIMED_DELAYED_MEMBER_SCRIPT
            .find("ZREM")
            .expect("script removes delayed member");
        assert!(score_pos < xadd_pos);
        assert!(xadd_pos < zrem_pos);
    }

    #[test]
    fn delayed_corrupt_drop_script_checks_owner_before_removing_member() {
        let get_pos = DROP_CLAIMED_DELAYED_MEMBER_SCRIPT
            .find("GET")
            .expect("script checks claim owner");
        let zrem_pos = DROP_CLAIMED_DELAYED_MEMBER_SCRIPT
            .find("ZREM")
            .expect("script removes delayed member");
        let del_pos = DROP_CLAIMED_DELAYED_MEMBER_SCRIPT
            .find("DEL")
            .expect("script deletes claim key");
        assert!(get_pos < zrem_pos);
        assert!(zrem_pos < del_pos);
    }

    #[test]
    fn delayed_scripts_delete_owned_claim_when_member_already_moved() {
        let move_score_pos = MOVE_CLAIMED_DELAYED_MEMBER_SCRIPT
            .find("ZSCORE")
            .expect("move script checks delayed membership");
        let move_del_pos = MOVE_CLAIMED_DELAYED_MEMBER_SCRIPT[move_score_pos..]
            .find("DEL")
            .expect("move script deletes stale owned claim")
            + move_score_pos;
        let move_return_pos = MOVE_CLAIMED_DELAYED_MEMBER_SCRIPT[move_score_pos..]
            .find("return 0")
            .expect("move script skips missing delayed member")
            + move_score_pos;
        assert!(move_score_pos < move_del_pos);
        assert!(move_del_pos < move_return_pos);

        let drop_zrem_pos = DROP_CLAIMED_DELAYED_MEMBER_SCRIPT
            .find("ZREM")
            .expect("drop script removes delayed member");
        let drop_del_pos = DROP_CLAIMED_DELAYED_MEMBER_SCRIPT[drop_zrem_pos..]
            .find("DEL")
            .expect("drop script deletes stale owned claim")
            + drop_zrem_pos;
        let drop_return_pos = DROP_CLAIMED_DELAYED_MEMBER_SCRIPT[drop_zrem_pos..]
            .find("return 0")
            .expect("drop script skips missing delayed member")
            + drop_zrem_pos;
        assert!(drop_zrem_pos < drop_del_pos);
        assert!(drop_del_pos < drop_return_pos);
    }

    #[test]
    fn delayed_mutations_read_remaining_count_in_the_same_pipeline() {
        let moved = vec![(
            "valid".to_string(),
            "claim-valid".to_string(),
            HashMap::from([("id".to_string(), "a".to_string())]),
        )];
        let corrupt = vec![("corrupt".to_string(), "claim-corrupt".to_string())];
        let pipeline = delayed_mutation_pipeline(
            "scheduler-a",
            "queue-delayed:demo:jobs",
            "queue:demo:jobs:s",
            None,
            &moved,
            &corrupt,
        );
        let commands = parse_packed_commands(&pipeline.get_packed_pipeline());

        assert_eq!(commands.len(), 3);
        assert_eq!(commands[0][0], "EVAL");
        assert_eq!(commands[1][0], "EVAL");
        assert_eq!(commands[2], ["ZCARD", "queue-delayed:demo:jobs"]);
    }

    #[test]
    fn delayed_wake_read_error_metric_counts_failures() {
        let metrics = Metrics::default();
        record_queue_delayed_wake_read_error(&metrics);
        record_queue_delayed_wake_read_error(&metrics);

        let rendered = metrics.render_prometheus();
        assert!(
            rendered.contains("wdl_queue_delayed_wake_read_errors_total{service=\"scheduler\"} 2")
        );
    }

    #[test]
    fn delayed_claim_and_move_contention_metrics_count_only_nonzero_values() {
        let metrics = Metrics::default();
        record_queue_delayed_claim_misses(&metrics, 2);
        record_queue_delayed_move_skips(&metrics, 1);
        record_queue_delayed_move_skips(&metrics, 0);
        record_queue_delayed_corrupt_members(&metrics, 3);

        let rendered = metrics.render_prometheus();
        assert!(rendered.contains("wdl_queue_delayed_claim_misses_total{service=\"scheduler\"} 2"));
        assert!(rendered.contains("wdl_queue_delayed_move_skips_total{service=\"scheduler\"} 1"));
        assert!(
            rendered.contains("wdl_queue_delayed_corrupt_members_total{service=\"scheduler\"} 3")
        );
    }
}
