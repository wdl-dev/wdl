use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;

use redis::AsyncCommands;
use serde_json::json;
use wdl_rust_common::identity::{is_valid_route_ns, is_valid_worker_name};
use wdl_rust_common::queue_keys::is_valid_queue_name;
use wdl_rust_common::redis_eval::StaticRedisScript;
use wdl_rust_common::worker_contract::parse_version_tag;

use crate::{
    AppState, CONSUMER_GROUP, LogLevel, QueueState, SchedulerError, SchedulerResult,
    indexed_existing_data_keys, indexed_existing_keys, log, repair_data_index, repair_index,
};

use super::{
    Consumer, MAX_BATCH_SIZE_CAP, MAX_BATCH_TIMEOUT_MS, MAX_QUEUE_DELAY_SECONDS, MAX_RETRIES,
    QUEUE_CONSUMER_INDEX_KEY, QUEUE_CONSUMER_SCAN_PATTERN, QUEUE_DELAYED_INDEX_KEY,
    QUEUE_DELAYED_SCAN_PATTERN, QUEUE_STREAM_INDEX_KEY, QUEUE_STREAM_SCAN_PATTERN,
    parse_consumer_key, queue_consumer_key, queue_stream_key,
};

const QUEUE_RECONCILE_CONSUMER_HASH_BATCH_SIZE: usize = 128;
const RECONCILE_QUEUE_GROUPS_SCRIPT: &str = r#"
local results = {}
for index = 2, #KEYS do
  local result = redis.pcall('XGROUP', 'CREATE', KEYS[index], ARGV[1], '0', 'MKSTREAM')
  if type(result) == 'table' and result.err then
    if string.sub(result.err, 1, 9) ~= 'BUSYGROUP' then
      results[#results + 1] = {0, result.err}
    else
      local indexed = redis.pcall('SADD', KEYS[1], KEYS[index])
      if type(indexed) == 'table' and indexed.err then
        results[#results + 1] = {0, indexed.err}
      else
        results[#results + 1] = {1, ''}
      end
    end
  else
    local indexed = redis.pcall('SADD', KEYS[1], KEYS[index])
    if type(indexed) == 'table' and indexed.err then
      results[#results + 1] = {0, indexed.err}
    else
      results[#results + 1] = {1, ''}
    end
  end
end
return results
"#;

static RECONCILE_QUEUE_GROUPS: StaticRedisScript =
    StaticRedisScript::new(RECONCILE_QUEUE_GROUPS_SCRIPT);

fn bounded_i64(value: Option<&String>, fallback: i64, min: i64, max: i64) -> Option<i64> {
    match value {
        None => Some(fallback),
        Some(value) => value
            .parse::<i64>()
            .ok()
            .filter(|parsed| (min..=max).contains(parsed)),
    }
}

pub(crate) fn hydrate_consumer(
    ns: &str,
    queue: &str,
    hash: &HashMap<String, String>,
) -> Option<Consumer> {
    if !is_valid_route_ns(ns) || !is_valid_queue_name(queue) {
        return None;
    }
    let worker = hash
        .get("worker")
        .filter(|value| is_valid_worker_name(value))
        .cloned()?;
    let version = hash
        .get("version")
        .filter(|value| parse_version_tag(value).is_ok())
        .cloned()?;
    let max_batch_size =
        bounded_i64(hash.get("max_batch_size"), 10, 1, MAX_BATCH_SIZE_CAP as i64)? as usize;
    let max_batch_timeout_ms = bounded_i64(
        hash.get("max_batch_timeout_ms"),
        5000,
        0,
        MAX_BATCH_TIMEOUT_MS,
    )?;
    let max_retries = bounded_i64(hash.get("max_retries"), 3, 0, MAX_RETRIES)?;
    let retry_delay_secs =
        bounded_i64(hash.get("retry_delay_secs"), 0, 0, MAX_QUEUE_DELAY_SECONDS)?;
    let dead_letter_queue = match hash.get("dead_letter_queue") {
        None => None,
        Some(value) if is_valid_queue_name(value) && value != queue => Some(value.clone()),
        Some(_) => return None,
    };
    Some(Consumer {
        ns: ns.to_string(),
        queue: queue.to_string(),
        max_batch_size,
        max_batch_timeout_ms,
        max_retries,
        retry_delay_secs,
        dead_letter_queue,
        worker_id: format!("{ns}:{worker}:{version}"),
    })
}

#[derive(Debug, PartialEq, Eq)]
struct ConsumerResolution {
    consumer: Option<Consumer>,
    invalid_projection: bool,
}

fn resolve_consumer_projection(
    ns: &str,
    queue: &str,
    hash: &HashMap<String, String>,
) -> ConsumerResolution {
    let consumer = hydrate_consumer(ns, queue, hash);
    ConsumerResolution {
        invalid_projection: !hash.is_empty() && consumer.is_none(),
        consumer,
    }
}

fn parse_reconcile_group_reply(
    reply: Vec<(i64, String)>,
    expected: usize,
) -> SchedulerResult<Vec<Result<(), String>>> {
    if reply.len() != expected {
        return Err(SchedulerError::internal_error(
            "queue consumer group reconcile count mismatch",
        ));
    }
    reply
        .into_iter()
        .map(|(status, message)| match status {
            1 if message.is_empty() => Ok(Ok(())),
            0 if !message.is_empty() => Ok(Err(message)),
            _ => Err(SchedulerError::internal_error(
                "invalid queue consumer group reconcile response",
            )),
        })
        .collect()
}

async fn reconcile_queue_groups(
    state: &AppState,
    stream_keys: &[String],
) -> SchedulerResult<Vec<Result<(), String>>> {
    if stream_keys.is_empty() {
        return Ok(Vec::new());
    }
    let mut keys = Vec::with_capacity(stream_keys.len() + 1);
    keys.push(QUEUE_STREAM_INDEX_KEY);
    keys.extend(stream_keys.iter().map(String::as_str));
    let reply: Vec<(i64, String)> = state
        .data_redis
        .with_conn(async |mut conn| {
            RECONCILE_QUEUE_GROUPS
                .prepare_invoke(&keys, &[CONSUMER_GROUP])
                .invoke_async(&mut conn)
                .await
        })
        .await?;
    parse_reconcile_group_reply(reply, stream_keys.len())
}

pub(crate) async fn queue_reconcile(state: AppState) -> SchedulerResult<()> {
    let mut seen = HashSet::new();
    let mut registry_changed = false;
    let mut reconcile_error_count = 0usize;
    let mut first_reconcile_error = None;
    let consumer_keys = indexed_existing_keys(&state, QUEUE_CONSUMER_INDEX_KEY, "hash").await?;
    for consumer_key_chunk in consumer_keys.chunks(QUEUE_RECONCILE_CONSUMER_HASH_BATCH_SIZE) {
        let consumer_hashes: Vec<HashMap<String, String>> = state
            .redis
            .with_conn(async |mut conn| {
                let mut pipe = redis::pipe();
                for key in consumer_key_chunk {
                    pipe.cmd("HGETALL").arg(key);
                }
                pipe.query_async(&mut conn).await
            })
            .await?;
        let mut resolved = Vec::with_capacity(consumer_key_chunk.len());
        for (key, hash) in consumer_key_chunk.iter().zip(consumer_hashes) {
            let Some((ns, queue)) = parse_consumer_key(key) else {
                continue;
            };
            let Some(consumer) = hydrate_consumer(&ns, &queue, &hash) else {
                continue;
            };
            let stream_key = queue_stream_key(&ns, &queue);
            resolved.push((stream_key, consumer));
        }
        let stream_keys = resolved
            .iter()
            .map(|(stream_key, _)| stream_key.clone())
            .collect::<Vec<_>>();
        let group_results = match reconcile_queue_groups(&state, &stream_keys).await {
            Ok(results) => results,
            Err(err) => {
                reconcile_error_count += resolved.len();
                first_reconcile_error.get_or_insert_with(|| err.message.clone());
                // The whole batch has an unknown outcome. Preserve any prior
                // in-memory consumers for these streams while later chunks
                // continue; per-stream script errors below remain removable.
                seen.extend(stream_keys);
                continue;
            }
        };
        let mut registry = state.queues.registry.write().await;
        for ((stream_key, consumer), group_result) in resolved.into_iter().zip(group_results) {
            if let Err(message) = group_result {
                reconcile_error_count += 1;
                first_reconcile_error.get_or_insert(message);
                continue;
            }
            seen.insert(stream_key.clone());
            let previous = registry.insert(stream_key, consumer.clone());
            if previous.as_ref() != Some(&consumer) {
                registry_changed = true;
            }
        }
    }
    {
        let mut registry = state.queues.registry.write().await;
        let before_len = registry.len();
        registry.retain(|key, _| seen.contains(key));
        if registry.len() != before_len {
            registry_changed = true;
        }
    }
    // consumer_streams is a derived snapshot. Refresh after additions and the
    // final retain so blocking reads see the registry state produced by this pass.
    refresh_consumer_streams(&state).await;

    let streams = indexed_existing_data_keys(&state, QUEUE_STREAM_INDEX_KEY, "stream").await?;
    {
        let mut known = state.queues.known_streams.write().await;
        known.clear();
        known.extend(streams);
    }
    let delayed = indexed_existing_data_keys(&state, QUEUE_DELAYED_INDEX_KEY, "zset").await?;
    let delayed_changed = {
        let delayed = delayed.into_iter().collect::<HashSet<_>>();
        let mut known = state.queues.known_delayed.write().await;
        if *known == delayed {
            false
        } else {
            *known = delayed;
            true
        }
    };
    if registry_changed || delayed_changed {
        state.queues.delayed_changed.notify_one();
    }
    if reconcile_error_count > 0 {
        return Err(SchedulerError::internal_error(format!(
            "queue consumer group reconcile failed for {reconcile_error_count} streams; first error: {}",
            first_reconcile_error.as_deref().unwrap_or("unknown error")
        )));
    }
    Ok(())
}

pub(crate) async fn queue_index_repair(state: AppState) -> SchedulerResult<()> {
    let consumer_result = repair_index(
        &state,
        QUEUE_CONSUMER_INDEX_KEY,
        QUEUE_CONSUMER_SCAN_PATTERN,
        "hash",
    )
    .await;
    let stream_result = repair_data_index(
        &state,
        QUEUE_STREAM_INDEX_KEY,
        QUEUE_STREAM_SCAN_PATTERN,
        "stream",
    )
    .await;
    let delayed_result = repair_data_index(
        &state,
        QUEUE_DELAYED_INDEX_KEY,
        QUEUE_DELAYED_SCAN_PATTERN,
        "zset",
    )
    .await;
    consumer_result?;
    stream_result?;
    delayed_result?;
    Ok(())
}

pub(crate) async fn refresh_consumer_streams(state: &AppState) {
    refresh_consumer_streams_for(&state.queues).await;
}

async fn refresh_consumer_streams_for(queues: &QueueState) {
    // Lock order is registry -> consumer_streams. Readers clone consumer_streams
    // before consulting registry, so no path holds consumer_streams while waiting
    // for registry.
    let registry = queues.registry.read().await;
    let snapshot = sorted_consumer_streams(&registry);
    *queues.consumer_streams.write().await = snapshot;
}

pub(crate) fn sorted_consumer_streams(registry: &HashMap<String, Consumer>) -> Arc<Vec<String>> {
    let mut streams = registry.keys().cloned().collect::<Vec<_>>();
    streams.sort();
    Arc::new(streams)
}

pub(crate) async fn resolve_consumer(
    state: &AppState,
    stream_key: &str,
    ns: &str,
    queue: &str,
) -> Result<Option<Consumer>, redis::RedisError> {
    let resolution =
        resolve_consumer_with_hash_loader(&state.queues, stream_key, ns, queue, |key| async move {
            state
                .redis
                .with_conn(async |mut conn| conn.hgetall(key).await)
                .await
        })
        .await?;
    if resolution.invalid_projection {
        log(
            state,
            LogLevel::Warn,
            "queue_consumer_projection_invalid",
            json!({ "ns": ns, "queue": queue }),
        );
    }
    Ok(resolution.consumer)
}

async fn resolve_consumer_with_hash_loader<F, Fut>(
    queues: &QueueState,
    stream_key: &str,
    ns: &str,
    queue: &str,
    load_hash: F,
) -> Result<ConsumerResolution, redis::RedisError>
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = Result<HashMap<String, String>, redis::RedisError>>,
{
    let hash = load_hash(queue_consumer_key(ns, queue)).await?;
    let resolution = resolve_consumer_projection(ns, queue, &hash);
    write_resolved_consumer(queues, stream_key, resolution.consumer.as_ref()).await;
    Ok(resolution)
}

async fn write_resolved_consumer(
    queues: &QueueState,
    stream_key: &str,
    consumer: Option<&Consumer>,
) {
    let previous = if let Some(consumer) = consumer {
        queues
            .registry
            .write()
            .await
            .insert(stream_key.to_string(), consumer.clone())
    } else {
        queues.registry.write().await.remove(stream_key)
    };
    let registry_changed = previous.as_ref() != consumer;
    if registry_changed {
        refresh_consumer_streams_for(queues).await;
        queues.delayed_changed.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::scheduler_projection_contract;
    use std::sync::{Arc as StdArc, Mutex};

    fn str_map(items: &[(&str, &str)]) -> HashMap<String, String> {
        items
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    fn valid_consumer_hash(worker: &str, version: &str) -> HashMap<String, String> {
        str_map(&[
            ("worker", worker),
            ("version", version),
            ("max_batch_size", "10"),
            ("max_batch_timeout_ms", "5000"),
            ("max_retries", "3"),
        ])
    }

    #[test]
    fn queue_index_lookup_plan_keeps_existing_members_and_removes_stale_members() {
        assert_eq!(
            crate::classify_index_members(Vec::new(), Vec::new()),
            crate::IndexedMemberPlan {
                existing: Vec::new(),
                stale: Vec::new(),
            }
        );

        assert_eq!(
            crate::classify_index_members(vec!["queue:demo:old:s".to_string()], vec![0],),
            crate::IndexedMemberPlan {
                existing: Vec::new(),
                stale: vec!["queue:demo:old:s".to_string()],
            }
        );

        assert_eq!(
            crate::classify_index_members(
                vec![
                    "queue:demo:jobs:s".to_string(),
                    "queue:demo:old:s".to_string(),
                ],
                vec![1, 0],
            ),
            crate::IndexedMemberPlan {
                existing: vec!["queue:demo:jobs:s".to_string()],
                stale: vec!["queue:demo:old:s".to_string()],
            }
        );
    }

    #[test]
    fn hydrate_consumer_preserves_defaults_and_valid_explicit_bounds() {
        let defaults = hydrate_consumer(
            "demo",
            "jobs",
            &str_map(&[("worker", "w"), ("version", "v1")]),
        )
        .unwrap();
        assert_eq!(defaults.max_batch_size, 10);
        assert_eq!(defaults.max_batch_timeout_ms, 5000);
        assert_eq!(defaults.max_retries, 3);
        assert_eq!(defaults.retry_delay_secs, 0);
        assert_eq!(defaults.dead_letter_queue, None);
        assert_eq!(defaults.worker_id, "demo:w:v1");

        let explicit = hydrate_consumer(
            "demo",
            "jobs",
            &str_map(&[
                ("worker", "w"),
                ("version", "v2"),
                ("max_batch_size", "25"),
                ("max_batch_timeout_ms", "1500"),
                ("max_retries", "7"),
                ("retry_delay_secs", "30"),
                ("dead_letter_queue", "failures"),
            ]),
        )
        .unwrap();
        assert_eq!(explicit.max_batch_size, 25);
        assert_eq!(explicit.max_batch_timeout_ms, 1500);
        assert_eq!(explicit.max_retries, 7);
        assert_eq!(explicit.retry_delay_secs, 30);
        assert_eq!(explicit.dead_letter_queue.as_deref(), Some("failures"));

        let zeroes = hydrate_consumer(
            "demo",
            "jobs",
            &str_map(&[
                ("worker", "w"),
                ("version", "v3"),
                ("max_batch_size", "100"),
                ("max_retries", "0"),
                ("max_batch_timeout_ms", "0"),
                ("retry_delay_secs", "0"),
            ]),
        )
        .unwrap();
        assert_eq!(zeroes.max_batch_size, MAX_BATCH_SIZE_CAP);
        assert_eq!(zeroes.max_retries, 0);
        assert_eq!(zeroes.max_batch_timeout_ms, 0);
        assert_eq!(zeroes.retry_delay_secs, 0);
    }

    #[test]
    fn hydrate_consumer_rejects_present_invalid_projection_fields() {
        for (field, value) in [
            ("max_batch_size", "0"),
            ("max_batch_size", "101"),
            ("max_batch_size", "not-a-number"),
            ("max_batch_timeout_ms", "-1"),
            ("max_batch_timeout_ms", "60001"),
            ("max_retries", "-1"),
            ("max_retries", "101"),
            ("retry_delay_secs", "-1"),
            ("retry_delay_secs", "86401"),
            ("dead_letter_queue", "bad:queue"),
            ("dead_letter_queue", "jobs"),
        ] {
            let mut hash = valid_consumer_hash("w", "v1");
            hash.insert(field.to_string(), value.to_string());
            assert!(
                hydrate_consumer("demo", "jobs", &hash).is_none(),
                "{field}={value} must fail closed"
            );
        }
    }

    #[test]
    fn reconcile_group_reply_preserves_per_stream_results() {
        assert_eq!(
            parse_reconcile_group_reply(
                vec![
                    (1, String::new()),
                    (0, "WRONGTYPE bad stream".to_string()),
                    (1, String::new()),
                ],
                3,
            )
            .unwrap(),
            vec![Ok(()), Err("WRONGTYPE bad stream".to_string()), Ok(())]
        );

        let err = parse_reconcile_group_reply(vec![(1, String::new())], 2)
            .expect_err("reply count mismatches must fail closed");
        assert_eq!(err.code, "internal_error");

        let err = parse_reconcile_group_reply(vec![(7, String::new())], 1)
            .expect_err("unknown script statuses must fail closed");
        assert_eq!(err.code, "internal_error");
    }

    #[test]
    fn reconcile_group_script_isolates_errors_and_indexes_only_healthy_streams() {
        let pcall = RECONCILE_QUEUE_GROUPS_SCRIPT
            .find("redis.pcall")
            .expect("script uses pcall for per-stream XGROUP errors");
        let busygroup = RECONCILE_QUEUE_GROUPS_SCRIPT
            .find("BUSYGROUP")
            .expect("script distinguishes an existing consumer group");
        let index = RECONCILE_QUEUE_GROUPS_SCRIPT
            .find("redis.pcall('SADD'")
            .expect("script indexes reconciled streams");
        let final_return = RECONCILE_QUEUE_GROUPS_SCRIPT
            .rfind("return results")
            .expect("script returns every per-stream result");

        assert!(pcall < busygroup);
        assert!(busygroup < index);
        assert!(index < final_return);
    }

    #[test]
    fn hydrate_consumer_matches_control_projection_fixture() {
        let fixture = scheduler_projection_contract();
        let contract = fixture.queue_consumer;
        let consumer = hydrate_consumer(&contract.ns, &contract.queue, &contract.fields)
            .expect("fixture queue consumer projection must hydrate");

        assert_eq!(
            consumer.worker_id,
            format!("{}:{}:{}", contract.ns, contract.worker, contract.version)
        );
        assert_eq!(consumer.max_batch_size, 12);
        assert_eq!(consumer.max_batch_timeout_ms, 250);
        assert_eq!(consumer.max_retries, 4);
        assert_eq!(consumer.retry_delay_secs, 17);
        assert_eq!(consumer.dead_letter_queue.as_deref(), Some("jobs-dlq"));
    }

    #[test]
    fn hydrate_consumer_rejects_noncanonical_dispatch_identity() {
        assert!(hydrate_consumer("demo", "jobs", &HashMap::new()).is_none());
        assert!(hydrate_consumer("demo", "jobs", &str_map(&[("version", "v1")])).is_none());
        assert!(hydrate_consumer("demo", "jobs", &str_map(&[("worker", "w")])).is_none());
        assert!(
            hydrate_consumer("__platform__", "jobs", &valid_consumer_hash("w", "v1")).is_none()
        );
        assert!(
            hydrate_consumer("demo", "jobs", &valid_consumer_hash("bad:worker", "v1")).is_none()
        );
        assert!(hydrate_consumer("demo", "jobs", &valid_consumer_hash("w", "v01")).is_none());
    }

    #[test]
    fn sorted_consumer_streams_returns_stable_key_snapshot() {
        let mut registry = HashMap::new();
        let jobs = hydrate_consumer("demo", "jobs", &valid_consumer_hash("w", "v1")).unwrap();
        registry.insert(queue_stream_key("demo", "z"), jobs.clone());
        registry.insert(queue_stream_key("demo", "a"), jobs);

        let snapshot = sorted_consumer_streams(&registry);
        assert_eq!(
            snapshot.as_ref(),
            &vec!["queue:demo:a:s".to_string(), "queue:demo:z:s".to_string()]
        );
    }

    #[tokio::test]
    async fn resolve_consumer_loads_authoritative_hash_and_refreshes_registry() {
        let queues = QueueState::default();
        let stream_key = queue_stream_key("demo", "jobs");
        let stale =
            hydrate_consumer("demo", "jobs", &valid_consumer_hash("old-worker", "v1")).unwrap();
        queues
            .registry
            .write()
            .await
            .insert(stream_key.clone(), stale);
        refresh_consumer_streams_for(&queues).await;

        let loaded_keys = StdArc::new(Mutex::new(Vec::new()));
        let authoritative = str_map(&[
            ("worker", "fresh-worker"),
            ("version", "v2"),
            ("max_batch_size", "4"),
            ("max_batch_timeout_ms", "5000"),
            ("max_retries", "3"),
        ]);
        let resolution = resolve_consumer_with_hash_loader(&queues, &stream_key, "demo", "jobs", {
            let loaded_keys = loaded_keys.clone();
            move |key| {
                let loaded_keys = loaded_keys.clone();
                let authoritative = authoritative.clone();
                async move {
                    loaded_keys.lock().unwrap().push(key);
                    Ok::<_, redis::RedisError>(authoritative)
                }
            }
        })
        .await
        .unwrap();
        assert!(!resolution.invalid_projection);
        let consumer = resolution.consumer.unwrap();

        assert_eq!(
            loaded_keys.lock().unwrap().as_slice(),
            &[queue_consumer_key("demo", "jobs")]
        );
        assert_eq!(consumer.worker_id, "demo:fresh-worker:v2");
        assert_eq!(consumer.max_batch_size, 4);
        assert_eq!(
            queues
                .registry
                .read()
                .await
                .get(&stream_key)
                .map(|consumer| consumer.worker_id.as_str()),
            Some("demo:fresh-worker:v2")
        );
        assert_eq!(
            queues.consumer_streams.read().await.as_ref(),
            &vec![stream_key]
        );
    }

    #[tokio::test]
    async fn resolve_consumer_treats_missing_or_invalid_identity_as_absent() {
        for (authoritative, invalid_projection) in [
            (HashMap::new(), false),
            (
                str_map(&[("worker", "worker"), ("version", "invalid")]),
                true,
            ),
        ] {
            let queues = QueueState::default();
            let stream_key = queue_stream_key("demo", "jobs");
            let stale =
                hydrate_consumer("demo", "jobs", &valid_consumer_hash("old-worker", "v1")).unwrap();
            queues
                .registry
                .write()
                .await
                .insert(stream_key.clone(), stale);
            refresh_consumer_streams_for(&queues).await;

            let resolution = resolve_consumer_with_hash_loader(
                &queues,
                &stream_key,
                "demo",
                "jobs",
                |_| async { Ok::<_, redis::RedisError>(authoritative) },
            )
            .await
            .unwrap();

            assert_eq!(resolution.consumer, None);
            assert_eq!(resolution.invalid_projection, invalid_projection);
            assert!(!queues.registry.read().await.contains_key(&stream_key));
            assert!(queues.consumer_streams.read().await.is_empty());
        }
    }
}
