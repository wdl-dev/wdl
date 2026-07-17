use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;

use redis::{AsyncCommands, Value};
use wdl_rust_common::identity::{is_valid_route_ns, is_valid_worker_name};
use wdl_rust_common::queue_keys::is_valid_queue_name;
use wdl_rust_common::worker_contract::parse_version_tag;

use crate::{
    AppState, CONSUMER_GROUP, MAX_BATCH_SIZE_CAP, QueueState, SchedulerResult, indexed_data_keys,
    indexed_keys,
};

use super::{
    Consumer, QUEUE_CONSUMER_INDEX_KEY, QUEUE_CONSUMER_SCAN_PATTERN, QUEUE_DELAYED_INDEX_KEY,
    QUEUE_DELAYED_SCAN_PATTERN, QUEUE_STREAM_INDEX_KEY, QUEUE_STREAM_SCAN_PATTERN,
    parse_consumer_key, queue_consumer_key, queue_stream_key, redis_error_is_busygroup,
};

const QUEUE_RECONCILE_CONSUMER_HASH_BATCH_SIZE: usize = 128;

fn finite_or(value: Option<&String>, fallback: i64) -> i64 {
    value
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(fallback)
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
        finite_or(hash.get("max_batch_size"), 10).clamp(1, MAX_BATCH_SIZE_CAP as i64) as usize;
    let max_batch_timeout_ms = finite_or(hash.get("max_batch_timeout_ms"), 5000);
    let max_retries = finite_or(hash.get("max_retries"), 3);
    let retry_delay_secs = finite_or(hash.get("retry_delay_secs"), 0).max(0);
    let dead_letter_queue = hash
        .get("dead_letter_queue")
        .filter(|value| !value.is_empty())
        .cloned();
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

async fn flush_indexed_streams(
    state: &AppState,
    indexed_streams: &mut Vec<String>,
) -> SchedulerResult<()> {
    if indexed_streams.is_empty() {
        return Ok(());
    }
    let streams = std::mem::take(indexed_streams);
    let _: i64 = state
        .data_redis
        .with_conn(async |mut conn| {
            redis::cmd("SADD")
                .arg(QUEUE_STREAM_INDEX_KEY)
                .arg(streams)
                .query_async(&mut conn)
                .await
        })
        .await?;
    Ok(())
}

pub(crate) async fn queue_reconcile(state: AppState) -> SchedulerResult<()> {
    let mut seen = HashSet::new();
    let mut registry_changed = false;
    let consumer_keys = indexed_keys(
        &state,
        QUEUE_CONSUMER_INDEX_KEY,
        QUEUE_CONSUMER_SCAN_PATTERN,
    )
    .await?;
    let mut indexed_streams = Vec::new();
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
        for (key, hash) in consumer_key_chunk.iter().zip(consumer_hashes) {
            let Some((ns, queue)) = parse_consumer_key(key) else {
                continue;
            };
            let Some(consumer) = hydrate_consumer(&ns, &queue, &hash) else {
                continue;
            };
            let stream_key = queue_stream_key(&ns, &queue);
            seen.insert(stream_key.clone());
            // Register the consumer before MKSTREAM: consume_loop may pick up
            // the stream before its group exists. The NOGROUP branch there
            // triggers an immediate reconcile, and XGROUP CREATE is idempotent
            // via BUSYGROUP so a retry does no harm.
            let previous = state
                .queues
                .registry
                .write()
                .await
                .insert(stream_key.clone(), consumer.clone());
            if previous.as_ref() != Some(&consumer) {
                registry_changed = true;
            }
            let group_result: Result<Value, redis::RedisError> = state
                .data_redis
                .with_conn(async |mut conn| {
                    let stream_key = stream_key.clone();
                    redis::cmd("XGROUP")
                        .arg("CREATE")
                        .arg(stream_key)
                        .arg(CONSUMER_GROUP)
                        .arg("0")
                        .arg("MKSTREAM")
                        .query_async(&mut conn)
                        .await
                })
                .await;
            if let Err(err) = group_result
                && !redis_error_is_busygroup(&err)
            {
                flush_indexed_streams(&state, &mut indexed_streams).await?;
                return Err(err.into());
            }
            indexed_streams.push(stream_key);
            if indexed_streams.len() >= QUEUE_RECONCILE_CONSUMER_HASH_BATCH_SIZE {
                flush_indexed_streams(&state, &mut indexed_streams).await?;
            }
        }
    }
    flush_indexed_streams(&state, &mut indexed_streams).await?;
    {
        let mut registry = state.queues.registry.write().await;
        let before_len = registry.len();
        registry.retain(|key, _| seen.contains(key));
        if registry.len() != before_len {
            registry_changed = true;
        }
    }
    // consumer_streams is a derived snapshot. Refresh after every successful
    // reconcile so a prior XGROUP error after registry insertion cannot
    // strand a stream outside the consumer loop.
    refresh_consumer_streams(&state).await;

    let streams =
        indexed_data_keys(&state, QUEUE_STREAM_INDEX_KEY, QUEUE_STREAM_SCAN_PATTERN).await?;
    {
        let mut known = state.queues.known_streams.write().await;
        known.clear();
        known.extend(streams);
    }
    let delayed =
        indexed_data_keys(&state, QUEUE_DELAYED_INDEX_KEY, QUEUE_DELAYED_SCAN_PATTERN).await?;
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
    resolve_consumer_with_hash_loader(&state.queues, stream_key, ns, queue, |key| async move {
        state
            .redis
            .with_conn(async |mut conn| conn.hgetall(key).await)
            .await
    })
    .await
}

async fn resolve_consumer_with_hash_loader<F, Fut>(
    queues: &QueueState,
    stream_key: &str,
    ns: &str,
    queue: &str,
    load_hash: F,
) -> Result<Option<Consumer>, redis::RedisError>
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = Result<HashMap<String, String>, redis::RedisError>>,
{
    let hash = load_hash(queue_consumer_key(ns, queue)).await?;
    let consumer = hydrate_consumer(ns, queue, &hash);
    write_resolved_consumer(queues, stream_key, consumer.as_ref()).await;
    Ok(consumer)
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
    fn queue_index_lookup_plan_covers_backfill_and_stale_branches() {
        assert_eq!(
            crate::classify_index_members(Vec::new(), Vec::new()),
            crate::IndexedMemberPlan {
                existing: Vec::new(),
                stale: Vec::new(),
                needs_scan: true,
            }
        );

        assert_eq!(
            crate::classify_index_members(vec!["queue:demo:old:s".to_string()], vec![0],),
            crate::IndexedMemberPlan {
                existing: Vec::new(),
                stale: vec!["queue:demo:old:s".to_string()],
                needs_scan: true,
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
                needs_scan: false,
            }
        );

        assert_eq!(crate::backfill_index_members(&[]), None);
        assert_eq!(
            crate::backfill_index_members(&["queue:demo:jobs:s".to_string()]),
            Some(vec!["queue:demo:jobs:s".to_string()])
        );
    }

    #[test]
    fn hydrate_consumer_preserves_defaults_and_runtime_batch_cap() {
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
                ("max_batch_size", "1000"),
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
        let consumer = resolve_consumer_with_hash_loader(&queues, &stream_key, "demo", "jobs", {
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
        .unwrap()
        .unwrap();

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
        for authoritative in [
            HashMap::new(),
            str_map(&[("worker", "worker"), ("version", "invalid")]),
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

            let consumer = resolve_consumer_with_hash_loader(
                &queues,
                &stream_key,
                "demo",
                "jobs",
                |_| async { Ok::<_, redis::RedisError>(authoritative) },
            )
            .await
            .unwrap();

            assert_eq!(consumer, None);
            assert!(!queues.registry.read().await.contains_key(&stream_key));
            assert!(queues.consumer_streams.read().await.is_empty());
        }
    }
}
