use std::sync::Arc;
use std::time::Duration;

use redis::streams::StreamReadReply;
use serde_json::json;
use tokio::time::sleep;

use crate::{
    AppState, CONSUMER_GROUP, LogLevel, QueueState, SchedulerError, SchedulerResult, log, now_ms,
    redis_fields_with_error, scheduler_fields_with_error,
};

use super::{
    Consumer, StreamEntry, dispatch_messages, entries_to_messages, move_to_orphaned,
    parse_stream_key, queue_reconcile_after_nogroup, redis_error_is_nogroup, resolve_consumer,
    stream_id_to_entry,
};

struct QueueStreamClaim {
    queues: Arc<QueueState>,
    stream: String,
}

impl QueueStreamClaim {
    fn acquire(queues: Arc<QueueState>, stream: String) -> Option<Self> {
        let inserted = {
            let mut dispatching = queues
                .dispatching_streams
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            dispatching.insert(stream.clone())
        };
        inserted.then_some(Self { queues, stream })
    }
}

impl Drop for QueueStreamClaim {
    fn drop(&mut self) {
        {
            let mut dispatching = self
                .queues
                .dispatching_streams
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            dispatching.remove(&self.stream);
        }
        self.queues.stream_dispatch_completed.notify_one();
    }
}

pub(crate) async fn queue_consume_loop(state: AppState) -> SchedulerResult<()> {
    let mut conn = state
        .data_redis_client
        .get_connection_manager_with_config(crate::blocking_redis_connection_config())
        .await
        .map_err(SchedulerError::from)?;
    while !state.is_shutting_down() {
        let dispatch_completed = state.queues.stream_dispatch_completed.notified();
        let registered_streams = state.queues.consumer_streams.read().await.clone();
        let (streams, has_in_flight) =
            available_queue_streams(registered_streams.as_slice(), &state.queues);
        if streams.is_empty() {
            tokio::select! {
                _ = sleep(Duration::from_millis(state.config.queue_block_ms)) => {}
                _ = dispatch_completed, if has_in_flight => {}
                _ = state.shutdown.stop_notified() => break,
            }
            continue;
        }
        let read_count = queue_xread_count(&state, streams.as_slice()).await;
        let entries: Result<StreamReadReply, redis::RedisError> = {
            let mut cmd = redis::cmd("XREADGROUP");
            cmd.arg("GROUP")
                .arg(CONSUMER_GROUP)
                .arg(&state.instance_id)
                .arg("COUNT")
                .arg(read_count)
                .arg("BLOCK")
                .arg(state.config.queue_block_ms)
                .arg("STREAMS");
            for stream in &streams {
                cmd.arg(stream);
            }
            for _ in &streams {
                cmd.arg(">");
            }
            cmd.query_async(&mut conn).await
        };
        let reply = match entries {
            Ok(reply) => reply,
            Err(err) if redis_error_is_nogroup(&err) => {
                // A stream in the read snapshot has no group. Force group repair
                // synchronously so the next iteration can retry the whole snapshot.
                if let Err(reconcile_err) = queue_reconcile_after_nogroup(state.clone()).await {
                    log(
                        &state,
                        LogLevel::Error,
                        "queue_reconcile_failed",
                        scheduler_fields_with_error(
                            json!({ "source": "xreadgroup_nogroup" }),
                            &reconcile_err,
                        ),
                    );
                    sleep(Duration::from_millis(500)).await;
                } else {
                    sleep(Duration::from_millis(50)).await;
                }
                continue;
            }
            Err(err) if is_block_timeout(&err) => {
                continue;
            }
            Err(err) => {
                log(
                    &state,
                    LogLevel::Error,
                    "queue_xreadgroup_failed",
                    redis_fields_with_error(json!({}), &err),
                );
                sleep(Duration::from_millis(1000)).await;
                continue;
            }
        };
        if reply.keys.is_empty() {
            continue;
        }
        for key in reply.keys {
            let stream_key = key.key;
            let raw = key
                .ids
                .into_iter()
                .map(stream_id_to_entry)
                .collect::<Vec<_>>();
            if raw.is_empty() {
                continue;
            }
            let Ok(permit) = state.dispatch.queue.clone().acquire_owned().await else {
                break;
            };
            let Some(stream_claim) =
                QueueStreamClaim::acquire(state.queues.clone(), stream_key.clone())
            else {
                continue;
            };
            let child = state.clone();
            let panic_fields = json!({ "stream": &stream_key });
            state.spawn_tracked("queue_stream_dispatch_failed", panic_fields, async move {
                let _stream_claim = stream_claim;
                let _permit = permit;
                dispatch_queue_stream(&child, stream_key, raw).await;
            });
        }
    }
    Ok(())
}

async fn dispatch_queue_stream(state: &AppState, stream_key: String, raw: Vec<StreamEntry>) {
    let Some((ns, queue)) = parse_stream_key(&stream_key) else {
        log(
            state,
            LogLevel::Warn,
            "queue_stream_unparseable",
            json!({ "stream": stream_key }),
        );
        return;
    };
    match resolve_consumer(state, &stream_key, &ns, &queue).await {
        Ok(Some(consumer)) => {
            let messages = entries_to_messages(raw, now_ms());
            if let Err(err) =
                dispatch_messages(state, messages, &stream_key, &consumer, "queue").await
            {
                log(
                    state,
                    LogLevel::Error,
                    "queue_stream_dispatch_failed",
                    scheduler_fields_with_error(
                        json!({
                            "stream": stream_key,
                            "ns": ns,
                            "queue": queue,
                        }),
                        &err,
                    ),
                );
            }
        }
        Ok(None) => {
            if let Err(err) = move_to_orphaned(state, &stream_key, &ns, &queue, raw).await {
                log(
                    state,
                    LogLevel::Error,
                    "queue_stream_dispatch_failed",
                    scheduler_fields_with_error(
                        json!({
                            "stream": stream_key,
                            "ns": ns,
                            "queue": queue,
                        }),
                        &err,
                    ),
                );
            }
        }
        Err(err) => {
            log(
                state,
                LogLevel::Error,
                "queue_stream_dispatch_failed",
                redis_fields_with_error(
                    json!({
                        "stream": stream_key,
                        "ns": ns,
                        "queue": queue,
                    }),
                    &err,
                ),
            );
        }
    }
}

fn available_queue_streams<'a>(streams: &'a [String], queues: &QueueState) -> (Vec<&'a str>, bool) {
    let dispatching = queues
        .dispatching_streams
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let available = streams
        .iter()
        .filter(|stream| !dispatching.contains(stream.as_str()))
        .map(String::as_str)
        .collect();
    (available, !dispatching.is_empty())
}

pub(crate) fn queue_xread_count_from_consumers<'a>(
    consumers: impl IntoIterator<Item = Option<&'a Consumer>>,
) -> usize {
    consumers
        .into_iter()
        .filter_map(|consumer| consumer.map(|c| c.max_batch_size))
        .min()
        .unwrap_or(1)
}

async fn queue_xread_count(state: &AppState, streams: &[&str]) -> usize {
    let registry = state.queues.registry.read().await;
    queue_xread_count_from_consumers(streams.iter().map(|stream| registry.get(*stream)))
}

pub(crate) fn is_block_timeout(err: &redis::RedisError) -> bool {
    // XREADGROUP BLOCK timeout returns Nil → empty StreamReadReply, so this
    // only fires on true I/O-level timeouts (e.g. a response timeout set on
    // the connection manager).
    err.is_timeout()
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::ErrorKind;
    use std::io;

    fn consumer(max_batch_size: usize) -> Consumer {
        Consumer {
            ns: "demo".to_string(),
            queue: "jobs".to_string(),
            max_batch_size,
            max_batch_timeout_ms: 1000,
            max_retries: 3,
            retry_delay_secs: 0,
            dead_letter_queue: None,
            worker_id: "demo:w:v1".to_string(),
        }
    }

    #[test]
    fn is_block_timeout_matches_io_timeout_only() {
        let timed_out: redis::RedisError = io::Error::from(io::ErrorKind::TimedOut).into();
        assert!(is_block_timeout(&timed_out));
        let would_block: redis::RedisError = io::Error::from(io::ErrorKind::WouldBlock).into();
        assert!(is_block_timeout(&would_block));
        // Non-IO errors must NOT be misclassified as a BLOCK timeout — that
        // would silently swallow real protocol failures (NOGROUP, parser
        // errors, etc.) into a quiet `continue`.
        let client_err: redis::RedisError = (ErrorKind::Client, "boom").into();
        assert!(!is_block_timeout(&client_err));
    }

    #[test]
    fn queue_xread_count_never_exceeds_any_current_consumer_batch_cap() {
        let small = consumer(2);
        let large = consumer(25);

        assert_eq!(
            queue_xread_count_from_consumers([Some(&small), Some(&large)]),
            2
        );
        assert_eq!(queue_xread_count_from_consumers([Some(&large)]), 25);
        assert_eq!(queue_xread_count_from_consumers([None]), 1);
    }

    #[test]
    fn active_stream_is_excluded_until_its_dispatch_claim_drops() {
        let queues = Arc::new(QueueState::default());
        let streams = vec![
            "queue:demo:slow:s".to_string(),
            "queue:demo:fast:s".to_string(),
        ];
        let claim = QueueStreamClaim::acquire(queues.clone(), streams[0].clone()).unwrap();

        let (available, has_in_flight) = available_queue_streams(&streams, &queues);
        assert_eq!(available, [streams[1].as_str()]);
        assert!(has_in_flight);
        assert!(QueueStreamClaim::acquire(queues.clone(), streams[0].clone()).is_none());

        drop(claim);
        let (available, has_in_flight) = available_queue_streams(&streams, &queues);
        assert_eq!(available, [streams[0].as_str(), streams[1].as_str()]);
        assert!(!has_in_flight);
    }

    #[tokio::test]
    async fn dropping_a_stream_claim_wakes_a_consumer_with_no_available_streams() {
        let queues = Arc::new(QueueState::default());
        let claim =
            QueueStreamClaim::acquire(queues.clone(), "queue:demo:jobs:s".to_string()).unwrap();
        let completed = queues.stream_dispatch_completed.notified();

        drop(claim);

        tokio::time::timeout(Duration::from_millis(100), completed)
            .await
            .expect("claim drop should notify the consume loop");
    }
}
