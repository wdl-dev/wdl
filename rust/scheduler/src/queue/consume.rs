use std::time::Duration;

use redis::streams::StreamReadReply;
use serde_json::json;
use tokio::time::sleep;

use crate::{
    AppState, CONSUMER_GROUP, LogLevel, SchedulerError, SchedulerResult, log, now_ms,
    redis_fields_with_error, scheduler_fields_with_error,
};

use super::{
    Consumer, dispatch_messages, entries_to_messages, move_to_orphaned, parse_stream_key,
    queue_reconcile, redis_error_is_nogroup, resolve_consumer, stream_id_to_entry,
};

pub(crate) async fn queue_consume_loop(state: AppState) -> SchedulerResult<()> {
    let mut conn = state
        .data_redis_client
        .get_connection_manager_with_config(crate::blocking_redis_connection_config())
        .await
        .map_err(SchedulerError::from)?;
    while !state.is_shutting_down() {
        let streams = state.queues.consumer_streams.read().await.clone();
        if streams.is_empty() {
            sleep(Duration::from_millis(state.config.queue_block_ms)).await;
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
            for stream in streams.iter() {
                cmd.arg(stream);
            }
            for _ in streams.iter() {
                cmd.arg(">");
            }
            cmd.query_async(&mut conn).await
        };
        let reply = match entries {
            Ok(reply) => reply,
            Err(err) if redis_error_is_nogroup(&err) => {
                // Likely a newly registered stream whose group hasn't been
                // MKSTREAM'd yet — run reconcile synchronously so the next
                // iteration finds the group. BUSYGROUP on retry is fine.
                if let Err(reconcile_err) = queue_reconcile(state.clone()).await {
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
        let mut tasks = Vec::new();
        for key in reply.keys {
            let Some(guard) = state.begin_in_flight() else {
                break;
            };
            let child = state.clone();
            let Ok(permit) = state.dispatch.queue.clone().acquire_owned().await else {
                break;
            };
            tasks.push(tokio::spawn(async move {
                let _guard = guard;
                let _permit = permit;
                let stream_key = key.key;
                let raw = key
                    .ids
                    .into_iter()
                    .map(stream_id_to_entry)
                    .collect::<Vec<_>>();
                if raw.is_empty() {
                    return;
                }
                let Some((ns, queue)) = parse_stream_key(&stream_key) else {
                    log(
                        &child,
                        LogLevel::Warn,
                        "queue_stream_unparseable",
                        json!({ "stream": stream_key }),
                    );
                    return;
                };
                match resolve_consumer(&child, &stream_key, &ns, &queue).await {
                    Ok(Some(consumer)) => {
                        let messages = entries_to_messages(raw, now_ms());
                        if let Err(err) =
                            dispatch_messages(&child, messages, &stream_key, &consumer, "queue")
                                .await
                        {
                            log(
                                &child,
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
                        if let Err(err) =
                            move_to_orphaned(&child, &stream_key, &ns, &queue, raw).await
                        {
                            log(
                                &child,
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
                            &child,
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
            }));
        }
        for task in tasks {
            if let Err(err) = task.await {
                log(
                    &state,
                    LogLevel::Error,
                    "queue_stream_dispatch_failed",
                    crate::error_fields(
                        if err.is_panic() { "Panic" } else { "JoinError" },
                        err.to_string(),
                    ),
                );
            }
        }
    }
    Ok(())
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

async fn queue_xread_count(state: &AppState, streams: &[String]) -> usize {
    let registry = state.queues.registry.read().await;
    queue_xread_count_from_consumers(streams.iter().map(|stream| registry.get(stream)))
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
}
