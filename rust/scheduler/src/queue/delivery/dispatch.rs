use std::collections::VecDeque;

use redis::Value;
use serde_json::json;

use crate::{
    AppState, CONSUMER_GROUP, LogLevel, RuntimeResponse, SERVICE, SchedulerError, SchedulerResult,
    log, now_ms, post_runtime,
};

use super::super::{Consumer, OutcomePlan, QueueMessage};
use super::message::messages_for_runtime;
use super::outcome::decide_outcome;
use super::retry::{
    record_queue_dispatch_failures, record_queue_messages, retry_messages_batch,
    terminal_messages_batch,
};

pub(crate) async fn apply_queue_outcome(
    state: &AppState,
    res: &RuntimeResponse,
    messages: &[QueueMessage],
    stream_key: &str,
    consumer: &Consumer,
) -> SchedulerResult<()> {
    match decide_outcome(res, messages) {
        OutcomePlan::RetryAll {
            kind,
            reason,
            messages,
        } => {
            if kind == "transport_error" {
                log(
                    state,
                    LogLevel::Warn,
                    "queue_dispatch_transport_error",
                    json!({
                        "worker_id": consumer.worker_id,
                        "queue": consumer.queue,
                        "error_message": reason,
                    }),
                );
            }
            let retry_all_count = messages.len();
            let retries = messages
                .into_iter()
                .map(|msg| (msg, consumer.retry_delay_secs))
                .collect::<Vec<_>>();
            retry_messages_batch(state, retries, stream_key, consumer).await?;
            record_queue_dispatch_failures(&state.metrics, kind, retry_all_count);
        }
        OutcomePlan::TerminalAll {
            kind,
            reason,
            messages,
        } => {
            let terminal_count = messages.len();
            log(
                state,
                LogLevel::Warn,
                "queue_dispatch_terminal_error",
                json!({
                    "worker_id": consumer.worker_id,
                    "queue": consumer.queue,
                    "kind": kind,
                    "reason": reason.clone(),
                    "message_count": terminal_count,
                }),
            );
            terminal_messages_batch(state, messages, stream_key, consumer, &reason).await?;
            record_queue_dispatch_failures(&state.metrics, kind, terminal_count);
        }
        OutcomePlan::Normal { to_ack, to_retry } => {
            if !to_ack.is_empty() {
                let acked = to_ack.len();
                let ids = to_ack
                    .iter()
                    .map(|m| m.stream_id.clone())
                    .collect::<Vec<_>>();
                let key = stream_key.to_string();
                state
                    .data_redis
                    .with_conn(async |mut conn| {
                        redis::pipe()
                            .atomic()
                            .cmd("XACK")
                            .arg(&key)
                            .arg(CONSUMER_GROUP)
                            .arg(&ids)
                            .cmd("XDEL")
                            .arg(&key)
                            .arg(&ids)
                            .query_async::<Value>(&mut conn)
                            .await
                    })
                    .await?;
                record_queue_messages(&state.metrics, "ack", acked);
            }
            let retries = to_retry
                .into_iter()
                .map(|(msg, delay)| (msg, delay.unwrap_or(consumer.retry_delay_secs)))
                .collect::<Vec<_>>();
            retry_messages_batch(state, retries, stream_key, consumer).await?;
        }
    }
    Ok(())
}

fn queue_dispatch_request_id(
    kind: &str,
    instance_id: &str,
    queue: &str,
    offset: usize,
    split_depth: usize,
) -> String {
    let base = format!("{kind}-{instance_id}-{}-{queue}-{offset}", now_ms());
    if split_depth == 0 {
        base
    } else {
        format!("{base}-split{split_depth}")
    }
}

fn should_split_oversized_batch(res: &RuntimeResponse, batch_len: usize) -> bool {
    res.status == Some(413) && batch_len > 1
}

fn split_oversized_batch(
    mut messages: Vec<QueueMessage>,
) -> Option<(Vec<QueueMessage>, Vec<QueueMessage>)> {
    if messages.len() <= 1 {
        return None;
    }
    let right = messages.split_off(messages.len() / 2);
    Some((messages, right))
}

pub(crate) async fn dispatch_messages(
    state: &AppState,
    messages: Vec<QueueMessage>,
    stream_key: &str,
    consumer: &Consumer,
    kind: &str,
) -> SchedulerResult<()> {
    let size = consumer.max_batch_size;
    for (index, chunk) in messages.chunks(size).enumerate() {
        let mut pending = VecDeque::from([(index * size, 0_usize, chunk.to_vec())]);
        while let Some((offset, split_depth, batch)) = pending.pop_front() {
            let request_id = queue_dispatch_request_id(
                kind,
                &state.instance_id,
                &consumer.queue,
                offset,
                split_depth,
            );
            let fired_at = now_ms();
            let res = post_runtime(
                state,
                "/_queued",
                json!({
                    "queue": consumer.queue,
                    "messages": messages_for_runtime(&batch),
                }),
                &consumer.worker_id,
                &request_id,
            )
            .await;
            let duration_ms = now_ms() - fired_at;
            let outcome = crate::runtime_outcome_label(&res);
            state.metrics.observe(
                "queue_batch_duration_ms",
                &[("service", SERVICE), ("outcome", outcome)],
                duration_ms as f64,
            );
            log(
                state,
                LogLevel::Info,
                "queue_batch_dispatched",
                json!({
                    "request_id": request_id.as_str(),
                    "worker_id": consumer.worker_id,
                    "queue": consumer.queue,
                    "batch_size": batch.len(),
                    "max_batch_timeout_ms": consumer.max_batch_timeout_ms,
                    "duration_ms": duration_ms,
                    "outcome": outcome,
                }),
            );
            if should_split_oversized_batch(&res, batch.len()) {
                let batch_size = batch.len();
                let Some((left, right)) = split_oversized_batch(batch) else {
                    let err = SchedulerError::internal_error(
                        "oversized queue batch split requested for single-message batch",
                    );
                    log(
                        state,
                        LogLevel::Error,
                        "queue_batch_split_failed",
                        json!({
                            "request_id": request_id,
                            "worker_id": consumer.worker_id,
                            "queue": consumer.queue,
                            "batch_size": batch_size,
                            "error_code": err.code,
                            "error_message": err.message.as_str(),
                        }),
                    );
                    return Err(err);
                };
                let right_offset = offset + left.len();
                log(
                    state,
                    LogLevel::Warn,
                    "queue_batch_split_after_oversize",
                    json!({
                        "request_id": request_id,
                        "worker_id": consumer.worker_id,
                        "queue": consumer.queue,
                        "left_size": left.len(),
                        "right_size": right.len(),
                    }),
                );
                pending.push_front((right_offset, split_depth + 1, right));
                pending.push_front((offset, split_depth + 1, left));
                continue;
            }
            apply_queue_outcome(state, &res, &batch, stream_key, consumer).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(id: &str, stream_id: &str) -> QueueMessage {
        QueueMessage {
            stream_id: stream_id.to_string(),
            id: id.to_string(),
            body_b64: "aGVsbG8=".to_string(),
            content_type: "text".to_string(),
            attempts: "0".to_string(),
            first_seen_ms: "1699999999999".to_string(),
        }
    }

    #[test]
    fn oversized_runtime_batches_split_until_single_message() {
        let batch = vec![
            msg("a", "1-0"),
            msg("b", "2-0"),
            msg("c", "3-0"),
            msg("d", "4-0"),
            msg("e", "5-0"),
        ];
        let (left, right) = split_oversized_batch(batch).expect("batch should split");

        assert_eq!(
            left.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert_eq!(
            right.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
            ["c", "d", "e"]
        );
        assert!(split_oversized_batch(vec![msg("single", "1-0")]).is_none());
    }

    #[test]
    fn only_multi_message_413_batches_are_split_before_outcome_mapping() {
        let oversized = RuntimeResponse {
            status: Some(413),
            json: None,
            text: Some("payload too large".to_string()),
            error: None,
        };
        let auth = RuntimeResponse {
            status: Some(401),
            json: None,
            text: Some("unauthorized".to_string()),
            error: None,
        };

        assert!(should_split_oversized_batch(&oversized, 2));
        assert!(!should_split_oversized_batch(&oversized, 1));
        assert!(!should_split_oversized_batch(&auth, 2));
    }
}
