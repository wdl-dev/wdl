use std::collections::{HashMap, HashSet};

use redis::Pipeline;
use serde_json::json;
use wdl_rust_common::redis_eval::append_eval_cmd;

use crate::{
    AppState, CONSUMER_GROUP, LogLevel, Metrics, SERVICE, SchedulerError, SchedulerResult, log,
    now_ms,
};

use super::super::{
    Consumer, MAX_QUEUE_DELAY_SECONDS, QUEUE_DELAYED_INDEX_KEY, QueueMessage, RetryAction,
    queue_delayed_key, queue_dlq_key,
};

const MILLIS_PER_SECOND: i64 = 1_000;
const TRANSITION_IMMEDIATE: &str = "immediate";
const TRANSITION_DELAY: &str = "delay";
const TRANSITION_DLQ: &str = "dlq";

const QUEUE_MESSAGE_TRANSITION_SCRIPT: &str = r#"
local function failure(result)
  if type(result) == "table" and result.err then
    return {0, tostring(result.err)}
  end
  return nil
end

local mode = ARGV[1]
local target_result
if mode == "immediate" then
  target_result = redis.pcall(
    "XADD", KEYS[2], "*",
    "id", ARGV[6],
    "body_b64", ARGV[7],
    "content_type", ARGV[8],
    "attempts", ARGV[9],
    "first_seen_ms", ARGV[10])
elseif mode == "delay" then
  local indexed = redis.pcall("SADD", KEYS[3], KEYS[2])
  local indexed_failure = failure(indexed)
  if indexed_failure then
    return indexed_failure
  end
  target_result = redis.pcall("ZADD", KEYS[2], ARGV[4], ARGV[5])
elseif mode == "dlq" then
  target_result = redis.pcall(
    "XADD", KEYS[2], "MAXLEN", "~", ARGV[3], "*",
    "id", ARGV[6],
    "body_b64", ARGV[7],
    "content_type", ARGV[8],
    "attempts", ARGV[9],
    "first_seen_ms", ARGV[10],
    "reason", ARGV[11])
else
  return {0, "invalid queue transition mode"}
end

local target_failure = failure(target_result)
if target_failure then
  return target_failure
end
local acked = redis.pcall("XACK", KEYS[1], ARGV[12], ARGV[2])
local ack_failure = failure(acked)
if ack_failure then
  return ack_failure
end
local deleted = redis.pcall("XDEL", KEYS[1], ARGV[2])
local delete_failure = failure(deleted)
if delete_failure then
  return delete_failure
end
return {1, ""}
"#;

struct DlqLog {
    target: String,
    msg_id: String,
    attempts: i64,
}

struct InvalidAttemptLog {
    msg_id: String,
    stream_id: String,
    attempts: String,
}

struct QueueTransition {
    action: &'static str,
    msg_id: String,
    stream_id: String,
    delayed_key: Option<String>,
    dlq_log: Option<DlqLog>,
    invalid_attempt_log: Option<InvalidAttemptLog>,
}

pub(crate) struct RetryBatchPlan {
    pub(crate) pipe: Pipeline,
    transitions: Vec<QueueTransition>,
}

struct TransitionBatchOutcome {
    retry_count: usize,
    dlq_count: usize,
    delayed_keys: HashSet<String>,
    dlq_logs: Vec<DlqLog>,
    invalid_attempt_logs: Vec<InvalidAttemptLog>,
    failures: Vec<QueueTransitionFailure>,
}

struct QueueTransitionFailure {
    action: &'static str,
    msg_id: String,
    stream_id: String,
    message: String,
}

pub(crate) fn decide_retry_action(
    msg: &QueueMessage,
    delay_secs: i64,
    max_retries: i64,
    dead_letter_queue: Option<&str>,
    queue: &str,
    now: i64,
) -> RetryAction {
    let attempts = parse_next_attempts(&msg.attempts);
    let mut base = HashMap::from([
        ("id".to_string(), msg.id.clone()),
        ("body_b64".to_string(), msg.body_b64.clone()),
        ("content_type".to_string(), msg.content_type.clone()),
        ("attempts".to_string(), attempts.to_string()),
        ("first_seen_ms".to_string(), msg.first_seen_ms.clone()),
    ]);

    if attempts > max_retries {
        base.insert("reason".to_string(), "max_retries_exceeded".to_string());
        return RetryAction::Dlq {
            attempts,
            target: dead_letter_queue.unwrap_or(queue).to_string(),
            entry: base,
        };
    }
    if delay_secs > 0 {
        return RetryAction::Delay {
            visible_at_ms: retry_visible_at_ms(now, delay_secs),
            entry: base,
        };
    }
    RetryAction::Immediate { entry: base }
}

fn retry_visible_at_ms(now: i64, delay_secs: i64) -> i64 {
    now.saturating_add(
        delay_secs
            .clamp(0, MAX_QUEUE_DELAY_SECONDS)
            .saturating_mul(MILLIS_PER_SECOND),
    )
}

fn parse_next_attempts(raw: &str) -> i64 {
    match raw.parse::<i64>() {
        Ok(attempts) if attempts >= 0 => attempts.checked_add(1).unwrap_or(1),
        _ => 1,
    }
}

fn valid_attempts(raw: &str) -> bool {
    matches!(raw.parse::<i64>(), Ok(attempts) if attempts >= 0 && attempts.checked_add(1).is_some())
}

pub(crate) async fn retry_messages_batch(
    state: &AppState,
    retries: Vec<(QueueMessage, i64)>,
    stream_key: &str,
    consumer: &Consumer,
) -> SchedulerResult<()> {
    if retries.is_empty() {
        return Ok(());
    }
    let plan = build_retry_batch_plan(
        retries,
        stream_key,
        consumer,
        state.config.max_dlq_len,
        now_ms(),
    )?;
    execute_transition_plan(state, plan, consumer, None).await
}

pub(crate) async fn terminal_messages_batch(
    state: &AppState,
    messages: Vec<QueueMessage>,
    stream_key: &str,
    consumer: &Consumer,
    reason: &str,
) -> SchedulerResult<()> {
    if messages.is_empty() {
        return Ok(());
    }
    let plan = build_terminal_batch_plan(
        messages,
        stream_key,
        consumer,
        state.config.max_dlq_len,
        reason,
    )?;
    execute_transition_plan(state, plan, consumer, Some(reason)).await
}

fn entry_field<'a>(entry: &'a HashMap<String, String>, field: &str) -> SchedulerResult<&'a str> {
    entry
        .get(field)
        .map(String::as_str)
        .ok_or_else(|| SchedulerError::internal_error(format!("queue retry entry missing {field}")))
}

fn append_transition(
    pipe: &mut Pipeline,
    keys: &[&str],
    mode: &'static str,
    msg: &QueueMessage,
    entry: &HashMap<String, String>,
    max_dlq_len: usize,
    delayed: Option<(i64, &str)>,
) -> SchedulerResult<()> {
    let max_dlq_len = max_dlq_len.to_string();
    let visible_at_ms = delayed
        .map(|(value, _)| value.to_string())
        .unwrap_or_default();
    let delayed_member = delayed.map(|(_, member)| member).unwrap_or("");
    append_eval_cmd(
        pipe,
        QUEUE_MESSAGE_TRANSITION_SCRIPT,
        keys,
        &[
            mode,
            &msg.stream_id,
            &max_dlq_len,
            &visible_at_ms,
            delayed_member,
            entry_field(entry, "id")?,
            entry_field(entry, "body_b64")?,
            entry_field(entry, "content_type")?,
            entry_field(entry, "attempts")?,
            entry_field(entry, "first_seen_ms")?,
            entry.get("reason").map(String::as_str).unwrap_or(""),
            CONSUMER_GROUP,
        ],
    );
    Ok(())
}

fn invalid_attempt_log(msg: &QueueMessage) -> Option<InvalidAttemptLog> {
    (!valid_attempts(&msg.attempts)).then(|| InvalidAttemptLog {
        msg_id: msg.id.clone(),
        stream_id: msg.stream_id.clone(),
        attempts: msg.attempts.clone(),
    })
}

fn parse_transition_results(
    transitions: Vec<QueueTransition>,
    replies: Vec<(i64, String)>,
) -> SchedulerResult<TransitionBatchOutcome> {
    if replies.len() != transitions.len() {
        return Err(SchedulerError::internal_error(
            "queue transition pipeline reply count mismatch",
        ));
    }
    let mut outcome = TransitionBatchOutcome {
        retry_count: 0,
        dlq_count: 0,
        delayed_keys: HashSet::new(),
        dlq_logs: Vec::new(),
        invalid_attempt_logs: Vec::new(),
        failures: Vec::new(),
    };
    for (transition, (code, message)) in transitions.into_iter().zip(replies) {
        match (code, message.is_empty()) {
            (1, true) => {
                match transition.action {
                    TRANSITION_IMMEDIATE | TRANSITION_DELAY => outcome.retry_count += 1,
                    TRANSITION_DLQ => outcome.dlq_count += 1,
                    _ => {
                        return Err(SchedulerError::internal_error(
                            "invalid queue transition action",
                        ));
                    }
                }
                if let Some(key) = transition.delayed_key {
                    outcome.delayed_keys.insert(key);
                }
                if let Some(log_entry) = transition.dlq_log {
                    outcome.dlq_logs.push(log_entry);
                }
                if let Some(log_entry) = transition.invalid_attempt_log {
                    outcome.invalid_attempt_logs.push(log_entry);
                }
            }
            (0, false) => outcome.failures.push(QueueTransitionFailure {
                action: transition.action,
                msg_id: transition.msg_id,
                stream_id: transition.stream_id,
                message,
            }),
            _ => {
                return Err(SchedulerError::internal_error(
                    "invalid queue transition response",
                ));
            }
        }
    }
    Ok(outcome)
}

async fn execute_transition_plan(
    state: &AppState,
    plan: RetryBatchPlan,
    consumer: &Consumer,
    terminal_reason: Option<&str>,
) -> SchedulerResult<()> {
    let RetryBatchPlan { pipe, transitions } = plan;
    let replies: Vec<(i64, String)> = state
        .data_redis
        .with_conn(async |mut conn| pipe.query_async(&mut conn).await)
        .await?;
    let outcome = parse_transition_results(transitions, replies)?;

    if !outcome.delayed_keys.is_empty() {
        state
            .queues
            .known_delayed
            .write()
            .await
            .extend(outcome.delayed_keys);
        state.queues.delayed_changed.notify_one();
    }
    for log_entry in outcome.dlq_logs {
        let mut fields = json!({
            "queue": consumer.queue,
            "dlq": log_entry.target,
            "msg_id": log_entry.msg_id,
            "attempts": log_entry.attempts,
            "max_retries": consumer.max_retries,
        });
        if let Some(reason) = terminal_reason {
            fields["reason"] = json!(reason);
        }
        log(state, LogLevel::Warn, "queue_message_to_dlq", fields);
    }
    for log_entry in outcome.invalid_attempt_logs {
        log(
            state,
            LogLevel::Warn,
            "queue_message_invalid_attempts",
            json!({
                "queue": consumer.queue,
                "msg_id": log_entry.msg_id,
                "stream_id": log_entry.stream_id,
                "attempts": log_entry.attempts,
            }),
        );
    }
    for failure in outcome.failures {
        log(
            state,
            LogLevel::Error,
            "queue_message_transition_failed",
            json!({
                "queue": consumer.queue,
                "msg_id": failure.msg_id,
                "stream_id": failure.stream_id,
                "action": failure.action,
                "error_message": failure.message,
            }),
        );
    }
    record_queue_messages(&state.metrics, "retry", outcome.retry_count);
    record_queue_messages(&state.metrics, "dlq", outcome.dlq_count);
    Ok(())
}

pub(crate) fn build_retry_batch_plan(
    retries: Vec<(QueueMessage, i64)>,
    stream_key: &str,
    consumer: &Consumer,
    max_dlq_len: usize,
    now: i64,
) -> SchedulerResult<RetryBatchPlan> {
    let stream_key_owned = stream_key.to_string();
    let mut pipe = redis::pipe();
    let mut transitions = Vec::with_capacity(retries.len());

    for (msg, delay_secs) in retries {
        let invalid_attempt_log = invalid_attempt_log(&msg);
        match decide_retry_action(
            &msg,
            delay_secs,
            consumer.max_retries,
            consumer.dead_letter_queue.as_deref(),
            &consumer.queue,
            now,
        ) {
            RetryAction::Dlq {
                attempts,
                target,
                entry,
            } => {
                let dlq_key = queue_dlq_key(&consumer.ns, &target);
                append_transition(
                    &mut pipe,
                    &[&stream_key_owned, &dlq_key],
                    TRANSITION_DLQ,
                    &msg,
                    &entry,
                    max_dlq_len,
                    None,
                )?;
                transitions.push(QueueTransition {
                    action: TRANSITION_DLQ,
                    msg_id: msg.id.clone(),
                    stream_id: msg.stream_id.clone(),
                    delayed_key: None,
                    dlq_log: Some(DlqLog {
                        target,
                        msg_id: msg.id,
                        attempts,
                    }),
                    invalid_attempt_log,
                });
            }
            RetryAction::Delay {
                visible_at_ms,
                entry,
            } => {
                let delayed_key = queue_delayed_key(&consumer.ns, &consumer.queue);
                let member = serde_json::to_string(&entry)?;
                append_transition(
                    &mut pipe,
                    &[&stream_key_owned, &delayed_key, QUEUE_DELAYED_INDEX_KEY],
                    TRANSITION_DELAY,
                    &msg,
                    &entry,
                    max_dlq_len,
                    Some((visible_at_ms, &member)),
                )?;
                transitions.push(QueueTransition {
                    action: TRANSITION_DELAY,
                    msg_id: msg.id,
                    stream_id: msg.stream_id,
                    delayed_key: Some(delayed_key),
                    dlq_log: None,
                    invalid_attempt_log,
                });
            }
            RetryAction::Immediate { entry } => {
                append_transition(
                    &mut pipe,
                    &[&stream_key_owned, &stream_key_owned],
                    TRANSITION_IMMEDIATE,
                    &msg,
                    &entry,
                    max_dlq_len,
                    None,
                )?;
                transitions.push(QueueTransition {
                    action: TRANSITION_IMMEDIATE,
                    msg_id: msg.id,
                    stream_id: msg.stream_id,
                    delayed_key: None,
                    dlq_log: None,
                    invalid_attempt_log,
                });
            }
        }
    }

    Ok(RetryBatchPlan { pipe, transitions })
}

pub(crate) fn build_terminal_batch_plan(
    messages: Vec<QueueMessage>,
    stream_key: &str,
    consumer: &Consumer,
    max_dlq_len: usize,
    reason: &str,
) -> SchedulerResult<RetryBatchPlan> {
    let stream_key_owned = stream_key.to_string();
    let mut pipe = redis::pipe();
    let mut transitions = Vec::with_capacity(messages.len());

    for msg in messages {
        let invalid_attempt_log = invalid_attempt_log(&msg);
        let attempts = parse_next_attempts(&msg.attempts);
        let entry = HashMap::from([
            ("id".to_string(), msg.id.clone()),
            ("body_b64".to_string(), msg.body_b64.clone()),
            ("content_type".to_string(), msg.content_type.clone()),
            ("attempts".to_string(), attempts.to_string()),
            ("first_seen_ms".to_string(), msg.first_seen_ms.clone()),
            ("reason".to_string(), reason.to_string()),
        ]);
        let target = consumer
            .dead_letter_queue
            .as_deref()
            .unwrap_or(&consumer.queue)
            .to_string();
        let dlq_key = queue_dlq_key(&consumer.ns, &target);
        append_transition(
            &mut pipe,
            &[&stream_key_owned, &dlq_key],
            TRANSITION_DLQ,
            &msg,
            &entry,
            max_dlq_len,
            None,
        )?;
        transitions.push(QueueTransition {
            action: TRANSITION_DLQ,
            msg_id: msg.id.clone(),
            stream_id: msg.stream_id.clone(),
            delayed_key: None,
            dlq_log: Some(DlqLog {
                target,
                msg_id: msg.id,
                attempts,
            }),
            invalid_attempt_log,
        });
    }

    Ok(RetryBatchPlan { pipe, transitions })
}

pub(crate) fn record_queue_messages(metrics: &Metrics, outcome: &str, count: usize) {
    if count == 0 {
        return;
    }
    metrics.increment(
        "queue_messages",
        &[("service", SERVICE), ("outcome", outcome)],
        count as f64,
    );
}

pub(crate) fn record_queue_dispatch_failures(metrics: &Metrics, kind: &str, count: usize) {
    if count == 0 {
        return;
    }
    metrics.increment(
        "queue_dispatch_failures",
        &[("service", SERVICE), ("kind", kind)],
        count as f64,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::parse_packed_commands;

    fn msg(id: &str, stream_id: &str, attempts: &str) -> QueueMessage {
        QueueMessage {
            stream_id: stream_id.to_string(),
            id: id.to_string(),
            body_b64: "aGVsbG8=".to_string(),
            content_type: "text".to_string(),
            attempts: attempts.to_string(),
            first_seen_ms: "1699999999999".to_string(),
        }
    }

    fn eval_parts(command: &[String]) -> (&[String], &[String]) {
        assert_eq!(command[0], "EVAL");
        let key_count = command[2].parse::<usize>().expect("valid EVAL key count");
        (&command[3..3 + key_count], &command[3 + key_count..])
    }

    #[test]
    fn queue_message_metric_counts_messages_not_batches() {
        let metrics = Metrics::default();
        record_queue_messages(&metrics, "ack", 3);
        record_queue_messages(&metrics, "retry", 1);
        record_queue_messages(&metrics, "ack", 2);
        record_queue_messages(&metrics, "dlq", 0);

        let rendered = metrics.render_prometheus();
        assert!(
            rendered.contains("wdl_queue_messages_total{outcome=\"ack\",service=\"scheduler\"} 5")
        );
        assert!(
            rendered
                .contains("wdl_queue_messages_total{outcome=\"retry\",service=\"scheduler\"} 1")
        );
        assert!(!rendered.contains("outcome=\"dlq\""));
    }

    #[test]
    fn queue_dispatch_failure_metric_is_separate_from_message_outcomes() {
        let metrics = Metrics::default();
        record_queue_dispatch_failures(&metrics, "transport_error", 2);
        record_queue_dispatch_failures(&metrics, "handler_error", 3);
        record_queue_dispatch_failures(&metrics, "handler_error", 0);

        let rendered = metrics.render_prometheus();
        assert!(rendered.contains(
            "wdl_queue_dispatch_failures_total{kind=\"transport_error\",service=\"scheduler\"} 2"
        ));
        assert!(rendered.contains(
            "wdl_queue_dispatch_failures_total{kind=\"handler_error\",service=\"scheduler\"} 3"
        ));
        assert!(!rendered.contains("wdl_queue_messages_total"));
    }

    #[test]
    fn decide_retry_action_preserves_off_by_one_retry_contract() {
        match decide_retry_action(&msg("a", "1-0", "0"), 0, 3, None, "jobs", 0) {
            RetryAction::Immediate { entry } => {
                assert_eq!(entry.get("attempts").map(String::as_str), Some("1"));
                assert!(!entry.contains_key("reason"));
            }
            _ => panic!("expected immediate retry"),
        }
        match decide_retry_action(&msg("a", "1-0", "2"), 0, 3, None, "jobs", 0) {
            RetryAction::Immediate { entry } => {
                assert_eq!(entry.get("attempts").map(String::as_str), Some("3"));
            }
            _ => panic!("expected retry at attempts=3"),
        }
        match decide_retry_action(&msg("a", "1-0", "3"), 0, 3, None, "jobs", 0) {
            RetryAction::Dlq {
                attempts,
                target,
                entry,
            } => {
                assert_eq!(attempts, 4);
                assert_eq!(target, "jobs");
                assert_eq!(
                    entry.get("reason").map(String::as_str),
                    Some("max_retries_exceeded")
                );
            }
            _ => panic!("expected DLQ after max retries exceeded"),
        }
    }

    #[test]
    fn decide_retry_action_handles_delay_dlq_override_invalid_attempts_and_zero_max() {
        match decide_retry_action(&msg("a", "1-0", "0"), 15, 3, None, "jobs", 1_000_000) {
            RetryAction::Delay {
                visible_at_ms,
                entry,
            } => {
                assert_eq!(visible_at_ms, 1_015_000);
                assert!(!entry.contains_key("reason"));
            }
            _ => panic!("expected delayed retry"),
        }

        match decide_retry_action(&msg("a", "1-0", "99"), 0, 3, Some("failures"), "jobs", 0) {
            RetryAction::Dlq { target, entry, .. } => {
                assert_eq!(target, "failures");
                assert_eq!(entry.get("id").map(String::as_str), Some("a"));
                assert_eq!(entry.get("body_b64").map(String::as_str), Some("aGVsbG8="));
                assert_eq!(entry.get("content_type").map(String::as_str), Some("text"));
                assert_eq!(
                    entry.get("first_seen_ms").map(String::as_str),
                    Some("1699999999999")
                );
            }
            _ => panic!("expected explicit DLQ target"),
        }

        match decide_retry_action(&msg("a", "1-0", "bogus"), 0, 3, None, "jobs", 0) {
            RetryAction::Immediate { entry } => {
                assert_eq!(entry.get("attempts").map(String::as_str), Some("1"));
            }
            _ => panic!("expected invalid attempts to start at 1"),
        }

        match decide_retry_action(&msg("a", "1-0", "-1"), 0, 3, None, "jobs", 0) {
            RetryAction::Immediate { entry } => {
                assert_eq!(entry.get("attempts").map(String::as_str), Some("1"));
            }
            _ => panic!("expected negative attempts to start at 1"),
        }

        match decide_retry_action(
            &msg("a", "1-0", &i64::MAX.to_string()),
            0,
            3,
            None,
            "jobs",
            0,
        ) {
            RetryAction::Immediate { entry } => {
                assert_eq!(entry.get("attempts").map(String::as_str), Some("1"));
            }
            _ => panic!("expected overflow attempts to start at 1"),
        }

        match decide_retry_action(&msg("a", "1-0", "0"), 0, 0, None, "jobs", 0) {
            RetryAction::Dlq { attempts, .. } => assert_eq!(attempts, 1),
            _ => panic!("expected max_retries=0 to DLQ first failure"),
        }
    }

    #[test]
    fn decide_retry_action_clamps_large_delays_and_saturates_visible_time() {
        match decide_retry_action(&msg("a", "1-0", "0"), i64::MAX, 3, None, "jobs", 1_000_000) {
            RetryAction::Delay { visible_at_ms, .. } => {
                assert_eq!(
                    visible_at_ms,
                    1_000_000 + MAX_QUEUE_DELAY_SECONDS * MILLIS_PER_SECOND
                );
            }
            _ => panic!("expected oversized delay to be clamped into delayed retry"),
        }

        match decide_retry_action(&msg("a", "1-0", "0"), 5, 3, None, "jobs", i64::MAX - 1) {
            RetryAction::Delay { visible_at_ms, .. } => {
                assert_eq!(visible_at_ms, i64::MAX);
            }
            _ => panic!("expected visible_at_ms to saturate at i64::MAX"),
        }

        match decide_retry_action(&msg("a", "1-0", "0"), -1, 3, None, "jobs", 1_000_000) {
            RetryAction::Immediate { .. } => {}
            _ => panic!("expected negative delay to remain immediate retry"),
        }
    }

    #[test]
    fn retry_batch_plan_clamps_worker_controlled_retry_delays() {
        let consumer = Consumer {
            ns: "demo".to_string(),
            queue: "jobs".to_string(),
            max_batch_size: 10,
            max_batch_timeout_ms: 5000,
            max_retries: 3,
            retry_delay_secs: 0,
            dead_letter_queue: None,
            worker_id: "demo:worker:v1".to_string(),
        };
        let plan = build_retry_batch_plan(
            vec![(msg("huge", "9-0", "0"), i64::MAX)],
            "queue:demo:jobs:s",
            &consumer,
            99,
            1_000_000,
        )
        .unwrap();

        let commands = parse_packed_commands(&plan.pipe.get_packed_pipeline());
        assert_eq!(commands.len(), 1);
        let (_, args) = eval_parts(&commands[0]);
        assert_eq!(args[0], TRANSITION_DELAY);
        assert_eq!(
            args[3],
            (1_000_000 + MAX_QUEUE_DELAY_SECONDS * MILLIS_PER_SECOND).to_string()
        );
    }

    #[test]
    fn retry_batch_plan_builds_one_isolated_script_per_message() {
        let consumer = Consumer {
            ns: "demo".to_string(),
            queue: "jobs".to_string(),
            max_batch_size: 10,
            max_batch_timeout_ms: 5000,
            max_retries: 1,
            retry_delay_secs: 0,
            dead_letter_queue: Some("failures".to_string()),
            worker_id: "demo:worker:v1".to_string(),
        };
        let plan = build_retry_batch_plan(
            vec![
                (msg("immediate", "1-0", "0"), 0),
                (msg("delayed", "2-0", "0"), 7),
                (msg("delayed-2", "2-1", "0"), 9),
                (msg("dead", "3-0", "1"), 0),
            ],
            "queue:demo:jobs:s",
            &consumer,
            99,
            1_000_000,
        )
        .unwrap();

        let commands = parse_packed_commands(&plan.pipe.get_packed_pipeline());
        assert_eq!(commands.len(), 4);
        let (immediate_keys, immediate_args) = eval_parts(&commands[0]);
        assert_eq!(immediate_keys, ["queue:demo:jobs:s", "queue:demo:jobs:s"]);
        assert_eq!(immediate_args[0], TRANSITION_IMMEDIATE);
        assert_eq!(immediate_args[1], "1-0");

        let (delayed_keys, delayed_args) = eval_parts(&commands[1]);
        assert_eq!(
            delayed_keys,
            [
                "queue:demo:jobs:s",
                "queue-delayed:demo:jobs",
                QUEUE_DELAYED_INDEX_KEY
            ]
        );
        assert_eq!(delayed_args[0], TRANSITION_DELAY);
        assert_eq!(delayed_args[3], "1007000");

        let (_, second_delayed_args) = eval_parts(&commands[2]);
        assert_eq!(second_delayed_args[3], "1009000");

        let (dlq_keys, dlq_args) = eval_parts(&commands[3]);
        assert_eq!(dlq_keys, ["queue:demo:jobs:s", "queue:demo:failures:dlq"]);
        assert_eq!(dlq_args[0], TRANSITION_DLQ);
        assert_eq!(dlq_args[2], "99");
        assert_eq!(dlq_args[10], "max_retries_exceeded");

        let outcome = parse_transition_results(
            plan.transitions,
            vec![
                (1, String::new()),
                (1, String::new()),
                (1, String::new()),
                (1, String::new()),
            ],
        )
        .unwrap();
        assert_eq!(outcome.retry_count, 3);
        assert_eq!(outcome.dlq_count, 1);
        assert!(outcome.delayed_keys.contains("queue-delayed:demo:jobs"));
        assert_eq!(outcome.dlq_logs.len(), 1);
        assert_eq!(outcome.dlq_logs[0].target, "failures");
        assert_eq!(outcome.dlq_logs[0].msg_id, "dead");
        assert_eq!(outcome.dlq_logs[0].attempts, 2);
        assert!(outcome.invalid_attempt_logs.is_empty());
        assert!(outcome.failures.is_empty());
    }

    #[test]
    fn transition_script_finishes_target_writes_before_source_removal() {
        let delayed_index = QUEUE_MESSAGE_TRANSITION_SCRIPT
            .find(r#"redis.pcall("SADD", KEYS[3], KEYS[2])"#)
            .expect("delayed index write");
        let delayed_payload = QUEUE_MESSAGE_TRANSITION_SCRIPT
            .find(r#"redis.pcall("ZADD", KEYS[2], ARGV[4], ARGV[5])"#)
            .expect("delayed payload write");
        let ack = QUEUE_MESSAGE_TRANSITION_SCRIPT
            .find(r#"redis.pcall("XACK", KEYS[1], ARGV[12], ARGV[2])"#)
            .expect("source ack");
        let delete = QUEUE_MESSAGE_TRANSITION_SCRIPT
            .find(r#"redis.pcall("XDEL", KEYS[1], ARGV[2])"#)
            .expect("source delete");
        assert!(delayed_index < delayed_payload);
        assert!(delayed_payload < ack);
        assert!(ack < delete);
        assert!(QUEUE_MESSAGE_TRANSITION_SCRIPT.contains("return {0, tostring(result.err)}"));
    }

    #[test]
    fn transition_script_keeps_immediate_retries_untrimmed() {
        let (_, after_immediate) = QUEUE_MESSAGE_TRANSITION_SCRIPT
            .split_once(r#"if mode == "immediate" then"#)
            .expect("immediate retry branch");
        let (immediate_branch, _) = after_immediate
            .split_once(r#"elseif mode == "delay" then"#)
            .expect("delayed retry branch");

        assert!(immediate_branch.contains(r#""XADD", KEYS[2], "*""#));
        assert!(
            !immediate_branch.contains("MAXLEN"),
            "the main queue stream must not trim retry messages"
        );
    }

    #[test]
    fn transition_results_isolate_item_failures_and_keep_later_successes() {
        let consumer = Consumer {
            ns: "demo".to_string(),
            queue: "jobs".to_string(),
            max_batch_size: 10,
            max_batch_timeout_ms: 5000,
            max_retries: 3,
            retry_delay_secs: 0,
            dead_letter_queue: None,
            worker_id: "demo:worker:v1".to_string(),
        };
        let plan = build_retry_batch_plan(
            vec![
                (msg("first", "1-0", "0"), 0),
                (msg("bad", "2-0", "0"), 7),
                (msg("last", "3-0", "0"), 0),
            ],
            "queue:demo:jobs:s",
            &consumer,
            99,
            1_000_000,
        )
        .unwrap();

        let outcome = parse_transition_results(
            plan.transitions,
            vec![
                (1, String::new()),
                (0, "WRONGTYPE target".to_string()),
                (1, String::new()),
            ],
        )
        .unwrap();

        assert_eq!(outcome.retry_count, 2);
        assert_eq!(outcome.dlq_count, 0);
        assert!(outcome.delayed_keys.is_empty());
        assert_eq!(outcome.failures.len(), 1);
        assert_eq!(outcome.failures[0].action, TRANSITION_DELAY);
        assert_eq!(outcome.failures[0].msg_id, "bad");
        assert_eq!(outcome.failures[0].stream_id, "2-0");
    }

    #[test]
    fn retry_batch_plan_records_invalid_attempts_for_warning_logs() {
        let consumer = Consumer {
            ns: "demo".to_string(),
            queue: "jobs".to_string(),
            max_batch_size: 10,
            max_batch_timeout_ms: 5000,
            max_retries: 3,
            retry_delay_secs: 0,
            dead_letter_queue: None,
            worker_id: "demo:worker:v1".to_string(),
        };
        let plan = build_retry_batch_plan(
            vec![(msg("bad", "9-0", "not-a-number"), 0)],
            "queue:demo:jobs:s",
            &consumer,
            99,
            1_000_000,
        )
        .unwrap();

        let commands = parse_packed_commands(&plan.pipe.get_packed_pipeline());
        let (_, args) = eval_parts(&commands[0]);
        assert_eq!(args[8], "1");
        let outcome = parse_transition_results(plan.transitions, vec![(1, String::new())]).unwrap();
        assert_eq!(outcome.retry_count, 1);
        assert_eq!(outcome.dlq_count, 0);
        assert_eq!(outcome.invalid_attempt_logs.len(), 1);
        assert_eq!(outcome.invalid_attempt_logs[0].msg_id, "bad");
        assert_eq!(outcome.invalid_attempt_logs[0].stream_id, "9-0");
        assert_eq!(outcome.invalid_attempt_logs[0].attempts, "not-a-number");

        let negative_plan = build_retry_batch_plan(
            vec![(msg("negative", "10-0", "-1"), 0)],
            "queue:demo:jobs:s",
            &consumer,
            99,
            1_000_000,
        )
        .unwrap();
        let negative_outcome =
            parse_transition_results(negative_plan.transitions, vec![(1, String::new())]).unwrap();
        assert_eq!(negative_outcome.invalid_attempt_logs.len(), 1);
        assert_eq!(negative_outcome.invalid_attempt_logs[0].attempts, "-1");

        let overflow_plan = build_retry_batch_plan(
            vec![(msg("overflow", "11-0", &i64::MAX.to_string()), 0)],
            "queue:demo:jobs:s",
            &consumer,
            99,
            1_000_000,
        )
        .unwrap();
        let overflow_outcome =
            parse_transition_results(overflow_plan.transitions, vec![(1, String::new())]).unwrap();
        assert_eq!(overflow_outcome.invalid_attempt_logs.len(), 1);
        assert_eq!(
            overflow_outcome.invalid_attempt_logs[0].attempts,
            i64::MAX.to_string()
        );
    }

    #[test]
    fn terminal_batch_plan_sends_messages_directly_to_dlq_with_reason() {
        let consumer = Consumer {
            ns: "demo".to_string(),
            queue: "jobs".to_string(),
            max_batch_size: 10,
            max_batch_timeout_ms: 5000,
            max_retries: 3,
            retry_delay_secs: 0,
            dead_letter_queue: Some("failures".to_string()),
            worker_id: "demo:worker:v1".to_string(),
        };
        let plan = build_terminal_batch_plan(
            vec![(msg("bad", "9-0", "0"))],
            "queue:demo:jobs:s",
            &consumer,
            99,
            "queue_message_decode_failed",
        )
        .unwrap();

        let commands = parse_packed_commands(&plan.pipe.get_packed_pipeline());
        let (keys, args) = eval_parts(&commands[0]);
        assert_eq!(keys, ["queue:demo:jobs:s", "queue:demo:failures:dlq"]);
        assert_eq!(args[0], TRANSITION_DLQ);
        assert_eq!(args[2], "99");
        assert_eq!(args[10], "queue_message_decode_failed");
        let outcome = parse_transition_results(plan.transitions, vec![(1, String::new())]).unwrap();
        assert_eq!(outcome.retry_count, 0);
        assert_eq!(outcome.dlq_count, 1);
        assert_eq!(outcome.dlq_logs[0].target, "failures");
        assert_eq!(outcome.dlq_logs[0].msg_id, "bad");
        assert_eq!(outcome.dlq_logs[0].attempts, 1);
    }
}
