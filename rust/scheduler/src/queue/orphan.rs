use redis::streams::{StreamAutoClaimReply, StreamRangeReply};
use serde_json::json;
use wdl_rust_common::redis_eval::{StaticRedisScript, append_eval_cmd};

use crate::{
    AppState, CONSUMER_GROUP, LogLevel, SchedulerResult, log, now_ms, redis_fields_with_error,
    scheduler_fields_with_error,
};

use super::{
    Consumer, MAX_BATCH_SIZE_CAP, StreamEntry, dispatch_messages, entries_to_messages,
    parse_stream_key, queue_orphaned_key, redis_error_is_nogroup, resolve_consumer,
    stream_id_to_entry,
};

const CLEANUP_EMPTY_ORPHANED_STREAM_SCRIPT: &str = r#"
if redis.call("XLEN", KEYS[1]) ~= 0 then
  return 0
end
local destroy = redis.pcall("XGROUP", "DESTROY", KEYS[1], ARGV[1])
if type(destroy) == "table" and destroy.err and not string.find(destroy.err, "NOGROUP") then
  return redis.error_reply(destroy.err)
end
redis.call("DEL", KEYS[1])
return 1
"#;

static CLEANUP_EMPTY_ORPHANED_STREAM: StaticRedisScript =
    StaticRedisScript::new(CLEANUP_EMPTY_ORPHANED_STREAM_SCRIPT);

const MOVE_PEL_TO_ORPHANED_SCRIPT: &str = r#"
local entry = redis.call("XRANGE", KEYS[1], ARGV[1], ARGV[1])
if #entry == 0 then
  redis.call("XACK", KEYS[1], ARGV[3], ARGV[1])
  return 0
end
local fields = entry[1][2]
local args = { "XADD", KEYS[2] }
if ARGV[2] ~= "" then
  table.insert(args, "MAXLEN")
  table.insert(args, "~")
  table.insert(args, ARGV[2])
end
table.insert(args, "*")
for i = 1, #fields do
  table.insert(args, fields[i])
end
table.insert(args, "reason")
table.insert(args, "consumer-removed")
table.insert(args, "source")
table.insert(args, "pel")
redis.call(unpack(args))
redis.call("XACK", KEYS[1], ARGV[3], ARGV[1])
redis.call("XDEL", KEYS[1], ARGV[1])
return 1
"#;

const MOVE_STREAM_TAIL_TO_ORPHANED_SCRIPT: &str = r#"
local entry = redis.call("XRANGE", KEYS[1], ARGV[1], ARGV[1])
if #entry == 0 then
  return 0
end
local pending = redis.pcall("XPENDING", KEYS[1], ARGV[3], ARGV[1], ARGV[1], 1)
if pending.err then
  if string.find(pending.err, "NOGROUP") then
    pending = {}
  else
    return redis.error_reply(pending.err)
  end
end
if #pending ~= 0 then
  return 0
end
local fields = entry[1][2]
local args = { "XADD", KEYS[2] }
if ARGV[2] ~= "" then
  table.insert(args, "MAXLEN")
  table.insert(args, "~")
  table.insert(args, ARGV[2])
end
table.insert(args, "*")
for i = 1, #fields do
  table.insert(args, fields[i])
end
table.insert(args, "reason")
table.insert(args, "consumer-removed")
table.insert(args, "source")
table.insert(args, "stream-tail")
redis.call(unpack(args))
redis.call("XDEL", KEYS[1], ARGV[1])
return 1
"#;

pub(crate) async fn xrange_count(
    state: &AppState,
    stream_key: &str,
    start: &str,
    end: &str,
    count: usize,
) -> Result<Vec<StreamEntry>, redis::RedisError> {
    let reply: StreamRangeReply = state
        .data_redis
        .with_conn(async |mut conn| {
            let stream_key = stream_key.to_string();
            let start = start.to_string();
            let end = end.to_string();
            redis::cmd("XRANGE")
                .arg(stream_key)
                .arg(start)
                .arg(end)
                .arg("COUNT")
                .arg(count)
                .query_async(&mut conn)
                .await
        })
        .await?;
    Ok(reply.ids.into_iter().map(stream_id_to_entry).collect())
}

pub(crate) async fn move_to_orphaned(
    state: &AppState,
    stream_key: &str,
    ns: &str,
    queue: &str,
    pel_entries: Vec<StreamEntry>,
) -> SchedulerResult<()> {
    let orphaned_key = queue_orphaned_key(ns, queue);
    let tail = xrange_count(
        state,
        stream_key,
        "-",
        "+",
        state.config.queue_sweep_batch_size,
    )
    .await?;
    let pel_count = move_pel_to_orphaned(state, stream_key, &orphaned_key, pel_entries).await?;
    let tail_count = move_stream_tail_to_orphaned(state, stream_key, &orphaned_key, tail).await?;
    log(
        state,
        LogLevel::Info,
        "queue_messages_orphaned",
        json!({ "ns": ns, "queue": queue, "count": pel_count }),
    );
    if tail_count > 0 {
        log(
            state,
            LogLevel::Info,
            "queue_stream_tail_orphaned",
            json!({ "ns": ns, "queue": queue, "count": tail_count }),
        );
    }

    let cleanup_result = cleanup_empty_orphaned_stream(state, stream_key).await;
    let cleaned = match cleanup_result {
        Ok(cleaned) => cleaned,
        Err(err) => {
            log(
                state,
                LogLevel::Error,
                "queue_orphaned_cleanup_failed",
                redis_fields_with_error(json!({ "ns": ns, "queue": queue }), &err),
            );
            false
        }
    };
    if cleaned {
        state.queues.known_streams.write().await.remove(stream_key);
    }
    Ok(())
}

async fn cleanup_empty_orphaned_stream(
    state: &AppState,
    stream_key: &str,
) -> Result<bool, redis::RedisError> {
    let cleaned: i64 = state
        .data_redis
        .with_conn(async |mut conn| {
            let stream_key = stream_key.to_string();
            CLEANUP_EMPTY_ORPHANED_STREAM
                .prepare_invoke(&[stream_key.as_str()], &[CONSUMER_GROUP])
                .invoke_async(&mut conn)
                .await
        })
        .await?;
    Ok(cleaned == 1)
}

async fn move_pel_to_orphaned(
    state: &AppState,
    stream_key: &str,
    orphaned_key: &str,
    pel_entries: Vec<StreamEntry>,
) -> SchedulerResult<usize> {
    if pel_entries.is_empty() {
        return Ok(0);
    }
    let moved: Vec<i64> = state
        .data_redis
        .with_conn(async |mut conn| {
            let mut pipe = redis::pipe();
            let trim_arg = state.config.max_orphaned_len.to_string();
            for entry in pel_entries {
                append_eval_cmd(
                    &mut pipe,
                    MOVE_PEL_TO_ORPHANED_SCRIPT,
                    &[stream_key, orphaned_key],
                    &[entry.id.as_str(), trim_arg.as_str(), CONSUMER_GROUP],
                );
            }
            pipe.query_async(&mut conn).await
        })
        .await?;
    Ok(moved.into_iter().filter(|count| *count == 1).count())
}

async fn move_stream_tail_to_orphaned(
    state: &AppState,
    stream_key: &str,
    orphaned_key: &str,
    tail: Vec<StreamEntry>,
) -> SchedulerResult<usize> {
    if tail.is_empty() {
        return Ok(0);
    }
    let moved: Vec<i64> = state
        .data_redis
        .with_conn(async |mut conn| {
            let mut pipe = redis::pipe();
            let trim_arg = state.config.max_orphaned_len.to_string();
            for entry in tail {
                append_eval_cmd(
                    &mut pipe,
                    MOVE_STREAM_TAIL_TO_ORPHANED_SCRIPT,
                    &[stream_key, orphaned_key],
                    &[entry.id.as_str(), trim_arg.as_str(), CONSUMER_GROUP],
                );
            }
            pipe.query_async(&mut conn).await
        })
        .await?;
    Ok(moved.into_iter().filter(|count| *count == 1).count())
}

pub(crate) async fn queue_pel_reap(state: AppState) -> SchedulerResult<()> {
    let stream_keys = state
        .queues
        .known_streams
        .read()
        .await
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    let mut tasks = Vec::new();
    for stream_key in stream_keys {
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
            if let Some((ns, queue)) = parse_stream_key(&stream_key)
                && let Err(err) =
                    queue_pel_reap_one(child.clone(), stream_key, ns.clone(), queue.clone()).await
            {
                log(
                    &child,
                    LogLevel::Error,
                    "queue_pel_reap_failed",
                    scheduler_fields_with_error(
                        json!({
                            "ns": ns,
                            "queue": queue,
                        }),
                        &err,
                    ),
                );
            }
        }));
    }
    for task in tasks {
        if let Err(err) = task.await {
            log(
                &state,
                LogLevel::Error,
                "queue_pel_reap_failed",
                crate::error_fields(
                    if err.is_panic() { "Panic" } else { "JoinError" },
                    err.to_string(),
                ),
            );
        }
    }
    Ok(())
}

pub(crate) async fn queue_pel_reap_one(
    state: AppState,
    stream_key: String,
    ns: String,
    queue: String,
) -> SchedulerResult<()> {
    let consumer = resolve_consumer(&state, &stream_key, &ns, &queue).await?;
    let instance_id = state.instance_id.clone();
    let queue_pel_idle_ms =
        queue_pel_reap_idle_ms(state.config.queue_pel_idle_ms, consumer.is_some());
    let claim_count = queue_pel_claim_count(consumer.as_ref());
    let result: Result<StreamAutoClaimReply, redis::RedisError> = state
        .data_redis
        .with_conn(async |mut conn| {
            let stream_key = stream_key.clone();
            redis::cmd("XAUTOCLAIM")
                .arg(stream_key)
                .arg(CONSUMER_GROUP)
                .arg(instance_id)
                .arg(queue_pel_idle_ms)
                .arg("0")
                .arg("COUNT")
                .arg(claim_count)
                .query_async(&mut conn)
                .await
        })
        .await;
    let claimed = match result {
        Ok(reply) => reply
            .claimed
            .into_iter()
            .map(stream_id_to_entry)
            .collect::<Vec<_>>(),
        Err(err) if redis_error_is_nogroup(&err) => {
            if consumer.is_none() {
                move_to_orphaned(&state, &stream_key, &ns, &queue, Vec::new()).await?;
            }
            return Ok(());
        }
        Err(err) => {
            log(
                &state,
                LogLevel::Error,
                "queue_pel_xautoclaim_failed",
                redis_fields_with_error(json!({ "ns": ns, "queue": queue }), &err),
            );
            return Ok(());
        }
    };

    let Some(consumer) = consumer else {
        move_to_orphaned(&state, &stream_key, &ns, &queue, claimed).await?;
        return Ok(());
    };
    if claimed.is_empty() {
        return Ok(());
    }
    let messages = entries_to_messages(claimed, now_ms());
    log(
        &state,
        LogLevel::Info,
        "queue_pel_redelivered",
        json!({ "ns": ns, "queue": queue, "count": messages.len() }),
    );
    dispatch_messages(&state, messages, &stream_key, &consumer, "pel").await
}

fn queue_pel_reap_idle_ms(configured_idle_ms: u64, consumer_present: bool) -> u64 {
    if consumer_present {
        configured_idle_ms
    } else {
        0
    }
}

fn queue_pel_claim_count(consumer: Option<&Consumer>) -> usize {
    consumer
        .map(|consumer| consumer.max_batch_size)
        .unwrap_or(MAX_BATCH_SIZE_CAP)
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn missing_consumer_pel_reap_claims_pending_entries_without_idle_delay() {
        assert_eq!(queue_pel_reap_idle_ms(65_000, false), 0);
        assert_eq!(queue_pel_reap_idle_ms(65_000, true), 65_000);
    }

    #[test]
    fn pel_reap_claim_count_matches_consumer_batch_cap_when_consumer_exists() {
        assert_eq!(queue_pel_claim_count(Some(&consumer(2))), 2);
        assert_eq!(queue_pel_claim_count(None), MAX_BATCH_SIZE_CAP);
    }

    #[test]
    fn cleanup_empty_orphaned_stream_script_checks_and_deletes_atomically() {
        let xlen_pos = CLEANUP_EMPTY_ORPHANED_STREAM_SCRIPT
            .find("XLEN")
            .expect("script checks source stream length");
        let xgroup_pos = CLEANUP_EMPTY_ORPHANED_STREAM_SCRIPT
            .find("XGROUP")
            .expect("script destroys consumer group only after empty check");
        let del_pos = CLEANUP_EMPTY_ORPHANED_STREAM_SCRIPT
            .find("DEL")
            .expect("script deletes source stream only after empty check");
        assert!(xlen_pos < xgroup_pos);
        assert!(xgroup_pos < del_pos);
        assert!(
            CLEANUP_EMPTY_ORPHANED_STREAM_SCRIPT.contains("return 0"),
            "script must leave non-empty streams untouched"
        );
        assert!(
            CLEANUP_EMPTY_ORPHANED_STREAM_SCRIPT.contains(r#"type(destroy) == "table""#),
            "redis.pcall success returns a scalar for XGROUP DESTROY, so .err must be guarded"
        );
    }

    #[test]
    fn pel_orphan_script_reads_entry_before_xadd_and_xack_xdel() {
        let xrange_pos = MOVE_PEL_TO_ORPHANED_SCRIPT
            .find("XRANGE")
            .expect("script re-reads source entry");
        let xadd_pos = MOVE_PEL_TO_ORPHANED_SCRIPT
            .find("XADD")
            .expect("script writes orphan stream");
        let xack_pos = MOVE_PEL_TO_ORPHANED_SCRIPT[xadd_pos..]
            .find("XACK")
            .expect("script clears PEL entry");
        let xack_pos = xack_pos + xadd_pos;
        let xdel_pos = MOVE_PEL_TO_ORPHANED_SCRIPT[xack_pos..]
            .find("XDEL")
            .expect("script deletes source entry");
        let xdel_pos = xdel_pos + xack_pos;
        assert!(xrange_pos < xadd_pos);
        assert!(xadd_pos < xack_pos);
        assert!(xack_pos < xdel_pos);
    }

    #[test]
    fn tail_orphan_script_reads_entry_before_xadd_and_xdel() {
        let xrange_pos = MOVE_STREAM_TAIL_TO_ORPHANED_SCRIPT
            .find("XRANGE")
            .expect("script re-reads source entry");
        let xpending_pos = MOVE_STREAM_TAIL_TO_ORPHANED_SCRIPT
            .find("XPENDING")
            .expect("script checks pending ownership before orphaning tail entry");
        assert!(
            MOVE_STREAM_TAIL_TO_ORPHANED_SCRIPT.contains("NOGROUP"),
            "script treats missing consumer group as unread tail cleanup"
        );
        let xadd_pos = MOVE_STREAM_TAIL_TO_ORPHANED_SCRIPT
            .find("XADD")
            .expect("script writes orphan stream");
        let xdel_pos = MOVE_STREAM_TAIL_TO_ORPHANED_SCRIPT
            .find("XDEL")
            .expect("script deletes source entry");
        assert!(xrange_pos < xadd_pos);
        assert!(xpending_pos < xadd_pos);
        assert!(xadd_pos < xdel_pos);
    }
}
