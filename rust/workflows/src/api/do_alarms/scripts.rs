use wdl_rust_common::redis_eval::StaticRedisScript;

pub(super) static SET_DO_ALARM: StaticRedisScript = StaticRedisScript::new(SET_DO_ALARM_SCRIPT);
pub(super) static DELETE_DO_ALARM: StaticRedisScript =
    StaticRedisScript::new(DELETE_DO_ALARM_SCRIPT);
pub(super) static CLAIM_DO_ALARM: StaticRedisScript = StaticRedisScript::new(CLAIM_DO_ALARM_SCRIPT);
pub(super) static FINALIZE_DO_ALARM: StaticRedisScript =
    StaticRedisScript::new(FINALIZE_DO_ALARM_SCRIPT);
pub(super) static DISCARD_CORRUPT_DO_ALARM: StaticRedisScript =
    StaticRedisScript::new(DISCARD_CORRUPT_DO_ALARM_SCRIPT);
pub(super) static RETRY_DO_ALARM: StaticRedisScript = StaticRedisScript::new(RETRY_DO_ALARM_SCRIPT);

pub(super) const SET_DO_ALARM_SCRIPT: &str = r#"
local generation = tonumber(redis.call("HGET", KEYS[1], "generation") or "0")
generation = generation + 1
redis.call("HSET", KEYS[1],
  "status", "waiting",
  "generation", generation,
  "ns", ARGV[1],
  "worker", ARGV[2],
  "scheduledVersion", ARGV[3],
  "doStorageId", ARGV[4],
  "className", ARGV[5],
  "objectName", ARGV[6],
  "dueAtMs", ARGV[7],
  "retryCount", ARGV[8],
  "rowToken", ARGV[9],
  "createdAtMs", redis.call("HGET", KEYS[1], "createdAtMs") or ARGV[10],
  "updatedAtMs", ARGV[10]
)
redis.call("HDEL", KEYS[1], "runToken", "runLeaseExpiresAtMs", "lastError")
redis.call("ZADD", KEYS[2], ARGV[7], ARGV[11])
redis.call("SREM", KEYS[3], ARGV[11])
redis.call("SADD", KEYS[4], ARGV[11])
return generation
"#;

pub(super) const DELETE_DO_ALARM_SCRIPT: &str = r#"
local row_token = redis.call("HGET", KEYS[1], "rowToken")
if not row_token then
  return 0
end
if row_token ~= ARGV[1] then
  return 0
end
redis.call("DEL", KEYS[1])
redis.call("ZREM", KEYS[2], ARGV[2])
redis.call("SREM", KEYS[3], ARGV[2])
redis.call("SREM", KEYS[4], ARGV[2])
return 1
"#;

pub(super) const CLEANUP_DO_ALARM_FOR_STORAGE_SCRIPT: &str = r#"
local storage_id = redis.call("HGET", KEYS[1], "doStorageId")
if not storage_id then
  redis.call("ZREM", KEYS[2], ARGV[1])
  redis.call("SREM", KEYS[3], ARGV[1])
  redis.call("SREM", KEYS[4], ARGV[1])
  return 2
end
if storage_id ~= ARGV[2] then
  return 0
end
redis.call("DEL", KEYS[1])
redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("SREM", KEYS[3], ARGV[1])
redis.call("SREM", KEYS[4], ARGV[1])
return 1
"#;

pub(super) const MOVE_DUE_DO_ALARM_SCRIPT: &str = r#"
local job_ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, ARGV[2])
local moved = 0
for _, job_id in ipairs(job_ids) do
  if redis.call("ZSCORE", KEYS[1], job_id) then
    redis.call("SADD", KEYS[2], job_id)
    redis.call("SADD", KEYS[3], ARGV[3])
    if redis.call("ZREM", KEYS[1], job_id) == 1 then
      moved = moved + 1
    end
  end
end
return moved
"#;

pub(super) const CLAIM_DO_ALARM_SCRIPT: &str = r#"
local now = tonumber(ARGV[2])
if redis.call("SISMEMBER", KEYS[3], ARGV[1]) ~= 1 then
  return { "keep" }
end
local status = redis.call("HGET", KEYS[1], "status")
if not status then
  local ns = redis.call("HGET", KEYS[1], "ns")
  local worker = redis.call("HGET", KEYS[1], "worker")
  redis.call("DEL", KEYS[1])
  redis.call("ZREM", KEYS[2], ARGV[1])
  redis.call("SREM", KEYS[3], ARGV[1])
  if ns and worker then
    redis.call("SREM", "wf:internal:do-alarm:by-worker:" .. ns .. ":" .. worker, ARGV[1])
  end
  return { "missing" }
end
if status == "running" then
  local lease = tonumber(redis.call("HGET", KEYS[1], "runLeaseExpiresAtMs") or "0")
  if lease > now then
    return { "keep" }
  end
elseif status ~= "waiting" then
  local ns = redis.call("HGET", KEYS[1], "ns")
  local worker = redis.call("HGET", KEYS[1], "worker")
  redis.call("DEL", KEYS[1])
  redis.call("ZREM", KEYS[2], ARGV[1])
  redis.call("SREM", KEYS[3], ARGV[1])
  if ns and worker then
    redis.call("SREM", "wf:internal:do-alarm:by-worker:" .. ns .. ":" .. worker, ARGV[1])
  end
  return { "missing" }
end
local due = tonumber(redis.call("HGET", KEYS[1], "dueAtMs") or "")
if due and due > now then
  redis.call("ZADD", KEYS[2], due, ARGV[1])
  redis.call("SREM", KEYS[3], ARGV[1])
  return { "keep" }
end
redis.call("HSET", KEYS[1],
  "status", "running",
  "runToken", ARGV[3],
  "runLeaseExpiresAtMs", ARGV[4],
  "updatedAtMs", ARGV[2]
)
return redis.call("HGETALL", KEYS[1])
"#;

pub(super) const FINALIZE_DO_ALARM_SCRIPT: &str = r#"
local run_token = redis.call("HGET", KEYS[1], "runToken")
if run_token ~= ARGV[1] then
  return 0
end
redis.call("DEL", KEYS[1])
redis.call("ZREM", KEYS[2], ARGV[2])
redis.call("SREM", KEYS[3], ARGV[2])
redis.call("SREM", KEYS[4], ARGV[2])
return 1
"#;

pub(super) const DISCARD_CORRUPT_DO_ALARM_SCRIPT: &str = r#"
local run_token = redis.call("HGET", KEYS[1], "runToken")
if not run_token or run_token ~= ARGV[1] then
  return 0
end
redis.call("DEL", KEYS[1])
redis.call("ZREM", KEYS[2], ARGV[2])
redis.call("SREM", KEYS[3], ARGV[2])
if ARGV[3] == "1" then
  redis.call("SREM", KEYS[4], ARGV[2])
end
return 1
"#;

pub(super) const RETRY_DO_ALARM_SCRIPT: &str = r#"
local run_token = redis.call("HGET", KEYS[1], "runToken")
if run_token ~= ARGV[1] then
  return 0
end
local retry_count = tonumber(redis.call("HGET", KEYS[1], "retryCount") or "0")
if retry_count >= tonumber(ARGV[5]) then
  redis.call("DEL", KEYS[1])
  redis.call("ZREM", KEYS[2], ARGV[2])
  redis.call("SREM", KEYS[3], ARGV[2])
  redis.call("SREM", KEYS[4], ARGV[2])
  return 2
end
retry_count = retry_count + 1
redis.call("HSET", KEYS[1],
  "status", "waiting",
  "retryCount", retry_count,
  "dueAtMs", ARGV[3],
  "updatedAtMs", ARGV[4],
  "lastError", ARGV[6]
)
redis.call("HDEL", KEYS[1], "runToken", "runLeaseExpiresAtMs")
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[2])
redis.call("SREM", KEYS[3], ARGV[2])
return 1
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_script_fences_row_token_and_clears_running_claim() {
        assert!(SET_DO_ALARM_SCRIPT.contains("\"rowToken\", ARGV[9]"));
        assert!(SET_DO_ALARM_SCRIPT.contains("redis.call(\"HDEL\", KEYS[1], \"runToken\""));
    }

    #[test]
    fn job_scripts_use_consistent_key_order() {
        assert!(SET_DO_ALARM_SCRIPT.contains("redis.call(\"ZADD\", KEYS[2], ARGV[7], ARGV[11])"));
        assert!(SET_DO_ALARM_SCRIPT.contains("redis.call(\"SREM\", KEYS[3], ARGV[11])"));
        assert!(DELETE_DO_ALARM_SCRIPT.contains("redis.call(\"ZREM\", KEYS[2], ARGV[2])"));
        assert!(DELETE_DO_ALARM_SCRIPT.contains("redis.call(\"SREM\", KEYS[3], ARGV[2])"));
        assert!(
            CLEANUP_DO_ALARM_FOR_STORAGE_SCRIPT.contains("redis.call(\"ZREM\", KEYS[2], ARGV[1])")
        );
        assert!(
            CLEANUP_DO_ALARM_FOR_STORAGE_SCRIPT.contains("redis.call(\"SREM\", KEYS[3], ARGV[1])")
        );
        assert!(FINALIZE_DO_ALARM_SCRIPT.contains("redis.call(\"ZREM\", KEYS[2], ARGV[2])"));
        assert!(FINALIZE_DO_ALARM_SCRIPT.contains("redis.call(\"SREM\", KEYS[3], ARGV[2])"));
        assert!(DISCARD_CORRUPT_DO_ALARM_SCRIPT.contains("redis.call(\"ZREM\", KEYS[2], ARGV[2])"));
        assert!(DISCARD_CORRUPT_DO_ALARM_SCRIPT.contains("redis.call(\"SREM\", KEYS[3], ARGV[2])"));
        assert!(RETRY_DO_ALARM_SCRIPT.contains("redis.call(\"ZADD\", KEYS[2], ARGV[3], ARGV[2])"));
        assert!(RETRY_DO_ALARM_SCRIPT.contains("redis.call(\"SREM\", KEYS[3], ARGV[2])"));
    }

    #[test]
    fn set_script_writes_due_index_before_removing_ready_hint() {
        let due_pos = SET_DO_ALARM_SCRIPT
            .find("redis.call(\"ZADD\", KEYS[2], ARGV[7], ARGV[11])")
            .expect("setAlarm must write due index");
        let ready_pos = SET_DO_ALARM_SCRIPT
            .find("redis.call(\"SREM\", KEYS[3], ARGV[11])")
            .expect("setAlarm must remove ready hint");

        assert!(due_pos < ready_pos);
    }

    #[test]
    fn move_due_script_removes_due_after_ready_hints() {
        let ready_pos = MOVE_DUE_DO_ALARM_SCRIPT
            .find("redis.call(\"SADD\", KEYS[2], job_id)")
            .expect("move-due must add ready hint");
        let active_pos = MOVE_DUE_DO_ALARM_SCRIPT
            .find("redis.call(\"SADD\", KEYS[3], ARGV[3])")
            .expect("move-due must add active shard hint");
        let remove_pos = MOVE_DUE_DO_ALARM_SCRIPT
            .find("redis.call(\"ZREM\", KEYS[1], job_id)")
            .expect("move-due must remove due entry");

        assert!(ready_pos < remove_pos);
        assert!(active_pos < remove_pos);
    }

    #[test]
    fn claim_script_repairs_future_due_index_before_removing_ready_hint() {
        let future_branch = CLAIM_DO_ALARM_SCRIPT
            .split("if due and due > now then")
            .nth(1)
            .expect("claim script should have a future-due branch");
        let repair_pos = future_branch
            .find("redis.call(\"ZADD\", KEYS[2], due, ARGV[1])")
            .expect("future waiting jobs must repair due index");
        let remove_pos = future_branch
            .find("redis.call(\"SREM\", KEYS[3], ARGV[1])")
            .expect("future waiting jobs must remove stale ready hint");

        assert!(repair_pos < remove_pos);
    }

    #[test]
    fn delete_script_is_row_token_fenced() {
        assert!(DELETE_DO_ALARM_SCRIPT.contains("row_token ~= ARGV[1]"));
        assert!(DELETE_DO_ALARM_SCRIPT.contains("redis.call(\"DEL\", KEYS[1])"));
    }

    #[test]
    fn worker_cleanup_script_is_storage_id_fenced() {
        let mismatch_pos = CLEANUP_DO_ALARM_FOR_STORAGE_SCRIPT
            .find("storage_id ~= ARGV[2]")
            .expect("cleanup must compare the stored storage id");
        let return_pos = CLEANUP_DO_ALARM_FOR_STORAGE_SCRIPT[mismatch_pos..]
            .find("return 0")
            .expect("cleanup must keep jobs from a different storage id")
            + mismatch_pos;
        let delete_pos = CLEANUP_DO_ALARM_FOR_STORAGE_SCRIPT
            .find("redis.call(\"DEL\", KEYS[1])")
            .expect("cleanup should delete matching jobs");
        assert!(return_pos < delete_pos);
        assert!(
            CLEANUP_DO_ALARM_FOR_STORAGE_SCRIPT.contains("redis.call(\"SREM\", KEYS[4], ARGV[1])")
        );
    }

    #[test]
    fn retry_script_discards_after_max_tries() {
        assert!(RETRY_DO_ALARM_SCRIPT.contains("retry_count >= tonumber(ARGV[5])"));
        assert!(RETRY_DO_ALARM_SCRIPT.contains("return 2"));
    }

    #[test]
    fn retry_script_writes_due_index_before_removing_ready_hint() {
        let retry_branch = RETRY_DO_ALARM_SCRIPT
            .split("redis.call(\"HDEL\", KEYS[1], \"runToken\", \"runLeaseExpiresAtMs\")")
            .nth(1)
            .expect("retry script should clear the running claim");
        let due_pos = retry_branch
            .find("redis.call(\"ZADD\", KEYS[2], ARGV[3], ARGV[2])")
            .expect("retry must write due index");
        let ready_pos = retry_branch
            .find("redis.call(\"SREM\", KEYS[3], ARGV[2])")
            .expect("retry must remove ready hint");

        assert!(due_pos < ready_pos);
    }

    #[test]
    fn finalize_and_corrupt_discard_scripts_are_run_token_fenced() {
        assert!(FINALIZE_DO_ALARM_SCRIPT.contains("run_token ~= ARGV[1]"));
        assert!(FINALIZE_DO_ALARM_SCRIPT.contains("redis.call(\"SREM\", KEYS[4]"));
        assert!(DISCARD_CORRUPT_DO_ALARM_SCRIPT.contains("run_token ~= ARGV[1]"));
        assert!(DISCARD_CORRUPT_DO_ALARM_SCRIPT.contains("if ARGV[3] == \"1\" then"));
    }
}
