use crate::{AppState, Redis};
use redis::AsyncCommands;
use wdl_rust_common::redis_eval::StaticRedisScript;

const INDEX_MEMBER_CHECK_CHUNK_SIZE: usize = 128;

const REMOVE_STALE_INDEX_MEMBERS_SCRIPT: &str = r#"
local results = {}
for i = 2, #ARGV do
  local member = ARGV[i]
  local type_reply = redis.call("TYPE", member)
  local actual_type = type(type_reply) == "table" and type_reply.ok or type_reply
  if actual_type == ARGV[1] then
    results[#results + 1] = 1
  else
    redis.call("SREM", KEYS[1], member)
    results[#results + 1] = 0
  end
end
return results
"#;

static REMOVE_STALE_INDEX_MEMBERS: StaticRedisScript =
    StaticRedisScript::new(REMOVE_STALE_INDEX_MEMBERS_SCRIPT);

async fn scan_typed_keys_on(
    redis: &Redis,
    pattern: &str,
    key_type: &str,
) -> Result<Vec<String>, redis::RedisError> {
    let mut cursor = 0_u64;
    let mut keys = Vec::new();
    loop {
        let pattern = pattern.to_string();
        let key_type = key_type.to_string();
        let (next, batch): (u64, Vec<String>) = redis
            .with_conn(async |mut conn| {
                scan_index_page(cursor, &pattern, &key_type)
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        keys.extend(batch);
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    Ok(keys)
}

async fn indexed_existing_keys_on(
    redis: &Redis,
    index_key: &str,
    expected_type: &str,
) -> Result<Vec<String>, redis::RedisError> {
    let indexed: Vec<String> = redis
        .with_conn(async |mut conn| {
            let index_key = index_key.to_string();
            conn.smembers(index_key).await
        })
        .await?;
    index_members_plan(redis, index_key, expected_type, indexed)
        .await
        .map(|plan| plan.existing)
}

fn scan_index_page(cursor: u64, pattern: &str, key_type: &str) -> redis::Cmd {
    let mut command = redis::cmd("SCAN");
    command
        .arg(cursor)
        .arg("MATCH")
        .arg(pattern)
        .arg("COUNT")
        .arg(200)
        .arg("TYPE")
        .arg(key_type);
    command
}

async fn repair_index_on(
    redis: &Redis,
    index_key: &str,
    pattern: &str,
    key_type: &str,
) -> Result<(), redis::RedisError> {
    let index_key = index_key.to_string();
    let pattern = pattern.to_string();
    let key_type = key_type.to_string();
    redis
        .with_conn(async |mut conn| {
            let mut cursor = 0_u64;
            loop {
                let (next, batch): (u64, Vec<String>) =
                    scan_index_page(cursor, &pattern, &key_type)
                        .query_async(&mut conn)
                        .await?;
                if !batch.is_empty() {
                    redis::cmd("SADD")
                        .arg(&index_key)
                        .arg(batch)
                        .query_async::<()>(&mut conn)
                        .await?;
                }
                cursor = next;
                if cursor == 0 {
                    break;
                }
            }
            Ok(())
        })
        .await
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct IndexedMemberPlan {
    pub(crate) existing: Vec<String>,
    pub(crate) stale: Vec<String>,
}

pub(crate) fn classify_index_members(members: Vec<String>, checks: Vec<i64>) -> IndexedMemberPlan {
    let mut existing = Vec::new();
    let mut stale = Vec::new();
    for (member, exists) in members.into_iter().zip(checks) {
        if exists > 0 {
            existing.push(member);
        } else {
            stale.push(member);
        }
    }
    IndexedMemberPlan { existing, stale }
}

async fn index_members_plan(
    redis: &Redis,
    index_key: &str,
    expected_type: &str,
    members: Vec<String>,
) -> Result<IndexedMemberPlan, redis::RedisError> {
    if members.is_empty() {
        return Ok(IndexedMemberPlan {
            existing: Vec::new(),
            stale: Vec::new(),
        });
    }
    let mut checks = Vec::with_capacity(members.len());
    for chunk in members.chunks(INDEX_MEMBER_CHECK_CHUNK_SIZE) {
        let chunk = chunk.to_vec();
        let chunk_checks: Vec<i64> = redis
            .with_conn(async |mut conn| {
                let mut args = Vec::with_capacity(chunk.len() + 1);
                args.push(expected_type);
                args.extend(chunk.iter().map(String::as_str));
                REMOVE_STALE_INDEX_MEMBERS
                    .prepare_invoke(&[index_key], &args)
                    .invoke_async(&mut conn)
                    .await
            })
            .await?;
        checks.extend(chunk_checks);
    }
    let plan = classify_index_members(members, checks);
    Ok(plan)
}

pub(crate) async fn indexed_existing_keys(
    state: &AppState,
    index_key: &str,
    expected_type: &str,
) -> Result<Vec<String>, redis::RedisError> {
    indexed_existing_keys_on(&state.redis, index_key, expected_type).await
}

pub(crate) async fn indexed_keys_after_backfill(
    state: &AppState,
    index_key: &str,
    backfilled_key: &str,
    pattern: &str,
    expected_type: &str,
) -> Result<Vec<String>, redis::RedisError> {
    let backfilled: i64 = state
        .redis
        .with_conn(async |mut conn| {
            let backfilled_key = backfilled_key.to_string();
            conn.exists(backfilled_key).await
        })
        .await?;
    if backfilled > 0 {
        // Once the one-time backfill has crossed pre-index data, writers own
        // this projection. Empty means no discovered keys; do not fall back to
        // keyspace SCAN on every reconcile tick.
        return indexed_existing_keys_on(&state.redis, index_key, expected_type).await;
    }

    let scanned = scan_typed_keys_on(&state.redis, pattern, expected_type).await?;
    state
        .redis
        .with_conn(async |mut conn| {
            let mut pipe = redis::pipe();
            if !scanned.is_empty() {
                pipe.cmd("SADD").arg(index_key).arg(scanned.clone());
            }
            pipe.cmd("SET").arg(backfilled_key).arg("1");
            pipe.query_async::<()>(&mut conn).await
        })
        .await?;
    Ok(scanned)
}

pub(crate) async fn indexed_existing_data_keys(
    state: &AppState,
    index_key: &str,
    expected_type: &str,
) -> Result<Vec<String>, redis::RedisError> {
    indexed_existing_keys_on(&state.data_redis, index_key, expected_type).await
}

pub(crate) async fn repair_index(
    state: &AppState,
    index_key: &str,
    pattern: &str,
    key_type: &str,
) -> Result<(), redis::RedisError> {
    repair_index_on(&state.redis, index_key, pattern, key_type).await
}

pub(crate) async fn repair_data_index(
    state: &AppState,
    index_key: &str,
    pattern: &str,
    key_type: &str,
) -> Result<(), redis::RedisError> {
    repair_index_on(&state.data_redis, index_key, pattern, key_type).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::parse_packed_commands;

    #[test]
    fn index_scan_filters_to_the_authoritative_key_type() {
        let command = scan_index_page(7, "queue:*:*:s", "stream");
        let commands = parse_packed_commands(&command.get_packed_command());

        assert_eq!(
            commands,
            vec![vec![
                "SCAN",
                "7",
                "MATCH",
                "queue:*:*:s",
                "COUNT",
                "200",
                "TYPE",
                "stream",
            ]]
        );
    }

    #[test]
    fn stale_index_cleanup_rechecks_type_at_the_remove_point() {
        let type_check = REMOVE_STALE_INDEX_MEMBERS_SCRIPT
            .find(r#"redis.call("TYPE", member)"#)
            .expect("cleanup must recheck the referenced key type");
        let removal = REMOVE_STALE_INDEX_MEMBERS_SCRIPT
            .find(r#"redis.call("SREM", KEYS[1], member)"#)
            .expect("cleanup must remove only the checked index member");
        assert!(type_check < removal);
        assert!(REMOVE_STALE_INDEX_MEMBERS_SCRIPT.contains("actual_type == ARGV[1]"));
    }
}
