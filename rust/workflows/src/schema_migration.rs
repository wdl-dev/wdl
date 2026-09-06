use std::error::Error;
use std::fmt;

use serde::Serialize;
use wdl_rust_common::{
    redis_conn::{redis_client_from_url, redis_client_from_url_with_db},
    time::{now_ms, random_hex_64},
};

use crate::{
    DO_ALARM_KEY_PREFIX, Redis, WORKFLOWS_ARCHIVE_REDIS_DB, WORKFLOWS_REDIS_DB,
    WORKFLOWS_SCHEMA_VERSION, WorkflowError, WorkflowResult, is_do_alarm_key,
    read_schema2_instance, schema_version_key, schema3_migration_key,
    validated_workflows_redis_urls,
};

const LEGACY_SCHEMA_VERSION: &str = "2";
const SCAN_COUNT: usize = 100;
const COPY_PROBE_KEY: &str = "wf:__schema3-migrate-copy-probe__";
const MIGRATION_IN_PROGRESS_PREFIX: &str = "in_progress:";
const MIGRATION_COMPLETE: &str = "complete";

#[derive(Clone, Copy, Debug)]
pub enum Schema3MigrationMode {
    Check,
    Apply,
    Resume,
}

impl Schema3MigrationMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Check => "check",
            Self::Apply => "apply",
            Self::Resume => "resume",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MigrationPhase {
    Schema2Active,
    Copying,
    Schema3Verified,
    Schema3Current,
}

impl MigrationPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Schema2Active => "schema2_active",
            Self::Copying => "copying",
            Self::Schema3Verified => "schema3_verified",
            Self::Schema3Current => "schema3_current",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum MigrationState {
    None,
    InProgress(String),
    Complete,
}

impl MigrationState {
    fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::InProgress(_) => "in_progress",
            Self::Complete => "complete",
        }
    }
}

#[derive(Debug)]
struct Schema3MigrationError(String);

impl fmt::Display for Schema3MigrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for Schema3MigrationError {}

type MigrationResult<T> = Result<T, Schema3MigrationError>;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct MemorySnapshot {
    used_memory_bytes: Option<u64>,
    max_memory_bytes: Option<u64>,
    free_memory_bytes: Option<u64>,
    max_memory_policy: Option<String>,
}

#[derive(Clone, Copy, Debug)]
struct SourceStats {
    alarm_keys_scanned: u64,
    instances_scanned: u64,
    steps_converted: u64,
    estimated_copy_bytes: Option<u64>,
}

impl Default for SourceStats {
    fn default() -> Self {
        Self {
            alarm_keys_scanned: 0,
            instances_scanned: 0,
            steps_converted: 0,
            estimated_copy_bytes: Some(0),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Schema3MigrationReport {
    ok: bool,
    command: &'static str,
    mode: &'static str,
    phase: &'static str,
    migration_state: &'static str,
    active_db: i64,
    archive_db: i64,
    archive_key_count: u64,
    alarm_key_count: Option<u64>,
    instance_count: Option<u64>,
    converted_step_count: Option<u64>,
    estimated_copy_bytes: Option<u64>,
    archive_deleted: bool,
    memory_before: MemorySnapshot,
    memory: MemorySnapshot,
    warnings: Vec<&'static str>,
}

fn migration_error(message: impl Into<String>) -> Schema3MigrationError {
    Schema3MigrationError(message.into())
}

fn redis_error(context: &'static str, error: redis::RedisError) -> Schema3MigrationError {
    let code = error.code().unwrap_or("redis_error");
    migration_error(format!("{context} ({code})"))
}

async fn db_size(redis: &Redis, context: &'static str) -> MigrationResult<u64> {
    redis
        .with_conn(async |mut conn| redis::cmd("DBSIZE").query_async(&mut conn).await)
        .await
        .map_err(|err| redis_error(context, err))
}

async fn schema_marker(redis: &Redis, context: &'static str) -> MigrationResult<Option<String>> {
    redis
        .with_conn(async |mut conn| {
            redis::cmd("GET")
                .arg(schema_version_key())
                .query_async(&mut conn)
                .await
        })
        .await
        .map_err(|err| redis_error(context, err))
}

fn classify_phase(
    active_marker: Option<&str>,
    active_size: u64,
    archive_marker: Option<&str>,
    archive_size: u64,
) -> MigrationResult<MigrationPhase> {
    match (active_marker, active_size, archive_marker, archive_size) {
        (Some(LEGACY_SCHEMA_VERSION), _, None, 0) => Ok(MigrationPhase::Schema2Active),
        (None, _, Some(LEGACY_SCHEMA_VERSION), _) => Ok(MigrationPhase::Copying),
        (Some(WORKFLOWS_SCHEMA_VERSION), _, Some(LEGACY_SCHEMA_VERSION), _) => {
            Ok(MigrationPhase::Schema3Verified)
        }
        (Some(WORKFLOWS_SCHEMA_VERSION), _, None, 0) => Ok(MigrationPhase::Schema3Current),
        _ => Err(migration_error(
            "Workflows schema3 migration database state is invalid",
        )),
    }
}

async fn inspect_phase(active: &Redis, archive: &Redis) -> MigrationResult<MigrationPhase> {
    let (active_marker, active_size, archive_marker, archive_size) = tokio::try_join!(
        schema_marker(active, "Unable to read the active Workflows schema marker"),
        db_size(active, "Unable to read active Workflows DB size"),
        schema_marker(
            archive,
            "Unable to read the archive Workflows schema marker"
        ),
        db_size(archive, "Unable to read archive Workflows DB size"),
    )?;
    classify_phase(
        active_marker.as_deref(),
        active_size,
        archive_marker.as_deref(),
        archive_size,
    )
}

fn parse_migration_state(raw: Option<&str>) -> MigrationResult<MigrationState> {
    match raw {
        None => Ok(MigrationState::None),
        Some(MIGRATION_COMPLETE) => Ok(MigrationState::Complete),
        Some(value) if value.starts_with(MIGRATION_IN_PROGRESS_PREFIX) => {
            let token = &value[MIGRATION_IN_PROGRESS_PREFIX.len()..];
            if token.len() == 16 && token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                Ok(MigrationState::InProgress(value.to_string()))
            } else {
                Err(migration_error(
                    "Workflows schema3 migration token is corrupt",
                ))
            }
        }
        Some(_) => Err(migration_error(
            "Workflows schema3 migration state is corrupt",
        )),
    }
}

async fn migration_state(redis: &Redis) -> MigrationResult<MigrationState> {
    let raw: Option<String> = redis
        .with_conn(async |mut conn| {
            redis::cmd("GET")
                .arg(schema3_migration_key())
                .query_async(&mut conn)
                .await
        })
        .await
        .map_err(|err| redis_error("Unable to read Workflows schema3 migration state", err))?;
    parse_migration_state(raw.as_deref())
}

pub(crate) async fn ensure_schema_migration_complete(redis: &Redis) -> WorkflowResult<()> {
    let raw: Option<String> = redis
        .with_conn(async |mut conn| {
            redis::cmd("GET")
                .arg(schema3_migration_key())
                .query_async(&mut conn)
                .await
        })
        .await?;
    validate_startup_state(raw.as_deref())
}

fn validate_startup_state(raw: Option<&str>) -> WorkflowResult<()> {
    match parse_migration_state(raw) {
        Ok(MigrationState::None | MigrationState::Complete) => Ok(()),
        Ok(MigrationState::InProgress(_)) => Err(WorkflowError::schema_mismatch(
            "Workflows schema3 migration is incomplete; resume the operator task before starting Workflows",
        )),
        Err(_) => Err(WorkflowError::schema_mismatch(
            "Workflows schema3 migration state is corrupt",
        )),
    }
}

fn validate_state_phase(state: &MigrationState, phase: MigrationPhase) -> MigrationResult<()> {
    let valid = matches!(
        (state, phase),
        (MigrationState::None, MigrationPhase::Schema2Active)
            | (MigrationState::InProgress(_), MigrationPhase::Schema2Active)
            | (MigrationState::InProgress(_), MigrationPhase::Copying)
            | (
                MigrationState::InProgress(_),
                MigrationPhase::Schema3Verified
            )
            | (MigrationState::Complete, MigrationPhase::Schema3Verified)
            | (
                MigrationState::None | MigrationState::Complete,
                MigrationPhase::Schema3Current
            )
    );
    if valid {
        Ok(())
    } else {
        Err(migration_error(
            "Workflows schema3 migration coordination and database states do not match",
        ))
    }
}

async fn scan_page(
    redis: &Redis,
    cursor: u64,
    pattern: Option<&str>,
    context: &'static str,
) -> MigrationResult<(u64, Vec<String>)> {
    redis
        .with_conn(async move |mut conn| {
            let mut command = redis::cmd("SCAN");
            command.arg(cursor);
            if let Some(pattern) = pattern {
                command.arg("MATCH").arg(pattern);
            }
            command
                .arg("COUNT")
                .arg(SCAN_COUNT)
                .query_async(&mut conn)
                .await
        })
        .await
        .map_err(|err| redis_error(context, err))
}

fn is_do_alarm_state_key(key: &str) -> bool {
    key.starts_with("wf:internal:do-alarm:{") && key.ends_with("}:state")
}

fn schema2_key_type(key: &str) -> Option<&'static str> {
    if key == schema_version_key() || key == "wf:ready:cursor" {
        return Some("string");
    }
    if key.starts_with("wf:instance:") {
        return match key.rsplit_once(':')?.1 {
            "state" | "payloads" | "steps" | "step-summaries" | "events" => Some("hash"),
            "step-summary-index" | "events-by-type" => Some("zset"),
            _ => None,
        };
    }
    if let Some(alarm) = key.strip_prefix(DO_ALARM_KEY_PREFIX) {
        return if is_do_alarm_state_key(key) {
            Some("hash")
        } else if alarm == "ready:cursor" {
            Some("string")
        } else if alarm.starts_with("ready:") || alarm.starts_with("by-worker:") {
            Some("set")
        } else if alarm.starts_with("due:") {
            Some("zset")
        } else {
            None
        };
    }
    if ["wf:ready:", "wf:by-worker:", "wf:by-version:"]
        .iter()
        .any(|prefix| key.starts_with(prefix))
    {
        Some("set")
    } else if key == "wf:retention"
        || ["wf:due:", "wf:by-workflow:", "wf:pending-version:"]
            .iter()
            .any(|prefix| key.starts_with(prefix))
    {
        Some("zset")
    } else {
        None
    }
}

async fn validate_archive_source(redis: &Redis, control: &Redis) -> MigrationResult<SourceStats> {
    let mut cursor = 0;
    let mut ttl_keys = 0usize;
    let mut latest_running_alarm_lease = None;
    let mut stats = SourceStats::default();
    loop {
        let (next, keys) = scan_page(
            redis,
            cursor,
            None,
            "Unable to scan the schema-2 Workflows database",
        )
        .await?;
        if !keys.is_empty() {
            if keys.iter().any(|key| schema2_key_type(key).is_none()) {
                return Err(migration_error(
                    "Workflows DB 2 contains keys outside the schema-2 owner set and is not dedicated",
                ));
            }
            let key_info: Vec<(String, i64)> = redis
                .with_conn({
                    let keys = keys.clone();
                    async move |mut conn| {
                        let mut pipe = redis::pipe();
                        for key in keys {
                            pipe.cmd("TYPE").arg(&key);
                            pipe.cmd("PTTL").arg(key);
                        }
                        pipe.query_async(&mut conn).await
                    }
                })
                .await
                .map_err(|err| redis_error("Unable to inspect schema-2 key types and TTLs", err))?;
            if key_info.len() != keys.len() {
                return Err(migration_error(
                    "Schema-2 key inspection returned an invalid reply",
                ));
            }
            for (key, (actual_type, ttl)) in keys.iter().zip(key_info) {
                if Some(actual_type.as_str()) != schema2_key_type(key) {
                    return Err(migration_error(format!(
                        "Schema-2 key {key} has invalid Redis type {actual_type}"
                    )));
                }
                ttl_keys += usize::from(ttl >= 0);
            }

            let copy_keys = keys
                .iter()
                .filter(|key| key.as_str() != schema_version_key())
                .cloned()
                .collect::<Vec<_>>();
            if !copy_keys.is_empty() {
                let usage: Result<Vec<Option<u64>>, redis::RedisError> = redis
                    .with_conn({
                        let copy_keys = copy_keys.clone();
                        async move |mut conn| {
                            let mut pipe = redis::pipe();
                            for key in copy_keys {
                                pipe.cmd("MEMORY").arg("USAGE").arg(key);
                            }
                            pipe.query_async(&mut conn).await
                        }
                    })
                    .await;
                stats.estimated_copy_bytes = match (stats.estimated_copy_bytes, usage.ok()) {
                    (Some(current), Some(values)) => Some(
                        values
                            .into_iter()
                            .flatten()
                            .fold(current, u64::saturating_add),
                    ),
                    _ => None,
                };
            }

            stats.alarm_keys_scanned +=
                keys.iter().filter(|key| is_do_alarm_key(key)).count() as u64;
            for key in &keys {
                if key.starts_with("wf:instance:") && key.ends_with(":state") {
                    let instance = read_schema2_instance(redis, control, key)
                        .await
                        .map_err(|err| migration_error(format!("{key}: {}", err.message)))?;
                    stats.instances_scanned += 1;
                    stats.steps_converted += instance.step_count() as u64;
                } else if key.starts_with("wf:instance:") {
                    let (prefix, suffix) = key
                        .rsplit_once(':')
                        .ok_or_else(|| migration_error("Invalid schema-2 instance key"))?;
                    if !matches!(
                        suffix,
                        "payloads"
                            | "steps"
                            | "step-summaries"
                            | "step-summary-index"
                            | "events"
                            | "events-by-type"
                    ) {
                        return Err(migration_error("Unknown schema-2 instance key"));
                    }
                    let exists: bool = redis
                        .with_conn(async |mut conn| {
                            redis::cmd("EXISTS")
                                .arg(format!("{prefix}:state"))
                                .query_async(&mut conn)
                                .await
                        })
                        .await
                        .map_err(|err| redis_error("Unable to inspect instance owner", err))?;
                    if !exists {
                        return Err(migration_error(
                            "Schema-2 instance data has no owning state",
                        ));
                    }
                }
            }

            let alarm_state_keys = keys
                .into_iter()
                .filter(|key| is_do_alarm_state_key(key))
                .collect::<Vec<_>>();
            if !alarm_state_keys.is_empty() {
                let states: Vec<(Option<String>, Option<String>)> = redis
                    .with_conn(async move |mut conn| {
                        let mut pipe = redis::pipe();
                        for key in alarm_state_keys {
                            pipe.cmd("HMGET")
                                .arg(key)
                                .arg("status")
                                .arg("runLeaseExpiresAtMs");
                        }
                        pipe.query_async(&mut conn).await
                    })
                    .await
                    .map_err(|err| {
                        redis_error("Unable to inspect schema-2 DO alarm states", err)
                    })?;
                for (status, lease) in states {
                    if status.as_deref() != Some("running") {
                        continue;
                    }
                    let lease = lease
                        .and_then(|value| value.parse::<i64>().ok())
                        .filter(|value| *value > 0)
                        .ok_or_else(|| {
                            migration_error("Schema-2 running DO alarm claim has an invalid lease")
                        })?;
                    latest_running_alarm_lease = Some(
                        latest_running_alarm_lease.map_or(lease, |latest: i64| latest.max(lease)),
                    );
                }
            }
        }
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    if ttl_keys > 0 {
        return Err(migration_error(
            "Schema-2 Workflows state still contains expiring Redis keys; stop all writers and wait for transient TTL keys to drain",
        ));
    }
    if latest_running_alarm_lease.is_some_and(|lease| lease > now_ms()) {
        return Err(migration_error(
            "Schema-2 Workflows state still contains unexpired running DO alarm claims; settle alarm delivery or wait for the claim lease before migration",
        ));
    }
    Ok(stats)
}

async fn acquire_migration(redis: &Redis) -> MigrationResult<String> {
    let value = format!("{MIGRATION_IN_PROGRESS_PREFIX}{}", random_hex_64());
    let acquired: Option<String> = redis
        .with_conn({
            let value = value.clone();
            async move |mut conn| {
                redis::cmd("SET")
                    .arg(schema3_migration_key())
                    .arg(value)
                    .arg("NX")
                    .query_async(&mut conn)
                    .await
            }
        })
        .await
        .map_err(|err| redis_error("Unable to acquire schema3 migration ownership", err))?;
    if acquired.as_deref() != Some("OK") {
        return Err(migration_error(
            "Another schema3 migration task owns the migration; use resume only after confirming that task has exited",
        ));
    }
    Ok(value)
}

async fn validate_copy_support(archive: &Redis) -> MigrationResult<()> {
    let copied: i64 = archive
        .with_conn(async |mut conn| {
            redis::cmd("COPY")
                .arg(COPY_PROBE_KEY)
                .arg(COPY_PROBE_KEY)
                .arg("DB")
                .arg(WORKFLOWS_REDIS_DB)
                .query_async(&mut conn)
                .await
        })
        .await
        .map_err(|err| redis_error("Valkey does not permit cross-database COPY", err))?;
    if copied != 0 {
        return Err(migration_error(
            "Schema3 migration COPY probe unexpectedly found its reserved source key",
        ));
    }
    Ok(())
}

async fn resume_migration(redis: &Redis, state: &MigrationState) -> MigrationResult<String> {
    let MigrationState::InProgress(previous) = state else {
        return Err(migration_error(
            "No incomplete schema3 migration is available to resume",
        ));
    };
    let value = format!("{MIGRATION_IN_PROGRESS_PREFIX}{}", random_hex_64());
    let acquired: Option<String> = redis
        .with_conn({
            let previous = previous.clone();
            let value = value.clone();
            async move |mut conn| {
                redis::cmd("SET")
                    .arg(schema3_migration_key())
                    .arg(value)
                    .arg("IFEQ")
                    .arg(previous)
                    .query_async(&mut conn)
                    .await
            }
        })
        .await
        .map_err(|err| redis_error("Unable to resume schema3 migration ownership", err))?;
    if acquired.as_deref() != Some("OK") {
        return Err(migration_error(
            "Schema3 migration ownership changed while attempting resume",
        ));
    }
    Ok(value)
}

async fn assert_migration_owner(redis: &Redis, expected: &str) -> MigrationResult<()> {
    let current = migration_state(redis).await?;
    if current == MigrationState::InProgress(expected.to_string()) {
        Ok(())
    } else {
        Err(migration_error("Schema3 migration ownership was lost"))
    }
}

async fn finish_migration(redis: &Redis, expected: &str) -> MigrationResult<()> {
    let updated: Option<String> = redis
        .with_conn({
            let expected = expected.to_string();
            async move |mut conn| {
                redis::cmd("SET")
                    .arg(schema3_migration_key())
                    .arg(MIGRATION_COMPLETE)
                    .arg("IFEQ")
                    .arg(expected)
                    .query_async(&mut conn)
                    .await
            }
        })
        .await
        .map_err(|err| redis_error("Unable to finalize schema3 migration state", err))?;
    if updated.as_deref() != Some("OK") {
        return Err(migration_error(
            "Schema3 migration ownership changed before finalization",
        ));
    }
    Ok(())
}

async fn swap_active_to_archive(active: &Redis) -> MigrationResult<()> {
    let response: String = active
        .with_conn(async |mut conn| {
            redis::cmd("SWAPDB")
                .arg(WORKFLOWS_REDIS_DB)
                .arg(WORKFLOWS_ARCHIVE_REDIS_DB)
                .query_async(&mut conn)
                .await
        })
        .await
        .map_err(|err| redis_error("Unable to swap Workflows DB 2 into archive DB 15", err))?;
    if response != "OK" {
        return Err(migration_error(
            "Valkey returned an invalid SWAPDB response",
        ));
    }
    Ok(())
}

async fn validate_destination(active: &Redis, archive: &Redis) -> MigrationResult<()> {
    let mut cursor = 0;
    loop {
        let (next, keys) = scan_page(
            active,
            cursor,
            None,
            "Unable to scan the schema-3 Workflows database",
        )
        .await?;
        if keys
            .iter()
            .any(|key| schema2_key_type(key).is_none() || key == schema_version_key())
        {
            return Err(migration_error(
                "Incomplete schema-3 destination contains foreign state or a premature marker",
            ));
        }
        if !keys.is_empty() {
            let exists: Vec<i64> = archive
                .with_conn(async move |mut conn| {
                    let mut pipe = redis::pipe();
                    for key in keys.iter().filter(|key| !key.starts_with("wf:ready:")) {
                        pipe.cmd("EXISTS").arg(key);
                    }
                    pipe.query_async(&mut conn).await
                })
                .await
                .map_err(|err| {
                    redis_error("Unable to validate the partial migration destination", err)
                })?;
            if exists.into_iter().any(|value| value != 1) {
                return Err(migration_error(
                    "Schema-3 destination contains a key absent from the schema-2 archive",
                ));
            }
        }
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    Ok(())
}

async fn copy_schema2_keys(archive: &Redis, active: &Redis) -> MigrationResult<()> {
    validate_destination(active, archive).await?;
    let mut cursor = 0;
    loop {
        let (next, keys) = scan_page(
            archive,
            cursor,
            None,
            "Unable to scan archived Workflows keys",
        )
        .await?;
        for key in keys.into_iter().filter(|key| key != schema_version_key()) {
            // One COPY at a time avoids queueing a page of potentially large collections.
            let copied: i64 = archive
                .with_conn(async |mut conn| {
                    redis::cmd("COPY")
                        .arg(&key)
                        .arg(&key)
                        .arg("DB")
                        .arg(WORKFLOWS_REDIS_DB)
                        .arg("REPLACE")
                        .query_async(&mut conn)
                        .await
                })
                .await
                .map_err(|err| redis_error("Unable to copy archived Workflows state", err))?;
            if copied != 1 {
                return Err(migration_error(
                    "A schema-2 key disappeared during migration",
                ));
            }
            verify_copied_key(archive, active, &key).await?;
        }
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    Ok(())
}

async fn verify_copied_key(source: &Redis, target: &Redis, key: &str) -> MigrationResult<()> {
    let kind: String = source
        .with_conn(async |mut conn| redis::cmd("TYPE").arg(key).query_async(&mut conn).await)
        .await
        .map_err(|err| redis_error("Unable to inspect copied key type", err))?;
    let size_command = match kind.as_str() {
        "hash" => "HLEN",
        "set" => "SCARD",
        "zset" => "ZCARD",
        "string" => "STRLEN",
        _ => {
            return Err(migration_error(
                "Schema-2 key has an unsupported Redis type",
            ));
        }
    };
    let count = async |redis: &Redis| -> MigrationResult<u64> {
        redis
            .with_conn(async |mut conn| {
                redis::cmd(size_command)
                    .arg(key)
                    .query_async(&mut conn)
                    .await
            })
            .await
            .map_err(|err| redis_error("Unable to verify copied key size", err))
    };
    if count(source).await? != count(target).await? {
        return Err(migration_error(
            "Copied key size does not match its archive",
        ));
    }
    let mut cursor = 0u64;
    loop {
        let (next, matches) = match kind.as_str() {
            "hash" => {
                let (next, entries): (u64, Vec<(String, Vec<u8>)>) = source
                    .with_conn(async |mut conn| {
                        redis::cmd("HSCAN")
                            .arg(key)
                            .arg(cursor)
                            .arg("COUNT")
                            .arg(SCAN_COUNT)
                            .query_async(&mut conn)
                            .await
                    })
                    .await
                    .map_err(|err| redis_error("Unable to verify archived hash", err))?;
                let values: Vec<Option<Vec<u8>>> = if entries.is_empty() {
                    vec![]
                } else {
                    target
                        .with_conn(async |mut conn| {
                            redis::cmd("HMGET")
                                .arg(key)
                                .arg(entries.iter().map(|(field, _)| field).collect::<Vec<_>>())
                                .query_async(&mut conn)
                                .await
                        })
                        .await
                        .map_err(|err| redis_error("Unable to verify copied hash", err))?
                };
                (
                    next,
                    values.len() == entries.len()
                        && values
                            .iter()
                            .zip(&entries)
                            .all(|(actual, (_, expected))| actual.as_ref() == Some(expected)),
                )
            }
            "set" => {
                let (next, entries): (u64, Vec<String>) = source
                    .with_conn(async |mut conn| {
                        redis::cmd("SSCAN")
                            .arg(key)
                            .arg(cursor)
                            .arg("COUNT")
                            .arg(SCAN_COUNT)
                            .query_async(&mut conn)
                            .await
                    })
                    .await
                    .map_err(|err| redis_error("Unable to verify archived set", err))?;
                let present: Vec<bool> = if entries.is_empty() {
                    vec![]
                } else {
                    target
                        .with_conn(async |mut conn| {
                            redis::cmd("SMISMEMBER")
                                .arg(key)
                                .arg(&entries)
                                .query_async(&mut conn)
                                .await
                        })
                        .await
                        .map_err(|err| redis_error("Unable to verify copied set", err))?
                };
                (
                    next,
                    present.len() == entries.len() && present.iter().all(|value| *value),
                )
            }
            "zset" => {
                let (next, entries): (u64, Vec<(String, f64)>) = source
                    .with_conn(async |mut conn| {
                        redis::cmd("ZSCAN")
                            .arg(key)
                            .arg(cursor)
                            .arg("COUNT")
                            .arg(SCAN_COUNT)
                            .query_async(&mut conn)
                            .await
                    })
                    .await
                    .map_err(|err| redis_error("Unable to verify archived sorted set", err))?;
                let scores: Vec<Option<f64>> = if entries.is_empty() {
                    vec![]
                } else {
                    target
                        .with_conn(async |mut conn| {
                            redis::cmd("ZMSCORE")
                                .arg(key)
                                .arg(entries.iter().map(|(member, _)| member).collect::<Vec<_>>())
                                .query_async(&mut conn)
                                .await
                        })
                        .await
                        .map_err(|err| redis_error("Unable to verify copied sorted set", err))?
                };
                (
                    next,
                    scores.len() == entries.len()
                        && scores
                            .iter()
                            .zip(&entries)
                            .all(|(actual, (_, expected))| *actual == Some(*expected)),
                )
            }
            "string" => {
                let read = async |redis: &Redis| -> MigrationResult<Option<Vec<u8>>> {
                    redis
                        .with_conn(async |mut conn| {
                            redis::cmd("GET").arg(key).query_async(&mut conn).await
                        })
                        .await
                        .map_err(|err| redis_error("Unable to verify copied string", err))
                };
                (0, read(source).await? == read(target).await?)
            }
            _ => unreachable!(),
        };
        if !matches {
            return Err(migration_error(format!(
                "Copied key differs from archive: {key}"
            )));
        }
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    Ok(())
}

async fn convert_instances(
    archive: &Redis,
    active: &Redis,
    control: &Redis,
) -> MigrationResult<()> {
    let mut cursor = 0;
    loop {
        let (next, keys) = scan_page(
            archive,
            cursor,
            Some("wf:instance:*:state"),
            "Unable to scan archived instances",
        )
        .await?;
        for key in keys {
            let instance = read_schema2_instance(archive, control, &key)
                .await
                .map_err(|err| migration_error(format!("{key}: {}", err.message)))?;
            instance
                .install(active)
                .await
                .map_err(|err| migration_error(format!("{key}: {}", err.message)))?;
        }
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    Ok(())
}

async fn verify_key_survival(
    archive: &Redis,
    active: &Redis,
    expected_archive_size: u64,
) -> MigrationResult<()> {
    if db_size(archive, "Unable to verify archive size").await? != expected_archive_size {
        return Err(migration_error(
            "Archive key count changed during migration",
        ));
    }
    let mut cursor = 0;
    loop {
        let (next, keys) =
            scan_page(archive, cursor, None, "Unable to verify migrated keys").await?;
        let keys: Vec<_> = keys
            .into_iter()
            .filter(|key| key != schema_version_key())
            .collect();
        if !keys.is_empty() {
            let present: Vec<bool> = active
                .with_conn(async |mut conn| {
                    let mut pipe = redis::pipe();
                    for key in keys {
                        pipe.cmd("EXISTS").arg(key);
                    }
                    pipe.query_async(&mut conn).await
                })
                .await
                .map_err(|err| redis_error("Unable to verify migrated key presence", err))?;
            if present.iter().any(|exists| !exists) {
                return Err(migration_error("A migrated key was lost before completion"));
            }
        }
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    Ok(())
}

async fn publish_schema3_marker(active: &Redis) -> MigrationResult<()> {
    let installed: Option<String> = active
        .with_conn(async |mut conn| {
            redis::cmd("SET")
                .arg(schema_version_key())
                .arg(WORKFLOWS_SCHEMA_VERSION)
                .arg("NX")
                .query_async(&mut conn)
                .await
        })
        .await
        .map_err(|err| redis_error("Unable to publish the schema-3 marker", err))?;
    if installed.as_deref() != Some("OK") {
        return Err(migration_error(
            "Schema-3 marker appeared before migration completed",
        ));
    }
    Ok(())
}

fn parse_memory_snapshot(info: &str) -> MemorySnapshot {
    let numeric = |name: &str| {
        info.lines().find_map(|line| {
            let (candidate, value) = line.trim_end_matches('\r').split_once(':')?;
            (candidate == name)
                .then(|| value.parse::<u64>().ok())
                .flatten()
        })
    };
    let text = |name: &str| {
        info.lines().find_map(|line| {
            let (candidate, value) = line.trim_end_matches('\r').split_once(':')?;
            (candidate == name).then(|| value.to_string())
        })
    };
    let used_memory_bytes = numeric("used_memory");
    let max_memory_bytes = numeric("maxmemory");
    let free_memory_bytes = used_memory_bytes
        .zip(max_memory_bytes)
        .and_then(|(used, max)| (max > 0).then(|| max.saturating_sub(used)));
    MemorySnapshot {
        used_memory_bytes,
        max_memory_bytes,
        free_memory_bytes,
        max_memory_policy: text("maxmemory_policy"),
    }
}

fn capacity_warnings(
    memory: &MemorySnapshot,
    estimated_copy_bytes: Option<u64>,
    copy_required: bool,
) -> Vec<&'static str> {
    let mut warnings = Vec::new();
    if memory.max_memory_bytes == Some(0) {
        warnings.push("redis_maxmemory_is_unbounded; confirm host or container headroom");
    }
    if memory.used_memory_bytes.is_none()
        || memory.max_memory_bytes.is_none()
        || memory.max_memory_policy.is_none()
    {
        warnings.push("Valkey memory capacity is unavailable; confirm capacity manually");
    }
    if memory.max_memory_bytes.is_some_and(|max| max > 0)
        && memory.max_memory_policy.as_deref() != Some("noeviction")
    {
        warnings.push("configured eviction policy may evict keys under memory pressure");
    }
    if copy_required
        && memory
            .free_memory_bytes
            .zip(estimated_copy_bytes)
            .is_some_and(|(free, estimate)| estimate > free)
    {
        warnings.push("estimated COPY bytes exceed reported free memory");
    }
    if copy_required && estimated_copy_bytes.is_none() {
        warnings.push("COPY memory estimate is unavailable; confirm capacity manually");
    }
    warnings
}

async fn memory_snapshot(redis: &Redis) -> MemorySnapshot {
    let info: Result<String, redis::RedisError> = redis
        .with_conn(async |mut conn| {
            redis::cmd("INFO")
                .arg("MEMORY")
                .query_async(&mut conn)
                .await
        })
        .await;
    info.as_deref()
        .map(parse_memory_snapshot)
        .unwrap_or_default()
}

async fn connect_databases() -> MigrationResult<(Redis, Redis, Redis, Redis)> {
    let (url, control_url) = validated_workflows_redis_urls();
    let active_client = redis_client_from_url_with_db(&url, Some(WORKFLOWS_REDIS_DB))
        .map_err(|_| migration_error("Workflows Redis URL is invalid"))?;
    let archive_client = redis_client_from_url_with_db(&url, Some(WORKFLOWS_ARCHIVE_REDIS_DB))
        .map_err(|_| migration_error("Workflows Redis URL does not support archive DB 15"))?;
    let coordination_client = redis_client_from_url_with_db(&url, Some(0))
        .map_err(|_| migration_error("Workflows Redis URL does not support coordination DB 0"))?;
    let control_client = redis_client_from_url(&control_url)
        .map_err(|_| migration_error("Control Redis URL is invalid"))?;
    let (active, archive, coordination, control) = tokio::try_join!(
        active_client.get_connection_manager(),
        archive_client.get_connection_manager(),
        coordination_client.get_connection_manager(),
        control_client.get_connection_manager(),
    )
    .map_err(|err| {
        redis_error(
            "Unable to connect to Workflows active, archive, and coordination databases",
            err,
        )
    })?;
    Ok((
        Redis::new(active),
        Redis::new(archive),
        Redis::new(coordination),
        Redis::new(control),
    ))
}

async fn execute_migration(
    active: &Redis,
    archive: &Redis,
    coordination: &Redis,
    control: &Redis,
    owner: &str,
    mut phase: MigrationPhase,
) -> MigrationResult<()> {
    assert_migration_owner(coordination, owner).await?;
    if phase == MigrationPhase::Schema2Active {
        swap_active_to_archive(active).await?;
        phase = inspect_phase(active, archive).await?;
    }
    if phase == MigrationPhase::Copying {
        assert_migration_owner(coordination, owner).await?;
        let archive_size = db_size(archive, "Unable to count the immutable archive").await?;
        copy_schema2_keys(archive, active).await?;
        convert_instances(archive, active, control).await?;
        verify_key_survival(archive, active, archive_size).await?;
        assert_migration_owner(coordination, owner).await?;
        publish_schema3_marker(active).await?;
        phase = inspect_phase(active, archive).await?;
    }
    if phase != MigrationPhase::Schema3Verified {
        return Err(migration_error(
            "Workflows schema3 migration did not reach the prepared schema-3 phase",
        ));
    }
    assert_migration_owner(coordination, owner).await?;
    finish_migration(coordination, owner).await
}

async fn delete_completed_archive(
    active: &Redis,
    archive: &Redis,
    coordination: &Redis,
) -> MigrationResult<()> {
    if migration_state(coordination).await? != MigrationState::Complete {
        return Err(migration_error(
            "Archive deletion requires a completed schema3 migration",
        ));
    }
    match inspect_phase(active, archive).await? {
        MigrationPhase::Schema3Current => return Ok(()),
        MigrationPhase::Schema3Verified => {}
        _ => {
            return Err(migration_error(
                "Archive deletion requires a verified schema-3 active database",
            ));
        }
    }
    let mut cursor = 0;
    loop {
        let (next, keys) = scan_page(
            archive,
            cursor,
            None,
            "Unable to inspect archive before deletion",
        )
        .await?;
        if keys.iter().any(|key| schema2_key_type(key).is_none()) {
            return Err(migration_error(
                "Archive contains foreign keys; refusing deletion",
            ));
        }
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    let _: String = archive
        .with_conn(async |mut conn| {
            redis::cmd("FLUSHDB")
                .arg("ASYNC")
                .query_async(&mut conn)
                .await
        })
        .await
        .map_err(|err| redis_error("Unable to delete the completed archive", err))?;
    Ok(())
}

pub async fn run_schema3_migration(
    mode: Schema3MigrationMode,
    delete_archive: bool,
) -> Result<(), Box<dyn Error>> {
    if delete_archive && matches!(mode, Schema3MigrationMode::Check) {
        return Err(migration_error("--delete-archive is only valid with apply or resume").into());
    }
    let (active, archive, coordination, control) = connect_databases().await?;
    let initial_phase = inspect_phase(&active, &archive).await?;
    let initial_state = migration_state(&coordination).await?;
    validate_state_phase(&initial_state, initial_phase)?;

    let source_stats = if matches!(
        initial_phase,
        MigrationPhase::Schema3Verified | MigrationPhase::Schema3Current
    ) {
        None
    } else {
        let source = if initial_phase == MigrationPhase::Schema2Active {
            &active
        } else {
            &archive
        };
        let stats = validate_archive_source(source, &control).await?;
        validate_copy_support(&archive).await?;
        if initial_phase == MigrationPhase::Copying && matches!(mode, Schema3MigrationMode::Check) {
            validate_destination(&active, &archive).await?;
        }
        Some(stats)
    };
    let memory_before = memory_snapshot(&active).await;
    let warnings = capacity_warnings(
        &memory_before,
        source_stats.and_then(|stats| stats.estimated_copy_bytes),
        source_stats.is_some(),
    );

    if !matches!(mode, Schema3MigrationMode::Check)
        && !(matches!(mode, Schema3MigrationMode::Apply)
            && (initial_state == MigrationState::Complete
                || initial_phase == MigrationPhase::Schema3Current))
    {
        let owner = match mode {
            Schema3MigrationMode::Apply => acquire_migration(&coordination).await?,
            Schema3MigrationMode::Resume => resume_migration(&coordination, &initial_state).await?,
            Schema3MigrationMode::Check => unreachable!(),
        };
        execute_migration(
            &active,
            &archive,
            &coordination,
            &control,
            &owner,
            initial_phase,
        )
        .await?;
    }
    if delete_archive {
        delete_completed_archive(&active, &archive, &coordination).await?;
    }

    let phase = inspect_phase(&active, &archive).await?;
    let state = migration_state(&coordination).await?;
    validate_state_phase(&state, phase)?;
    let archive_key_count = match phase {
        MigrationPhase::Schema2Active => db_size(&active, "Unable to count schema-2 keys").await?,
        MigrationPhase::Copying
        | MigrationPhase::Schema3Verified
        | MigrationPhase::Schema3Current => {
            db_size(&archive, "Unable to count archived schema-2 keys").await?
        }
    };
    let report = Schema3MigrationReport {
        ok: true,
        command: "schema3-migrate",
        mode: mode.as_str(),
        phase: phase.as_str(),
        migration_state: state.as_str(),
        active_db: WORKFLOWS_REDIS_DB,
        archive_db: WORKFLOWS_ARCHIVE_REDIS_DB,
        archive_key_count,
        alarm_key_count: source_stats.map(|stats| stats.alarm_keys_scanned),
        instance_count: source_stats.map(|stats| stats.instances_scanned),
        converted_step_count: source_stats.map(|stats| stats.steps_converted),
        estimated_copy_bytes: source_stats.and_then(|stats| stats.estimated_copy_bytes),
        archive_deleted: delete_archive,
        memory_before,
        memory: memory_snapshot(&active).await,
        warnings,
    };
    println!(
        "{}",
        serde_json::to_string(&report)
            .map_err(|_| migration_error("Unable to serialize report"))?
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_phase_accepts_only_resumable_database_states() {
        assert_eq!(
            classify_phase(Some("2"), 10, None, 0).unwrap(),
            MigrationPhase::Schema2Active
        );
        assert_eq!(
            classify_phase(None, 0, Some("2"), 10).unwrap(),
            MigrationPhase::Copying
        );
        assert_eq!(
            classify_phase(None, 3, Some("2"), 10).unwrap(),
            MigrationPhase::Copying
        );
        assert_eq!(
            classify_phase(Some("3"), 4, Some("2"), 10).unwrap(),
            MigrationPhase::Schema3Verified
        );
        for state in [(Some("2"), 1, Some("2"), 1), (Some("3"), 1, Some("3"), 1)] {
            assert!(classify_phase(state.0, state.1, state.2, state.3).is_err());
        }
    }

    #[test]
    fn migration_coordination_state_is_strict_and_phase_bound() {
        let in_progress = format!("{MIGRATION_IN_PROGRESS_PREFIX}0123456789abcdef");
        assert_eq!(parse_migration_state(None).unwrap(), MigrationState::None);
        assert_eq!(
            parse_migration_state(Some(&in_progress)).unwrap(),
            MigrationState::InProgress(in_progress.clone())
        );
        assert_eq!(
            parse_migration_state(Some(MIGRATION_COMPLETE)).unwrap(),
            MigrationState::Complete
        );
        assert!(parse_migration_state(Some("in_progress:bad")).is_err());
        assert!(validate_state_phase(&MigrationState::None, MigrationPhase::Schema2Active).is_ok());
        assert!(
            validate_state_phase(
                &MigrationState::InProgress(in_progress.clone()),
                MigrationPhase::Schema2Active
            )
            .is_ok()
        );
        assert!(
            validate_state_phase(&MigrationState::Complete, MigrationPhase::Schema3Verified)
                .is_ok()
        );
        assert!(validate_state_phase(&MigrationState::None, MigrationPhase::Copying).is_err());
        assert!(
            validate_state_phase(&MigrationState::Complete, MigrationPhase::Schema2Active).is_err()
        );
    }

    #[test]
    fn incomplete_or_corrupt_migration_blocks_startup_but_complete_does_not() {
        assert!(validate_startup_state(None).is_ok());
        assert!(validate_startup_state(Some(MIGRATION_COMPLETE)).is_ok());
        for state in ["in_progress:0123456789abcdef", "in_progress:bad", "unknown"] {
            assert_eq!(
                validate_startup_state(Some(state)).unwrap_err().code,
                "workflow_schema_mismatch"
            );
        }
    }

    #[test]
    fn alarm_key_classification_is_narrow() {
        assert!(is_do_alarm_key("wf:internal:do-alarm:due:0"));
        assert!(is_do_alarm_state_key(
            "wf:internal:do-alarm:{doa-abc}:state"
        ));
        assert!(!is_do_alarm_state_key("wf:internal:do-alarm:ready:active"));
        assert!(!is_do_alarm_key("wf:instance:{demo:wf:id}:state"));
    }

    #[test]
    fn schema2_archive_accepts_only_owned_key_families() {
        for key in [
            "wf:schema_version",
            "wf:instance:{demo:wf:id}:state",
            "wf:ready:active",
            "wf:due:0",
            "wf:by-worker:demo:worker",
            "wf:by-workflow:demo:worker:wf_key",
            "wf:by-version:demo:worker:1",
            "wf:pending-version:demo:worker:1",
            "wf:retention",
            "wf:internal:do-alarm:ready:active",
        ] {
            assert!(schema2_key_type(key).is_some(), "{key}");
        }
        for key in ["routes:demo", "wf:defs:demo:worker", "wf:unknown"] {
            assert!(schema2_key_type(key).is_none(), "{key}");
        }
    }

    #[test]
    fn schema2_key_types_follow_the_persisted_family_contract() {
        for (key, expected) in [
            ("wf:schema_version", "string"),
            ("wf:ready:cursor", "string"),
            ("wf:ready:active", "set"),
            ("wf:ready:0", "set"),
            ("wf:due:0", "zset"),
            ("wf:retention", "zset"),
            ("wf:by-worker:demo:worker", "set"),
            ("wf:by-version:demo:worker:v1", "set"),
            ("wf:by-workflow:demo:worker:wf_key", "zset"),
            ("wf:pending-version:demo:worker:v1", "zset"),
            ("wf:instance:{demo:wf:id}:state", "hash"),
            ("wf:instance:{demo:wf:id}:payloads", "hash"),
            ("wf:instance:{demo:wf:id}:steps", "hash"),
            ("wf:instance:{demo:wf:id}:step-summaries", "hash"),
            ("wf:instance:{demo:wf:id}:step-summary-index", "zset"),
            ("wf:instance:{demo:wf:id}:events", "hash"),
            ("wf:instance:{demo:wf:id}:events-by-type", "zset"),
            ("wf:internal:do-alarm:{doa-abc}:state", "hash"),
            ("wf:internal:do-alarm:ready:cursor", "string"),
            ("wf:internal:do-alarm:ready:active", "set"),
            ("wf:internal:do-alarm:ready:0", "set"),
            ("wf:internal:do-alarm:due:0", "zset"),
            ("wf:internal:do-alarm:by-worker:demo:worker", "set"),
            (
                "wf:internal:do-alarm:by-worker:demo:worker:cleanup-snapshot:1",
                "set",
            ),
        ] {
            assert_eq!(schema2_key_type(key), Some(expected), "{key}");
        }
        for key in [
            "wf:instance:{demo:wf:id}:unknown",
            "wf:internal:do-alarm:unknown",
        ] {
            assert_eq!(schema2_key_type(key), None, "{key}");
        }
    }

    #[test]
    fn memory_report_is_advisory() {
        let memory = parse_memory_snapshot(
            "# Memory\r\nused_memory:900\r\nmaxmemory:1000\r\nmaxmemory_policy:volatile-lru\r\n",
        );
        assert_eq!(
            memory,
            MemorySnapshot {
                used_memory_bytes: Some(900),
                max_memory_bytes: Some(1000),
                free_memory_bytes: Some(100),
                max_memory_policy: Some("volatile-lru".to_string()),
            }
        );
        assert_eq!(
            capacity_warnings(&memory, Some(200), true),
            vec![
                "configured eviction policy may evict keys under memory pressure",
                "estimated COPY bytes exceed reported free memory",
            ]
        );
        let unlimited = parse_memory_snapshot(
            "used_memory:100\r\nmaxmemory:0\r\nmaxmemory_policy:noeviction\r\n",
        );
        assert_eq!(
            capacity_warnings(&unlimited, Some(200), true),
            vec!["redis_maxmemory_is_unbounded; confirm host or container headroom"]
        );
        assert_eq!(
            capacity_warnings(&MemorySnapshot::default(), None, true),
            vec![
                "Valkey memory capacity is unavailable; confirm capacity manually",
                "COPY memory estimate is unavailable; confirm capacity manually",
            ]
        );
        assert_eq!(
            capacity_warnings(&MemorySnapshot::default(), None, false),
            vec!["Valkey memory capacity is unavailable; confirm capacity manually",]
        );
    }
}
