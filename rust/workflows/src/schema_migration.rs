use std::error::Error;
use std::fmt;

use serde::Serialize;
use wdl_rust_common::{
    redis_conn::redis_client_from_url_with_db,
    time::{now_ms, random_hex_64},
};

use crate::{
    DO_ALARM_KEY_PREFIX, Redis, WORKFLOWS_ARCHIVE_REDIS_DB, WORKFLOWS_REDIS_DB,
    WORKFLOWS_SCHEMA_VERSION, WorkflowError, WorkflowResult, is_do_alarm_key, schema_version_key,
    schema3_reset_key, validated_workflows_redis_urls,
};

const LEGACY_SCHEMA_VERSION: &str = "2";
const SCAN_COUNT: usize = 100;
const COPY_PROBE_KEY: &str = "wf:__schema3-reset-copy-probe__";
const RESET_IN_PROGRESS_PREFIX: &str = "in_progress:";
const RESET_ARCHIVE_PENDING: &str = "archive_pending";

#[derive(Clone, Copy, Debug)]
pub enum Schema3ResetMode {
    Check,
    Apply,
    Resume,
}

impl Schema3ResetMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Check => "check",
            Self::Apply => "apply",
            Self::Resume => "resume",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResetPhase {
    Schema2Active,
    AlarmCopying,
    Schema3Prepared,
}

impl ResetPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Schema2Active => "schema2_active",
            Self::AlarmCopying => "alarm_copying",
            Self::Schema3Prepared => "schema3_prepared",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ResetState {
    None,
    InProgress(String),
    ArchivePending,
}

impl ResetState {
    fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::InProgress(_) => "in_progress",
            Self::ArchivePending => "archive_pending",
        }
    }
}

#[derive(Debug)]
struct Schema3ResetError(String);

impl fmt::Display for Schema3ResetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for Schema3ResetError {}

type ResetResult<T> = Result<T, Schema3ResetError>;

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
    estimated_alarm_copy_bytes: Option<u64>,
}

impl Default for SourceStats {
    fn default() -> Self {
        Self {
            alarm_keys_scanned: 0,
            estimated_alarm_copy_bytes: Some(0),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Schema3ResetReport {
    ok: bool,
    command: &'static str,
    mode: &'static str,
    phase: &'static str,
    reset_state: &'static str,
    active_db: i64,
    archive_db: i64,
    archive_key_count: u64,
    alarm_key_count: Option<u64>,
    estimated_alarm_copy_bytes: Option<u64>,
    memory: MemorySnapshot,
    warnings: Vec<&'static str>,
}

fn reset_error(message: impl Into<String>) -> Schema3ResetError {
    Schema3ResetError(message.into())
}

fn redis_error(context: &'static str, error: redis::RedisError) -> Schema3ResetError {
    let code = error.code().unwrap_or("redis_error");
    reset_error(format!("{context} ({code})"))
}

async fn db_size(redis: &Redis, context: &'static str) -> ResetResult<u64> {
    redis
        .with_conn(async |mut conn| redis::cmd("DBSIZE").query_async(&mut conn).await)
        .await
        .map_err(|err| redis_error(context, err))
}

async fn schema_marker(redis: &Redis, context: &'static str) -> ResetResult<Option<String>> {
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
) -> ResetResult<ResetPhase> {
    match (active_marker, active_size, archive_marker, archive_size) {
        (Some(LEGACY_SCHEMA_VERSION), _, None, 0) => Ok(ResetPhase::Schema2Active),
        (None, _, Some(LEGACY_SCHEMA_VERSION), _) => Ok(ResetPhase::AlarmCopying),
        (Some(WORKFLOWS_SCHEMA_VERSION), _, Some(LEGACY_SCHEMA_VERSION), _) => {
            Ok(ResetPhase::Schema3Prepared)
        }
        _ => Err(reset_error(
            "Workflows schema3 reset database state is invalid",
        )),
    }
}

async fn inspect_phase(active: &Redis, archive: &Redis) -> ResetResult<ResetPhase> {
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

fn parse_reset_state(raw: Option<&str>) -> ResetResult<ResetState> {
    match raw {
        None => Ok(ResetState::None),
        Some(RESET_ARCHIVE_PENDING) => Ok(ResetState::ArchivePending),
        Some(value) if value.starts_with(RESET_IN_PROGRESS_PREFIX) => {
            let token = &value[RESET_IN_PROGRESS_PREFIX.len()..];
            if token.len() == 16 && token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                Ok(ResetState::InProgress(value.to_string()))
            } else {
                Err(reset_error("Workflows schema3 reset token is corrupt"))
            }
        }
        Some(_) => Err(reset_error("Workflows schema3 reset state is corrupt")),
    }
}

async fn reset_state(redis: &Redis) -> ResetResult<ResetState> {
    let raw: Option<String> = redis
        .with_conn(async |mut conn| {
            redis::cmd("GET")
                .arg(schema3_reset_key())
                .query_async(&mut conn)
                .await
        })
        .await
        .map_err(|err| redis_error("Unable to read Workflows schema3 reset state", err))?;
    parse_reset_state(raw.as_deref())
}

pub(crate) async fn workflow_migration_pending(redis: &Redis) -> WorkflowResult<bool> {
    let raw: Option<String> = redis
        .with_conn(async |mut conn| {
            redis::cmd("GET")
                .arg(schema3_reset_key())
                .query_async(&mut conn)
                .await
        })
        .await?;
    match parse_reset_state(raw.as_deref()) {
        Ok(ResetState::None) => Ok(false),
        Ok(ResetState::ArchivePending) => Ok(true),
        Ok(ResetState::InProgress(_)) => Err(WorkflowError::schema_mismatch(
            "Workflows schema3 reset is incomplete; resume the operator task before starting Workflows",
        )),
        Err(_) => Err(WorkflowError::schema_mismatch(
            "Workflows schema3 reset state is corrupt",
        )),
    }
}

fn validate_state_phase(state: &ResetState, phase: ResetPhase) -> ResetResult<()> {
    let valid = matches!(
        (state, phase),
        (ResetState::None, ResetPhase::Schema2Active)
            | (ResetState::InProgress(_), ResetPhase::Schema2Active)
            | (ResetState::InProgress(_), ResetPhase::AlarmCopying)
            | (ResetState::InProgress(_), ResetPhase::Schema3Prepared)
            | (ResetState::ArchivePending, ResetPhase::Schema3Prepared)
    );
    if valid {
        Ok(())
    } else {
        Err(reset_error(
            "Workflows schema3 reset coordination and database states do not match",
        ))
    }
}

async fn scan_page(
    redis: &Redis,
    cursor: u64,
    pattern: Option<&str>,
    context: &'static str,
) -> ResetResult<(u64, Vec<String>)> {
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

fn is_schema2_workflows_key(key: &str) -> bool {
    key == schema_version_key()
        || key == "wf:retention"
        || [
            "wf:instance:",
            "wf:ready:",
            "wf:due:",
            "wf:by-worker:",
            "wf:by-workflow:",
            "wf:by-version:",
            "wf:pending-version:",
            DO_ALARM_KEY_PREFIX,
        ]
        .iter()
        .any(|prefix| key.starts_with(prefix))
}

async fn validate_archive_source(redis: &Redis) -> ResetResult<SourceStats> {
    let mut cursor = 0;
    let mut ttl_keys = 0usize;
    let mut foreign_keys = 0usize;
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
            foreign_keys += keys
                .iter()
                .filter(|key| !is_schema2_workflows_key(key))
                .count();
            let ttls: Vec<i64> = redis
                .with_conn({
                    let keys = keys.clone();
                    async move |mut conn| {
                        let mut pipe = redis::pipe();
                        for key in keys {
                            pipe.cmd("PTTL").arg(key);
                        }
                        pipe.query_async(&mut conn).await
                    }
                })
                .await
                .map_err(|err| redis_error("Unable to inspect schema-2 key TTLs", err))?;
            ttl_keys += ttls.into_iter().filter(|ttl| *ttl >= 0).count();

            let alarm_keys = keys
                .iter()
                .filter(|key| is_do_alarm_key(key))
                .cloned()
                .collect::<Vec<_>>();
            if !alarm_keys.is_empty() {
                let usage: Result<Vec<Option<u64>>, redis::RedisError> = redis
                    .with_conn({
                        let alarm_keys = alarm_keys.clone();
                        async move |mut conn| {
                            let mut pipe = redis::pipe();
                            for key in alarm_keys {
                                pipe.cmd("MEMORY").arg("USAGE").arg(key);
                            }
                            pipe.query_async(&mut conn).await
                        }
                    })
                    .await;
                stats.alarm_keys_scanned = stats
                    .alarm_keys_scanned
                    .saturating_add(alarm_keys.len() as u64);
                stats.estimated_alarm_copy_bytes =
                    match (stats.estimated_alarm_copy_bytes, usage.ok()) {
                        (Some(current), Some(values)) => Some(
                            values
                                .into_iter()
                                .flatten()
                                .fold(current, u64::saturating_add),
                        ),
                        _ => None,
                    };
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
                        .ok_or_else(|| {
                            reset_error("Schema-2 running DO alarm claim has an invalid lease")
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
    if foreign_keys > 0 {
        return Err(reset_error(
            "Workflows DB 2 contains keys outside the schema-2 owner set and is not dedicated",
        ));
    }
    if ttl_keys > 0 {
        return Err(reset_error(
            "Schema-2 Workflows state still contains expiring Redis keys; stop all writers and wait for transient TTL keys to drain",
        ));
    }
    if latest_running_alarm_lease.is_some_and(|lease| lease > now_ms()) {
        return Err(reset_error(
            "Schema-2 Workflows state still contains unexpired running DO alarm claims; settle alarm delivery or wait for the claim lease before reset",
        ));
    }
    Ok(stats)
}

async fn acquire_reset(redis: &Redis) -> ResetResult<String> {
    let value = format!("{RESET_IN_PROGRESS_PREFIX}{}", random_hex_64());
    let acquired: Option<String> = redis
        .with_conn({
            let value = value.clone();
            async move |mut conn| {
                redis::cmd("SET")
                    .arg(schema3_reset_key())
                    .arg(value)
                    .arg("NX")
                    .query_async(&mut conn)
                    .await
            }
        })
        .await
        .map_err(|err| redis_error("Unable to acquire schema3 reset ownership", err))?;
    if acquired.as_deref() != Some("OK") {
        return Err(reset_error(
            "Another schema3 reset task owns the migration; use resume only after confirming that task has exited",
        ));
    }
    Ok(value)
}

async fn validate_copy_support(archive: &Redis) -> ResetResult<()> {
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
        return Err(reset_error(
            "Schema3 reset COPY probe unexpectedly found its reserved source key",
        ));
    }
    Ok(())
}

async fn resume_reset(redis: &Redis, state: &ResetState) -> ResetResult<String> {
    let ResetState::InProgress(previous) = state else {
        return Err(reset_error(
            "No incomplete schema3 reset is available to resume",
        ));
    };
    let value = format!("{RESET_IN_PROGRESS_PREFIX}{}", random_hex_64());
    let acquired: Option<String> = redis
        .with_conn({
            let previous = previous.clone();
            let value = value.clone();
            async move |mut conn| {
                redis::cmd("SET")
                    .arg(schema3_reset_key())
                    .arg(value)
                    .arg("IFEQ")
                    .arg(previous)
                    .query_async(&mut conn)
                    .await
            }
        })
        .await
        .map_err(|err| redis_error("Unable to resume schema3 reset ownership", err))?;
    if acquired.as_deref() != Some("OK") {
        return Err(reset_error(
            "Schema3 reset ownership changed while attempting resume",
        ));
    }
    Ok(value)
}

async fn assert_reset_owner(redis: &Redis, expected: &str) -> ResetResult<()> {
    let current = reset_state(redis).await?;
    if current == ResetState::InProgress(expected.to_string()) {
        Ok(())
    } else {
        Err(reset_error("Schema3 reset ownership was lost"))
    }
}

async fn finish_reset(redis: &Redis, expected: &str) -> ResetResult<()> {
    let updated: Option<String> = redis
        .with_conn({
            let expected = expected.to_string();
            async move |mut conn| {
                redis::cmd("SET")
                    .arg(schema3_reset_key())
                    .arg(RESET_ARCHIVE_PENDING)
                    .arg("IFEQ")
                    .arg(expected)
                    .query_async(&mut conn)
                    .await
            }
        })
        .await
        .map_err(|err| redis_error("Unable to finalize schema3 reset state", err))?;
    if updated.as_deref() != Some("OK") {
        return Err(reset_error(
            "Schema3 reset ownership changed before finalization",
        ));
    }
    Ok(())
}

async fn swap_active_to_archive(active: &Redis) -> ResetResult<()> {
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
        return Err(reset_error("Valkey returned an invalid SWAPDB response"));
    }
    Ok(())
}

async fn validate_alarm_destination(active: &Redis, archive: &Redis) -> ResetResult<()> {
    let mut cursor = 0;
    loop {
        let (next, keys) = scan_page(
            active,
            cursor,
            None,
            "Unable to scan the schema-3 Workflows database",
        )
        .await?;
        let existing_alarm_keys = keys
            .iter()
            .filter(|key| is_do_alarm_key(key))
            .cloned()
            .collect::<Vec<_>>();
        if keys.iter().any(|key| !is_do_alarm_key(key)) {
            return Err(reset_error(
                "Schema-3 Workflows DB contains state outside the alarm projection",
            ));
        }
        if !existing_alarm_keys.is_empty() {
            let exists: Vec<i64> = archive
                .with_conn(async move |mut conn| {
                    let mut pipe = redis::pipe();
                    for key in existing_alarm_keys {
                        pipe.cmd("EXISTS").arg(key);
                    }
                    pipe.query_async(&mut conn).await
                })
                .await
                .map_err(|err| {
                    redis_error("Unable to validate existing migrated DO alarm keys", err)
                })?;
            if exists.into_iter().any(|value| value != 1) {
                return Err(reset_error(
                    "Schema-3 Workflows DB contains a DO alarm key absent from the immutable schema-2 archive",
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

async fn copy_alarm_keys(archive: &Redis, active: &Redis) -> ResetResult<()> {
    validate_alarm_destination(active, archive).await?;
    let pattern = format!("{DO_ALARM_KEY_PREFIX}*");
    let mut cursor = 0;
    loop {
        let (next, keys) = scan_page(
            archive,
            cursor,
            Some(&pattern),
            "Unable to scan archived DO alarm keys",
        )
        .await?;
        if !keys.is_empty() {
            let copied: Vec<i64> = archive
                .with_conn(async move |mut conn| {
                    let mut pipe = redis::pipe();
                    for key in keys {
                        pipe.cmd("COPY")
                            .arg(&key)
                            .arg(&key)
                            .arg("DB")
                            .arg(WORKFLOWS_REDIS_DB)
                            .arg("REPLACE");
                    }
                    pipe.query_async(&mut conn).await
                })
                .await
                .map_err(|err| redis_error("Unable to carry DO alarm keys into schema 3", err))?;
            if copied.into_iter().any(|value| value != 1) {
                return Err(reset_error(
                    "A schema-2 DO alarm key disappeared during carry-forward",
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

async fn publish_schema3_marker(active: &Redis) -> ResetResult<()> {
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
        return Err(reset_error(
            "Schema-3 marker appeared before alarm carry-forward completed",
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
    estimated_alarm_copy_bytes: Option<u64>,
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
            .zip(estimated_alarm_copy_bytes)
            .is_some_and(|(free, estimate)| estimate > free)
    {
        warnings.push("estimated alarm COPY bytes exceed reported free memory");
    }
    if copy_required && estimated_alarm_copy_bytes.is_none() {
        warnings.push("alarm COPY memory estimate is unavailable; confirm capacity manually");
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

async fn connect_databases() -> ResetResult<(Redis, Redis, Redis)> {
    let (url, _) = validated_workflows_redis_urls();
    let active_client = redis_client_from_url_with_db(&url, Some(WORKFLOWS_REDIS_DB))
        .map_err(|_| reset_error("Workflows Redis URL is invalid"))?;
    let archive_client = redis_client_from_url_with_db(&url, Some(WORKFLOWS_ARCHIVE_REDIS_DB))
        .map_err(|_| reset_error("Workflows Redis URL does not support archive DB 15"))?;
    let coordination_client = redis_client_from_url_with_db(&url, Some(0))
        .map_err(|_| reset_error("Workflows Redis URL does not support coordination DB 0"))?;
    let (active, archive, coordination) = tokio::try_join!(
        active_client.get_connection_manager(),
        archive_client.get_connection_manager(),
        coordination_client.get_connection_manager(),
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
    ))
}

async fn execute_reset(
    active: &Redis,
    archive: &Redis,
    coordination: &Redis,
    owner: &str,
    mut phase: ResetPhase,
) -> ResetResult<()> {
    assert_reset_owner(coordination, owner).await?;
    if phase == ResetPhase::Schema2Active {
        swap_active_to_archive(active).await?;
        phase = inspect_phase(active, archive).await?;
    }
    if phase == ResetPhase::AlarmCopying {
        assert_reset_owner(coordination, owner).await?;
        copy_alarm_keys(archive, active).await?;
        assert_reset_owner(coordination, owner).await?;
        publish_schema3_marker(active).await?;
        phase = inspect_phase(active, archive).await?;
    }
    if phase != ResetPhase::Schema3Prepared {
        return Err(reset_error(
            "Workflows schema3 reset did not reach the prepared schema-3 phase",
        ));
    }
    assert_reset_owner(coordination, owner).await?;
    finish_reset(coordination, owner).await
}

pub async fn run_schema3_reset(mode: Schema3ResetMode) -> Result<(), Box<dyn Error>> {
    let (active, archive, coordination) = connect_databases().await?;
    let initial_phase = inspect_phase(&active, &archive).await?;
    let initial_state = reset_state(&coordination).await?;
    validate_state_phase(&initial_state, initial_phase)?;

    let source_stats = if initial_phase == ResetPhase::Schema3Prepared {
        None
    } else {
        let source = if initial_phase == ResetPhase::Schema2Active {
            &active
        } else {
            &archive
        };
        let stats = validate_archive_source(source).await?;
        validate_copy_support(&archive).await?;
        if initial_phase == ResetPhase::AlarmCopying && matches!(mode, Schema3ResetMode::Check) {
            validate_alarm_destination(&active, &archive).await?;
        }
        Some(stats)
    };
    let memory = memory_snapshot(&active).await;
    let warnings = capacity_warnings(
        &memory,
        source_stats.and_then(|stats| stats.estimated_alarm_copy_bytes),
        initial_phase != ResetPhase::Schema3Prepared,
    );

    if !matches!(mode, Schema3ResetMode::Check)
        && !(matches!(mode, Schema3ResetMode::Apply) && initial_state == ResetState::ArchivePending)
    {
        let owner = match mode {
            Schema3ResetMode::Apply => acquire_reset(&coordination).await?,
            Schema3ResetMode::Resume => resume_reset(&coordination, &initial_state).await?,
            Schema3ResetMode::Check => unreachable!(),
        };
        execute_reset(&active, &archive, &coordination, &owner, initial_phase).await?;
    }

    let phase = inspect_phase(&active, &archive).await?;
    let state = reset_state(&coordination).await?;
    validate_state_phase(&state, phase)?;
    let archive_key_count = match phase {
        ResetPhase::Schema2Active => db_size(&active, "Unable to count schema-2 keys").await?,
        ResetPhase::AlarmCopying | ResetPhase::Schema3Prepared => {
            db_size(&archive, "Unable to count archived schema-2 keys").await?
        }
    };
    let alarm_key_count = if phase == ResetPhase::Schema3Prepared {
        Some(
            db_size(&active, "Unable to count migrated schema-3 alarm keys")
                .await?
                .saturating_sub(1),
        )
    } else {
        source_stats.map(|stats| stats.alarm_keys_scanned)
    };
    let report = Schema3ResetReport {
        ok: true,
        command: "schema3-reset",
        mode: mode.as_str(),
        phase: phase.as_str(),
        reset_state: state.as_str(),
        active_db: WORKFLOWS_REDIS_DB,
        archive_db: WORKFLOWS_ARCHIVE_REDIS_DB,
        archive_key_count,
        alarm_key_count,
        estimated_alarm_copy_bytes: source_stats.and_then(|stats| stats.estimated_alarm_copy_bytes),
        memory,
        warnings,
    };
    println!(
        "{}",
        serde_json::to_string(&report).map_err(|_| reset_error("Unable to serialize report"))?
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_phase_accepts_only_resumable_database_states() {
        assert_eq!(
            classify_phase(Some("2"), 10, None, 0).unwrap(),
            ResetPhase::Schema2Active
        );
        assert_eq!(
            classify_phase(None, 0, Some("2"), 10).unwrap(),
            ResetPhase::AlarmCopying
        );
        assert_eq!(
            classify_phase(None, 3, Some("2"), 10).unwrap(),
            ResetPhase::AlarmCopying
        );
        assert_eq!(
            classify_phase(Some("3"), 4, Some("2"), 10).unwrap(),
            ResetPhase::Schema3Prepared
        );
        for state in [
            (Some("3"), 1, None, 0),
            (Some("2"), 1, Some("2"), 1),
            (Some("3"), 1, Some("3"), 1),
        ] {
            assert!(classify_phase(state.0, state.1, state.2, state.3).is_err());
        }
    }

    #[test]
    fn reset_coordination_state_is_strict_and_phase_bound() {
        let in_progress = format!("{RESET_IN_PROGRESS_PREFIX}0123456789abcdef");
        assert_eq!(parse_reset_state(None).unwrap(), ResetState::None);
        assert_eq!(
            parse_reset_state(Some(&in_progress)).unwrap(),
            ResetState::InProgress(in_progress.clone())
        );
        assert_eq!(
            parse_reset_state(Some(RESET_ARCHIVE_PENDING)).unwrap(),
            ResetState::ArchivePending
        );
        assert!(parse_reset_state(Some("in_progress:bad")).is_err());
        assert!(validate_state_phase(&ResetState::None, ResetPhase::Schema2Active).is_ok());
        assert!(
            validate_state_phase(
                &ResetState::InProgress(in_progress.clone()),
                ResetPhase::Schema2Active
            )
            .is_ok()
        );
        assert!(
            validate_state_phase(&ResetState::ArchivePending, ResetPhase::Schema3Prepared).is_ok()
        );
        assert!(validate_state_phase(&ResetState::None, ResetPhase::AlarmCopying).is_err());
        assert!(
            validate_state_phase(&ResetState::ArchivePending, ResetPhase::Schema2Active).is_err()
        );
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
            assert!(is_schema2_workflows_key(key), "{key}");
        }
        for key in ["routes:demo", "wf:defs:demo:worker", "wf:unknown"] {
            assert!(!is_schema2_workflows_key(key), "{key}");
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
                "estimated alarm COPY bytes exceed reported free memory",
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
                "alarm COPY memory estimate is unavailable; confirm capacity manually",
            ]
        );
        assert_eq!(
            capacity_warnings(&MemorySnapshot::default(), None, false),
            vec!["Valkey memory capacity is unavailable; confirm capacity manually",]
        );
    }
}
