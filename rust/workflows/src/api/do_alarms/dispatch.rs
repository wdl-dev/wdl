use std::time::Duration;

use serde::Serialize;
use serde_json::{Value as JsonValue, json};
use wdl_rust_common::internal_auth::INTERNAL_AUTH_HEADER;
use wdl_rust_common::redis_eval::append_eval_cmd;
use wdl_rust_common::time::now_ms;
use wdl_rust_common::worker_contract::{
    do_storage_id_key, parse_version_tag, routes_key, worker_versions_key,
};

use crate::{
    AppState, DO_ALARM_READY_BATCH_MAX, DispatchTaskUnavailable, DoAlarmJobKeys, LogLevel,
    WorkflowError, WorkflowResult, do_alarm_shard_queue_keys, fields_with_error, log,
};

use super::super::{
    ReadyAdmissionConfig, ReadyAdmissionOutcome, ReadyAdmissionResult, admit_ready_members,
    due_shards_with_due_members, eval_script, remove_ready_member_if_state_missing,
};
use super::model::{DoAlarmJob, job_from_state, map_hgetall};
use super::scripts::{
    CLAIM_DO_ALARM, DISCARD_CORRUPT_DO_ALARM, FINALIZE_DO_ALARM, MOVE_DUE_DO_ALARM_SCRIPT,
    RETRY_DO_ALARM,
};

const DO_ALARM_MOVE_DUE_LIMIT: usize = 100;

fn do_alarm_ready_admission_config(concurrency: usize) -> ReadyAdmissionConfig {
    ReadyAdmissionConfig {
        batch_size: concurrency.min(DO_ALARM_READY_BATCH_MAX),
        concurrency,
        prune_on_error: true,
    }
}

fn saturating_i64_ms(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[derive(Default)]
pub(crate) struct DoAlarmAdmissionResult {
    pub(crate) due_moved: usize,
    pub(crate) admitted: usize,
    pub(crate) capacity_blocked: bool,
    pub(crate) error: Option<WorkflowError>,
}

impl DoAlarmAdmissionResult {
    fn from_ready(due_moved: usize, result: ReadyAdmissionResult<usize>) -> Self {
        Self {
            due_moved,
            admitted: result.counters,
            capacity_blocked: result.capacity_blocked,
            error: result.error,
        }
    }
}

enum ClaimDoAlarmResult {
    Job(Box<DoAlarmJob>),
    None,
    DiscardedCorrupt,
}

enum DoAlarmDispatchError {
    Retryable(String),
    InFlightUnknown(String),
}

enum DoAlarmAdmission {
    Dispatch {
        job: Box<DoAlarmJob>,
        dispatch_version: String,
    },
    Immediate,
}

#[derive(Debug, PartialEq, Eq)]
enum AlarmDispatchVersionDecision {
    Original,
    Retarget(String),
    Discard,
}

fn alarm_dispatch_version_decision(
    job_storage_id: &str,
    job_version: &str,
    current_storage_id: Option<&str>,
    active_version: Option<&str>,
    retained_score: Option<f64>,
) -> WorkflowResult<AlarmDispatchVersionDecision> {
    if current_storage_id != Some(job_storage_id) {
        return Ok(AlarmDispatchVersionDecision::Discard);
    }
    if active_version == Some(job_version) || retained_score.is_some() {
        return Ok(AlarmDispatchVersionDecision::Original);
    }
    let Some(active_version) = active_version else {
        return Ok(AlarmDispatchVersionDecision::Discard);
    };
    parse_version_tag(active_version)
        .map_err(|_| WorkflowError::invalid_state("Active worker version is invalid"))?;
    Ok(AlarmDispatchVersionDecision::Retarget(
        active_version.to_string(),
    ))
}

pub(crate) async fn move_due_do_alarms(app: &AppState) -> WorkflowResult<usize> {
    let queue = do_alarm_shard_queue_keys();
    let now = now_ms();
    let now_arg = now.to_string();
    let limit = DO_ALARM_MOVE_DUE_LIMIT.to_string();
    let shards = due_shards_with_due_members(app, queue, now).await?;
    if shards.is_empty() {
        return Ok(0);
    }
    let counts: Vec<i64> = app
        .redis
        .with_conn(async |mut conn| {
            let mut pipe = redis::pipe();
            for shard in shards {
                let due = queue.due(shard);
                let ready = queue.ready(shard);
                let shard_arg = shard.to_string();
                append_eval_cmd(
                    &mut pipe,
                    MOVE_DUE_DO_ALARM_SCRIPT,
                    &[&due, &ready, queue.ready_active()],
                    &[&now_arg, &limit, &shard_arg],
                );
            }
            pipe.query_async(&mut conn).await
        })
        .await?;
    Ok(counts.into_iter().map(|count| count.max(0) as usize).sum())
}

async fn claim_do_alarm(app: &AppState, job_id: &str) -> WorkflowResult<ClaimDoAlarmResult> {
    let keys = DoAlarmJobKeys::new(job_id.to_string());
    let state_key = keys.state();
    let due_key = keys.due();
    let ready_key = keys.ready();
    let now = now_ms();
    let run_token = format!(
        "doa-run-{}-{}",
        wdl_rust_common::time::random_hex_64(),
        app.next_run_claim_sequence()
    );
    let lease_expires = now.saturating_add(saturating_i64_ms(app.config.do_alarm_claim_lease_ms));
    let now_arg = now.to_string();
    let lease_arg = lease_expires.to_string();
    let result: Vec<String> = eval_script(
        app,
        &CLAIM_DO_ALARM,
        &[&state_key, &due_key, &ready_key],
        &[job_id, &now_arg, &run_token, &lease_arg],
    )
    .await?;
    if result
        .first()
        .is_some_and(|value| matches!(value.as_str(), "missing" | "keep"))
    {
        return Ok(ClaimDoAlarmResult::None);
    }
    let state = map_hgetall(result);
    match job_from_state(job_id.to_string(), state.clone()) {
        Ok(job) => Ok(ClaimDoAlarmResult::Job(Box::new(job))),
        Err(err) => {
            if let Some(run_token) = state.get("runToken") {
                let by_worker = state
                    .get("ns")
                    .zip(state.get("worker"))
                    .map(|(ns, worker)| crate::do_alarm_by_worker_key(ns, worker));
                discard_corrupt_do_alarm(app, job_id, run_token, by_worker.as_deref()).await?;
            }
            log(
                app,
                LogLevel::Warn,
                "do_alarm_corrupt_discarded",
                json!({
                    "job_id": job_id,
                    "error_message": err.message,
                }),
            );
            Ok(ClaimDoAlarmResult::DiscardedCorrupt)
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DoAlarmDispatchRequest<'a> {
    ns: &'a str,
    worker: &'a str,
    version: &'a str,
    do_storage_id: &'a str,
    class_name: &'a str,
    object_name: &'a str,
    retry_count: u64,
    token: &'a str,
}

async fn dispatch_do_alarm(
    app: &AppState,
    job: &DoAlarmJob,
    dispatch_version: &str,
) -> Result<JsonValue, DoAlarmDispatchError> {
    let url = format!(
        "http://{}:{}/internal/do/alarms/dispatch",
        app.config.do_runtime_host, app.config.do_runtime_port
    );
    let request = DoAlarmDispatchRequest {
        ns: &job.ns,
        worker: &job.worker,
        version: dispatch_version,
        do_storage_id: &job.do_storage_id,
        class_name: &job.class_name,
        object_name: &job.object_name,
        retry_count: job.retry_count,
        token: &job.row_token,
    };
    let response = app
        .http
        .post(url)
        .header(
            INTERNAL_AUTH_HEADER,
            app.config.internal_auth_tokens.current.as_str(),
        )
        .timeout(Duration::from_millis(app.config.dispatch_timeout_ms))
        .json(&request)
        .send()
        .await
        .map_err(|err| {
            if err.is_timeout() {
                DoAlarmDispatchError::InFlightUnknown(err.to_string())
            } else {
                DoAlarmDispatchError::Retryable(err.to_string())
            }
        })?;
    let status = response.status();
    let body = response.text().await.map_err(|err| {
        if err.is_timeout() {
            DoAlarmDispatchError::InFlightUnknown(err.to_string())
        } else {
            DoAlarmDispatchError::Retryable(err.to_string())
        }
    })?;
    if !status.is_success() {
        return Err(DoAlarmDispatchError::Retryable(format!(
            "do-runtime returned {}: {}",
            status.as_u16(),
            body
        )));
    }
    if body.trim().is_empty() {
        return Ok(JsonValue::Null);
    }
    serde_json::from_str(&body).map_err(|err| DoAlarmDispatchError::Retryable(err.to_string()))
}

async fn resolve_alarm_dispatch_version(
    app: &AppState,
    job: &DoAlarmJob,
) -> WorkflowResult<Option<String>> {
    let storage_key = do_storage_id_key(&job.ns, &job.worker);
    let (current_storage_id, active_version, retained_score): (
        Option<String>,
        Option<String>,
        Option<f64>,
    ) = app
        .control_redis
        .with_conn(async |mut conn| {
            let mut pipe = redis::pipe();
            pipe.cmd("GET").arg(storage_key);
            pipe.cmd("HGET").arg(routes_key(&job.ns)).arg(&job.worker);
            pipe.cmd("ZSCORE")
                .arg(worker_versions_key(&job.ns, &job.worker))
                .arg(&job.version);
            pipe.query_async(&mut conn).await
        })
        .await?;
    match alarm_dispatch_version_decision(
        &job.do_storage_id,
        &job.version,
        current_storage_id.as_deref(),
        active_version.as_deref(),
        retained_score,
    )? {
        AlarmDispatchVersionDecision::Original => Ok(Some(job.version.clone())),
        AlarmDispatchVersionDecision::Retarget(active_version) => {
            log(
                app,
                LogLevel::Info,
                "do_alarm_retargeted",
                json!({
                    "namespace": job.ns,
                    "worker": job.worker,
                    "class_name": job.class_name,
                    "object_name": job.object_name,
                    "job_id": job.job_id,
                    "previous_version": job.version,
                    "dispatch_version": active_version,
                }),
            );
            Ok(Some(active_version))
        }
        AlarmDispatchVersionDecision::Discard => Ok(None),
    }
}

async fn finalize_claimed_do_alarm(app: &AppState, job: &DoAlarmJob) -> WorkflowResult<bool> {
    let keys = job.keys();
    let state_key = keys.state();
    let due_key = keys.due();
    let ready_key = keys.ready();
    let by_worker = job.by_worker_key();
    let changed: i64 = eval_script(
        app,
        &FINALIZE_DO_ALARM,
        &[&state_key, &due_key, &ready_key, &by_worker],
        &[&job.run_token, &job.job_id],
    )
    .await?;
    Ok(changed == 1)
}

async fn discard_corrupt_do_alarm(
    app: &AppState,
    job_id: &str,
    run_token: &str,
    by_worker: Option<&str>,
) -> WorkflowResult<bool> {
    let keys = DoAlarmJobKeys::new(job_id.to_string());
    let state_key = keys.state();
    let due_key = keys.due();
    let ready_key = keys.ready();
    let has_by_worker = if by_worker.is_some() { "1" } else { "0" };
    let changed: i64 = eval_script(
        app,
        &DISCARD_CORRUPT_DO_ALARM,
        &[
            &state_key,
            &due_key,
            &ready_key,
            by_worker.unwrap_or("__wdl_no_do_alarm_by_worker__"),
        ],
        &[run_token, job_id, has_by_worker],
    )
    .await?;
    Ok(changed == 1)
}

fn retry_delay_ms(app: &AppState, retry_count: u64) -> u64 {
    retry_delay_ms_from_parts(
        app.config.do_alarm_retry_delay_ms,
        app.config.do_alarm_retry_max_delay_ms,
        app.config.do_alarm_retry_jitter,
        retry_count,
        wdl_rust_common::time::random_unit_f64(),
    )
}

fn retry_delay_ms_from_parts(
    base_ms: u64,
    max_ms: u64,
    jitter: f64,
    retry_count: u64,
    random: f64,
) -> u64 {
    let base = base_ms.max(1);
    let max = max_ms.max(base);
    let exponent = retry_count.min(20);
    let backoff = base.saturating_mul(1u64.checked_shl(exponent as u32).unwrap_or(u64::MAX));
    let capped = backoff.min(max);
    if jitter == 0.0 {
        return capped;
    }
    let random = random.clamp(0.0, 1.0);
    let spread = (capped as f64 * jitter).round() as u64;
    let offset = ((random * 2.0 - 1.0) * spread as f64).round() as i128;
    let adjusted = (capped as i128).saturating_add(offset);
    adjusted.clamp(1, max as i128) as u64
}

async fn retry_do_alarm(
    app: &AppState,
    job: &DoAlarmJob,
    error_message: &str,
) -> WorkflowResult<i64> {
    let keys = job.keys();
    let state_key = keys.state();
    let due_key = keys.due();
    let ready_key = keys.ready();
    let by_worker = job.by_worker_key();
    let now = now_ms();
    let delay_ms = retry_delay_ms(app, job.retry_count);
    let next_due = now.saturating_add(saturating_i64_ms(delay_ms));
    let next_due = next_due.to_string();
    let now = now.to_string();
    let max_tries = app.config.do_alarm_retry_max_tries.to_string();
    eval_script(
        app,
        &RETRY_DO_ALARM,
        &[&state_key, &due_key, &ready_key, &by_worker],
        &[
            &job.run_token,
            &job.job_id,
            &next_due,
            &now,
            &max_tries,
            error_message,
        ],
    )
    .await
}

async fn remove_ready_if_missing(app: &AppState, job_id: &str) -> WorkflowResult<bool> {
    let keys = DoAlarmJobKeys::new(job_id.to_string());
    remove_ready_member_if_state_missing(app, &keys.state(), &keys.ready(), job_id).await
}

fn increment_do_alarm_outcome(app: &AppState, outcome: &'static str) {
    app.metrics
        .increment("do_alarm_dispatches", &[("outcome", outcome)], 1.0);
}

async fn prepare_ready_do_alarm_job(
    app: &AppState,
    job_id: &str,
) -> WorkflowResult<DoAlarmAdmission> {
    let job = match claim_do_alarm(app, job_id).await? {
        ClaimDoAlarmResult::Job(job) => *job,
        ClaimDoAlarmResult::DiscardedCorrupt => {
            increment_do_alarm_outcome(app, "discarded");
            return Ok(DoAlarmAdmission::Immediate);
        }
        ClaimDoAlarmResult::None => {
            remove_ready_if_missing(app, job_id).await?;
            return Ok(DoAlarmAdmission::Immediate);
        }
    };
    let Some(dispatch_version) = resolve_alarm_dispatch_version(app, &job).await? else {
        if finalize_claimed_do_alarm(app, &job).await? {
            increment_do_alarm_outcome(app, "discarded");
        }
        log(
            app,
            LogLevel::Warn,
            "do_alarm_discarded",
            json!({
                "namespace": job.ns,
                "worker": job.worker,
                "class_name": job.class_name,
                "object_name": job.object_name,
                "job_id": job.job_id,
                "retry_count": job.retry_count,
                "error_message": "DO alarm target is no longer retained or active",
            }),
        );
        return Ok(DoAlarmAdmission::Immediate);
    };
    Ok(DoAlarmAdmission::Dispatch {
        job: Box::new(job),
        dispatch_version,
    })
}

async fn finish_claimed_do_alarm(
    app: &AppState,
    job: &DoAlarmJob,
    dispatch_version: &str,
) -> WorkflowResult<()> {
    match dispatch_do_alarm(app, job, dispatch_version).await {
        Ok(body) => {
            if finalize_claimed_do_alarm(app, job).await? {
                increment_do_alarm_outcome(app, "delivered");
                let ignored = body
                    .get("ignored")
                    .and_then(JsonValue::as_bool)
                    .unwrap_or(false);
                log(
                    app,
                    if ignored {
                        LogLevel::Info
                    } else {
                        LogLevel::Debug
                    },
                    "do_alarm_delivered",
                    json!({
                        "namespace": job.ns,
                        "worker": job.worker,
                        "class_name": job.class_name,
                        "object_name": job.object_name,
                        "job_id": job.job_id,
                        "ignored": ignored,
                    }),
                );
            }
        }
        Err(DoAlarmDispatchError::InFlightUnknown(err)) => {
            increment_do_alarm_outcome(app, "in_flight_unknown");
            log(
                app,
                LogLevel::Warn,
                "do_alarm_dispatch_in_flight_unknown",
                json!({
                    "namespace": job.ns,
                    "worker": job.worker,
                    "class_name": job.class_name,
                    "object_name": job.object_name,
                    "job_id": job.job_id,
                    "retry_count": job.retry_count,
                    "error_message": err,
                }),
            );
        }
        Err(DoAlarmDispatchError::Retryable(err)) => {
            let outcome = retry_do_alarm(app, job, &err).await?;
            match outcome {
                1 => increment_do_alarm_outcome(app, "retried"),
                2 => increment_do_alarm_outcome(app, "discarded"),
                _ => {}
            }
            log(
                app,
                if outcome == 2 {
                    LogLevel::Warn
                } else {
                    LogLevel::Info
                },
                if outcome == 2 {
                    "do_alarm_discarded"
                } else {
                    "do_alarm_retry_scheduled"
                },
                json!({
                    "namespace": job.ns,
                    "worker": job.worker,
                    "class_name": job.class_name,
                    "object_name": job.object_name,
                    "job_id": job.job_id,
                    "retry_count": job.retry_count,
                    "error_message": err,
                }),
            );
        }
    }
    Ok(())
}

fn log_do_alarm_dispatch_task_error(app: &AppState, job: &DoAlarmJob, err: &WorkflowError) {
    log(
        app,
        LogLevel::Warn,
        "do_alarm_dispatch_task_error",
        fields_with_error(
            json!({
                "namespace": job.ns,
                "worker": job.worker,
                "class_name": job.class_name,
                "object_name": job.object_name,
                "job_id": job.job_id,
                "retry_count": job.retry_count,
                "error_code": err.code,
            }),
            "Error",
            &err.message,
        ),
    );
}

async fn process_ready_do_alarm_job(
    app: &AppState,
    job_id: String,
) -> WorkflowResult<ReadyAdmissionOutcome<usize>> {
    let guard = match app.begin_dispatch_task(&app.dispatch.do_alarm) {
        Ok(guard) => guard,
        Err(DispatchTaskUnavailable::Stopping) => {
            return Ok(ReadyAdmissionOutcome::stop_after_current_batch(0));
        }
        Err(DispatchTaskUnavailable::AtCapacity) => {
            return Ok(ReadyAdmissionOutcome::capacity_unavailable(0));
        }
    };
    match prepare_ready_do_alarm_job(app, &job_id).await? {
        DoAlarmAdmission::Dispatch {
            job,
            dispatch_version,
        } => {
            let panic_fields = json!({
                "namespace": job.ns,
                "worker": job.worker,
                "job_id": job.job_id,
            });
            let state = app.clone();
            app.spawn_tracked(
                guard,
                "do_alarm_dispatch_task_panicked",
                panic_fields,
                async move {
                    if let Err(err) = finish_claimed_do_alarm(&state, &job, &dispatch_version).await
                    {
                        log_do_alarm_dispatch_task_error(&state, &job, &err);
                    }
                },
            );
            Ok(ReadyAdmissionOutcome::admitted(1))
        }
        DoAlarmAdmission::Immediate => {
            drop(guard);
            Ok(ReadyAdmissionOutcome::capacity_released(0))
        }
    }
}

fn merge_admitted(target: &mut usize, delta: usize) {
    *target += delta;
}

pub(crate) async fn admit_ready_do_alarms(
    app: &AppState,
) -> WorkflowResult<DoAlarmAdmissionResult> {
    let due_moved = move_due_do_alarms(app).await?;
    let result = admit_ready_members(
        app,
        do_alarm_shard_queue_keys(),
        do_alarm_ready_admission_config(app.config.do_alarm_dispatch_concurrency),
        0,
        |_, job_id| process_ready_do_alarm_job(app, job_id),
        merge_admitted,
    )
    .await?;
    Ok(DoAlarmAdmissionResult::from_ready(due_moved, result))
}

#[cfg(test)]
mod tests {
    use super::{
        AlarmDispatchVersionDecision, DoAlarmAdmissionResult, ReadyAdmissionResult,
        alarm_dispatch_version_decision, do_alarm_ready_admission_config,
        retry_delay_ms_from_parts, saturating_i64_ms,
    };

    #[test]
    fn alarm_dispatch_version_decision_preserves_fences_and_retargets() {
        let cases = [
            (
                "storage mismatch",
                Some("storage-b"),
                Some("v2"),
                Some(1.0),
                AlarmDispatchVersionDecision::Discard,
            ),
            (
                "active original",
                Some("storage-a"),
                Some("v1"),
                None,
                AlarmDispatchVersionDecision::Original,
            ),
            (
                "retained original",
                Some("storage-a"),
                Some("v2"),
                Some(1.0),
                AlarmDispatchVersionDecision::Original,
            ),
            (
                "retarget active",
                Some("storage-a"),
                Some("v2"),
                None,
                AlarmDispatchVersionDecision::Retarget("v2".to_string()),
            ),
            (
                "no active version",
                Some("storage-a"),
                None,
                None,
                AlarmDispatchVersionDecision::Discard,
            ),
        ];

        for (label, storage, active, retained_score, expected) in cases {
            assert_eq!(
                alarm_dispatch_version_decision(
                    "storage-a",
                    "v1",
                    storage,
                    active,
                    retained_score,
                )
                .expect(label),
                expected,
                "{label}",
            );
        }
    }

    #[test]
    fn alarm_dispatch_version_decision_rejects_invalid_retarget() {
        let err = alarm_dispatch_version_decision(
            "storage-a",
            "v1",
            Some("storage-a"),
            Some("bad/version"),
            None,
        )
        .expect_err("invalid active version must fail closed");

        assert_eq!(err.code, "workflow_invalid_state");
    }

    #[test]
    fn ready_batch_is_one_bounded_admission_wave() {
        let config = do_alarm_ready_admission_config(32);

        assert_eq!(config.batch_size, 32);
        assert_eq!(config.concurrency, 32);
        assert!(config.prune_on_error);
    }

    #[test]
    fn admission_result_can_report_partial_activity_with_an_error() {
        let result = DoAlarmAdmissionResult::from_ready(
            2,
            ReadyAdmissionResult {
                counters: 3,
                error: Some(crate::WorkflowError::internal_error("candidate failed")),
                capacity_blocked: true,
            },
        );

        assert_eq!(result.due_moved, 2);
        assert_eq!(result.admitted, 3);
        assert!(result.capacity_blocked);
        assert_eq!(
            result.error.as_ref().map(|err| err.code),
            Some("internal_error")
        );
    }

    #[test]
    fn retry_delay_caps_with_zero_jitter() {
        assert_eq!(
            retry_delay_ms_from_parts(5_000, 1024 * 1000, 0.0, 99, 1.0),
            1024 * 1000
        );
    }

    #[test]
    fn retry_delay_jitter_does_not_exceed_max_delay() {
        assert_eq!(
            retry_delay_ms_from_parts(5_000, 1024 * 1000, 0.25, 99, 1.0),
            1024 * 1000
        );
    }

    #[test]
    fn retry_delay_uses_wide_signed_math_for_huge_caps() {
        assert_eq!(
            retry_delay_ms_from_parts(u64::MAX, u64::MAX, 1.0, 0, 1.0),
            u64::MAX
        );
        assert_eq!(
            retry_delay_ms_from_parts(u64::MAX, u64::MAX, 1.0, 0, 0.0),
            1
        );
    }

    #[test]
    fn u64_millisecond_config_saturates_before_i64_arithmetic() {
        assert_eq!(saturating_i64_ms(42), 42);
        assert_eq!(saturating_i64_ms(u64::MAX), i64::MAX);
    }

    #[test]
    fn dispatch_timeout_preserves_running_claim_until_lease_expiry() {
        let source = include_str!("dispatch.rs");
        let implementation = source
            .split("\n#[cfg(test)]")
            .next()
            .expect("dispatch implementation should precede tests");
        let timeout_branch = implementation
            .find("Err(DoAlarmDispatchError::InFlightUnknown(err))")
            .expect("in-flight unknown dispatches must have their own branch");
        let retry_branch = implementation
            .find("Err(DoAlarmDispatchError::Retryable(err))")
            .expect("retryable dispatch errors must stay separate");

        assert!(implementation.contains("if err.is_timeout()"));
        assert!(implementation.contains("DoAlarmDispatchError::InFlightUnknown(err.to_string())"));
        assert!(implementation.contains("app.config.do_alarm_claim_lease_ms"));
        assert!(timeout_branch < retry_branch);
        assert!(
            implementation[timeout_branch..retry_branch]
                .contains("increment_do_alarm_outcome(app, \"in_flight_unknown\")")
        );
        assert!(implementation.contains("\"do_alarm_dispatch_in_flight_unknown\""));
        assert!(implementation.contains("DoAlarmDispatchError::InFlightUnknown(err)"));
    }
}
