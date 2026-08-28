use std::env;

use serde_json::json;
use wdl_rust_common::env::{
    DEFAULT_REDIS_URL, env_u16, env_u64, env_usize, optional_env, positive_or,
};
use wdl_rust_common::internal_auth::{InternalAuthTokens, internal_auth_tokens_from_env};
use wdl_rust_common::log::{LogLevel, emit_log_line, log_level_from_env};
use wdl_rust_common::redis_conn::redis_client_from_url_with_db;

pub(crate) const WORKFLOW_READY_BATCH_SIZE: usize = 128;
pub(crate) const DO_ALARM_READY_BATCH_MAX: usize = 100;
pub(crate) const WORKFLOWS_REDIS_DB: i64 = 2;

#[derive(Clone)]
pub(crate) struct Config {
    pub(crate) redis_url: String,
    pub(crate) control_redis_url: String,
    pub(crate) runtime_host: String,
    pub(crate) runtime_port: u16,
    pub(crate) system_runtime_host: String,
    pub(crate) system_runtime_port: u16,
    pub(crate) do_runtime_host: String,
    pub(crate) do_runtime_port: u16,
    pub(crate) metrics_port: u16,
    pub(crate) dispatch_timeout_ms: u64,
    pub(crate) ready_dispatch_concurrency: usize,
    pub(crate) do_alarm_dispatch_concurrency: usize,
    pub(crate) run_lease_ms: u64,
    pub(crate) do_alarm_claim_lease_ms: u64,
    pub(crate) do_alarm_retry_delay_ms: u64,
    pub(crate) do_alarm_retry_max_delay_ms: u64,
    pub(crate) do_alarm_retry_jitter: f64,
    pub(crate) do_alarm_retry_max_tries: u64,
    pub(crate) shutdown_drain_ms: u64,
    pub(crate) progress_callback_lookup_concurrency: usize,
    pub(crate) progress_callback_concurrency: usize,
    pub(crate) internal_auth_tokens: InternalAuthTokens,
    pub(crate) log_level: LogLevel,
}

fn workflows_redis_url() -> String {
    if let Some(value) = optional_env("WORKFLOWS_REDIS_DB") {
        let configured = value.parse::<i64>().unwrap_or_else(|_| {
            panic!("WORKFLOWS_REDIS_DB must be 2; remove the override or use WORKFLOWS_REDIS_URL to select an endpoint with a dedicated DB 2")
        });
        assert_eq!(
            configured, WORKFLOWS_REDIS_DB,
            "WORKFLOWS_REDIS_DB must be 2; remove the override or use WORKFLOWS_REDIS_URL to select an endpoint with a dedicated DB 2"
        );
    }
    optional_env("WORKFLOWS_REDIS_URL")
        .unwrap_or_else(|| env::var("REDIS_URL").unwrap_or_else(|_| DEFAULT_REDIS_URL.to_string()))
}

fn control_redis_url() -> String {
    if let Some(url) = optional_env("CONTROL_REDIS_URL") {
        return url;
    }
    env::var("REDIS_URL").unwrap_or_else(|_| DEFAULT_REDIS_URL.to_string())
}

fn redis_database_identity(
    url: &str,
    db: Option<i64>,
    invalid_message: &'static str,
) -> (redis::ConnectionAddr, i64) {
    let client =
        redis_client_from_url_with_db(url, db).unwrap_or_else(|_| panic!("{invalid_message}"));
    let info = client.get_connection_info();
    (info.addr().clone(), info.redis_settings().db())
}

pub(crate) fn config_from_env() -> Config {
    let redis_url = workflows_redis_url();
    let control_redis_url = control_redis_url();
    let workflows_identity = redis_database_identity(
        &redis_url,
        Some(WORKFLOWS_REDIS_DB),
        "Workflows Redis URL is invalid",
    );
    let control_identity =
        redis_database_identity(&control_redis_url, None, "Control Redis URL is invalid");
    if workflows_identity == control_identity {
        panic!("Workflows DB 2 must not share the Control Redis database");
    }
    if let Some(data_redis_url) = optional_env("DATA_REDIS_URL") {
        let data_identity =
            redis_database_identity(&data_redis_url, None, "Data Redis URL is invalid");
        if workflows_identity == data_identity {
            panic!("Workflows DB 2 must not share the data-plane Redis database");
        }
    }
    let runtime_host = env::var("RUNTIME_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let runtime_port = env_u16("RUNTIME_PORT", 8088);
    let dispatch_timeout_ms = env_u64("WORKFLOWS_DISPATCH_TIMEOUT_MS", 60_000);
    let minimum_run_lease_ms = dispatch_timeout_ms.saturating_add(10_000);
    let default_do_alarm_claim_lease_ms = 300_000_u64.max(minimum_run_lease_ms);
    let configured_run_lease_ms = env::var("WORKFLOWS_RUN_LEASE_MS").ok();
    let parsed_run_lease_ms = positive_or(configured_run_lease_ms.clone(), minimum_run_lease_ms);
    let run_lease_ms = parsed_run_lease_ms.max(minimum_run_lease_ms);
    let configured_do_alarm_claim_lease_ms = env::var("WORKFLOWS_DO_ALARM_CLAIM_LEASE_MS").ok();
    let parsed_do_alarm_claim_lease_ms = positive_or(
        configured_do_alarm_claim_lease_ms.clone(),
        default_do_alarm_claim_lease_ms,
    );
    let do_alarm_claim_lease_ms = parsed_do_alarm_claim_lease_ms.max(minimum_run_lease_ms);
    let log_level = log_level_from_env();
    if configured_run_lease_ms.is_some() && parsed_run_lease_ms < minimum_run_lease_ms {
        emit_log_line(
            "workflows",
            LogLevel::Warn,
            log_level,
            "workflows_run_lease_clamped",
            json!({
                "configured_ms": parsed_run_lease_ms,
                "effective_ms": run_lease_ms,
                "minimum_ms": minimum_run_lease_ms,
                "dispatch_timeout_ms": dispatch_timeout_ms,
            }),
        );
    }
    if configured_do_alarm_claim_lease_ms.is_some()
        && parsed_do_alarm_claim_lease_ms < minimum_run_lease_ms
    {
        emit_log_line(
            "workflows",
            LogLevel::Warn,
            log_level,
            "workflows_do_alarm_claim_lease_clamped",
            json!({
                "configured_ms": parsed_do_alarm_claim_lease_ms,
                "effective_ms": do_alarm_claim_lease_ms,
                "minimum_ms": minimum_run_lease_ms,
                "dispatch_timeout_ms": dispatch_timeout_ms,
            }),
        );
    }
    Config {
        redis_url,
        control_redis_url,
        system_runtime_host: env::var("SYSTEM_RUNTIME_HOST")
            .unwrap_or_else(|_| runtime_host.clone()),
        system_runtime_port: env_u16("SYSTEM_RUNTIME_PORT", runtime_port),
        do_runtime_host: env::var("DO_RUNTIME_HOST").unwrap_or_else(|_| "do-runtime".to_string()),
        do_runtime_port: env_u16("DO_RUNTIME_PORT", 8788),
        runtime_host,
        runtime_port,
        metrics_port: env_u16("WORKFLOWS_PORT", 9120),
        dispatch_timeout_ms,
        ready_dispatch_concurrency: env_usize(
            "WORKFLOWS_READY_DISPATCH_CONCURRENCY",
            WORKFLOW_READY_BATCH_SIZE,
        )
        .clamp(1, WORKFLOW_READY_BATCH_SIZE),
        do_alarm_dispatch_concurrency: env_usize("WORKFLOWS_DO_ALARM_DISPATCH_CONCURRENCY", 32)
            .clamp(1, DO_ALARM_READY_BATCH_MAX),
        run_lease_ms,
        do_alarm_claim_lease_ms,
        do_alarm_retry_delay_ms: env_u64("WORKFLOWS_DO_ALARM_RETRY_DELAY_MS", 5_000),
        do_alarm_retry_max_delay_ms: env_u64("WORKFLOWS_DO_ALARM_RETRY_MAX_DELAY_MS", 1024 * 1000),
        do_alarm_retry_jitter: env::var("WORKFLOWS_DO_ALARM_RETRY_JITTER")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value >= 0.0)
            .map(|value| value.min(1.0))
            .unwrap_or(0.25),
        do_alarm_retry_max_tries: env_u64("WORKFLOWS_DO_ALARM_RETRY_MAX_TRIES", 6),
        shutdown_drain_ms: env_u64("WORKFLOWS_SHUTDOWN_DRAIN_MS", 25_000),
        progress_callback_lookup_concurrency: env_usize(
            "WORKFLOWS_PROGRESS_CALLBACK_LOOKUP_CONCURRENCY",
            128,
        ),
        progress_callback_concurrency: env_usize("WORKFLOWS_PROGRESS_CALLBACK_CONCURRENCY", 32),
        internal_auth_tokens: internal_auth_tokens_from_env()
            .expect("WDL_INTERNAL_AUTH_TOKEN must be configured"),
        log_level,
    }
}
