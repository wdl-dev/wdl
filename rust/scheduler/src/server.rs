use std::net::SocketAddr;
use std::sync::Arc;

use axum::response::Response;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{Value as JsonValue, json};
use wdl_rust_common::env::optional_env;
use wdl_rust_common::health::healthcheck_http_200;
use wdl_rust_common::metrics::prometheus_response;
use wdl_rust_common::redis_conn::redis_client_from_url;
use wdl_rust_common::shutdown::shutdown_signal;

use crate::{
    AppState, DispatchSemaphores, LogLevel, Metrics, QueueState, Redis, SERVICE, ShutdownState,
    config_from_env, error_fields, log, random_hex_64, run_startup_reconciliation,
    spawn_background_tasks,
};

async fn metrics_handler(axum::extract::State(state): axum::extract::State<AppState>) -> Response {
    prometheus_response(&state.metrics)
}

async fn healthz_handler(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Json<JsonValue> {
    Json(json!({
        "ok": true,
        "service": SERVICE,
        "instance": state.instance_id,
    }))
}

pub fn healthcheck() -> i32 {
    healthcheck_http_200("SCHEDULER_METRICS_PORT", 9110, "/_healthz")
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(config_from_env());
    let redis_configured = optional_env("REDIS_URL").is_some();
    let data_redis_configured = optional_env("DATA_REDIS_URL").is_some();
    let redis_client = redis_client_from_url(&config.redis_url)?;
    let data_redis_client = redis_client_from_url(&config.data_redis_url)?;
    let (conn, data_conn) = tokio::try_join!(
        redis_client.get_connection_manager(),
        data_redis_client.get_connection_manager(),
    )?;
    let state = AppState {
        redis: Redis::new(conn),
        data_redis: Redis::new(data_conn),
        data_redis_client,
        http: reqwest::Client::new(),
        metrics: Arc::new(Metrics::default()),
        queues: Arc::new(QueueState::default()),
        shutdown: Arc::new(ShutdownState::default()),
        dispatch: Arc::new(DispatchSemaphores::new(
            config.cron_max_concurrency,
            config.queue_max_concurrency,
        )),
        config: config.clone(),
        instance_id: random_hex_64(),
    };

    log(
        &state,
        LogLevel::Info,
        "scheduler_started",
        json!({
            "instance": state.instance_id,
            "redis_configured": redis_configured,
            "data_redis_configured": data_redis_configured,
            "runtime": format!("{}:{}", config.runtime_host, config.runtime_port),
            "system_runtime": format!("{}:{}", config.system_runtime_host, config.system_runtime_port),
            "workflows": config.workflows_host.as_ref().map(|host| format!("{}:{}", host, config.workflows_port)),
            "workflows_tick_interval_ms": config.workflows_tick_interval_ms,
            "workflows_tick_active_interval_ms": config.workflows_tick_active_interval_ms,
            "workflows_tick_timeout_ms": config.workflows_tick_timeout_ms,
            "max_concurrency": config.max_concurrency,
            "cron_max_concurrency": config.cron_max_concurrency,
            "queue_max_concurrency": config.queue_max_concurrency,
            "implementation": "rust",
        }),
    );

    let app = Router::new()
        .route("/_metrics", get(metrics_handler))
        .route("/_healthz", get(healthz_handler))
        .with_state(state.clone());
    let addr = SocketAddr::from(([0, 0, 0, 0], config.metrics_port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let server_state = state.clone();
    tokio::spawn(async move {
        log(
            &server_state,
            LogLevel::Info,
            "metrics_listening",
            json!({ "port": server_state.config.metrics_port }),
        );
        if let Err(err) = axum::serve(listener, app).await {
            log(
                &server_state,
                LogLevel::Error,
                "metrics_server_failed",
                error_fields("Error", err.to_string()),
            );
        }
    });

    run_startup_reconciliation(state.clone()).await;
    spawn_background_tasks(state.clone());

    shutdown_signal().await?;
    state.request_shutdown().await;
    log(
        &state,
        LogLevel::Info,
        "scheduler_shutdown",
        json!({ "instance": state.instance_id }),
    );
    Ok(())
}
