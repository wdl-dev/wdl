use std::env;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::{
    AppState, LogLevel, Metrics, Redis, SERVICE, ShutdownState, WorkflowError, WorkflowResult,
    check_delete_lifecycle, claim_step, commit_step_error, commit_step_success, config_from_env,
    create_batch, create_instance, ensure_workflows_schema, get_instance, list_instances, log,
    pause_instance, read_do_alarm_cleanup_request, read_do_alarm_delete_request,
    read_do_alarm_set_request, read_lifecycle_check_request, read_replay_step_page,
    read_workflow_replay_request, read_workflow_request, read_workflow_step_request,
    register_sleep, register_wait, restart_instance, resume_instance, send_event, status_instance,
    terminate_instance, tick_workflows, workflow_error_fields,
};
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use serde_json::{Value as JsonValue, json};
use tokio::sync::Semaphore;
use wdl_rust_common::health::healthcheck_http_200;
use wdl_rust_common::internal_auth::{
    INTERNAL_AUTH_FAILURE_CODE, INTERNAL_AUTH_FAILURE_MESSAGE, internal_auth_failure_response,
    internal_auth_headers_match,
};
use wdl_rust_common::metrics::prometheus_response;
use wdl_rust_common::request_id::request_id_from_headers;
use wdl_rust_common::shutdown::shutdown_signal;
use wdl_rust_common::time::duration_ms_for_log;

impl IntoResponse for WorkflowError {
    fn into_response(self) -> Response {
        let code = self.code;
        let message = self.message;
        let status = self.status;
        let mut response = (
            status,
            Json(json!({
                "error": code,
                "message": message,
            })),
        )
            .into_response();
        response
            .extensions_mut()
            .insert(ResponseError { code, message });
        response
    }
}

#[derive(Clone)]
struct ResponseError {
    code: &'static str,
    message: String,
}

fn workflow_json<T: Serialize>(
    response: T,
    context: &'static str,
) -> WorkflowResult<Json<JsonValue>> {
    serde_json::to_value(response)
        .map(Json)
        .map_err(|err| WorkflowError::internal_error(format!("{context}: {err}")))
}

async fn create_handler(
    State(state): State<AppState>,
    body: Body,
) -> WorkflowResult<Json<JsonValue>> {
    let req = read_workflow_request(body).await?;
    let response = create_instance(&state, req).await?;
    workflow_json(response, "workflow create response serializes")
}

async fn create_batch_handler(
    State(state): State<AppState>,
    body: Body,
) -> WorkflowResult<Json<JsonValue>> {
    let req = read_workflow_request(body).await?;
    let response = create_batch(&state, req).await?;
    workflow_json(response, "workflow batch response serializes")
}

async fn get_handler(State(state): State<AppState>, body: Body) -> WorkflowResult<Json<JsonValue>> {
    let req = read_workflow_request(body).await?;
    let response = get_instance(&state, req).await?;
    workflow_json(response, "workflow get response serializes")
}

async fn instances_handler(
    State(state): State<AppState>,
    body: Body,
) -> WorkflowResult<Json<JsonValue>> {
    let req = read_workflow_request(body).await?;
    let response = list_instances(&state, req).await?;
    workflow_json(response, "workflow instances response serializes")
}

async fn status_handler(
    State(state): State<AppState>,
    body: Body,
) -> WorkflowResult<Json<JsonValue>> {
    let req = read_workflow_request(body).await?;
    let response = status_instance(&state, req).await?;
    workflow_json(response, "workflow status response serializes")
}

async fn pause_handler(
    State(state): State<AppState>,
    body: Body,
) -> WorkflowResult<Json<JsonValue>> {
    let req = read_workflow_request(body).await?;
    let response = pause_instance(&state, req).await?;
    workflow_json(response, "workflow pause response serializes")
}

async fn resume_handler(
    State(state): State<AppState>,
    body: Body,
) -> WorkflowResult<Json<JsonValue>> {
    let req = read_workflow_request(body).await?;
    let response = resume_instance(&state, req).await?;
    workflow_json(response, "workflow resume response serializes")
}

async fn restart_handler(
    State(state): State<AppState>,
    body: Body,
) -> WorkflowResult<Json<JsonValue>> {
    let req = read_workflow_request(body).await?;
    let response = restart_instance(&state, req).await?;
    workflow_json(response, "workflow restart response serializes")
}

async fn terminate_handler(
    State(state): State<AppState>,
    body: Body,
) -> WorkflowResult<Json<JsonValue>> {
    let req = read_workflow_request(body).await?;
    let response = terminate_instance(&state, req).await?;
    workflow_json(response, "workflow terminate response serializes")
}

async fn lifecycle_check_delete_handler(
    State(state): State<AppState>,
    body: Body,
) -> WorkflowResult<Json<JsonValue>> {
    let req = read_lifecycle_check_request(body).await?;
    let response = check_delete_lifecycle(&state, req).await?;
    workflow_json(response, "workflow lifecycle response serializes")
}

async fn tick_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> WorkflowResult<Json<JsonValue>> {
    let request_id = request_id_from_headers(&headers);
    let response = tick_workflows(&state, request_id.as_deref()).await?;
    log(
        &state,
        LogLevel::Debug,
        "workflow_tick",
        json!({
            "request_id": request_id,
            "dispatched": response.dispatched,
            "completed": response.completed,
            "failed": response.failed,
            "suspended": response.suspended,
            "due_moved": response.due_moved,
            "retention_cleaned": response.retention_cleaned,
            "do_alarm_due_moved": response.do_alarm_due_moved,
            "do_alarm_dispatched": response.do_alarm_dispatched,
            "do_alarm_delivered": response.do_alarm_delivered,
            "do_alarm_retried": response.do_alarm_retried,
            "do_alarm_discarded": response.do_alarm_discarded,
            "do_alarm_skipped": response.do_alarm_skipped,
        }),
    );
    workflow_json(response, "workflow tick response serializes")
}

async fn do_alarm_set_handler(
    State(state): State<AppState>,
    body: Body,
) -> WorkflowResult<Json<JsonValue>> {
    let req = read_do_alarm_set_request(body).await?;
    let response = crate::set_do_alarm(&state, req).await?;
    workflow_json(response, "DO alarm set response serializes")
}

async fn do_alarm_delete_handler(
    State(state): State<AppState>,
    body: Body,
) -> WorkflowResult<Json<JsonValue>> {
    let req = read_do_alarm_delete_request(body).await?;
    let response = crate::delete_do_alarm(&state, req).await?;
    workflow_json(response, "DO alarm delete response serializes")
}

async fn do_alarm_cleanup_worker_handler(
    State(state): State<AppState>,
    body: Body,
) -> WorkflowResult<Json<JsonValue>> {
    let req = read_do_alarm_cleanup_request(body).await?;
    let response = crate::cleanup_do_alarms_for_worker(&state, req).await?;
    workflow_json(response, "DO alarm cleanup response serializes")
}

async fn claim_step_handler(
    State(state): State<AppState>,
    body: Body,
) -> WorkflowResult<Json<JsonValue>> {
    let req = read_workflow_step_request(body).await?;
    let response = claim_step(&state, req).await?;
    Ok(Json(response))
}

async fn replay_steps_handler(
    State(state): State<AppState>,
    body: Body,
) -> WorkflowResult<Json<JsonValue>> {
    let req = read_workflow_replay_request(body).await?;
    let response = read_replay_step_page(&state, req).await?;
    Ok(Json(response))
}

async fn commit_step_success_handler(
    State(state): State<AppState>,
    body: Body,
) -> WorkflowResult<Json<JsonValue>> {
    let req = read_workflow_step_request(body).await?;
    let response = commit_step_success(&state, req).await?;
    Ok(Json(response))
}

async fn commit_step_error_handler(
    State(state): State<AppState>,
    body: Body,
) -> WorkflowResult<Json<JsonValue>> {
    let req = read_workflow_step_request(body).await?;
    let response = commit_step_error(&state, req).await?;
    Ok(Json(response))
}

async fn register_sleep_handler(
    State(state): State<AppState>,
    body: Body,
) -> WorkflowResult<Json<JsonValue>> {
    let req = read_workflow_step_request(body).await?;
    let response = register_sleep(&state, req).await?;
    Ok(Json(response))
}

async fn register_wait_handler(
    State(state): State<AppState>,
    body: Body,
) -> WorkflowResult<Json<JsonValue>> {
    let req = read_workflow_step_request(body).await?;
    let response = register_wait(&state, req).await?;
    Ok(Json(response))
}

async fn send_event_handler(
    State(state): State<AppState>,
    body: Body,
) -> WorkflowResult<Json<JsonValue>> {
    let req = read_workflow_request(body).await?;
    let response = send_event(&state, req).await?;
    Ok(Json(response))
}

// Keep this low-cardinality map in sync with the Router routes above. Raw paths
// must not be used as metric labels.
fn route_name(method: &axum::http::Method, path: &str) -> &'static str {
    match (method.as_str(), path) {
        ("GET", "/_healthz") => "healthz",
        ("GET", "/_metrics") => "metrics",
        ("POST", "/internal/workflows/create") => "workflow_create",
        ("POST", "/internal/workflows/create-batch") => "workflow_create_batch",
        ("POST", "/internal/workflows/get") => "workflow_get",
        ("POST", "/internal/workflows/instances") => "workflow_instances",
        ("POST", "/internal/workflows/status") => "workflow_status",
        ("POST", "/internal/workflows/pause") => "workflow_pause",
        ("POST", "/internal/workflows/resume") => "workflow_resume",
        ("POST", "/internal/workflows/terminate") => "workflow_terminate",
        ("POST", "/internal/workflows/restart") => "workflow_restart",
        ("POST", "/internal/workflows/send-event") => "workflow_send_event",
        ("POST", "/internal/workflows/lifecycle/check-delete") => "workflow_check_delete",
        ("POST", "/internal/workflows/tick") => "workflow_tick",
        ("POST", "/internal/workflows/do-alarms/set") => "do_alarm_set",
        ("POST", "/internal/workflows/do-alarms/delete") => "do_alarm_delete",
        ("POST", "/internal/workflows/do-alarms/cleanup-worker") => "do_alarm_cleanup_worker",
        ("POST", "/internal/workflows/claim-step") => "workflow_claim_step",
        ("POST", "/internal/workflows/replay-steps") => "workflow_replay_steps",
        ("POST", "/internal/workflows/commit-step-success") => "workflow_commit_step_success",
        ("POST", "/internal/workflows/commit-step-error") => "workflow_commit_step_error",
        ("POST", "/internal/workflows/register-sleep") => "workflow_register_sleep",
        ("POST", "/internal/workflows/register-wait") => "workflow_register_wait",
        _ => "unknown",
    }
}

fn record_request_complete(
    state: &AppState,
    method: &str,
    route: &str,
    status: StatusCode,
    request_id: Option<&str>,
    started_at: Instant,
    error: Option<(&str, &str)>,
) {
    let elapsed = started_at.elapsed();
    let duration_ms = elapsed.as_secs_f64() * 1000.0;
    let log_duration_ms = duration_ms_for_log(elapsed);
    let status_label = status.as_u16().to_string();
    state.metrics.increment(
        "requests",
        &[
            ("service", SERVICE),
            ("route", route),
            ("status", &status_label),
        ],
        1.0,
    );
    state.metrics.observe(
        "request_duration_ms",
        &[("service", SERVICE), ("route", route)],
        duration_ms,
    );
    if status.is_server_error() {
        state.metrics.increment(
            "request_errors",
            &[
                ("service", SERVICE),
                ("route", route),
                ("status", &status_label),
            ],
            1.0,
        );
    }
    let probe = matches!(route, "healthz" | "metrics");
    if !probe || status.is_server_error() {
        log(
            state,
            if status.is_server_error() {
                LogLevel::Error
            } else {
                LogLevel::Info
            },
            "request_complete",
            json!({
                "request_id": request_id,
                "method": method,
                "route": route,
                "status": status.as_u16(),
                "duration_ms": log_duration_ms,
                "error_code": error.map(|(code, _)| code),
                "error_message": error.map(|(_, message)| message),
            }),
        );
    }
}

fn response_error(response: &Response) -> Option<(&'static str, &str)> {
    response
        .extensions()
        .get::<ResponseError>()
        .map(|err| (err.code, err.message.as_str()))
}

async fn track_request(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let started_at = Instant::now();
    let method = request.method().clone();
    let route = route_name(&method, request.uri().path());
    let request_id = request_id_from_headers(request.headers());
    if !matches!(route, "healthz" | "metrics")
        && !internal_auth_headers_match(request.headers(), &state.config.internal_auth_tokens)
    {
        let response = internal_auth_failure_response();
        record_request_complete(
            &state,
            method.as_str(),
            route,
            response.status(),
            request_id.as_deref(),
            started_at,
            Some((INTERNAL_AUTH_FAILURE_CODE, INTERNAL_AUTH_FAILURE_MESSAGE)),
        );
        return response;
    }
    let Some(_guard) = state.begin_in_flight() else {
        let response =
            WorkflowError::internal_error("Workflows service is shutting down").into_response();
        let error = response_error(&response);
        record_request_complete(
            &state,
            method.as_str(),
            route,
            response.status(),
            request_id.as_deref(),
            started_at,
            error,
        );
        return response;
    };
    let response = next.run(request).await;
    let error = response_error(&response);
    record_request_complete(
        &state,
        method.as_str(),
        route,
        response.status(),
        request_id.as_deref(),
        started_at,
        error,
    );
    response
}

async fn metrics_handler(State(state): State<AppState>) -> Response {
    prometheus_response(&state.metrics)
}

async fn healthz_handler(State(state): State<AppState>) -> WorkflowResult<Json<JsonValue>> {
    let start = Instant::now();
    let pong: String = state
        .redis
        .with_conn(async |mut conn| redis::cmd("PING").query_async(&mut conn).await)
        .await?;
    state.metrics.observe(
        "workflow_redis_ping_duration_ms",
        &[],
        start.elapsed().as_secs_f64() * 1000.0,
    );
    state
        .metrics
        .increment("workflow_health_checks", &[("outcome", "ok")], 1.0);
    Ok(Json(json!({
        "ok": pong == "PONG",
        "service": SERVICE,
        "instance": state.instance_id,
    })))
}

pub fn healthcheck() -> i32 {
    healthcheck_http_200("WORKFLOWS_PORT", 9120, "/_healthz")
}

pub(crate) fn random_instance_id() -> String {
    format!("wf-{}", wdl_rust_common::time::random_hex_64())
}

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(config_from_env());
    let redis_configured = env::var("WORKFLOWS_REDIS_URL")
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
        || env::var("REDIS_URL")
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false);
    let redis_client = redis::Client::open(config.redis_url.as_str())?;
    let redis_conn = redis_client.get_connection_manager().await?;
    let control_redis_client = redis::Client::open(config.control_redis_url.as_str())?;
    let control_redis_conn = control_redis_client.get_connection_manager().await?;
    let state = AppState {
        redis: Redis::new(redis_conn),
        control_redis: Redis::new(control_redis_conn),
        http: reqwest::Client::new(),
        metrics: Arc::new(Metrics::default()),
        shutdown: Arc::new(ShutdownState::default()),
        progress_callback_lookups: Arc::new(Semaphore::new(
            config.progress_callback_lookup_concurrency,
        )),
        progress_callbacks: Arc::new(Semaphore::new(config.progress_callback_concurrency)),
        progress_callback_cache: Arc::new(Mutex::new(Default::default())),
        config: config.clone(),
        instance_id: random_instance_id(),
        run_claim_counter: Arc::new(AtomicU64::new(0)),
    };
    ensure_workflows_schema(&state)
        .await
        .map_err(|err| std::io::Error::other(format!("{}: {}", err.code, err.message)))?;

    log(
        &state,
        LogLevel::Info,
        "workflows_started",
        json!({
            "instance": state.instance_id,
            "redis_configured": redis_configured,
            "port": config.metrics_port,
            "progress_callback_lookup_concurrency": config.progress_callback_lookup_concurrency,
            "progress_callback_concurrency": config.progress_callback_concurrency,
        }),
    );

    let app = Router::new()
        .route("/_healthz", get(healthz_handler))
        .route("/_metrics", get(metrics_handler))
        .route("/internal/workflows/create", post(create_handler))
        .route(
            "/internal/workflows/create-batch",
            post(create_batch_handler),
        )
        .route("/internal/workflows/get", post(get_handler))
        .route("/internal/workflows/instances", post(instances_handler))
        .route("/internal/workflows/status", post(status_handler))
        .route("/internal/workflows/pause", post(pause_handler))
        .route("/internal/workflows/resume", post(resume_handler))
        .route("/internal/workflows/terminate", post(terminate_handler))
        .route("/internal/workflows/restart", post(restart_handler))
        .route("/internal/workflows/send-event", post(send_event_handler))
        .route(
            "/internal/workflows/lifecycle/check-delete",
            post(lifecycle_check_delete_handler),
        )
        .route("/internal/workflows/tick", post(tick_handler))
        .route(
            "/internal/workflows/do-alarms/set",
            post(do_alarm_set_handler),
        )
        .route(
            "/internal/workflows/do-alarms/delete",
            post(do_alarm_delete_handler),
        )
        .route(
            "/internal/workflows/do-alarms/cleanup-worker",
            post(do_alarm_cleanup_worker_handler),
        )
        .route("/internal/workflows/claim-step", post(claim_step_handler))
        .route(
            "/internal/workflows/replay-steps",
            post(replay_steps_handler),
        )
        .route(
            "/internal/workflows/commit-step-success",
            post(commit_step_success_handler),
        )
        .route(
            "/internal/workflows/commit-step-error",
            post(commit_step_error_handler),
        )
        .route(
            "/internal/workflows/register-sleep",
            post(register_sleep_handler),
        )
        .route(
            "/internal/workflows/register-wait",
            post(register_wait_handler),
        )
        .layer(middleware::from_fn_with_state(state.clone(), track_request))
        .with_state(state.clone());
    let addr = SocketAddr::from(([0, 0, 0, 0], config.metrics_port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let server_state = state.clone();
    tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app).await {
            log(
                &server_state,
                LogLevel::Error,
                "workflows_server_failed",
                workflow_error_fields(&WorkflowError::internal_error(err.to_string())),
            );
        }
    });

    shutdown_signal().await?;
    state.request_shutdown().await;
    log(
        &state,
        LogLevel::Info,
        "workflows_shutdown",
        json!({ "instance": state.instance_id }),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderValue, Method};
    use serde::ser;

    struct FailingSerialize;

    impl Serialize for FailingSerialize {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: ser::Serializer,
        {
            Err(ser::Error::custom("serialization failed"))
        }
    }

    #[test]
    fn workflow_request_routes_are_bounded_for_metrics() {
        assert_eq!(route_name(&Method::GET, "/_healthz"), "healthz");
        assert_eq!(route_name(&Method::GET, "/_metrics"), "metrics");
        assert_eq!(
            route_name(&Method::POST, "/internal/workflows/commit-step-success"),
            "workflow_commit_step_success"
        );
        assert_eq!(
            route_name(&Method::POST, "/internal/workflows/replay-steps"),
            "workflow_replay_steps"
        );
        assert_eq!(
            route_name(&Method::POST, "/internal/workflows/lifecycle/check-delete"),
            "workflow_check_delete"
        );
        assert_eq!(
            route_name(&Method::GET, "/internal/workflows/tick"),
            "unknown"
        );
    }

    #[test]
    fn random_instance_id_uses_workflows_prefix_and_common_hex_suffix() {
        let id = random_instance_id();
        let Some(suffix) = id.strip_prefix("wf-") else {
            panic!("workflow instance id must use wf- prefix: {id}");
        };
        assert_eq!(suffix.len(), 16);
        assert!(
            suffix.chars().all(|ch| ch.is_ascii_hexdigit())
                && suffix == suffix.to_ascii_lowercase()
        );
    }

    #[test]
    fn workflow_error_response_carries_error_code_for_request_complete() {
        let response = WorkflowError::step_mismatch("step changed").into_response();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(
            response
                .extensions()
                .get::<ResponseError>()
                .map(|err| err.code),
            Some("workflow_step_mismatch")
        );
        assert_eq!(
            response
                .extensions()
                .get::<ResponseError>()
                .map(|err| err.message.as_str()),
            Some("step changed")
        );
    }

    #[test]
    fn workflow_json_serialization_failure_returns_internal_error() {
        let err = workflow_json(FailingSerialize, "workflow test response serializes")
            .expect_err("serialization failure should become WorkflowError");

        assert_eq!(err.code, "internal_error");
        assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(err.message.contains("workflow test response serializes"));
        assert!(err.message.contains("serialization failed"));
    }

    #[test]
    fn request_id_from_headers_rejects_unsafe_values() {
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", HeaderValue::from_static("rid-123"));
        assert_eq!(
            request_id_from_headers(&headers).as_deref(),
            Some("rid-123")
        );

        headers.insert("x-request-id", HeaderValue::from_static("bad id"));
        assert_eq!(request_id_from_headers(&headers), None);

        headers.insert("x-request-id", HeaderValue::from_static(""));
        assert_eq!(request_id_from_headers(&headers), None);
    }
}
