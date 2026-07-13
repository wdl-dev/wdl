use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::extract::State;
use axum::http::header::CONTENT_LENGTH;
use axum::http::{HeaderValue, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use serde_json::{Value, json};
use wdl_rust_common::env::env_u16;
use wdl_rust_common::health::healthcheck_http_200;
use wdl_rust_common::internal_auth::{
    INTERNAL_AUTH_FAILURE_CODE, INTERNAL_AUTH_FAILURE_MESSAGE, InternalAuthTokens,
    internal_auth_failure_response, internal_auth_headers_match, internal_auth_tokens_from_env,
};
use wdl_rust_common::metrics::prometheus_response;
use wdl_rust_common::redis_conn::RedisConnection;
use wdl_rust_common::request_id::request_id_from_headers;
use wdl_rust_common::time::duration_ms_for_log;

// CF KV allows 25 MiB values; set Axum's body cap above that instead of
// inheriting its 2 MiB default for Bytes / Json extractors.
pub(crate) const MAX_KV_VALUE_BYTES: usize = 26 * 1024 * 1024;
pub(crate) const SERVICE: &str = "redis-proxy";

#[derive(Clone)]
pub(crate) struct AppState {
    control_redis: RedisConnection,
    data_redis: RedisConnection,
    metrics: Arc<Metrics>,
    secret_decryptor: secrets::SecretEnvelopeDecryptor,
    internal_auth_tokens: Arc<InternalAuthTokens>,
}

#[derive(Debug)]
pub(crate) struct AppError {
    pub(crate) status: StatusCode,
    pub(crate) code: &'static str,
    pub(crate) message: String,
}

pub(crate) type AppResult<T> = Result<T, AppError>;

#[cfg(test)]
pub(crate) mod test_support {
    fn read_resp_usize(packed: &[u8], offset: &mut usize) -> usize {
        let start = *offset;
        while &packed[*offset..*offset + 2] != b"\r\n" {
            *offset += 1;
        }
        let value = std::str::from_utf8(&packed[start..*offset])
            .unwrap()
            .parse::<usize>()
            .unwrap();
        *offset += 2;
        value
    }

    pub(crate) fn parse_packed_commands(packed: &[u8]) -> Vec<Vec<String>> {
        let mut offset = 0_usize;
        let mut commands = Vec::new();
        while offset < packed.len() {
            assert_eq!(packed[offset], b'*');
            offset += 1;
            let count = read_resp_usize(packed, &mut offset);
            let mut command = Vec::new();
            for _ in 0..count {
                assert_eq!(packed[offset], b'$');
                offset += 1;
                let len = read_resp_usize(packed, &mut offset);
                let end = offset + len;
                command.push(String::from_utf8(packed[offset..end].to_vec()).unwrap());
                offset = end;
                assert_eq!(&packed[offset..offset + 2], b"\r\n");
                offset += 2;
            }
            commands.push(command);
        }
        commands
    }
}

impl From<redis::RedisError> for AppError {
    fn from(err: redis::RedisError) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "redis_error",
            message: err.to_string(),
        }
    }
}

// No blanket From<serde_json::Error>: each JSON site must choose whether the
// failure came from user input (400) or internal persisted/generated data (500).
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = self.status;
        let code = self.code;
        let message = self.message;
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

impl AppError {
    pub(crate) fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_request",
            message: msg.into(),
        }
    }

    pub(crate) fn payload_too_large(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            code: "response_too_large",
            message: msg.into(),
        }
    }

    pub(crate) fn internal_error(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "internal_error",
            message: msg.into(),
        }
    }

    pub(crate) fn internal_json(err: serde_json::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "internal_json_error",
            message: err.to_string(),
        }
    }
}

impl AppState {
    pub(crate) async fn with_control_redis<T, F, Fut>(&self, f: F) -> Result<T, redis::RedisError>
    where
        F: FnOnce(redis::aio::ConnectionManager) -> Fut,
        Fut: std::future::Future<Output = Result<T, redis::RedisError>>,
    {
        self.control_redis.with_conn(f).await
    }

    // ConnectionManager multiplexes over one socket; clone is a cheap Arc bump.
    // The sidecar's ordinary KV/Queue/Logs routes are data-plane routes; the
    // runtime-load route opts into with_control_redis() explicitly.
    pub(crate) async fn with_redis<T, F, Fut>(&self, f: F) -> Result<T, redis::RedisError>
    where
        F: FnOnce(redis::aio::ConnectionManager) -> Fut,
        Fut: std::future::Future<Output = Result<T, redis::RedisError>>,
    {
        self.data_redis.with_conn(f).await
    }

    pub(crate) fn redis(&self) -> redis::aio::ConnectionManager {
        self.data_redis.clone_manager()
    }

    pub(crate) fn secret_decryptor(&self) -> &secrets::SecretEnvelopeDecryptor {
        &self.secret_decryptor
    }

    pub(crate) fn metrics(&self) -> &Metrics {
        self.metrics.as_ref()
    }
}

pub(crate) fn empty(status: StatusCode) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_LENGTH, HeaderValue::from_static("0"));
    response
}

async fn healthz() -> Json<Value> {
    Json(json!({ "ok": true }))
}

async fn metrics_handler(State(state): State<AppState>) -> Response {
    prometheus_response(&state.metrics)
}

fn route_name(method: &axum::http::Method, path: &str) -> &'static str {
    match (method.as_str(), path) {
        ("GET", "/_healthz") => "healthz",
        ("GET", "/_metrics") => "metrics",
        ("GET", "/runtime/load") => "runtime_load",
        ("GET", "/kv/get") => "kv_get",
        ("GET", "/kv/get-with-metadata") => "kv_get_with_metadata",
        ("POST", "/kv/get-batch") => "kv_get_batch",
        ("PUT", "/kv/put") => "kv_put",
        ("DELETE", "/kv/delete") => "kv_delete",
        ("GET", "/kv/list") => "kv_list",
        ("POST", "/queue/send") => "queue_send",
        ("GET", "/logs/tail/active") => "logs_tail_active",
        ("POST", "/logs/tail/append") => "logs_tail_append",
        _ => "unknown",
    }
}

fn record_request_complete(
    state: &AppState,
    method: &str,
    route: &'static str,
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
        observability::log_event(
            if status.is_server_error() {
                observability::LogLevel::Error
            } else {
                observability::LogLevel::Info
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
        && !internal_auth_headers_match(request.headers(), &state.internal_auth_tokens)
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

pub fn healthcheck() -> i32 {
    healthcheck_http_200("REDIS_PROXY_PORT", 7070, "/_healthz")
}

mod kv;
mod logs;
mod observability;
mod queue;
mod runtime;
mod secrets;

use kv::{kv_delete, kv_get, kv_get_batch, kv_get_with_metadata, kv_list, kv_put};
use logs::{logs_tail_active, logs_tail_append};
use observability::{Metrics, log_info, started_log};
use queue::queue_send;
use runtime::runtime_load;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let port = env_u16("REDIS_PROXY_PORT", 7070);
    let redis_configured = env::var("REDIS_URL")
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);
    let data_redis_configured = env::var("DATA_REDIS_URL")
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let data_redis_url = env::var("DATA_REDIS_URL").unwrap_or_else(|_| redis_url.clone());
    let control_client = redis::Client::open(redis_url.as_str())?;
    let data_client = redis::Client::open(data_redis_url.as_str())?;
    let control_conn = control_client.get_connection_manager().await?;
    let data_conn = data_client.get_connection_manager().await?;
    let metrics = Arc::new(Metrics::default());
    let state = AppState {
        control_redis: RedisConnection::new(control_conn),
        data_redis: RedisConnection::new(data_conn),
        metrics: metrics.clone(),
        secret_decryptor: secrets::SecretEnvelopeDecryptor::from_env(metrics)
            .map_err(|err| std::io::Error::other(format!("{}: {}", err.code, err.message)))?,
        internal_auth_tokens: Arc::new(
            internal_auth_tokens_from_env().map_err(std::io::Error::other)?,
        ),
    };

    let app = Router::new()
        .route("/_healthz", get(healthz))
        .route("/_metrics", get(metrics_handler))
        .route("/runtime/load", get(runtime_load))
        .route("/kv/get", get(kv_get))
        .route("/kv/get-with-metadata", get(kv_get_with_metadata))
        .route("/kv/get-batch", post(kv_get_batch))
        .route("/kv/put", put(kv_put))
        .route("/kv/delete", delete(kv_delete))
        .route("/kv/list", get(kv_list))
        .route("/queue/send", post(queue_send))
        .route("/logs/tail/active", get(logs_tail_active))
        .route("/logs/tail/append", post(logs_tail_append))
        .layer(DefaultBodyLimit::max(MAX_KV_VALUE_BYTES))
        .layer(middleware::from_fn_with_state(state.clone(), track_request))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    log_info(
        "started",
        started_log(port, redis_configured, data_redis_configured),
    );
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use axum::http::HeaderMap;
    use axum::response::IntoResponse;
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn app_error_response_uses_machine_error_and_human_message() {
        let response = AppError::bad_request("queue action missing entry").into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response
                .extensions()
                .get::<ResponseError>()
                .map(|err| err.code),
            Some("invalid_request")
        );
        assert_eq!(
            response
                .extensions()
                .get::<ResponseError>()
                .map(|err| err.message.as_str()),
            Some("queue action missing entry")
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            json!({
                "error": "invalid_request",
                "message": "queue action missing entry",
            })
        );
    }

    #[tokio::test]
    async fn app_error_internal_json_is_500_machine_error() {
        let json_err = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
        let response = AppError::internal_json(json_err).into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let parsed = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
        assert_eq!(parsed["error"], "internal_json_error");
        assert!(parsed["message"].as_str().unwrap().contains("EOF"));
    }

    #[test]
    fn kv_body_limit_covers_cf_value_size() {
        const _: () = assert!(MAX_KV_VALUE_BYTES > 25 * 1024 * 1024);
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

    #[test]
    fn route_name_uses_low_cardinality_labels() {
        use axum::http::Method;

        assert_eq!(route_name(&Method::GET, "/_metrics"), "metrics");
        assert_eq!(route_name(&Method::POST, "/queue/send"), "queue_send");
        assert_eq!(
            route_name(&Method::GET, "/kv/tenant-specific-key"),
            "unknown"
        );
    }
}
