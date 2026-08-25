use std::time::Duration;

use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::response_body::read_bounded_response_text;
use crate::{AppState, Config};
use wdl_rust_common::internal_auth::INTERNAL_AUTH_HEADER;

const MAX_RUNTIME_RESPONSE_BYTES: usize = 256 * 1024;

pub(crate) struct RuntimeResponse {
    pub(crate) status: Option<u16>,
    pub(crate) json: Option<JsonValue>,
    pub(crate) text: Option<String>,
    pub(crate) error: Option<String>,
}

pub(crate) fn runtime_outcome_label(res: &RuntimeResponse) -> &'static str {
    if res.error.is_none()
        && res
            .status
            .is_some_and(|status| (200..300).contains(&status))
        && res
            .json
            .as_ref()
            .and_then(|v| v.get("outcome"))
            .and_then(JsonValue::as_str)
            == Some("ok")
    {
        "ok"
    } else {
        "error"
    }
}

fn pick_runtime(config: &Config, worker_id: &str) -> (String, u16) {
    if worker_id.starts_with("__system__:") {
        (
            config.system_runtime_host.clone(),
            config.system_runtime_port,
        )
    } else {
        (config.runtime_host.clone(), config.runtime_port)
    }
}

pub(crate) async fn post_runtime<T>(
    state: &AppState,
    path: &str,
    body: &T,
    worker_id: &str,
    request_id: &str,
) -> RuntimeResponse
where
    T: Serialize + ?Sized,
{
    let (host, port) = pick_runtime(&state.config, worker_id);
    let url = format!("http://{host}:{port}{path}");
    let result = state
        .http
        .post(url)
        .header("content-type", "application/json")
        .header(
            INTERNAL_AUTH_HEADER,
            state.config.internal_auth_token.as_str(),
        )
        .header("x-worker-id", worker_id)
        .header("x-request-id", request_id)
        .timeout(Duration::from_millis(state.config.fire_timeout_ms))
        .json(body)
        .send()
        .await;
    let Ok(response) = result else {
        return RuntimeResponse {
            status: None,
            json: None,
            text: None,
            error: Some(result.unwrap_err().to_string()),
        };
    };
    let status = response.status().as_u16();
    let text = match read_bounded_response_text(
        response,
        MAX_RUNTIME_RESPONSE_BYTES,
        "runtime response body",
    )
    .await
    {
        Ok(text) => text,
        Err(error) => {
            return RuntimeResponse {
                status: Some(status),
                json: None,
                text: None,
                error: Some(error),
            };
        }
    };
    let parsed = serde_json::from_str::<JsonValue>(&text).ok();
    RuntimeResponse {
        status: Some(status),
        json: parsed,
        text: Some(text),
        error: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::{Read, Write};

    async fn raw_http_response(
        response: Vec<u8>,
    ) -> (reqwest::Response, std::thread::JoinHandle<()>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request).unwrap();
            let _ = stream.write_all(&response);
        });
        let response = reqwest::Client::new()
            .get(format!("http://{addr}/runtime"))
            .send()
            .await
            .unwrap();
        (response, server)
    }

    #[test]
    fn runtime_outcome_label_is_bounded_to_ok_or_error() {
        assert_eq!(
            runtime_outcome_label(&RuntimeResponse {
                status: Some(200),
                json: Some(json!({ "outcome": "ok" })),
                text: None,
                error: None,
            }),
            "ok"
        );
        assert_eq!(
            runtime_outcome_label(&RuntimeResponse {
                status: Some(500),
                json: Some(json!({ "outcome": "ok" })),
                text: None,
                error: None,
            }),
            "error"
        );
        assert_eq!(
            runtime_outcome_label(&RuntimeResponse {
                status: Some(200),
                json: Some(json!({ "outcome": "weird" })),
                text: None,
                error: None,
            }),
            "error"
        );
        assert_eq!(
            runtime_outcome_label(&RuntimeResponse {
                status: None,
                json: None,
                text: None,
                error: Some("ECONNREFUSED".to_string()),
            }),
            "error"
        );
    }

    #[tokio::test]
    async fn runtime_response_reader_rejects_oversized_content_length() {
        let (response, server) = raw_http_response(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                MAX_RUNTIME_RESPONSE_BYTES + 1
            )
            .into_bytes(),
        )
        .await;

        let error = read_bounded_response_text(
            response,
            MAX_RUNTIME_RESPONSE_BYTES,
            "runtime response body",
        )
        .await
        .unwrap_err();
        server.join().unwrap();

        assert_eq!(
            error,
            format!("runtime response body exceeds {MAX_RUNTIME_RESPONSE_BYTES} bytes")
        );
    }

    #[tokio::test]
    async fn runtime_response_reader_bounds_chunked_bodies_without_content_length() {
        let body = vec![b'x'; MAX_RUNTIME_RESPONSE_BYTES + 1];
        let mut response = format!(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n",
            body.len()
        )
        .into_bytes();
        response.extend_from_slice(&body);
        response.extend_from_slice(b"\r\n0\r\n\r\n");
        let (response, server) = raw_http_response(response).await;

        let error = read_bounded_response_text(
            response,
            MAX_RUNTIME_RESPONSE_BYTES,
            "runtime response body",
        )
        .await
        .unwrap_err();
        server.join().unwrap();

        assert_eq!(
            error,
            format!("runtime response body exceeds {MAX_RUNTIME_RESPONSE_BYTES} bytes")
        );
    }
}
