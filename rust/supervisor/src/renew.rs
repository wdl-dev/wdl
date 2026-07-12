use crate::{
    SupervisorConfig, log, renew_error_grace_ms, renew_interval_ms, renew_start_delay_ms,
    renew_timeout_ms, truncate_chars,
};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use wdl_rust_common::internal_auth::{INTERNAL_AUTH_HEADER, internal_auth_token_from_env};

struct State {
    started_at: Instant,
    had_success: bool,
    suppressed_errors: u64,
}

pub(crate) struct Handle {
    stop: Arc<Notify>,
}

impl Handle {
    pub(crate) fn stop(&self) {
        // notify_one stores a permit when no waiter is registered;
        // notify_waiters would lose the signal if Handle::stop raced the
        // loop's between-select gap under a multi-threaded runtime.
        self.stop.notify_one();
    }
}

pub(crate) struct Running {
    pub(crate) handle: Arc<Handle>,
    pub(crate) join: JoinHandle<()>,
}

pub(crate) fn start(
    config: &'static SupervisorConfig,
    client: Arc<reqwest::Client>,
) -> Result<Running, String> {
    let internal_auth_token = internal_auth_token_from_env()?;
    let stop = Arc::new(Notify::new());
    let stop_for_task = Arc::clone(&stop);
    let join = tokio::spawn(async move {
        run_loop(config, client, stop_for_task, internal_auth_token).await;
    });
    Ok(Running {
        handle: Arc::new(Handle { stop }),
        join,
    })
}

async fn run_loop(
    config: &'static SupervisorConfig,
    client: Arc<reqwest::Client>,
    stop: Arc<Notify>,
    internal_auth_token: String,
) {
    let mut state = State {
        started_at: Instant::now(),
        had_success: false,
        suppressed_errors: 0,
    };

    let mut delay = Duration::from_millis(renew_start_delay_ms(config));
    loop {
        tokio::select! {
            biased;
            _ = stop.notified() => return,
            _ = tokio::time::sleep(delay) => {}
        }

        tokio::select! {
            biased;
            _ = stop.notified() => return,
            _ = renew_once(config, &client, internal_auth_token.as_str(), &mut state) => {}
        }

        delay = Duration::from_millis(renew_interval_ms(config));
    }
}

async fn renew_once(
    config: &'static SupervisorConfig,
    client: &reqwest::Client,
    internal_auth_token: &str,
    state: &mut State,
) {
    let timeout = Duration::from_millis(renew_timeout_ms(config));
    let request = client
        .post(config.renew_url)
        .timeout(timeout)
        .header(INTERNAL_AUTH_HEADER, internal_auth_token)
        .header("content-length", "0");
    match request.send().await {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            let body_short = truncate_chars(&body, 500);
            if !(200..300).contains(&status) {
                log_renew_error(
                    config,
                    state,
                    config.renew_failed_event,
                    json!({ "status": status, "body": body_short }),
                );
                return;
            }
            state.had_success = true;
            state.suppressed_errors = 0;
            // Renew payload fields only drive partial-renew warnings. They do not
            // gate lease or shutdown decisions, unlike drain's strict success body.
            let payload: Value = serde_json::from_str(&body).unwrap_or_else(|_| json!({}));
            let lost = payload.get("lost").and_then(Value::as_u64).unwrap_or(0);
            let errors_len = payload
                .get("errors")
                .and_then(Value::as_array)
                .map(|arr| arr.len())
                .unwrap_or(0);
            if lost > 0 || errors_len > 0 {
                log::warn(
                    config.service,
                    config.renew_partial_event,
                    json!({ "body": body_short }),
                );
            }
        }
        Err(err) => {
            log_renew_error(
                config,
                state,
                config.renew_error_event,
                log::reqwest_error_fields(&err),
            );
        }
    }
}

fn log_renew_error(
    config: &SupervisorConfig,
    state: &mut State,
    event: &'static str,
    mut fields: Value,
) {
    let grace = Duration::from_millis(renew_error_grace_ms(config));
    let in_grace = !state.had_success && state.started_at.elapsed() < grace;
    if in_grace {
        state.suppressed_errors += 1;
        return;
    }
    if let Value::Object(ref mut map) = fields {
        map.insert("suppressed_errors".into(), state.suppressed_errors.into());
    }
    state.suppressed_errors = 0;
    log::warn(config.service, event, fields);
}

#[cfg(test)]
mod tests {
    use super::*;
    use wdl_rust_common::test_env::with_temp_env;

    #[test]
    fn start_fails_before_spawn_when_internal_auth_token_is_missing() {
        with_temp_env("WDL_INTERNAL_AUTH_TOKEN", None, || {
            let client = Arc::new(reqwest::Client::new());
            match start(&crate::D1_CONFIG, client) {
                Ok(_) => panic!("start must reject missing token"),
                Err(err) => assert!(err.contains("WDL_INTERNAL_AUTH_TOKEN must be configured")),
            }
        });
    }
}
