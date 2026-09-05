use crate::{
    KillSignal, SupervisorConfig, drain, log, renew, shutdown_timeout_ms, signal_exit_code,
    validate_shutdown_timing,
};
use serde_json::json;
use std::future::pending;
use std::os::unix::process::ExitStatusExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::time::Duration;
use tokio::process::Command;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::Notify;
use tokio::task::{JoinError, JoinHandle};

const EXPECTED_EXIT_UNSET: i32 = -1;

// Atomic flag for sync `is_exited()` checks before libc::kill (avoids
// signaling a recycled PID); Notify for async watchdog wake-up.
struct ChildExitedSignal {
    flag: AtomicBool,
    notify: Notify,
}

impl ChildExitedSignal {
    fn new() -> Self {
        Self {
            flag: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    fn set(&self) {
        self.flag.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    fn is_exited(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }
}

struct State {
    config: &'static SupervisorConfig,
    child_pid: libc::pid_t,
    shutdown_started: AtomicBool,
    expected_exit_code: AtomicI32,
    child_exited: Arc<ChildExitedSignal>,
    client: Arc<reqwest::Client>,
    renew: Arc<renew::Handle>,
}

pub(crate) async fn run(
    config: &'static SupervisorConfig,
    program: &'static str,
    args: Vec<String>,
) -> ! {
    validate_shutdown_timing(config);

    let client = Arc::new(
        reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("reqwest client must build"),
    );

    let renew_running = match renew::start(config, Arc::clone(&client)) {
        Ok(running) => running,
        Err(err) => {
            log::error(
                config.service,
                "internal_auth_token_config_error",
                json!({ "error_message": err }),
            );
            std::process::exit(1);
        }
    };
    let mut renew_join = Some(renew_running.join);

    let mut child = match Command::new(program)
        .args(&args)
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .spawn()
    {
        Ok(child) => child,
        Err(err) => {
            // Surface raw_os_error so callers can distinguish missing-binary
            // (ENOENT) / permission-denied (EACCES) / cgroup limits.
            let mut fields = json!({ "error_message": err.to_string() });
            if let Some(code) = err.raw_os_error() {
                fields["error_code"] = json!(format!("errno_{code}"));
            }
            log::error(config.service, "workerd_start_error", fields);
            std::process::exit(1);
        }
    };
    let child_pid: libc::pid_t = child
        .id()
        .expect("child pid available immediately")
        .cast_signed();

    let state = Arc::new(State {
        config,
        child_pid,
        shutdown_started: AtomicBool::new(false),
        expected_exit_code: AtomicI32::new(EXPECTED_EXIT_UNSET),
        child_exited: Arc::new(ChildExitedSignal::new()),
        client,
        renew: renew_running.handle,
    });

    let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");

    loop {
        tokio::select! {
            biased;
            _ = sigterm.recv() => {
                handle_signal(Arc::clone(&state), "SIGTERM");
            }
            _ = sigint.recv() => {
                handle_signal(Arc::clone(&state), "SIGINT");
            }
            res = child.wait() => {
                state.child_exited.set();
                stop_renew(&state, &mut renew_join).await;
                let code = compute_exit_code(res, state.expected_exit_code.load(Ordering::SeqCst));
                std::process::exit(code);
            }
            res = await_renew_exit(&mut renew_join) => {
                renew_join = None;
                handle_renew_exit(Arc::clone(&state), res);
            }
        }
    }
}

async fn await_renew_exit(join: &mut Option<JoinHandle<()>>) -> Result<(), JoinError> {
    match join {
        Some(handle) => handle.await,
        None => pending().await,
    }
}

async fn stop_renew(state: &State, join: &mut Option<JoinHandle<()>>) {
    state.renew.stop();
    if let Some(handle) = join.take() {
        let _ = handle.await;
    }
}

fn handle_renew_exit(state: Arc<State>, result: Result<(), JoinError>) {
    if state.shutdown_started.swap(true, Ordering::SeqCst) || state.child_exited.is_exited() {
        return;
    }
    let stop_timeout_ms = shutdown_timeout_ms(state.config);
    let mut fields = match result {
        Ok(()) => json!({ "reason": "returned" }),
        Err(err) => json!({
            "reason": if err.is_panic() { "panic" } else { "cancelled" },
            "error_message": err.to_string(),
        }),
    };
    if let Some(obj) = fields.as_object_mut() {
        obj.insert("action".into(), json!("terminate_child"));
        obj.insert("timeout_ms".into(), json!(stop_timeout_ms));
    }
    log::error(state.config.service, "renew_task_exited", fields);
    state.expected_exit_code.store(1, Ordering::SeqCst);
    if state.child_exited.is_exited() {
        return;
    }
    spawn_stop_watchdog(Arc::clone(&state), stop_timeout_ms);
    // SAFETY: child_pid is the direct child process spawned by this supervisor; the
    // exited flag is checked immediately before signaling to avoid PID reuse.
    unsafe { libc::kill(state.child_pid, libc::SIGTERM) };
}

fn compute_exit_code(res: std::io::Result<std::process::ExitStatus>, expected: i32) -> i32 {
    if expected != EXPECTED_EXIT_UNSET {
        return expected;
    }
    match res {
        Ok(status) => {
            if let Some(code) = status.code() {
                code
            } else if let Some(sig) = status.signal() {
                signal_exit_code(sig)
            } else {
                1
            }
        }
        Err(_) => 1,
    }
}

fn handle_signal(state: Arc<State>, signal_name: &'static str) {
    let already = state.shutdown_started.swap(true, Ordering::SeqCst);
    if already {
        let mut fields = json!({ "signal": signal_name });
        if state.config.repeated_signal_escalates && !state.child_exited.is_exited() {
            fields["action"] = json!("kill_child");
            log::warn(state.config.service, "shutdown_signal_repeated", fields);
            // SAFETY: child_pid is our direct child and child_exited.is_exited()
            // is false here, so the PID has not been recycled.
            unsafe { libc::kill(state.child_pid, libc::SIGKILL) };
        } else {
            log::warn(state.config.service, "shutdown_signal_repeated", fields);
        }
        return;
    }

    tokio::spawn(async move {
        do_shutdown(state, signal_name).await;
    });
}

async fn do_shutdown(state: Arc<State>, signal_name: &'static str) {
    log::info(
        state.config.service,
        "shutdown_started",
        json!({ "signal": signal_name }),
    );

    let stop_timeout_ms = shutdown_timeout_ms(state.config);
    spawn_stop_watchdog(Arc::clone(&state), stop_timeout_ms);

    state.renew.stop();
    let drained = drain::drain(state.config, &state.client, signal_name).await;
    if !drained {
        log::warn(
            state.config.service,
            state.config.shutdown_after_drain_failure_event,
            json!({ "signal": signal_name }),
        );
    }

    let final_signal = if drained {
        match state.config.kill_on_drain_success {
            KillSignal::Term => libc::SIGTERM,
            KillSignal::Kill => {
                state.expected_exit_code.store(0, Ordering::SeqCst);
                libc::SIGKILL
            }
        }
    } else {
        libc::SIGTERM
    };

    if state.child_exited.is_exited() {
        return;
    }
    // SAFETY: child_pid is the direct child process spawned by this supervisor; the
    // exited flag is checked immediately before signaling to avoid PID reuse.
    unsafe { libc::kill(state.child_pid, final_signal) };
}

fn spawn_stop_watchdog(state: Arc<State>, stop_timeout_ms: u64) {
    let child_exited = Arc::clone(&state.child_exited);
    let pid = state.child_pid;
    let service = state.config.service;
    tokio::spawn(async move {
        let notified = child_exited.notify.notified();
        tokio::pin!(notified);
        tokio::select! {
            biased;
            _ = &mut notified => {}
            _ = tokio::time::sleep(Duration::from_millis(stop_timeout_ms)) => {
                if child_exited.is_exited() {
                    return;
                }
                log::error(
                    service,
                    "workerd_stop_timeout",
                    json!({ "timeout_ms": stop_timeout_ms }),
                );
                // SAFETY: pid is the child process captured when the watchdog was
                // spawned; child_exited is checked immediately before signaling.
                unsafe { libc::kill(pid, libc::SIGKILL) };
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;

    #[test]
    fn compute_exit_code_preserves_child_status_and_signal_convention() {
        assert_eq!(
            compute_exit_code(
                Ok(std::process::ExitStatus::from_raw(7 << 8)),
                EXPECTED_EXIT_UNSET
            ),
            7
        );
        assert_eq!(
            compute_exit_code(
                Ok(std::process::ExitStatus::from_raw(libc::SIGTERM)),
                EXPECTED_EXIT_UNSET
            ),
            143
        );
        assert_eq!(
            compute_exit_code(
                Ok(std::process::ExitStatus::from_raw(libc::SIGINT)),
                EXPECTED_EXIT_UNSET
            ),
            130
        );
    }

    #[test]
    fn compute_exit_code_prefers_expected_exit_override() {
        assert_eq!(
            compute_exit_code(Ok(std::process::ExitStatus::from_raw(9 << 8)), 0),
            0
        );
        assert_eq!(
            compute_exit_code(Err(std::io::Error::other("wait failed")), 1),
            1
        );
    }

    #[tokio::test]
    async fn child_exited_signal_sets_sync_flag_and_wakes_waiters() {
        let signal = ChildExitedSignal::new();
        let notified = signal.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        assert!(!signal.is_exited());
        signal.set();

        assert!(signal.is_exited());
        tokio::time::timeout(Duration::from_millis(100), notified)
            .await
            .expect("waiter should be notified");
    }
}
