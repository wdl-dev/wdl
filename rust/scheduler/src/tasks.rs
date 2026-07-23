use std::panic::AssertUnwindSafe;
use std::time::Duration;

use futures_util::FutureExt;
use serde_json::json;
use tokio::time::{Instant, MissedTickBehavior, interval_at, sleep};

use crate::cron::{sweep, tick, wait_ms_until_next_slot};
use crate::queue::{
    queue_consume_loop, queue_delayed_dispatch_loop, queue_delayed_wake_loop, queue_index_repair,
    queue_pel_reap, queue_reconcile,
};
use crate::{
    AppState, LogLevel, SchedulerResult, error_fields, fields_with_error, log, now_ms,
    panic_payload_message, scheduler_error_fields, workflows_tick,
};

fn spawn_periodic<F, Fut>(
    state: AppState,
    failure_event: &'static str,
    ms: u64,
    delay_first: bool,
    mut f: F,
) where
    F: FnMut(AppState) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = SchedulerResult<()>> + Send + 'static,
{
    tokio::spawn(async move {
        let period = Duration::from_millis(ms);
        let start = periodic_start(Instant::now(), period, delay_first);
        let mut ticker = interval_at(start, period);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = state.shutdown.stop_notified() => break,
            }
            if state.is_shutting_down() {
                break;
            }
            let Some(guard) = state.begin_in_flight() else {
                break;
            };
            let _guard = guard;
            match AssertUnwindSafe(f(state.clone())).catch_unwind().await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    log(
                        &state,
                        LogLevel::Error,
                        failure_event,
                        scheduler_error_fields(&err),
                    );
                }
                Err(err) => {
                    log(
                        &state,
                        LogLevel::Error,
                        failure_event,
                        error_fields("Panic", panic_payload_message(err.as_ref())),
                    );
                }
            }
        }
    });
}

fn periodic_start(now: Instant, period: Duration, delay_first: bool) -> Instant {
    if delay_first { now + period } else { now }
}

fn spawn_workflows_tick_loop(state: AppState) {
    let idle_ms = state.config.workflows_tick_interval_ms;
    let active_ms = state.config.workflows_tick_active_interval_ms;
    tokio::spawn(async move {
        loop {
            if state.is_shutting_down() {
                break;
            }
            let active = {
                let Some(guard) = state.begin_in_flight() else {
                    break;
                };
                let _guard = guard;
                match AssertUnwindSafe(workflows_tick(state.clone()))
                    .catch_unwind()
                    .await
                {
                    Ok(Ok(summary)) => summary.needs_active_poll(),
                    Ok(Err(err)) => {
                        log(
                            &state,
                            LogLevel::Error,
                            "workflows_tick_failed",
                            scheduler_error_fields(&err),
                        );
                        false
                    }
                    Err(err) => {
                        log(
                            &state,
                            LogLevel::Error,
                            "workflows_tick_failed",
                            error_fields("Panic", panic_payload_message(err.as_ref())),
                        );
                        false
                    }
                }
            };
            let delay_ms = if active { active_ms } else { idle_ms };
            tokio::select! {
                _ = sleep(Duration::from_millis(delay_ms)) => {}
                _ = state.shutdown.stop_notified() => break,
            }
        }
    });
}

fn spawn_restart_loop<F, Fut>(
    state: AppState,
    failure_event: &'static str,
    hold_guard: bool,
    mut loop_fn: F,
) where
    F: FnMut(AppState) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = SchedulerResult<()>> + Send + 'static,
{
    tokio::spawn(async move {
        while !state.is_shutting_down() {
            let guard = if hold_guard {
                let Some(g) = state.begin_in_flight() else {
                    break;
                };
                Some(g)
            } else {
                None
            };
            let run_state = state.clone();
            let fut = loop_fn(run_state);
            let result = tokio::spawn(async move {
                let _guard = guard;
                fut.await
            })
            .await;
            match result {
                Ok(Ok(())) => break,
                Ok(Err(err)) => {
                    log(
                        &state,
                        LogLevel::Error,
                        failure_event,
                        restart_error_fields(&err),
                    );
                }
                Err(err) => {
                    log(
                        &state,
                        LogLevel::Error,
                        failure_event,
                        restart_join_error_fields(&err),
                    );
                }
            }
            if !state.is_shutting_down() {
                tokio::time::sleep(Duration::from_millis(1000)).await;
            }
        }
    });
}

fn restart_error_fields(err: &crate::SchedulerError) -> serde_json::Value {
    fields_with_error(json!({ "will_restart": true }), "Error", &err.message)
}

fn restart_join_error_fields(err: &tokio::task::JoinError) -> serde_json::Value {
    fields_with_error(
        json!({ "will_restart": true }),
        if err.is_panic() { "Panic" } else { "JoinError" },
        err.to_string(),
    )
}

fn spawn_queue_consumer(state: AppState) {
    spawn_restart_loop(state, "queue_consume_loop_fatal", true, queue_consume_loop);
}

fn spawn_cron_dispatch_loop(state: AppState) {
    // Cron dispatch runs one bounded tick per minute slot, so errors can be
    // logged inline before sleeping to the next slot. Queue dispatchers own
    // long-lived blocking loops and therefore use restart wrappers instead.
    tokio::spawn(async move {
        loop {
            if state.is_shutting_down() {
                break;
            }
            let Some(guard) = state.begin_in_flight() else {
                break;
            };
            let _guard = guard;
            match AssertUnwindSafe(tick(state.clone())).catch_unwind().await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    log(
                        &state,
                        LogLevel::Error,
                        "cron_dispatch_failed",
                        scheduler_error_fields(&err),
                    );
                }
                Err(err) => {
                    log(
                        &state,
                        LogLevel::Error,
                        "cron_dispatch_failed",
                        error_fields("Panic", panic_payload_message(err.as_ref())),
                    );
                }
            }
            drop(_guard);
            tokio::select! {
                _ = sleep(Duration::from_millis(wait_ms_until_next_slot(now_ms()))) => {}
                _ = state.shutdown.stop_notified() => break,
            }
        }
    });
}

fn spawn_queue_delayed_dispatcher(state: AppState) {
    spawn_restart_loop(
        state,
        "queue_delayed_dispatch_loop_fatal",
        true,
        queue_delayed_dispatch_loop,
    );
}

fn spawn_queue_delayed_wake_listener(state: AppState) {
    spawn_restart_loop(
        state,
        "queue_delayed_wake_loop_failed",
        false,
        queue_delayed_wake_loop,
    );
}

pub(crate) async fn run_startup_reconciliation(state: AppState) {
    if let Err(err) = sweep(state.clone()).await {
        log(
            &state,
            LogLevel::Error,
            "startup_sweep_failed",
            scheduler_error_fields(&err),
        );
    }
    if let Err(err) = queue_index_repair(state.clone()).await {
        log(
            &state,
            LogLevel::Error,
            "startup_queue_index_repair_failed",
            scheduler_error_fields(&err),
        );
    }
    if let Err(err) = queue_reconcile(state.clone()).await {
        log(
            &state,
            LogLevel::Error,
            "startup_queue_reconcile_failed",
            scheduler_error_fields(&err),
        );
    }
}

pub(crate) fn spawn_background_tasks(state: AppState) {
    spawn_cron_dispatch_loop(state.clone());
    spawn_periodic(
        state.clone(),
        "sweep_failed",
        state.config.sweep_ms,
        false,
        sweep,
    );
    spawn_periodic(
        state.clone(),
        "queue_index_repair_failed",
        state.config.sweep_ms,
        true,
        queue_index_repair,
    );
    spawn_periodic(
        state.clone(),
        "queue_reconcile_failed",
        state.config.queue_reconcile_ms,
        false,
        queue_reconcile,
    );
    spawn_queue_delayed_wake_listener(state.clone());
    spawn_queue_delayed_dispatcher(state.clone());
    if state.config.workflows_host.is_some() {
        spawn_workflows_tick_loop(state.clone());
    }
    spawn_periodic(
        state.clone(),
        "periodic_queue_pel_reap_failed",
        state.config.queue_pel_reap_ms,
        false,
        queue_pel_reap,
    );
    spawn_queue_consumer(state);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delayed_periodic_work_waits_one_full_period() {
        let now = Instant::now();
        let period = Duration::from_secs(300);

        assert_eq!(periodic_start(now, period, false), now);
        assert_eq!(periodic_start(now, period, true), now + period);
    }

    #[test]
    fn restart_error_fields_mark_business_errors_as_restartable() {
        let err = crate::SchedulerError::internal_error("queue loop failed");

        assert_eq!(
            restart_error_fields(&err),
            json!({
                "will_restart": true,
                "error_name": "Error",
                "error_message": "queue loop failed",
            })
        );
    }

    #[tokio::test]
    async fn restart_join_error_fields_classify_panics_for_restart_logs() {
        let join = tokio::spawn(async {
            panic!("queue loop panic");
        })
        .await
        .expect_err("task should panic");

        let fields = restart_join_error_fields(&join);

        assert_eq!(fields["will_restart"], true);
        assert_eq!(fields["error_name"], "Panic");
        assert!(
            fields["error_message"]
                .as_str()
                .expect("error message")
                .contains("queue loop panic")
        );
    }
}
