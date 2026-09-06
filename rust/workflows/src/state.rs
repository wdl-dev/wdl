use std::collections::{HashMap, VecDeque};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use futures_util::FutureExt;
use serde_json::Value as JsonValue;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use wdl_rust_common::redis_conn::RedisConnection;
pub(crate) use wdl_rust_common::shutdown::{InFlightGuard, ShutdownState};

use crate::{Config, LogLevel, Metrics, fields_with_error, log};

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) redis: Redis,
    pub(crate) control_redis: Redis,
    pub(crate) http: reqwest::Client,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) shutdown: Arc<ShutdownState>,
    pub(crate) dispatch: Arc<DispatchSemaphores>,
    pub(crate) progress_callback_lookups: Arc<Semaphore>,
    pub(crate) progress_callbacks: Arc<Semaphore>,
    pub(crate) progress_callback_cache: Arc<Mutex<ProgressCallbackCache>>,
    pub(crate) config: Arc<Config>,
    pub(crate) instance_id: String,
    pub(crate) run_claim_counter: Arc<AtomicU64>,
}

pub(crate) struct DispatchSemaphores {
    pub(crate) workflow: Arc<Semaphore>,
    pub(crate) do_alarm: Arc<Semaphore>,
}

pub(crate) struct DispatchTaskGuard {
    _in_flight: InFlightGuard,
    _permit: OwnedSemaphorePermit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DispatchTaskUnavailable {
    Stopping,
    AtCapacity,
}

impl DispatchTaskGuard {
    fn try_begin(
        shutdown: &Arc<ShutdownState>,
        permits: &Arc<Semaphore>,
    ) -> Result<Self, DispatchTaskUnavailable> {
        let in_flight = shutdown
            .begin_in_flight()
            .ok_or(DispatchTaskUnavailable::Stopping)?;
        let permit = permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| DispatchTaskUnavailable::AtCapacity)?;
        Ok(Self {
            _in_flight: in_flight,
            _permit: permit,
        })
    }
}

impl DispatchSemaphores {
    pub(crate) fn new(workflow_limit: usize, do_alarm_limit: usize) -> Self {
        Self {
            workflow: Arc::new(Semaphore::new(workflow_limit)),
            do_alarm: Arc::new(Semaphore::new(do_alarm_limit)),
        }
    }
}

#[derive(Default)]
pub(crate) struct ProgressCallbackCache {
    entries: HashMap<String, Option<String>>,
    order: VecDeque<String>,
}

impl ProgressCallbackCache {
    pub(crate) fn get(&self, key: &str) -> Option<Option<String>> {
        self.entries.get(key).cloned()
    }

    pub(crate) fn insert(&mut self, key: String, value: Option<String>, limit: usize) {
        if !self.entries.contains_key(&key) {
            while self.entries.len() >= limit {
                let Some(oldest) = self.order.pop_front() else {
                    self.entries.clear();
                    break;
                };
                self.entries.remove(&oldest);
            }
            self.order.push_back(key.clone());
        }
        self.entries.insert(key, value);
    }
}

pub(crate) type Redis = RedisConnection;

impl AppState {
    pub(crate) fn next_run_claim_sequence(&self) -> u64 {
        self.run_claim_counter.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn begin_in_flight(&self) -> Option<InFlightGuard> {
        self.shutdown.begin_in_flight()
    }

    pub(crate) fn begin_dispatch_task(
        &self,
        permits: &Arc<Semaphore>,
    ) -> Result<DispatchTaskGuard, DispatchTaskUnavailable> {
        DispatchTaskGuard::try_begin(&self.shutdown, permits)
    }

    pub(crate) fn spawn_tracked<F>(
        &self,
        guard: DispatchTaskGuard,
        panic_event: &'static str,
        panic_fields: JsonValue,
        future: F,
    ) where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let state = self.clone();
        tokio::spawn(async move {
            let _guard = guard;
            if let Err(err) = AssertUnwindSafe(future).catch_unwind().await {
                let message = err
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| err.downcast_ref::<&str>().map(|value| (*value).to_string()))
                    .unwrap_or_else(|| "background task panicked".to_string());
                log(
                    &state,
                    LogLevel::Error,
                    panic_event,
                    fields_with_error(panic_fields, "Panic", &message),
                );
            }
        });
    }

    pub(crate) async fn request_shutdown(&self) {
        self.shutdown
            .request_shutdown(self.config.shutdown_drain_ms)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_and_do_alarm_dispatch_capacity_are_independent() {
        let dispatch = DispatchSemaphores::new(2, 3);

        let _workflow_a = dispatch.workflow.try_acquire().unwrap();
        let _workflow_b = dispatch.workflow.try_acquire().unwrap();
        assert!(dispatch.workflow.try_acquire().is_err());
        assert_eq!(dispatch.do_alarm.available_permits(), 3);
    }

    #[tokio::test]
    async fn dispatch_task_guard_holds_capacity_and_shutdown_slot_until_task_finishes() {
        let shutdown = Arc::new(ShutdownState::default());
        let permits = Arc::new(Semaphore::new(1));
        let guard = DispatchTaskGuard::try_begin(&shutdown, &permits).expect("task should start");
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();

        let task = tokio::spawn(async move {
            let _guard = guard;
            release_rx.await.expect("test should release task");
        });

        assert_eq!(permits.available_permits(), 0);
        assert_eq!(shutdown.in_flight_count(), 1);
        assert!(matches!(
            DispatchTaskGuard::try_begin(&shutdown, &permits),
            Err(DispatchTaskUnavailable::AtCapacity)
        ));
        assert_eq!(shutdown.in_flight_count(), 1);

        release_tx.send(()).expect("task should still be running");
        task.await.expect("task should finish cleanly");
        assert_eq!(permits.available_permits(), 1);
        assert_eq!(shutdown.in_flight_count(), 0);
    }

    #[test]
    fn dispatch_task_guard_distinguishes_shutdown_from_capacity() {
        let shutdown = Arc::new(ShutdownState::default());
        let permits = Arc::new(Semaphore::new(1));
        shutdown.begin_shutdown();

        assert!(matches!(
            DispatchTaskGuard::try_begin(&shutdown, &permits),
            Err(DispatchTaskUnavailable::Stopping)
        ));
        assert_eq!(permits.available_permits(), 1);
        assert_eq!(shutdown.in_flight_count(), 0);
    }
}
