use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};

use futures_util::FutureExt;
use redis::aio::ConnectionManagerConfig;
use serde_json::Value as JsonValue;
use tokio::sync::{Notify, RwLock, Semaphore};
use wdl_rust_common::redis_conn::RedisConnection;
pub(crate) use wdl_rust_common::shutdown::{InFlightGuard, ShutdownState};

use crate::queue::Consumer;
use crate::{Config, LogLevel, Metrics, fields_with_error, log, panic_payload_message};

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) redis: Redis,
    pub(crate) data_redis: Redis,
    pub(crate) data_redis_client: redis::Client,
    pub(crate) http: reqwest::Client,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) queues: Arc<QueueState>,
    pub(crate) shutdown: Arc<ShutdownState>,
    pub(crate) dispatch: Arc<DispatchSemaphores>,
    pub(crate) config: Arc<Config>,
    pub(crate) instance_id: String,
}

pub(crate) struct DispatchSemaphores {
    pub(crate) cron: Arc<Semaphore>,
    pub(crate) queue: Arc<Semaphore>,
}

pub(crate) type Redis = RedisConnection;

#[derive(Default)]
pub(crate) struct QueueState {
    pub(crate) registry: RwLock<HashMap<String, Consumer>>,
    pub(crate) consumer_streams: RwLock<Arc<Vec<String>>>,
    pub(crate) dispatching_streams: Mutex<HashSet<String>>,
    pub(crate) stream_dispatch_completed: Notify,
    pub(crate) known_streams: RwLock<HashSet<String>>,
    pub(crate) known_delayed: RwLock<HashSet<String>>,
    pub(crate) delayed_changed: Notify,
}

impl DispatchSemaphores {
    pub(crate) fn new(cron_limit: usize, queue_limit: usize) -> Self {
        Self {
            cron: Arc::new(Semaphore::new(cron_limit)),
            queue: Arc::new(Semaphore::new(queue_limit)),
        }
    }
}

pub(crate) fn blocking_redis_connection_config() -> ConnectionManagerConfig {
    ConnectionManagerConfig::new().set_response_timeout(None)
}

impl AppState {
    pub(crate) fn is_shutting_down(&self) -> bool {
        self.shutdown.is_stopping()
    }

    pub(crate) fn begin_in_flight(&self) -> Option<InFlightGuard> {
        self.shutdown.begin_in_flight()
    }

    pub(crate) fn spawn_tracked<F>(
        &self,
        panic_event: &'static str,
        panic_fields: JsonValue,
        future: F,
    ) where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let Some(guard) = self.begin_in_flight() else {
            return;
        };
        let state = self.clone();
        tokio::spawn(async move {
            let _guard = guard;
            if let Err(err) = AssertUnwindSafe(future).catch_unwind().await {
                log(
                    &state,
                    LogLevel::Error,
                    panic_event,
                    fields_with_error(panic_fields, "Panic", panic_payload_message(err.as_ref())),
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
    fn dispatch_semaphores_keep_cron_and_queue_admission_independent() {
        let dispatch = DispatchSemaphores::new(2, 3);

        assert_eq!(dispatch.cron.available_permits(), 2);
        assert_eq!(dispatch.queue.available_permits(), 3);

        let _cron_a = dispatch.cron.try_acquire().unwrap();
        let _cron_b = dispatch.cron.try_acquire().unwrap();

        assert!(dispatch.cron.try_acquire().is_err());
        assert_eq!(dispatch.queue.available_permits(), 3);
        assert!(dispatch.queue.try_acquire().is_ok());
    }

    #[test]
    fn blocking_redis_connection_config_disables_response_timeout() {
        assert_eq!(blocking_redis_connection_config().response_timeout(), None);
    }
}
