use serde::Serialize;
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Consumer {
    pub(crate) ns: String,
    pub(crate) queue: String,
    pub(crate) max_batch_size: usize,
    pub(crate) max_batch_timeout_ms: i64,
    pub(crate) max_retries: i64,
    pub(crate) retry_delay_secs: i64,
    pub(crate) dead_letter_queue: Option<String>,
    pub(crate) worker_id: String,
}

#[derive(Clone, Debug)]
pub(crate) struct StreamEntry {
    pub(crate) id: String,
    pub(crate) fields: HashMap<String, String>,
}

#[derive(Clone, Debug)]
pub(crate) struct QueueMessage {
    pub(crate) stream_id: String,
    pub(crate) id: String,
    pub(crate) body_b64: String,
    pub(crate) content_type: String,
    pub(crate) attempts: String,
    pub(crate) first_seen_ms: String,
}

#[derive(Clone, Serialize)]
pub(crate) struct RuntimeMessage {
    pub(crate) id: String,
    pub(crate) body_b64: String,
    pub(crate) content_type: String,
    pub(crate) attempts: String,
    pub(crate) first_seen_ms: String,
}
pub(crate) enum OutcomePlan {
    Normal {
        to_ack: Vec<QueueMessage>,
        to_retry: Vec<(QueueMessage, Option<i64>)>,
    },
    RetryAll {
        kind: &'static str,
        reason: String,
        messages: Vec<QueueMessage>,
    },
    TerminalAll {
        kind: &'static str,
        reason: String,
        messages: Vec<QueueMessage>,
    },
}

pub(crate) enum RetryAction {
    Dlq {
        attempts: i64,
        target: String,
        entry: HashMap<String, String>,
    },
    Delay {
        visible_at_ms: i64,
        entry: HashMap<String, String>,
    },
    Immediate {
        entry: HashMap<String, String>,
    },
}
