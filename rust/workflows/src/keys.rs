use std::fmt::Write as _;

use sha2::{Digest, Sha256};

pub(crate) const DO_ALARM_READY_SHARDS: usize = 32;
pub(crate) const WORKFLOW_READY_SHARDS: usize = 32;

pub(crate) fn schema_version_key() -> &'static str {
    "wf:schema_version"
}

pub(crate) fn workflow_defs_key(ns: &str, worker: &str) -> String {
    format!("wf:defs:{ns}:{worker}")
}

pub(crate) fn instance_scope(ns: &str, workflow_key: &str, instance_id: &str) -> String {
    format!("{{{ns}:{workflow_key}:{instance_id}}}")
}

pub(crate) struct InstanceKeys {
    scope: String,
}

impl InstanceKeys {
    pub(crate) fn new(ns: &str, workflow_key: &str, instance_id: &str) -> Self {
        Self {
            scope: instance_scope(ns, workflow_key, instance_id),
        }
    }

    pub(crate) fn state(&self) -> String {
        format!("wf:instance:{}:state", self.scope)
    }

    pub(crate) fn payloads(&self) -> String {
        format!("wf:instance:{}:payloads", self.scope)
    }

    pub(crate) fn steps(&self) -> String {
        format!("wf:instance:{}:steps", self.scope)
    }

    pub(crate) fn step_summaries(&self) -> String {
        format!("wf:instance:{}:step-summaries", self.scope)
    }

    pub(crate) fn step_summary_index(&self) -> String {
        format!("wf:instance:{}:step-summary-index", self.scope)
    }

    pub(crate) fn events(&self) -> String {
        format!("wf:instance:{}:events", self.scope)
    }

    pub(crate) fn event_type_index(&self) -> String {
        format!("wf:instance:{}:events-by-type", self.scope)
    }
}

pub(crate) fn instance_state_key(ns: &str, workflow_key: &str, instance_id: &str) -> String {
    InstanceKeys::new(ns, workflow_key, instance_id).state()
}

pub(crate) fn instance_step_summaries_key(
    ns: &str,
    workflow_key: &str,
    instance_id: &str,
) -> String {
    InstanceKeys::new(ns, workflow_key, instance_id).step_summaries()
}

pub(crate) fn instance_step_summary_index_key(
    ns: &str,
    workflow_key: &str,
    instance_id: &str,
) -> String {
    InstanceKeys::new(ns, workflow_key, instance_id).step_summary_index()
}

pub(crate) fn ready_key(shard: usize) -> String {
    format!("wf:ready:{shard}")
}

#[derive(Clone, Copy)]
pub(crate) struct ShardQueueKeys {
    shard_count: usize,
    due: fn(usize) -> String,
    ready: fn(usize) -> String,
    ready_active: &'static str,
    ready_cursor: &'static str,
}

impl ShardQueueKeys {
    const fn new(
        shard_count: usize,
        due: fn(usize) -> String,
        ready: fn(usize) -> String,
        ready_active: &'static str,
        ready_cursor: &'static str,
    ) -> Self {
        Self {
            shard_count,
            due,
            ready,
            ready_active,
            ready_cursor,
        }
    }

    pub(crate) fn shard_count(&self) -> usize {
        self.shard_count
    }

    pub(crate) fn due(&self, shard: usize) -> String {
        (self.due)(shard)
    }

    pub(crate) fn ready(&self, shard: usize) -> String {
        (self.ready)(shard)
    }

    pub(crate) fn ready_active(&self) -> &'static str {
        self.ready_active
    }

    pub(crate) fn ready_cursor(&self) -> &'static str {
        self.ready_cursor
    }
}

pub(crate) fn workflow_shard_queue_keys() -> ShardQueueKeys {
    ShardQueueKeys::new(
        WORKFLOW_READY_SHARDS,
        due_key,
        ready_key,
        ready_active_key(),
        ready_cursor_key(),
    )
}

pub(crate) fn ready_active_key() -> &'static str {
    "wf:ready:active"
}

pub(crate) fn ready_cursor_key() -> &'static str {
    "wf:ready:cursor"
}

pub(crate) fn due_key(shard: usize) -> String {
    format!("wf:due:{shard}")
}

pub(crate) fn by_worker_key(ns: &str, worker: &str) -> String {
    format!("wf:by-worker:{ns}:{worker}")
}

pub(crate) fn by_workflow_key(ns: &str, worker: &str, workflow_key: &str) -> String {
    format!("wf:by-workflow:{ns}:{worker}:{workflow_key}")
}

pub(crate) fn by_version_key(ns: &str, worker: &str, version: &str) -> String {
    format!("wf:by-version:{ns}:{worker}:{version}")
}

pub(crate) fn pending_version_key(ns: &str, worker: &str, version: &str) -> String {
    format!("wf:pending-version:{ns}:{worker}:{version}")
}

pub(crate) fn retention_key() -> &'static str {
    "wf:retention"
}

pub(crate) fn do_alarm_job_id(
    ns: &str,
    worker: &str,
    do_storage_id: &str,
    class_name: &str,
    object_name: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(ns.as_bytes());
    hasher.update([0]);
    hasher.update(worker.as_bytes());
    hasher.update([0]);
    hasher.update(do_storage_id.as_bytes());
    hasher.update([0]);
    hasher.update(class_name.as_bytes());
    hasher.update([0]);
    hasher.update(object_name.as_bytes());
    let digest = hasher.finalize();
    let mut out = String::with_capacity("doa-".len() + 64);
    out.push_str("doa-");
    for byte in digest {
        write!(&mut out, "{byte:02x}").expect("writing hex into String cannot fail");
    }
    out
}

pub(crate) fn do_alarm_ready_shard(job_id: &str) -> usize {
    (wdl_rust_common::hash::fnv1a32(job_id.as_bytes()) as usize) % DO_ALARM_READY_SHARDS
}

pub(crate) fn do_alarm_due_key(shard: usize) -> String {
    format!("wf:internal:do-alarm:due:{shard}")
}

pub(crate) fn do_alarm_ready_key(shard: usize) -> String {
    format!("wf:internal:do-alarm:ready:{shard}")
}

pub(crate) fn do_alarm_ready_active_key() -> &'static str {
    "wf:internal:do-alarm:ready:active"
}

pub(crate) fn do_alarm_ready_cursor_key() -> &'static str {
    "wf:internal:do-alarm:ready:cursor"
}

pub(crate) fn do_alarm_shard_queue_keys() -> ShardQueueKeys {
    ShardQueueKeys::new(
        DO_ALARM_READY_SHARDS,
        do_alarm_due_key,
        do_alarm_ready_key,
        do_alarm_ready_active_key(),
        do_alarm_ready_cursor_key(),
    )
}

pub(crate) fn do_alarm_by_worker_key(ns: &str, worker: &str) -> String {
    format!("wf:internal:do-alarm:by-worker:{ns}:{worker}")
}

pub(crate) struct DoAlarmJobKeys {
    job_id: String,
    shard: usize,
}

impl DoAlarmJobKeys {
    pub(crate) fn new(job_id: impl Into<String>) -> Self {
        let job_id = job_id.into();
        let shard = do_alarm_ready_shard(&job_id);
        Self { job_id, shard }
    }

    pub(crate) fn job_id(&self) -> &str {
        &self.job_id
    }

    pub(crate) fn state(&self) -> String {
        format!("wf:internal:do-alarm:{{{}}}:state", self.job_id)
    }

    pub(crate) fn due(&self) -> String {
        do_alarm_due_key(self.shard)
    }

    pub(crate) fn ready(&self) -> String {
        do_alarm_ready_key(self.shard)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct DoAlarmJobIdFixture {
        input: DoAlarmJobIdFixtureInput,
        expected: String,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct DoAlarmJobIdFixtureInput {
        ns: String,
        worker: String,
        do_storage_id: String,
        class_name: String,
        object_name: String,
    }

    #[test]
    fn do_alarm_job_id_is_stable_sha256_with_nul_separators() {
        let fixtures: Vec<DoAlarmJobIdFixture> =
            serde_json::from_str(include_str!("../../../tests/fixtures/do-alarm-job-id.json"))
                .expect("DO alarm job id fixtures should parse");
        for fixture in fixtures {
            assert_eq!(
                do_alarm_job_id(
                    &fixture.input.ns,
                    &fixture.input.worker,
                    &fixture.input.do_storage_id,
                    &fixture.input.class_name,
                    &fixture.input.object_name,
                ),
                fixture.expected
            );
        }
    }

    #[test]
    fn do_alarm_job_id_bounds_untrusted_name_bytes_to_digest() {
        let id = do_alarm_job_id(
            "demo",
            "worker",
            "do_123",
            "Room",
            "name:with/slashes and spaces and unicode \u{2603}",
        );

        assert_eq!(id.len(), "doa-".len() + 64);
        assert!(id.starts_with("doa-"));
        assert!(id["doa-".len()..].chars().all(|ch| ch.is_ascii_hexdigit()));
    }

    #[test]
    fn do_alarm_keys_stay_in_internal_workflows_keyspace() {
        let keys = DoAlarmJobKeys::new("doa-abc");

        assert_eq!(keys.state(), "wf:internal:do-alarm:{doa-abc}:state");
        assert!(keys.due().starts_with("wf:internal:do-alarm:due:"));
        assert!(keys.ready().starts_with("wf:internal:do-alarm:ready:"));
        assert_eq!(
            do_alarm_ready_active_key(),
            "wf:internal:do-alarm:ready:active"
        );
        assert_eq!(
            do_alarm_ready_cursor_key(),
            "wf:internal:do-alarm:ready:cursor"
        );
        assert_eq!(
            do_alarm_by_worker_key("demo", "worker"),
            "wf:internal:do-alarm:by-worker:demo:worker"
        );

        let queue = do_alarm_shard_queue_keys();
        assert_eq!(queue.shard_count(), DO_ALARM_READY_SHARDS);
        assert_eq!(queue.due(7), do_alarm_due_key(7));
        assert_eq!(queue.ready(7), do_alarm_ready_key(7));
        assert_eq!(queue.ready_active(), do_alarm_ready_active_key());
        assert_eq!(queue.ready_cursor(), do_alarm_ready_cursor_key());
    }

    #[test]
    fn workflow_ready_cursor_key_is_stable() {
        assert_eq!(ready_cursor_key(), "wf:ready:cursor");

        let queue = workflow_shard_queue_keys();
        assert_eq!(queue.shard_count(), WORKFLOW_READY_SHARDS);
        assert_eq!(queue.due(5), due_key(5));
        assert_eq!(queue.ready(5), ready_key(5));
        assert_eq!(queue.ready_active(), ready_active_key());
        assert_eq!(queue.ready_cursor(), ready_cursor_key());
    }
}
