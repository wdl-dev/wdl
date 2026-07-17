use wdl_rust_common::{hash::fnv1a32, worker_contract::worker_bundle_key};

use crate::{InstanceKeys, WorkflowError, WorkflowResult, due_key, ready_key};

use super::READY_SHARDS;

pub(super) struct InstanceRouteKeys<'a> {
    ns: &'a str,
    workflow_key: &'a str,
    instance_id: &'a str,
    keys: InstanceKeys,
    shard: usize,
}

impl<'a> InstanceRouteKeys<'a> {
    pub(super) fn new(ns: &'a str, workflow_key: &'a str, instance_id: &'a str) -> Self {
        Self {
            ns,
            workflow_key,
            instance_id,
            keys: InstanceKeys::new(ns, workflow_key, instance_id),
            shard: ready_shard(ns, workflow_key, instance_id),
        }
    }

    pub(super) fn state(&self) -> String {
        self.keys.state()
    }

    pub(super) fn payloads(&self) -> String {
        self.keys.payloads()
    }

    pub(super) fn steps(&self) -> String {
        self.keys.steps()
    }

    pub(super) fn step_summaries(&self) -> String {
        self.keys.step_summaries()
    }

    pub(super) fn step_summary_index(&self) -> String {
        self.keys.step_summary_index()
    }

    pub(super) fn events(&self) -> String {
        self.keys.events()
    }

    pub(super) fn event_type_index(&self) -> String {
        self.keys.event_type_index()
    }

    pub(super) fn shard(&self) -> usize {
        self.shard
    }

    pub(super) fn ready(&self) -> String {
        ready_key(self.shard())
    }

    pub(super) fn due(&self) -> String {
        due_key(self.shard())
    }

    pub(super) fn token(&self) -> String {
        ready_token(self.ns, self.workflow_key, self.instance_id)
    }
}

pub(super) fn ready_shard(ns: &str, workflow_key: &str, instance_id: &str) -> usize {
    fnv1a32(format!("{ns}:{workflow_key}:{instance_id}").as_bytes()) as usize % READY_SHARDS
}

pub(super) fn bundle_key(ns: &str, worker: &str, version: &str) -> WorkflowResult<String> {
    worker_bundle_key(ns, worker, version)
        .map_err(|_| WorkflowError::invalid_state("Active worker version is invalid"))
}

pub(super) fn ready_token(ns: &str, workflow_key: &str, instance_id: &str) -> String {
    format!("{ns}\t{workflow_key}\t{instance_id}")
}

// Worker- and version-scoped referrer sets span multiple workflows, so their
// members include the workflow key. Workflow-scoped sets already carry the
// workflow key in the Redis key and store only the instance id.
pub(super) fn workflow_referrer_member(workflow_key: &str, instance_id: &str) -> String {
    format!("{workflow_key}\t{instance_id}")
}

pub(super) fn parse_workflow_referrer_member(member: &str) -> Option<(String, String)> {
    let mut parts = member.split('\t');
    let workflow_key = parts.next()?.to_string();
    let instance_id = parts.next()?.to_string();
    if parts.next().is_some() || workflow_key.is_empty() || instance_id.is_empty() {
        return None;
    }
    Some((workflow_key, instance_id))
}

pub(super) fn parse_ready_token(token: &str) -> Option<(String, String, String)> {
    let mut parts = token.split('\t');
    let ns = parts.next()?.to_string();
    let workflow_key = parts.next()?.to_string();
    let instance_id = parts.next()?.to_string();
    if parts.next().is_some() || ns.is_empty() || workflow_key.is_empty() || instance_id.is_empty()
    {
        return None;
    }
    Some((ns, workflow_key, instance_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundle_key_uses_canonical_version_tags() {
        assert_eq!(
            bundle_key("tenant", "worker", "v42").unwrap(),
            "worker:tenant:worker:v:42"
        );

        for version in ["", "v", "v0", "v01", "1", "V1", "v1a"] {
            let err = bundle_key("tenant", "worker", version).unwrap_err();
            assert_eq!(err.code, "workflow_invalid_state");
        }
    }
}
