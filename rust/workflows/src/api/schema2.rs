use std::collections::HashMap;

use serde_json::Value as JsonValue;

use crate::{
    InstanceKeys, Redis, WorkflowError, WorkflowResult, by_version_key, by_worker_key,
    ready_active_key, workflow_defs_key,
};

use super::identity::validate_instance_id_value;
use super::{
    InstanceIdentity, InstanceRouteKeys, MAX_WORKFLOW_INSTANCE_PAYLOAD_BYTES, WorkflowDef,
    bundle_key, identity_from_state, migrate_schema2_steps, parse_positive_identity_i64,
    workflow_referrer_member,
};

fn invalid(message: &str) -> WorkflowError {
    WorkflowError::invalid_state(format!("Schema-2 migration: {message}"))
}

async fn read_hash(redis: &Redis, key: &str) -> WorkflowResult<HashMap<String, String>> {
    let mut entries = HashMap::new();
    let mut bytes = 0usize;
    let mut cursor = 0u64;
    loop {
        let (next, page): (u64, Vec<(String, String)>) = redis
            .with_conn(async |mut conn| {
                redis::cmd("HSCAN")
                    .arg(key)
                    .arg(cursor)
                    .arg("COUNT")
                    .arg(100)
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        for (field, value) in page {
            let size = value.len();
            if let Some(previous) = entries.insert(field, value) {
                bytes -= previous.len();
            }
            bytes = bytes.saturating_add(size);
            if bytes > MAX_WORKFLOW_INSTANCE_PAYLOAD_BYTES {
                return Err(invalid("instance hash exceeds the aggregate payload limit"));
            }
        }
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    Ok(entries)
}

pub(crate) struct Schema2Instance {
    identity: InstanceIdentity,
    state: HashMap<String, String>,
    steps: HashMap<String, String>,
    enqueue: bool,
}

impl Schema2Instance {
    pub(crate) fn step_count(&self) -> usize {
        self.steps.len()
    }

    pub(crate) async fn install(&self, active: &Redis) -> WorkflowResult<()> {
        let id = &self.identity;
        let keys = InstanceRouteKeys::new(&id.ns, &id.workflow_key, &id.instance_id);
        active
            .with_conn(async |mut conn| {
                let mut pipe = redis::pipe();
                pipe.atomic();
                for (ordinal, step) in &self.steps {
                    pipe.cmd("HSET")
                        .arg(keys.steps())
                        .arg(ordinal)
                        .arg(step)
                        .ignore();
                }
                pipe.cmd("DEL").arg(keys.state()).ignore();
                pipe.cmd("HSET").arg(keys.state()).arg(&self.state).ignore();
                if self.enqueue {
                    pipe.cmd("SADD")
                        .arg(keys.ready())
                        .arg(keys.token())
                        .ignore();
                    pipe.cmd("SADD")
                        .arg(ready_active_key())
                        .arg(keys.shard())
                        .ignore();
                }
                pipe.query_async::<()>(&mut conn).await
            })
            .await?;
        if read_hash(active, &keys.state()).await? != self.state
            || read_hash(active, &keys.steps()).await? != self.steps
        {
            return Err(invalid("converted instance verification failed"));
        }
        Ok(())
    }
}

async fn verify_code(control: &Redis, id: &InstanceIdentity) -> WorkflowResult<()> {
    let bundle = bundle_key(&id.ns, &id.worker, &id.frozen_version)?;
    let (meta, definition): (Option<String>, Option<String>) = control
        .with_conn(async |mut conn| {
            redis::pipe()
                .cmd("HGET")
                .arg(bundle)
                .arg("__meta__")
                .cmd("HGET")
                .arg(workflow_defs_key(&id.ns, &id.worker))
                .arg(&id.workflow_name)
                .query_async(&mut conn)
                .await
        })
        .await?;
    let meta: JsonValue =
        serde_json::from_str(&meta.ok_or_else(|| invalid("pinned Worker version is missing"))?)
            .map_err(|_| invalid("pinned Worker metadata is corrupt"))?;
    let definition: WorkflowDef =
        serde_json::from_str(&definition.ok_or_else(|| invalid("Workflow definition is missing"))?)
            .map_err(|_| invalid("Workflow definition is corrupt"))?;
    if definition.workflow_key != id.workflow_key
        || !meta["workflows"].as_array().is_some_and(|exports| {
            exports.iter().any(|entry| {
                entry["workflowKey"] == id.workflow_key
                    && entry["name"] == id.workflow_name
                    && entry["className"] == id.class_name
            })
        })
    {
        return Err(invalid(
            "pinned Workflow export does not match instance identity",
        ));
    }
    Ok(())
}

pub(crate) async fn read_schema2_instance(
    source: &Redis,
    control: &Redis,
    state_key: &str,
) -> WorkflowResult<Schema2Instance> {
    let mut state = read_hash(source, state_key).await?;
    let field = |name: &str| {
        state
            .get(name)
            .cloned()
            .ok_or_else(|| invalid("instance identity is incomplete"))
    };
    let ns = field("ns")?;
    let workflow_key = field("workflowKey")?;
    let instance_id = field("instanceId")?;
    validate_instance_id_value(&instance_id)?;
    let identity = identity_from_state(&ns, &workflow_key, &instance_id, &state)?;
    parse_positive_identity_i64(&identity.generation, "generation")?;
    parse_positive_identity_i64(&identity.created_at_ms, "createdAtMs")?;
    if let Some(lease) = state.get("runLeaseExpiresAtMs") {
        parse_positive_identity_i64(lease, "runLeaseExpiresAtMs")?;
    }
    let keys = InstanceKeys::new(&ns, &workflow_key, &instance_id);
    if state_key != keys.state() || state.get("payloadsKey") != Some(&keys.payloads()) {
        return Err(invalid("instance key does not match stored identity"));
    }
    verify_code(control, &identity).await?;
    let member = workflow_referrer_member(&workflow_key, &instance_id);
    let (worker_ref, version_ref): (bool, bool) = source
        .with_conn(async |mut conn| {
            redis::pipe()
                .cmd("SISMEMBER")
                .arg(by_worker_key(&ns, &identity.worker))
                .arg(&member)
                .cmd("SISMEMBER")
                .arg(by_version_key(
                    &ns,
                    &identity.worker,
                    &identity.frozen_version,
                ))
                .arg(&member)
                .query_async(&mut conn)
                .await
        })
        .await?;
    if !worker_ref || !version_ref {
        return Err(invalid("instance lifecycle referrer is missing"));
    }

    let payloads = read_hash(source, &keys.payloads()).await?;
    let steps = read_hash(source, &keys.steps()).await?;
    let summaries = read_hash(source, &keys.step_summaries()).await?;
    let events = read_hash(source, &keys.events()).await?;
    let old_bytes: usize = [&payloads, &steps, &summaries, &events]
        .into_iter()
        .flat_map(|hash| hash.values())
        .map(String::len)
        .sum();
    if old_bytes > MAX_WORKFLOW_INSTANCE_PAYLOAD_BYTES
        || state
            .get("payloadBytes")
            .and_then(|raw| raw.parse::<usize>().ok())
            != Some(old_bytes)
    {
        return Err(invalid(
            "instance payload accounting is corrupt or exceeds its limit",
        ));
    }
    if state.get("paramsRef").map(String::as_str) != Some("params") {
        return Err(invalid("instance paramsRef is invalid"));
    }
    for field in ["paramsRef", "outputRef", "errorRef"] {
        if let Some(reference) = state.get(field)
            && !payloads.contains_key(reference)
        {
            return Err(invalid("instance payload reference is missing"));
        }
    }
    let converted = migrate_schema2_steps(&steps, &summaries, &payloads, &events)?;
    let new_bytes = old_bytes - steps.values().map(String::len).sum::<usize>()
        + converted.values().map(String::len).sum::<usize>();
    if new_bytes > MAX_WORKFLOW_INSTANCE_PAYLOAD_BYTES {
        return Err(invalid(
            "converted instance exceeds the 16 MiB payload limit",
        ));
    }
    let summary_count: usize = source
        .with_conn(async |mut conn| {
            redis::cmd("ZCARD")
                .arg(keys.step_summary_index())
                .query_async(&mut conn)
                .await
        })
        .await?;
    if summary_count != steps.len() {
        return Err(invalid("step summary index does not match stored steps"));
    }
    // The 1000-step admission limit is per dispatch turn, not per instance history.
    let fields: Vec<_> = steps.keys().collect();
    for batch in fields.chunks(100) {
        let scores: Vec<Option<f64>> = source
            .with_conn(async |mut conn| {
                redis::cmd("ZMSCORE")
                    .arg(keys.step_summary_index())
                    .arg(batch)
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        if scores.len() != batch.len()
            || batch
                .iter()
                .zip(scores)
                .any(|(field, score)| field.parse::<u32>().map(f64::from).ok() != score)
        {
            return Err(invalid("step summary index does not match stored steps"));
        }
    }
    let mut enqueue = match state.get("status").map(String::as_str) {
        Some("running" | "queued") => true,
        Some("waiting" | "paused" | "completed" | "failed" | "terminated" | "pending_create") => {
            false
        }
        _ => return Err(invalid("instance status is invalid")),
    };
    if state.get("status").map(String::as_str) == Some("waiting")
        && let Some(prefix) = state.get("waitingEventIndexPrefix")
    {
        let buffered: Vec<String> = source
            .with_conn(async |mut conn| {
                redis::cmd("ZRANGE")
                    .arg(keys.event_type_index())
                    .arg(format!("[{prefix}"))
                    .arg(format!("[{prefix}\u{ff}"))
                    .arg("BYLEX")
                    .arg("LIMIT")
                    .arg(0)
                    .arg(1)
                    .query_async(&mut conn)
                    .await
            })
            .await?;
        enqueue = !buffered.is_empty();
    }
    // Old dispatches must already have drained. In-flight root work may replay,
    // but completed durable steps remain intact and retain their original ordinals.
    if state.get("status").map(String::as_str) == Some("running") {
        state.insert("status".to_string(), "queued".to_string());
    }
    state.remove("runToken");
    state.remove("runLeaseExpiresAtMs");
    state.insert("payloadBytes".to_string(), new_bytes.to_string());
    Ok(Schema2Instance {
        identity,
        state,
        steps: converted,
        enqueue,
    })
}
