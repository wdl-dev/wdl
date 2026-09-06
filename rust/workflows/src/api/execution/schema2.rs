use std::collections::{BTreeMap, HashMap};

use serde_json::{
    Value as JsonValue,
    value::{RawValue, to_raw_value},
};

use crate::{EventRecord, WorkflowError, WorkflowResult};

use super::super::limits::{MAX_WORKFLOW_STEP_CONFIG_BYTES, MAX_WORKFLOW_STEP_NAME_BYTES};
use super::model::{
    MAX_STEP_DEPENDENCIES, STEP_KIND_DO, STEP_KIND_SLEEP, STEP_KIND_SLEEP_UNTIL,
    STEP_KIND_WAIT_FOR_EVENT, StepRecord, step_error_ref, step_event_ref, step_output_ref,
    step_summary_json,
};

fn invalid(message: &str) -> WorkflowError {
    WorkflowError::invalid_state(format!("Schema-2 migration: {message}"))
}

fn has_payload(
    record: &StepRecord,
    payloads: &HashMap<String, String>,
    output: bool,
) -> WorkflowResult<bool> {
    let (inline, reference) = if output {
        (record.output.as_ref(), record.output_ref.as_ref())
    } else {
        (record.error.as_ref(), record.error_ref.as_ref())
    };
    match (inline, reference) {
        (Some(_), Some(_)) => Err(invalid("step has both inline and referenced payload")),
        (Some(_), None) => Ok(true),
        (None, Some(reference)) if payloads.contains_key(reference) => Ok(true),
        (None, Some(_)) => Err(invalid("step payload is missing")),
        (None, None) => Ok(false),
    }
}

fn read_payload(raw: &str) -> WorkflowResult<JsonValue> {
    serde_json::from_str(raw).map_err(|_| invalid("instance payload is invalid JSON"))
}

fn output_matches(
    record: &StepRecord,
    payloads: &HashMap<String, String>,
    expected: &JsonValue,
) -> WorkflowResult<bool> {
    if let Some(output) = &record.output {
        return Ok(output == expected);
    }
    let raw = record
        .output_ref
        .as_ref()
        .and_then(|reference| payloads.get(reference))
        .ok_or_else(|| invalid("step payload is missing"))?;
    Ok(read_payload(raw)? == *expected)
}

fn sleep_config(config: &JsonValue, kind: &str) -> bool {
    let field = if kind == STEP_KIND_SLEEP {
        "durationMs"
    } else {
        "dueAtMs"
    };
    config.as_object().is_some_and(|object| {
        object.len() == 2
            && object.get("type").and_then(JsonValue::as_str) == Some(kind)
            && object
                .get(field)
                .and_then(JsonValue::as_f64)
                .is_some_and(|n| n.is_finite() && n >= 0.0)
    })
}

fn wait_config(config: &JsonValue) -> bool {
    config.as_object().is_some_and(|object| {
        object.len() == 3
            && object.get("type").and_then(JsonValue::as_str) == Some(STEP_KIND_WAIT_FOR_EVENT)
            && object
                .get("eventType")
                .and_then(JsonValue::as_str)
                .is_some_and(|s| !s.is_empty())
            && object.get("timeoutMs").is_some_and(|v| {
                v.is_null() || v.as_f64().is_some_and(|n| n.is_finite() && n >= 0.0)
            })
    })
}

fn infer_kind(
    record: &StepRecord,
    payloads: &HashMap<String, String>,
    consumed: Option<&EventRecord>,
) -> WorkflowResult<&'static str> {
    let output = has_payload(record, payloads, true)?;
    let error = has_payload(record, payloads, false)?;
    let config: JsonValue =
        serde_json::from_str(&record.config).map_err(|_| invalid("step config is invalid JSON"))?;
    if record.config.len() > MAX_WORKFLOW_STEP_CONFIG_BYTES {
        return Err(invalid("step config exceeds its limit"));
    }
    let kind = if error {
        STEP_KIND_DO
    } else if consumed.is_some()
        || record.output_ref.as_deref() == Some(step_event_ref(record.ordinal).as_str())
    {
        STEP_KIND_WAIT_FOR_EVENT
    } else if output {
        // A do() config may imitate a wait config. Only persisted event consumption
        // or the host-owned event payload reference proves a completed event wait.
        STEP_KIND_DO
    } else if sleep_config(&config, STEP_KIND_SLEEP) {
        STEP_KIND_SLEEP
    } else if sleep_config(&config, STEP_KIND_SLEEP_UNTIL) {
        STEP_KIND_SLEEP_UNTIL
    } else if wait_config(&config) {
        STEP_KIND_WAIT_FOR_EVENT
    } else {
        return Err(invalid("step operation kind cannot be determined"));
    };

    let valid = match kind {
        STEP_KIND_DO => {
            consumed.is_none()
                && record
                    .output_ref
                    .as_ref()
                    .is_none_or(|r| *r == step_output_ref(record.ordinal))
                && record
                    .error_ref
                    .as_ref()
                    .is_none_or(|r| *r == step_error_ref(record.ordinal))
                && match record.status.as_str() {
                    "completed" => output && !error && record.due_at_ms.is_none(),
                    "waiting" => !output && error && record.due_at_ms.is_some(),
                    "failed" => !output && error && record.due_at_ms.is_none(),
                    _ => false,
                }
        }
        STEP_KIND_SLEEP | STEP_KIND_SLEEP_UNTIL => {
            record.attempt == 1
                && !output
                && !error
                && consumed.is_none()
                && record.due_at_ms.is_some_and(|due| due > 0)
                && matches!(record.status.as_str(), "waiting" | "completed")
        }
        STEP_KIND_WAIT_FOR_EVENT => {
            record.attempt == 1
                && wait_config(&config)
                && !error
                && record
                    .output_ref
                    .as_ref()
                    .is_none_or(|r| *r == step_event_ref(record.ordinal))
                && match record.status.as_str() {
                    "waiting" => !output && consumed.is_none(),
                    "completed" => {
                        record.due_at_ms.is_none()
                            && output
                            && match consumed {
                                Some(event) => {
                                    // Match the legacy event writer/read round trip,
                                    // without retaining other payloads or writing parsed numbers.
                                    let value = read_payload(&payloads[&event.payload_ref])?;
                                    let expected = serde_json::to_string(&value)
                                        .and_then(|raw| serde_json::from_str(&raw))
                                        .map_err(|_| invalid("event output is corrupt"))?;
                                    config["eventType"] == event.event_type
                                        && output_matches(record, payloads, &expected)?
                                }
                                None => {
                                    !config["timeoutMs"].is_null()
                                        && output_matches(record, payloads, &JsonValue::Null)?
                                }
                            }
                    }
                    _ => false,
                }
        }
        _ => unreachable!(),
    };
    if !valid {
        return Err(invalid("step shape contradicts its operation kind"));
    }
    Ok(kind)
}

pub(crate) fn migrate_schema2_steps(
    steps: &HashMap<String, String>,
    summaries: &HashMap<String, String>,
    payloads: &HashMap<String, String>,
    events: &HashMap<String, String>,
) -> WorkflowResult<HashMap<String, String>> {
    // Keep the Value reader's grammar checks, but drop each parsed tree immediately.
    for raw in payloads.values() {
        read_payload(raw)?;
    }
    let mut consumed = BTreeMap::new();
    for (id, raw) in events {
        let event: EventRecord =
            serde_json::from_str(raw).map_err(|_| invalid("event record is corrupt"))?;
        if event.id != *id
            || event.payload_ref != format!("event:{id}")
            || !payloads.contains_key(&event.payload_ref)
            || event.created_at_ms <= 0
        {
            return Err(invalid("event identity or payload is corrupt"));
        }
        if let Some(ordinal) = event.consumed_by_ordinal
            && consumed.insert(ordinal, event).is_some()
        {
            return Err(invalid("multiple events consumed by one step"));
        }
    }
    if steps.len() != summaries.len() {
        return Err(invalid("step and summary counts do not match"));
    }
    let mut dependency_records = BTreeMap::new();
    let mut converted = HashMap::new();
    for (field, raw) in steps {
        let mut value: JsonValue =
            serde_json::from_str(raw).map_err(|_| invalid("step record is corrupt"))?;
        let object = value
            .as_object_mut()
            .ok_or_else(|| invalid("step record is not an object"))?;
        if object.contains_key("kind") {
            return Err(invalid("schema-2 step already contains kind"));
        }
        object.insert("kind".to_string(), JsonValue::String(String::new()));
        let mut record: StepRecord =
            serde_json::from_value(value).map_err(|_| invalid("step record fields are corrupt"))?;
        if field != &record.ordinal.to_string()
            || record.name_count == 0
            || record.attempt == 0
            || record.step_name.is_empty()
            || record.step_name.len() > MAX_WORKFLOW_STEP_NAME_BYTES
            || record.dependencies.len() > MAX_STEP_DEPENDENCIES
            || record.completed_at_ms.is_some() != (record.status == "completed")
            || record.failed_at_ms.is_some() != (record.status == "failed")
        {
            return Err(invalid("step identity or terminal timestamps are corrupt"));
        }
        record.kind =
            infer_kind(&record, payloads, consumed.remove(&record.ordinal).as_ref())?.to_string();
        let summary = summaries
            .get(field)
            .ok_or_else(|| invalid("step summary is missing"))?;
        let actual: JsonValue =
            serde_json::from_str(summary).map_err(|_| invalid("step summary is corrupt"))?;
        let expected: JsonValue = serde_json::from_str(&step_summary_json(&record)?)
            .map_err(|_| invalid("step summary serialization failed"))?;
        if actual != expected {
            return Err(invalid("step summary does not match its record"));
        }
        // Validation may parse floats approximately. Copy the original JSON
        // fields verbatim so migration cannot change a later replay value.
        let mut fields: BTreeMap<String, &RawValue> =
            serde_json::from_str(raw).map_err(|_| invalid("step record is corrupt"))?;
        let kind =
            to_raw_value(&record.kind).map_err(|_| invalid("step kind serialization failed"))?;
        fields.insert("kind".to_string(), &kind);
        converted.insert(
            field.clone(),
            serde_json::to_string(&fields).map_err(|_| invalid("step serialization failed"))?,
        );
        dependency_records.insert(
            record.ordinal,
            (record.status == "completed", record.dependencies),
        );
    }
    if !consumed.is_empty() {
        return Err(invalid("event consumption refers to a missing step"));
    }
    for (ordinal, (_, dependencies)) in &dependency_records {
        let mut previous = None;
        for dependency in dependencies {
            if dependency >= ordinal
                || previous.is_some_and(|p| p >= dependency)
                || !dependency_records
                    .get(dependency)
                    .is_some_and(|(completed, _)| *completed)
            {
                return Err(invalid("step dependency is not a prior completed step"));
            }
            previous = Some(dependency);
        }
    }
    Ok(converted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{MAX_WORKFLOW_INSTANCE_PAYLOAD_BYTES, MAX_WORKFLOW_RESULT_BYTES};
    use serde_json::json;

    fn fixture() -> JsonValue {
        serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/workflow-schema2-steps.json"
        ))
        .unwrap()
    }

    fn convert(
        record: JsonValue,
        payloads: HashMap<String, String>,
        events: HashMap<String, String>,
    ) -> WorkflowResult<HashMap<String, String>> {
        convert_raw(&record.to_string(), payloads, events)
    }

    fn convert_raw(
        raw: &str,
        payloads: HashMap<String, String>,
        events: HashMap<String, String>,
    ) -> WorkflowResult<HashMap<String, String>> {
        let mut current: JsonValue = serde_json::from_str(raw).unwrap();
        current["kind"] = json!("do");
        let typed: StepRecord = serde_json::from_value(current).unwrap();
        let field = typed.ordinal.to_string();
        migrate_schema2_steps(
            &HashMap::from([(field.clone(), raw.to_string())]),
            &HashMap::from([(field, step_summary_json(&typed).unwrap())]),
            &payloads,
            &events,
        )
    }

    fn serialized_fields(value: &JsonValue) -> HashMap<String, String> {
        value
            .as_object()
            .into_iter()
            .flatten()
            .map(|(field, value)| (field.clone(), value.to_string()))
            .collect()
    }

    #[test]
    fn migration_preserves_raw_numeric_fields_from_the_shared_fixture() {
        for case in fixture()["rawCases"].as_array().unwrap() {
            let raw = case["recordJson"].as_str().unwrap();
            let converted = convert_raw(raw, HashMap::new(), HashMap::new())
                .unwrap_or_else(|err| panic!("{}: {}", case["id"], err.message));
            let before: BTreeMap<String, &RawValue> = serde_json::from_str(raw).unwrap();
            let after: BTreeMap<String, &RawValue> = serde_json::from_str(&converted["0"]).unwrap();
            for (name, value) in &before {
                assert_eq!(after[name].get(), value.get(), "{} {name}", case["id"]);
            }
            assert_eq!(after.len(), before.len() + 1);
            assert_eq!(
                serde_json::from_str::<String>(after["kind"].get()).unwrap(),
                case["kind"].as_str().unwrap()
            );
        }
    }

    #[test]
    fn migration_accepts_a_float_event_output_written_by_the_legacy_owner() {
        let raw_event = "-5.417765023398954e-10";
        let event_output: JsonValue = serde_json::from_str(raw_event).unwrap();
        for referenced in [false, true] {
            let mut record = fixture()["defaults"].clone();
            record["config"] =
                json!("{\"eventType\":\"approved\",\"timeoutMs\":5000,\"type\":\"waitForEvent\"}");
            let mut payloads = HashMap::from([("event:1".to_string(), raw_event.to_string())]);
            if referenced {
                record["outputRef"] = json!("step:0:event");
                payloads.insert(
                    "step:0:event".to_string(),
                    serde_json::to_string(&event_output).unwrap(),
                );
            } else {
                record["output"] = event_output.clone();
            }
            let events = HashMap::from([("1".to_string(), json!({
                "id":"1", "type":"approved", "payloadRef":"event:1", "consumedByOrdinal":0, "createdAtMs":100
            }).to_string())]);
            let converted = convert(record, payloads, events).unwrap();
            let step: StepRecord = serde_json::from_str(&converted["0"]).unwrap();
            assert_eq!(step.kind, STEP_KIND_WAIT_FOR_EVENT);
        }
    }

    #[test]
    fn schema2_step_kinds_match_the_shared_fixture_without_losing_fields() {
        let fixture = fixture();
        assert_eq!(fixture["sourceSchema"], "2");
        assert_eq!(fixture["targetSchema"], crate::WORKFLOWS_SCHEMA_VERSION);
        for case in fixture["cases"].as_array().unwrap() {
            let mut record = fixture["defaults"].clone();
            record
                .as_object_mut()
                .unwrap()
                .extend(case["record"].as_object().unwrap().clone());
            let payloads = serialized_fields(&case["payloads"]);
            let events = serialized_fields(&case["events"]);
            let converted = convert(record.clone(), payloads, events)
                .unwrap_or_else(|err| panic!("{}: {}", case["id"], err.message));
            let mut expected = record;
            expected["kind"] = case["kind"].clone();
            assert_eq!(
                serde_json::from_str::<JsonValue>(&converted["0"]).unwrap(),
                expected,
                "{}",
                case["id"]
            );
        }
    }

    #[test]
    fn schema2_conversion_rejects_ambiguous_or_damaged_records() {
        let fixture = fixture();
        for patch in [
            json!({}),
            json!({"kind":"do", "output":1}),
            json!({"outputRef":"step:0:output"}),
            json!({"output":1, "error":2}),
            json!({"output":1, "dependencies":[0]}),
            json!({"output":1, "dependencies":[1]}),
            json!({"output":1, "status":"waiting", "completedAtMs":null}),
            json!({"config":"{\"type\":\"sleep\"}"}),
        ] {
            let mut record = fixture["defaults"].clone();
            record
                .as_object_mut()
                .unwrap()
                .extend(patch.as_object().unwrap().clone());
            assert!(
                convert(record, HashMap::new(), HashMap::new()).is_err(),
                "{patch}"
            );
        }
    }

    #[test]
    fn event_consumption_must_match_the_step_and_payload() {
        let fixture = fixture();
        let case = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["id"] == "event-inline")
            .unwrap();
        let mut record = fixture["defaults"].clone();
        record
            .as_object_mut()
            .unwrap()
            .extend(case["record"].as_object().unwrap().clone());
        let event = case["events"]["1"].clone();
        let payloads = serialized_fields(&case["payloads"]);
        for patch in [
            json!({"type":"wrong"}),
            json!({"consumedByOrdinal":99}),
            json!({"payloadRef":"missing"}),
        ] {
            let mut damaged = event.clone();
            damaged
                .as_object_mut()
                .unwrap()
                .extend(patch.as_object().unwrap().clone());
            assert!(
                convert(
                    record.clone(),
                    payloads.clone(),
                    HashMap::from([("1".to_string(), damaged.to_string())])
                )
                .is_err()
            );
        }
        assert!(
            convert(
                record,
                HashMap::from([("event:1".to_string(), json!("changed").to_string())]),
                HashMap::from([("1".to_string(), event.to_string())])
            )
            .is_err()
        );
    }

    #[test]
    fn unreferenced_payloads_still_use_the_value_readers_json_grammar() {
        for raw in [r#""\ud800""#, "1e400", "[0,]"] {
            let error = migrate_schema2_steps(
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::from([("unused".to_string(), raw.to_string())]),
                &HashMap::new(),
            )
            .unwrap_err();
            assert!(error.message.contains("instance payload is invalid JSON"));
        }
    }

    #[test]
    fn object_heavy_referenced_payloads_keep_their_raw_values_and_references() {
        let raw = format!("[{}]", vec![r#"{"":0}"#; 120_000].join(","));
        assert!(raw.len() < MAX_WORKFLOW_RESULT_BYTES);
        let defaults = fixture()["defaults"].clone();
        let mut steps = HashMap::new();
        let mut summaries = HashMap::new();
        let mut payloads = HashMap::new();
        for ordinal in 0..15 {
            let mut value = defaults.clone();
            value["ordinal"] = json!(ordinal);
            value["nameCount"] = json!(ordinal + 1);
            value["outputRef"] = json!(step_output_ref(ordinal));
            payloads.insert(step_output_ref(ordinal), raw.clone());
            steps.insert(ordinal.to_string(), value.to_string());
            value["kind"] = json!(STEP_KIND_DO);
            let typed: StepRecord = serde_json::from_value(value).unwrap();
            summaries.insert(ordinal.to_string(), step_summary_json(&typed).unwrap());
        }
        let bytes: usize = [&steps, &summaries, &payloads]
            .into_iter()
            .flat_map(|hash| hash.values())
            .map(String::len)
            .sum();
        assert!(bytes < MAX_WORKFLOW_INSTANCE_PAYLOAD_BYTES);
        let converted =
            migrate_schema2_steps(&steps, &summaries, &payloads, &HashMap::new()).unwrap();
        assert_eq!(converted.len(), steps.len());
        for ordinal in 0..15 {
            let record: StepRecord =
                serde_json::from_str(&converted[&ordinal.to_string()]).unwrap();
            assert_eq!(record.kind, STEP_KIND_DO);
            assert_eq!(record.output_ref, Some(step_output_ref(ordinal)));
            assert!(record.output.is_none());
            assert_eq!(payloads[&step_output_ref(ordinal)], raw);
        }
    }

    #[test]
    fn completed_parallel_history_keeps_sparse_ordinals_and_dependency_edges() {
        let defaults = fixture()["defaults"].clone();
        let mut steps = HashMap::new();
        let mut summaries = HashMap::new();
        for (ordinal, dependencies) in [(0, vec![]), (2, vec![0])] {
            let mut value = defaults.clone();
            value["ordinal"] = json!(ordinal);
            value["dependencies"] = json!(dependencies);
            value["output"] = json!(ordinal);
            steps.insert(ordinal.to_string(), value.to_string());
            value["kind"] = json!("do");
            let typed: StepRecord = serde_json::from_value(value).unwrap();
            summaries.insert(ordinal.to_string(), step_summary_json(&typed).unwrap());
        }
        let converted =
            migrate_schema2_steps(&steps, &summaries, &HashMap::new(), &HashMap::new()).unwrap();
        assert_eq!(converted.len(), 2);
        let leaf: StepRecord = serde_json::from_str(&converted["2"]).unwrap();
        assert_eq!(leaf.ordinal, 2);
        assert_eq!(leaf.dependencies, vec![0]);
        assert_eq!(leaf.kind, "do");
        summaries.remove("0");
        assert!(
            migrate_schema2_steps(&steps, &summaries, &HashMap::new(), &HashMap::new()).is_err()
        );
    }

    #[test]
    fn history_can_span_more_than_one_dispatch_turn() {
        let defaults = fixture()["defaults"].clone();
        let output = json!(vec![json!({"": 0}); 128]);
        assert!(output.to_string().len() < super::super::model::INLINE_STEP_PAYLOAD_BYTES_MAX);
        let mut steps = HashMap::new();
        let mut summaries = HashMap::new();
        for ordinal in 0..1500 {
            let mut value = defaults.clone();
            value["ordinal"] = json!(ordinal);
            value["nameCount"] = json!(ordinal + 1);
            value["dependencies"] = if ordinal == 0 {
                json!([])
            } else {
                json!([ordinal - 1])
            };
            value["output"] = output.clone();
            steps.insert(ordinal.to_string(), value.to_string());
            value["kind"] = json!("do");
            let typed: StepRecord = serde_json::from_value(value).unwrap();
            summaries.insert(ordinal.to_string(), step_summary_json(&typed).unwrap());
        }
        let converted =
            migrate_schema2_steps(&steps, &summaries, &HashMap::new(), &HashMap::new()).unwrap();
        assert_eq!(converted.len(), 1500);
        let last: StepRecord = serde_json::from_str(&converted["1499"]).unwrap();
        assert_eq!(last.dependencies, vec![1498]);
        assert_eq!(last.name_count, 1500);
        assert_eq!(last.output.as_ref(), Some(&output));

        let mut overwide: JsonValue = serde_json::from_str(&steps["1499"]).unwrap();
        overwide["dependencies"] = json!((0..=MAX_STEP_DEPENDENCIES).collect::<Vec<_>>());
        steps.insert("1499".to_string(), overwide.to_string());
        overwide["kind"] = json!("do");
        let typed: StepRecord = serde_json::from_value(overwide).unwrap();
        summaries.insert("1499".to_string(), step_summary_json(&typed).unwrap());
        assert!(
            migrate_schema2_steps(&steps, &summaries, &HashMap::new(), &HashMap::new()).is_err()
        );
    }
}
