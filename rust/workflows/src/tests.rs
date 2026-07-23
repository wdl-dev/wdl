use super::{
    COMMIT_RUNTIME_TERMINAL_SCRIPT, COMMIT_STEP_ERROR_SCRIPT, COMMIT_STEP_RECORD_SCRIPT,
    COMMIT_STEP_SUCCESS_SCRIPT, SEND_EVENT_SCRIPT,
};
use crate::api::{canonical_json, retry_due_at_ms, retry_policy};
use crate::{
    InstanceKeys, config_from_env, due_key, pending_version_key, ready_active_key, ready_key,
    schema_version_key, validate_instance_id_value,
};
use wdl_rust_common::test_env::with_temp_envs;
// Source-contract tests below intentionally couple to these module paths. If
// the files move, update the constants and the associated assertions together.
const RUNTIME_DISPATCH_SOURCE: &str = include_str!("api/tick/dispatch.rs");
const EXPECTED_EVENT_INDEX_STALE_SCAN_LIMIT: usize = 256;

fn temp_env<R>(items: &[(&str, Option<&str>)], f: impl FnOnce() -> R) -> R {
    let mut all_items = items.to_vec();
    if !all_items
        .iter()
        .any(|(key, _)| *key == "WDL_INTERNAL_AUTH_TOKEN")
    {
        all_items.push(("WDL_INTERNAL_AUTH_TOKEN", Some("test-internal-auth-token")));
    }
    if !all_items
        .iter()
        .any(|(key, _)| *key == "WORKFLOWS_DO_ALARM_DISPATCH_CONCURRENCY")
    {
        all_items.push(("WORKFLOWS_DO_ALARM_DISPATCH_CONCURRENCY", None));
    }
    with_temp_envs(&all_items, f)
}

#[test]
fn workflows_redis_url_defaults_to_db2() {
    temp_env(
        &[
            ("WORKFLOWS_REDIS_URL", None),
            ("CONTROL_REDIS_URL", None),
            ("WORKFLOWS_REDIS_DB", None),
            ("RUNTIME_HOST", None),
            ("RUNTIME_PORT", None),
            ("SYSTEM_RUNTIME_HOST", None),
            ("SYSTEM_RUNTIME_PORT", None),
            ("WORKFLOWS_DISPATCH_TIMEOUT_MS", None),
            ("WORKFLOWS_RUN_LEASE_MS", None),
            ("WORKFLOWS_DO_ALARM_CLAIM_LEASE_MS", None),
            ("WORKFLOWS_READY_DISPATCH_CONCURRENCY", None),
            ("WORKFLOWS_PROGRESS_CALLBACK_LOOKUP_CONCURRENCY", None),
            ("WORKFLOWS_PROGRESS_CALLBACK_CONCURRENCY", None),
            ("REDIS_URL", Some("redis://redis:6379")),
        ],
        || {
            let config = config_from_env();
            assert_eq!(config.redis_url, "redis://redis:6379/2");
            assert_eq!(config.control_redis_url, "redis://redis:6379");
            assert_eq!(config.runtime_host, "127.0.0.1");
            assert_eq!(config.runtime_port, 8088);
            assert_eq!(config.system_runtime_host, "127.0.0.1");
            assert_eq!(config.system_runtime_port, 8088);
            assert_eq!(config.run_lease_ms, 70_000);
            assert_eq!(config.do_alarm_claim_lease_ms, 300_000);
            assert_eq!(config.ready_dispatch_concurrency, 128);
            assert_eq!(config.do_alarm_dispatch_concurrency, 32);
            assert_eq!(config.progress_callback_lookup_concurrency, 128);
            assert_eq!(config.progress_callback_concurrency, 32);
        },
    );
}

#[test]
fn ready_dispatch_concurrency_stays_within_one_tick_batch() {
    for (configured, expected) in [("0", 128), ("73", 73), ("129", 128)] {
        temp_env(
            &[("WORKFLOWS_READY_DISPATCH_CONCURRENCY", Some(configured))],
            || {
                assert_eq!(config_from_env().ready_dispatch_concurrency, expected);
            },
        );
    }
}

#[test]
fn do_alarm_dispatch_concurrency_stays_within_one_tick_batch() {
    for (configured, expected) in [("0", 32), ("101", 100)] {
        temp_env(
            &[("WORKFLOWS_DO_ALARM_DISPATCH_CONCURRENCY", Some(configured))],
            || {
                assert_eq!(config_from_env().do_alarm_dispatch_concurrency, expected);
            },
        );
    }
}

#[test]
fn workflows_redis_db_applies_when_workflows_redis_url_unset() {
    temp_env(
        &[
            ("WORKFLOWS_REDIS_URL", None),
            ("CONTROL_REDIS_URL", None),
            ("WORKFLOWS_REDIS_DB", Some("9")),
            ("RUNTIME_HOST", None),
            ("RUNTIME_PORT", None),
            ("SYSTEM_RUNTIME_HOST", None),
            ("SYSTEM_RUNTIME_PORT", None),
            ("WORKFLOWS_DISPATCH_TIMEOUT_MS", None),
            ("WORKFLOWS_RUN_LEASE_MS", None),
            ("WORKFLOWS_DO_ALARM_CLAIM_LEASE_MS", None),
            ("WORKFLOWS_READY_DISPATCH_CONCURRENCY", None),
            ("WORKFLOWS_PROGRESS_CALLBACK_LOOKUP_CONCURRENCY", None),
            ("WORKFLOWS_PROGRESS_CALLBACK_CONCURRENCY", None),
            ("REDIS_URL", Some("redis://redis:6379")),
        ],
        || {
            let config = config_from_env();
            assert_eq!(config.redis_url, "redis://redis:6379/9");
            assert_eq!(config.control_redis_url, "redis://redis:6379");
        },
    );
}

#[test]
fn workflows_redis_db_replaces_existing_redis_url_db_suffix() {
    temp_env(
        &[
            ("WORKFLOWS_REDIS_URL", None),
            ("CONTROL_REDIS_URL", None),
            ("WORKFLOWS_REDIS_DB", Some("2")),
            ("RUNTIME_HOST", None),
            ("RUNTIME_PORT", None),
            ("SYSTEM_RUNTIME_HOST", None),
            ("SYSTEM_RUNTIME_PORT", None),
            ("WORKFLOWS_DISPATCH_TIMEOUT_MS", None),
            ("WORKFLOWS_RUN_LEASE_MS", None),
            ("WORKFLOWS_DO_ALARM_CLAIM_LEASE_MS", None),
            ("WORKFLOWS_READY_DISPATCH_CONCURRENCY", None),
            ("WORKFLOWS_PROGRESS_CALLBACK_LOOKUP_CONCURRENCY", None),
            ("WORKFLOWS_PROGRESS_CALLBACK_CONCURRENCY", None),
            ("REDIS_URL", Some("redis://redis:6379/1")),
        ],
        || {
            let config = config_from_env();
            assert_eq!(config.redis_url, "redis://redis:6379/2");
            assert_eq!(config.control_redis_url, "redis://redis:6379/1");
        },
    );
}

#[test]
fn workflows_redis_db_preserves_redis_url_query_suffix() {
    temp_env(
        &[
            ("WORKFLOWS_REDIS_URL", None),
            ("CONTROL_REDIS_URL", None),
            ("WORKFLOWS_REDIS_DB", Some("3")),
            ("RUNTIME_HOST", None),
            ("RUNTIME_PORT", None),
            ("SYSTEM_RUNTIME_HOST", None),
            ("SYSTEM_RUNTIME_PORT", None),
            ("WORKFLOWS_DISPATCH_TIMEOUT_MS", None),
            ("WORKFLOWS_RUN_LEASE_MS", None),
            ("WORKFLOWS_DO_ALARM_CLAIM_LEASE_MS", None),
            ("WORKFLOWS_READY_DISPATCH_CONCURRENCY", None),
            ("WORKFLOWS_PROGRESS_CALLBACK_LOOKUP_CONCURRENCY", None),
            ("WORKFLOWS_PROGRESS_CALLBACK_CONCURRENCY", None),
            ("REDIS_URL", Some("redis://redis:6379/1?protocol=resp3")),
        ],
        || {
            let config = config_from_env();
            assert_eq!(config.redis_url, "redis://redis:6379/3?protocol=resp3");
            assert_eq!(
                config.control_redis_url,
                "redis://redis:6379/1?protocol=resp3"
            );
        },
    );
}

#[test]
fn workflows_redis_db_preserves_redis_url_userinfo_and_ipv6_authority() {
    temp_env(
        &[
            ("WORKFLOWS_REDIS_URL", None),
            ("CONTROL_REDIS_URL", None),
            ("WORKFLOWS_REDIS_DB", Some("4")),
            ("RUNTIME_HOST", None),
            ("RUNTIME_PORT", None),
            ("SYSTEM_RUNTIME_HOST", None),
            ("SYSTEM_RUNTIME_PORT", None),
            ("WORKFLOWS_DISPATCH_TIMEOUT_MS", None),
            ("WORKFLOWS_RUN_LEASE_MS", None),
            ("WORKFLOWS_DO_ALARM_CLAIM_LEASE_MS", None),
            ("WORKFLOWS_READY_DISPATCH_CONCURRENCY", None),
            ("WORKFLOWS_PROGRESS_CALLBACK_LOOKUP_CONCURRENCY", None),
            ("WORKFLOWS_PROGRESS_CALLBACK_CONCURRENCY", None),
            (
                "REDIS_URL",
                Some("redis://user:pass@[::1]:6379/1?protocol=resp3"),
            ),
        ],
        || {
            let config = config_from_env();
            assert_eq!(
                config.redis_url,
                "redis://user:pass@[::1]:6379/4?protocol=resp3"
            );
            assert_eq!(
                config.control_redis_url,
                "redis://user:pass@[::1]:6379/1?protocol=resp3"
            );
        },
    );
}

#[test]
fn workflows_redis_url_uses_explicit_url() {
    temp_env(
        &[
            ("WORKFLOWS_REDIS_URL", Some("redis://other:6379/7")),
            ("CONTROL_REDIS_URL", Some("redis://control:6379")),
            ("RUNTIME_HOST", Some("runtime")),
            ("RUNTIME_PORT", Some("18088")),
            ("SYSTEM_RUNTIME_HOST", Some("system-runtime")),
            ("SYSTEM_RUNTIME_PORT", Some("18089")),
            ("WORKFLOWS_DISPATCH_TIMEOUT_MS", Some("1000")),
            ("WORKFLOWS_RUN_LEASE_MS", Some("12345")),
            ("WORKFLOWS_DO_ALARM_CLAIM_LEASE_MS", Some("23456")),
            ("WORKFLOWS_READY_DISPATCH_CONCURRENCY", Some("73")),
            ("WORKFLOWS_DO_ALARM_DISPATCH_CONCURRENCY", Some("41")),
            ("WORKFLOWS_PROGRESS_CALLBACK_LOOKUP_CONCURRENCY", Some("17")),
            ("WORKFLOWS_PROGRESS_CALLBACK_CONCURRENCY", Some("7")),
            ("REDIS_URL", Some("redis://redis:6379")),
            ("WORKFLOWS_REDIS_DB", Some("2")),
        ],
        || {
            let config = config_from_env();
            assert_eq!(config.redis_url, "redis://other:6379/7");
            assert_eq!(config.control_redis_url, "redis://control:6379");
            assert_eq!(config.runtime_host, "runtime");
            assert_eq!(config.runtime_port, 18088);
            assert_eq!(config.system_runtime_host, "system-runtime");
            assert_eq!(config.system_runtime_port, 18089);
            assert_eq!(config.run_lease_ms, 12345);
            assert_eq!(config.do_alarm_claim_lease_ms, 23456);
            assert_eq!(config.ready_dispatch_concurrency, 73);
            assert_eq!(config.do_alarm_dispatch_concurrency, 41);
            assert_eq!(config.progress_callback_lookup_concurrency, 17);
            assert_eq!(config.progress_callback_concurrency, 7);
        },
    );
}

#[test]
fn workflows_run_lease_clamps_above_dispatch_timeout() {
    temp_env(
        &[
            ("WORKFLOWS_REDIS_URL", Some("redis://other:6379/7")),
            ("CONTROL_REDIS_URL", Some("redis://control:6379")),
            ("RUNTIME_HOST", None),
            ("RUNTIME_PORT", None),
            ("SYSTEM_RUNTIME_HOST", None),
            ("SYSTEM_RUNTIME_PORT", None),
            ("WORKFLOWS_DISPATCH_TIMEOUT_MS", Some("60000")),
            ("WORKFLOWS_RUN_LEASE_MS", Some("12345")),
            ("WORKFLOWS_DO_ALARM_CLAIM_LEASE_MS", None),
            ("WORKFLOWS_READY_DISPATCH_CONCURRENCY", None),
            ("WORKFLOWS_PROGRESS_CALLBACK_LOOKUP_CONCURRENCY", None),
            ("WORKFLOWS_PROGRESS_CALLBACK_CONCURRENCY", None),
            ("REDIS_URL", Some("redis://redis:6379")),
            ("WORKFLOWS_REDIS_DB", Some("2")),
        ],
        || {
            let config = config_from_env();
            assert_eq!(config.dispatch_timeout_ms, 60_000);
            assert_eq!(config.run_lease_ms, 70_000);
            assert_eq!(config.do_alarm_claim_lease_ms, 300_000);
        },
    );
}

#[test]
fn workflows_do_alarm_claim_lease_clamps_above_dispatch_timeout() {
    temp_env(
        &[
            ("WORKFLOWS_REDIS_URL", Some("redis://other:6379/7")),
            ("CONTROL_REDIS_URL", Some("redis://control:6379")),
            ("RUNTIME_HOST", None),
            ("RUNTIME_PORT", None),
            ("SYSTEM_RUNTIME_HOST", None),
            ("SYSTEM_RUNTIME_PORT", None),
            ("WORKFLOWS_DISPATCH_TIMEOUT_MS", Some("60000")),
            ("WORKFLOWS_RUN_LEASE_MS", None),
            ("WORKFLOWS_DO_ALARM_CLAIM_LEASE_MS", Some("12345")),
            ("WORKFLOWS_READY_DISPATCH_CONCURRENCY", None),
            ("WORKFLOWS_PROGRESS_CALLBACK_LOOKUP_CONCURRENCY", None),
            ("WORKFLOWS_PROGRESS_CALLBACK_CONCURRENCY", None),
            ("REDIS_URL", Some("redis://redis:6379")),
            ("WORKFLOWS_REDIS_DB", Some("2")),
        ],
        || {
            let config = config_from_env();
            assert_eq!(config.dispatch_timeout_ms, 60_000);
            assert_eq!(config.do_alarm_claim_lease_ms, 70_000);
        },
    );
}

#[test]
fn workflow_instance_keys_share_cluster_hash_tag() {
    let keys = InstanceKeys::new("demo", "wf_abcd", "inst-1");
    let state = keys.state();
    let payloads = keys.payloads();
    let summaries = keys.step_summaries();
    let event_type_index = keys.event_type_index();
    assert_eq!(state, "wf:instance:{demo:wf_abcd:inst-1}:state");
    assert_eq!(payloads, "wf:instance:{demo:wf_abcd:inst-1}:payloads");
    assert_eq!(
        summaries,
        "wf:instance:{demo:wf_abcd:inst-1}:step-summaries"
    );
    assert_eq!(
        event_type_index,
        "wf:instance:{demo:wf_abcd:inst-1}:events-by-type"
    );
}

#[test]
fn workflow_ready_key_uses_fixed_shards() {
    assert_eq!(ready_active_key(), "wf:ready:active");
    assert_eq!(ready_key(0), "wf:ready:0");
    assert_eq!(ready_key(31), "wf:ready:31");
}

#[test]
fn pending_restart_key_is_scoped_to_the_target_version() {
    assert_eq!(
        pending_version_key("demo", "orders", "v3"),
        "wf:pending-version:demo:orders:v3"
    );
}

#[test]
fn workflow_schema_version_key_is_global_db2_marker() {
    assert_eq!(schema_version_key(), "wf:schema_version");
}

#[test]
fn workflow_due_key_uses_fixed_shards() {
    assert_eq!(due_key(0), "wf:due:0");
    assert_eq!(due_key(31), "wf:due:31");
}

#[test]
fn workflow_instance_id_rejects_token_delimiters_and_bad_shape() {
    for (invalid, expected_message_fragment) in [
        ("", "instanceId is required"),
        ("_leading", "instanceId must match"),
        ("has:colon", "instanceId must match"),
        ("has\ttab", "instanceId must match"),
        ("has/slash", "instanceId must match"),
    ] {
        let err = match validate_instance_id_value(invalid) {
            Ok(_) => panic!("id `{invalid}` should be rejected"),
            Err(err) => err,
        };
        assert_eq!(err.code, "invalid_request");
        assert!(
            err.message.contains(expected_message_fragment),
            "expected message for `{invalid}` to mention `{expected_message_fragment}`, got: {}",
            err.message
        );
    }
    validate_instance_id_value("Order_123-ok").expect("safe id should be accepted");
}

#[test]
fn workflow_retry_policy_rejects_non_integer_fields() {
    let limit_err = retry_policy(&serde_json::json!({
        "retries": { "limit": 1.5, "delayMs": 10 }
    }))
    .expect_err("fractional retry limit should be rejected");
    assert_eq!(limit_err.code, "invalid_request");
    assert!(limit_err.message.contains("limit must be an integer"));

    let delay_err = retry_policy(&serde_json::json!({
        "retries": { "limit": 2, "delayMs": "10" }
    }))
    .expect_err("string retry delay should be rejected");
    assert_eq!(delay_err.code, "invalid_request");
    assert!(delay_err.message.contains("delayMs must be an integer"));
}

#[test]
fn workflow_retry_policy_accepts_explicit_retry_config() {
    let policy = retry_policy(&serde_json::json!({
        "retries": { "limit": 3, "delayMs": 250, "backoff": "linear" }
    }))
    .expect("valid retry config");
    assert_eq!(policy.limit, 3);
    assert_eq!(policy.delay_ms, 250);
    assert_eq!(policy.backoff, "linear");
    assert_eq!(retry_due_at_ms(1_000, &policy, 2), 1_500);
}

#[test]
fn workflow_step_config_json_is_canonical() {
    let left = serde_json::json!({
        "b": 2,
        "a": { "z": true, "m": [3, { "y": null, "x": "ok" }] }
    });
    let right = serde_json::json!({
        "a": { "m": [3, { "x": "ok", "y": null }], "z": true },
        "b": 2
    });
    let encoded = canonical_json(&left).expect("left config should encode");
    assert_eq!(
        encoded,
        canonical_json(&right).expect("right config should encode")
    );
    assert_eq!(
        encoded,
        r#"{"a":{"m":[3,{"x":"ok","y":null}],"z":true},"b":2}"#
    );
}

#[test]
fn workflow_payload_writes_enforce_instance_aggregate_cap() {
    for script in [
        COMMIT_STEP_SUCCESS_SCRIPT,
        COMMIT_STEP_ERROR_SCRIPT,
        COMMIT_STEP_RECORD_SCRIPT,
        COMMIT_RUNTIME_TERMINAL_SCRIPT,
        SEND_EVENT_SCRIPT,
    ] {
        if script == SEND_EVENT_SCRIPT {
            assert!(
                script.contains("generation ~= ARGV[6]"),
                "send-event commit must fence against concurrent restart"
            );
            assert!(
                script.contains("current_event_seq + 1"),
                "send-event must compute the candidate event sequence before mutating state"
            );
            let cap_pos = script.find("next_payload_bytes > tonumber").unwrap();
            let state_write_pos = script.find(r#"redis.call("HSET", KEYS[1]"#).unwrap();
            assert!(
                cap_pos < state_write_pos,
                "send-event must check the aggregate cap before advancing eventSeq"
            );
        }
        assert!(
            script.contains("payloadBytes"),
            "script must maintain the per-instance aggregate payload counter"
        );
        assert!(
            script.contains("next_payload_bytes > tonumber"),
            "script must reject writes that exceed the aggregate payload cap"
        );
        assert!(
            script.contains("tonumber(ARGV["),
            "script must read the configured aggregate payload cap from script arguments"
        );
        assert!(
            script.contains("next_payload_bytes > tonumber(ARGV["),
            "script must compare computed aggregate payload bytes against the configured cap"
        );
        assert!(
            script.contains("return -1") || script.contains("return 2"),
            "script must surface aggregate-cap failures distinctly"
        );
    }
}

#[test]
fn workflow_payload_hash_writes_are_counter_guarded() {
    let scripts = [
        ("COMMIT_STEP_SUCCESS_SCRIPT", COMMIT_STEP_SUCCESS_SCRIPT),
        ("COMMIT_STEP_ERROR_SCRIPT", COMMIT_STEP_ERROR_SCRIPT),
        ("COMMIT_STEP_RECORD_SCRIPT", COMMIT_STEP_RECORD_SCRIPT),
        (
            "COMMIT_RUNTIME_TERMINAL_SCRIPT",
            COMMIT_RUNTIME_TERMINAL_SCRIPT,
        ),
        ("SEND_EVENT_SCRIPT", SEND_EVENT_SCRIPT),
    ];
    for (name, script) in scripts {
        let writes_payload_hash = script.contains(r#"redis.call("HSET", KEYS[2]"#)
            || script.contains(r#"redis.call("HSET", KEYS[4]"#);
        if writes_payload_hash {
            assert!(
                script.contains("payloadBytes"),
                "{name} writes the instance payload hash and must maintain payloadBytes"
            );
        }
        if script.contains("next_payload_bytes") {
            assert!(
                script.contains("next_payload_bytes > tonumber"),
                "{name} increments payloadBytes and must enforce the aggregate cap"
            );
            assert!(
                script.contains("return -1") || script.contains("return 2"),
                "{name} must surface aggregate-cap failures distinctly"
            );
        }
    }
}

#[test]
fn workflow_runtime_dispatch_reads_response_with_byte_cap() {
    let source = RUNTIME_DISPATCH_SOURCE;
    assert!(
        source.contains("read_runtime_response_text"),
        "runtime dispatch must route response reads through the bounded reader"
    );
    assert!(
        source.contains("MAX_WORKFLOW_RUNTIME_RESPONSE_BYTES"),
        "runtime response reads must enforce a byte cap before JSON parsing"
    );
    assert!(
        source.contains(".chunk().await"),
        "runtime response reads must stream chunks instead of buffering the full body"
    );
    assert!(
        !source.contains(".text().await"),
        "runtime response reads must not use reqwest response.text()"
    );
}

#[test]
fn workflow_runtime_dispatch_checks_status_before_json_parse() {
    let source = RUNTIME_DISPATCH_SOURCE;
    let status_check = source
        .find("if !status.is_success()")
        .expect("runtime dispatch should check HTTP status");
    let json_parse = source
        .find("serde_json::from_str(&text)")
        .expect("runtime dispatch should parse JSON bodies");
    assert!(
        status_check < json_parse,
        "HTTP status errors should not be masked by JSON parse failures"
    );
}

#[test]
fn workflow_admission_isolates_per_instance_dispatch_errors() {
    let tick_source = include_str!("api/tick.rs");
    assert!(
        tick_source.contains("ReadyTokenResult::DispatchError"),
        "runtime dispatch errors must become a per-token result instead of aborting the whole tick"
    );
    assert!(
        tick_source.contains("workflow_dispatch_error"),
        "isolated dispatch errors should still be visible in structured logs"
    );
}

#[test]
fn workflow_runtime_dispatch_timeout_releases_run_claim_unlike_do_alarm_dispatch() {
    let tick_source = include_str!("api/tick.rs");
    let runtime_dispatch_source = RUNTIME_DISPATCH_SOURCE;
    let do_alarm_dispatch_source = include_str!("api/do_alarms/dispatch.rs");

    assert!(
        runtime_dispatch_source
            .contains(".timeout(Duration::from_millis(app.config.dispatch_timeout_ms))"),
        "ordinary workflow runtime dispatch must keep using the explicit dispatch timeout boundary"
    );
    assert!(
        tick_source.contains("Err(err) => {\n            app.metrics")
            && tick_source
                .contains("release_run_claim(app, &identity, &claim, &previous_status).await?;"),
        "ordinary workflow dispatch errors, including reqwest timeouts, currently release the run claim for retry"
    );
    assert!(
        do_alarm_dispatch_source.contains("DoAlarmDispatchError::InFlightUnknown"),
        "DO alarms intentionally preserve running claims on timeout instead of immediately retrying"
    );
}

#[test]
fn workflow_step_metadata_writes_are_counter_guarded() {
    for (name, script) in [
        ("COMMIT_STEP_SUCCESS_SCRIPT", COMMIT_STEP_SUCCESS_SCRIPT),
        ("COMMIT_STEP_ERROR_SCRIPT", COMMIT_STEP_ERROR_SCRIPT),
        ("COMMIT_STEP_RECORD_SCRIPT", COMMIT_STEP_RECORD_SCRIPT),
    ] {
        assert!(
            script.contains("string.len(ARGV[6])"),
            "{name} must count the step record JSON against the instance byte cap"
        );
        assert!(
            script.contains("string.len(ARGV[8])")
                || script.contains("string.len(ARGV[12])")
                || script.contains("string.len(ARGV[14])"),
            "{name} must count the step summary JSON against the instance byte cap"
        );
        assert!(
            script.contains("old_record_bytes") && script.contains("old_summary_bytes"),
            "{name} must charge replacement records by delta to keep idempotent waiting rewrites stable"
        );
    }
    assert!(
        SEND_EVENT_SCRIPT.contains("string.len(record_json)"),
        "sendEvent must count event metadata against the instance byte cap"
    );
}

#[test]
fn workflow_ready_writes_update_active_shard_index() {
    let ready_source = include_str!("api/tick/ready.rs");
    let shared_source = include_str!("api/sharded_dispatch.rs");
    assert!(
        ready_source.contains(r#"redis.call("SADD", KEYS[2], ARGV[1])"#),
        "due-token promotion must add tokens to the ready shard set"
    );
    assert!(
        ready_source.contains(r#"redis.call("SADD", KEYS[3], ARGV[3])"#)
            && shared_source.contains(r#"keys.ready_active()"#),
        "due-token promotion must also mark the ready shard active"
    );
    assert!(
        SEND_EVENT_SCRIPT.contains(r#"redis.call("SADD", KEYS[4], ARGV[4])"#)
            && SEND_EVENT_SCRIPT.contains(r#"redis.call("SADD", KEYS[5], ARGV[7])"#),
        "sendEvent must write the ready token and ready-active index together"
    );
}

#[test]
fn workflow_do_alarm_mutations_avoid_whole_set_redis_commands() {
    let source = include_str!("api/do_alarms/mutations.rs").to_ascii_uppercase();
    for command in ["COPY", "SMEMBERS"] {
        assert!(
            !source.contains(&format!(r#"CMD("{command}")"#)),
            "DO alarm mutations must not use whole-set {command}"
        );
    }
}

#[test]
fn workflow_step_commits_update_records_summaries_and_summary_index() {
    for (name, script) in [
        ("COMMIT_STEP_SUCCESS_SCRIPT", COMMIT_STEP_SUCCESS_SCRIPT),
        ("COMMIT_STEP_ERROR_SCRIPT", COMMIT_STEP_ERROR_SCRIPT),
        ("COMMIT_STEP_RECORD_SCRIPT", COMMIT_STEP_RECORD_SCRIPT),
    ] {
        assert!(
            script.contains(r#"redis.call("HSET", KEYS[3], ARGV[5], ARGV[6])"#),
            "{name} must write the authoritative step record"
        );
        assert!(
            script.contains(r#"redis.call("HSET", KEYS[4], ARGV[5],"#),
            "{name} must write the bounded status summary"
        );
        assert!(
            script.contains(r#"redis.call("ZADD", KEYS[5], tonumber(ARGV[5]), ARGV[5])"#),
            "{name} must index the summary field for bounded includeSteps reads"
        );
    }
}

#[test]
fn workflow_step_commits_require_active_instance_status() {
    for (name, script) in [
        ("COMMIT_STEP_SUCCESS_SCRIPT", COMMIT_STEP_SUCCESS_SCRIPT),
        ("COMMIT_STEP_ERROR_SCRIPT", COMMIT_STEP_ERROR_SCRIPT),
        ("COMMIT_STEP_RECORD_SCRIPT", COMMIT_STEP_RECORD_SCRIPT),
    ] {
        assert!(
            script.contains(r#"local status = redis.call("HGET", KEYS[1], "status")"#),
            "{name} must read instance status before committing step state"
        );
        assert!(
            script.contains(r#"if status ~= "running" and status ~= "waiting" then"#),
            "{name} must reject stale step commits after the run leaves active execution state"
        );
        assert!(
            script.contains(r#"runLeaseExpiresAtMs"#)
                && script.contains(r#"if lease <= tonumber(ARGV["#),
            "{name} must reject stale step commits after the run claim lease expires"
        );
    }
}

#[test]
fn workflow_step_execution_fences_stay_inside_db2_lua() {
    let source = format!(
        "{}\n{}\n{}\n{}",
        include_str!("api/execution.rs"),
        include_str!("api/execution/history.rs"),
        include_str!("api/execution/sleep.rs"),
        include_str!("api/execution/events.rs")
    );
    assert!(
        source.contains("READ_STEP_RECORD_SCRIPT"),
        "step replay reads must go through the same DB2 active-claim fence as step commits"
    );
    assert!(
        source.contains("READ_REPLAY_STEP_PAGE_SCRIPT"),
        "step replay pages must go through the same DB2 active-claim fence as step claims"
    );
    assert!(
        !source.contains("verify_step_instance"),
        "step execution must not keep a separate preflight verifier outside the Redis script fence"
    );
    assert!(
        !source.contains("verify_workflow_def_values"),
        "step execution must not read DB0 workflow definitions per mutation"
    );
    assert!(
        !source.contains("read_public_state_by_id"),
        "step execution must not pre-read DB2 state outside script-owned claim fences"
    );
}

#[test]
fn workflow_wait_steps_record_event_index_prefix_for_lost_wakeup_protection() {
    assert!(
        COMMIT_STEP_RECORD_SCRIPT.contains(r#""waitingEventIndexPrefix", ARGV[16]"#),
        "waiting waitForEvent records must preserve the event type prefix for suspended claim clear"
    );
    assert!(
        COMMIT_STEP_RECORD_SCRIPT
            .contains(r#"redis.call("HDEL", KEYS[1], "waitingEventIndexPrefix")"#),
        "non-waiting step records must clear stale wait-event wake metadata"
    );
}

#[test]
fn workflow_event_buffering_uses_type_index() {
    let events_source = include_str!("api/execution/events.rs");
    assert!(
        SEND_EVENT_SCRIPT.contains(r#"redis.call("ZADD", KEYS[6], 0"#),
        "sendEvent must write the event-type index with the event record"
    );
    assert!(
        events_source.contains(r#"redis::cmd("ZRANGEBYLEX")"#),
        "waitForEvent must query the event-type index instead of scanning every event"
    );
    assert!(
        events_source.contains(r#".arg("LIMIT")"#),
        "waitForEvent must read event-type index candidates in bounded batches"
    );
    assert!(
        events_source.contains("EVENT_INDEX_SCAN_BATCH_SIZE"),
        "waitForEvent batch size must stay explicit and reviewable"
    );
    assert!(
        events_source.contains(&format!(
            "const EVENT_INDEX_STALE_SCAN_LIMIT: usize = {EXPECTED_EVENT_INDEX_STALE_SCAN_LIMIT};"
        )),
        "waitForEvent stale-index cleanup must stay bounded"
    );
    assert!(
        !events_source.contains(r#"redis::cmd("HGETALL")"#),
        "waitForEvent must not HGETALL the full event hash"
    );
    assert!(
        COMMIT_STEP_RECORD_SCRIPT.contains(r#"redis.call("ZREM", KEYS[9], ARGV[15])"#),
        "consuming a buffered event must remove it from the event-type index"
    );
}

#[test]
fn workflow_replay_payload_misses_use_stable_error_code() {
    let source = format!(
        "{}\n{}\n{}\n{}",
        include_str!("api/execution.rs"),
        include_str!("api/execution/history.rs"),
        include_str!("api/execution/events.rs"),
        include_str!("api/lifecycle/restart.rs")
    );
    for message in [
        "Workflow step output payload",
        "Workflow step error payload",
        "Workflow replay payload",
        "Workflow event payload",
        "Workflow wait output payload",
        "Workflow params payload",
    ] {
        let pos = source
            .find(message)
            .unwrap_or_else(|| panic!("{message} should be present"));
        let window_start = pos.saturating_sub(120);
        let window = &source[window_start..pos];
        assert!(
            window.contains("WorkflowError::payload_missing"),
            "{message} missing path must use workflow_payload_missing"
        );
    }
}
