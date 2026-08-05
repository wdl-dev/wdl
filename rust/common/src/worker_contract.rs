//! Worker version grammar and control-plane key helpers shared by Rust services.
//!
//! JavaScript control code owns the canonical version tag grammar in
//! `shared/worker-contract.js`: `v[1-9][0-9]*`, bounded to JavaScript's safe-integer
//! range. Rust services that read bundle hashes must use the same grammar so malformed
//! Redis state fails closed instead of silently normalizing to another worker version.
//!
//! `routes_key` / `worker_versions_key` / `worker_delete_lock_key` /
//! `do_storage_id_key` / `session_policy_key` mirror
//! `shared/worker-contract.js`'s lifecycle key builders. Control owns these keys; Rust
//! readers must build them here so a future key-grammar change updates JS and Rust
//! together.

use std::fmt;

const MAX_SAFE_VERSION: u64 = crate::JS_MAX_SAFE_INTEGER;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidVersionTag;

impl fmt::Display for InvalidVersionTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid version tag")
    }
}

impl std::error::Error for InvalidVersionTag {}

pub fn parse_version_tag(version: &str) -> Result<u64, InvalidVersionTag> {
    let raw = version.strip_prefix('v').ok_or(InvalidVersionTag)?;
    if raw.is_empty() || raw.starts_with('0') || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(InvalidVersionTag);
    }
    let parsed = raw.parse::<u64>().map_err(|_| InvalidVersionTag)?;
    (parsed <= MAX_SAFE_VERSION)
        .then_some(parsed)
        .ok_or(InvalidVersionTag)
}

pub fn worker_bundle_key(
    ns: &str,
    worker: &str,
    version: &str,
) -> Result<String, InvalidVersionTag> {
    let version = parse_version_tag(version)?;
    Ok(format!("worker:{ns}:{worker}:v:{version}"))
}

/// Active-route hash for a namespace: field=workerName, value=`v<int>`.
pub fn routes_key(ns: &str) -> String {
    format!("routes:{ns}")
}

/// Retained-version ZSET for a worker: score=int version, member=`v<int>`.
pub fn worker_versions_key(ns: &str, worker: &str) -> String {
    format!("worker-versions:{ns}:{worker}")
}

/// Worker-scoped Control mutation lock shared with lifecycle readers.
pub fn worker_delete_lock_key(ns: &str, worker: &str) -> String {
    format!("worker-delete-lock:{ns}:{worker}")
}

/// Logical Worker -> Durable Object storage pointer. Control owns writes; DO
/// runtime and workflows read it for owner/storage fencing.
pub fn do_storage_id_key(ns: &str, worker: &str) -> String {
    format!("worker:do-storage:{ns}:{worker}")
}

/// Active session policy projection committed with the route flip.
pub fn session_policy_key(ns: &str, worker: &str) -> String {
    format!("worker:session-policy:{ns}:{worker}")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionPolicyMode {
    Preserve,
    Restart,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionPolicyProjection {
    pub version: String,
    pub mode: SessionPolicyMode,
    pub restart_sequence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidSessionPolicyProjection;

impl fmt::Display for InvalidSessionPolicyProjection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid session policy projection")
    }
}

impl std::error::Error for InvalidSessionPolicyProjection {}

pub fn parse_session_policy_projection(
    raw: Option<&str>,
) -> Result<Option<SessionPolicyProjection>, InvalidSessionPolicyProjection> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let value: serde_json::Value =
        serde_json::from_str(raw).map_err(|_| InvalidSessionPolicyProjection)?;
    let object = value.as_object().ok_or(InvalidSessionPolicyProjection)?;
    let version = object
        .get("version")
        .and_then(serde_json::Value::as_str)
        .ok_or(InvalidSessionPolicyProjection)?;
    parse_version_tag(version).map_err(|_| InvalidSessionPolicyProjection)?;
    let mode = match object.get("mode").and_then(serde_json::Value::as_str) {
        Some("preserve") => SessionPolicyMode::Preserve,
        Some("restart") => SessionPolicyMode::Restart,
        _ => return Err(InvalidSessionPolicyProjection),
    };
    let restart_sequence = object
        .get("restartSequence")
        .and_then(json_safe_integer)
        .ok_or(InvalidSessionPolicyProjection)?;
    if mode == SessionPolicyMode::Restart && restart_sequence == 0 {
        return Err(InvalidSessionPolicyProjection);
    }
    Ok(Some(SessionPolicyProjection {
        version: version.to_string(),
        mode,
        restart_sequence,
    }))
}

fn json_safe_integer(value: &serde_json::Value) -> Option<u64> {
    const JS_SAFE_INTEGER_EXCLUSIVE_UPPER_BOUND: f64 = 9_007_199_254_740_992.0;

    if let Some(integer) = value.as_u64() {
        return (integer <= crate::JS_MAX_SAFE_INTEGER).then_some(integer);
    }
    let number = value.as_f64()?;
    (number.is_finite()
        && number >= 0.0
        && number.fract() == 0.0
        && number < JS_SAFE_INTEGER_EXCLUSIVE_UPPER_BOUND)
        .then_some(number as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_canonical_version_tags() {
        assert_eq!(parse_version_tag("v1").unwrap(), 1);
        assert_eq!(parse_version_tag("v42").unwrap(), 42);
    }

    #[test]
    fn matches_cross_language_version_fixture() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../../tests/fixtures/version-tags.json"))
                .expect("version tag fixture parses");
        for case in fixture["cases"]
            .as_array()
            .expect("version tag fixture cases is an array")
        {
            let tag = case["tag"].as_str().expect("version tag is a string");
            let expected = case["parsed"].as_u64();
            assert_eq!(parse_version_tag(tag).ok(), expected, "{tag:?}");
        }
    }

    #[test]
    fn rejects_malformed_version_tags() {
        for version in ["", "v", "v0", "v01", "1", "V1", "v1a"] {
            assert_eq!(parse_version_tag(version), Err(InvalidVersionTag));
            assert_eq!(
                worker_bundle_key("tenant", "worker", version),
                Err(InvalidVersionTag)
            );
        }
    }

    #[test]
    fn composes_worker_bundle_keys() {
        assert_eq!(
            worker_bundle_key("tenant", "worker", "v42").unwrap(),
            "worker:tenant:worker:v:42"
        );
    }

    #[test]
    fn composes_route_and_version_keys() {
        assert_eq!(routes_key("tenant"), "routes:tenant");
        assert_eq!(
            worker_versions_key("tenant", "worker"),
            "worker-versions:tenant:worker"
        );
        assert_eq!(
            worker_delete_lock_key("tenant", "worker"),
            "worker-delete-lock:tenant:worker"
        );
        assert_eq!(
            do_storage_id_key("tenant", "worker"),
            "worker:do-storage:tenant:worker"
        );
        assert_eq!(
            session_policy_key("tenant", "worker"),
            "worker:session-policy:tenant:worker"
        );
    }

    #[test]
    fn session_policy_projection_matches_cross_language_fixture() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/session-policy-projections.json"
        ))
        .expect("session policy fixture parses");
        for case in fixture["cases"]
            .as_array()
            .expect("session policy fixture cases is an array")
        {
            let raw = serde_json::to_string(&case["projection"])
                .expect("session policy projection serializes");
            let parsed = parse_session_policy_projection(Some(&raw));
            if case["valid"].as_bool().unwrap() {
                let projection = parsed
                    .expect("valid session policy projection parses")
                    .expect("fixture projection is present");
                let expected_mode = match case["projection"]["mode"].as_str().unwrap() {
                    "preserve" => SessionPolicyMode::Preserve,
                    "restart" => SessionPolicyMode::Restart,
                    mode => panic!("unexpected valid session policy mode {mode}"),
                };
                assert_eq!(
                    projection,
                    SessionPolicyProjection {
                        version: case["projection"]["version"].as_str().unwrap().to_string(),
                        mode: expected_mode,
                        restart_sequence: json_safe_integer(&case["projection"]["restartSequence"])
                            .expect("valid restart sequence"),
                    },
                    "{raw}"
                );
            } else {
                assert!(parsed.is_err(), "{raw}");
            }
        }
        assert_eq!(parse_session_policy_projection(None), Ok(None));
    }
}
