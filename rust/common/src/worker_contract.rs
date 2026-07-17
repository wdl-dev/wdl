//! Worker version grammar and control-plane key helpers shared by Rust services.
//!
//! JavaScript control code owns the canonical version tag grammar in
//! `shared/worker-contract.js`: `v[1-9][0-9]*`, bounded to JavaScript's safe-integer
//! range. Rust services that read bundle hashes must use the same grammar so malformed
//! Redis state fails closed instead of silently normalizing to another worker version.
//!
//! `routes_key` / `worker_versions_key` / `worker_delete_lock_key` /
//! `do_storage_id_key` mirror `shared/worker-contract.js`'s lifecycle key builders.
//! Control owns these keys; Rust readers must build them here so a future
//! key-grammar change updates JS and Rust together.

use std::fmt;

const MAX_SAFE_VERSION: u64 = 9_007_199_254_740_991;

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
    }
}
