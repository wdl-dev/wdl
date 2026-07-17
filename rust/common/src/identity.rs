//! Runtime worker identity grammar shared by Rust services.
//!
//! JavaScript owns the canonical tenant namespace, route namespace, worker-name,
//! and version grammars in `shared/ns-pattern.js` and `shared/worker-contract.js`. Rust
//! services that sit on protocol boundaries mirror those rules here so internal
//! requests cannot reach Redis key shapes that control would never write.

use crate::worker_contract::parse_version_tag;

pub fn is_valid_tenant_ns(ns: &str) -> bool {
    let bytes = ns.as_bytes();
    if bytes.is_empty()
        || bytes.len() > 63
        || ns == "admin"
        || is_reserved_ns(ns)
        || !bytes[0].is_ascii_alphanumeric()
        || !bytes[bytes.len() - 1].is_ascii_alphanumeric()
    {
        return false;
    }
    bytes
        .iter()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
}

pub fn is_valid_route_ns(ns: &str) -> bool {
    is_valid_tenant_ns(ns) || ns == "__system__"
}

pub fn is_valid_runtime_load_ns(ns: &str) -> bool {
    is_valid_route_ns(ns) || is_platform_tier_reserved_ns(ns)
}

pub fn is_valid_worker_name(worker: &str) -> bool {
    let bytes = worker.as_bytes();
    if bytes.is_empty() || bytes.len() > 255 {
        return false;
    }
    let first = bytes[0];
    first.is_ascii_alphanumeric()
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_' || *byte == b'-')
}

pub fn is_valid_runtime_worker_identity(ns: &str, worker: &str, version: &str) -> bool {
    is_valid_runtime_load_ns(ns)
        && is_valid_worker_name(worker)
        && parse_version_tag(version).is_ok()
}

fn is_reserved_ns(ns: &str) -> bool {
    matches!(ns, "__system__" | "__platform__" | "__community__")
}

fn is_platform_tier_reserved_ns(ns: &str) -> bool {
    ns == "__platform__"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::identity_cases;

    #[test]
    fn identity_grammar_matches_cross_language_fixture() {
        for (value, valid) in identity_cases("tenantNs") {
            assert_eq!(is_valid_tenant_ns(&value), valid, "tenantNs:{value:?}");
        }
        for (value, valid) in identity_cases("routeNs") {
            assert_eq!(is_valid_route_ns(&value), valid, "routeNs:{value:?}");
        }
        for (value, valid) in identity_cases("runtimeLoadNs") {
            assert_eq!(
                is_valid_runtime_load_ns(&value),
                valid,
                "runtimeLoadNs:{value:?}"
            );
        }
        for (value, valid) in identity_cases("workerNames") {
            assert_eq!(is_valid_worker_name(&value), valid, "workerNames:{value:?}");
        }
    }

    #[test]
    fn route_ns_accepts_tenants_and_system_only() {
        assert!(is_valid_route_ns("demo"));
        assert!(is_valid_route_ns("a-b-1"));
        assert!(is_valid_route_ns(&"a".repeat(63)));
        assert!(is_valid_route_ns("__system__"));

        for ns in [
            "",
            "-",
            "-demo",
            "demo-",
            &"a".repeat(64),
            "Demo",
            "admin",
            "__platform__",
            "__community__",
            "bad_name",
        ] {
            assert!(!is_valid_route_ns(ns), "{ns}");
        }
    }

    #[test]
    fn runtime_load_ns_accepts_platform_tier_reserved_namespaces() {
        assert!(is_valid_runtime_load_ns("demo"));
        assert!(is_valid_runtime_load_ns("__system__"));
        assert!(is_valid_runtime_load_ns("__platform__"));
        assert!(!is_valid_runtime_load_ns("__community__"));
    }

    #[test]
    fn worker_name_matches_js_control_grammar() {
        for worker in ["Worker", "worker_1", "worker-1", "A"] {
            assert!(is_valid_worker_name(worker), "{worker}");
        }
        for worker in [
            "",
            "_worker",
            "-worker",
            "bad/name",
            "bad.name",
            &"a".repeat(256),
        ] {
            assert!(!is_valid_worker_name(worker), "{worker}");
        }
    }

    #[test]
    fn runtime_worker_identity_requires_route_ns_worker_and_version() {
        assert!(is_valid_runtime_worker_identity("demo", "Worker_1", "v42"));
        assert!(is_valid_runtime_worker_identity(
            "__system__",
            "s3-cleanup",
            "v1"
        ));
        assert!(is_valid_runtime_worker_identity(
            "__platform__",
            "platform-api",
            "v1"
        ));

        for (ns, worker, version) in [
            ("Demo", "worker", "v1"),
            ("__community__", "worker", "v1"),
            ("demo", "/bad", "v1"),
            ("demo", "worker", "v0"),
            ("demo", "worker", "v01"),
        ] {
            assert!(!is_valid_runtime_worker_identity(ns, worker, version));
        }
    }
}
