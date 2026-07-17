//! Cross-crate primitives shared by WDL Rust services.
//!
//! This crate owns small behavior that must stay identical across Rust crates:
//! environment parsing, log-level parsing and common log-line formatting, HTTP
//! health probes, shutdown/in-flight tracking, metric storage/formatting, FNV hashing,
//! request-id sanitization, internal-auth constants/token matching, identity grammar,
//! worker contract and queue-key helpers, Redis connection and EVAL command helpers, time
//! helpers, structured error fields, test-only process-environment overrides, and
//! UTF-8-safe text helpers. It should not own service protocols, Redis schemas,
//! dispatch policy, or lifecycle behavior.

pub mod env;
pub mod hash;
pub mod health;
pub mod identity;
pub mod internal_auth;
pub mod log;
pub mod log_fields;
pub mod metrics;
pub mod queue_keys;
pub mod redis_conn;
pub mod redis_eval;
pub mod request_id;
pub mod shutdown;
#[cfg(any(test, feature = "test-support"))]
pub mod test_env;
pub mod text;
pub mod time;
pub mod worker_contract;

#[cfg(test)]
pub(crate) mod test_fixtures {
    pub(crate) fn observability_contract() -> serde_json::Value {
        serde_json::from_str(include_str!(
            "../../../tests/fixtures/observability-contract.json"
        ))
        .expect("observability contract fixture parses")
    }

    pub(crate) fn identity_cases(field: &str) -> Vec<(String, bool)> {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/cross-language-identity.json"
        ))
        .expect("identity fixture parses");
        fixture[field]
            .as_array()
            .expect("identity fixture field is an array")
            .iter()
            .map(|entry| {
                (
                    entry["value"]
                        .as_str()
                        .expect("identity fixture value is a string")
                        .to_string(),
                    entry["valid"]
                        .as_bool()
                        .expect("identity fixture valid is a boolean"),
                )
            })
            .collect()
    }
}
