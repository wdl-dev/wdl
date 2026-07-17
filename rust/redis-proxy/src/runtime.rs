use std::collections::HashMap;

use axum::extract::{Query, State};
use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::json;
use wdl_rust_common::identity::is_valid_runtime_worker_identity;
use wdl_rust_common::worker_contract::worker_bundle_key;

use crate::{AppError, AppResult, AppState};

pub(crate) const RUNTIME_LOAD_MAGIC: &[u8] = b"WDLLOAD!";

fn runtime_load_content_type() -> &'static str {
    "application/vnd.wdl.runtime-load"
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeLoadParams {
    ns: String,
    worker: String,
    version: String,
}
// Bundles are stored by integer ("worker:<ns>:<name>:v:42"), not the
// `v<int>` tag. Strip the prefix and validate so the Redis key shape
// matches how the control-plane writer (shared/worker-contract.js#bundleKey)
// spells it; otherwise a cold load misses the hash entirely.
pub(crate) fn bundle_key(ns: &str, worker: &str, version: &str) -> AppResult<String> {
    if !is_valid_runtime_worker_identity(ns, worker, version) {
        return Err(AppError::bad_request("invalid runtime worker identity"));
    }
    worker_bundle_key(ns, worker, version)
        .map_err(|_| AppError::bad_request(format!("invalid version {version}")))
}

pub(crate) type HashEntries = Vec<(String, Vec<u8>)>;

#[cfg(test)]
pub(crate) fn hash_to_text_object(items: HashEntries) -> AppResult<HashMap<String, String>> {
    let mut out = HashMap::with_capacity(items.len());
    for (key, value) in items {
        let value = String::from_utf8(value)
            .map_err(|_| AppError::internal_error("secret value is not valid utf-8"))?;
        out.insert(key, value);
    }
    Ok(out)
}

fn push_u32(out: &mut Vec<u8>, value: usize) -> AppResult<()> {
    let value = u32::try_from(value)
        .map_err(|_| AppError::internal_error("runtime load payload field exceeds u32"))?;
    out.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

pub(crate) fn encode_runtime_load(
    bundle: HashEntries,
    ns_secrets: HashMap<String, String>,
    worker_secrets: HashMap<String, String>,
) -> AppResult<Vec<u8>> {
    let header = serde_json::to_vec(&json!({
        "ns_secrets": ns_secrets,
        "worker_secrets": worker_secrets,
    }))
    .map_err(AppError::internal_json)?;

    let bundle_bytes = bundle.iter().try_fold(0usize, |total, (key, value)| {
        let entry_len = 8usize
            .checked_add(key.len())
            .and_then(|n| n.checked_add(value.len()))
            .ok_or_else(|| AppError::internal_error("runtime load payload too large"))?;
        total
            .checked_add(entry_len)
            .ok_or_else(|| AppError::internal_error("runtime load payload too large"))
    })?;
    let magic_len = RUNTIME_LOAD_MAGIC.len();
    let capacity = magic_len
        .checked_add(4)
        .and_then(|n| n.checked_add(header.len()))
        .and_then(|n| n.checked_add(4))
        .and_then(|n| n.checked_add(bundle_bytes))
        .ok_or_else(|| AppError::internal_error("runtime load payload too large"))?;

    let mut out = Vec::with_capacity(capacity);
    out.extend_from_slice(RUNTIME_LOAD_MAGIC);
    push_u32(&mut out, header.len())?;
    out.extend_from_slice(&header);
    push_u32(&mut out, bundle.len())?;
    for (key, value) in bundle {
        push_u32(&mut out, key.len())?;
        push_u32(&mut out, value.len())?;
        out.extend_from_slice(key.as_bytes());
        out.extend_from_slice(&value);
    }
    Ok(out)
}

pub(crate) async fn runtime_load(
    State(state): State<AppState>,
    Query(q): Query<RuntimeLoadParams>,
) -> AppResult<Response> {
    let key = bundle_key(&q.ns, &q.worker, &q.version)?;
    let ns_secrets_key = format!("secrets:{}", q.ns);
    let worker_secrets_key = format!("secrets:{}:{}", q.ns, q.worker);
    // Non-atomic: a concurrent secret PUT can straddle the three HGETALLs.
    // Cold-load tolerates a torn read.
    let (bundle, ns_secrets, worker_secrets): (HashEntries, HashEntries, HashEntries) = state
        .with_control_redis(async |mut conn| {
            redis::pipe()
                .cmd("HGETALL")
                .arg(key)
                .cmd("HGETALL")
                .arg(&ns_secrets_key)
                .cmd("HGETALL")
                .arg(&worker_secrets_key)
                .query_async(&mut conn)
                .await
        })
        .await?;

    let payload = encode_runtime_load(
        bundle,
        state
            .secret_decryptor()
            .decrypt_hash_entries(&ns_secrets_key, ns_secrets)?,
        state
            .secret_decryptor()
            .decrypt_hash_entries(&worker_secrets_key, worker_secrets)?,
    )?;
    Ok(([(CONTENT_TYPE, runtime_load_content_type())], payload).into_response())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use axum::http::StatusCode;

    use super::*;

    #[test]
    fn bundle_key_accepts_version_tag() {
        assert_eq!(
            bundle_key("tenant", "worker", "v42").unwrap(),
            "worker:tenant:worker:v:42"
        );
        assert_eq!(
            bundle_key("__system__", "s3-cleanup", "v1").unwrap(),
            "worker:__system__:s3-cleanup:v:1"
        );
        assert_eq!(
            bundle_key("__platform__", "platform-api", "v1").unwrap(),
            "worker:__platform__:platform-api:v:1"
        );
    }

    #[test]
    fn bundle_key_rejects_invalid_versions() {
        for (ns, worker, version) in [
            ("tenant", "worker", "1"),
            ("tenant", "worker", "v"),
            ("tenant", "worker", "v0"),
            ("tenant", "worker", "v01"),
            ("tenant", "worker", "vabc"),
            ("Tenant", "worker", "v1"),
            ("__community__", "worker", "v1"),
            ("tenant", "/bad", "v1"),
        ] {
            let err = bundle_key(ns, worker, version).unwrap_err();
            assert_eq!(err.status, StatusCode::BAD_REQUEST);
        }
    }

    #[test]
    fn hash_to_text_object_decodes_utf8_values() {
        let out = hash_to_text_object(vec![("TOKEN".to_string(), b"secret".to_vec())]).unwrap();
        assert_eq!(out.get("TOKEN").unwrap(), "secret");
    }

    #[test]
    fn hash_to_text_object_rejects_invalid_utf8() {
        let err = hash_to_text_object(vec![("TOKEN".to_string(), vec![0xff])]).unwrap_err();
        assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(err.code, "internal_error");
        assert_eq!(err.message, "secret value is not valid utf-8");
    }

    #[test]
    fn encode_runtime_load_uses_binary_bundle_envelope() {
        let payload = encode_runtime_load(
            vec![
                (
                    "__meta__".to_string(),
                    br#"{"mainModule":"worker.js"}"#.to_vec(),
                ),
                ("worker.js".to_string(), b"export default {};".to_vec()),
                ("data.bin".to_string(), vec![0, 1, 255]),
            ],
            HashMap::from([("NS_SECRET".to_string(), "ns".to_string())]),
            HashMap::from([("WORKER_SECRET".to_string(), "worker".to_string())]),
        )
        .unwrap();

        assert_eq!(&payload[0..RUNTIME_LOAD_MAGIC.len()], RUNTIME_LOAD_MAGIC);
        let header_len = u32::from_be_bytes(
            payload[RUNTIME_LOAD_MAGIC.len()..RUNTIME_LOAD_MAGIC.len() + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let header_start = RUNTIME_LOAD_MAGIC.len() + 4;
        let header_end = header_start + header_len;
        let header: serde_json::Value =
            serde_json::from_slice(&payload[header_start..header_end]).unwrap();
        assert_eq!(header["ns_secrets"]["NS_SECRET"], "ns");
        assert_eq!(header["worker_secrets"]["WORKER_SECRET"], "worker");
        assert_eq!(
            u32::from_be_bytes(payload[header_end..header_end + 4].try_into().unwrap()),
            3
        );
        assert!(
            payload
                .array_windows::<3>()
                .any(|chunk| chunk == &[0, 1, 255])
        );
    }
}
