use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

use crate::{AppError, AppResult, AppState};

use super::{KV_HASH_BUCKETS, field_to_user_key};

pub(crate) const KV_LIST_LIMIT_DEFAULT: u64 = 1000;
pub(crate) const KV_LIST_LIMIT_MAX: u64 = 1000;
pub(crate) const KV_LIST_CURSOR_OVERFLOW_MAX: usize = (KV_LIST_LIMIT_MAX as usize) * 2;
// WDL-owned KV v2 cursor envelope for carrying HSCAN bucket state between
// pages. Future incompatible cursor shapes should bump this to `wdl3:`.
pub(crate) const KV_LIST_CURSOR_PREFIX: &str = "wdl2:";

#[derive(Deserialize, Serialize)]
struct KvListCursor {
    bucket: u32,
    scan: String,
    overflow: Vec<String>,
}

fn is_valid_list_cursor_state(bucket: u32, overflow_len: usize) -> bool {
    bucket < KV_HASH_BUCKETS && overflow_len <= KV_LIST_CURSOR_OVERFLOW_MAX
}

pub(crate) fn normalize_list_limit(limit: Option<u64>) -> u64 {
    limit
        .unwrap_or(KV_LIST_LIMIT_DEFAULT)
        .clamp(1, KV_LIST_LIMIT_MAX)
}

pub(crate) fn decode_list_cursor(cursor: Option<String>) -> AppResult<(u32, String, Vec<String>)> {
    let Some(raw) = cursor else {
        return Ok((0, "0".to_string(), Vec::new()));
    };
    if let Some(encoded) = raw.strip_prefix(KV_LIST_CURSOR_PREFIX) {
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| AppError::bad_request("invalid KV list cursor"))?;
        let parsed = serde_json::from_slice::<KvListCursor>(&bytes)
            .map_err(|_| AppError::bad_request("invalid KV list cursor"))?;
        if !is_valid_list_cursor_state(parsed.bucket, parsed.overflow.len()) {
            return Err(AppError::bad_request("invalid KV list cursor"));
        }
        return Ok((parsed.bucket, parsed.scan, parsed.overflow));
    }
    Err(AppError::bad_request("invalid KV list cursor"))
}

pub(crate) fn encode_list_cursor(
    bucket: u32,
    scan: String,
    overflow: Vec<String>,
) -> AppResult<String> {
    if !is_valid_list_cursor_state(bucket, overflow.len()) {
        return Err(AppError::internal_error("invalid KV list cursor state"));
    }
    let json = serde_json::to_vec(&KvListCursor {
        bucket,
        scan,
        overflow,
    })
    .map_err(AppError::internal_json)?;
    Ok(format!(
        "{KV_LIST_CURSOR_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(json)
    ))
}

pub(crate) fn cursor_overflow_field_allowed(field: &str, prefix: &str) -> bool {
    field_to_user_key(field).is_ok_and(|key| key.starts_with(prefix))
}

pub(crate) async fn existing_cursor_overflow_fields(
    state: &AppState,
    hash_key: String,
    candidates: Vec<String>,
) -> AppResult<Vec<String>> {
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let pipe = existing_cursor_overflow_fields_pipeline(&hash_key, &candidates);
    let exists: Vec<i64> = state
        .with_redis(async |mut conn| pipe.query_async(&mut conn).await)
        .await?;
    Ok(candidates
        .into_iter()
        .zip(exists)
        .filter_map(|(key, exists)| (exists > 0).then_some(key))
        .collect())
}

fn existing_cursor_overflow_fields_pipeline(
    hash_key: &str,
    candidates: &[String],
) -> redis::Pipeline {
    let mut pipe = redis::pipe();
    for field in candidates {
        pipe.cmd("HEXISTS").arg(hash_key).arg(field);
    }
    pipe
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use wdl_rust_common::test_support::parse_packed_commands;

    #[test]
    fn encoded_list_cursors_round_trip_at_supported_boundaries() {
        for (bucket, scan, overflow) in [
            (0, "0", Vec::new()),
            (
                KV_HASH_BUCKETS - 1,
                "18446744073709551615",
                vec!["v:item".to_string(); KV_LIST_CURSOR_OVERFLOW_MAX],
            ),
        ] {
            let encoded = encode_list_cursor(bucket, scan.to_string(), overflow.clone()).unwrap();
            assert_eq!(
                decode_list_cursor(Some(encoded)).unwrap(),
                (bucket, scan.to_string(), overflow)
            );
        }
    }

    #[test]
    fn encoder_rejects_cursor_state_the_decoder_cannot_accept() {
        for result in [
            encode_list_cursor(KV_HASH_BUCKETS, "0".to_string(), Vec::new()),
            encode_list_cursor(
                0,
                "0".to_string(),
                vec!["v:item".to_string(); KV_LIST_CURSOR_OVERFLOW_MAX + 1],
            ),
        ] {
            let err = result.unwrap_err();
            assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(err.code, "internal_error");
        }
    }

    #[test]
    fn existing_cursor_overflow_fields_pipeline_only_probes_existence() {
        let candidates = vec!["v:first".to_string(), "v:second".to_string()];
        let commands = parse_packed_commands(
            &existing_cursor_overflow_fields_pipeline("kvh:tenant:store:b:1", &candidates)
                .get_packed_pipeline(),
        );

        assert_eq!(
            commands,
            [
                ["HEXISTS", "kvh:tenant:store:b:1", "v:first"],
                ["HEXISTS", "kvh:tenant:store:b:1", "v:second"],
            ]
        );
    }
}
