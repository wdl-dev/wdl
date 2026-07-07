use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aes_gcm::aead::AeadInOut;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce, Tag};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::Deserialize;
use wdl_rust_common::hash::fnv1a64;
use zeroize::Zeroizing;

use crate::SERVICE;
use crate::observability::Metrics;
use crate::runtime::HashEntries;
use crate::{AppError, AppResult};

const SECRET_ENVELOPE_PREFIX: &[u8] = b"WDL-ENC:";
const SECRET_ENVELOPE_VERSION: u8 = 1;
const SECRET_ENVELOPE_ALG: &str = "AES-256-GCM";
const SECRET_ENVELOPE_LOCAL_KEY_ENV: &str = "SECRET_ENVELOPE_LOCAL_KEY_B64";
const SECRET_ENVELOPE_KID_ENV: &str = "SECRET_ENVELOPE_KID";
const AES_256_KEY_BYTES: usize = 32;
const AES_GCM_IV_BYTES: usize = 12;
const AES_GCM_TAG_BYTES: usize = 16;
const DEK_CACHE_LIMIT: usize = 512;
const DEK_CACHE_SHARDS: usize = 16;
const DEK_CACHE_SHARD_LIMIT: usize = DEK_CACHE_LIMIT / DEK_CACHE_SHARDS;
const DEK_CACHE_TTL: Duration = Duration::from_secs(60);

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SecretEnvelope {
    v: u8,
    alg: String,
    kid: String,
    edek: String,
    iv: String,
    ct: String,
    tag: String,
}

#[derive(Clone)]
pub(crate) struct SecretEnvelopeDecryptor {
    local: Option<LocalProvider>,
    dek_cache: Arc<Vec<Mutex<HashMap<String, DekCacheEntry>>>>,
    metrics: Arc<Metrics>,
}

#[derive(Clone)]
struct LocalProvider {
    kid: String,
    // Root key-encryption key. Zeroized on drop so the KEK does not linger in
    // freed memory (core dump / swap / heap reuse) after the provider drops.
    key: Zeroizing<[u8; AES_256_KEY_BYTES]>,
}

struct DekCacheEntry {
    // Decrypted data-encryption key; zeroized when the cache entry is evicted.
    value: Zeroizing<Vec<u8>>,
    expires_at: Instant,
}

impl SecretEnvelopeDecryptor {
    pub(crate) fn from_env(metrics: Arc<Metrics>) -> AppResult<Self> {
        let key = std::env::var(SECRET_ENVELOPE_LOCAL_KEY_ENV).ok();
        let kid = std::env::var(SECRET_ENVELOPE_KID_ENV).ok();
        let local = match (key, kid) {
            (None, None) => None,
            (Some(key), Some(kid)) => {
                if !kid.starts_with("local:") {
                    return Err(secret_config_error(format!(
                        "{SECRET_ENVELOPE_KID_ENV} must be a canonical local provider kid"
                    )));
                }
                let decoded = Zeroizing::new(decode_config_base64(
                    &key,
                    SECRET_ENVELOPE_LOCAL_KEY_ENV,
                    false,
                )?);
                let key: [u8; AES_256_KEY_BYTES] = decoded.as_slice().try_into().map_err(|_| {
                    secret_config_error(format!(
                        "{SECRET_ENVELOPE_LOCAL_KEY_ENV} must decode to 32 bytes"
                    ))
                })?;
                Some(LocalProvider {
                    kid,
                    key: Zeroizing::new(key),
                })
            }
            _ => {
                return Err(secret_config_error(format!(
                    "{SECRET_ENVELOPE_LOCAL_KEY_ENV} and {SECRET_ENVELOPE_KID_ENV} must be configured together"
                )));
            }
        };
        Ok(Self {
            local,
            dek_cache: Arc::new(dek_cache_shards()),
            metrics,
        })
    }

    pub(crate) fn decrypt_hash_entries(
        &self,
        hash_key: &str,
        items: HashEntries,
    ) -> AppResult<HashMap<String, String>> {
        let mut out = HashMap::with_capacity(items.len());
        for (field, value) in items {
            let plaintext = self.decrypt_value(hash_key, &field, &value)?;
            out.insert(field, plaintext);
        }
        Ok(out)
    }

    fn decrypt_value(&self, hash_key: &str, field: &str, value: &[u8]) -> AppResult<String> {
        if !value.starts_with(SECRET_ENVELOPE_PREFIX) {
            return Err(secret_decrypt_error(
                "secret value is not envelope encrypted",
            ));
        }
        let json_bytes = &value[SECRET_ENVELOPE_PREFIX.len()..];
        let envelope: SecretEnvelope = serde_json::from_slice(json_bytes)
            .map_err(|_| secret_decrypt_error("secret envelope JSON is invalid"))?;
        validate_envelope(&envelope)?;
        let provider = self.local.as_ref().ok_or_else(|| {
            secret_decrypt_error("secret envelope local provider is not configured")
        })?;
        if envelope.kid != provider.kid {
            return Err(secret_decrypt_error(
                "secret envelope kid does not match configured provider kid",
            ));
        }
        let dek = self.decrypt_dek(provider, &envelope, hash_key, field)?;
        let plaintext = aes_gcm_decrypt(
            &dek,
            &decode_canonical_base64(&envelope.iv, "iv", false)?,
            &decode_canonical_base64(&envelope.ct, "ct", true)?,
            &decode_canonical_base64(&envelope.tag, "tag", false)?,
            payload_aad(hash_key, field).as_bytes(),
        )?;
        String::from_utf8(plaintext)
            .map_err(|_| secret_decrypt_error("decrypted secret plaintext is not valid utf-8"))
    }

    fn decrypt_dek(
        &self,
        provider: &LocalProvider,
        envelope: &SecretEnvelope,
        hash_key: &str,
        field: &str,
    ) -> AppResult<Zeroizing<Vec<u8>>> {
        // Scoped to storage location so a cache hit can't reuse a DEK across
        // a different (hash_key, field) and skip its data_key_aad binding.
        let cache_key = format!(
            "{}\0{}\0{}\0{}",
            envelope.kid, envelope.edek, hash_key, field
        );
        let now = Instant::now();
        let shard_index = dek_cache_shard(&cache_key);
        {
            let cache = self.dek_cache[shard_index]
                .lock()
                .expect("dek cache mutex poisoned");
            if let Some(cached) = cache.get(&cache_key)
                && cached.expires_at > now
            {
                record_dek_cache_lookup(&self.metrics, "hit");
                return Ok(cached.value.clone());
            }
        }
        record_dek_cache_lookup(&self.metrics, "miss");

        let edek = decode_canonical_base64(&envelope.edek, "edek", false)?;
        if edek.len() != AES_GCM_IV_BYTES + AES_256_KEY_BYTES + AES_GCM_TAG_BYTES {
            return Err(secret_decrypt_error(
                "local provider edek has invalid length",
            ));
        }
        let iv = &edek[..AES_GCM_IV_BYTES];
        let ct = &edek[AES_GCM_IV_BYTES..AES_GCM_IV_BYTES + AES_256_KEY_BYTES];
        let tag = &edek[AES_GCM_IV_BYTES + AES_256_KEY_BYTES..];
        let dek = Zeroizing::new(aes_gcm_decrypt(
            provider.key.as_slice(),
            iv,
            ct,
            tag,
            data_key_aad(&envelope.kid, hash_key, field).as_bytes(),
        )?);
        if dek.len() != AES_256_KEY_BYTES {
            return Err(secret_decrypt_error(
                "decrypted data key has invalid length",
            ));
        }
        let mut cache = self.dek_cache[shard_index]
            .lock()
            .expect("dek cache mutex poisoned");
        if cache.len() >= DEK_CACHE_SHARD_LIMIT {
            let before_prune = cache.len();
            cache.retain(|_, entry| entry.expires_at > now);
            let expired = before_prune.saturating_sub(cache.len());
            if expired > 0 {
                record_dek_cache_eviction(&self.metrics, "expired", expired);
            }
            if cache.len() >= DEK_CACHE_SHARD_LIMIT
                && let Some(oldest_key) = cache
                    .iter()
                    .min_by_key(|(_, entry)| entry.expires_at)
                    .map(|(key, _)| key.clone())
            {
                cache.remove(&oldest_key);
                record_dek_cache_eviction(&self.metrics, "capacity", 1);
            }
        }
        cache.insert(
            cache_key,
            DekCacheEntry {
                value: dek.clone(),
                expires_at: now + DEK_CACHE_TTL,
            },
        );
        Ok(dek)
    }
}

fn dek_cache_shards() -> Vec<Mutex<HashMap<String, DekCacheEntry>>> {
    (0..DEK_CACHE_SHARDS)
        .map(|_| Mutex::new(HashMap::new()))
        .collect()
}

fn dek_cache_shard(cache_key: &str) -> usize {
    fnv1a64(cache_key.as_bytes()) as usize % DEK_CACHE_SHARDS
}

fn record_dek_cache_lookup(metrics: &Metrics, outcome: &'static str) {
    metrics.increment(
        "secret_dek_cache_lookups",
        &[("service", SERVICE), ("outcome", outcome)],
        1.0,
    );
}

fn record_dek_cache_eviction(metrics: &Metrics, reason: &'static str, count: usize) {
    metrics.increment(
        "secret_dek_cache_evictions",
        &[("service", SERVICE), ("reason", reason)],
        count as f64,
    );
}

fn validate_envelope(envelope: &SecretEnvelope) -> AppResult<()> {
    if envelope.v != SECRET_ENVELOPE_VERSION || envelope.alg != SECRET_ENVELOPE_ALG {
        return Err(secret_decrypt_error(
            "secret envelope version or algorithm is unsupported",
        ));
    }
    if envelope.kid.is_empty()
        || envelope.edek.is_empty()
        || envelope.iv.is_empty()
        || envelope.tag.is_empty()
    {
        return Err(secret_decrypt_error(
            "secret envelope is missing a required field",
        ));
    }
    Ok(())
}

fn decode_canonical_base64(value: &str, field: &str, allow_empty: bool) -> AppResult<Vec<u8>> {
    if value.is_empty() && !allow_empty {
        return Err(secret_decrypt_error(format!(
            "{field} must be a non-empty base64 string"
        )));
    }
    let bytes = BASE64
        .decode(value)
        .map_err(|_| secret_decrypt_error(format!("{field} is not valid base64")))?;
    if BASE64.encode(&bytes) != value {
        return Err(secret_decrypt_error(format!(
            "{field} is not canonical base64"
        )));
    }
    Ok(bytes)
}

fn decode_config_base64(value: &str, field: &str, allow_empty: bool) -> AppResult<Vec<u8>> {
    decode_canonical_base64(value, field, allow_empty)
        .map_err(|err| secret_config_error(err.message))
}

fn storage_aad(hash_key: &str, field: &str) -> String {
    format!("{hash_key}\0{field}\0{SECRET_ENVELOPE_VERSION}")
}

fn payload_aad(hash_key: &str, field: &str) -> String {
    format!(
        "WDL-SECRET\0{}\0v={SECRET_ENVELOPE_VERSION}\0alg={SECRET_ENVELOPE_ALG}",
        storage_aad(hash_key, field)
    )
}

fn data_key_aad(kid: &str, hash_key: &str, field: &str) -> String {
    format!("WDL-SECRET-DEK\0{kid}\0{}", storage_aad(hash_key, field))
}

fn aes_gcm_decrypt(key: &[u8], iv: &[u8], ct: &[u8], tag: &[u8], aad: &[u8]) -> AppResult<Vec<u8>> {
    if key.len() != AES_256_KEY_BYTES
        || iv.len() != AES_GCM_IV_BYTES
        || tag.len() != AES_GCM_TAG_BYTES
    {
        return Err(secret_decrypt_error(
            "secret envelope has invalid AES-GCM material",
        ));
    }
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|_| secret_decrypt_error("secret envelope key has invalid length"))?;
    let mut plaintext = ct.to_vec();
    let nonce = Nonce::try_from(iv)
        .map_err(|_| secret_decrypt_error("secret envelope has invalid AES-GCM material"))?;
    let tag = Tag::try_from(tag)
        .map_err(|_| secret_decrypt_error("secret envelope has invalid AES-GCM material"))?;
    cipher
        .decrypt_inout_detached(&nonce, aad, plaintext.as_mut_slice().into(), &tag)
        .map_err(|_| secret_decrypt_error("secret envelope authentication failed"))?;
    Ok(plaintext)
}

fn secret_decrypt_error(message: impl Into<String>) -> AppError {
    // Runtime-load intentionally exposes one bounded decrypt code for malformed
    // envelopes, unknown kids, and authentication failures. JS control and the
    // migration tool keep finer-grained local errors for operator feedback.
    AppError {
        status: axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        code: "secret_decrypt_failed",
        message: message.into(),
    }
}

fn secret_config_error(message: impl Into<String>) -> AppError {
    AppError {
        status: axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        code: "secret_encryption_unconfigured",
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_metrics() -> Arc<Metrics> {
        Arc::new(Metrics::default())
    }

    fn decryptor_without_provider() -> SecretEnvelopeDecryptor {
        SecretEnvelopeDecryptor {
            local: None,
            dek_cache: Arc::new(dek_cache_shards()),
            metrics: test_metrics(),
        }
    }

    fn decryptor_with_local(kid: &str) -> SecretEnvelopeDecryptor {
        SecretEnvelopeDecryptor {
            local: Some(LocalProvider {
                kid: kid.to_string(),
                key: Zeroizing::new(*b"0123456789abcdef0123456789abcdef"),
            }),
            dek_cache: Arc::new(dek_cache_shards()),
            metrics: test_metrics(),
        }
    }

    #[test]
    fn missing_provider_allows_empty_hashes_but_rejects_secret_values() {
        let decryptor = decryptor_without_provider();
        assert!(
            decryptor
                .decrypt_hash_entries("secrets:demo", vec![])
                .unwrap()
                .is_empty()
        );
        let err = decryptor
            .decrypt_hash_entries(
                "secrets:demo",
                vec![("TOKEN".to_string(), b"plain".to_vec())],
            )
            .unwrap_err();
        assert_eq!(err.code, "secret_decrypt_failed");
    }

    #[test]
    fn invalid_local_provider_key_is_configuration_error() {
        let err =
            decode_config_base64("not-base64", SECRET_ENVELOPE_LOCAL_KEY_ENV, false).unwrap_err();
        assert_eq!(err.code, "secret_encryption_unconfigured");
    }

    #[test]
    fn dek_cache_shard_selection_is_bounded_and_distributed() {
        let mut seen = std::collections::HashSet::new();
        for idx in 0..128 {
            let shard = dek_cache_shard(&format!("cache-key-{idx}"));
            assert!(shard < DEK_CACHE_SHARDS);
            seen.insert(shard);
        }
        assert!(
            seen.len() > 1,
            "expected cache keys to map across more than one shard"
        );
    }

    #[test]
    fn decrypts_js_generated_envelope_vector() {
        let decryptor = decryptor_with_local("local:test:secret-envelope:v1");
        let envelope = concat!(
            r#"WDL-ENC:{"v":1,"alg":"AES-256-GCM","kid":"local:test:secret-envelope:v1","#,
            r#""edek":"ICEiIyQlJicoKSorrmiFFNwSNL789zDTZysVGjsTpksuXqulg0Mt7JrBiLTG0wE1moqP31Jd+edymdiU","#,
            r#""iv":"LC0uLzAxMjM0NTY3","ct":"IZhLZQvljbpT5euHV5YP","tag":"0wuyjfzeZfuY5hD3HRoKdg=="}"#
        );
        let out = decryptor
            .decrypt_hash_entries(
                "secrets:demo:api",
                vec![("TOKEN".to_string(), envelope.as_bytes().to_vec())],
            )
            .unwrap();
        assert_eq!(out.get("TOKEN").unwrap(), "sensitive-value");
    }

    #[test]
    fn decrypts_js_generated_empty_string_vector() {
        let decryptor = decryptor_with_local("local:test:secret-envelope:v1");
        let envelope = concat!(
            r#"WDL-ENC:{"v":1,"alg":"AES-256-GCM","kid":"local:test:secret-envelope:v1","#,
            r#""edek":"ICEiIyQlJicoKSorrmiFFNwSNL789zDTZysVGjsTpksuXqulg0Mt7JrBiLSmHuPVe4uS7JepH053l0Yt","#,
            r#""iv":"LC0uLzAxMjM0NTY3","ct":"","tag":"TTVUTxM6DfSV+VZ/AODK1w=="}"#
        );
        let out = decryptor
            .decrypt_hash_entries(
                "secrets:demo:api",
                vec![("EMPTY".to_string(), envelope.as_bytes().to_vec())],
            )
            .unwrap();
        assert_eq!(out.get("EMPTY").unwrap(), "");
    }

    #[test]
    fn decrypt_rejects_storage_location_mismatch() {
        let decryptor = decryptor_with_local("local:test:secret-envelope:v1");
        let envelope = concat!(
            r#"WDL-ENC:{"v":1,"alg":"AES-256-GCM","kid":"local:test:secret-envelope:v1","#,
            r#""edek":"ICEiIyQlJicoKSorrmiFFNwSNL789zDTZysVGjsTpksuXqulg0Mt7JrBiLTG0wE1moqP31Jd+edymdiU","#,
            r#""iv":"LC0uLzAxMjM0NTY3","ct":"IZhLZQvljbpT5euHV5YP","tag":"0wuyjfzeZfuY5hD3HRoKdg=="}"#
        );
        let err = decryptor
            .decrypt_hash_entries(
                "secrets:other:api",
                vec![("TOKEN".to_string(), envelope.as_bytes().to_vec())],
            )
            .unwrap_err();
        assert_eq!(err.code, "secret_decrypt_failed");
    }

    #[test]
    fn decrypt_rejects_unknown_kid() {
        let decryptor = decryptor_with_local("local:test:secret-envelope:v2");
        let envelope = concat!(
            r#"WDL-ENC:{"v":1,"alg":"AES-256-GCM","kid":"local:test:secret-envelope:v1","#,
            r#""edek":"ICEiIyQlJicoKSorrmiFFNwSNL789zDTZysVGjsTpksuXqulg0Mt7JrBiLTG0wE1moqP31Jd+edymdiU","#,
            r#""iv":"LC0uLzAxMjM0NTY3","ct":"IZhLZQvljbpT5euHV5YP","tag":"0wuyjfzeZfuY5hD3HRoKdg=="}"#
        );
        let err = decryptor
            .decrypt_hash_entries(
                "secrets:demo:api",
                vec![("TOKEN".to_string(), envelope.as_bytes().to_vec())],
            )
            .unwrap_err();
        assert_eq!(err.code, "secret_decrypt_failed");
    }

    #[test]
    fn dek_cache_is_scoped_to_storage_location() {
        let decryptor = decryptor_with_local("local:test:secret-envelope:v1");
        let envelope = concat!(
            r#"WDL-ENC:{"v":1,"alg":"AES-256-GCM","kid":"local:test:secret-envelope:v1","#,
            r#""edek":"ICEiIyQlJicoKSorrmiFFNwSNL789zDTZysVGjsTpksuXqulg0Mt7JrBiLTG0wE1moqP31Jd+edymdiU","#,
            r#""iv":"LC0uLzAxMjM0NTY3","ct":"IZhLZQvljbpT5euHV5YP","tag":"0wuyjfzeZfuY5hD3HRoKdg=="}"#
        );
        // First read at the matching location decrypts and caches the DEK.
        let out = decryptor
            .decrypt_hash_entries(
                "secrets:demo:api",
                vec![("TOKEN".to_string(), envelope.as_bytes().to_vec())],
            )
            .unwrap();
        assert_eq!(out.get("TOKEN").unwrap(), "sensitive-value");
        assert_eq!(
            decryptor
                .dek_cache
                .iter()
                .map(|shard| shard.lock().expect("dek cache mutex poisoned").len())
                .sum::<usize>(),
            1
        );

        // Same envelope at the same location hits the cache.
        let again = decryptor
            .decrypt_hash_entries(
                "secrets:demo:api",
                vec![("TOKEN".to_string(), envelope.as_bytes().to_vec())],
            )
            .unwrap();
        assert_eq!(again.get("TOKEN").unwrap(), "sensitive-value");

        // Same edek at a different location is a cache miss (the key is scoped
        // to hash_key/field) and fails closed at DEK decryption via data_key_aad.
        let err = decryptor
            .decrypt_hash_entries(
                "secrets:other:api",
                vec![("TOKEN".to_string(), envelope.as_bytes().to_vec())],
            )
            .unwrap_err();
        assert_eq!(err.code, "secret_decrypt_failed");

        let metrics = decryptor.metrics.render_prometheus();
        assert!(metrics.contains(
            r#"wdl_secret_dek_cache_lookups_total{outcome="miss",service="redis-proxy"} 2"#
        ));
        assert!(metrics.contains(
            r#"wdl_secret_dek_cache_lookups_total{outcome="hit",service="redis-proxy"} 1"#
        ));
    }

    #[test]
    fn aes_gcm_decrypt_uses_detached_tag_without_concat_message() {
        let source = include_str!("secrets.rs");
        let helper = source
            .split("fn aes_gcm_decrypt")
            .nth(1)
            .expect("aes_gcm_decrypt helper should exist");
        assert!(
            helper.contains("decrypt_inout_detached"),
            "AES-GCM decrypt should pass the tag separately instead of concatenating ct||tag"
        );
        assert!(
            !helper.contains("extend_from_slice(tag)"),
            "AES-GCM decrypt should not allocate a ct||tag message buffer"
        );
    }

    #[test]
    fn decrypt_records_expired_dek_cache_evictions() {
        let decryptor = decryptor_with_local("local:test:secret-envelope:v1");
        {
            let mut cache = decryptor.dek_cache[0]
                .lock()
                .expect("dek cache mutex poisoned");
            for idx in 0..DEK_CACHE_SHARD_LIMIT {
                cache.insert(
                    format!("expired-{idx}"),
                    DekCacheEntry {
                        value: Zeroizing::new(vec![0; AES_256_KEY_BYTES]),
                        expires_at: Instant::now() - Duration::from_secs(1),
                    },
                );
            }
        }
        let envelope = concat!(
            r#"WDL-ENC:{"v":1,"alg":"AES-256-GCM","kid":"local:test:secret-envelope:v1","#,
            r#""edek":"ICEiIyQlJicoKSorrmiFFNwSNL789zDTZysVGjsTpksuXqulg0Mt7JrBiLTG0wE1moqP31Jd+edymdiU","#,
            r#""iv":"LC0uLzAxMjM0NTY3","ct":"IZhLZQvljbpT5euHV5YP","tag":"0wuyjfzeZfuY5hD3HRoKdg=="}"#
        );
        let out = decryptor
            .decrypt_hash_entries(
                "secrets:demo:api",
                vec![("TOKEN".to_string(), envelope.as_bytes().to_vec())],
            )
            .unwrap();
        assert_eq!(out.get("TOKEN").unwrap(), "sensitive-value");
        let metrics = decryptor.metrics.render_prometheus();
        assert!(metrics.contains(&format!(
            r#"wdl_secret_dek_cache_evictions_total{{reason="expired",service="redis-proxy"}} {DEK_CACHE_SHARD_LIMIT}"#
        )));
    }
}
