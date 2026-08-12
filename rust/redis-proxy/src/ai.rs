use std::collections::{BTreeMap, HashSet};

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use wdl_rust_common::identity::is_valid_runtime_load_ns;

use crate::{AppError, AppResult, AppState};

const AI_PROVIDER_MAX_COUNT: usize = 8;
const AI_MODELS_PER_PROVIDER_MAX: usize = 32;
const AI_NAMESPACE_MODEL_MAX_COUNT: usize = 128;
const AI_PROVIDER_RECORD_MAX_BYTES: usize = 64 * 1024;
const AI_UPSTREAM_MODEL_MAX_BYTES: usize = 256;
const AI_CREDENTIAL_MAX_BYTES: usize = 16 * 1024;

const RESOLVE_SCRIPT: &str = r#"
return {
  redis.call('HGET', KEYS[1], ARGV[1]),
  redis.call('HGET', KEYS[2], ARGV[1])
}
"#;

const MODELS_SCRIPT: &str = r#"
return {
  redis.call('HGETALL', KEYS[1]),
  redis.call('HKEYS', KEYS[2])
}
"#;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Protocol {
    Responses,
    ChatCompletions,
    Embeddings,
    Realtime,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Transport {
    Http,
    Sse,
    ResponsesWebsocket,
    RealtimeWebsocket,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum ProviderKind {
    Openai,
    Xai,
    Deepseek,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Capabilities {
    function_tools: bool,
    structured_output: bool,
    reasoning: bool,
    previous_response_id: bool,
    provider_tools: bool,
    binary_frames: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ModelDescriptor {
    upstream_model: String,
    protocol: Protocol,
    transports: Vec<Transport>,
    input_modalities: Vec<String>,
    output_modalities: Vec<String>,
    capabilities: Capabilities,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ProviderRecord {
    revision: String,
    kind: ProviderKind,
    models: BTreeMap<String, ModelDescriptor>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResolveRequest {
    ns: String,
    model: String,
    protocol: Protocol,
    transport: Transport,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ModelsRequest {
    ns: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct ResolveResponse {
    provider: String,
    alias: String,
    kind: ProviderKind,
    upstream_model: String,
    protocol: Protocol,
    transport: Transport,
    destination: String,
    credential: String,
    input_modalities: Vec<String>,
    capabilities: Capabilities,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ModelListEntry {
    id: String,
    protocol: Protocol,
    transports: Vec<Transport>,
    input_modalities: Vec<String>,
    output_modalities: Vec<String>,
    capabilities: Capabilities,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ModelsResponse {
    models: Vec<ModelListEntry>,
}

fn ai_error(status: StatusCode, code: &'static str, message: impl Into<String>) -> AppError {
    AppError {
        status,
        code,
        message: message.into(),
    }
}

fn providers_key(ns: &str) -> String {
    format!("ai:providers:{ns}")
}

fn credentials_key(ns: &str) -> String {
    format!("ai:provider-credentials:{ns}")
}

fn valid_provider_alias(value: &str) -> bool {
    valid_bounded_alias(value, 32, |ch| ch == '-')
}

fn valid_model_alias(value: &str) -> bool {
    valid_bounded_alias(value, 64, |ch| matches!(ch, '-' | '_' | '.'))
}

fn valid_bounded_alias(value: &str, max: usize, extra: impl Fn(char) -> bool) -> bool {
    if value.is_empty() || value.len() > max {
        return false;
    }
    let mut chars = value.chars();
    let first = chars.next().unwrap();
    let last = value.chars().next_back().unwrap();
    (first.is_ascii_lowercase() || first.is_ascii_digit())
        && (last.is_ascii_lowercase() || last.is_ascii_digit())
        && chars.all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || extra(ch))
}

fn split_model_reference(model: &str) -> AppResult<(&str, &str)> {
    let mut parts = model.split('/');
    let provider = parts.next().unwrap_or_default();
    let alias = parts.next().unwrap_or_default();
    if parts.next().is_some() || !valid_provider_alias(provider) || !valid_model_alias(alias) {
        return Err(AppError::bad_request("model must be <provider>/<alias>"));
    }
    Ok((provider, alias))
}

fn validate_request_identity(ns: &str) -> AppResult<()> {
    if !is_valid_runtime_load_ns(ns) {
        return Err(AppError::bad_request("invalid AI namespace"));
    }
    Ok(())
}

fn validate_credential(credential: &str) -> AppResult<()> {
    if credential.is_empty()
        || credential.len() > AI_CREDENTIAL_MAX_BYTES
        || !credential.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
    {
        return Err(ai_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "ai_state_corrupt",
            "AI provider credential violates its token grammar",
        ));
    }
    Ok(())
}

fn validate_string_set(values: &[String], allowed: &[&str]) -> bool {
    !values.is_empty()
        && values.len() <= allowed.len()
        && values.iter().all(|value| allowed.contains(&value.as_str()))
        && values.windows(2).all(|pair| pair[0] < pair[1])
}

fn validate_upstream_model(value: &str) -> bool {
    !value.is_empty() && value.len() <= AI_UPSTREAM_MODEL_MAX_BYTES
}

fn validate_protocol_transports(protocol: &Protocol, transports: &[Transport]) -> bool {
    let allowed = match protocol {
        Protocol::Responses => &[
            Transport::Http,
            Transport::Sse,
            Transport::ResponsesWebsocket,
        ][..],
        Protocol::ChatCompletions => &[Transport::Http, Transport::Sse][..],
        Protocol::Embeddings => &[Transport::Http][..],
        Protocol::Realtime => &[Transport::RealtimeWebsocket][..],
    };
    !transports.is_empty()
        && transports.iter().all(|value| allowed.contains(value))
        && transports
            .windows(2)
            .all(|pair| transport_order(&pair[0]) < transport_order(&pair[1]))
}

fn validate_descriptor(descriptor: &ModelDescriptor) -> bool {
    if !validate_upstream_model(&descriptor.upstream_model)
        || !validate_string_set(
            &descriptor.input_modalities,
            &["audio", "file", "image", "text"],
        )
        || !validate_string_set(&descriptor.output_modalities, &["audio", "text"])
    {
        return false;
    }
    validate_protocol_transports(&descriptor.protocol, &descriptor.transports)
}

fn provider_supports_protocol(
    kind: &ProviderKind,
    protocol: &Protocol,
    transports: &[Transport],
) -> bool {
    match kind {
        ProviderKind::Openai | ProviderKind::Xai => true,
        ProviderKind::Deepseek => {
            matches!(protocol, Protocol::Responses | Protocol::ChatCompletions)
                && transports
                    .iter()
                    .all(|transport| matches!(transport, Transport::Http | Transport::Sse))
        }
    }
}

fn provider_supports_descriptor(kind: &ProviderKind, descriptor: &ModelDescriptor) -> bool {
    provider_supports_protocol(kind, &descriptor.protocol, &descriptor.transports)
        && (!matches!(kind, ProviderKind::Deepseek)
            || (descriptor.input_modalities == ["text"]
                && descriptor.output_modalities == ["text"]
                && !descriptor.capabilities.previous_response_id))
}

fn transport_order(value: &Transport) -> u8 {
    match value {
        Transport::Http => 0,
        Transport::RealtimeWebsocket => 1,
        Transport::ResponsesWebsocket => 2,
        Transport::Sse => 3,
    }
}

fn parse_provider_record(raw: &[u8]) -> AppResult<ProviderRecord> {
    if raw.len() > AI_PROVIDER_RECORD_MAX_BYTES {
        return Err(ai_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "ai_state_corrupt",
            "AI provider record exceeds its byte limit",
        ));
    }
    let record: ProviderRecord = serde_json::from_slice(raw).map_err(|_| {
        ai_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "ai_state_corrupt",
            "AI provider record is malformed",
        )
    })?;
    if record.revision.len() != 32
        || !record
            .revision
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        || record.models.is_empty()
        || record.models.len() > AI_MODELS_PER_PROVIDER_MAX
        || record.models.iter().any(|(alias, descriptor)| {
            !valid_model_alias(alias)
                || !validate_descriptor(descriptor)
                || !provider_supports_descriptor(&record.kind, descriptor)
        })
    {
        return Err(ai_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "ai_state_corrupt",
            "AI provider record violates its contract",
        ));
    }
    let canonical = serde_json::to_vec(&record).map_err(AppError::internal_json)?;
    if canonical != raw {
        return Err(ai_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "ai_state_corrupt",
            "AI provider record is not canonical",
        ));
    }
    Ok(record)
}

fn destination(
    kind: &ProviderKind,
    protocol: &Protocol,
    transport: &Transport,
) -> AppResult<String> {
    let host = match kind {
        ProviderKind::Openai => "api.openai.com",
        ProviderKind::Xai => "api.x.ai",
        ProviderKind::Deepseek => "api.deepseek.com",
    };
    let path = match protocol {
        Protocol::Responses => "/v1/responses",
        Protocol::ChatCompletions => "/v1/chat/completions",
        Protocol::Embeddings => "/v1/embeddings",
        Protocol::Realtime => "/v1/realtime",
    };
    if matches!(kind, ProviderKind::Deepseek) {
        if matches!(
            transport,
            Transport::ResponsesWebsocket | Transport::RealtimeWebsocket
        ) || matches!(protocol, Protocol::Realtime)
        {
            return Err(ai_error(
                StatusCode::CONFLICT,
                "ai_transport_unavailable",
                "DeepSeek does not expose the selected WebSocket transport",
            ));
        }
        let deepseek_path = match protocol {
            Protocol::Responses => "/responses",
            Protocol::ChatCompletions => "/chat/completions",
            Protocol::Embeddings | Protocol::Realtime => {
                return Err(ai_error(
                    StatusCode::CONFLICT,
                    "ai_transport_unavailable",
                    "DeepSeek does not expose the selected protocol",
                ));
            }
        };
        return Ok(format!("https://{host}{deepseek_path}"));
    }
    let scheme = if matches!(
        transport,
        Transport::ResponsesWebsocket | Transport::RealtimeWebsocket
    ) {
        "wss"
    } else {
        "https"
    };
    Ok(format!("{scheme}://{host}{path}"))
}

pub(crate) async fn resolve(
    State(state): State<AppState>,
    Json(request): Json<ResolveRequest>,
) -> AppResult<Json<ResolveResponse>> {
    validate_request_identity(&request.ns)?;
    let (provider, alias) = split_model_reference(&request.model)?;
    let providers_key = providers_key(&request.ns);
    let credentials_key = credentials_key(&request.ns);
    let query_providers_key = providers_key.clone();
    let query_credentials_key = credentials_key.clone();
    let query_provider = provider.to_string();
    let (record_raw, credential_raw): (Option<Vec<u8>>, Option<Vec<u8>>) = state
        .with_control_redis(move |mut conn| async move {
            redis::cmd("EVAL")
                .arg(RESOLVE_SCRIPT)
                .arg(2)
                .arg(&query_providers_key)
                .arg(&query_credentials_key)
                .arg(&query_provider)
                .query_async(&mut conn)
                .await
        })
        .await?;
    let record_raw = record_raw.ok_or_else(|| {
        ai_error(
            StatusCode::NOT_FOUND,
            "ai_provider_not_found",
            "AI provider not found",
        )
    })?;
    let record = parse_provider_record(&record_raw)?;
    let descriptor = record.models.get(alias).ok_or_else(|| {
        ai_error(
            StatusCode::NOT_FOUND,
            "ai_model_not_found",
            "AI model not found",
        )
    })?;
    if descriptor.protocol != request.protocol
        || !descriptor.transports.contains(&request.transport)
    {
        return Err(ai_error(
            StatusCode::CONFLICT,
            "ai_model_protocol_mismatch",
            "AI model does not expose the requested protocol and transport",
        ));
    }
    let credential_raw = credential_raw.ok_or_else(|| {
        ai_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "ai_credential_not_configured",
            "AI provider credential is not configured",
        )
    })?;
    let credential =
        state
            .secret_decryptor()
            .decrypt_hash_field(&credentials_key, provider, &credential_raw)?;
    validate_credential(&credential)?;
    let resolved_destination = destination(&record.kind, &request.protocol, &request.transport)?;
    Ok(Json(ResolveResponse {
        provider: provider.to_string(),
        alias: alias.to_string(),
        kind: record.kind,
        upstream_model: descriptor.upstream_model.clone(),
        protocol: request.protocol,
        transport: request.transport,
        destination: resolved_destination,
        credential,
        input_modalities: descriptor.input_modalities.clone(),
        capabilities: descriptor.capabilities.clone(),
    }))
}

pub(crate) async fn models(
    State(state): State<AppState>,
    Json(request): Json<ModelsRequest>,
) -> AppResult<Json<ModelsResponse>> {
    validate_request_identity(&request.ns)?;
    let providers_key = providers_key(&request.ns);
    let credentials_key = credentials_key(&request.ns);
    let (raw_providers, credential_names): (BTreeMap<String, Vec<u8>>, Vec<String>) = state
        .with_control_redis(|mut conn| async move {
            redis::cmd("EVAL")
                .arg(MODELS_SCRIPT)
                .arg(2)
                .arg(&providers_key)
                .arg(&credentials_key)
                .query_async(&mut conn)
                .await
        })
        .await?;
    if raw_providers.len() > AI_PROVIDER_MAX_COUNT {
        return Err(ai_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "ai_state_corrupt",
            "AI provider count exceeds its bound",
        ));
    }
    let configured: HashSet<&str> = credential_names.iter().map(String::as_str).collect();
    let mut entries = Vec::new();
    let mut model_count = 0usize;
    for (provider, raw) in raw_providers {
        if !valid_provider_alias(&provider) {
            return Err(ai_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ai_state_corrupt",
                "AI provider name is malformed",
            ));
        }
        let record = parse_provider_record(&raw)?;
        model_count += record.models.len();
        if model_count > AI_NAMESPACE_MODEL_MAX_COUNT {
            return Err(ai_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ai_state_corrupt",
                "AI model count exceeds its bound",
            ));
        }
        if !configured.contains(provider.as_str()) {
            continue;
        }
        for (alias, descriptor) in record.models {
            entries.push(ModelListEntry {
                id: format!("{provider}/{alias}"),
                protocol: descriptor.protocol,
                transports: descriptor.transports,
                input_modalities: descriptor.input_modalities,
                output_modalities: descriptor.output_modalities,
                capabilities: descriptor.capabilities,
            });
        }
    }
    Ok(Json(ModelsResponse { models: entries }))
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;

    #[test]
    fn ai_contract_fixture_matches_rust_readers() {
        let fixture: Value =
            serde_json::from_str(include_str!("../../../tests/fixtures/ai-contract.json"))
                .expect("AI contract fixture parses");
        let limits = &fixture["limits"];
        assert_eq!(limits["providerMaxCount"], AI_PROVIDER_MAX_COUNT);
        assert_eq!(limits["modelsPerProviderMax"], AI_MODELS_PER_PROVIDER_MAX);
        assert_eq!(
            limits["namespaceModelMaxCount"],
            AI_NAMESPACE_MODEL_MAX_COUNT
        );
        assert_eq!(
            limits["providerRecordMaxBytes"],
            AI_PROVIDER_RECORD_MAX_BYTES
        );
        assert_eq!(limits["upstreamModelMaxBytes"], AI_UPSTREAM_MODEL_MAX_BYTES);
        assert_eq!(limits["credentialMaxBytes"], AI_CREDENTIAL_MAX_BYTES);
        for item in fixture["aliases"].as_array().unwrap() {
            let value = item["value"].as_str().unwrap();
            assert_eq!(
                valid_provider_alias(value),
                item["provider"].as_bool().unwrap(),
                "{value}"
            );
            assert_eq!(
                valid_model_alias(value),
                item["model"].as_bool().unwrap(),
                "{value}"
            );
        }
        for item in fixture["upstreamModels"].as_array().unwrap() {
            let parsed = serde_json::from_str::<String>(item["json"].as_str().unwrap())
                .ok()
                .filter(|value| validate_upstream_model(value));
            assert_eq!(
                parsed.is_some(),
                item["valid"].as_bool().unwrap(),
                "{}",
                item["name"]
            );
        }
        for item in fixture["destinations"].as_array().unwrap() {
            let kind = serde_json::from_value::<ProviderKind>(item["kind"].clone()).unwrap();
            let protocol = serde_json::from_value::<Protocol>(item["protocol"].clone()).unwrap();
            let transport = serde_json::from_value::<Transport>(item["transport"].clone()).unwrap();
            let actual = destination(&kind, &protocol, &transport).ok();
            assert_eq!(
                actual.as_deref(),
                item["destination"].as_str(),
                "{}/{}/{}",
                item["kind"],
                item["protocol"],
                item["transport"]
            );
        }
        for item in fixture["providerRecords"].as_array().unwrap() {
            let valid = serde_json::from_value::<ProviderRecord>(item["value"].clone())
                .ok()
                .and_then(|record| serde_json::to_vec(&record).ok())
                .is_some_and(|raw| parse_provider_record(&raw).is_ok());
            assert_eq!(valid, item["valid"].as_bool().unwrap(), "{}", item["name"]);
        }
        for item in fixture["resolveRequests"].as_array().unwrap() {
            let parsed = serde_json::from_value::<ResolveRequest>(item["value"].clone())
                .map_err(|_| ())
                .and_then(|request| {
                    validate_request_identity(&request.ns).map_err(|_| ())?;
                    split_model_reference(&request.model).map_err(|_| ())?;
                    Ok(())
                });
            assert_eq!(parsed.is_ok(), item["valid"].as_bool().unwrap());
        }
        for item in fixture["modelsRequests"].as_array().unwrap() {
            let parsed = serde_json::from_value::<ModelsRequest>(item["value"].clone())
                .map_err(|_| ())
                .and_then(|request| validate_request_identity(&request.ns).map_err(|_| ()));
            assert_eq!(parsed.is_ok(), item["valid"].as_bool().unwrap());
        }
        for item in fixture["resolveResponses"].as_array().unwrap() {
            let parsed = serde_json::from_value::<ResolveResponse>(item["value"].clone())
                .ok()
                .filter(|response| {
                    let transports = [response.transport.clone()];
                    valid_provider_alias(&response.provider)
                        && valid_model_alias(&response.alias)
                        && validate_upstream_model(&response.upstream_model)
                        && validate_protocol_transports(&response.protocol, &transports)
                        && validate_string_set(
                            &response.input_modalities,
                            &["audio", "file", "image", "text"],
                        )
                        && !response.destination.is_empty()
                        && validate_credential(&response.credential).is_ok()
                        && provider_supports_protocol(
                            &response.kind,
                            &response.protocol,
                            &transports,
                        )
                });
            assert_eq!(parsed.is_some(), item["valid"].as_bool().unwrap());
        }
        for item in fixture["modelsResponses"].as_array().unwrap() {
            let parsed = serde_json::from_value::<ModelsResponse>(item["value"].clone())
                .ok()
                .filter(|response| {
                    response.models.len() <= AI_NAMESPACE_MODEL_MAX_COUNT
                        && response
                            .models
                            .windows(2)
                            .all(|pair| pair[0].id < pair[1].id)
                        && response.models.iter().all(|model| {
                            split_model_reference(&model.id).is_ok()
                                && validate_protocol_transports(&model.protocol, &model.transports)
                                && validate_string_set(
                                    &model.input_modalities,
                                    &["audio", "file", "image", "text"],
                                )
                                && validate_string_set(&model.output_modalities, &["audio", "text"])
                        })
                });
            assert_eq!(parsed.is_some(), item["valid"].as_bool().unwrap());
        }
    }

    #[test]
    fn decrypted_credentials_remain_bounded() {
        assert!(validate_credential("token").is_ok());
        assert!(validate_credential("").is_err());
        assert!(validate_credential("token\nvalue").is_err());
        assert!(validate_credential("token value").is_err());
        assert!(validate_credential("token-\u{5bc6}\u{94a5}").is_err());
        assert!(validate_credential(&"x".repeat(AI_CREDENTIAL_MAX_BYTES + 1)).is_err());
    }
}
