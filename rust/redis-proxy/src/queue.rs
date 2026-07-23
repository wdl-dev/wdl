use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use redis::Pipeline;
use serde::{Deserialize, Serialize};
use wdl_rust_common::identity::is_valid_runtime_load_ns;
use wdl_rust_common::queue_keys::{
    QUEUE_DELAYED_INDEX_KEY, QUEUE_DELAYED_WAKE_KEY_FIELD, QUEUE_DELAYED_WAKE_STREAM,
    QUEUE_DELAYED_WAKE_VISIBLE_AT_FIELD, QUEUE_STREAM_INDEX_KEY, is_valid_queue_name,
    queue_delayed_key, queue_stream_key,
};

use crate::{AppError, AppResult, AppState, empty};

pub(crate) const MAX_QUEUE_MESSAGE_BYTES: usize = 128_000;
pub(crate) const MAX_QUEUE_BATCH_MESSAGES: usize = 100;
pub(crate) const MAX_QUEUE_BATCH_BYTES: usize = 256_000;
const QUEUE_DELAYED_WAKE_MAX_LEN: usize = 1000;
const MAX_SAFE_VISIBLE_AT: f64 = 9_007_199_254_740_991.0;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NsIdParams {
    ns: String,
    id: String,
}

impl NsIdParams {
    fn validate_scope(&self) -> AppResult<()> {
        if !is_valid_runtime_load_ns(&self.ns) {
            return Err(AppError::bad_request("invalid queue namespace"));
        }
        if !is_valid_queue_name(&self.id) {
            return Err(AppError::bad_request("invalid queue name"));
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct QueueAction {
    entry: Option<QueueEntry>,
    #[serde(rename = "visibleAt")]
    visible_at: Option<f64>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct QueueEntry {
    id: String,
    body_b64: String,
    content_type: String,
    attempts: String,
    first_seen_ms: String,
}

pub(crate) fn pipe_delayed_wake(pipe: &mut Pipeline, delayed_key: &str, visible_at: f64) {
    pipe.cmd("XADD")
        .arg(QUEUE_DELAYED_WAKE_STREAM)
        .arg("MAXLEN")
        .arg("~")
        .arg(QUEUE_DELAYED_WAKE_MAX_LEN)
        .arg("*")
        .arg(QUEUE_DELAYED_WAKE_KEY_FIELD)
        .arg(delayed_key)
        .arg(QUEUE_DELAYED_WAKE_VISIBLE_AT_FIELD)
        .arg(visible_at.to_string());
}

pub(crate) fn pipe_queue_discovery_indexes(
    pipe: &mut Pipeline,
    stream_key: &str,
    delayed_key: &str,
    has_immediate: bool,
    has_delayed: bool,
) {
    if has_immediate {
        pipe.cmd("SADD").arg(QUEUE_STREAM_INDEX_KEY).arg(stream_key);
    }
    if has_delayed {
        pipe.cmd("SADD")
            .arg(QUEUE_DELAYED_INDEX_KEY)
            .arg(delayed_key);
    }
}

fn pipe_immediate_queue_entry(pipe: &mut Pipeline, stream_key: &str, entry: QueueEntry) {
    pipe.cmd("XADD")
        .arg(stream_key)
        .arg("*")
        .arg("id")
        .arg(entry.id)
        .arg("body_b64")
        .arg(entry.body_b64)
        .arg("content_type")
        .arg(entry.content_type)
        .arg("attempts")
        .arg(entry.attempts)
        .arg("first_seen_ms")
        .arg(entry.first_seen_ms);
}

fn build_queue_send_pipeline(
    actions: Vec<QueueAction>,
    stream_key: &str,
    delayed_key: &str,
) -> AppResult<Pipeline> {
    let has_delayed = actions
        .iter()
        .any(|action| action.visible_at.unwrap_or(0.0) > 0.0);
    let has_immediate = actions
        .iter()
        .any(|action| action.visible_at.unwrap_or(0.0) <= 0.0);
    let mut pipe = Pipeline::new();
    // Single immediate sends stay non-transactional. Consumer registration
    // and empty-index backfill repair an index write that loses a race with XADD.
    if actions.len() > 1 || has_delayed {
        pipe.atomic();
    }

    let mut delayed_entries = Vec::new();
    let mut earliest_delayed = None;
    for action in actions {
        let entry = action
            .entry
            .ok_or_else(|| AppError::bad_request("queue action missing entry"))?;
        let visible_at = action.visible_at.unwrap_or(0.0);
        if visible_at > 0.0 {
            delayed_entries.push((
                visible_at.to_string(),
                serde_json::to_string(&entry).map_err(AppError::internal_json)?,
            ));
            earliest_delayed =
                Some(earliest_delayed.map_or(visible_at, |earliest: f64| earliest.min(visible_at)));
        } else {
            pipe_immediate_queue_entry(&mut pipe, stream_key, entry);
        }
    }

    if let Some(earliest_delayed) = earliest_delayed {
        let mut zadd = redis::cmd("ZADD");
        zadd.arg(delayed_key);
        for (score, member) in delayed_entries {
            zadd.arg(score).arg(member);
        }
        pipe.add_command(zadd);
        pipe_delayed_wake(&mut pipe, delayed_key, earliest_delayed);
    }

    pipe_queue_discovery_indexes(
        &mut pipe,
        stream_key,
        delayed_key,
        has_immediate,
        has_delayed,
    );
    Ok(pipe)
}

// Validate the standard btoa alphabet and compute the exact decoded byte count
// without allocating the decoded payload just to enforce producer size caps.
pub(crate) fn base64_decoded_len(encoded: &str) -> AppResult<usize> {
    let bytes = encoded.as_bytes();
    if bytes.is_empty() {
        return Ok(0);
    }
    if !bytes.len().is_multiple_of(4) {
        return Err(AppError::bad_request(
            "queue action body_b64 is not valid base64",
        ));
    }
    let padding = if bytes.ends_with(b"==") {
        2
    } else if bytes.ends_with(b"=") {
        1
    } else {
        0
    };
    for (idx, b) in bytes.iter().enumerate() {
        let in_padding = idx >= bytes.len() - padding;
        let valid_data = b.is_ascii_alphanumeric() || *b == b'+' || *b == b'/';
        if (in_padding && *b != b'=') || (!in_padding && !valid_data) {
            return Err(AppError::bad_request(
                "queue action body_b64 is not valid base64",
            ));
        }
    }
    Ok((bytes.len() / 4) * 3 - padding)
}

pub(crate) fn validate_queue_actions(actions: &[QueueAction]) -> AppResult<()> {
    if actions.is_empty() {
        return Err(AppError::bad_request(
            "body must be a non-empty action array",
        ));
    }
    if actions.len() > MAX_QUEUE_BATCH_MESSAGES {
        return Err(AppError::bad_request(format!(
            "queue batch exceeds {MAX_QUEUE_BATCH_MESSAGES} message limit"
        )));
    }
    let mut total_bytes = 0_usize;
    for action in actions {
        if let Some(visible_at) = action.visible_at
            && (!visible_at.is_finite()
                || visible_at < 0.0
                || visible_at.fract() != 0.0
                || visible_at > MAX_SAFE_VISIBLE_AT)
        {
            return Err(AppError::bad_request(
                "queue action visibleAt must be a non-negative integer timestamp",
            ));
        }
        let entry = action
            .entry
            .as_ref()
            .ok_or_else(|| AppError::bad_request("queue action missing entry"))?;
        let body_bytes = base64_decoded_len(&entry.body_b64)?;
        if body_bytes > MAX_QUEUE_MESSAGE_BYTES {
            return Err(AppError::bad_request(format!(
                "queue message body exceeds {MAX_QUEUE_MESSAGE_BYTES} byte limit"
            )));
        }
        total_bytes = total_bytes
            .checked_add(body_bytes)
            .ok_or_else(|| AppError::bad_request("queue batch byte count overflow"))?;
        if total_bytes > MAX_QUEUE_BATCH_BYTES {
            return Err(AppError::bad_request(format!(
                "queue batch body exceeds {MAX_QUEUE_BATCH_BYTES} byte limit"
            )));
        }
    }
    Ok(())
}

pub(crate) fn parse_queue_actions_body(body: &[u8]) -> AppResult<Vec<QueueAction>> {
    serde_json::from_slice(body).map_err(|err| {
        AppError::bad_request(format!("body must be a valid queue action array: {err}"))
    })
}

pub(crate) async fn queue_send(
    State(state): State<AppState>,
    Query(q): Query<NsIdParams>,
    body: Bytes,
) -> AppResult<Response> {
    q.validate_scope()?;
    let actions = parse_queue_actions_body(&body)?;
    validate_queue_actions(&actions)?;
    let stream_key = queue_stream_key(&q.ns, &q.id);
    let delayed_key = queue_delayed_key(&q.ns, &q.id);
    let pipe = build_queue_send_pipeline(actions, &stream_key, &delayed_key)?;
    let _: redis::Value = state
        .with_redis(async |mut conn| pipe.query_async(&mut conn).await)
        .await?;
    Ok(empty(StatusCode::NO_CONTENT))
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use serde_json::json;

    use super::*;
    use crate::test_support::parse_packed_commands;

    fn queue_action(body_bytes: usize) -> QueueAction {
        serde_json::from_value(json!({
            "entry": {
                "id": "msg",
                "body_b64": BASE64_STANDARD.encode(vec![b'x'; body_bytes]),
                "content_type": "text",
                "attempts": "0",
                "first_seen_ms": "1"
            },
            "visibleAt": 0
        }))
        .unwrap()
    }

    fn named_queue_action(id: &str, visible_at: f64) -> QueueAction {
        let mut action = queue_action(1);
        action.entry.as_mut().unwrap().id = id.to_string();
        action.visible_at = Some(visible_at);
        action
    }

    #[test]
    fn queue_params_validate_runtime_load_namespace_and_queue_name() {
        let valid: NsIdParams = serde_urlencoded::from_str("ns=demo&id=jobs-1").unwrap();
        valid.validate_scope().unwrap();
        let system: NsIdParams = serde_urlencoded::from_str("ns=__system__&id=jobs").unwrap();
        system.validate_scope().unwrap();
        let platform: NsIdParams = serde_urlencoded::from_str("ns=__platform__&id=jobs").unwrap();
        platform.validate_scope().unwrap();

        for raw in [
            "ns=admin&id=jobs",
            "ns=__community__&id=jobs",
            "ns=bad:ns&id=jobs",
            "ns=demo&id=Jobs",
            "ns=demo&id=bad:id",
        ] {
            let q: NsIdParams = serde_urlencoded::from_str(raw).unwrap();
            let err = q.validate_scope().unwrap_err();
            assert_eq!(err.status, StatusCode::BAD_REQUEST, "{raw}");
        }
    }

    #[test]
    fn parse_queue_actions_body_maps_bad_json_to_invalid_request() {
        let err = match parse_queue_actions_body(b"{") {
            Ok(_) => panic!("expected invalid queue JSON to fail"),
            Err(err) => err,
        };
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert_eq!(err.code, "invalid_request");
        assert!(err.message.contains("valid queue action array"));
    }

    #[test]
    fn queue_limits_accept_cf_max_message_and_batch_size() {
        let actions = vec![queue_action(MAX_QUEUE_MESSAGE_BYTES)];
        validate_queue_actions(&actions).unwrap();

        let actions = (0..MAX_QUEUE_BATCH_MESSAGES)
            .map(|_| queue_action(1))
            .collect::<Vec<_>>();
        validate_queue_actions(&actions).unwrap();
    }

    #[test]
    fn queue_limits_reject_empty_and_too_many_messages() {
        let err = validate_queue_actions(&[]).unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);

        let actions = (0..=MAX_QUEUE_BATCH_MESSAGES)
            .map(|_| queue_action(1))
            .collect::<Vec<_>>();
        let err = validate_queue_actions(&actions).unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert_eq!(err.code, "invalid_request");
        assert!(err.message.contains("message limit"));
    }

    #[test]
    fn queue_limits_reject_oversized_message_and_batch_body() {
        let err = validate_queue_actions(&[queue_action(MAX_QUEUE_MESSAGE_BYTES + 1)]).unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert_eq!(err.code, "invalid_request");
        assert!(err.message.contains("message body"));

        let actions = vec![
            queue_action(100_000),
            queue_action(100_000),
            queue_action(100_001),
        ];
        let err = validate_queue_actions(&actions).unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert_eq!(err.code, "invalid_request");
        assert!(err.message.contains("batch body"));
    }

    #[test]
    fn queue_limits_reject_invalid_base64_body() {
        let actions: Vec<QueueAction> = serde_json::from_value(json!([{
            "entry": {
                "id": "msg",
                "body_b64": "not base64!!!",
                "content_type": "text",
                "attempts": "0",
                "first_seen_ms": "1"
            },
            "visibleAt": 0
        }]))
        .unwrap();
        let err = validate_queue_actions(&actions).unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert_eq!(err.code, "invalid_request");
        assert!(err.message.contains("base64"));
    }

    #[test]
    fn queue_limits_reject_non_canonical_visible_at() {
        for visible_at in [
            -1.0,
            0.5,
            MAX_SAFE_VISIBLE_AT + 2.0,
            f64::NAN,
            f64::INFINITY,
        ] {
            let mut action = queue_action(1);
            action.visible_at = Some(visible_at);
            let err = validate_queue_actions(&[action]).unwrap_err();
            assert_eq!(err.status, StatusCode::BAD_REQUEST);
            assert_eq!(err.code, "invalid_request");
            assert!(err.message.contains("visibleAt"));
        }

        let mut immediate = queue_action(1);
        immediate.visible_at = Some(0.0);
        validate_queue_actions(&[immediate]).unwrap();

        let mut delayed = queue_action(1);
        delayed.visible_at = Some(1_780_000_000_000.0);
        validate_queue_actions(&[delayed]).unwrap();
    }

    #[test]
    fn queue_base64_length_handles_padding_without_decoding() {
        assert_eq!(base64_decoded_len("").unwrap(), 0);
        assert_eq!(base64_decoded_len("YQ==").unwrap(), 1);
        assert_eq!(base64_decoded_len("YWI=").unwrap(), 2);
        assert_eq!(base64_decoded_len("YWJj").unwrap(), 3);

        for invalid in ["Y", "Y===", "YW=J", "not base64!!!"] {
            let err = base64_decoded_len(invalid).unwrap_err();
            assert_eq!(err.status, StatusCode::BAD_REQUEST);
            assert_eq!(err.code, "invalid_request");
            assert!(err.message.contains("base64"));
        }
    }

    #[test]
    fn delayed_queue_wake_pipeline_is_bounded_and_names_the_delayed_key() {
        let mut pipe = redis::pipe();
        pipe_delayed_wake(&mut pipe, "queue-delayed:demo:jobs", 1_234_567.0);
        let packed = String::from_utf8(pipe.get_packed_pipeline()).unwrap();
        assert!(packed.contains(QUEUE_DELAYED_WAKE_STREAM));
        assert!(packed.contains("MAXLEN"));
        assert!(packed.contains(QUEUE_DELAYED_WAKE_KEY_FIELD));
        assert!(packed.contains("queue-delayed:demo:jobs"));
        assert!(packed.contains(QUEUE_DELAYED_WAKE_VISIBLE_AT_FIELD));
    }

    #[test]
    fn queue_send_pipeline_batches_delayed_entries_and_preserves_immediate_stream() {
        let delayed_key = "queue-delayed:demo:jobs";
        let stream_key = "queue:demo:jobs:s";
        let later = named_queue_action("later", 200.0);
        let later_json = serde_json::to_string(later.entry.as_ref().unwrap()).unwrap();
        let sooner = named_queue_action("sooner", 100.0);
        let sooner_json = serde_json::to_string(sooner.entry.as_ref().unwrap()).unwrap();
        let actions = vec![named_queue_action("now", 0.0), later, sooner];

        let pipe = build_queue_send_pipeline(actions, stream_key, delayed_key).unwrap();
        let commands = parse_packed_commands(&pipe.get_packed_pipeline());

        assert_eq!(commands.first().unwrap(), &["MULTI"]);
        assert_eq!(commands.last().unwrap(), &["EXEC"]);

        let main_xadds = commands
            .iter()
            .filter(|command| {
                command.first().map(String::as_str) == Some("XADD") && command[1] == stream_key
            })
            .collect::<Vec<_>>();
        assert_eq!(main_xadds.len(), 1);
        assert!(!main_xadds[0].iter().any(|arg| arg == "MAXLEN"));

        let zadds = commands
            .iter()
            .filter(|command| command.first().map(String::as_str) == Some("ZADD"))
            .collect::<Vec<_>>();
        assert_eq!(
            zadds,
            vec![&vec![
                "ZADD".to_string(),
                delayed_key.to_string(),
                "200".to_string(),
                later_json,
                "100".to_string(),
                sooner_json,
            ]]
        );

        let wakes = commands
            .iter()
            .filter(|command| {
                command.first().map(String::as_str) == Some("XADD")
                    && command[1] == QUEUE_DELAYED_WAKE_STREAM
            })
            .collect::<Vec<_>>();
        assert_eq!(wakes.len(), 1);
        assert_eq!(
            wakes[0]
                .windows(2)
                .find(|pair| pair[0] == QUEUE_DELAYED_WAKE_VISIBLE_AT_FIELD)
                .unwrap()[1],
            "100"
        );
    }

    #[test]
    fn queue_send_pipeline_keeps_single_immediate_send_non_transactional() {
        let pipe = build_queue_send_pipeline(
            vec![named_queue_action("now", 0.0)],
            "queue:demo:jobs:s",
            "queue-delayed:demo:jobs",
        )
        .unwrap();
        let commands = parse_packed_commands(&pipe.get_packed_pipeline());
        assert!(
            !commands
                .iter()
                .flatten()
                .any(|arg| arg == "MULTI" || arg == "EXEC")
        );
    }

    #[test]
    fn queue_send_indexes_main_and_delayed_keys() {
        let mut pipe = redis::pipe();
        pipe_queue_discovery_indexes(
            &mut pipe,
            "queue:demo:jobs:s",
            "queue-delayed:demo:jobs",
            true,
            true,
        );

        let commands = parse_packed_commands(&pipe.get_packed_pipeline());
        assert_eq!(
            commands,
            vec![
                vec![
                    "SADD".to_string(),
                    QUEUE_STREAM_INDEX_KEY.to_string(),
                    "queue:demo:jobs:s".to_string(),
                ],
                vec![
                    "SADD".to_string(),
                    QUEUE_DELAYED_INDEX_KEY.to_string(),
                    "queue-delayed:demo:jobs".to_string(),
                ],
            ]
        );

        let mut pipe = redis::pipe();
        pipe_queue_discovery_indexes(
            &mut pipe,
            "queue:demo:jobs:s",
            "queue-delayed:demo:jobs",
            true,
            false,
        );
        assert_eq!(
            parse_packed_commands(&pipe.get_packed_pipeline()),
            vec![vec![
                "SADD".to_string(),
                QUEUE_STREAM_INDEX_KEY.to_string(),
                "queue:demo:jobs:s".to_string(),
            ]]
        );

        let mut pipe = redis::pipe();
        pipe_queue_discovery_indexes(
            &mut pipe,
            "queue:demo:jobs:s",
            "queue-delayed:demo:jobs",
            false,
            true,
        );
        assert_eq!(
            parse_packed_commands(&pipe.get_packed_pipeline()),
            vec![vec![
                "SADD".to_string(),
                QUEUE_DELAYED_INDEX_KEY.to_string(),
                "queue-delayed:demo:jobs".to_string(),
            ]]
        );
    }
}
