#[cfg(feature = "axum")]
use axum::http::HeaderMap;

#[cfg(feature = "axum")]
const REQUEST_ID_HEADER: &str = "x-request-id";

pub fn sanitize_request_id(raw: &str) -> Option<String> {
    let first = raw
        .split(',')
        .next()?
        .trim_matches(|ch: char| ch.is_ascii_whitespace());
    if first.is_empty() || first.len() > 128 {
        return None;
    }
    if first
        .bytes()
        .any(|byte| !(0x21..=0x7e).contains(&byte) || byte == b'"' || byte == b'\\')
    {
        return None;
    }
    Some(first.to_string())
}

#[cfg(feature = "axum")]
pub fn request_id_from_headers(headers: &HeaderMap) -> Option<String> {
    headers
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(sanitize_request_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn sanitize_request_id_keeps_bounded_first_token() {
        assert_eq!(
            sanitize_request_id(" rid-123 , ignored").as_deref(),
            Some("rid-123")
        );
        assert_eq!(
            sanitize_request_id(&"a".repeat(128)).as_deref(),
            Some(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            )
        );
    }

    #[test]
    fn sanitize_request_id_rejects_empty_long_or_unsafe_values() {
        assert_eq!(sanitize_request_id(""), None);
        assert_eq!(sanitize_request_id("   "), None);
        assert_eq!(sanitize_request_id(&"a".repeat(129)), None);
        assert_eq!(sanitize_request_id("bad id"), None);
        assert_eq!(sanitize_request_id("bad\"id"), None);
        assert_eq!(sanitize_request_id("bad\\id"), None);
        assert_eq!(sanitize_request_id("bad\nid"), None);
        assert_eq!(sanitize_request_id("café"), None);
        assert_eq!(sanitize_request_id("\u{85}rid"), None);
    }

    #[cfg(feature = "axum")]
    #[test]
    fn request_id_header_adapter_rejects_non_ascii_wire_values() {
        use axum::http::HeaderValue;

        let mut headers = HeaderMap::new();
        headers.insert(
            REQUEST_ID_HEADER,
            HeaderValue::from_bytes("é".as_bytes()).expect("obs-text header value"),
        );
        assert_eq!(request_id_from_headers(&headers), None);
    }

    #[test]
    fn sanitize_request_id_follows_cross_language_fixture_contract() {
        let fixtures: Vec<Value> = serde_json::from_str(include_str!(
            "../../../tests/fixtures/request-id-sanitizer.json"
        ))
        .expect("request-id sanitizer fixtures are valid JSON");
        for case in fixtures {
            let raw = case
                .get("raw")
                .and_then(Value::as_str)
                .expect("fixture raw is a string");
            let expected = case.get("sanitized").and_then(Value::as_str);
            assert_eq!(sanitize_request_id(raw).as_deref(), expected, "raw={raw:?}");
        }
    }
}
