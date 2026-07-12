pub fn sanitize_request_id(raw: &str) -> Option<String> {
    let first = raw
        .split(',')
        .next()?
        .trim_matches(|ch: char| ch.is_whitespace() || ch == '\u{feff}');
    if first.is_empty() || first.len() > 128 {
        return None;
    }
    if first.chars().any(|ch| {
        ch.is_whitespace()
            || ch == '\u{feff}'
            || ch == '"'
            || ch == '\\'
            || (ch as u32) < 0x20
            || ch == '\u{7f}'
            || ('\u{80}'..='\u{9f}').contains(&ch)
    }) {
        return None;
    }
    Some(first.to_string())
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
