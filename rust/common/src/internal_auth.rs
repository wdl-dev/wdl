use std::env;

#[cfg(feature = "axum")]
use axum::{
    http::{HeaderMap, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
};
use subtle::ConstantTimeEq;

pub const INTERNAL_AUTH_HEADER: &str = "x-wdl-internal-auth";
pub const INTERNAL_AUTH_ENV: &str = "WDL_INTERNAL_AUTH_TOKEN";
pub const INTERNAL_AUTH_PREVIOUS_ENV: &str = "WDL_INTERNAL_AUTH_PREVIOUS_TOKEN";
pub const INTERNAL_AUTH_FAILURE_CODE: &str = "internal_auth_failed";
pub const INTERNAL_AUTH_FAILURE_MESSAGE: &str = "Internal authentication failed";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InternalAuthTokens {
    pub current: String,
    pub previous: Option<String>,
}

fn validate_internal_auth_token(name: &str, token: String) -> Result<String, String> {
    if token.is_empty()
        || token
            .bytes()
            .any(|byte| !(0x21..=0x7e).contains(&byte) || byte == b',')
    {
        return Err(format!(
            "{name} must be configured as visible ASCII without whitespace or commas"
        ));
    }
    Ok(token)
}

pub fn internal_auth_token_from_env() -> Result<String, String> {
    let token = env::var(INTERNAL_AUTH_ENV)
        .map_err(|_| format!("{INTERNAL_AUTH_ENV} must be configured"))?;
    validate_internal_auth_token(INTERNAL_AUTH_ENV, token)
}

pub fn internal_auth_tokens_from_env() -> Result<InternalAuthTokens, String> {
    let current = internal_auth_token_from_env()?;
    let previous = match env::var(INTERNAL_AUTH_PREVIOUS_ENV) {
        Ok(token) if token.is_empty() => None,
        Ok(token) => Some(validate_internal_auth_token(
            INTERNAL_AUTH_PREVIOUS_ENV,
            token,
        )?),
        Err(_) => None,
    };
    Ok(InternalAuthTokens { current, previous })
}

pub fn internal_auth_matches(actual: Option<&str>, expected: &str) -> bool {
    let Some(actual) = actual else {
        return false;
    };
    if actual.is_empty() {
        return false;
    }
    let actual = actual.as_bytes();
    let expected = expected.as_bytes();
    let max = actual.len().max(expected.len());
    let mut equal = actual.len().ct_eq(&expected.len());
    for i in 0..max {
        let actual_byte = *actual.get(i).unwrap_or(&0);
        let expected_byte = *expected.get(i).unwrap_or(&0);
        equal &= actual_byte.ct_eq(&expected_byte);
    }
    bool::from(equal)
}

pub fn internal_auth_matches_any(actual: Option<&str>, expected: &InternalAuthTokens) -> bool {
    internal_auth_matches(actual, expected.current.as_str())
        || expected
            .previous
            .as_deref()
            .is_some_and(|previous| internal_auth_matches(actual, previous))
}

#[cfg(feature = "axum")]
pub fn internal_auth_header_value(headers: &HeaderMap) -> Option<&str> {
    let mut values = headers.get_all(INTERNAL_AUTH_HEADER).iter();
    let value = values.next()?.to_str().ok()?;
    if values.next().is_some() {
        return None;
    }
    Some(value)
}

#[cfg(feature = "axum")]
pub fn internal_auth_headers_match(headers: &HeaderMap, expected: &InternalAuthTokens) -> bool {
    internal_auth_matches_any(internal_auth_header_value(headers), expected)
}

#[cfg(feature = "axum")]
pub fn internal_auth_failure_response() -> Response {
    let body = serde_json::json!({
        "error": INTERNAL_AUTH_FAILURE_CODE,
        "message": INTERNAL_AUTH_FAILURE_MESSAGE,
    })
    .to_string();
    (
        StatusCode::UNAUTHORIZED,
        [(CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contract() -> serde_json::Value {
        serde_json::from_str(include_str!(
            "../../../tests/fixtures/internal-auth-contract.json"
        ))
        .expect("internal auth contract fixture")
    }

    #[test]
    fn internal_auth_literals_and_rotation_match_contract() {
        let contract = contract();
        assert_eq!(contract["header"], INTERNAL_AUTH_HEADER);
        assert_eq!(contract["currentEnv"], INTERNAL_AUTH_ENV);
        assert_eq!(contract["previousEnv"], INTERNAL_AUTH_PREVIOUS_ENV);
        assert_eq!(contract["failure"]["error"], INTERNAL_AUTH_FAILURE_CODE);
        assert_eq!(
            contract["failure"]["message"],
            INTERNAL_AUTH_FAILURE_MESSAGE
        );

        for case in contract["tokenCases"].as_array().expect("tokenCases array") {
            let value = case["value"].as_str().expect("token value").to_string();
            assert_eq!(
                validate_internal_auth_token(INTERNAL_AUTH_ENV, value).is_ok(),
                case["accepted"].as_bool().expect("accepted boolean")
            );
        }

        for case in contract["rotationCases"]
            .as_array()
            .expect("rotationCases array")
        {
            let tokens = InternalAuthTokens {
                current: case["current"].as_str().expect("current token").to_string(),
                previous: case["previous"].as_str().map(str::to_string),
            };
            assert_eq!(
                internal_auth_matches_any(case["actual"].as_str(), &tokens),
                case["accepted"].as_bool().expect("accepted boolean")
            );
        }

        #[cfg(feature = "axum")]
        for case in contract["headerCases"]
            .as_array()
            .expect("headerCases array")
        {
            let tokens = InternalAuthTokens {
                current: case["current"].as_str().expect("current token").to_string(),
                previous: case["previous"].as_str().map(str::to_string),
            };
            let mut headers = HeaderMap::new();
            for value in case["values"].as_array().expect("header values") {
                headers.append(
                    INTERNAL_AUTH_HEADER,
                    value.as_str().expect("header value").parse().unwrap(),
                );
            }
            assert_eq!(
                internal_auth_headers_match(&headers, &tokens),
                case["accepted"].as_bool().expect("accepted boolean")
            );
        }
    }

    #[test]
    fn internal_auth_matches_requires_exact_token() {
        assert!(internal_auth_matches(Some("secret"), "secret"));
        assert!(!internal_auth_matches(Some("secret"), "secreu"));
        assert!(!internal_auth_matches(Some("secre"), "secret"));
        assert!(!internal_auth_matches(Some("secret"), "secret2"));
        assert!(!internal_auth_matches(Some(""), "secret"));
        assert!(!internal_auth_matches(None, "secret"));
    }

    #[test]
    fn internal_auth_matches_any_accepts_current_or_previous() {
        let tokens = InternalAuthTokens {
            current: "new".to_string(),
            previous: Some("old".to_string()),
        };
        assert!(internal_auth_matches_any(Some("new"), &tokens));
        assert!(internal_auth_matches_any(Some("old"), &tokens));
        assert!(!internal_auth_matches_any(Some("wrong"), &tokens));
        assert!(!internal_auth_matches_any(None, &tokens));
    }

    #[cfg(feature = "axum")]
    #[test]
    fn internal_auth_headers_match_accepts_current_or_previous_header() {
        let tokens = InternalAuthTokens {
            current: "new".to_string(),
            previous: Some("old".to_string()),
        };
        let mut headers = HeaderMap::new();

        headers.insert(INTERNAL_AUTH_HEADER, "new".parse().unwrap());
        assert!(internal_auth_headers_match(&headers, &tokens));

        headers.insert(INTERNAL_AUTH_HEADER, "old".parse().unwrap());
        assert!(internal_auth_headers_match(&headers, &tokens));

        headers.insert(INTERNAL_AUTH_HEADER, "wrong".parse().unwrap());
        assert!(!internal_auth_headers_match(&headers, &tokens));
    }

    #[test]
    fn internal_auth_tokens_must_be_visible_ascii_without_commas() {
        assert_eq!(
            validate_internal_auth_token(INTERNAL_AUTH_ENV, "ascii-token".to_string()).unwrap(),
            "ascii-token"
        );
        assert!(validate_internal_auth_token(INTERNAL_AUTH_ENV, "".to_string()).is_err());
        assert!(validate_internal_auth_token(INTERNAL_AUTH_ENV, "tokén".to_string()).is_err());
        assert!(
            validate_internal_auth_token(INTERNAL_AUTH_ENV, "token,other".to_string()).is_err()
        );
        assert!(validate_internal_auth_token(INTERNAL_AUTH_ENV, " token".to_string()).is_err());
        assert!(validate_internal_auth_token(INTERNAL_AUTH_ENV, "token ".to_string()).is_err());
        assert!(
            validate_internal_auth_token(INTERNAL_AUTH_ENV, "token\0value".to_string()).is_err()
        );
    }

    #[cfg(feature = "axum")]
    #[tokio::test]
    async fn internal_auth_failure_response_matches_contract() {
        let contract = contract();
        let response = internal_auth_failure_response();
        assert_eq!(
            u64::from(response.status().as_u16()),
            contract["failure"]["status"]
                .as_u64()
                .expect("failure status")
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("internal auth failure body");
        let body: serde_json::Value =
            serde_json::from_slice(&body).expect("internal auth failure JSON");
        assert_eq!(body["error"], contract["failure"]["error"]);
        assert_eq!(body["message"], contract["failure"]["message"]);
    }
}
