use std::env;
use std::str::FromStr;

pub fn positive_or<T>(raw: Option<String>, fallback: T) -> T
where
    T: Copy + From<u8> + FromStr + PartialOrd,
{
    raw.and_then(|value| value.parse::<T>().ok())
        .filter(|value| *value > T::from(0))
        .unwrap_or(fallback)
}

pub fn env_positive<T>(name: &str, fallback: T) -> T
where
    T: Copy + From<u8> + FromStr + PartialOrd,
{
    positive_or(env::var(name).ok(), fallback)
}

pub fn env_u16(name: &str, fallback: u16) -> u16 {
    env_positive(name, fallback)
}

pub fn env_u64(name: &str, fallback: u64) -> u64 {
    env_positive(name, fallback)
}

pub fn env_usize(name: &str, fallback: usize) -> usize {
    env_positive(name, fallback)
}

pub fn positive_bounded_decimal_or(raw: Option<String>, fallback: u64, max: u64) -> u64 {
    raw.filter(|value| {
        let bytes = value.as_bytes();
        matches!(bytes.first(), Some(b'1'..=b'9')) && bytes.iter().all(u8::is_ascii_digit)
    })
    .and_then(|value| value.parse::<u64>().ok())
    .filter(|value| *value <= max)
    .unwrap_or(fallback)
}

/// Default endpoint used by sidecars when no Redis URL is configured.
pub const DEFAULT_REDIS_URL: &str = "redis://localhost:6379";

pub fn optional_env(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owner_ttl_fixture() -> serde_json::Value {
        serde_json::from_str(include_str!("../../../tests/fixtures/owner-ttl-env.json"))
            .expect("owner TTL fixture must be valid JSON")
    }

    fn owner_drain_timeout_fixture() -> serde_json::Value {
        serde_json::from_str(include_str!(
            "../../../tests/fixtures/owner-drain-timeout-env.json"
        ))
        .expect("owner drain timeout fixture must be valid JSON")
    }

    #[test]
    fn positive_or_accepts_only_positive_values() {
        assert_eq!(positive_or::<u16>(Some("42".to_string()), 7), 42);
        assert_eq!(positive_or::<u64>(Some("0".to_string()), 7), 7);
        assert_eq!(positive_or::<i64>(Some("-1".to_string()), 7), 7);
        assert_eq!(positive_or::<usize>(Some("-1".to_string()), 7), 7);
        assert_eq!(positive_or::<usize>(Some(String::new()), 7), 7);
        assert_eq!(positive_or::<usize>(Some("nope".to_string()), 7), 7);
        assert_eq!(positive_or::<usize>(Some("12.9".to_string()), 7), 7);
        assert_eq!(positive_or::<usize>(Some("1e3".to_string()), 7), 7);
        assert_eq!(
            positive_or::<u64>(Some("18446744073709551616".to_string()), 7),
            7
        );
        assert_eq!(positive_or::<usize>(None, 7), 7);
    }

    #[test]
    fn owner_ttl_values_match_the_cross_language_contract() {
        let fixture = owner_ttl_fixture();
        let fallback = fixture["fallback"].as_u64().expect("fallback");
        let max = fixture["max"].as_u64().expect("max");
        for case in fixture["cases"].as_array().expect("cases") {
            let raw = case["raw"].as_str().map(str::to_owned);
            let expected = case["expected"].as_u64().expect("expected");
            assert_eq!(
                positive_bounded_decimal_or(raw, fallback, max),
                expected,
                "{}",
                case["name"].as_str().expect("name")
            );
        }
    }

    #[test]
    fn owner_drain_timeout_values_match_the_cross_language_contract() {
        let fixture = owner_drain_timeout_fixture();
        let fallback = fixture["fallback"].as_u64().expect("fallback");
        let max = fixture["max"].as_u64().expect("max");
        for case in fixture["cases"].as_array().expect("cases") {
            let raw = case["raw"].as_str().map(str::to_owned);
            let expected = case["expected"].as_u64().expect("expected");
            assert_eq!(
                positive_bounded_decimal_or(raw, fallback, max),
                expected,
                "{}",
                case["name"].as_str().expect("name")
            );
        }
    }
}
