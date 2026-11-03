use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis()
        .min(i64::MAX as u128) as i64
}

pub(crate) fn duration_ms_for_log(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

pub fn random_hex_64() -> String {
    let mut bytes = [0_u8; 8];
    getrandom::fill(&mut bytes).expect("OS random source must be available");
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn random_unit_f64() -> f64 {
    let mut bytes = [0_u8; 8];
    getrandom::fill(&mut bytes).expect("OS random source must be available");
    let value = u64::from_be_bytes(bytes) >> 11;
    (value as f64) * (1.0 / ((1_u64 << 53) as f64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_ms_returns_non_negative_unix_millis() {
        assert!(now_ms() >= 0);
    }

    #[test]
    fn duration_ms_for_log_returns_stable_integer_millis() {
        assert_eq!(duration_ms_for_log(Duration::from_nanos(2_309_406)), 2);
        assert_eq!(duration_ms_for_log(Duration::from_micros(10_837)), 10);
    }

    #[test]
    fn random_hex_64_is_16_lowercase_hex_chars() {
        let id = random_hex_64();
        assert_eq!(id.len(), 16, "instance id must be 16 hex chars");
        assert!(
            id.chars().all(|c| c.is_ascii_hexdigit()) && id == id.to_ascii_lowercase(),
            "instance id must be lowercase hex: {id}"
        );
    }

    #[test]
    fn random_unit_f64_stays_in_unit_interval() {
        for _ in 0..128 {
            let value = random_unit_f64();
            assert!(value >= 0.0);
            assert!(value < 1.0);
        }
    }
}
