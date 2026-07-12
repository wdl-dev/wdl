use std::str::FromStr;

use chrono::{TimeZone, Utc};
use chrono_tz::Tz;
use croner::Cron;

use crate::{SchedulerError, SchedulerResult};

pub(crate) fn slot_ms_for(ms: i64) -> i64 {
    ms - ms.rem_euclid(60_000)
}

pub(crate) fn slot_key(slot_ms: i64) -> String {
    format!("cron-slot:{slot_ms}")
}

pub(crate) fn slot_expire_at(slot_ms: i64) -> i64 {
    slot_ms.div_euclid(1000) + 600
}

pub(crate) fn wait_ms_until_next_slot(now_ms: i64) -> u64 {
    (slot_ms_for(now_ms) + 60_000 - now_ms).max(1) as u64
}

pub(crate) fn lease_key(slot_ms: i64, reference: &str) -> String {
    format!("cron-lease:{slot_ms}:{reference}")
}

pub(crate) fn next_fire_ms(cron: &str, timezone: &str, after_ms: i64) -> SchedulerResult<i64> {
    let schedule =
        Cron::from_str(cron).map_err(|err| SchedulerError::internal_error(err.to_string()))?;
    let tz: Tz = timezone
        .parse()
        .map_err(|_| SchedulerError::internal_error(format!("invalid timezone {timezone}")))?;
    let utc = Utc
        .timestamp_millis_opt(after_ms)
        .single()
        .ok_or_else(|| SchedulerError::internal_error(format!("invalid timestamp {after_ms}")))?;
    let local = utc.with_timezone(&tz);
    let next = schedule
        .find_next_occurrence(&local, false)
        .map_err(|err| SchedulerError::internal_error(err.to_string()))?;
    Ok(next.with_timezone(&Utc).timestamp_millis())
}

#[cfg(test)]
mod tests {
    use chrono::Timelike;

    use super::*;
    use crate::cron::reference::ref_for;
    use crate::test_fixtures::scheduler_projection_contract;

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct CronParityCase {
        id: String,
        cron: String,
        timezone: String,
        after_ms: i64,
        next_ms: Option<i64>,
        rust_next_ms: Option<i64>,
        known_divergence: Option<String>,
    }

    #[test]
    fn cron_key_builders_compose_scheduler_keys() {
        let fixture = scheduler_projection_contract();
        assert_eq!(slot_key(fixture.cron.slot_ms), fixture.cron.slot_key);
        assert_eq!(
            slot_expire_at(fixture.cron.slot_ms),
            fixture.cron.slot_expire_at
        );
        let reference = ref_for("demo", "hello", "abc123", 7);
        assert_eq!(
            lease_key(1_710_000_000_000, &reference),
            "cron-lease:1710000000000:demo:hello:abc123:7"
        );
    }

    #[test]
    fn cron_wall_clock_waits_for_the_next_minute_boundary() {
        assert_eq!(wait_ms_until_next_slot(1_710_000_000_000), 60_000);
        assert_eq!(wait_ms_until_next_slot(1_710_000_000_001), 59_999);
        assert_eq!(wait_ms_until_next_slot(1_710_000_059_999), 1);
    }

    #[test]
    fn next_fire_handles_timezone() {
        let after = Utc
            .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp_millis();
        let next = next_fire_ms("0 9 * * *", "Asia/Shanghai", after).unwrap();
        let next_utc = Utc.timestamp_millis_opt(next).unwrap();
        assert_eq!(next_utc.hour(), 1);
    }

    #[test]
    fn next_fire_matches_shared_js_rust_parity_fixture() {
        let cases: Vec<CronParityCase> =
            serde_json::from_str(include_str!("../../../../tests/fixtures/cron-parity.json"))
                .expect("cron parity fixture must parse");
        for case in cases {
            let expected = case
                .rust_next_ms
                .or(case.next_ms)
                .expect("case must set an expected time");
            assert_eq!(
                next_fire_ms(&case.cron, &case.timezone, case.after_ms).unwrap(),
                expected,
                "{}",
                case.id
            );
            if case.known_divergence.is_none() {
                assert_eq!(
                    case.next_ms,
                    Some(expected),
                    "{} should use shared expected time",
                    case.id
                );
            }
        }
    }
}
