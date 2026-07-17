use serde::Deserialize;
use wdl_rust_common::{
    identity::{is_valid_route_ns, is_valid_worker_name},
    worker_contract::parse_version_tag,
};

const CRON_WORKER_KEY_PREFIX: &str = "crons:";
pub(crate) const CRON_WORKER_KEY_SCAN_PATTERN: &str = "crons:*:*";

#[derive(Clone, Debug)]
pub(crate) struct RefParts {
    pub(crate) ns: String,
    pub(crate) worker: String,
    pub(crate) cron_id: String,
    pub(crate) r#gen: i64,
}

#[derive(Deserialize)]
struct CronMeta {
    version: String,
}

#[derive(Deserialize)]
pub(crate) struct CronEntry {
    pub(crate) cron: String,
    pub(crate) timezone: String,
    pub(crate) r#gen: i64,
}

pub(crate) enum RefVerdict {
    Fire {
        entry: CronEntry,
        active_version: String,
    },
    Stale(&'static str),
    Corrupt,
}

pub(crate) fn cron_worker_key(ns: &str, worker: &str) -> String {
    format!("{CRON_WORKER_KEY_PREFIX}{ns}:{worker}")
}

pub(crate) fn parse_cron_worker_key(key: &str) -> Option<(&str, &str)> {
    let (ns, worker) = key.strip_prefix(CRON_WORKER_KEY_PREFIX)?.split_once(':')?;
    if !is_valid_route_ns(ns) || !is_valid_worker_name(worker) {
        return None;
    }
    Some((ns, worker))
}

pub(crate) fn ref_for(ns: &str, worker: &str, cron_id: &str, r#gen: i64) -> String {
    format!("{ns}:{worker}:{cron_id}:{gen}")
}

pub(crate) fn parse_ref(reference: &str) -> Option<RefParts> {
    let parts = reference.split(':').collect::<Vec<_>>();
    if parts.len() != 4 {
        return None;
    }
    if parts.iter().any(|part| part.is_empty()) {
        return None;
    }
    if !is_valid_route_ns(parts[0]) || !is_valid_worker_name(parts[1]) {
        return None;
    }
    let r#gen = parts[3].parse::<i64>().ok()?;
    Some(RefParts {
        ns: parts[0].to_string(),
        worker: parts[1].to_string(),
        cron_id: parts[2].to_string(),
        r#gen,
    })
}

fn validated_cron_meta_version(meta: CronMeta) -> Option<String> {
    parse_version_tag(&meta.version).ok()?;
    Some(meta.version)
}

pub(crate) fn cron_meta_version(raw: &str) -> Option<String> {
    validated_cron_meta_version(serde_json::from_str::<CronMeta>(raw).ok()?)
}

pub(crate) fn classify_ref(
    parts: &RefParts,
    meta_str: Option<String>,
    entry_str: Option<String>,
) -> RefVerdict {
    let Some(meta_str) = meta_str else {
        return RefVerdict::Stale("missing_meta");
    };
    let Ok(meta) = serde_json::from_str::<CronMeta>(&meta_str) else {
        return RefVerdict::Corrupt;
    };
    let Some(entry_str) = entry_str else {
        return RefVerdict::Stale("missing_entry");
    };
    let Ok(entry) = serde_json::from_str::<CronEntry>(&entry_str) else {
        return RefVerdict::Corrupt;
    };
    let Some(active_version) = validated_cron_meta_version(meta) else {
        return RefVerdict::Corrupt;
    };
    if entry.r#gen != parts.r#gen {
        return RefVerdict::Stale("gen_mismatch");
    }
    RefVerdict::Fire {
        entry,
        active_version,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::test_fixtures::{scheduler_projection_contract, version_tag_cases};

    #[test]
    fn cron_projection_contract_matches_control_writer() {
        let fixture = scheduler_projection_contract();
        let cron = fixture.cron;

        assert_eq!(cron_worker_key(&cron.ns, &cron.worker), cron.worker_key);
        assert_eq!(
            parse_cron_worker_key(&cron.worker_key),
            Some((cron.ns.as_str(), cron.worker.as_str()))
        );
        assert_eq!(
            ref_for(&cron.ns, &cron.worker, &cron.cron_id, cron.r#gen),
            cron.reference
        );
        let parts = parse_ref(&cron.reference).expect("fixture cron reference must parse");
        match classify_ref(&parts, Some(cron.meta.json), Some(cron.entry.json)) {
            RefVerdict::Fire {
                entry,
                active_version,
            } => {
                assert_eq!(active_version, cron.meta.version);
                assert_eq!(entry.cron, cron.entry.cron);
                assert_eq!(entry.timezone, cron.entry.timezone);
                assert_eq!(entry.r#gen, cron.entry.r#gen);
            }
            _ => panic!("fixture cron projection must be fireable"),
        }
    }

    #[test]
    fn parse_ref_round_trip() {
        let reference = ref_for("demo", "hello", "abc123", 7);
        assert_eq!(reference, "demo:hello:abc123:7");
        let parts = parse_ref(&reference).unwrap();
        assert_eq!(parts.ns, "demo");
        assert_eq!(parts.worker, "hello");
        assert_eq!(parts.cron_id, "abc123");
        assert_eq!(parts.r#gen, 7);
    }

    #[test]
    fn parse_ref_rejects_malformed_refs() {
        assert!(parse_ref("demo:hello:abc123").is_none());
        assert!(parse_ref("demo:hello:abc123:7:extra").is_none());
        assert!(parse_ref("").is_none());
        assert!(parse_ref(":hello:abc123:7").is_none());
        assert!(parse_ref("demo::abc123:7").is_none());
        assert!(parse_ref("demo:hello::7").is_none());
        assert!(parse_ref("demo:hello:abc123:x").is_none());
        assert!(parse_ref("demo:hello:abc123:").is_none());
        assert!(parse_ref("__platform__:hello:abc123:7").is_none());
        assert!(parse_ref("Demo:hello:abc123:7").is_none());
        assert!(parse_ref("demo:/bad:abc123:7").is_none());
    }

    #[test]
    fn parse_cron_worker_key_rejects_non_worker_hash_keys() {
        assert_eq!(parse_cron_worker_key("queue:demo:jobs"), None);
        assert_eq!(parse_cron_worker_key("crons:demo"), None);
        assert_eq!(parse_cron_worker_key("crons::worker"), None);
        assert_eq!(parse_cron_worker_key("crons:demo:"), None);
        assert_eq!(parse_cron_worker_key("crons:demo:worker:extra"), None);
        assert_eq!(parse_cron_worker_key("crons:__platform__:worker"), None);
        assert_eq!(parse_cron_worker_key("crons:Demo:worker"), None);
        assert_eq!(parse_cron_worker_key("crons:demo:/bad"), None);
    }

    #[test]
    fn classify_ref_fire_carries_entry_and_active_version() {
        let parts = RefParts {
            ns: "demo".to_string(),
            worker: "hello".to_string(),
            cron_id: "abc".to_string(),
            r#gen: 3,
        };
        match classify_ref(
            &parts,
            Some(json!({ "version": "v5", "seq": 3 }).to_string()),
            Some(json!({ "cron": "*/5 * * * *", "timezone": "UTC", "gen": 3 }).to_string()),
        ) {
            RefVerdict::Fire {
                entry,
                active_version,
            } => {
                assert_eq!(active_version, "v5");
                assert_eq!(entry.cron, "*/5 * * * *");
                assert_eq!(entry.timezone, "UTC");
            }
            _ => panic!("expected fire verdict"),
        }
    }

    #[test]
    fn classify_ref_versions_match_cross_language_fixture() {
        let parts = RefParts {
            ns: "demo".to_string(),
            worker: "hello".to_string(),
            cron_id: "abc".to_string(),
            r#gen: 3,
        };
        let entry = Some(json!({ "cron": "*/5 * * * *", "timezone": "UTC", "gen": 3 }).to_string());
        for (version, valid) in version_tag_cases() {
            let verdict = classify_ref(
                &parts,
                Some(json!({ "version": version }).to_string()),
                entry.clone(),
            );
            assert_eq!(
                matches!(verdict, RefVerdict::Fire { .. }),
                valid,
                "{version:?}"
            );
        }
    }

    #[test]
    fn classify_ref_marks_missing_or_mismatched_state_stale() {
        let parts = RefParts {
            ns: "demo".to_string(),
            worker: "hello".to_string(),
            cron_id: "abc".to_string(),
            r#gen: 3,
        };
        assert!(matches!(
            classify_ref(
                &parts,
                None,
                Some(json!({ "cron": "*/5 * * * *", "timezone": "UTC", "gen": 3 }).to_string())
            ),
            RefVerdict::Stale("missing_meta")
        ));
        assert!(matches!(
            classify_ref(&parts, Some(json!({ "version": "v5" }).to_string()), None),
            RefVerdict::Stale("missing_entry")
        ));
        assert!(matches!(
            classify_ref(
                &parts,
                Some(json!({ "version": "v5" }).to_string()),
                Some(json!({ "cron": "*/5 * * * *", "timezone": "UTC", "gen": 4 }).to_string())
            ),
            RefVerdict::Stale("gen_mismatch")
        ));
    }

    #[test]
    fn classify_ref_marks_corrupt_json_separately_from_stale() {
        let parts = RefParts {
            ns: "demo".to_string(),
            worker: "hello".to_string(),
            cron_id: "abc".to_string(),
            r#gen: 3,
        };
        assert!(matches!(
            classify_ref(
                &parts,
                Some("{not json".to_string()),
                Some(json!({ "cron": "*/5 * * * *", "timezone": "UTC", "gen": 3 }).to_string())
            ),
            RefVerdict::Corrupt
        ));
        assert!(matches!(
            classify_ref(
                &parts,
                Some(json!({ "version": "v5" }).to_string()),
                Some("[broken".to_string())
            ),
            RefVerdict::Corrupt
        ));
    }
}
