use std::collections::HashMap;

use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SchedulerProjectionContract {
    pub(crate) cron: CronProjectionContract,
    pub(crate) queue_consumer: QueueConsumerProjectionContract,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CronProjectionContract {
    pub(crate) worker_index_key: String,
    pub(crate) ns: String,
    pub(crate) worker: String,
    pub(crate) worker_key: String,
    pub(crate) slot_ms: i64,
    pub(crate) slot_key: String,
    pub(crate) slot_expire_at: i64,
    pub(crate) cron_id: String,
    pub(crate) r#gen: i64,
    pub(crate) reference: String,
    pub(crate) meta: CronMetaContract,
    pub(crate) entry: CronEntryContract,
}

#[derive(Deserialize)]
pub(crate) struct CronMetaContract {
    pub(crate) version: String,
    pub(crate) json: String,
}

#[derive(Deserialize)]
pub(crate) struct CronEntryContract {
    pub(crate) cron: String,
    pub(crate) timezone: String,
    pub(crate) r#gen: i64,
    pub(crate) json: String,
}

#[derive(Deserialize)]
pub(crate) struct QueueConsumerProjectionContract {
    pub(crate) ns: String,
    pub(crate) queue: String,
    pub(crate) worker: String,
    pub(crate) version: String,
    pub(crate) fields: HashMap<String, String>,
}

pub(crate) fn scheduler_projection_contract() -> SchedulerProjectionContract {
    serde_json::from_str(include_str!(
        "../../../tests/fixtures/scheduler-projection-contract.json"
    ))
    .expect("scheduler projection contract fixture must parse")
}

pub(crate) fn version_tag_cases() -> Vec<(String, bool)> {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../tests/fixtures/version-tags.json"))
            .expect("version tag fixture must parse");
    fixture["cases"]
        .as_array()
        .expect("version tag fixture cases must be an array")
        .iter()
        .map(|entry| {
            (
                entry["tag"]
                    .as_str()
                    .expect("version tag must be a string")
                    .to_string(),
                entry["parsed"].as_u64().is_some(),
            )
        })
        .collect()
}
