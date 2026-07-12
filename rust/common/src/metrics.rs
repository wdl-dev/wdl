use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Mutex, MutexGuard};

#[cfg(feature = "axum")]
use axum::body::Body;
#[cfg(feature = "axum")]
use axum::http::HeaderValue;
#[cfg(feature = "axum")]
use axum::http::header::{CONTENT_LENGTH, CONTENT_TYPE};
#[cfg(feature = "axum")]
use axum::response::Response;
use serde_json::json;

use crate::hash::fnv1a64;
use crate::log::{LogLevel, emit_log_line};

const CARDINALITY_WARN_LIMIT: usize = 100;
const METRIC_SHARDS: usize = 16;
#[cfg(feature = "axum")]
const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

pub struct MetricStore {
    counters: Vec<Mutex<HashMap<String, MetricSample>>>,
    gauges: Vec<Mutex<HashMap<String, MetricSample>>>,
    summaries: Vec<Mutex<HashMap<String, SummarySample>>>,
    cardinality: Mutex<CardinalityState>,
}

#[derive(Clone)]
struct MetricSample {
    name: String,
    labels: BTreeMap<String, String>,
    value: f64,
}

#[derive(Clone)]
struct SummarySample {
    name: String,
    labels: BTreeMap<String, String>,
    count: u64,
    sum: f64,
    max: f64,
}

#[derive(Default)]
struct CardinalityState {
    series_by_name: HashMap<String, usize>,
    warned: HashSet<String>,
}

impl MetricStore {
    pub fn increment(&self, name: &str, labels: &[(&str, &str)], delta: f64) {
        let key = metric_key_for_label_pairs(name, labels);
        let mut counters = lock_metric(&self.counters[metric_shard(&key)]);
        let mut inserted = false;
        counters
            .entry(key)
            .and_modify(|sample| sample.value += delta)
            .or_insert_with(|| {
                inserted = true;
                MetricSample {
                    name: name.to_string(),
                    labels: labels_map(labels),
                    value: delta,
                }
            });
        drop(counters);
        if inserted {
            self.track_series(name);
        }
    }

    pub fn observe(&self, name: &str, labels: &[(&str, &str)], value: f64) {
        let key = metric_key_for_label_pairs(name, labels);
        let mut summaries = lock_metric(&self.summaries[metric_shard(&key)]);
        let mut inserted = false;
        summaries
            .entry(key)
            .and_modify(|sample| {
                sample.count += 1;
                sample.sum += value;
                sample.max = sample.max.max(value);
            })
            .or_insert_with(|| {
                inserted = true;
                SummarySample {
                    name: name.to_string(),
                    labels: labels_map(labels),
                    count: 1,
                    sum: value,
                    max: value,
                }
            });
        drop(summaries);
        if inserted {
            self.track_series(name);
        }
    }

    pub fn add_gauge(&self, name: &str, labels: &[(&str, &str)], delta: f64) {
        let key = metric_key_for_label_pairs(name, labels);
        let mut gauges = lock_metric(&self.gauges[metric_shard(&key)]);
        let mut inserted = false;
        gauges
            .entry(key)
            .and_modify(|sample| sample.value += delta)
            .or_insert_with(|| {
                inserted = true;
                MetricSample {
                    name: name.to_string(),
                    labels: labels_map(labels),
                    value: delta,
                }
            });
        drop(gauges);
        if inserted {
            self.track_series(name);
        }
    }

    pub fn render_prometheus(&self) -> String {
        let mut lines = Vec::new();
        let mut emitted_types = HashSet::new();

        let mut counters = self.collect_counters();
        counters.sort_by(sample_cmp);
        for sample in counters {
            let suffix = format!("wdl_{}_total", sample.name);
            emit_type(&mut lines, &mut emitted_types, &suffix, "counter");
            lines.push(format!(
                "{}{} {}",
                suffix,
                format_labels(&sample.labels),
                sample.value
            ));
        }

        let mut gauges = self.collect_gauges();
        gauges.sort_by(sample_cmp);
        for sample in gauges {
            let name = format!("wdl_{}", sample.name);
            emit_type(&mut lines, &mut emitted_types, &name, "gauge");
            lines.push(format!(
                "{}{} {}",
                name,
                format_labels(&sample.labels),
                sample.value
            ));
        }

        let mut summaries = self.collect_summaries();
        summaries.sort_by(summary_sample_cmp);
        for sample in &summaries {
            let base = format!("wdl_{}", sample.name);
            emit_type(&mut lines, &mut emitted_types, &base, "summary");
            lines.push(format!(
                "{}_count{} {}",
                base,
                format_labels(&sample.labels),
                sample.count
            ));
            lines.push(format!(
                "{}_sum{} {}",
                base,
                format_labels(&sample.labels),
                sample.sum
            ));
        }
        for sample in &summaries {
            let name = format!("wdl_{}_max", sample.name);
            emit_type(&mut lines, &mut emitted_types, &name, "gauge");
            lines.push(format!(
                "{}{} {}",
                name,
                format_labels(&sample.labels),
                sample.max
            ));
        }

        lines.join("\n") + "\n"
    }

    fn collect_counters(&self) -> Vec<MetricSample> {
        collect_samples(&self.counters)
    }

    fn collect_gauges(&self) -> Vec<MetricSample> {
        collect_samples(&self.gauges)
    }

    fn collect_summaries(&self) -> Vec<SummarySample> {
        collect_samples(&self.summaries)
    }

    fn track_series(&self, name: &str) {
        let mut state = lock_metric(&self.cardinality);
        let next = state.series_by_name.get(name).copied().unwrap_or(0) + 1;
        state.series_by_name.insert(name.to_string(), next);
        if next >= CARDINALITY_WARN_LIMIT && state.warned.insert(name.to_string()) {
            emit_log_line(
                "observability",
                LogLevel::Warn,
                LogLevel::Debug,
                "metric_cardinality_warning",
                json!({
                    "metric": name,
                    "series": next,
                    "limit": CARDINALITY_WARN_LIMIT,
                }),
            );
        }
    }
}

impl Default for MetricStore {
    fn default() -> Self {
        Self {
            counters: metric_shards(),
            gauges: metric_shards(),
            summaries: metric_shards(),
            cardinality: Mutex::new(CardinalityState::default()),
        }
    }
}

#[cfg(feature = "axum")]
pub fn prometheus_response(metrics: &MetricStore) -> Response {
    let body = metrics.render_prometheus();
    let len = body.len();
    let mut response = Response::new(Body::from(body));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static(PROMETHEUS_CONTENT_TYPE),
    );
    response.headers_mut().insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&len.to_string()).expect("content length is ASCII digits"),
    );
    response
}

pub fn labels_map(labels: &[(&str, &str)]) -> BTreeMap<String, String> {
    labels
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

pub fn metric_key(name: &str, labels: &BTreeMap<String, String>) -> String {
    let suffix = labels
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("{name}|{suffix}")
}

fn metric_key_for_label_pairs(name: &str, labels: &[(&str, &str)]) -> String {
    if labels.is_empty() {
        return format!("{name}|");
    }
    let mut sorted = labels.to_vec();
    sorted.sort_by(|left, right| left.0.cmp(right.0));
    if sorted.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return metric_key(name, &labels_map(labels));
    }
    let label_len = sorted
        .iter()
        .map(|(key, value)| key.len() + 1 + value.len())
        .sum::<usize>();
    let mut out =
        String::with_capacity(name.len() + 1 + label_len + sorted.len().saturating_sub(1));
    out.push_str(name);
    out.push('|');
    for (index, (key, value)) in sorted.into_iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(key);
        out.push('=');
        out.push_str(value);
    }
    out
}

pub fn format_labels(labels: &BTreeMap<String, String>) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let body = labels
        .iter()
        .map(|(key, value)| format!(r#"{key}="{}""#, escape_label(value)))
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{body}}}")
}

pub fn escape_label(value: &str) -> String {
    value
        .replace('\\', r"\\")
        .replace('\n', r"\n")
        .replace('\r', r"\r")
        .replace('"', r#"\""#)
}

fn collect_samples<T: Clone>(shards: &[Mutex<HashMap<String, T>>]) -> Vec<T> {
    shards
        .iter()
        .flat_map(|shard| lock_metric(shard).values().cloned().collect::<Vec<_>>())
        .collect()
}

fn lock_metric<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn metric_shards<T>() -> Vec<Mutex<HashMap<String, T>>> {
    (0..METRIC_SHARDS)
        .map(|_| Mutex::new(HashMap::new()))
        .collect()
}

fn metric_shard(key: &str) -> usize {
    fnv1a64(key.as_bytes()) as usize % METRIC_SHARDS
}

fn emit_type(lines: &mut Vec<String>, emitted: &mut HashSet<String>, name: &str, kind: &str) {
    if emitted.insert(name.to_string()) {
        lines.push(format!("# TYPE {name} {kind}"));
    }
}

fn sample_cmp(left: &MetricSample, right: &MetricSample) -> std::cmp::Ordering {
    left.name
        .cmp(&right.name)
        .then_with(|| left.labels.cmp(&right.labels))
}

fn summary_sample_cmp(left: &SummarySample, right: &SummarySample) -> std::cmp::Ordering {
    left.name
        .cmp(&right.name)
        .then_with(|| left.labels.cmp(&right.labels))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::observability_contract;

    static PANIC_HOOK_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn metrics_contract_matches_cross_language_fixture() {
        let contract = observability_contract();
        let prefix = contract["metricPrefix"]
            .as_str()
            .expect("metricPrefix is a string");
        assert_eq!(prefix, "wdl");
        assert_eq!(
            contract["cardinalityWarnLimit"]
                .as_u64()
                .expect("cardinalityWarnLimit is a number") as usize,
            CARDINALITY_WARN_LIMIT
        );
        #[cfg(feature = "axum")]
        assert_eq!(
            contract["prometheusContentType"]
                .as_str()
                .expect("prometheusContentType is a string"),
            PROMETHEUS_CONTENT_TYPE
        );

        let metrics = MetricStore::default();
        metrics.increment(
            "requests",
            &[("service", "test"), ("route", "healthz"), ("status", "200")],
            1.0,
        );
        metrics.observe(
            "request_duration_ms",
            &[("service", "test"), ("route", "healthz")],
            1.0,
        );
        metrics.increment(
            "request_errors",
            &[("service", "test"), ("route", "healthz"), ("status", "500")],
            1.0,
        );
        let body = metrics.render_prometheus();
        for family in contract["requestMetricFamilies"]
            .as_array()
            .expect("requestMetricFamilies is an array")
        {
            let family = family.as_str().expect("request metric family is a string");
            assert!(body.contains(&format!("{prefix}_{family}")), "{family}");
        }
        for label in contract["requestMetricLabels"]
            .as_array()
            .expect("requestMetricLabels is an array")
        {
            let label = label.as_str().expect("request metric label is a string");
            assert!(body.contains(&format!(r#"{label}=""#)), "{label}");
        }
    }

    #[test]
    fn labels_map_sorts_labels_by_name() {
        let labels = labels_map(&[("service", "scheduler"), ("outcome", "ok")]);
        assert_eq!(
            labels.keys().cloned().collect::<Vec<_>>(),
            vec!["outcome".to_string(), "service".to_string()]
        );
    }

    #[test]
    fn metric_key_uses_sorted_label_suffix() {
        let labels = labels_map(&[("service", "scheduler"), ("outcome", "ok")]);
        assert_eq!(
            metric_key("events", &labels),
            "events|outcome=ok,service=scheduler"
        );
        assert_eq!(
            metric_key_for_label_pairs("events", &[("service", "scheduler"), ("outcome", "ok")]),
            metric_key("events", &labels)
        );
    }

    #[test]
    fn metric_key_for_label_pairs_matches_label_map_duplicate_key_semantics() {
        let labels = [
            ("route", "first"),
            ("service", "scheduler"),
            ("route", "last"),
        ];
        assert_eq!(
            metric_key_for_label_pairs("events", &labels),
            metric_key("events", &labels_map(&labels))
        );
        assert_eq!(
            metric_key_for_label_pairs("events", &labels),
            "events|route=last,service=scheduler"
        );
    }

    #[test]
    fn metric_store_materializes_label_maps_only_for_new_series() {
        let source = include_str!("metrics.rs");
        for method in ["pub fn increment", "pub fn observe", "pub fn add_gauge"] {
            let start = source.find(method).expect("metric method exists");
            let end = source[start..]
                .find("drop(")
                .expect("metric method drops shard lock")
                + start;
            let body = &source[start..end];
            assert!(
                body.contains("metric_key_for_label_pairs"),
                "{method} should build lookup keys from borrowed label pairs"
            );
            assert!(
                !body.contains("let labels = labels_map(labels);"),
                "{method} must not allocate owned label maps before finding an existing series"
            );
        }
    }

    #[test]
    fn format_labels_renders_prometheus_shape() {
        let labels = labels_map(&[("route", "kv_get"), ("service", "redis-proxy")]);
        assert_eq!(
            format_labels(&labels),
            r#"{route="kv_get",service="redis-proxy"}"#
        );
    }

    #[test]
    fn escape_label_escapes_prometheus_control_chars() {
        let contract = observability_contract();
        for entry in contract["labelEscapes"]
            .as_array()
            .expect("labelEscapes is an array")
        {
            let raw = entry["raw"].as_str().expect("raw is a string");
            let escaped = entry["escaped"].as_str().expect("escaped is a string");
            assert_eq!(escape_label(raw), escaped, "{raw:?}");
        }
    }

    #[test]
    fn metric_store_renders_counter_gauge_summary_shape() {
        let metrics = MetricStore::default();
        metrics.increment(
            "requests",
            &[("service", "test"), ("route", "healthz"), ("status", "200")],
            1.0,
        );
        metrics.add_gauge("in_flight", &[("service", "test")], 2.0);
        metrics.add_gauge("in_flight", &[("service", "test")], -1.0);
        metrics.observe(
            "request_duration_ms",
            &[("service", "test"), ("route", "healthz")],
            3.5,
        );

        let body = metrics.render_prometheus();
        assert!(body.contains("# TYPE wdl_requests_total counter"));
        assert!(
            body.contains(r#"wdl_requests_total{route="healthz",service="test",status="200"} 1"#)
        );
        assert!(body.contains("# TYPE wdl_in_flight gauge"));
        assert!(body.contains(r#"wdl_in_flight{service="test"} 1"#));
        assert!(body.contains("# TYPE wdl_request_duration_ms summary"));
        assert!(
            body.contains(r#"wdl_request_duration_ms_count{route="healthz",service="test"} 1"#)
        );
        assert!(
            body.contains(r#"wdl_request_duration_ms_sum{route="healthz",service="test"} 3.5"#)
        );
        assert!(
            body.contains(r#"wdl_request_duration_ms_max{route="healthz",service="test"} 3.5"#)
        );
    }

    #[cfg(feature = "axum")]
    #[test]
    fn prometheus_response_sets_text_headers() {
        let metrics = MetricStore::default();
        metrics.increment("requests", &[("service", "test")], 1.0);

        let response = prometheus_response(&metrics);

        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            PROMETHEUS_CONTENT_TYPE
        );
        assert_eq!(
            response.headers().get(CONTENT_LENGTH).unwrap(),
            metrics.render_prometheus().len().to_string().as_str()
        );
    }

    #[test]
    fn metric_store_tracks_cardinality_once_per_new_series() {
        let metrics = MetricStore::default();
        metrics.increment("requests", &[("route", "a")], 1.0);
        metrics.increment("requests", &[("route", "a")], 1.0);
        metrics.observe("requests", &[("route", "b")], 2.0);
        metrics.add_gauge("requests", &[("route", "c")], 3.0);

        let state = metrics
            .cardinality
            .lock()
            .expect("metrics cardinality lock poisoned");
        assert_eq!(state.series_by_name.get("requests"), Some(&3));
        assert!(state.warned.is_empty());
    }

    #[test]
    fn metric_store_cardinality_warns_once_per_metric_name() {
        let metrics = MetricStore::default();
        for idx in 0..CARDINALITY_WARN_LIMIT {
            let route = format!("route_{idx}");
            metrics.increment("requests", &[("route", &route)], 1.0);
        }
        for idx in 0..CARDINALITY_WARN_LIMIT {
            let route = format!("another_{idx}");
            metrics.increment("requests", &[("route", &route)], 1.0);
        }

        let state = metrics
            .cardinality
            .lock()
            .expect("metrics cardinality lock poisoned");
        assert_eq!(
            state.series_by_name.get("requests"),
            Some(&(CARDINALITY_WARN_LIMIT * 2))
        );
        assert!(state.warned.contains("requests"));
        assert_eq!(state.warned.len(), 1);
    }

    #[test]
    fn metric_lock_recovers_poisoned_mutex() {
        let mutex = std::sync::Arc::new(Mutex::new(1_u64));
        let poisoned = std::sync::Arc::clone(&mutex);
        let _hook_guard = PANIC_HOOK_LOCK.lock().expect("panic hook test lock");
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::thread::spawn(move || {
            let _guard = poisoned.lock().expect("test mutex locks before panic");
            panic!("poison test mutex");
        })
        .join();
        std::panic::set_hook(previous_hook);
        assert!(result.is_err());

        let mut guard = lock_metric(&mutex);
        *guard += 1;
        assert_eq!(*guard, 2);
    }
}
