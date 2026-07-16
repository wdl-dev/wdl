import { test } from "node:test";
import assert from "node:assert/strict";
import {
  MetricsRegistry,
  generateRequestId,
  ensureRequestId,
  sanitizeRequestId,
  createLogger,
  logStructured,
  createLogLevelBinder,
  setLogLevel,
  formatError,
  recordRedisCommand,
  recordRequestComplete,
} from "../../shared/observability.js";
import { parseStdoutJson } from "../helpers/json-payload.js";
import { readRepositoryJson } from "../helpers/load-shared-module.js";
import { OBSERVABILITY_NOOP_URL } from "../helpers/mocks/observability.js";
import { withCapturedConsole } from "../helpers/output-capture.js";
import { withMockedPropertyDescriptor } from "../helpers/mock-global.js";
import {
  requestIdFromOptions,
  sanitizeRequestId as sanitizeRuntimeRequestId,
} from "../../runtime/_wdl-request-id.js";

const REQUEST_ID_FIXTURES = readRepositoryJson("tests/fixtures/request-id-sanitizer.json");
const OBSERVABILITY_CONTRACT = readRepositoryJson("tests/fixtures/observability-contract.json");
const OBSERVABILITY_NOOP = await import(OBSERVABILITY_NOOP_URL);

test("loaded-runtime request-id sanitizer matches the shared contract", () => {
  for (const fixture of REQUEST_ID_FIXTURES) {
    assert.equal(sanitizeRuntimeRequestId(fixture.raw), fixture.sanitized);
  }
});

test("loaded-runtime request-id options ignore inherited values", async () => {
  await withMockedPropertyDescriptor(/** @type {any} */ (Object.prototype), "requestId", {
    configurable: true,
    value: "inherited-value",
  }, async () => {
    await withMockedPropertyDescriptor(/** @type {any} */ (Object.prototype), "requestIdProvider", {
      configurable: true,
      value: () => "inherited-provider",
    }, async () => {
      assert.equal(requestIdFromOptions({}), null);
      assert.equal(requestIdFromOptions({ requestId: "own-value" }), "own-value");
      assert.equal(requestIdFromOptions({ requestIdProvider: () => "own-provider" }), "own-provider");
    });
  });
});

test("generateRequestId produces 16 lowercase hex chars", () => {
  for (let i = 0; i < 20; i++) {
    const id = generateRequestId();
    assert.match(id, /^[0-9a-f]{16}$/);
  }
});

test("ensureRequestId passes through upstream id unchanged (Headers-like)", () => {
  const headers = { get: (/** @type {string} */ k) => (k === "x-request-id" ? "abc123" : null) };
  assert.equal(ensureRequestId(headers), "abc123");
});

test("ensureRequestId reads from plain object headers (node http)", () => {
  assert.equal(ensureRequestId({ "x-request-id": "xyz" }), "xyz");
});

test("ensureRequestId mints fresh id when missing", () => {
  assert.match(ensureRequestId({}), /^[0-9a-f]{16}$/);
  assert.match(ensureRequestId(null), /^[0-9a-f]{16}$/);
});

test("ensureRequestId takes first piece when header is comma-joined (node dup headers)", () => {
  assert.equal(ensureRequestId({ "x-request-id": "first-id, second-id" }), "first-id");
});

test("ensureRequestId takes first element when header is string[]", () => {
  assert.equal(ensureRequestId({ "x-request-id": ["first-id", "second-id"] }), "first-id");
});

test("ensureRequestId mints fresh id when upstream value is malformed", () => {
  // whitespace embedded
  assert.match(ensureRequestId({ "x-request-id": "has space" }), /^[0-9a-f]{16}$/);
  // CRLF injection attempt
  assert.match(
    ensureRequestId({ "x-request-id": "ok\r\nX-Injected: evil" }),
    /^[0-9a-f]{16}$/
  );
  // quote / backslash (would break JSON log escaping)
  assert.match(ensureRequestId({ "x-request-id": 'a"b' }), /^[0-9a-f]{16}$/);
  assert.match(ensureRequestId({ "x-request-id": "a\\b" }), /^[0-9a-f]{16}$/);
  // unreasonable length
  assert.match(
    ensureRequestId({ "x-request-id": "x".repeat(200) }),
    /^[0-9a-f]{16}$/
  );
  // empty string / empty after split
  assert.match(ensureRequestId({ "x-request-id": "" }), /^[0-9a-f]{16}$/);
  assert.match(ensureRequestId({ "x-request-id": "," }), /^[0-9a-f]{16}$/);
  // non-string (array of non-strings)
  assert.match(ensureRequestId({ "x-request-id": [null] }), /^[0-9a-f]{16}$/);
});

test("ensureRequestId preserves well-formed upstream id verbatim", () => {
  assert.equal(ensureRequestId({ "x-request-id": "trace-abc-123" }), "trace-abc-123");
});

test("sanitizeRequestId follows the cross-language fixture contract", () => {
  for (const { raw, sanitized } of REQUEST_ID_FIXTURES) {
    assert.equal(sanitizeRequestId(raw), sanitized, `raw=${JSON.stringify(raw)}`);
  }
});

test("observability noop uses production request-id and error normalization", () => {
  assert.equal(OBSERVABILITY_NOOP.sanitizeRequestId, sanitizeRequestId);
  assert.equal(OBSERVABILITY_NOOP.formatError, formatError);
  assert.equal(OBSERVABILITY_NOOP.ensureRequestId({ "x-request-id": "bad id" }), "rid");
});

test("formatError normalizes null, Error objects, and primitive throwables", () => {
  assert.deepEqual(formatError(null), { error_message: "Unknown error" });
  assert.deepEqual(formatError(new TypeError("boom")), {
    error_name: "TypeError",
    error_message: "boom",
  });
  assert.deepEqual(formatError(42), { error_message: "42" });
  assert.deepEqual(formatError(Object.create(null)), { error_message: "Unknown error" });
});

test("formatError promotes stable code/reason fields to error_code", () => {
  const coded = new Error("denied");
  /** @type {any} */ (coded).code = "acl_denied";
  assert.deepEqual(formatError(coded), {
    error_name: "Error",
    error_message: "denied",
    error_code: "acl_denied",
  });

  const reasoned = new Error("auth unavailable");
  /** @type {any} */ (reasoned).reason = "auth_unavailable";
  assert.equal(formatError(reasoned).error_code, "auth_unavailable");
});

test("formatError contains throwing Error field getters", () => {
  const broken = new Error("original");
  for (const field of ["name", "message", "code"]) {
    Object.defineProperty(broken, field, {
      configurable: true,
      get() { throw new Error(`formatter read ${field}`); },
    });
  }
  Object.defineProperty(broken, "reason", { value: "tenant_failure" });

  assert.deepEqual(formatError(broken), {
    error_name: "Error",
    error_message: "Unknown error",
    error_code: "tenant_failure",
  });
});

test("formatError contains a throwing Error classification trap", () => {
  const throwable = new Proxy(Object.create(null), {
    getPrototypeOf() { throw new Error("prototype trap"); },
  });

  assert.deepEqual(formatError(throwable), {
    error_message: "Unknown error",
  });
});

test("MetricsRegistry: renderPrometheus emits counter TYPE + totals", () => {
  const reg = new MetricsRegistry();
  reg.increment("requests", { service: "test", status: "200" });
  reg.increment("requests", { service: "test", status: "200" });
  const out = reg.renderPrometheus();
  assert.match(out, /^# TYPE wdl_requests_total counter$/m);
  assert.match(out, /wdl_requests_total\{service="test",status="200"\} 2/);
});

test("MetricsRegistry: label ordering preserves order-independent series identity", () => {
  const reg = new MetricsRegistry();
  reg.increment("requests", { status: "200", service: "test" });
  reg.increment("requests", { service: "test", status: "200" });

  const out = reg.renderPrometheus();
  assert.match(out, /wdl_requests_total\{service="test",status="200"\} 2/);
});

test("MetricsRegistry: order-independent label ordering does not inflate cardinality warnings", () => {
  const reg = new MetricsRegistry();
  withCapturedConsole(({ stdout }) => {
    for (let i = 0; i < 60; i += 1) {
      reg.increment("requests", { status: String(i), service: "test" });
      reg.increment("requests", { service: "test", status: String(i) });
    }
    assert.equal(stdout.length, 0, `unexpected warn: ${stdout.join(" | ")}`);
  });
});

// Renderer always appends `_total` to the source name. Locks that contract so
// a future change to the suffix logic can't silently reshape every counter
// name on the wire — and as a reminder that callers must supply the bare
// stem (`cron_fires`, not `cron_fires_total`).
test("MetricsRegistry: renderPrometheus appends exactly one _total to the source name", () => {
  const reg = new MetricsRegistry();
  reg.increment("hits", { service: "test" });
  const out = reg.renderPrometheus();
  assert.match(out, /^wdl_hits_total\{service="test"\} 1$/m);
  assert.ok(
    !/wdl_hits_total_total/.test(out),
    `renderer produced a double _total suffix: ${out}`
  );
});

test("MetricsRegistry: summary and _max live in separate families", () => {
  const reg = new MetricsRegistry();
  reg.observe("request_duration_ms", { service: "test" }, 5);
  reg.observe("request_duration_ms", { service: "test" }, 15);
  const out = reg.renderPrometheus();
  assert.match(out, /^# TYPE wdl_request_duration_ms summary$/m);
  assert.match(out, /^# TYPE wdl_request_duration_ms_max gauge$/m);
  assert.match(out, /wdl_request_duration_ms_count\{service="test"\} 2/);
  assert.match(out, /wdl_request_duration_ms_sum\{service="test"\} 20/);
  assert.match(out, /wdl_request_duration_ms_max\{service="test"\} 15/);
  // _max must NOT appear inside the summary family block.
  const summaryIdx = out.indexOf("# TYPE wdl_request_duration_ms summary");
  const maxTypeIdx = out.indexOf("# TYPE wdl_request_duration_ms_max gauge");
  const summaryBlock = out.slice(summaryIdx, maxTypeIdx);
  assert.ok(
    !/wdl_request_duration_ms_max/.test(summaryBlock),
    "_max sample leaked into summary family"
  );
});

test("MetricsRegistry: gauges render as their own family and escape label values", () => {
  const reg = new MetricsRegistry("custom");
  reg.setGauge("inflight", { path: 'a"b\\c\n\r', service: "gateway" }, 3);
  const out = reg.renderPrometheus();
  assert.match(out, /^# TYPE custom_inflight gauge$/m);
  assert.ok(
    out.includes('custom_inflight{path="a\\"b\\\\c\\n\\r",service="gateway"} 3'),
    out
  );
});

test("MetricsRegistry: observability literals follow the cross-language contract", () => {
  const reg = new MetricsRegistry();
  reg.increment("requests", { service: "test", route: "healthz", status: "200" });
  reg.observe("request_duration_ms", { service: "test", route: "healthz" }, 1);
  reg.increment("request_errors", { service: "test", route: "healthz", status: "500" });
  const out = reg.renderPrometheus();

  assert.match(out, new RegExp(`^# TYPE ${OBSERVABILITY_CONTRACT.metricPrefix}_requests_total counter$`, "m"));
  for (const family of OBSERVABILITY_CONTRACT.requestMetricFamilies) {
    assert.match(out, new RegExp(`\\b${OBSERVABILITY_CONTRACT.metricPrefix}_${family}`), family);
  }
  for (const label of OBSERVABILITY_CONTRACT.requestMetricLabels) {
    assert.match(out, new RegExp(`\\b${label}="`), label);
  }

  for (const { raw, escaped } of OBSERVABILITY_CONTRACT.labelEscapes) {
    const escapedReg = new MetricsRegistry("escape");
    escapedReg.setGauge("probe", { value: raw }, 1);
    assert.match(escapedReg.renderPrometheus(), new RegExp(`value="${RegExp.escape(escaped)}"`), raw);
  }
});

test("MetricsRegistry: cardinality warning fires per-metric-name as structured JSON", () => {
  const reg = new MetricsRegistry();
  withCapturedConsole(({ stdout, stderr }) => {
    // Metric A: 99 distinct series — below threshold, no warning.
    for (let i = 0; i < 99; i++) {
      reg.increment("wide_counter_a", { bucket: String(i) });
    }
    // Metric B: 5 distinct series, totally unrelated. Combined map size is
    // 104 > 100; an implementation that checks global size would incorrectly
    // warn about B here.
    for (let i = 0; i < 5; i++) {
      reg.increment("small_counter_b", { bucket: String(i) });
    }
    assert.equal(stdout.length, 0, `unexpected warn: ${stdout.join(" | ")}`);

    // Push metric A past 100; this is when the warning should fire, and
    // only for name "wide_counter_a".
    reg.increment("wide_counter_a", { bucket: "100" });
    assert.equal(stderr.length, 0);
    assert.equal(stdout.length, 1);
    const warning = parseStdoutJson(stdout[0], "metric cardinality warning");
    assert.equal(warning.service, "observability");
    assert.equal(warning.level, "warn");
    assert.equal(warning.event, "metric_cardinality_warning");
    assert.equal(warning.metric, "wide_counter_a");
    assert.equal(warning.series, 100);
    assert.equal(warning.limit, OBSERVABILITY_CONTRACT.cardinalityWarnLimit);
    assert.ok(!stdout[0].includes("small_counter_b"));

    // Subsequent series for A still don't re-warn (once per metric).
    reg.increment("wide_counter_a", { bucket: "101" });
    assert.equal(stdout.length, 1);
    const out = reg.renderPrometheus();
    assert.match(out, /wdl_wide_counter_a_total\{bucket="100"\} 1/);
    assert.doesNotMatch(out, /bucket="101"/);

    // Existing series keep updating after the cap; only brand-new label
    // combinations are dropped.
    reg.increment("wide_counter_a", { bucket: "0" });
    assert.match(reg.renderPrometheus(), /wdl_wide_counter_a_total\{bucket="0"\} 2/);
  });
});

test("structured log envelope follows the cross-language contract", () => {
  const { orderedKeys, timestampShape } = OBSERVABILITY_CONTRACT.logEnvelope;
  /** @type {Array<{ name: string, priority: number, stream: string }>} */
  const levels = OBSERVABILITY_CONTRACT.logEnvelope.levels;
  setLogLevel("debug");
  try {
    const log = createLogger("test-svc");
    withCapturedConsole(({ stdout, stderr }) => {
      for (const { name } of levels) log(name, `event_${name}`, { probe: true });

      const emitted = [
        ...stdout.map((line) => ({ line, stream: "stdout" })),
        ...stderr.map((line) => ({ line, stream: "stderr" })),
      ].map((record) => ({
        ...record,
        payload: parseStdoutJson(record.line, `${record.stream} log payload`),
      }));
      assert.equal(emitted.length, levels.length);
      for (const expected of levels) {
        const record = emitted.find(({ payload }) => payload.level === expected.name);
        assert.ok(record, `missing ${expected.name} log line`);
        assert.equal(record.stream, expected.stream, `${expected.name} stream`);
        const { payload } = record;
        assert.deepEqual(Object.keys(payload).slice(0, orderedKeys.length), orderedKeys);
        assert.equal(payload.ts.replace(/\d/g, "0"), timestampShape);
      }
    });

    for (const minimum of levels) {
      setLogLevel(minimum.name);
      withCapturedConsole(({ stdout, stderr }) => {
        for (const { name } of levels) log(name, `gated_${name}`);
        const actual = [...stdout, ...stderr]
          .map((line) => parseStdoutJson(line, `minimum=${minimum.name} log payload`).level)
          .toSorted();
        const expected = levels
          .filter(({ priority }) => priority >= minimum.priority)
          .map(({ name }) => name)
          .toSorted();
        assert.deepEqual(actual, expected, `minimum=${minimum.name}`);
      });
    }
  } finally {
    setLogLevel("info");
  }
});

test("setLogLevel=warn suppresses info but passes error through", () => {
  setLogLevel("warn");
  try {
    const log = createLogger("test-svc");
    withCapturedConsole(({ stdout, stderr }) => {
      log("info", "request_complete", { route: "fetch" });
      log("debug", "noisy", {});
      assert.equal(stdout.length, 0, "info / debug must be gated out");
      log("warn", "something", {});
      log("error", "bad", {});
      assert.equal(stdout.length, 1, "warn routes to console.log");
      assert.equal(stderr.length, 1, "error routes to console.error");
      // Payload shape: JSON with service + level + event.
      const warnPayload = parseStdoutJson(stdout[0], "warn log payload");
      assert.equal(warnPayload.service, "test-svc");
      assert.equal(warnPayload.level, "warn");
      assert.equal(warnPayload.event, "something");
    });
  } finally {
    setLogLevel("info");
  }
});

test("logStructured emits createLogger-shaped JSON", () => {
  setLogLevel("info");
  try {
    withCapturedConsole(({ stdout, stderr }) => {
      logStructured("test-svc", "warn", "config_invalid", { variable: "X" });
      logStructured("test-svc", "error", "config_failed", { variable: "Y" });
      assert.equal(stdout.length, 1);
      assert.equal(stderr.length, 1);
      const warning = parseStdoutJson(stdout[0], "structured warn log");
      const error = parseStdoutJson(stderr[0], "structured error log");
      assert.equal(warning.service, "test-svc");
      assert.equal(warning.level, "warn");
      assert.equal(warning.event, "config_invalid");
      assert.equal(warning.variable, "X");
      assert.equal(error.level, "error");
      assert.equal(error.event, "config_failed");
      assert.equal(error.variable, "Y");
    });
  } finally {
    setLogLevel("info");
  }
});

test("logStructured does not throw on BigInt or circular fields", () => {
  /** @type {Record<string, unknown>} */
  const fields = { count: 3n };
  fields.self = fields;

  withCapturedConsole(({ stdout }) => {
    logStructured("test-svc", "warn", "odd_payload", fields);

    assert.equal(stdout.length, 1);
    const payload = parseStdoutJson(stdout[0], "safe structured log");
    assert.equal(payload.count, "3");
    assert.deepEqual(payload.self, { count: "3", self: "[Circular]" });
  });
});

test("setLogLevel does not affect MetricsRegistry counters (metrics bypass the gate)", () => {
  setLogLevel("error");
  try {
    const reg = new MetricsRegistry();
    withCapturedConsole(({ stdout, stderr }) => {
      const log = createLogger("test-svc");
      log("info", "should_be_suppressed", {});
      log("warn", "also_suppressed", {});
      reg.increment("requests", { service: "test-svc", status: "200" });
      reg.observe("request_duration_ms", { service: "test-svc" }, 42);
      assert.equal(stdout.length, 0, "info+warn must be gated under error level");
      assert.equal(stderr.length, 0, "no errors happened");
    });
    // Metrics still recorded despite log silence.
    const out = reg.renderPrometheus();
    assert.match(out, /wdl_requests_total\{service="test-svc",status="200"\} 1/);
    assert.match(out, /wdl_request_duration_ms_count\{service="test-svc"\} 1/);
  } finally {
    setLogLevel("info");
  }
});

test("createLogLevelBinder applies LOG_LEVEL only once", () => {
  setLogLevel("info");
  try {
    const bind = createLogLevelBinder();
    bind({ LOG_LEVEL: "warn" });
    bind({ LOG_LEVEL: "debug" });
    const log = createLogger("test-svc");
    withCapturedConsole(({ stdout }) => {
      log("info", "gated", {});
      log("warn", "visible", {});
      assert.equal(stdout.length, 1, "second bind must not lower the threshold");
      assert.equal(parseStdoutJson(stdout[0], "visible warn log").event, "visible");
    });
  } finally {
    setLogLevel("info");
  }
});

test("createLogLevelBinder is safe when LOG_LEVEL is missing", () => {
  setLogLevel("info");
  try {
    const bind = createLogLevelBinder();
    bind({});
    bind({ LOG_LEVEL: "error" });
    const log = createLogger("test-svc");
    withCapturedConsole(({ stdout }) => {
      log("info", "still_visible", {});
      assert.equal(stdout.length, 1, "missing first LOG_LEVEL must not throw or bind a later level");
    });
  } finally {
    setLogLevel("info");
  }
});

test("recordRedisCommand emits bounded metrics and logs failures", () => {
  /** @type {{ increments: any[], observations: any[], logs: any[] }} */
  const calls = { increments: [], observations: [], logs: [] };
  const metrics = {
    /** @param {string} name @param {any} labels */
    increment(name, labels) { calls.increments.push({ name, labels }); },
    /** @param {string} name @param {any} labels @param {number} value */
    observe(name, labels, value) { calls.observations.push({ name, labels, value }); },
  };
  const log = (/** @type {string} */ level, /** @type {string} */ event, /** @type {any} */ fields) =>
    calls.logs.push({ level, event, fields });

  recordRedisCommand({
    metrics,
    log,
    service: "gateway",
    event: { command: "HGETALL", ok: true, duration_ms: 3 },
  });
  recordRedisCommand({
    metrics,
    log,
    service: "gateway",
    event: { command: "GET", ok: false, duration_ms: 5, error_message: "redis down" },
  });

  assert.deepEqual(calls.increments, [
    {
      name: "redis_commands",
      labels: { service: "gateway", command: "HGETALL", outcome: "ok" },
    },
    {
      name: "redis_commands",
      labels: { service: "gateway", command: "GET", outcome: "error" },
    },
  ]);
  assert.deepEqual(calls.observations, [
    {
      name: "redis_command_duration_ms",
      labels: { service: "gateway", command: "HGETALL" },
      value: 3,
    },
    {
      name: "redis_command_duration_ms",
      labels: { service: "gateway", command: "GET" },
      value: 5,
    },
  ]);
  assert.deepEqual(calls.logs, [
    {
      level: "error",
      event: "redis_command_failed",
      fields: { command: "GET", duration_ms: 5, error_message: "redis down" },
    },
  ]);
});

test("recordRedisCommand records metrics even when no logger is available", () => {
  /** @type {{ increments: any[], observations: any[] }} */
  const calls = { increments: [], observations: [] };
  const metrics = {
    /** @param {string} name @param {any} labels */
    increment(name, labels) { calls.increments.push({ name, labels }); },
    /** @param {string} name @param {any} labels @param {number} value */
    observe(name, labels, value) { calls.observations.push({ name, labels, value }); },
  };

  recordRedisCommand({
    metrics,
    log: null,
    service: "control",
    event: { command: "GET", ok: false, duration_ms: 9, error_message: "redis down" },
  });

  assert.deepEqual(calls.increments, [
    {
      name: "redis_commands",
      labels: { service: "control", command: "GET", outcome: "error" },
    },
  ]);
  assert.deepEqual(calls.observations, [
    {
      name: "redis_command_duration_ms",
      labels: { service: "control", command: "GET" },
      value: 9,
    },
  ]);
});

test("recordRedisCommand bounds unknown command labels", () => {
  /** @type {{ increments: any[], observations: any[], logs: any[] }} */
  const calls = { increments: [], observations: [], logs: [] };
  const metrics = {
    /** @param {string} name @param {any} labels */
    increment(name, labels) { calls.increments.push({ name, labels }); },
    /** @param {string} name @param {any} labels @param {number} value */
    observe(name, labels, value) { calls.observations.push({ name, labels, value }); },
  };
  const log = (/** @type {string} */ level, /** @type {string} */ event, /** @type {any} */ fields) =>
    calls.logs.push({ level, event, fields });

  recordRedisCommand({
    metrics,
    log,
    service: "gateway",
    event: { command: "debug-object", ok: false, duration_ms: 4, error_message: "disabled" },
  });

  assert.deepEqual(calls.increments, [
    {
      name: "redis_commands",
      labels: { service: "gateway", command: "OTHER", outcome: "error" },
    },
  ]);
  assert.deepEqual(calls.observations, [
    {
      name: "redis_command_duration_ms",
      labels: { service: "gateway", command: "OTHER" },
      value: 4,
    },
  ]);
  assert.deepEqual(calls.logs, [
    {
      level: "error",
      event: "redis_command_failed",
      fields: { command: "debug-object", duration_ms: 4, error_message: "disabled" },
    },
  ]);
});

test("recordRedisCommand logs WATCH invalidation as optimistic conflict noise", () => {
  /** @type {{ increments: any[], observations: any[], logs: any[] }} */
  const calls = { increments: [], observations: [], logs: [] };
  const metrics = {
    /** @param {string} name @param {any} labels */
    increment(name, labels) { calls.increments.push({ name, labels }); },
    /** @param {string} name @param {any} labels @param {number} value */
    observe(name, labels, value) { calls.observations.push({ name, labels, value }); },
  };
  const log = (/** @type {string} */ level, /** @type {string} */ event, /** @type {any} */ fields) =>
    calls.logs.push({ level, event, fields });

  recordRedisCommand({
    metrics,
    log,
    service: "control",
    event: { command: "MULTI_EXEC", ok: false, duration_ms: 6, error_message: "watch invalidation" },
  });

  assert.deepEqual(calls.increments, [
    {
      name: "redis_commands",
      labels: { service: "control", command: "MULTI_EXEC", outcome: "error" },
    },
  ]);
  assert.deepEqual(calls.observations, [
    {
      name: "redis_command_duration_ms",
      labels: { service: "control", command: "MULTI_EXEC" },
      value: 6,
    },
  ]);
  assert.deepEqual(calls.logs, [
    {
      level: "warn",
      event: "redis_watch_invalidation",
      fields: { command: "MULTI_EXEC", duration_ms: 6 },
    },
  ]);
});

test("recordRedisCommand logs failures even when metrics are not scrapeable", () => {
  /** @type {any[]} */
  const calls = [];
  const log = (/** @type {string} */ level, /** @type {string} */ event, /** @type {any} */ fields) =>
    calls.push({ level, event, fields });

  recordRedisCommand({
    metrics: null,
    log,
    service: "auth",
    event: { command: "HGET", ok: false, duration_ms: 7, error_message: "redis down" },
  });

  assert.deepEqual(calls, [
    {
      level: "error",
      event: "redis_command_failed",
      fields: { command: "HGET", duration_ms: 7, error_message: "redis down" },
    },
  ]);
});

function captureLog() {
  /** @type {Array<{ level: string, event: string, fields: any }>} */
  const out = [];
  return {
    log: (/** @type {string} */ level, /** @type {string} */ event, /** @type {any} */ fields) =>
      out.push({ level, event, fields }),
    entries: out,
  };
}

test("recordRequestComplete: emits requests counter, duration summary, and request_complete log on 2xx", () => {
  const reg = new MetricsRegistry();
  const { log, entries } = captureLog();
  recordRequestComplete({
    service: "gateway",
    metrics: reg,
    log,
    method: "GET",
    requestId: "req-1",
    route: "worker_fetch_subdomain",
    status: 200,
    startedAt: Date.now() - 50,
    extras: { namespace: "demo", worker: "hello", version: "v3" },
  });
  const out = reg.renderPrometheus();
  assert.match(
    out,
    /wdl_requests_total\{route="worker_fetch_subdomain",service="gateway",status="200"\} 1/
  );
  assert.match(
    out,
    /wdl_request_duration_ms_count\{route="worker_fetch_subdomain",service="gateway"\} 1/
  );
  assert.ok(!/wdl_request_errors_total/.test(out), "2xx must not touch request_errors");
  assert.equal(entries.length, 1);
  assert.equal(entries[0].level, "info");
  assert.equal(entries[0].event, "request_complete");
  assert.equal(entries[0].fields.request_id, "req-1");
  assert.equal(entries[0].fields.namespace, "demo");
  assert.equal(entries[0].fields.worker, "hello");
  assert.equal(entries[0].fields.version, "v3");
  assert.equal(typeof entries[0].fields.duration_ms, "number");
});

test("recordRequestComplete: metrics omit method label but logs keep raw method", () => {
  const reg = new MetricsRegistry();
  const { log, entries } = captureLog();
  recordRequestComplete({
    service: "gateway",
    metrics: reg,
    log,
    method: "PURGE-tenant-controlled-1",
    requestId: "req-method",
    route: "worker_fetch_pattern",
    status: 404,
    startedAt: Date.now(),
  });
  const out = reg.renderPrometheus();
  assert.match(
    out,
    /wdl_requests_total\{route="worker_fetch_pattern",service="gateway",status="404"\} 1/
  );
  assert.doesNotMatch(out, /PURGE-tenant-controlled-1/);
  assert.doesNotMatch(out, /method=/);
  assert.equal(entries.length, 1);
  assert.equal(entries[0].fields.method, "PURGE-tenant-controlled-1");
});

test("recordRequestComplete: logs without metrics for non-scrapeable tiers", () => {
  const { log, entries } = captureLog();
  recordRequestComplete({
    service: "control",
    metrics: null,
    log,
    method: "POST",
    requestId: "req-log-only",
    route: "worker_api",
    status: 201,
    startedAt: Date.now(),
  });
  assert.equal(entries.length, 1);
  assert.equal(entries[0].event, "request_complete");
  assert.equal(entries[0].fields.route, "worker_api");
  assert.equal(entries[0].fields.status, 201);
});

test("recordRequestComplete: 5xx increments request_errors and logs at error level even without a thrown error", () => {
  const reg = new MetricsRegistry();
  const { log, entries } = captureLog();
  recordRequestComplete({
    service: "runtime",
    metrics: reg,
    log,
    method: "POST",
    requestId: "req-2",
    route: "worker_fetch",
    status: 502,
    startedAt: Date.now(),
  });
  const out = reg.renderPrometheus();
  assert.match(
    out,
    /wdl_request_errors_total\{route="worker_fetch",service="runtime",status="502"\} 1/
  );
  assert.equal(entries.length, 1);
  assert.equal(entries[0].level, "error");
});

test("recordRequestComplete: suppresses log on successful probe routes but still emits metrics", () => {
  const reg = new MetricsRegistry();
  const { log, entries } = captureLog();
  recordRequestComplete({
    service: "admin", metrics: reg, log,
    method: "GET", requestId: "r", route: "healthz", status: 200,
    startedAt: Date.now(),
  });
  recordRequestComplete({
    service: "admin", metrics: reg, log,
    method: "GET", requestId: "r", route: "metrics", status: 200,
    startedAt: Date.now(),
  });
  assert.equal(entries.length, 0, "successful probes must not log request_complete");
  const out = reg.renderPrometheus();
  assert.match(out, /wdl_requests_total\{route="healthz",service="admin",status="200"\} 1/);
  assert.match(out, /wdl_requests_total\{route="metrics",service="admin",status="200"\} 1/);
});

test("recordRequestComplete: probe routes still log when they 5xx or throw", () => {
  const reg = new MetricsRegistry();
  const { log, entries } = captureLog();
  recordRequestComplete({
    service: "admin", metrics: reg, log,
    method: "GET", requestId: "r", route: "healthz", status: 500,
    startedAt: Date.now(),
  });
  recordRequestComplete({
    service: "admin", metrics: reg, log,
    method: "GET", requestId: "r", route: "metrics", status: 200,
    startedAt: Date.now(),
    error: new Error("scrape blew up"),
  });
  assert.equal(entries.length, 2);
  assert.equal(entries[0].level, "error", "5xx probe still logs at error level");
  assert.equal(entries[1].fields.error_message, "scrape blew up");
});

test("recordRequestComplete: null/undefined extras keys are stripped", () => {
  const reg = new MetricsRegistry();
  const { log, entries } = captureLog();
  recordRequestComplete({
    service: "gateway", metrics: reg, log,
    method: "GET", requestId: "r", route: "worker_fetch", status: 502,
    startedAt: Date.now(),
    extras: { namespace: null, worker: undefined, version: "v3" },
  });
  assert.equal(entries.length, 1);
  assert.ok(!("namespace" in entries[0].fields), "null extras key must be omitted");
  assert.ok(!("worker" in entries[0].fields), "undefined extras key must be omitted");
  assert.equal(entries[0].fields.version, "v3");
});

test("recordRequestComplete: error argument flows into log via formatError", () => {
  const reg = new MetricsRegistry();
  const { log, entries } = captureLog();
  recordRequestComplete({
    service: "gateway", metrics: reg, log,
    method: "GET", requestId: "r", route: "worker_fetch", status: 502,
    startedAt: Date.now(),
    error: new TypeError("bad"),
  });
  assert.equal(entries[0].fields.error_name, "TypeError");
  assert.equal(entries[0].fields.error_message, "bad");
});

test("recordRequestComplete: falsey thrown values remain errors", () => {
  for (const [error, message] of [[0, "0"], [false, "false"], [null, "Unknown error"], [undefined, "Unknown error"]]) {
    const { log, entries } = captureLog();
    recordRequestComplete({
      service: "runtime", metrics: null, log,
      method: "POST", requestId: "r", route: "worker_scheduled", status: 200,
      startedAt: Date.now(), error, hasError: true,
    });
    assert.equal(entries.length, 1);
    assert.equal(entries[0].level, "error");
    assert.equal(entries[0].fields.error_message, message);
  }
});

test("setLogLevel ignores unknown names (keeps previous level)", () => {
  setLogLevel("warn");
  try {
    setLogLevel("bogus");
    const log = createLogger("test-svc");
    withCapturedConsole(({ stdout }) => {
      log("info", "gated", {});
      assert.equal(stdout.length, 0, "level should still be warn, not reset");
    });
  } finally {
    setLogLevel("info");
  }
});
