import { execFileSync } from "node:child_process";
import { before, beforeEach } from "node:test";
import { responseJson } from "./http-response.js";
import { assertStatus } from "./assertions.js";

import {
  COMPILE_WORKERD_LOCAL_ARGS,
  DOCKER_COMPOSE_BUILD_ARGS,
  shouldPrepareIntegrationArtifacts,
} from "../../../scripts/integration-environment.js";

import { ROOT, GATEWAY_HOST, GATEWAY_PORT } from "./env.js";
import { sh } from "./cli.js";
import { composeUp } from "./compose.js";
import { ensureD1SingleRuntime, ensureDoSingleRuntime } from "./runtimes.js";
import { internalHttpRequest } from "./internal-http.js";
import { adminFetch } from "./admin-http.js";
import { gatewayFetch } from "./gateway-http.js";
import { delay, waitUntil } from "../../helpers/timing.js";
import { QUEUE_CONSUMER_INDEX_KEY, QUEUE_STREAM_INDEX_KEY, queueConsumerKey, queueStreamKey } from "./queue.js";
import { redisDel, redisFlushAll, redisHSet, redisSAdd, redisSet, redisSRem, redisXInfoGroups } from "./redis.js";

export { delay, waitUntil };

// Tests that seed cron-slot:<current minute> directly must leave enough time
// for scheduler restart + startup tick before the slot becomes "missed".
// The small post-rollover buffer only ensures Date.now() samples the new slot.
export async function waitForCurrentSlotFixtureWindow(minRemainingMs = 20_000) {
  const remaining = 60_000 - (Date.now() % 60_000);
  if (remaining < minRemainingMs) {
    await delay(remaining + 250);
  }
}

let stackReady = false;
let directArtifactsPrepared = false;

function prepareDirectIntegrationArtifacts() {
  if (
    directArtifactsPrepared ||
    process.env.WDL_INTEGRATION_NO_BUILD === "1" ||
    !shouldPrepareIntegrationArtifacts()
  ) {
    return;
  }
  execFileSync(process.execPath, COMPILE_WORKERD_LOCAL_ARGS, {
    cwd: ROOT,
    stdio: "inherit",
  });
  execFileSync("docker", DOCKER_COMPOSE_BUILD_ARGS, {
    cwd: ROOT,
    stdio: "inherit",
  });
  directArtifactsPrepared = true;
}

// Restart runtime/gateway/system-runtime per run so workerLoader's failed-load
// cache sees the freshly compiled workerd configs and prebuilt images.
// SLOT_PREPPED=1 is the caller's promise this slot has already done that.
export async function ensureStackUp() {
  if (stackReady) return;
  prepareDirectIntegrationArtifacts();
  composeUp("--wait test-probe", { stdio: "pipe" });
  if (process.env.WDL_INTEGRATION_SLOT_PREPPED === "1") {
    ensureD1SingleRuntime();
    ensureDoSingleRuntime();
    await waitForGateway();
    await waitForGatewaySubscriber();
    stackReady = true;
    return;
  }
  const state = sh("docker compose ps --format '{{.Service}} {{.State}}'").trim();
  const required = [
    "test-probe", "redis", "s3mock", "redis-proxy-user", "redis-proxy-system", "redis-proxy-do",
    "d1-runtime", "do-runtime", "user-runtime", "system-runtime", "gateway", "scheduler",
    "workflows",
  ];
  const missing = required.filter((s) => !new RegExp(`^${RegExp.escape(s)} running`, "m").test(state));
  if (missing.length) {
    composeUp("", { stdio: "inherit" });
  } else {
    // Sequential, dep-order restart: parallel restart lets gateway's
    // workerd getaddrinfo("user-runtime") before Docker DNS updates,
    // which then caches the failure for the process lifetime.
    sh("docker compose restart redis-proxy-user", { stdio: "pipe" });
    composeUp("--wait redis-proxy-user", { stdio: "pipe" });
    sh("docker compose restart redis-proxy-system", { stdio: "pipe" });
    composeUp("--wait redis-proxy-system", { stdio: "pipe" });
    sh("docker compose restart d1-runtime", { stdio: "pipe" });
    composeUp("--wait d1-runtime", { stdio: "pipe" });
    sh("docker compose restart redis-proxy-do", { stdio: "pipe" });
    composeUp("--wait redis-proxy-do", { stdio: "pipe" });
    sh("docker compose restart do-runtime", { stdio: "pipe" });
    composeUp("--wait do-runtime", { stdio: "pipe" });
    sh("docker compose restart user-runtime", { stdio: "pipe" });
    composeUp("--wait user-runtime", { stdio: "pipe" });
    sh("docker compose restart system-runtime", { stdio: "pipe" });
    composeUp("--wait system-runtime", { stdio: "pipe" });
    sh("docker compose restart gateway", { stdio: "pipe" });
    composeUp("--wait gateway", { stdio: "pipe" });
    composeUp("--force-recreate --wait scheduler", { stdio: "pipe" });
  }
  ensureD1SingleRuntime();
  ensureDoSingleRuntime();
  await waitForGateway();
  // Subscriber must be attached before any PUBLISH — early ones are
  // lost (Pub/Sub is non-persistent).
  await waitForGatewaySubscriber();
  stackReady = true;
}

/**
 * @param {{
 *   reset?: boolean,
 *   afterStackUp?: () => void | Promise<void>,
 *   beforeEachReset?: () => void | Promise<void>,
 * }} [options]
 */
export function setupIntegrationSuite(options = {}) {
  const reset = options.reset ?? true;
  before(async () => {
    await ensureStackUp();
    await options.afterStackUp?.();
  });

  if (reset) {
    beforeEach(async () => {
      await resetStack();
      await options.beforeEachReset?.();
    });
  } else if (options.beforeEachReset) {
    beforeEach(options.beforeEachReset);
  }
}

export async function waitForGateway() {
  await waitUntil("gateway :8080", async () => {
    const res = await fetch(`http://${GATEWAY_HOST}:${GATEWAY_PORT}/`, {
      headers: { Host: "probe.workers.local" },
    }).catch(() => null);
    return res !== null;
  });
  // /healthz doesn't exercise external bindings; this does.
  await waitForGatewayToRuntime();
}

export async function waitForGatewaySubscriber() {
  await waitUntil("gateway subscriber connected", async () => {
    const body = await readGatewayHealth().catch(() => null);
    return body && body.subscriber_connected === true;
  });
}

export async function readGatewayHealth() {
  const res = await fetch(`http://${GATEWAY_HOST}:${GATEWAY_PORT}/healthz`);
  if (!res.ok) {
    throw new Error(`gateway healthz failed: ${res.status} ${await res.text()}`);
  }
  return await responseJson(res);
}

/**
 * @param {string} label
 * @param {(health: any) => boolean} predicate
 * @param {{ timeoutMs?: number, intervalMs?: number }} [opts]
 */
export async function waitForGatewayCacheState(label, predicate, opts = {}) {
  await waitUntil(label, async () => {
    const health = await readGatewayHealth().catch(() => null);
    return Boolean(health && predicate(health));
  }, {
    timeoutMs: opts.timeoutMs ?? 4000,
    intervalMs: opts.intervalMs ?? 25,
  });
}

export async function waitForScheduler() {
  const ns = `it-scheduler-ready-${process.pid}-${Date.now()}`;
  const queue = "ready";
  const consumerKey = queueConsumerKey(ns, queue);
  const streamKey = queueStreamKey(ns, queue);
  redisHSet(consumerKey, {
    worker: "probe",
    version: "v1",
    max_batch_size: "100",
    max_batch_timeout_ms: "1000",
    max_retries: "0",
    retry_delay_secs: "0",
  });
  redisSAdd(QUEUE_CONSUMER_INDEX_KEY, consumerKey);

  try {
    await waitUntil("scheduler :9110", async () => {
      try {
        const res = internalHttpRequest("scheduler", 9110, "/_healthz", "GET");
        if (res.status !== 200) return false;
        const body = responseJson(res);
        return body?.ok === true;
      } catch {
        return false;
      }
    }, { timeoutMs: 10_000, intervalMs: 100 });
    await waitUntil("scheduler queue reconcile ready", async () => {
      const groups = redisXInfoGroups(streamKey, { db: 1 });
      return groups.includes("wdl-scheduler");
    }, { timeoutMs: 15_000, intervalMs: 250 });
  } finally {
    redisSRem(QUEUE_CONSUMER_INDEX_KEY, consumerKey);
    redisDel(consumerKey);
    redisSRem(QUEUE_STREAM_INDEX_KEY, streamKey, { db: 1 });
    redisDel(streamKey, { db: 1 });
  }
}

// /healthz alone doesn't catch the workerd-DNS-stuck-on-<external>
// state; a real gateway → user-runtime round trip does. Covers the
// shared external-binding resolution layer — per-external config
// correctness (system-runtime aliases etc.) is *fail-fast*, by the
// first test that hits it, not readiness's job.
export async function waitForGatewayToRuntime() {
  const ns = "stack-probe";
  const name = "ready";
  const code = `export default { fetch() { return new Response("ready"); } };`;
  await waitUntil("gateway → control upstream", async () => {
    const dep = await adminFetch(`/ns/${ns}/worker/${name}/deploy`, {
      method: "POST",
      body: JSON.stringify({ code }),
    });
    if (!dep.ok) throw new Error(`probe deploy failed: ${dep.status} ${await dep.text()}`);
    const { version } = await responseJson(dep);
    const prom = await adminFetch(`/ns/${ns}/worker/${name}/promote`, {
      method: "POST",
      body: JSON.stringify({ version }),
    });
    if (!prom.ok) throw new Error(`probe promote failed: ${prom.status} ${await prom.text()}`);
    return true;
  }, { timeoutMs: 30000, intervalMs: 500 });

  await waitUntil("gateway → runtime upstream", async () => {
    const res = await gatewayFetch(ns, `/${name}`).catch(() => null);
    return Boolean(res && res.status === 200 && (await res.text()) === "ready");
  }, { timeoutMs: 30000, intervalMs: 500 });
}

// Polls the proxy's /logs/tail/active until `<ns>:<worker>` is hot,
// or throws on timeout. PUBLISH propagation isn't deterministic so a
// fixed setTimeout would flake under CI load.
/**
 * @param {string} ns
 * @param {string} worker
 * @param {{ timeoutMs?: number, intervalMs?: number }} [opts]
 */
export async function waitForActivation(ns, worker, opts = {}) {
  const target = `${ns}:${worker}`;
  const timeoutMs = opts.timeoutMs ?? 4000;
  const intervalMs = opts.intervalMs ?? 25;
  const deadline = Date.now() + timeoutMs;
  let lastSeen = null;
  while (Date.now() < deadline) {
    let parsed;
    try {
      const res = internalHttpRequest("redis-proxy-user", 7070, "/logs/tail/active", "GET");
      parsed = responseJson(res);
    } catch {
      await delay(intervalMs);
      continue;
    }
    const list = Array.isArray(parsed?.active) ? parsed.active : [];
    lastSeen = list;
    if (list.includes(target)) return true;
    await delay(intervalMs);
  }
  throw new Error(
    `waitForActivation timed out after ${timeoutMs}ms waiting for ${JSON.stringify(target)}; ` +
    `last proxy active=${JSON.stringify(lastSeen)}`,
  );
}

// FLUSHALL + /reload; short wait for the pub/sub round-trip — raise
// if stale state starts leaking.
export async function resetStack() {
  redisFlushAll();
  redisSet("wf:schema_version", "2", { db: 2 });
  const reload = await adminFetch("/reload", { method: "POST" });
  assertStatus(reload, 200, "gateway reload after reset");
  await waitForGatewayCacheState(
    "gateway route caches clear after reset",
    (health) => health.namespace_cache_size === 0 && health.pattern_cache_size === 0
  );
}
