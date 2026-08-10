import { composeProfileUp, composeRestart, composeUp } from "./compose.js";
import { sh } from "./cli.js";
import { serviceInternalGet } from "./internal-http.js";
import { waitUntil } from "../../helpers/timing.js";

async function waitForDoRouter() {
  await waitUntil("Envoy do_router upstream", () => {
    try {
      return serviceInternalGet("envoy", 18788, "/healthz").status === 200;
    } catch {
      return false;
    }
  }, { timeoutMs: 10_000, intervalMs: 100 });
}

export function stopD1MultiRuntimes() {
  sh(["docker", "compose", "rm", "-sf", "d1-runtime-a", "d1-runtime-b", "d1-runtime-c"], {
    stdio: "pipe",
    env: { COMPOSE_PROFILES: "d1-multi" },
  });
}

export function ensureD1SingleRuntime() {
  // Force-recreate so d1-runtime process-local owner/drain state from a
  // prior multi-runtime test never leaks into a later single-runtime test.
  // Already-clean stacks skip the recreate.
  const state = sh(["docker", "compose", "ps", "--format", "{{.Service}} {{.State}}"], {
    stdio: "pipe",
  });
  const multiRunning = /^d1-runtime-[abc] +running/m.test(state);
  const singleHealthy = /^d1-runtime +running/m.test(state);
  if (!multiRunning && singleHealthy) return;
  stopD1MultiRuntimes();
  sh(["docker", "compose", "rm", "-sf", "d1-runtime"], { stdio: "pipe" });
  return composeUp(["--force-recreate", "--wait", "d1-runtime"], { stdio: "pipe" });
}

/** @param {{ ownerLeaseGuardMs?: number }} [options] */
export function recreateD1MultiRuntimes({ ownerLeaseGuardMs } = {}) {
  sh(["docker", "compose", "stop", "d1-runtime"], { stdio: "pipe" });
  const opts = {
    stdio: "pipe",
    env: {
      ...process.env,
      ...(ownerLeaseGuardMs == null ? {} : { D1_OWNER_LEASE_GUARD_MS: String(ownerLeaseGuardMs) }),
    },
  };
  // The three D1 runtimes intentionally share one localDisk volume in these
  // tests; recreate them one at a time so workerd does not race metadata
  // initialization across fresh processes.
  composeProfileUp("d1-multi", ["--force-recreate", "--wait", "d1-runtime-a"], opts);
  composeProfileUp("d1-multi", ["--force-recreate", "--wait", "d1-runtime-b"], opts);
  return composeProfileUp("d1-multi", ["--force-recreate", "--wait", "d1-runtime-c"], opts);
}

export function stopDoMultiRuntimes() {
  sh(["docker", "compose", "rm", "-sf", "do-runtime-a", "do-runtime-b", "do-runtime-c"], {
    stdio: "pipe",
    env: { COMPOSE_PROFILES: "do-multi" },
  });
}

export async function ensureDoSingleRuntime() {
  const state = sh(["docker", "compose", "ps", "--format", "{{.Service}} {{.State}}"], {
    stdio: "pipe",
  });
  const multiRunning = /^do-runtime-[abc] +running/m.test(state);
  const singleHealthy = /^do-runtime +running/m.test(state);
  if (multiRunning || !singleHealthy) {
    stopDoMultiRuntimes();
    sh(["docker", "compose", "rm", "-sf", "do-runtime"], { stdio: "pipe" });
    composeUp(["--force-recreate", "--wait", "do-runtime"], { stdio: "pipe" });
  }
  await waitForDoRouter();
}

export async function restartDoSingleRuntime() {
  composeRestart("do-runtime");
  await waitForDoRouter();
}

/** @param {{ preventEviction?: boolean }} [options] */
export async function recreateDoSingleRuntime({ preventEviction } = {}) {
  stopDoMultiRuntimes();
  sh(["docker", "compose", "rm", "-sf", "do-runtime"], { stdio: "pipe" });
  composeUp(["--force-recreate", "--wait", "do-runtime"], {
    stdio: "pipe",
    env: {
      ...process.env,
      ...(preventEviction == null ? {} : { DO_PREVENT_EVICTION: String(preventEviction) }),
    },
  });
  await waitForDoRouter();
}

/**
 * @param {{
 *   ownerTtlSeconds?: number,
 *   ownerLeaseGuardMs?: number,
 *   renewStartDelayMs?: number,
 *   renewIntervalMs?: number,
 *   preventEviction?: boolean,
 * }} [options]
 */
export async function recreateDoMultiRuntimes({ ownerTtlSeconds, ownerLeaseGuardMs, renewStartDelayMs, renewIntervalMs, preventEviction } = {}) {
  sh(["docker", "compose", "stop", "do-runtime"], { stdio: "pipe" });
  const opts = {
    stdio: "pipe",
    env: {
      ...process.env,
      ...(ownerTtlSeconds == null ? {} : { DO_OWNER_TTL_SECONDS: String(ownerTtlSeconds) }),
      ...(ownerLeaseGuardMs == null ? {} : { DO_OWNER_LEASE_GUARD_MS: String(ownerLeaseGuardMs) }),
      ...(renewStartDelayMs == null ? {} : { DO_RENEW_START_DELAY_MS: String(renewStartDelayMs) }),
      ...(renewIntervalMs == null ? {} : { DO_RENEW_INTERVAL_MS: String(renewIntervalMs) }),
      ...(preventEviction == null ? {} : { DO_PREVENT_EVICTION: String(preventEviction) }),
    },
  };
  composeProfileUp("do-multi", ["--force-recreate", "--wait", "do-runtime-a"], opts);
  composeProfileUp("do-multi", ["--force-recreate", "--wait", "do-runtime-b"], opts);
  composeProfileUp("do-multi", ["--force-recreate", "--wait", "do-runtime-c"], opts);
  await waitForDoRouter();
}

/**
 * @param {() => Promise<unknown>} fn
 * @param {{
 *   ownerTtlSeconds?: number,
 *   ownerLeaseGuardMs?: number,
 *   renewStartDelayMs?: number,
 *   renewIntervalMs?: number,
 *   preventEviction?: boolean,
 * }} [options]
 */
export async function withDoMultiRuntimes(fn, options = {}) {
  await recreateDoMultiRuntimes(options);
  try {
    return await fn();
  } finally {
    await ensureDoSingleRuntime();
  }
}

export function startDoOwnerTask() {
  composeProfileUp("do-multi", ["--force-recreate", "--wait", "do-runtime-a"], {
    stdio: "pipe",
  });
  const container = sh(["docker", "compose", "ps", "-q", "do-runtime-a"], {
    stdio: "pipe",
    env: { COMPOSE_PROFILES: "do-multi" },
  }).trim();
  const network = sh(
    [
      "docker",
      "inspect",
      "-f",
      "{{range $name, $_ := .NetworkSettings.Networks}}{{println $name}}{{end}}",
      container,
    ],
    { stdio: "pipe" }
  ).trim().split("\n")[0];
  // Drop the shared do-runtime-router alias so Envoy's STRICT_DNS pool only
  // resolves the singleton do-runtime; this makes the first WS owner-hint hop
  // deterministically enter through a non-owner task.
  sh(["docker", "network", "disconnect", network, container], { stdio: "pipe" });
  sh(["docker", "network", "connect", "--alias", "do-runtime-a", network, container], {
    stdio: "pipe",
  });
}

export function stopDoOwnerTask() {
  return sh(["docker", "compose", "rm", "-sf", "do-runtime-a"], {
    stdio: "pipe",
    env: { COMPOSE_PROFILES: "do-multi" },
  });
}

/** @param {() => Promise<unknown>} fn */
export async function withDoOwnerTask(fn) {
  startDoOwnerTask();
  try {
    return await fn();
  } finally {
    stopDoOwnerTask();
    await ensureDoSingleRuntime();
  }
}
