import {
  D1ProtocolError,
  normalizeQueryRequest,
} from "d1-runtime-protocol";
import { redisClient } from "d1-runtime-owner-registry";

const TEST_HOOK_CONTROLS = new Set(["hold-transaction"]);

/**
 * @typedef {{ D1_TEST_HOOKS?: unknown, [key: string]: unknown }} D1TestHookEnv
 * @typedef {{ dbKey: string, taskId?: string | null }} D1TestHookOwner
 * @typedef {{ sql: string, params: unknown[] }} D1TestHookStatement
 * @typedef {{ __control: string, __holdMs?: unknown, owner: D1TestHookOwner, statements: D1TestHookStatement[] }} D1TestHookRequest
 * @typedef {{ env: D1TestHookEnv, sql: { exec(sql: string, ...params: unknown[]): unknown }, state: { storage: { transactionSync(callback: () => unknown): unknown } } }} D1TestHookActor
 */

/** @param {D1TestHookEnv} env */
function testHooksEnabled(env) {
  return env?.D1_TEST_HOOKS === "1";
}

/** @param {unknown} body */
export function normalizeD1TestHookRequest(body) {
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    throw new D1ProtocolError(400, "invalid-body", "request body must be an object");
  }
  const record = /** @type {Record<string, unknown>} */ (body);
  const control = record.__control;
  if (typeof control !== "string" || !TEST_HOOK_CONTROLS.has(control)) {
    throw new D1ProtocolError(
      400,
      "invalid-control",
      `__control must be one of ${Array.from(TEST_HOOK_CONTROLS).join(", ")}`
    );
  }
  const query = normalizeQueryRequest({ ...record, __control: undefined, __holdMs: undefined });
  return {
    ...query,
    __control: control,
    __holdMs: record.__holdMs,
  };
}

/** @param {D1TestHookEnv} env */
export function assertD1TestHooksEnabled(env) {
  if (!testHooksEnabled(env)) {
    throw new D1ProtocolError(404, "not-found", "D1 test hook is disabled");
  }
}

/** @param {unknown} body */
export function isD1ActorTestHook(body) {
  const control = body !== null &&
    typeof body === "object" &&
    !Array.isArray(body)
    ? /** @type {Record<string, unknown>} */ (body).__control
    : null;
  return body !== null &&
    typeof body === "object" &&
    !Array.isArray(body) &&
    typeof control === "string" &&
    TEST_HOOK_CONTROLS.has(control);
}

/** @param {D1TestHookActor} actor @param {D1TestHookRequest} body */
export async function runD1ActorTestHook(actor, body) {
  assertD1TestHooksEnabled(actor.env);
  if (body.__control === "hold-transaction") {
    return await holdTransactionForTest(actor, body.owner, body.statements, body.__holdMs);
  }
  throw new D1ProtocolError(400, "invalid-control", "Unknown D1 test hook control");
}

/** @param {D1TestHookActor} actor @param {D1TestHookOwner} owner @param {D1TestHookStatement[]} statements @param {unknown} holdMs */
async function holdTransactionForTest(actor, owner, statements, holdMs) {
  const ms = Number.isFinite(Number(holdMs)) && Number(holdMs) > 0
    ? Math.min(Number(holdMs), 60_000)
    : 30_000;
  actor.sql.exec("create table if not exists _wdl_d1_test_hooks (id text primary key, created_at text)");
  actor.sql.exec(
    "insert or replace into _wdl_d1_test_hooks (id, created_at) values (?, ?)",
    "hold-transaction-started",
    String(Date.now())
  );
  await redisClient(actor.env).set(
    `d1:test-hook:hold-started:${encodeURIComponent(owner.dbKey)}`,
    owner.taskId || "unknown",
    { ttl: 60 }
  );
  actor.state.storage.transactionSync(() => {
    for (const statement of statements) {
      actor.sql.exec(statement.sql, ...statement.params);
    }
    const deadline = Date.now() + ms;
    while (Date.now() < deadline) {
      // Keep this as a synchronous busy-wait: using setTimeout/await would
      // yield the Durable Object request and stop modeling an in-flight
      // SQLite transaction that is killed while still open.
    }
    throw new D1ProtocolError(500, "test-transaction-held", "D1 test transaction hold completed without crash");
  });
  return { held: true };
}
