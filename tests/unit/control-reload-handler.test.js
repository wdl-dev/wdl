import { test } from "node:test";
import assert from "node:assert/strict";
import { controlSharedStubUrl } from "../helpers/control-shared-stub.js";
import {
  applyModuleReplacements,
  moduleDataUrl,
  readRepositoryFile,
} from "../helpers/load-shared-module.js";
import { assertJsonResponse } from "../helpers/response-json.js";

const controlSharedUrl = controlSharedStubUrl(`
export async function publishReload(requestId) {
  globalThis.__reloadHandlerState.requestIds.push(requestId);
  return globalThis.__reloadHandlerState.publishResult;
}
`);

const src = applyModuleReplacements(readRepositoryFile("control/handlers/reload.js"), [
  [/from "control-shared";/, `from ${JSON.stringify(controlSharedUrl)};`],
]);

const { handle } = await import(moduleDataUrl(src));

/** @param {any} publishResult */
function resetReloadHandlerState(publishResult) {
  /** @type {any} */ (globalThis).__reloadHandlerState = {
    requestIds: [],
    publishResult,
  };
  return /** @type {any} */ (globalThis).__reloadHandlerState;
}

test("reload handler maps internal publish metrics to camelCase API fields", async () => {
  const state = resetReloadHandlerState({
    ok: true,
    declarations: {
      ok: true,
      declaredHosts: 4,
      declarationKeysRemoved: 1,
      duration_ms: 3,
    },
    routes: {
      ok: true,
      channel: "routes:flush",
      receivers: 2,
      duration_ms: 7,
    },
    patterns: {
      ok: true,
      channel: "patterns:invalidate",
      receivers: 3,
      duration_ms: 11,
    },
  });

  const response = await handle({ requestId: "rid-reload" });

  assert.deepEqual(state.requestIds, ["rid-reload"]);
  await assertJsonResponse(response, 200, {
    reload: {
      ok: true,
      declarations: {
        ok: true,
        declaredHosts: 4,
        declarationKeysRemoved: 1,
        durationMs: 3,
      },
      routes: {
        ok: true,
        channel: "routes:flush",
        receivers: 2,
        durationMs: 7,
      },
      patterns: {
        ok: true,
        channel: "patterns:invalidate",
        receivers: 3,
        durationMs: 11,
      },
    },
  });
});

test("reload handler preserves publish failure status with camelCase duration", async () => {
  resetReloadHandlerState({
    ok: false,
    declarations: {
      ok: true,
      declaredHosts: 0,
      declarationKeysRemoved: 0,
      duration_ms: 2,
    },
    routes: {
      ok: false,
      channel: "routes:flush",
      error: "redis down",
      duration_ms: 5,
    },
    patterns: {
      ok: true,
      channel: "patterns:invalidate",
      receivers: 1,
      duration_ms: 6,
    },
  });

  const response = await handle({ requestId: "rid-reload-fail" });

  await assertJsonResponse(response, 502, {
    reload: {
      ok: false,
      declarations: {
        ok: true,
        declaredHosts: 0,
        declarationKeysRemoved: 0,
        durationMs: 2,
      },
      routes: {
        ok: false,
        channel: "routes:flush",
        durationMs: 5,
        error: "redis down",
      },
      patterns: {
        ok: true,
        channel: "patterns:invalidate",
        receivers: 1,
        durationMs: 6,
      },
    },
  });
});

test("reload handler does not invent publish results when declaration repair fails", async () => {
  resetReloadHandlerState({
    ok: false,
    declarations: {
      ok: false,
      error: "declared host repair failed",
      duration_ms: 4,
    },
  });

  const response = await handle({ requestId: "rid-reload-repair-fail" });

  await assertJsonResponse(response, 502, {
    reload: {
      ok: false,
      declarations: {
        ok: false,
        durationMs: 4,
        error: "declared host repair failed",
      },
    },
  });
});
