import { test } from "node:test";
import assert from "node:assert/strict";
import {
  AI_RUNTIME_SETTINGS,
  aiRuntimeSetting,
} from "../../shared/ai-runtime-config.js";

test("AI runtime settings apply their canonical defaults and hard maxima", () => {
  for (const rawName of Object.keys(AI_RUNTIME_SETTINGS)) {
    const name = /** @type {keyof typeof AI_RUNTIME_SETTINGS} */ (rawName);
    const spec = AI_RUNTIME_SETTINGS[name];
    assert.equal(aiRuntimeSetting({}, name), spec.defaultValue, name);
    assert.equal(aiRuntimeSetting({ [name]: "0" }, name), spec.defaultValue, name);
    assert.equal(aiRuntimeSetting({ [name]: "invalid" }, name), spec.defaultValue, name);
    assert.equal(aiRuntimeSetting({ [name]: String(spec.maxValue + 1) }, name), spec.maxValue, name);
    assert.equal(aiRuntimeSetting({ [name]: "1.9" }, name), 1, name);
  }
});
