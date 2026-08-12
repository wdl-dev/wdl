export const AI_RUNTIME_SETTINGS = Object.freeze({
  AI_REQUEST_MAX_IN_FLIGHT: { defaultValue: 32, maxValue: 4096 },
  AI_STREAM_MAX_IN_FLIGHT: { defaultValue: 16, maxValue: 1024 },
  AI_WS_MAX_SESSIONS: { defaultValue: 8, maxValue: 1024 },
  AI_REQUEST_BUDGET_MS: { defaultValue: 120_000, maxValue: 10 * 60_000 },
  AI_STREAM_IDLE_TIMEOUT_MS: { defaultValue: 30_000, maxValue: 30 * 60_000 },
  AI_STREAM_MAX_DURATION_MS: { defaultValue: 300_000, maxValue: 60 * 60_000 },
  AI_WS_HANDSHAKE_BUDGET_MS: { defaultValue: 15_000, maxValue: 120_000 },
  AI_WS_IDLE_TIMEOUT_MS: { defaultValue: 120_000, maxValue: 30 * 60_000 },
  AI_WS_MAX_DURATION_MS: { defaultValue: 24 * 60_000, maxValue: 2 * 60 * 60_000 },
});

/**
 * @param {Record<string, unknown>} env
 * @param {keyof typeof AI_RUNTIME_SETTINGS} name
 */
export function aiRuntimeSetting(env, name) {
  const spec = AI_RUNTIME_SETTINGS[name];
  const raw = Number(env[name] ?? spec.defaultValue);
  if (!Number.isFinite(raw) || raw <= 0) return spec.defaultValue;
  return Math.max(1, Math.min(Math.trunc(raw), spec.maxValue));
}
