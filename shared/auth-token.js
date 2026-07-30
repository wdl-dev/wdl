// Single source of truth for the `x-admin-token` header sanitizer.
// Both auth (the verifier) and control (the caller that pre-extracts
// before handing the token to AUTH.verify) must reject the same shapes
// identically — drift between the two would let a sanitation bypass on
// either side surface as an auth policy bug that's hard to correlate.

import { utf8ByteLength } from "./utf8.js";

export const MAX_TOKEN_HEADER_BYTES = 256;

// Returns the cleaned token, or null if the header is missing / dirty
// (caller treats both as 401 missing token).
/**
 * @param {{ get(name: string): string | null } | Record<string, unknown> | null | undefined} headersLike
 * @returns {string | null}
 */
export function extractToken(headersLike) {
  if (!headersLike) return null;
  const raw =
    typeof headersLike.get === "function"
      ? headersLike.get("x-admin-token")
      : /** @type {Record<string, unknown>} */ (headersLike)["x-admin-token"];
  // Multi-valued headers join with `, `; we never try to pick a "good"
  // element, concatenated values are dirty by construction.
  if (Array.isArray(raw)) return null;
  if (typeof raw !== "string") return null;
  if (raw.includes(",")) return null;
  const trimmed = raw.trim();
  if (!trimmed) return null;
  if (utf8ByteLength(trimmed) > MAX_TOKEN_HEADER_BYTES) return null;
  for (let i = 0; i < trimmed.length; i++) {
    const code = trimmed.charCodeAt(i);
    if (code < 0x20 || code === 0x7f) return null;
  }
  return trimmed;
}
