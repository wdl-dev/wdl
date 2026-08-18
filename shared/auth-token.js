// Canonical `x-admin-token` sanitizer used by Control before it calls Auth.

import { utf8ByteLength } from "./utf8.js";

const MAX_TOKEN_HEADER_BYTES = 256;

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
