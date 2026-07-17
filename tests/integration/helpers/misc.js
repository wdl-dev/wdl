import { createHash } from "node:crypto";
import { bundleKey } from "../../../shared/worker-contract.js";

import { redisHGetJson } from "./redis.js";

/** @param {string} cron @param {string} timezone */
export function cronId(cron, timezone) {
  return createHash("sha1").update(`${cron}|${timezone}`).digest("hex").slice(0, 10);
}

/**
 * @param {string} ns
 * @param {string} name
 * @param {string} version
 */
export function readMeta(ns, name, version) {
  const key = bundleKey(ns, name, version);
  return redisHGetJson(key, "__meta__", { label: `${key} __meta__` });
}
