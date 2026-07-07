/** @param {string} ns */
export function nsSecretsKey(ns) {
  return `secrets:${ns}`;
}

/** @param {string} ns @param {string} worker */
export function workerSecretsKey(ns, worker) {
  return `secrets:${ns}:${worker}`;
}
