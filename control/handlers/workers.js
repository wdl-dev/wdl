import { jsonResponse, jsonError, requireControlLog, requireControlRedis } from "control-shared";
import { routesKey, workersIndexKey, workerVersionsKey } from "control-lib";
import { workerSecretsKey } from "shared-secret-keys";

/** @param {{ method: string, nsName: string, requestId: string }} args */
export async function handle({ method, nsName, requestId }) {
  const redis = requireControlRedis();
  const log = requireControlLog();

  if (method !== "GET") {
    return jsonError(405, "method_not_allowed", "Method not allowed");
  }

  const workers = await redis.session(async (session) => {
    const names = /** @type {string[]} */ ((await session.sMembers(workersIndexKey(nsName))).toSorted());

    const routesHash = /** @type {Record<string, string>} */ (await session.hGetAll(routesKey(nsName)));

    const versionKeys = names.map((name) => workerVersionsKey(nsName, name));
    const secretKeys = names.map((name) => workerSecretsKey(nsName, name));
    const versionsByWorker = await session.zRangeMany(versionKeys, 0, -1);
    const secretFlags = await session.existsMany(secretKeys);

    return names.map((name, idx) => {
      const activeVersion = routesHash[name] || null;
      const versions = versionsByWorker[idx] || [];
      return {
        name,
        activeVersion,
        versions,
        versionCount: versions.length,
        hasSecrets: Boolean(secretFlags[idx]),
      };
    });
  });

  log("info", "workers_listed", {
    request_id: requestId,
    namespace: nsName,
    count: workers.length,
  });
  return jsonResponse(200, { namespace: nsName, workers });
}
