import { jsonResponse, jsonError, requireControlLog, requireControlRedis } from "control-shared";
import { workflowDefsKey, workersIndexKey } from "control-lib";
import { workerSecretsKey } from "shared-secret-keys";
import { routesKey, workerVersionsKey } from "shared-worker-contract";

/** @param {{ method: string, nsName: string, requestId: string }} args */
export async function handle({ method, nsName, requestId }) {
  const redis = requireControlRedis();
  const log = requireControlLog();

  if (method !== "GET") {
    return jsonError(405, "method_not_allowed", "Method not allowed");
  }

  const workers = await redis.session(async (session) => {
    const snapshot = await session.sMembersAndHGetAll(
      workersIndexKey(nsName),
      routesKey(nsName)
    );
    const names = snapshot.members.toSorted();
    const routesHash = snapshot.hash;

    const versionKeys = names.map((name) => workerVersionsKey(nsName, name));
    const secretKeys = names.map((name) => workerSecretsKey(nsName, name));
    const definitionKeys = names.map((name) => workflowDefsKey(nsName, name));
    const {
      ranges: versionsByWorker,
      exists: stateFlags,
    } = await session.zRangeManyAndExistsMany(
      versionKeys,
      [...secretKeys, ...definitionKeys],
      0,
      -1
    );
    const secretFlags = stateFlags.slice(0, names.length);
    const definitionFlags = stateFlags.slice(names.length);

    return names.map((name, idx) => {
      const activeVersion = routesHash[name] || null;
      const versions = versionsByWorker[idx] || [];
      return {
        name,
        activeVersion,
        versions,
        versionCount: versions.length,
        hasSecrets: Boolean(secretFlags[idx]),
        hasWorkflowDefs: Boolean(definitionFlags[idx]),
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
