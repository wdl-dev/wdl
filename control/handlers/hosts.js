import {
  jsonResponse,
  jsonError,
  readJsonBody,
  codedErrorLogFields,
  codedErrorResponse,
  requireControlLog,
  requireControlRedis,
} from "control-shared";
import { reconcileHosts, RoutingError } from "control-routing";
import { platformDomainFromEnv } from "shared-ns-pattern";
import { hostsKey } from "shared-worker-contract";

/**
 * @param {{ request: Request, env: Record<string, unknown>, method: string, nsName: string, requestId: string }} args
 */
export async function handle({ request, env, method, nsName, requestId }) {
  const redis = requireControlRedis();
  const log = requireControlLog();

  if (method === "GET") {
    const hosts = await redis.sMembers(hostsKey(nsName));
    return jsonResponse(200, {
      namespace: nsName,
      hosts: [...hosts].toSorted(),
    });
  }
  if (method === "POST") {
    const parsed = await readJsonBody(request, { requireObject: true });
    if (parsed.response) return parsed.response;
    const body = /** @type {Record<string, unknown>} */ (parsed.body);
    try {
      const platformDomain = platformDomainFromEnv(env);
      const hosts = await reconcileHosts(redis, nsName, body, platformDomain);
      log("info", "hosts_reconciled", {
        request_id: requestId,
        namespace: nsName,
        host_count: hosts.length,
      });
      return jsonResponse(200, { namespace: nsName, hosts });
    } catch (err) {
      if (err instanceof RoutingError) {
        log(err.status >= 500 ? "error" : "warn", "hosts_reconcile_rejected", {
          request_id: requestId,
          namespace: nsName,
          ...codedErrorLogFields(err, "routing_error"),
        });
        return codedErrorResponse(err, "routing_error");
      }
      throw err;
    }
  }
  return jsonError(405, "method_not_allowed", "Method not allowed");
}
