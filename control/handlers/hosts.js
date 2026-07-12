import {
  jsonResponse,
  jsonError,
  readJsonBody,
  codedErrorResponse,
  requireControlLog,
  requireControlRedis,
} from "control-shared";
import { reconcileHosts, RoutingError } from "control-routing";
import { platformDomainFromEnv } from "control-lib";
import { hostsKey } from "shared-version";

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
        return codedErrorResponse(err, "routing_error");
      }
      throw err;
    }
  }
  return jsonError(405, "method_not_allowed", "Method not allowed");
}
