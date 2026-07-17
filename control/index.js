// Control's state-change seam back to gateway is Redis: promote HSETs +
// PUBLISHes, gateway clears caches on subscribe. Admin-host HTTP ingress is
// handled by gateway before control sees the request. Pub/sub is
// fire-and-forget: promote returns 200 once EXEC succeeds; gateway convergence
// is eventual.

import {
  NS_RE,
  parseControlRoute,
  configuredPublicUrl,
  platformVersionFromPackageJson,
  projectAccessPrincipal,
  isAdminAcceptableNs,
} from "control-lib";
import { configuredHostname, isValidWorkerName, WORKER_NAME_RE } from "shared-ns-pattern";
import {
  ensureInit,
  authorizeControlRequest,
  internalErrorResponse,
  jsonError,
  jsonResponse,
  requireControlLog,
  state,
} from "control-shared";
import { createHttpRequestScope } from "shared-request-scope";
import PACKAGE_JSON_SOURCE from "wdl-package-json-source";
import { handle as handleReload } from "control-handlers-reload";
import { handle as handleAuthTokens } from "control-handlers-auth-tokens";
import { handle as handleNsSecrets } from "control-handlers-ns-secrets";
import { handle as handleHosts } from "control-handlers-hosts";
import { handle as handleWorkerSecrets } from "control-handlers-worker-secrets";
import { handle as handleVersions } from "control-handlers-versions";
import { handle as handleDeploy } from "control-handlers-deploy";
import { handle as handlePromote } from "control-handlers-promote";
import { handle as handleWorkers } from "control-handlers-workers";
import { handle as handleDelete } from "control-handlers-delete";
import { handle as handleD1 } from "control-handlers-d1";
import { handle as handleR2 } from "control-handlers-r2";
import { handle as handleLogsTail } from "control-handlers-logs-tail";
import { handle as handleWorkflows } from "control-handlers-workflows";

/**
 * @typedef {{
 *   kind?: string,
 *   ns?: string,
 *   worker?: string,
 *   workerAction?: string,
 *   subPath?: string[],
 *   secretKey?: string,
 *   tokenId?: string,
 *   action?: string,
 *   scopeRoute?: string,
 * }} ControlRouteInfo
 * @typedef {{ ok: true, principal: import("control-lib").AccessPrincipal | null, tokenId: string, status: number }} ControlAuthOk
 * @typedef {{ request: Request, env: Record<string, unknown>, ctx?: ExecutionContext, url: URL, method: string, auth?: ControlAuthOk, requestId: string }} DispatchContext
 */

/**
 * @param {string} nsName
 * @param {{ includeGrammar?: boolean }} [options]
 */
function invalidNamespaceResponse(nsName, { includeGrammar = false } = {}) {
  const grammar = includeGrammar ? ` Must match ${NS_RE} (or be a reserved __ns__).` : "";
  return jsonError(400, "invalid_namespace", `Invalid namespace "${nsName}".${grammar}`);
}

/**
 * @param {string} nsName
 * @param {{ includeGrammar?: boolean }} [options]
 */
function namespaceValidationResponse(nsName, options) {
  return isAdminAcceptableNs(nsName) ? null : invalidNamespaceResponse(nsName, options);
}

/** @param {string} name */
function invalidWorkerNameResponse(name) {
  return jsonError(
    400,
    "invalid_worker_name",
    `Invalid worker name ${JSON.stringify(name)}. Must match ${WORKER_NAME_RE} (letters, digits, underscores, and hyphens; starts with a letter or digit; up to 255 chars).`
  );
}

const NS_SCOPED_KINDS = new Set([
  "nsSecrets",
  "hosts",
  "logsTail",
  "d1",
  "r2",
  "workflows",
  "workers",
]);

const PLATFORM_VERSION = platformVersionFromPackageJson(PACKAGE_JSON_SOURCE);
const MIN_CLI_VERSION = "0.11.0";

/**
 * @param {Request} request
 * @param {URL} requestUrl
 */
function publicRequestUrl(request, requestUrl) {
  const proto = request.headers.get("x-forwarded-proto");
  if (proto === "http" || proto === "https") {
    const publicUrl = new URL(requestUrl.href);
    publicUrl.protocol = `${proto}:`;
    return publicUrl;
  }
  return requestUrl;
}

/**
 * @param {URL} requestUrl
 * @param {Record<string, unknown>} env
 * @param {import("control-lib").AccessPrincipal | null} principal
 */
function whoamiUrls(requestUrl, env, principal) {
  /** @type {{ control: string, namespace?: string, assets?: string }} */
  const urls = { control: requestUrl.origin };
  if (principal?.kind === "ns" && typeof principal.ns === "string") {
    const platformDomain = configuredHostname(env.PLATFORM_DOMAIN);
    if (platformDomain) {
      const namespaceUrl = new URL(requestUrl.href);
      namespaceUrl.hostname = `${principal.ns}.${platformDomain}`;
      namespaceUrl.pathname = "/";
      namespaceUrl.search = "";
      namespaceUrl.hash = "";
      urls.namespace = namespaceUrl.origin;
    }
  }
  const assets = configuredPublicUrl(env.ASSETS_CDN_BASE);
  if (assets) urls.assets = assets;
  return urls;
}

/**
 * @param {ControlRouteInfo} routeInfo
 * @param {DispatchContext} context
 */
async function dispatchNamespaceRoute(routeInfo, context) {
  const { request, env, ctx, url, method, requestId } = context;
  if (typeof routeInfo.kind !== "string" || !NS_SCOPED_KINDS.has(routeInfo.kind)) return null;

  const nsName = routeInfo.ns;
  if (typeof nsName !== "string") return jsonError(400, "invalid_path", "Invalid namespace route");
  const invalidNs = namespaceValidationResponse(nsName, { includeGrammar: true });
  if (invalidNs) return invalidNs;

  switch (routeInfo.kind) {
    case "nsSecrets":
      return await handleNsSecrets({
        request, env, method,
        nsName,
        secretKey: routeInfo.secretKey,
        requestId,
      });
    case "hosts":
      return await handleHosts({
        request, env, method, nsName, requestId,
      });
    case "logsTail":
      // SSE: echoResponseWithRequestId rewraps the streaming body
      // without buffering, so request_complete logs at headers-sent
      // time while the body keeps flowing under ctx.waitUntil.
      return await handleLogsTail({
        request, env, ctx: ctx ?? null, ns: nsName, requestId,
      });
    case "d1":
      return await handleD1({
        request,
        env: /** @type {import("control-d1-lifecycle").D1RuntimeEnv} */ (env),
        method,
        ns: nsName,
        subPath: routeInfo.subPath ?? [],
        requestId,
      });
    case "r2":
      return await handleR2({
        method, url, ns: nsName, subPath: routeInfo.subPath ?? [], requestId,
      });
    case "workflows":
      return await handleWorkflows({
        method, url, ns: nsName, subPath: routeInfo.subPath ?? [], requestId,
      });
    case "workers":
      return await handleWorkers({
        method, nsName, requestId,
      });
  }
}

/**
 * @param {ControlRouteInfo} routeInfo
 * @param {DispatchContext & { auth: ControlAuthOk }} context
 */
async function dispatchWorkerRoute(routeInfo, context) {
  const { request, env, url, method, auth, requestId } = context;
  const { ns, worker: name, workerAction: action, subPath } = routeInfo;
  if (typeof ns !== "string" || typeof name !== "string" || !Array.isArray(subPath)) {
    return jsonError(400, "invalid_path", "Invalid worker route");
  }
  const invalidNs = namespaceValidationResponse(ns, { includeGrammar: true });
  if (invalidNs) return invalidNs;
  if (!isValidWorkerName(name)) {
    return invalidWorkerNameResponse(name);
  }

  if (action === "secrets") {
    return await handleWorkerSecrets({
      request, env, method, ns, name, subPath, requestId,
    });
  }

  if (action === "versions") {
    return await handleVersions({
      method, ns, name, subPath,
      principal: auth.principal,
      requestId,
    });
  }

  // `ops` can reach actionless worker routes too; these POST checks are
  // the method gate for deploy/promote/delete.
  if (method === "POST" && action === "deploy") {
    return await handleDeploy({ request, env, ns, name, requestId });
  }

  if (method === "POST" && action === "promote") {
    return await handlePromote({ request, env, ns, name, requestId });
  }

  if (method === "POST" && action === "delete") {
    return await handleDelete({
      request, env, url, ns, name,
      principal: auth.principal,
      requestId,
    });
  }

  return jsonError(404, "not_found", "Not found");
}

export default {
  /**
   * @param {Request} request
   * @param {Record<string, unknown>} env
   * @param {ExecutionContext} ctx
   */
  async fetch(request, env, ctx) {
    ensureInit(env);
    const method = request.method;
    /** @type {string | null} */
    let namespace = null;
    /** @type {string | null} */
    let worker = null;
    const scope = createHttpRequestScope({
      request,
      service: state.service,
      metrics: null,
      log: requireControlLog(),
      route: "unknown",
      extras: () => ({ namespace, worker }),
    });
    const requestId = scope.requestId;

    /** @param {Response} resp */
    function finalize(resp) {
      return scope.respond(resp);
    }

    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      const routeInfo = /** @type {ControlRouteInfo} */ (parseControlRoute(pathname, method) || {});
      scope.setRoute(
        pathname === "/reload" ? "reload" :
        pathname === "/whoami" ? "whoami" :
        "worker_api"
      );

      // Authorize before reading body / touching Redis.
      // Dispatch params may exist for 404/405 handler continuity, but auth
      // sees the old classifier shape: no exact action match means no ns.
      const authRouteInfo = typeof routeInfo.action === "string"
        ? { action: routeInfo.action, ns: routeInfo.ns }
        : {};
      const authResult = await authorizeControlRequest(request, env, authRouteInfo, requestId);
      if (!authResult.ok) {
        const rejected = /** @type {{ status: number, error: string, message: string }} */ (authResult);
        return finalize(jsonError(rejected.status, rejected.error, rejected.message));
      }
      const auth = /** @type {ControlAuthOk} */ (authResult);

      namespace = typeof routeInfo.ns === "string" ? routeInfo.ns : null;
      worker = typeof routeInfo.worker === "string" ? routeInfo.worker : null;
      scope.setRoute(typeof routeInfo.scopeRoute === "string" ? routeInfo.scopeRoute : "worker_api");

      // `ops` can pass auth for actionless shapes; only POST may reload.
      if (routeInfo.kind === "reload" && method === "POST") {
        return finalize(await handleReload({ requestId }));
      }
      if (routeInfo.kind === "reload") {
        return finalize(jsonError(405, "method_not_allowed", "Method not allowed"));
      }

      if (routeInfo.kind === "whoami" && method === "GET") {
        const principal = projectAccessPrincipal(auth.principal);
        const publicUrl = publicRequestUrl(request, url);
        return finalize(jsonResponse(200, {
          ok: true,
          principal,
          tokenId: auth.tokenId,
          requestId,
          platformVersion: PLATFORM_VERSION,
          minCliVersion: MIN_CLI_VERSION,
          urls: whoamiUrls(publicUrl, env, principal),
        }));
      }
      if (routeInfo.kind === "whoami") {
        return finalize(jsonError(405, "method_not_allowed", "Method not allowed"));
      }

      if (routeInfo.kind === "authTokens") {
        if (routeInfo.tokenId !== undefined && typeof routeInfo.tokenId !== "string") {
          return finalize(jsonError(400, "invalid_path", "Invalid auth token route"));
        }
        return finalize(await handleAuthTokens({
          request, env, url, method, tokenId: routeInfo.tokenId, auth, requestId,
        }));
      }
      if (routeInfo.kind === "authDelegatedTokens") {
        return finalize(await handleAuthTokens({
          request, env, url, method, auth, requestId, routeKind: "delegatedTokens",
        }));
      }

      const namespaceResponse = await dispatchNamespaceRoute(routeInfo, {
        request, env, ctx, url, method, requestId,
      });
      if (namespaceResponse) {
        return finalize(namespaceResponse);
      }

      if (routeInfo.kind !== "worker") {
        scope.setRoute("invalid_path");
        return finalize(jsonError(400, "invalid_path", "Invalid path. Use /ns/<ns>/worker/<name>/<action>"));
      }
      return finalize(await dispatchWorkerRoute(routeInfo, {
        request, env, url, method, auth, requestId,
      }));
    } catch (err) {
      scope.markError(err);
      return scope.respond(internalErrorResponse(500, "internal_error", "Internal error", requestId));
    } finally {
      scope.complete();
    }
  },
};
