// Worker → worker RPC via workerLoader. Subclassing WorkerEntrypoint (not
// a plain object with `fetch`) is load-bearing: env values cross isolate
// boundaries via structured clone, and plain objects with function
// properties hit DataCloneError — workerd has to hand the loaded worker a
// JSRPC stub instead.
//
// The Proxy in the constructor synthesizes a forwarder for any unknown
// method name so env.X.<anyMethod>(...) reaches the target's named
// entrypoint. Protected by service-binding RPC integration tests.

import { WorkerEntrypoint } from "cloudflare:workers";
import { getLoadedWorkerStub } from "runtime-load";
import { fetchTailFields, startTailEnvelope } from "runtime-tail-forwarder";
import { INTERNAL_AUTH_HEADER } from "shared-internal-auth";
import { sanitizeRequestId } from "shared-observability";

const SERVICE_BINDING_INTERNAL_HEADERS = [
  "x-worker-id",
  "x-worker-prefix",
  "x-wdl-upstream-binding",
  INTERNAL_AUTH_HEADER,
];

/**
 * @typedef {{ targetNs: string, targetWorker: string, targetVersion: string, targetEntrypoint?: string | null, callerNs?: string, callerSecrets?: Record<string, string> }} ServiceBindingProps
 * @typedef {Record<string, (...args: unknown[]) => Promise<unknown>> & { fetch(request: Request): Promise<Response> }} ServiceEntrypoint
 * @typedef {{ getEntrypoint(name?: string | null, opts?: { props: Record<string, unknown> }): ServiceEntrypoint }} ServiceLoadedWorker
 * @typedef {{ LOADER: { get(key: string, loader: () => Promise<unknown>): ServiceLoadedWorker } }} ServiceLoaderEnv
 * @typedef {ServiceLoaderEnv & Record<string, unknown>} ServiceBindingEnv
 * @typedef {{ ctx: ExecutionContext & { props: ServiceBindingProps }, env: ServiceBindingEnv }} ServiceBindingSelf
 * @typedef {Error & { __sbAnnotated?: true }} AnnotatableServiceBindingError
 */

export class ServiceBinding extends WorkerEntrypoint {
  /**
   * @param {ExecutionContext} ctx
   * @param {unknown} env
   */
  constructor(ctx, env) {
    super(ctx, env);
    return new Proxy(this, {
      get(target, prop, receiver) {
        if (typeof prop !== "string") return Reflect.get(target, prop, receiver);
        // `await stub` probes `.then`; synthesizing one would make the stub
        // look like a pending Promise.
        if (prop === "then" || prop === "toJSON") return undefined;
        const real = Reflect.get(target, prop, receiver);
        if (real !== undefined) return real;
        /** @param {...unknown} args */
        const forwarder = (...args) => forwardRpc(serviceBinding(target), prop, args);
        return forwarder;
      },
    });
  }

  /** @param {Request} request */
  async fetch(request) {
    const self = serviceBinding(this);
    const { targetEntrypoint } = self.ctx.props;
    const targetId = targetIdOf(self.ctx.props);
    const forwarded = new Request(request);
    for (const header of SERVICE_BINDING_INTERNAL_HEADERS) forwarded.headers.delete(header);
    // Platform-set; caller cannot forge another worker's identity.
    forwarded.headers.set("x-worker-id", targetId);
    // ALS doesn't cross JSRPC in workerd — the outer rid isn't visible here,
    // so we can only preserve what the caller forwarded, never mint one.
    const requestId = sanitizeRequestId(forwarded.headers.get("x-request-id"));
    if (requestId) forwarded.headers.set("x-request-id", requestId);
    else forwarded.headers.delete("x-request-id");
    const identity = {
      namespace: self.ctx.props.targetNs,
      workerName: self.ctx.props.targetWorker,
      workerId: targetId,
      requestId,
    };
    const baseFields = fetchTailFields(forwarded);
    const tail = startTailEnvelope({
      env: self.env,
      ctx: self.ctx,
      identity,
      event: "worker_fetch",
      fields: baseFields,
    });
    try {
      const response = await targetStub(self, requestId)
        .getEntrypoint(targetEntrypoint ?? null, targetEntrypointOpts(self))
        .fetch(forwarded);
      tail.finish({
        outcome: "ok",
        status: response.status,
      });
      return response;
    } catch (err) {
      tail.finishError(err);
      throw err;
    }
  }
}

// Propagate caller identity into the target's ctx.props via workerd's
// `getEntrypoint(name, { props })` — only channel that crosses the JSRPC
// boundary without piggybacking on request body/headers.
/** @param {ServiceBindingSelf} self */
function targetEntrypointOpts(self) {
  const { callerNs, callerSecrets } = self.ctx.props;
  if (callerNs === undefined && callerSecrets === undefined) return undefined;
  /** @type {Record<string, unknown>} */
  const props = {};
  if (callerNs !== undefined) props.callerNs = callerNs;
  if (callerSecrets !== undefined) props.callerSecrets = callerSecrets;
  return { props };
}

/** @param {ServiceBindingProps} props */
function targetIdOf(props) {
  return `${props.targetNs}:${props.targetWorker}:${props.targetVersion}`;
}

/** @param {ServiceBinding} binding @returns {ServiceBindingSelf} */
function serviceBinding(binding) {
  return /** @type {ServiceBindingSelf} */ (/** @type {unknown} */ (binding));
}

/**
 * @param {ServiceBindingSelf} self
 * @param {string | null} requestId
 */
function targetStub(self, requestId) {
  const { targetNs, targetWorker, targetVersion } = self.ctx.props;
  const targetId = targetIdOf(self.ctx.props);
  // The pinned target version may not be active, so service-binding cold-loads
  // record the isolate but deliberately leave sibling eviction disabled.
  return getLoadedWorkerStub({
    requestId,
    env: self.env,
    ctx: self.ctx,
    ns: targetNs,
    worker: targetWorker,
    version: targetVersion,
    workerId: targetId,
  }).stub;
}

/**
 * @param {ServiceBindingSelf} self
 * @param {string} method
 * @param {unknown[]} args
 */
async function forwardRpc(self, method, args) {
  const { targetEntrypoint } = self.ctx.props;
  const targetId = targetIdOf(self.ctx.props);
  const entry = targetStub(self, null).getEntrypoint(
    targetEntrypoint ?? null,
    targetEntrypointOpts(self)
  );
  try {
    return await entry[method](...args);
  } catch (err) {
    annotateServiceBindingError(err, targetEntrypoint, method, targetId);
    throw err;
  }
}

/**
 * @param {unknown} err
 * @param {string | null | undefined} targetEntrypoint
 * @param {string} method
 * @param {string} targetId
 */
function annotateServiceBindingError(err, targetEntrypoint, method, targetId) {
  if (!shouldAnnotateServiceBindingError(err)) return;
  try {
    err.message = `${err.message} [service binding ${targetEntrypoint ?? "default"}#${method} → ${targetId}]`;
    Object.defineProperty(err, "__sbAnnotated", { value: true });
  } catch {
    // Frozen / non-writable message — leave as-is.
  }
}

/** @param {unknown} err @returns {err is AnnotatableServiceBindingError} */
function shouldAnnotateServiceBindingError(err) {
  // Mutate in place instead of wrapping so enhanced_error_serialization keeps
  // Error names, instanceof behavior, and own fields across JSRPC. The
  // non-enumerable marker dedups Error hops; plain-object structured clone
  // drops it, so existing multi-hop plain-object behavior is preserved.
  return Boolean(err && typeof err === "object" &&
    typeof /** @type {{ message?: unknown, __sbAnnotated?: unknown }} */ (err).message === "string" &&
    !/** @type {{ message?: unknown, __sbAnnotated?: unknown }} */ (err).__sbAnnotated);
}
