// Service bindings: arbitrary-method RPC, named entrypoints, and
// cross-ns exports with allowed_callers ACL. Baseline same-ns fetch()
// coverage lives in service-bindings.test.js — no overlap here.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  adminPost,
  assertIntegrationJson,
  deployAndPromote,
  gatewayFetch,
  readIntegrationJson,
  uniqueNs,
  setupIntegrationSuite,
  responseJson,
  workerFetchCallerSource,
} from "./helpers/index.js";

setupIntegrationSuite();

const MULTI_ENTRY_TARGET = `
import { WorkerEntrypoint } from "cloudflare:workers";
export class Auth extends WorkerEntrypoint {
  async verifyToken(token) { return { ok: true, who: "auth", token }; }
  async whoami() { return { entrypoint: "Auth" }; }
}
export class Admin extends WorkerEntrypoint {
  async whoami() { return { entrypoint: "Admin" }; }
  async deleteAll() { return { danger: true, from: "Admin" }; }
}
export default class extends WorkerEntrypoint {
  async add(a, b) { return a + b; }
  async echo(payload) { return payload; }
  async caller() { return { callerNs: this.ctx.props.callerNs ?? null }; }
  async boom() { throw new Error("target-said-no"); }
  async fetch() { return new Response(JSON.stringify({ via: "default-fetch" }), {
    headers: { "content-type": "application/json" } }); }
}
`;

const DEFAULT_EXPORTS = (/** @type {string[]} */ allowedCallers) => [
  { entrypoint: "default", allowedCallers },
];

test("arbitrary-method RPC: env.X.myMethod(args) dispatches with structured args", async () => {
  const ns = uniqueNs("sb");
  await deployAndPromote(ns, "target", { code: MULTI_ENTRY_TARGET });
  await deployAndPromote(ns, "caller", {
    code: workerFetchCallerSource(`
      const a = await env.T.add(2, 3);
      const b = await env.T.echo({ nested: { list: [1, 2, 3] }, s: "hi" });
      return Response.json({ a, b });
    `),
    bindings: { T: { type: "service", service: "target" } },
  });

  const res = await gatewayFetch(ns, "/caller/");
  const body = await readIntegrationJson(res, 200, "arbitrary-method RPC");
  assert.equal(body.a, 5);
  assert.deepEqual(body.b, { nested: { list: [1, 2, 3] }, s: "hi" });
});

test("named entrypoint: binding threads through to the picked class", async () => {
  const ns = uniqueNs("sb");
  await deployAndPromote(ns, "target", {
    code: MULTI_ENTRY_TARGET,
    exports: [
      { entrypoint: "Auth", allowedCallers: [] },
      { entrypoint: "Admin", allowedCallers: [] },
    ],
  });
  await deployAndPromote(ns, "caller", {
    code: workerFetchCallerSource(`
      const authWho = await env.AUTH.whoami();
      const adminWho = await env.ADMIN.whoami();
      const tok = await env.AUTH.verifyToken("abc");
      return Response.json({ authWho, adminWho, tok });
    `),
    bindings: {
      AUTH: { type: "service", service: "target", entrypoint: "Auth" },
      ADMIN: { type: "service", service: "target", entrypoint: "Admin" },
    },
  });

  const res = await gatewayFetch(ns, "/caller/");
  const body = await readIntegrationJson(res, 200, "named entrypoint RPC");
  assert.equal(body.authWho.entrypoint, "Auth");
  assert.equal(body.adminWho.entrypoint, "Admin");
  assert.deepEqual(body.tok, { ok: true, who: "auth", token: "abc" });
});

test("named entrypoint: each concurrent RPC call receives a fresh class instance", async () => {
  const ns = uniqueNs("sbinst");
  const targetCode = `
    import { WorkerEntrypoint } from "cloudflare:workers";
    let nextInstanceId = 0;
    function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }
    export class Api extends WorkerEntrypoint {
      constructor(ctx, env) {
        super(ctx, env);
        this.instanceId = ++nextInstanceId;
        this.invocations = 0;
        this.activeRid = null;
      }
      async touch(request) {
        const rid = request.headers.get("x-request-id");
        const url = new URL(request.url);
        const slot = Number(url.searchParams.get("slot") || 0);
        const before = this.activeRid;
        this.activeRid = rid;
        this.invocations += 1;
        await sleep(40 + slot * 10);
        const after = this.activeRid;
        return { rid, before, after, instanceId: this.instanceId, invocations: this.invocations };
      }
    }
    export default { fetch() { return new Response("target"); } };
  `;
  await deployAndPromote(ns, "target", {
    code: targetCode,
    exports: [{ entrypoint: "Api", allowedCallers: ["*"] }],
  });
  await deployAndPromote(ns, "caller", {
    code: workerFetchCallerSource(`
      return Response.json(await env.API.touch(new Request(req)));
    `),
    bindings: { API: { type: "service", service: "target", entrypoint: "Api" } },
  });

  const rows = [];
  for (let round = 1; round <= 3; round += 1) {
    rows.push(...await Promise.all([0, 1, 2].map(async (slot) => {
      const rid = `rid-${round}-${slot}`;
      const res = await gatewayFetch(ns, `/caller/?round=${round}&slot=${slot}`, {
        headers: { "x-request-id": rid },
      });
      const text = await res.text();
      assert.equal(res.status, 200, text);
      return responseJson({ body: text });
    })));
  }

  const instanceIds = rows.map((row) => row.instanceId);
  assert.equal(new Set(instanceIds).size, rows.length);
  for (const row of rows) {
    assert.equal(row.before, null);
    assert.equal(row.after, row.rid);
    assert.equal(row.invocations, 1);
  }
});

test("named entrypoint: omitted entrypoint dispatches default fetch", async () => {
  const ns = uniqueNs("sb");
  await deployAndPromote(ns, "target", { code: MULTI_ENTRY_TARGET });
  await deployAndPromote(ns, "caller", {
    code: workerFetchCallerSource(`
      const r = await env.T.fetch(new Request("https://x/"));
      return Response.json(await r.json());
    `),
    bindings: { T: { type: "service", service: "target" } },
  });
  const res = await gatewayFetch(ns, "/caller/");
  const body = await readIntegrationJson(res, 200, "default fetch via service binding");
  assert.equal(body.via, "default-fetch");
});

test("default fetch via service binding reaches function-default targets without D1/R2", async () => {
  const ns = uniqueNs("sbfn");
  await deployAndPromote(ns, "target", {
    code: `
      export default function(request) {
        return new Response("target-fn:" + new URL(request.url).pathname);
      }
    `,
  });
  await deployAndPromote(ns, "caller", {
    code: workerFetchCallerSource(`
      const r = await env.T.fetch(new Request("https://target/path"));
      return new Response(await r.text());
    `),
    bindings: { T: { type: "service", service: "target" } },
  });

  const res = await gatewayFetch(ns, "/caller/");

  assert.equal(res.status, 200);
  assert.equal(await res.text(), "target-fn:/path");
});

test("RPC error: target method throws — caller sees annotated message", async () => {
  const ns = uniqueNs("sb");
  await deployAndPromote(ns, "target", { code: MULTI_ENTRY_TARGET });
  await deployAndPromote(ns, "caller", {
    code: workerFetchCallerSource(`
      await env.T.boom();
      return Response.json({ reached: "unreachable" });
    `),
    bindings: { T: { type: "service", service: "target" } },
  });
  const res = await gatewayFetch(ns, "/caller/");
  const body = await readIntegrationJson(res, 500, "RPC error response");
  assert.match(body.err, /target-said-no/);
  assert.match(body.err, /service binding/);
  assert.match(body.err, new RegExp(RegExp.escape(`${ns}:target:v1`)));
});

test("cross-ns + ACL allow: target lists caller ns → deploy + call succeed", async () => {
  const targetNs = uniqueNs("sbt");
  const callerNs = uniqueNs("sbc");
  await deployAndPromote(targetNs, "billing", {
    code: MULTI_ENTRY_TARGET,
    exports: DEFAULT_EXPORTS([callerNs]),
  });
  await deployAndPromote(callerNs, "caller", {
    code: workerFetchCallerSource(`
      const sum = await env.B.add(40, 2);
      return Response.json({ sum });
    `),
    bindings: { B: { type: "service", service: "billing", ns: targetNs } },
  });
  const res = await gatewayFetch(callerNs, "/caller/");
  const body = await readIntegrationJson(res, 200, "cross-ns ACL allow");
  assert.equal(body.sum, 42);
});

test("service binding callerNs is runtime-signed: caller cannot forge via ctx.props tampering", async () => {
  const targetNs = uniqueNs("sbt");
  const callerNs = uniqueNs("sbc");
  await deployAndPromote(targetNs, "billing", {
    code: MULTI_ENTRY_TARGET,
    exports: DEFAULT_EXPORTS([callerNs]),
  });
  await deployAndPromote(callerNs, "caller", {
    code: workerFetchCallerSource(`
      const attempts = [];
      try {
        env.B.ctx.props.callerNs = "attacker";
        attempts.push("mutate-ok");
      } catch (err) {
        attempts.push("mutate-threw:" + err.message);
      }
      try {
        Object.defineProperty(env.B.ctx, "props", { value: { callerNs: "attacker" } });
        attempts.push("defineProperty-ok");
      } catch (err) {
        attempts.push("defineProperty-threw:" + err.message);
      }
      const viaRpc = await env.B.caller();
      return Response.json({ attempts, viaRpc });
    `),
    bindings: { B: { type: "service", service: "billing", ns: targetNs } },
  });
  const res = await gatewayFetch(callerNs, "/caller/");
  const body = await readIntegrationJson(res, 200, "callerNs runtime signing");
  assert.equal(body.viaRpc.callerNs, callerNs);
  assert.notEqual(body.viaRpc.callerNs, "attacker");
});

test("cross-ns + ACL wildcard: any caller ns passes", async () => {
  const targetNs = uniqueNs("sbt");
  const callerNs = uniqueNs("sbc");
  await deployAndPromote(targetNs, "pub", {
    code: MULTI_ENTRY_TARGET,
    exports: DEFAULT_EXPORTS(["*"]),
  });
  await deployAndPromote(callerNs, "caller", {
    code: workerFetchCallerSource(`
      return Response.json(await env.P.echo({ hello: "wild" }));
    `),
    bindings: { P: { type: "service", service: "pub", ns: targetNs } },
  });
  const res = await gatewayFetch(callerNs, "/caller/");
  await assertIntegrationJson(res, 200, { hello: "wild" }, "cross-ns wildcard ACL");
});

test("cross-ns + ACL deny: target without default export → 403 at deploy", async () => {
  const targetNs = uniqueNs("sbt");
  const callerNs = uniqueNs("sbc");
  await deployAndPromote(targetNs, "priv", { code: MULTI_ENTRY_TARGET });
  const res = await adminPost(`/ns/${callerNs}/worker/caller/deploy`, {
    code: workerFetchCallerSource(`return new Response("nope");`),
    bindings: { P: { type: "service", service: "priv", ns: targetNs } },
  });
  assert.equal(res.status, 403);
  assert.match(res.json.message, /declare \[\[exports\]\] entrypoint "default"/);
  assert.match(res.json.message, new RegExp(RegExp.escape(targetNs)));
});

test("cross-ns + ACL deny: explicit list without caller → 403", async () => {
  const targetNs = uniqueNs("sbt");
  const callerNs = uniqueNs("sbc");
  await deployAndPromote(targetNs, "priv", {
    code: MULTI_ENTRY_TARGET,
    exports: DEFAULT_EXPORTS(["someone-else"]),
  });
  const res = await adminPost(`/ns/${callerNs}/worker/caller/deploy`, {
    code: workerFetchCallerSource(`return new Response("nope");`),
    bindings: { P: { type: "service", service: "priv", ns: targetNs } },
  });
  assert.equal(res.status, 403);
});

test("self-target: same ns + same name still rejected", async () => {
  const ns = uniqueNs("sb");
  await deployAndPromote(ns, "caller", { code: MULTI_ENTRY_TARGET });
  const res = await adminPost(`/ns/${ns}/worker/caller/deploy`, {
    code: workerFetchCallerSource(`return new Response("x");`),
    bindings: { T: { type: "service", service: "caller" } },
  });
  assert.equal(res.status, 400);
  assert.match(res.json.message, /cannot target self/);
});

test("self-target: cross-ns same name is legitimate", async () => {
  const nsA = uniqueNs("sba");
  const nsB = uniqueNs("sbb");
  await deployAndPromote(nsA, "twin", {
    code: MULTI_ENTRY_TARGET,
    exports: DEFAULT_EXPORTS([nsB]),
  });
  // Same worker name in a different ns must not trip the self-target guard.
  await deployAndPromote(nsB, "twin", {
    code: workerFetchCallerSource(`
      const r = await env.X.add(1, 1);
      return Response.json({ r });
    `),
    bindings: { X: { type: "service", service: "twin", ns: nsA } },
  });
  const res = await gatewayFetch(nsB, "/twin/");
  await assertIntegrationJson(res, 200, { r: 2 }, "cross-ns same-name service binding");
});

test("ACL follows version: pinned caller keeps working after target tightens ACL", async () => {
  const targetNs = uniqueNs("sbt");
  const callerNs = uniqueNs("sbc");
  await deployAndPromote(targetNs, "shared", {
    code: MULTI_ENTRY_TARGET,
    exports: DEFAULT_EXPORTS([callerNs]),
  });
  await deployAndPromote(callerNs, "caller", {
    code: workerFetchCallerSource(`return Response.json(await env.S.echo("v1"));`),
    bindings: { S: { type: "service", service: "shared", ns: targetNs } },
  });
  const first = await gatewayFetch(callerNs, "/caller/");
  assert.equal(await readIntegrationJson(first, 200, "ACL pinned caller initial"), "v1");

  await deployAndPromote(targetNs, "shared", {
    code: MULTI_ENTRY_TARGET,
    exports: DEFAULT_EXPORTS([]),
  });
  const pinned = await gatewayFetch(callerNs, "/caller/");
  assert.equal(await readIntegrationJson(pinned, 200, "ACL pinned caller after revocation"),
    "v1", "pinned caller must keep working after ACL revocation");

  const res = await adminPost(`/ns/${callerNs}/worker/caller/deploy`, {
    code: workerFetchCallerSource(`return Response.json(await env.S.echo("redeploy"));`),
    bindings: { S: { type: "service", service: "shared", ns: targetNs } },
  });
  assert.equal(res.status, 403);
});

test("cross-ns version pinning: caller stays on target v1 after target promotes v2", async () => {
  const targetNs = uniqueNs("sbt");
  const callerNs = uniqueNs("sbc");

  const V1 = `
    import { WorkerEntrypoint } from "cloudflare:workers";
    export default class extends WorkerEntrypoint {
      async tag() { return { v: 1 }; }
    }`;
  const V2 = `
    import { WorkerEntrypoint } from "cloudflare:workers";
    export default class extends WorkerEntrypoint {
      async tag() { return { v: 2 }; }
    }`;

  await deployAndPromote(targetNs, "shared", { code: V1, exports: DEFAULT_EXPORTS(["*"]) });
  await deployAndPromote(callerNs, "caller", {
    code: workerFetchCallerSource(`return Response.json(await env.S.tag());`),
    bindings: { S: { type: "service", service: "shared", ns: targetNs } },
  });
  const first = await readIntegrationJson(await gatewayFetch(callerNs, "/caller/"), 200, "cross-ns pin v1");
  assert.equal(first.v, 1);

  await deployAndPromote(targetNs, "shared", { code: V2, exports: DEFAULT_EXPORTS(["*"]) });
  const afterPromote = await readIntegrationJson(
    await gatewayFetch(callerNs, "/caller/"),
    200,
    "cross-ns pin after target promote"
  );
  assert.equal(afterPromote.v, 1, "cross-ns caller must stay pinned to v1");

  await deployAndPromote(callerNs, "caller", {
    code: workerFetchCallerSource(`return Response.json(await env.S.tag());`),
    bindings: { S: { type: "service", service: "shared", ns: targetNs } },
  });
  const refreshed = await readIntegrationJson(
    await gatewayFetch(callerNs, "/caller/"),
    200,
    "cross-ns pin after caller redeploy"
  );
  assert.equal(refreshed.v, 2);
});

test("deploy: entrypoint with non-identifier rejected 400", async () => {
  const ns = uniqueNs("sb");
  await deployAndPromote(ns, "target", { code: MULTI_ENTRY_TARGET });
  const res = await adminPost(`/ns/${ns}/worker/caller/deploy`, {
    code: workerFetchCallerSource(`return new Response("x");`),
    bindings: { T: { type: "service", service: "target", entrypoint: "1Bad" } },
  });
  assert.equal(res.status, 400);
  assert.match(res.json.message, /entrypoint must be a JS identifier/);
});

test("deploy: ns with invalid shape rejected 400", async () => {
  const ns = uniqueNs("sb");
  const res = await adminPost(`/ns/${ns}/worker/caller/deploy`, {
    code: workerFetchCallerSource(`return new Response("x");`),
    bindings: { T: { type: "service", service: "t", ns: "UPPER_NS" } },
  });
  assert.equal(res.status, 400);
  assert.match(res.json.message, /ns must match/);
});

test("exports strict mode: binding non-listed entrypoint → 400 'not exported'", async () => {
  const targetNs = uniqueNs("sbt");
  const callerNs = uniqueNs("sbc");
  await deployAndPromote(targetNs, "auth", {
    code: MULTI_ENTRY_TARGET,
    exports: [{ entrypoint: "Auth", allowedCallers: ["*"] }],
  });
  const res = await adminPost(`/ns/${callerNs}/worker/caller/deploy`, {
    code: workerFetchCallerSource(`return new Response("x");`),
    bindings: {
      X: { type: "service", service: "auth", ns: targetNs, entrypoint: "Admin" },
    },
  });
  assert.equal(res.status, 400);
  assert.match(res.json.message, /not exported/);
});

test("exports strict mode: per-entrypoint allowed_callers controls each entrypoint", async () => {
  const targetNs = uniqueNs("sbt");
  const callerNs = uniqueNs("sbc");
  await deployAndPromote(targetNs, "multi", {
    code: MULTI_ENTRY_TARGET,
    exports: [
      { entrypoint: "Auth", allowedCallers: ["*"] },
      { entrypoint: "Admin", allowedCallers: ["operator-ns"] },
    ],
  });
  await deployAndPromote(callerNs, "caller", {
    code: workerFetchCallerSource(`return Response.json(await env.A.whoami());`),
    bindings: {
      A: { type: "service", service: "multi", ns: targetNs, entrypoint: "Auth" },
    },
  });
  const res = await gatewayFetch(callerNs, "/caller/");
  assert.equal((await readIntegrationJson(res, 200, "exports strict auth entrypoint")).entrypoint, "Auth");

  const denied = await adminPost(`/ns/${callerNs}/worker/denied/deploy`, {
    code: workerFetchCallerSource(`return new Response("x");`),
    bindings: {
      B: { type: "service", service: "multi", ns: targetNs, entrypoint: "Admin" },
    },
  });
  assert.equal(denied.status, 403);
});

test("exports strict mode: same-ns caller bypasses allowed_callers but still fails visibility for unlisted entrypoints", async () => {
  const ns = uniqueNs("sb");
  await deployAndPromote(ns, "target", {
    code: MULTI_ENTRY_TARGET,
    // Empty list rejects cross-ns but same-ns still bypasses at ACL layer.
    exports: [{ entrypoint: "Auth", allowedCallers: [] }],
  });
  await deployAndPromote(ns, "caller", {
    code: workerFetchCallerSource(`return Response.json(await env.A.whoami());`),
    bindings: { A: { type: "service", service: "target", entrypoint: "Auth" } },
  });
  const res = await gatewayFetch(ns, "/caller/");
  assert.equal((await readIntegrationJson(res, 200, "same-ns auth entrypoint")).entrypoint, "Auth");

  // Visibility check (exports-strict) applies to every caller including same-ns.
  const denied = await adminPost(`/ns/${ns}/worker/denied/deploy`, {
    code: workerFetchCallerSource(`return new Response("x");`),
    bindings: { B: { type: "service", service: "target", entrypoint: "Admin" } },
  });
  assert.equal(denied.status, 400);
  assert.match(denied.json.message, /not exported/);
});

// Multi-hop chain (A → B → C): regression anchors for the in-place
// annotation contract in runtime/bindings/service.js combined with the
// enhanced_error_serialization platform floor. Behavior the chain tests
// lock:
//   - Error .name survives JSRPC (TypeError stays TypeError).
//   - Own properties (.code / .status) survive top-level, not on .cause.
//   - No .cause wrapper introduced at any hop.
//   - __sbAnnotated dedup is ASYMMETRIC: Error → host-object path keeps
//     the non-enumerable marker (1 tag across hops); plain-obj-with-
//     message → default structured clone drops it (N tags across hops).
//   - Plain object without string .message skips the guard entirely.
const CHAIN_INNER_SRC = `
  import { WorkerEntrypoint } from "cloudflare:workers";
  export default class extends WorkerEntrypoint {
    async boom() {
      const err = new TypeError("from-inner");
      err.code = "E_INNER";
      err.status = 418;
      throw err;
    }
    async boomPlainNoMessage() {
      // eslint-disable-next-line no-throw-literal
      throw { code: "E_PLAIN", detail: "plain-obj" };
    }
    async boomPlainWithMessage() {
      // eslint-disable-next-line no-throw-literal
      throw { message: "from-inner-plain", code: "E_PLAIN_MSG", detail: "plain-msg" };
    }
  }
`;

const CHAIN_MID_SRC = `
  import { WorkerEntrypoint } from "cloudflare:workers";
  export default class extends WorkerEntrypoint {
    async relay() { return await this.env.INNER.boom(); }
    async relayPlainNoMessage() { return await this.env.INNER.boomPlainNoMessage(); }
    async relayPlainWithMessage() { return await this.env.INNER.boomPlainWithMessage(); }
  }
`;

const CHAIN_OUTER_SRC = `
  export default {
    async fetch(req, env) {
      const mode = new URL(req.url).searchParams.get("mode") ?? "typed";
      try {
        if (mode === "plain-no-msg") await env.MID.relayPlainNoMessage();
        else if (mode === "plain-msg") await env.MID.relayPlainWithMessage();
        else await env.MID.relay();
        return Response.json({ reached: "unreachable" }, { status: 500 });
      } catch (err) {
        return Response.json({
          name: err && err.name,
          message: err && err.message,
          code: err && err.code,
          status: err && err.status,
          detail: err && err.detail,
          isError: err instanceof Error,
          hasCause: !!(err && err.cause),
        });
      }
    },
  };
`;

/** @param {string} ns */
async function setupErrorChain(ns) {
  await deployAndPromote(ns, "inner", { code: CHAIN_INNER_SRC });
  await deployAndPromote(ns, "mid", {
    code: CHAIN_MID_SRC,
    bindings: { INNER: { type: "service", service: "inner" } },
  });
  await deployAndPromote(ns, "caller", {
    code: CHAIN_OUTER_SRC,
    bindings: { MID: { type: "service", service: "mid" } },
  });
}

test("service binding chain: two hops preserve Error subclass name (regression anchor vs wrap-with-cause)", async () => {
  const ns = uniqueNs("sbc");
  await setupErrorChain(ns);
  const body = await readIntegrationJson(await gatewayFetch(ns, "/caller/"), 200, "service binding chain typed error");
  assert.equal(body.name, "TypeError",
    "Error subclass name must survive through JSRPC + service binding annotation");
});

test("service binding chain: custom error fields stay at top level through two hops", async () => {
  const ns = uniqueNs("sbc");
  await setupErrorChain(ns);
  const body = await readIntegrationJson(await gatewayFetch(ns, "/caller/"), 200, "service binding chain custom fields");
  assert.equal(body.code, "E_INNER",
    "custom .code must stay at err.code, not migrate to err.cause.code");
  assert.equal(body.status, 418);
});

test("service binding chain: no .cause wrapper introduced at either hop", async () => {
  const ns = uniqueNs("sbc");
  await setupErrorChain(ns);
  const body = await readIntegrationJson(await gatewayFetch(ns, "/caller/"), 200, "service binding chain cause check");
  assert.equal(body.hasCause, false,
    "service binding must annotate in-place, not wrap with { cause }");
});

test("service binding chain: __sbAnnotated dedup keeps the tag anchored at the throw site (one tag, not one-per-hop)", async () => {
  const ns = uniqueNs("sbc");
  await setupErrorChain(ns);
  const body = await readIntegrationJson(await gatewayFetch(ns, "/caller/"), 200, "service binding chain dedup");
  assert.match(body.message, /from-inner/);
  const tags = body.message.match(/\[service binding /g) ?? [];
  // Revisit if __sbAnnotated is ever made enumerable or removed.
  assert.equal(tags.length, 1,
    `dedup must anchor the tag at the throw site, got: ${JSON.stringify(body.message)}`);
  assert.match(body.message, new RegExp(RegExp.escape(`${ns}:inner:v1`)));
});

test("service binding chain: plain-object throw WITHOUT string .message reaches caller untouched", async () => {
  const ns = uniqueNs("sbc");
  await setupErrorChain(ns);
  const body = await readIntegrationJson(
    await gatewayFetch(ns, "/caller/?mode=plain-no-msg"),
    200,
    "service binding chain plain object without message"
  );
  assert.equal(body.code, "E_PLAIN",
    `plain-object throw must deliver .code to caller; got ${JSON.stringify(body)}`);
  assert.equal(body.detail, "plain-obj");
  const msg = body.message ?? "";
  assert.ok(!/\[service binding /.test(msg),
    `plain-object w/o message must not be annotated; got message: ${JSON.stringify(msg)}`);
});

test("service binding chain: plain-object throw WITH string .message is annotated in place (fields preserved, dedup asymmetric)", async () => {
  const ns = uniqueNs("sbc");
  await setupErrorChain(ns);
  const body = await readIntegrationJson(
    await gatewayFetch(ns, "/caller/?mode=plain-msg"),
    200,
    "service binding chain plain object with message"
  );
  assert.equal(body.code, "E_PLAIN_MSG",
    `.code must survive in place; got ${JSON.stringify(body)}`);
  assert.equal(body.detail, "plain-msg");
  assert.equal(body.hasCause, false, "must not introduce .cause wrapper");
  assert.match(body.message, /from-inner-plain/);
  const tags = body.message.match(/\[service binding /g) ?? [];
  assert.equal(tags.length, 2,
    `plain-object dedup doesn't carry across hops (non-enumerable __sbAnnotated dropped by default structured clone); got: ${JSON.stringify(body.message)}`);
});
