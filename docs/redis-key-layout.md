# Redis Key Layout

This document is the active cross-module Redis / Valkey key map. Module docs own the
deep semantics for each feature; this file records the DB split, cross-cutting key
families, and ownership rules that span modules.

## Database Split

WDL uses a deliberate logical split:

- **`DB 0`, control plane:** bundles, routes/patterns, auth, D1/DO owner state, cron
  config, queue-consumer config, lifecycle metadata, and workflow definitions
  (`wf:defs:*`).
- **`DB 1`, data plane:** KV hash buckets, queue streams, delayed queues, orphan streams,
  and live log-tail streams.
- **`DB 2`, workflows:** `wf:schema_version`, instance state, step records/summaries,
  ready/due shards, events and event-type indexes, payload refs, retention indexes,
  restart target-version blockers, and run leases.

Local compose, Kubernetes, and Terraform enable this split. Rust services and the
Rust `redis-proxy` use `DATA_REDIS_URL` / `DATA_REDIS_DB` to select the
data-plane Redis connection/database; embedded JS control/log-tail paths use
`DATA_REDIS_ADDR` plus `DATA_REDIS_DB` because their RESP client accepts a
host:port address. Deployments that omit those data-plane variables keep
data-plane keys on the control Redis connection/database until they opt in.
Workflows is different: when `WORKFLOWS_REDIS_URL` is omitted, the workflows
service still defaults to DB 2; it uses DB 0 only when `WORKFLOWS_REDIS_DB=0` is
set explicitly.

## Global Control Keys

```text
routes:<ns>                     Hash, { workerName -> activeVersion }
namespaces                      Set, namespaces with at least one active worker
workers:<ns>                    Set, worker names with worker-owned lifecycle state
worker:<ns>:<name>:next_version String, monotonic version counter, survives delete
cron:seq:<ns>:<name>            String, permanent Cron generation high-water mark
worker-versions:<ns>:<name>     ZSET, score=int version, member="v<int>"
worker:<ns>:<name>:v:<int>      Hash, bundle bytes plus __meta__
worker-delete-lock:<ns>:<name>  String EX 30, per-worker delete critical-section lock;
                               value is whole:<token> or version:<token>; DO first-owner
                               claim WATCHes it and only whole fences ownership
worker-version-referrers:<ns>:<name>:<version>
                                Set, canonical JSON version-pinned caller refs
hosts:<ns>                      Set, declared operator host intent
declared-hosts                  Set, hosts declared by at least one namespace
declared-hosts:revision         String, monotonic host-declaration mutation revision
host-declarations:<host>        Set, namespaces declaring this host
ns-hosts:<ns>                   Set, active host reverse index maintained by promote
patterns:<host>                 Hash, slot -> v2 tab-separated projection
auth:hash:<sha256_hex>          String, token id lookup for presented plaintext token
auth:token:<tokenId>            Hash, token metadata plus SHA-256 hash, no plaintext
auth:delegated-issue-lock:<issuerTokenId>:<templateId>
                                  String EX, delegated-token issuer/template issue lock
secrets:<ns>                    Hash, namespace-level WDL-ENC envelopes
secrets:<ns>:<worker>           Hash, worker-level WDL-ENC envelopes
```

`worker:<ns>:<name>:v:<int>` uses a positive JavaScript-safe integer version in the
key, not the `"v<int>"` tag. Test fixtures that seed Redis directly must use
`shared/worker-contract.js#bundleKey`.

`cron:seq:<ns>:<name>` is Control's permanent Cron generation allocator. It survives
an empty Cron projection and whole-worker deletion so stale `cron-slot:*` refs cannot
match a recreated entry. Allocations start at generation `1024`; lower values are
reserved and never issued by the permanent allocator.

`namespaces` is an active worker gate. It is populated when a namespace has an active
worker route and may be removed when the last active worker is deleted.
Namespace-level resources such as secrets and data-plane state can outlive membership
in this set. Auth reads this set during delegated token issue only as a best-effort
generated-namespace collision signal, not as a permanent namespace registry.

`routes:<ns>` and `worker-versions:<ns>:<name>` are constructed only through
`shared/worker-contract.js#routesKey` / `#workerVersionsKey` (and their Rust mirror
`rust/common/src/worker_contract.rs#routes_key` / `#worker_versions_key`). Control is the
sole writer; sanctioned readers are gateway and workflows. Gateway reads it for
route resolution. Workflows reads it for active export resolution during workflow
create / verify, and for internal DO alarm retargeting when a fired alarm's
scheduled version is no longer retained. A key-grammar change must update the JS
helper, the Rust helper, and every reader together.

`workers:<ns>` means the worker has worker-owned lifecycle state: retained bundle,
active projection, worker-level secrets, or workflow definitions. Secret-only and
definitions-only workers are intentionally listed and whole-deletable.

## Route And Host Projection

Subdomain routing reads `routes:<ns>`. Pattern routing first checks `declared-hosts`,
then reads `patterns:<host>` and uses the slot value's embedded `version` to construct
`x-worker-id` without consulting `routes:<ns>`. Pattern slot values are compact
`v2\t<ns>\t<worker>\t<version>\t<kind>\t<value>` records encoded by
`shared/route-projection.js`, not JSON. Promote updates both projections in the same
Redis transaction. Control mutation and delete paths fail closed on a nonempty slot that
cannot be decoded; they do not treat an unknown owner as an empty slot.

`hosts:<ns>` is operator intent: the namespace is allowed to use those hosts.
`declared-hosts` is a gateway gate for hosts declared by at least one namespace.
`host-declarations:<host>` records the declaring namespaces so one namespace removing a
host does not clear the global gate while another namespace still declares it. Host
reconcile changes the source and derived declaration indexes together and increments
`declared-hosts:revision` in the same transaction. `POST /reload` watches that revision
while rebuilding the two declaration indexes from `hosts:<ns>`, so a concurrent host
reconcile retries the repair before gateway cache invalidation is published.
`ns-hosts:<ns>` is the active reverse index: the namespace currently owns at least one
slot on those hosts. `hosts:<ns>` is expected to be a superset. Host reconcile uses
`ns-hosts:<ns>` as a fast path before scanning `patterns:<host>`.

Pattern `slot` is the original wrangler pattern, such as `/mcp` or `/mcp/*`; it is the
Redis hash field. `kind` is `exact` or `prefix` and drives gateway matching semantics.

## Bundle Metadata

The `__meta__` field is small JSON metadata. Module bytes remain raw RESP-safe bytes,
not base64. Typical fields include:

```json
{
  "mainModule": "worker.js",
  "compatibilityDate": "2026-04-24",
  "compatibilityFlags": [],
  "modules": { "worker.js": { "type": "module" } },
  "bindings": {},
  "vars": {},
  "routes": [],
  "crons": [],
  "queueConsumers": [],
  "assets": { "token": "...", "prefix": "assets/<ns>/<worker>/<token>/" },
  "exports": []
}
```

Control writes `__meta__` as a JSON object. Control-plane consumers parse required
bundle metadata through `control/lib.js::parseBundleMeta()`; malformed JSON, arrays,
and scalar values fail closed as `corrupt_meta`. Absence remains use-site-specific.
Paths that need metadata to compute a correct projection change, uniqueness proof,
lifecycle cleanup, workflow view, or environment budget fail closed while their
authoritative route or index still names the bundle. Deploy discovery/link preflight
does not classify absence as `corrupt_meta`; the watched commit remains authoritative
and rejects a missing pinned service target as `target_drift`.

Routes, crons, queue consumers, bindings, vars, exports, workflow definitions, and asset
prefixes are version metadata. Rollback is a promote of an older immutable version.

## Feature Key Families

Feature modules own the detailed contracts:

- D1: [D1](modules/d1.md)
- Durable Objects: [Durable Objects](modules/durable-objects.md)
- Queues and cron: [Queues and Cron](modules/queues-cron.md)
- Workflows: [Workflows](modules/workflows.md)
- Log tail: [Log Tail And Observability](modules/log-tail-observability.md)
- Runtime/KV/R2/ASSETS/service/platform bindings: [Runtime](modules/runtime.md)
- Control/auth/lifecycle/delete blockers: [Control And Auth](modules/control-auth.md)

Cross-cutting constraints:

- Persisted D1/DO owner records must reconstruct the encoded scope of the Redis key
  under which they are read. A syntactically valid record stored under another scope
  fails closed before forwarding, takeover, renewal, or release. DO owner resolution
  also binds the record's canonical namespace and worker to the invoking bundle before
  reading that bundle's active storage pointer.
- Indexes are usually repairable projections, not authority. The module doc must state
  which key is authoritative before adding a writer.
- Lifecycle and delete blocker indexes are authoritative where the module says they are;
  do not add request-path fallback scans that bypass those indexes.
- Queue main streams are not trimmed because at-least-once delivery is the contract.
  Diagnostic streams such as DLQ, orphan, and log-tail streams may use bounded
  approximate trim.
- Secret hash values are `WDL-ENC:` envelopes in steady state. There is no plaintext
  fallback on `/runtime/load`.
- Workflows owns DB 2 instance state. `wf:ready:cursor` is the internal ready-shard
  fairness cursor. Control owns only DB 0 `wf:defs:*`; other tiers must not write DB 2
  directly.
- `wf:pending-version:<ns>:<worker>:<version>` is a Workflows-owned, 30-second restart
  blocker. Version-delete checks it with `wf:by-version`, and the successful-restart
  DB 2 script atomically revalidates the initial marker before replacing it with the
  durable version referrer. The ZSET key has a refreshed 60-second TTL so abandoned
  marker keys are physically reclaimed.
- Workflows also owns internal DB 2 `wf:internal:do-alarm:*` jobs for Durable Object
  alarm backend scheduling. do-runtime writes alarms through the workflows HTTP API
  instead of writing those keys directly. `wf:internal:do-alarm:ready:cursor` is the
  internal ready-shard fairness cursor, not tenant state.
