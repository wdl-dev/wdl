# Control And Auth

## Purpose

Control is the static control-plane worker that owns deploy, promote, lifecycle, secret,
route, D1, R2, workflow, log-tail, and auth-token APIs. Auth is a static JSRPC worker
that verifies and issues scoped admin tokens.

## Current Implementation

Control runs inside system-runtime on `:8082` and is reached through gateway's
admin-host branch. It is not dynamically loaded by workerLoader.

Main files:

- `control/index.js`: request dispatcher.
- `control/lib.js`: pure route/key utilities, bundle metadata parsing, and referrer
  shaping.
- `control/shared.js`: auth wrapper, Redis singletons, publish helpers, state-bound
  workflow transport wiring, and lifecycle helpers.
- `control/errors.js`, `control/json-body.js`, `control/optimistic.js`: pure Control
  response and request-body contracts plus the Control adapter over the shared
  optimistic retry loop.
- `control/workflows-client.js`: canonical internal Workflows POST transport with
  explicit caller-owned timeout selection.
- `control/handlers/*`: endpoint handlers.
- `control/routing.js`: promote/reconcile WATCH/MULTI logic.
- `auth/index.js`, `auth/lib.js`, `shared/auth-roles.js`: token store, role table,
  authorization evaluation.

## Interfaces

- Admin-host HTTP APIs use `x-admin-token`.
- Control calls `env.AUTH.verify({ token, action, ns, requestId })`.
- Auth exposes JSRPC methods: verify, issue, list, revoke.
- CLI uses the control URL; it does not talk directly to Redis or AWS for
  ordinary deploys.

## Admin API Surface

All client-facing endpoints are reached through the control URL, which gateway
matches through its admin-host branch, and require `X-Admin-Token`. There is no
separate public/admin-facing port; clients do not call the internal system-runtime
`:8082` control socket directly. Ordinary JSON errors are `{ "error":
"<machine-code>", "message": "<human text>" }`; extra fields are additive, and
clients should branch on `error`. `message` is a safe human summary, not a stable
reason taxonomy; auth-specific reject reasons surface as `error`.

Worker lifecycle:

| Method | Path | Contract |
|---|---|---|
| `GET` | `/ns/<ns>/workers` | Lists workers with namespace-owned state, including deploy-only, active, secret-only, and workflow-definitions-only workers. Each result reports `hasSecrets` and `hasWorkflowDefs`. |
| `GET` | `/ns/<ns>/worker/<name>/versions` | Lists retained versions and active status. |
| `POST` | `/ns/<ns>/worker/<name>/deploy` | Creates a new immutable version from shorthand code or full module manifest; routes, crons, queue consumers, service refs, platform refs, assets, vars, bindings, and `exports` are version metadata. Python modules and upstream experimental compatibility flags are rejected before commit. |
| `POST` | `/ns/<ns>/worker/<name>/promote` | Promotes `{"version":"vN"}` through the WATCH/MULTI routing path. Host declaration failures are 403; live pattern conflicts are 409; exhausted transaction contention is 503. |
| `DELETE` | `/ns/<ns>/worker/<name>/versions/<version>` | Deletes one retained non-active version after active-route, service-ref, lifecycle, and delete-lock blockers pass. Referrer redaction is principal-aware. |
| `POST` | `/ns/<ns>/worker/<name>/delete` | Whole-worker delete. `?dry_run=1` returns computed impact and blockers without writing. Redaction matches single-version delete. |

Host, secret, data, and auth operations:

| Method | Path | Contract |
|---|---|---|
| `GET` / `POST` | `/ns/<ns>/hosts` | Lists or reconciles declared hosts. Reconcile normalizes hosts, rejects platform-domain hosts, and returns 409 when removing a host with live owned patterns. |
| `POST` | `/reload` | Ops-only route resync: rebuilds the declared-host gate from `hosts:<ns>` under the host-declaration revision fence, then publishes `routes:flush ""` and `patterns:invalidate "*"`. The repair incrementally bounds key scans and preflights set cardinalities, rejecting more than 10,000 combined source keys, declaration members, and stale reverse-index keys before member materialization or mutation. |
| `GET` | `/ns/<ns>/worker/<name>/secrets` | Lists worker-level secret keys only; there is no API that reads secret values back. |
| `PUT` / `DELETE` | `/ns/<ns>/worker/<name>/secrets/<KEY>` | Mutates one worker-level secret. PUT stores a `WDL-ENC:` envelope; active workers are bumped and promoted to force fresh cold-loads. |
| `GET` | `/ns/<ns>/secrets` | Lists namespace-level secret keys only; there is no API that reads secret values back. |
| `PUT` / `DELETE` | `/ns/<ns>/secrets/<KEY>` | Mutates one namespace-level secret. No version bump; changes take effect on the next natural cold-load. |
| `GET` / `POST` / `DELETE` | `/ns/<ns>/d1/databases[/<databaseRef>]` | Lists, creates, or deletes D1 databases. Create flips provisional metadata ready after d1-runtime initialization; delete tombstones and best-effort releases owner lease. |
| `POST` | `/ns/<ns>/d1/databases/<databaseRef>/query` | Operator SQL execute path used by `wdl d1 execute`. |
| `GET` / `POST` | `/ns/<ns>/d1/databases/<databaseRef>/migrations[...]` | Migration list/status/apply. Apply is forward-only under an advisory Redis UX lock; owner serialization and SQLite transactions remain the correctness boundary. |
| `GET` | `/ns/<ns>/r2/buckets` | Lists virtual buckets with objects under `r2/<ns>/`; empty declared buckets are invisible until first object write. |
| `GET` | `/ns/<ns>/r2/buckets/<bucket>/objects` | Lists objects with prefix/delimiter/limit/cursor. |
| `HEAD` / `GET` / `DELETE` | `/ns/<ns>/r2/buckets/<bucket>/objects/<key>` | Reads metadata, streams bytes, or deletes one object. `.` / `..` path segments are rejected; delete is one idempotent S3 DELETE with no existence claim. Missing-object `HEAD` returns an empty 404 by HTTP `HEAD` semantics; clients must use status, not a JSON body, on that path. |
| `GET` | `/ns/<ns>/logs/tail?worker=<name>[&worker=<name>...]` | SSE live-tail session. First connect starts at stream tail. Single-worker reconnect can resume with `Last-Event-ID` or `?since`; multi-worker sessions fresh-start. |
| `GET` | `/whoami` | Validates the current token and returns only the authenticated principal, token id, request id, WDL platform version, minimum supported CLI version, and public URL hints. It is self-introspection, not token lifecycle management, and never returns token plaintext, hashes, other token records, or raw workerd version. |
| `POST` / `GET` / `DELETE` | `/auth/tokens[...]` | Ops-only token issue/list/revoke. `kind="ops"` is rejected because ops is bootstrap-only; token plaintext is returned once; `bootstrap` is protected and rotated by env update plus redeploy. |
| `POST` | `/auth/delegated-tokens` | Narrow delegated token issue for `token-issuer` credentials. The request names a server-side template; Auth, not Control or the caller, computes the target `kind`, generated namespace, label, expiry, active quota, and response metadata. |

## Control Operation Models

Control lifecycle operations are split so each critical transition has one authority:

- Deploy parses the supported Wrangler/JSONC shape, validates bindings and routes,
  allocates the next immutable version through `worker:<ns>:<worker>:next_version`,
  writes bundle metadata/modules/assets, then enters the same promote path used by
  explicit promotion. Before allocation, deploy estimates final WorkerCode under
  workerd's 64 MiB limit, including runtime/do-runtime-injected wrapper/client modules
  and workflow import rewrites. The watched commit path is the authoritative code-budget
  and headroomed `workerLoader` env-budget check after version allocation and metadata
  materialization, such as resolved D1 database ids and workflow keys, before writing
  the version.
- Promote is the only active-route flip. It WATCHes the delete lock, bundle metadata, D1
  refs, service-binding target refs, queue consumer keys, host declarations, and pattern
  keys needed for the candidate. The EXEC updates active routes, host reverse indexes,
  cron/queue projections, lifecycle indexes, and invalidation publications as one
  reviewed transition.
- Secret update/delete stages the secret-store mutation inside
  `bumpActiveAndPromote()` when an active route exists, so the budget check, secret
  hash write, bundle copy, and route flip share one WATCH/MULTI transaction. If no
  active route exists, retained versions are budget-checked before the direct secret
  hash write; only secret-only workers with no retained versions defer their first
  load-time budget check to deploy. Secret PUT validates the plaintext size and shape,
  encrypts it into a `WDL-ENC:` envelope before the Redis mutation/WATCH retry loop,
  and reuses the same envelope across retries. Envelope JSON uses the fixed
  `v,alg,kid,edek,iv,ct,tag` field order and each base64 field is canonical; Control and
  redis-proxy both reject non-canonical persisted forms so malformed direct Redis writes
  fail closed. Runtime therefore sees a new immutable
  version id instead of mutable in-place secret changes.
  Secret DELETE removes the target field from the env estimate before decrypting the
  remaining secret hashes, so deleting the corrupt target can still succeed. Any corrupt
  remaining namespace or worker secret fails closed; direct Redis repair is not a
  supported consistency path. Namespace-secret mutations WATCH the retained
  worker/version metadata they need to re-estimate before commit; if concurrent metadata
  changes keep invalidating that view, control returns
  `namespace_secret_mutation_contention`.
- Version delete and whole-worker delete are fail-closed. They collect blockers from
  active routes, retained versions, service refs, D1 refs, workflow lifecycle checks,
  queue/cron projections, and delete locks before committing Redis lifecycle deletion.
  S3 object cleanup is enqueued only after Redis commit succeeds.
- Worker delete lock values are a `whole:` or `version:` operation kind followed by a
  server-generated random token; `s3cleanup:<id>` task ids are also server-generated.
  `x-request-id` is diagnostic only and may be reused by clients or retries; it must not
  become a lock token or cleanup-task id. Delete, D1 migration, and delegated issue locks
  use the token-fenced primitive in `shared/redis-lock.js`; release is token-scoped and
  best-effort because TTL expiry bounds a leaked advisory lock and a release error must
  not replace the operation's real result. Before a final lifecycle mutation, version
  and whole-worker delete refresh the token once, then verify it inside the final WATCH
  snapshot so an expired request cannot commit under a replacement holder's lock.
- Auth is not a middleware convention. `parseControlRoute()` assigns an action, control
  sends that action and namespace to auth, and auth evaluates the stored token record
  against `shared/auth-roles.js`. Dispatcher code should not infer permissions from URL
  prefixes on its own.
- Delegated token issue is intentionally outside the direct `auth.token.*` lifecycle.
  `POST /auth/delegated-tokens` uses action `auth.delegated_token.issue`, so
  `/auth/tokens` issue/list/revoke remain strict ops-only while `token-issuer`
  credentials can only ask Auth to materialize one configured template.
- `/whoami` is the only namespace-less non-ops action. It is allowed for any valid token
  because it reports only the current token's own principal, token id, request id, and
  public diagnostics. `platformVersion` is the WDL version derived from the bundled
  workerd date version in the `wdl.` namespace, for example `workerd` `1.20260531.1`
  becomes `wdl.20260531.1`; it is not the project release tag, whose final counter can
  advance for WDL-only patches on the same workerd date. The raw workerd version is
  not returned. `minCliVersion` is the minimum supported downstream CLI version.
  `urls.control` is the public origin that reached control. When ingress supplies a
  single `x-forwarded-proto` value of `http` or `https`, `/whoami` uses that protocol
  for `urls.control` and `urls.namespace`; otherwise it falls back to the request URL
  protocol seen by control. `urls.namespace` is returned only for tenant namespace
  tokens when `PLATFORM_DOMAIN` is explicitly configured. The gate is grammar-only —
  any valid DNS hostname passes and publicness is the operator's responsibility;
  Control never advertises the unset `workers.local` fallback as a public URL.
  `urls.assets` is returned only when `ASSETS_CDN_BASE` is a safe absolute `http`/`https`
  URL, with query and fragment stripped. The endpoint must not grow into token list,
  token lookup, or secret-bearing diagnostics.
- A route shape has an `action` only when method, length, and verb exactly match an
  authorized shape. Missing action is deliberate: non-ops hit the auth unknown-action
  red line, while ops can still reach dispatcher/handler path and method validation
  instead of being rejected by auth. Known top-level route wrong-method cases such as
  `/reload` return `405 method_not_allowed` after ops auth succeeds.

## Redis / Storage Contracts

Control owns DB 0 metadata:

- Bundles and versions.
- Active routes and host patterns.
- Worker lifecycle indexes.
- Secret hashes.
- D1/DO metadata and referrer indexes.
- Cron and queue consumer projections.
- Workflow definition key allocation (`wf:defs:*`), while workflow instance state lives
  in DB 2.

Control uses WATCH/MULTI/EXEC for active-version flips, routing changes, delete locks,
and lifecycle index updates. Worker lifecycle indexes are authoritative; handlers should
not add fallback scans of bundle state.

Secret mutation validates env-var grammar, runtime-reserved names, and reserved
`Object.prototype` keys before persistence so every accepted key can be materialized by
the runtime env builder.

Auth stores token records in Redis and evaluates against `shared/auth-roles.js`.

Key families:

| Key | Type | Owner | Authority | Cleanup/delete semantics |
|---|---|---|---|---|
| `namespaces` | Set | Control | Active namespace gate for gateway/control listings and a best-effort delegated-token collision signal. | Updated when worker lifecycle state changes; whole-worker delete removes the namespace when no active worker remains. Namespace-scoped resources can still outlive this membership. |
| `workers:<ns>` | Set | Control | Worker lifecycle index. | Whole-worker delete removes the member after blockers pass. |
| `worker-versions:<ns>:<worker>` | ZSET | Control | Retained version index. | Version delete removes members after referrer checks. |
| `worker:<ns>:<worker>:v:<n>` | Hash | Control | Immutable bundle/version metadata and modules. | Retained until version/worker delete. |
| `worker:<ns>:<worker>:next_version` | String counter | Control | Monotonic next version number for a logical worker name. | Survives whole-worker delete so worker ids never recycle. |
| `cron:seq:<ns>:<worker>` | String counter | Control | Permanent Cron generation high-water mark. | Survives empty Cron projections and whole-worker delete so stale slot refs never match recreated entries. |
| `routes:<ns>` | Hash | Control | Active worker -> version route map. | Promote/delete updates and publishes route invalidation. |
| `hosts:<ns>` | Set | Control | Declared host allow-list for a namespace. | Promote checks membership; host reconcile updates the declared set. |
| `declared-hosts` | Set | Control | Global gateway gate for hosts declared by at least one namespace. | Host reconcile owns ordinary writes; `/reload` repairs it from `hosts:<ns>`. |
| `declared-hosts:revision` | String counter | Control | Optimistic fence for declared-host repair. | Host reconcile increments it in the same transaction as declaration changes; `/reload` retries if it changes during rebuild. |
| `host-declarations:<host>` | Set | Control | Namespaces that currently declare a host. | Prevents one namespace removal from clearing the global gate for another namespace. |
| `ns-hosts:<ns>` | Set | Control | Active host reverse index for a namespace. | Promote/reconcile maintains SADD/SREM deltas in the same EXEC. |
| `patterns:<host>` | Hash | Control | Pattern-host route slots; values are compact `v2` tab-separated projections. | Reconcile/promote updates and publishes pattern invalidation. |
| `worker-version-referrers:<ns>:<worker>:<version>` | Set | Control | Rebuildable service-binding referrer index. | Blocks version delete while callers reference the version. |
| `worker-delete-lock:<ns>:<worker>` | String EX | Control | Per-worker delete critical-section lock; value is `whole:<token>` or `version:<token>`. | Expires automatically; execute delete releases by completion. Only `whole` fences new DO ownership. |
| `secrets:<ns>`, `secrets:<ns>:<worker>` | Hash | Control | Namespace and worker secret stores; values are `WDL-ENC:` envelopes. Control encrypts writes; redis-proxy decrypts only during `/runtime/load`. | Deleted by secret lifecycle or worker delete. |
| `queue:__system__:worker-delete-s3-cleanup:s` | DB 1 Stream | Control/s3-cleanup worker | Best-effort post-commit object cleanup queue; logical queue name is `worker-delete-s3-cleanup`. | Enqueued only after Redis delete commit succeeds; enqueue failure returns `cleanup_queue_failed` warning. |
| `auth:token:<tokenId>` | Hash | Auth | Authoritative token record. | Revoke/expiry delete active record and write tombstone fields. |
| `auth:hash:<sha256>` | String | Auth | Plaintext-token hash -> token id lookup. | Deleted on revoke/expiry. |

Auth-specific contract:

- `shared/auth-roles.js` is the capability table; do not infer permissions from route
  names.
- The verify hot path is token hash lookup, token record shape validation, then
  `evaluateAccess({ action, ns, kind, principalNs })`. Record validation must prove that
  the stored `kind` exists in `ROLES`, tenant-bound tokens carry a tenant namespace,
  platform-tier tokens carry a platform-tier reserved namespace, and unbound ops tokens
  carry no `ns`.
- Access red lines run in a fixed order before role narrowing: unknown role, reserved
  namespace, reserved tenant namespace names, unknown action, auth-token operations
  requiring ops, system operations requiring ops, namespace scope, then role action
  narrowing.
- `ops-observer` is cross-namespace read-only and intentionally lacks secret value,
  workflow payload, arbitrary SQL, R2 object head/body, token-list, and write
  permissions.
- Bound roles must self-cohere with stored `ns`: tenant roles bind tenant namespaces,
  platform roles bind platform-tier reserved namespaces, and unbound ops roles must not
  store `ns`.
- Reserved namespaces are exact literals from `shared/ns-pattern.js`: `__system__`,
  `__platform__`, and `__community__`. Only `__platform__` is currently in
  `PLATFORM_TIER_RESERVED_NS`; `__system__` is system-runtime/control-plane reserved,
  and `__community__` is reserved but not currently a platform-tier role namespace.
- Reserved namespace checks happen both in route/auth red lines and in role scope
  checks; they are not interchangeable.
- `issue` can mint `ns`, `platform`, `platform-observer`, and `ops-observer` tokens.
  `token-issuer` is also direct-issuable by ops, but requires camelCase
  `issueTemplates` naming existing delegated issue templates. Auth stores that allowlist
  as Redis `issue_templates` JSON array string and rejects public `issue_templates`
  input so API shape and storage shape do not mix. `ops` is bootstrap-only. Token
  plaintext is generated once, shown once, and stored only by SHA-256 hash.
- Built-in delegated issue templates are `wdl-chat-ns-pool` for workshop pools and
  `wdl-cli-integration` for short-lived hosted CLI live integration namespaces.
- `token-issuer` is an unbound role; aside from `/whoami` self-introspection, its only
  non-diagnostic action is `auth.delegated_token.issue`. A delegated issue request
  accepts only `{ template }`;
  caller-provided `kind`, `ns`, `label`, `expiresAt`, or template allowlist fields are
  rejected. Auth re-reads the issuer token record under the final issue-lock WATCH,
  verifies it is active and has the template allowlist entry, applies the code-defined
  Auth template registry, then writes a target token with `created_by`,
  `issue_template`, and `issue_template_version` metadata. Active quota is a live
  credential capacity guard
  based on `created_by + issue_template + expires_at`; it is not an environment quota.
  Generated namespaces are ordinary tenant namespace strings embedded in the issued
  token record. Auth serializes issue for each issuer/template with
  `auth:delegated-issue-lock:<issuerTokenId>:<templateId>` before counting quota, then
  rejects generated namespace candidates that already appear in the control-maintained
  `namespaces` set or in any scanned token record's stored `ns`, regardless of token
  kind, issuer, template, expiry, or revocation state. Quota computation is
  fail-closed: a malformed delegated token record for the same issuer/template blocks
  new delegated issue until the storage contract violation is repaired. Token list
  remains an operator repair surface and reports malformed `issue_templates` fields as
  invalid entries instead of failing the entire list.
  Delegated namespace collision checks are best-effort in V1: unbound/full-plane
  tokens can still perform namespace-scoped writes that leave no auth-visible `ns`
  token record, while `namespaces` only reflects active workers. Routine delegated
  namespace workflows should use namespace-bound credentials for namespace-scoped
  writes; a future persistent namespace fact index is the durable fix for this
  residual risk.
- `expiresAt` must be strict ISO-8601 UTC with millisecond precision and a real calendar
  round trip. Expired tokens are lazily collected on first verify after expiry by
  deleting the active hash index and writing `expired_at`; revoked tokens use
  `revoked_at`.
- Reserved token id `bootstrap` is the infrastructure-managed ops token. `BOOTSTRAP_TOKEN`
  is upserted on auth cold start, `revoke("bootstrap")` is forbidden, and rotation is
  an environment update plus redeploy. Verify caches the ensured bootstrap hash but can
  re-ensure it after Redis loss so bootstrap can recover a flushed auth store.

## Ownership / Concurrency / Failure Semantics

- `parseControlRoute()` is the single URL-to-action parser.
- If a route shape has no action, non-ops fail closed at auth while ops may reach
  dispatcher/handler path and method validation; known top-level wrong-method routes
  return `405 method_not_allowed`.
- Control maps JSON errors as `{ error, message }`; details are additive.
- Details may add fields but must not override `error`, `message`, or legacy `reason`.
  Auth reject reason is the `error` machine code; logs may carry `reason` as diagnostic
  context.
- `control/errors.js::ControlAbort` is the base in-Control coded abort contract.
  Routing and Auth retain their boundary-specific error classes; Deploy may subclass
  `ControlAbort` where commit cleanup requires a distinct catch boundary.
- `control/json-body.js` owns bounded Control JSON parsing and its `400`/`413` wire
  mapping. `control/optimistic.js` binds strict `WatchError` recognition and Redis
  sessions to the retry loop owned by `shared/optimistic-retry.js`.
- `control/lib.js::parseBundleMeta()` is the single parser for persisted bundle
  `__meta__`. It requires a JSON object and accepts an error factory so routing,
  workflows, delete, deploy, and env-budget paths retain their own catch boundaries.
  Absence remains use-site-specific. Paths that need metadata to compute a correct
  projection change, uniqueness proof, lifecycle cleanup, workflow view, or environment
  budget fail closed while their authoritative route or index still names the bundle.
  Deploy discovery/link preflight does not classify absence as `corrupt_meta`; the
  watched commit rejects a missing pinned service target as `target_drift`.
- Delegated issue 409 reasons have distinct retry meaning:
  `delegated_issue_busy` is retryable after the issuer/template lock clears,
  `active_quota_exceeded` is not retryable until existing delegated credentials expire
  or are revoked, and `namespace_collision` means Auth exhausted the configured
  candidate retry budget.
- Control 5xx responses use generic/safe messages. Internal exception text, auth Redis
  diagnostics, backend messages, and provider errors belong in logs unless the endpoint
  explicitly owns a diagnostic response field. Structured coded-error diagnostic strings
  are truncated to 2,048 characters before log emission.
- Deploy returns `worker_code_invalid` when final WorkerCode would collide with injected
  WDL runtime/do-runtime reserved module names or lacks required bundle metadata, and
  `worker_code_too_large` when final WorkerCode, including runtime/do-runtime-injected
  modules and generated workflow keys, exceeds workerd's 64 MiB dynamic code limit.
  Deploy and secret mutations return `worker_env_too_large` when the estimated
  `workerLoader` env exceeds WDL's headroomed 1 MiB budget.
  `worker_env_too_large` details include `namespace`, optional `worker`, `env_bytes`,
  `max_env_bytes`, `upstream_max_env_bytes`, and `headroom_bytes`. Deploy-time
  per-version checks also include `version`. Secret mutations that re-estimate an
  existing version also include `source_version` and `estimated_version`.
  `source_version` identifies the stored version to inspect, delete, or redeploy;
  `estimated_version` is the tag used for budget sizing; on worker-secret bump paths
  it is the exact allocated bump version.
- Control never calls gateway directly. It writes Redis and publishes invalidation
  messages.
- Control encrypts secret PUT values before entering Redis mutation loops.
  Encryption/provider failure returns a control error and does not write a plaintext
  fallback.
- Worker delete commits Redis lifecycle state first; async S3 cleanup enqueue is
  best-effort and returns warning if it fails.
- The `s3-cleanup` system worker persists cleanup tasks in D1. Cron replay owns retries
  after the row exists, uses minute-scale exponential backoff capped at 30 minutes for
  S3 failures, and checkpoints large prefix cleanup after each S3 List/Delete page so
  normal pagination progress does not consume failure attempts or restart from the
  beginning after a scheduler timeout. Each run processes one page, so very large
  prefixes drain across multiple cron or queue dispatches instead of holding one
  scheduler dispatch open for minutes.
- All Control-to-Workflows internal POSTs use the canonical transport in
  `control/workflows-client.js`. Callers retain endpoint-specific timeout, non-2xx, and
  response-body interpretation. Workflow management calls and the lifecycle delete scan
  have no client-side timeout because the scan is unbounded by namespace size. DO-alarm
  cleanup uses a five-second timeout. Workflow lifecycle blockers fail closed on service
  errors.
- AUTH JSRPC errors or Redis explosions are control-plane failures and map to 503
  fail-closed behavior, not tenant-visible authorization fallbacks.

## Security Boundaries

- Auth role table is the source of truth for principal capabilities.
- Reserved namespace red lines are enforced in route parsing and auth evaluation.
- `ops` is full plane. `ops-observer`, `ns`, `platform`, `platform-observer`, and
  `token-issuer` have narrower scopes.
- Platform double-pin rule: platform role cross-namespace details require both platform
  principal kind and matching platform-tier namespace.
- `x-admin-token` sanitization is shared between control and auth.
- Control carries no token-shaped environment variable. The only server-side token env
  is auth's `BOOTSTRAP_TOKEN`; control reaches auth through the `AUTH` binding.

## Observability

Control and auth are primarily log-observed. They do not expose public metrics sockets.
Request id is propagated through gateway -> control -> auth and appears in logs.
Verify outcomes are logged as success, reject, or error; 5xx outcomes are error logs,
4xx rejects are warning logs.

## Deployment / Rollout Notes

- Control and runtime should roll together when bundle metadata shape changes.
- Control and gateway must keep route invalidation channel names aligned.
- Auth role changes should be reviewed as security boundary changes and tested against
  reserved namespace behavior.

## Tests That Protect This Module

- `tests/unit/control-lib.test.js`
- `tests/unit/control-routing.test.js`
- `tests/unit/control-delete-handler.test.js`
- `tests/unit/control-deploy-watch.test.js`
- `tests/unit/control-lifecycle-indexes.test.js`
- `tests/unit/control-shared-stub.test.js`
- `tests/unit/control-handlers-workflows.test.js`
- `tests/unit/control-d1-migrations.test.js`
- `tests/unit/redis-lock.test.js`
- `tests/unit/auth-lib.test.js`
- `tests/unit/auth-index.test.js`
- `tests/integration/auth-worker.test.js`
- `tests/integration/auth-platform.test.js`
- `tests/integration/system-pool-auth.test.js`
- `tests/unit/style-contracts.test.js`

## Known Constraints And Non-Goals

- Control is not topology-aware for gateway.
- Auth tokens are bearer tokens; storage and revocation are Redis-backed.
- Control APIs are admin/operator APIs, not tenant data-plane APIs.
- Control/runtime/auth/CLI ship as one release. Do not add fake dual-shape fallbacks for
  in-tree protocol response migrations unless a real external rollout requirement
  exists.
