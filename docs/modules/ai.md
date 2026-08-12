# AI Binding

## Purpose And Scope

WDL provides a namespace-scoped AI binding for agent and inference workloads. Tenant
code receives `env.AI.fetch()`, `env.AI.run()`, and `env.AI.models()` while provider
credentials remain in the platform control plane. The initial provider adapters target
the official OpenAI, xAI, and DeepSeek APIs.

AI is a BYO-provider capability. Each namespace owns its provider metadata and encrypted
credentials. WDL does not currently provide a managed model catalog, shared platform
credential, token accounting, billing, quota enforcement, or distributed tenant
fairness. The process-local concurrency controls described below are runtime safety
bounds, not product quotas.

## Current Implementation

An AI request crosses these owners:

1. Wrangler `[ai]` configuration produces one bundle binding with shape
   `{ "type": "ai" }`.
2. Runtime materializes an `AiBinding` host entrypoint with immutable
   `{ ns, worker, version, binding }` props. Generated wrapper code replaces the raw
   host stub with the tenant-realm `Ai` facade for both handler env and imported env.
3. The host binding asks the colocated redis-proxy to resolve a public model id. The
   proxy atomically reads provider metadata and its encrypted credential from DB 0,
   validates canonical state, decrypts the credential, and returns an exact official
   destination.
4. Runtime independently checks that destination against its built-in adapter table,
   attaches the credential, and sends the request through the public-only
   `AI_NETWORK` service.
5. JSON responses, semantic SSE frames, or WebSocket frames return to the tenant
   without exposing the credential or resolver-only metadata.

The same facade is available inside Durable Objects. DO calls consume the independent
process-local pools of the do-runtime replica, not the user-runtime pools. A host-side
`waitUntil()` watchdog owns the final permit release, so actor teardown does not make
pool cleanup depend on delivery of a tenant cancellation event.

The host `AiBinding` prototype intentionally exposes only `fetch()`. `run()` and
`models()` live in the generated tenant-realm facade, where `AbortSignal`, native
`Response`, `ReadableStream`, and WebSocket objects do not have to cross a second JSRPC
method boundary.

## Public Interfaces

Declare at most one AI binding:

```toml
[ai]
binding = "AI"
```

The persisted bundle shape is:

```json
{
  "bindings": {
    "AI": { "type": "ai" }
  }
}
```

Provider selection is part of the model id passed by tenant code, not Wrangler
configuration. Model ids use `<provider>/<alias>`, for example `openai/primary`.

### Tenant facade

- `await env.AI.models()` returns the configured models whose provider credential is
  present. Entries expose public id, protocol, transports, modalities, and capability
  hints; they omit upstream model id, provider revision, destination, and credential.
- `await env.AI.run(model, inputs, options?)` selects the descriptor protocol, replaces
  the public model alias with the upstream model id, and returns parsed JSON, a
  `ReadableStream` for `stream: true`, or a `101 Response` for
  `{ websocket: true }`.
- `await env.AI.fetch(input, init?)` is the raw OpenAI-compatible surface. It accepts
  only the virtual origin `https://ai.wdl` and the paths listed below.

The only `run()` options are `signal` and `websocket`. WebSocket mode requires
`inputs === null`; the signal bounds setup, while the returned socket owns the session
lifecycle. Unsupported Cloudflare options fail explicitly. Applications that need the
raw provider `Response` use `fetch()`; `returnRawResponse` is not supported.

| Method | Virtual path | Protocol / result |
|---|---|---|
| `GET` | `/v1/models` | Bounded public model list |
| `POST` | `/v1/responses` | Responses JSON or semantic SSE |
| `POST` | `/v1/chat/completions` | Chat Completions JSON or SSE |
| `POST` | `/v1/embeddings` | Embeddings JSON |
| `GET Upgrade` | `/v1/responses?model=<id>` | Responses WebSocket |
| `GET Upgrade` | `/v1/realtime?model=<id>` | Realtime WebSocket |

For HTTP inference, the request must be JSON and the model must be in the body. Tenant
authorization, endpoint, host, redirect, and arbitrary outbound headers are never
forwarded. WDL forwards bounded response headers such as content type, retry delay, and
provider request id, then replaces `x-request-id` with the WDL request id.

`run()` forwards native provider request and response fields. Function tools,
structured output, reasoning fields, provider tools, and multimodal inputs therefore
remain usable when the selected provider/model supports them. WDL does not execute
tools, validate tool arguments, or automatically continue an agent loop.

The official OpenAI JavaScript SDK works for JSON, SSE, and cancellation with
`baseURL: "https://ai.wdl/v1"`, any non-empty placeholder API key, and
`fetch: env.AI.fetch.bind(env.AI)`. WDL discards the SDK authorization header and uses
the configured namespace credential. SDK WebSocket helpers are not a compatibility
claim; applications use the binding WebSocket surface directly.

### Provider management

Control owns these namespace-scoped routes:

| Method and path | Action |
|---|---|
| `GET /ns/<ns>/ai/providers` | `ai.provider.read` |
| `GET /ns/<ns>/ai/providers/<name>` | `ai.provider.read` |
| `PUT /ns/<ns>/ai/providers/<name>` | `ai.provider.write` |
| `DELETE /ns/<ns>/ai/providers/<name>` | `ai.provider.write` |
| `PUT /ns/<ns>/ai/providers/<name>/credential` | `ai.provider.write` |
| `GET /ns/<ns>/ai/models` | `ai.model.list` |

A provider write contains `kind` and one or more model descriptors. Control generates
a new 128-bit revision and atomically clears any old credential. A credential write
must carry that exact revision, preventing changed metadata from silently inheriting a
credential approved for the previous destination/model set. Provider deletion removes
metadata and credential together, including credential-only repair residue.
Credentials are bounded visible-ASCII bearer tokens without whitespace; malformed
values are rejected before encryption.

Provider and model alias grammar is owned by `shared/ns-pattern.js`. `upstreamModel` is
an opaque, non-empty provider identifier with a 256-byte UTF-8 limit; it deliberately
does not use alias grammar.

| Provider kind | Supported adapter protocols |
|---|---|
| `openai` | Responses, Chat Completions, Embeddings, Responses WebSocket, Realtime WebSocket |
| `xai` | Responses, Chat Completions, Embeddings, Responses WebSocket, Realtime WebSocket |
| `deepseek` | Responses compatibility and Chat Completions over HTTP/SSE |

The adapter table fixes official destinations in code. Provider metadata cannot supply
an endpoint. Model descriptors must still advertise only capabilities actually
supported by their upstream model; provider errors remain authoritative for native
fields that WDL does not interpret.

## Redis And Persisted State

AI uses DB 0:

| Key | Type | Owner |
|---|---|---|
| `ai:providers:<ns>` | Hash: provider name to canonical provider JSON | Control writes; redis-proxy reads |
| `ai:provider-credentials:<ns>` | Hash: provider name to `WDL-ENC:` credential envelope | Control writes; redis-proxy decrypts |

Provider JSON includes `revision`, `kind`, and a canonical alias-sorted `models` map.
Control allows at most eight providers, 32 models per provider, 128 models per
namespace, and 64 KiB per provider record. Credentials are non-empty and at most
16 KiB, and use the visible-ASCII bearer-token grammar described above.

`/ai/resolve` reads one provider record and credential in one Lua snapshot.
`/ai/models` reads the complete bounded provider snapshot plus credential-field
presence in one Lua call; it returns configured hints without decrypting every
credential. Malformed, non-canonical, torn, or over-limit provider state fails closed,
and `/ai/resolve` also fails closed on an undecryptable credential. The shared JS/Rust
grammar is pinned by `tests/fixtures/ai-contract.json`.

Provider state follows namespace-secret lifecycle: it may outlive the last Worker in a
namespace. Deleting and later recreating the last Worker does not delete provider
metadata or credentials. Only explicit provider deletion removes them.

## Concurrency, Bounds, And Failure Semantics

Runtime maintains three independent, fail-fast pools per service replica:

| Pool | Default | Holds |
|---|---:|---|
| request | 32 | Model listing and every HTTP request through bounded body admission; non-streaming inference remains here through completion |
| stream | 16 | SSE inference after bounded body admission until a terminal event, error, cancellation, or deadline |
| websocket | 8 | Responses or Realtime WebSocket sessions |

Pool saturation returns `429 ai_capacity_exhausted`; requests are not queued. User and
DO runtimes have separate module state and therefore separate pools. These bounds limit
one process, not a namespace across replicas.

The fixed byte limits are 8 MiB request JSON, 16 MiB non-streaming response, 32 MiB SSE
response, 1 MiB SSE frame, 1 MiB WebSocket frame, and 128 MiB per WebSocket direction.
Default time bounds are 120 seconds for request/setup, 30 seconds SSE idle, five minutes
SSE duration, 15 seconds WebSocket handshake, two minutes WebSocket idle, and 24 minutes
operator WebSocket duration. The effective WebSocket duration is the lower of the
operator bound and the official adapter bound.

The host registers its watchdog before request-body, resolver, or provider I/O. An SSE
request atomically transfers its lease from the request pool to the stream pool after
bounded body admission; it never holds both permits. Normal completion can release
early; an idempotent deadline remains the final release/abort owner if workerd does not
deliver a mid-response `AbortSignal`, stream cancellation, or socket teardown.
SSE requires a protocol terminal event (`response.completed`, `response.incomplete`,
`response.failed`, `error`, or Chat Completions `[DONE]`); EOF before that event is an
error. A top-level `error` event is forwarded before the stream permit records a
`provider_error` outcome. Responses are not silently retried after provider I/O can
have side effects.

WebSocket text frames must be JSON. WDL pins model fields to the resolved upstream
model, enforces advertised binary-frame support, and propagates bounded close codes and
reasons. It does not reconnect an AI WebSocket or resume a provider session.

## Security Boundaries

- Credentials are encrypted at rest, are never written into bundle metadata or tenant
  env, and are never returned by Control list/get routes.
- redis-proxy is the only decrypting reader. Its `/ai/*` routes require the internal
  auth token; the host binding receives plaintext only in the local resolver response.
- Runtime validates the resolver destination a second time before attaching the
  credential. Only exact official HTTPS/WSS destinations are accepted.
- Provider traffic uses `AI_NETWORK`, a public-only workerd network service. It does not
  use runtime's internal/private outbound path.
- Tenant-supplied provider destinations and authentication headers are not forwarded;
  native multimodal content fields remain part of the provider request. Redirects are
  rejected.
- Namespace, worker, version, provider errors, and request ids may appear in structured
  logs; credentials and tenant request bodies do not.

## Observability

AI calls use the shared binding metrics:

- `wdl_binding_operations_total{service,binding="ai",operation,outcome}`
- `wdl_binding_operation_duration_ms{service,binding="ai",operation,outcome}`

Pool state uses bounded labels only:

- `wdl_ai_pool_in_use{service,pool}`
- `wdl_ai_pool_high_water{service,pool}`
- `wdl_ai_pool_events_total{service,pool,outcome}`

Rejected host calls emit `ai_binding_request_rejected`; Control mutations emit bounded
AI lifecycle events. Namespace, worker, version, and raw error text stay in logs rather
than metric labels.

## Deployment / Rollout Notes

The receiver-before-sender order is redis-proxy, then user-runtime/do-runtime host
readers. Pause Control mutations while rolling system-runtime because that one service
delivers both the system reader and Control writer; after old tasks drain, resume
mutations and publish the CLI that can emit `[ai]`. Runtime and do-runtime must be able
to materialize the new binding before Control accepts bundle metadata that references
it.

Before public release, the official-provider matrix must pass from the actual Tokyo ECS
user-runtime and do-runtime egress for OpenAI, xAI, and DeepSeek. Local fake-provider
integration proves protocol and secret boundaries but is not evidence of regional
provider availability.

## Tests

- `tests/unit/ai-contract.test.js` and the Rust `ai_contract_fixture_matches_rust_readers`
  test pin the shared persisted and resolver grammar.
- `tests/unit/control-ai-handler.test.js` covers revision CAS, encryption, canonical
  state, aggregate limits, residual cleanup, and zero-Worker persistence semantics.
- `tests/unit/runtime-ai-client.test.js` and
  `tests/unit/runtime-ai-binding.test.js` cover facade options, official destinations,
  byte/frame bounds, SSE terminals, slow-upload cancellation, watchdogs, pool lease
  transfer, and WebSocket model pinning.
- `tests/integration/ai-binding.test.js` covers handler/imported env, provider rotation,
  zero-Worker recreation, JSON/SSE, Responses and Realtime WebSockets, OpenAI SDK use,
  credential non-exposure, and DO caller teardown.

Integration uses an in-repo fake official provider. Real credentials must never be
committed or printed by test output.

## Known Constraints And Non-Goals

- No arbitrary OpenAI-compatible endpoint is accepted. New providers require a reviewed
  adapter with an exact destination and conformance tests.
- No managed catalog, shared platform credential, usage aggregation, billing, quota,
  distributed fairness, or automatic provider failover exists.
- No `toMarkdown()`, asynchronous batch inference, background Responses, stored file
  APIs, WebRTC, SIP, or automatic tool execution is implemented.
- DeepSeek continuation (`previous_response_id`/conversation state), stored responses,
  embeddings, and WebSocket transports are rejected.
- Provider-native warnings and semantic events pass through; WDL does not invent a
  private provider event protocol.
- An AI WebSocket inside a Durable Object is an ordinary outbound session. DO
  hibernation does not make that provider socket durable or reconnectable.
