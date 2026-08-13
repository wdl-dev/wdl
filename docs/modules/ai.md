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
   `{ ns, worker, version }` props. Generated wrapper code replaces the raw host stub
   with the tenant-realm `Ai` facade in positional env and in invocation-time reads from
   an enabled imported env proxy. It carries the current request id into every
   facade-created host request.
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

Importable env follows workerd compatibility semantics. Code may retain
`import { env } from "cloudflare:workers"` at module scope and read `env.AI` during an
invocation to obtain the facade. It must not cache `const ai = env.AI` during module
evaluation and expect `run()` or `models()`: evaluation precedes wrapper invocation, so
that value is the raw binding-scoped host stub with `fetch()` only. With
`disallow_importable_env`, imported env exposes no AI binding; positional handler and
Durable Object env still expose the full facade.

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
Malformed public model ids fail host admission with `400 ai_invalid_model` before
resolver or provider I/O.

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
provider request id. `x-request-id` remains the WDL request id; a provider that uses
that same header is exposed as `x-ai-provider-request-id` instead.

`run()` forwards native provider request and response fields. Function tools,
structured output, reasoning fields, provider tools, and multimodal inputs therefore
remain usable when the selected provider/model supports them. Input modalities include
text, image, audio, and direct Responses file items; WDL does not provide provider file
upload or lifecycle APIs. WDL does not execute tools, validate tool arguments, or
automatically continue an agent loop.
Successful responses that cannot be decoded as JSON fail `run()` with an `AIError`
whose status is `502`. For non-success responses, `run()` preserves the bounded
OpenAI-compatible provider message and string code, falling back to the provider type
when no code exists. `fetch()` remains the raw-response escape hatch.

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
a new 128-bit revision. An update within the same provider kind preserves its existing
credential because the kind owns the current bearer-authentication shape; creating a
provider over credential-only residue or changing provider kind clears the credential
in the same transaction. A credential write must carry the exact current revision, so
a delayed write cannot attach to another provider incarnation. Provider deletion
removes metadata and credential together, including credential-only repair residue.
Credentials are bounded visible-ASCII bearer tokens without whitespace; malformed
values are rejected before encryption.

Provider and model alias grammar is owned by `shared/ns-pattern.js`. `upstreamModel` is
an opaque, non-empty, well-formed Unicode provider identifier with a 256-byte UTF-8
limit; it deliberately does not use alias grammar.

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

`/ai/resolve` accepts `{ ns, model, protocol, transport }` and reads one provider record
and credential in one Lua snapshot. `/ai/models` accepts `{ ns }`, reads the complete
bounded provider snapshot plus credential-field presence in one Lua call, and returns
configured hints without decrypting every credential. The Lua snapshot rejects either
hash when its cardinality exceeds the provider bound before materializing provider
records or credential field names. Binding name is not part of either wire request
because one Worker can declare at most one AI binding. Malformed, non-canonical, torn,
or over-limit provider state fails closed, and `/ai/resolve` also fails closed on an
undecryptable credential. The shared JS/Rust grammar is pinned by
`tests/fixtures/ai-contract.json`; resolver responses contain only fields consumed by
the host request path.

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
Oversized non-streaming responses, rejected WebSocket handshake bodies, redirects, and
invalid streaming content types abort provider I/O before releasing their permit; body
cancellation is only a secondary cleanup signal.
SSE requires a protocol terminal event (`response.completed`, `response.incomplete`,
`response.failed`, `error`, or Chat Completions `[DONE]`); EOF before that event is an
error. Terminal frames are forwarded unchanged before the stream permit records
`completed`, `provider_incomplete`, `provider_failed`, or `provider_error`. A semantic
terminal event or tenant cancellation also aborts the provider fetch independently of
stream-cancellation delivery. Responses are not silently retried after provider I/O can
have side effects.

WebSocket text frames must be JSON. WDL pins model fields to the resolved upstream
model, enforces advertised binary-frame support, and propagates bounded close codes and
reasons. Provider-loss closes that Gateway would otherwise treat as reconnectable are
translated to terminal `1013 AI provider connection lost`, so Gateway cannot silently
replace the provider session. The initial upgrade also disables Gateway backend
replacement, so runtime loss closes the public session with `1012 service restart`
instead of creating a new provider session behind the same client connection. These
guarantees apply to the WebSocket owned by the AI binding. If tenant code terminates a
separate `WebSocketPair` and bridges it to the AI socket, it must preserve the AI upgrade
headers on its own `101` response so Gateway retains the terminal policy:

```js
const aiUpgrade = await env.AI.run(model, null, { websocket: true });
// ...bridge aiUpgrade.webSocket to client...
return new Response(null, {
  status: 101,
  webSocket: client,
  headers: aiUpgrade.headers,
});
```

Gateway consumes and removes the internal policy header before the public response. If
the bridge omits these headers, the tenant WebSocket keeps Gateway's ordinary bounded
backend replacement behavior, and runtime loss can create a fresh provider session.
WDL does not reconnect a WebSocket owned by the AI binding or resume its provider
session.

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
- `wdl_binding_operation_duration_ms{service,binding="ai",operation}`

Pool state uses bounded labels only:

- `wdl_ai_pool_in_use{service,pool}`
- `wdl_ai_pool_high_water{service,pool}`
- `wdl_ai_pool_events_total{service,pool,outcome}`

Rejected host calls emit `ai_binding_request_rejected`; rejected Control AI mutations
emit bounded structured events through the shared Control request log. Namespace,
worker, version, and raw error text stay in logs rather than metric labels.

## Deployment / Rollout Notes

The receiver-before-sender order is redis-proxy and Gateway, then
user-runtime/do-runtime host readers. Gateway must understand application-terminal
provider closes before AI WebSockets are public. Pause Control mutations while rolling
system-runtime because that one service delivers both the system reader and Control
writer; after old tasks drain, resume mutations and publish the CLI that can emit
`[ai]`. Runtime and do-runtime must be able to materialize the new binding before
Control accepts bundle metadata that references it.

Before public release, the official-provider matrix must pass from the actual Tokyo ECS
user-runtime and do-runtime egress for OpenAI, xAI, and DeepSeek. Local fake-provider
integration proves protocol and secret boundaries but is not evidence of regional
provider availability.

## Tests

- `tests/unit/ai-contract.test.js` and the Rust `ai_contract_fixture_matches_rust_readers`
  test pin the shared persisted/resolver grammar and exact official destinations.
- `tests/unit/control-ai-handler.test.js` covers revision CAS, encryption, canonical
  state, aggregate limits, residual cleanup, and zero-Worker persistence semantics.
- `tests/unit/runtime-ai-client.test.js` and
  `tests/unit/runtime-ai-binding.test.js` cover facade options, official destinations,
  byte/frame bounds, SSE terminals, slow-upload cancellation, watchdogs, pool lease
  transfer, and WebSocket model pinning.
- `tests/integration/ai-binding.test.js` covers positional and live imported env,
  explicit importable-env disablement, provider rotation, zero-Worker recreation,
  JSON/SSE, Responses and Realtime WebSockets, OpenAI SDK use, credential non-exposure,
  terminal user-runtime/do-runtime loss, and DO caller teardown.

Integration uses an in-repo fake official provider. Real credentials must never be
committed or printed by test output.

## Known Constraints And Non-Goals

- No arbitrary OpenAI-compatible endpoint is accepted. New providers require a reviewed
  adapter with an exact destination and conformance tests.
- No managed catalog, shared platform credential, usage aggregation, billing, quota,
  distributed fairness, or automatic provider failover exists.
- No `toMarkdown()`, asynchronous batch inference, background Responses, stored file
  APIs, WebRTC, SIP, or automatic tool execution is implemented.
- DeepSeek non-text input or output, continuation (`previous_response_id`/conversation
  state), stored responses, embeddings, and WebSocket transports are rejected before
  provider I/O.
- Provider-native warnings and semantic events pass through; WDL does not invent a
  private provider event protocol.
- An AI WebSocket inside a Durable Object is an ordinary outbound session. DO
  hibernation does not make that provider socket durable or reconnectable.
