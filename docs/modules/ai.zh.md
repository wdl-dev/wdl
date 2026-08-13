# AI Binding

## 目的与范围

WDL 提供 namespace-scoped AI binding，用于 agent 和 inference workload。Tenant code 获得 `env.AI.fetch()`、`env.AI.run()` 和 `env.AI.models()`，provider credential 保留在平台 control plane。支持的 adapter 面向 OpenAI、xAI 和 DeepSeek 官方 API。

AI 是 BYO provider 能力。每个 namespace 拥有自己的 provider metadata 和加密 credential。WDL 目前不提供 managed model catalog、平台共享 credential、token 计量、计费、quota enforcement 或分布式 tenant fairness。下文的 process-local control 提供有限的 admission 与 lifetime bound，不是产品配额或通用容量保证。

## 当前实现

AI request 经过以下 owner：

1. Wrangler `[ai]` 配置生成一个 `{ "type": "ai" }` bundle binding。
2. Runtime 使用不可变的 `{ ns, worker, version }` props materialize `AiBinding` host entrypoint。Generated wrapper 会在 positional env，以及启用了 importable env 时 invocation 内对 imported env proxy 的实时读取中，把 raw host stub 替换成 tenant-realm `Ai` facade。Facade 在每个 loaded Worker module lifecycle 内懒加载一份有界 public model catalog snapshot，并把当前 request id 带入每个 facade 创建的 host request。
3. Host binding 请求 colocated redis-proxy 解析 public model id。Proxy 从 DB 0 原子读取 provider metadata 与加密 credential，校验 canonical state，解密 credential，并返回精确的官方 destination。
4. Runtime 再用内建 adapter table 独立校验 destination，附加 credential，并通过 public-only `AI_NETWORK` service 发出请求。
5. JSON response、semantic SSE frame 或 WebSocket frame 返回 tenant；credential 和 resolver-only metadata 不会暴露。

同一个 facade 也可在 Durable Object 中使用。DO 调用消耗 do-runtime replica 自己的 process-local pool，不与 user-runtime 共用。Host-side `waitUntil()` watchdog 持有最终 permit release，因此 actor teardown 不会让 pool cleanup 依赖 tenant cancellation event 是否被投递。

Host `AiBinding` prototype 只暴露 `fetch()`。`run()` 和 `models()` 位于 generated tenant-realm facade，使 `AbortSignal`、native `Response`、`ReadableStream` 和 WebSocket object 不需要再跨第二层 JSRPC method boundary。

Importable env 遵循 workerd compatibility 语义。代码可以在 module scope 保存 `import { env } from "cloudflare:workers"` 得到的 proxy，并在 invocation 内实时读取 `env.AI` 取得 facade；不能在 module evaluation 时缓存 `const ai = env.AI` 后期待它提供 `run()` 或 `models()`，因为 module evaluation 早于 wrapper invocation，此时只能看到仅有 `fetch()` 的 binding-scoped raw host stub。设置 `disallow_importable_env` 后，imported env 不暴露 AI binding；positional handler 和 Durable Object env 仍暴露完整 facade。

## 对外接口

最多声明一个 AI binding：

```toml
[ai]
binding = "AI"
```

持久化 bundle shape 为：

```json
{
  "bindings": {
    "AI": { "type": "ai" }
  }
}
```

Provider 选择由 tenant code 传入的 model id 决定，不写在 Wrangler 配置里。Model id 使用 `<provider>/<alias>`，例如 `openai/primary`。

Malformed public model id 会在 resolver 或 provider I/O 前由 host admission 以 `400 ai_invalid_model` 拒绝。

### Tenant facade

- `await env.AI.models()` 返回 loaded Worker module 懒加载并缓存的 model catalog snapshot 副本。Snapshot 包含所有已配置的 provider model，不受 credential 是否存在影响。Entry 暴露 public id、protocol、transports、modalities 和 capability hints，不暴露 upstream model id、provider revision、destination、credential 或 credential status。
- `await env.AI.run(model, inputs, options?)` 按 descriptor 选择 protocol，把 public alias 替换成 upstream model id，并返回 parsed JSON、`stream: true` 时的 `ReadableStream`，或 `{ websocket: true }` 时的 `101 Response`。
- `await env.AI.fetch(input, init?)` 是 raw OpenAI-compatible surface，只接受 virtual origin `https://ai.wdl` 和下表路径。

`run()` option 只有 `signal` 和 `websocket`。WebSocket mode 要求 `inputs === null`；signal 只约束 setup，返回的 socket 自己拥有 session lifecycle。不支持的 Cloudflare option 会显式失败。需要 raw provider `Response` 时使用 `fetch()`；不支持 `returnRawResponse`。

`run()` 与 `models()` 在 loaded Worker module 生命周期内共享同一份成功加载的 catalog snapshot。并发 cold read 不会以跨 invocation Promise 的形式保留；首个成功完成的读取会成为共享 snapshot，失败读取不会缓存。Provider alias、protocol、transport、modality 或 capability 变化会在该 module 重载后可见；需要当前低层列表的调用方可使用 `fetch("https://ai.wdl/v1/models")`。每次 inference 与新 socket 仍会执行权威 `/ai/resolve`，因此 credential 和 upstream model 轮换无需 redeploy 即对下一次调用生效。Credential presence 不参与 catalog snapshot，补齐原本缺失的 credential 后，下一次调用也无需重载 catalog。过期 descriptor 会在 resolver 校验处 fail closed，不会成为附加 credential 或选择 provider destination 的权威来源。

| Method | Virtual path | Protocol / result |
|---|---|---|
| `GET` | `/v1/models` | 有界 public model list |
| `POST` | `/v1/responses` | Responses JSON 或 semantic SSE |
| `POST` | `/v1/chat/completions` | Chat Completions JSON 或 SSE |
| `POST` | `/v1/embeddings` | Embeddings JSON |
| `GET Upgrade` | `/v1/responses?model=<id>` | Responses WebSocket |
| `GET Upgrade` | `/v1/realtime?model=<id>` | Realtime WebSocket |

HTTP inference request 必须是 JSON，model 位于 body。Tenant authorization、endpoint、host、redirect 和任意 outbound header 都不会被转发。WDL 只转发有界的 content type、retry delay 和 provider request id 等 response header。`x-request-id` 始终是 WDL request id；使用同名 header 的 provider id 改由 `x-ai-provider-request-id` 暴露。

`run()` 透传 provider-native request/response field。只要所选 provider/model 支持，function tools、structured output、reasoning field、provider tools 和 multimodal input 都可使用。Input modality 包括 text、image、audio 和 Responses direct file item；WDL 不提供 provider file upload 或 lifecycle API。WDL 不执行 tool、不校验 tool argument，也不会自动继续 agent loop。

无法解码为 JSON 的成功响应会让 `run()` 以 status 为 `502` 的 `AIError` 失败。对于非成功响应，`run()` 保留有界 OpenAI-compatible provider message 和字符串 code；没有 code 时回退到 provider type。`fetch()` 仍是 raw-response escape hatch。

OpenAI 官方 JavaScript SDK 可通过 `baseURL: "https://ai.wdl/v1"`、任意非空 placeholder API key 和 `fetch: env.AI.fetch.bind(env.AI)` 使用 JSON、SSE 与 cancellation。WDL 丢弃 SDK authorization header，在 host binding 内附加 namespace credential。SDK WebSocket helper 不属于兼容承诺；应用直接使用 binding WebSocket surface。

### Provider 管理

Control 拥有以下 namespace-scoped route：

| Method 和 path | Action |
|---|---|
| `GET /ns/<ns>/ai/providers` | `ai.provider.read` |
| `GET /ns/<ns>/ai/providers/<name>` | `ai.provider.read` |
| `PUT /ns/<ns>/ai/providers/<name>` | `ai.provider.write` |
| `DELETE /ns/<ns>/ai/providers/<name>` | `ai.provider.write` |
| `PUT /ns/<ns>/ai/providers/<name>/credential` | `ai.provider.write` |
| `GET /ns/<ns>/ai/models` | `ai.model.list` |

Provider write 包含 `kind` 和一个或多个 model descriptor。Control 生成新的 128-bit revision。同一 provider kind 内更新 metadata 时保留已有 credential，因为 kind 拥有当前 bearer authentication shape；在 credential-only residue 上创建 provider 或改变 provider kind 时，Control 会在同一事务中清除 credential。Credential write 必须携带当前完全相同的 revision，防止延迟写入挂到另一个 provider incarnation。Provider delete 会一起删除 metadata 与 credential，包括 credential-only repair residue。Credential 是有字节上限、不含空白的 visible-ASCII bearer token；malformed value 在加密前被拒绝。

Provider 和 model alias grammar 由 `shared/ns-pattern.js` 拥有。`upstreamModel` 是非空、well-formed Unicode 的 opaque provider identifier，UTF-8 上限 256 bytes；它有意不使用 alias grammar。

| Provider kind | 支持的 adapter protocol |
|---|---|
| `openai` | Responses、Chat Completions、Embeddings、Responses WebSocket、Realtime WebSocket |
| `xai` | Responses、Chat Completions、Embeddings、Responses WebSocket、Realtime WebSocket |
| `deepseek` | HTTP/SSE 的 Responses compatibility 与 Chat Completions |

Adapter table 在代码中固定官方 destination，provider metadata 不能提供 endpoint。Model descriptor 仍必须只声明 upstream model 实际支持的能力；WDL 不解释的 native field 由 provider error 作为权威结果。

## Redis 与持久状态

AI 使用 DB 0：

| Key | Type | Owner |
|---|---|---|
| `ai:providers:<ns>` | Hash：provider name 到 canonical provider JSON | Control 写；redis-proxy 读 |
| `ai:provider-credentials:<ns>` | Hash：provider name 到 `WDL-ENC:` credential envelope | Control 写；redis-proxy 解密 |

Provider JSON 包含 `revision`、`kind` 和 canonical alias-sorted `models` map。Control 限制每个 namespace 最多八个 provider、每个 provider 32 个 model、每个 namespace 128 个 model、每条 provider record 64 KiB；credential 非空、最多 16 KiB，并使用上述 visible-ASCII bearer-token grammar。

`/ai/resolve` 接受 `{ ns, model, protocol, transport }`，在一个 Lua snapshot 中读取一条 provider record 与 credential；它会检查两个 hash 的 cardinality，并校验目标 record 与 credential。`/ai/models` 接受 `{ ns }`，在一个 Lua 调用中读取完整有界 provider snapshot 和 credential field names，校验全部已物化 field，并返回 provider model metadata，不解密 credential，也不按 credential presence 过滤 catalog。每个 Worker 最多声明一个 AI binding，因此两条 wire request 都不携带 binding name。每个 reader 都会对它实际物化的 malformed、non-canonical、torn 或 over-limit state fail closed；`/ai/resolve` 还会对无法解密的目标 credential fail closed。共享 JS/Rust grammar 由 `tests/fixtures/ai-contract.json` 固定；resolver response 只包含 host request path 实际消费的字段。

Provider state 跟随 namespace secret lifecycle，可以活过 namespace 中最后一个 Worker。删除并重新创建最后一个 Worker 不会删除 provider metadata 或 credential；只有显式 provider delete 才会删除。

## 并发、边界与失败语义

Runtime 在每个 service replica 内维护三个独立且 fail-fast 的 pool：

| Pool | 默认值 | 持有范围 |
|---|---:|---|
| request | 64 | Model list 与所有 HTTP request 的有界 body admission；non-streaming inference 会一直持有到完成 |
| stream | 64 | SSE inference 在有界 body admission 后持有，直到 terminal event、error、cancellation 或 deadline |
| websocket | 32 | Responses 或 Realtime WebSocket session |

以上是 user-runtime 与 do-runtime 默认值。较小的 system-runtime tier 通过部署配置覆盖为 32 个 request、16 个 stream 和 8 个 WebSocket permit。Pool size 是 admission limit，不是 token quota、计费控制，也不代表为每个请求预留了其最大 byte allowance。

Pool saturation 返回 `429 ai_capacity_exhausted`，不会排队。User runtime 和 DO runtime 使用不同 module state，因此 pool 独立。这些边界限制单个进程，不限制跨 replica 的 namespace。它们是 self-hosted runtime 的部署默认值，不是通用 hosted product limit。私有部署可通过文档中的 runtime 环境设置按资源规模调整；容量有限的 hosted preview 也可选择自己的部署值，而不改变 binding 合同。

固定 byte limit 为：使用严格 UTF-8 的 request JSON 1 MiB、HTTP request JSON 与 client-originated WebSocket JSON 的 container 最多嵌套 128 层、non-streaming response 4 MiB、SSE response 32 MiB、SSE frame 1 MiB、WebSocket frame 1 MiB、每个 WebSocket direction 累计计费 64 MiB。1 MiB request 上限同时约束 tenant ingress 和替换 model 后的最终 provider JSON；HTTP container depth 会在 `JSON.parse()` 物化 object graph 前，对有界 JSON text 执行检查。WebSocket session 上限约束生命周期累计用量；workerd 没有暴露 JavaScript queued-byte 或 backpressure signal，因此该上限不是对已排队 send 的进程内存保证。Client frame 按接收字节与完成 model 固定后的转发字节的较大值计费，provider frame 按接收字节计费。按默认 session 数计算，单方向 `32 * 64 MiB` 只是触发生命周期关闭前理论上的累计接收上界，不是预留内存或实测 native queue 大小；operator 必须按 runtime 内存和实际流量分布调整 session pool。默认时间边界为：request/setup 120 秒、SSE idle 30 秒、SSE duration 五分钟、WebSocket handshake 15 秒、WebSocket idle 两分钟、operator WebSocket duration 24 分钟。有效 WebSocket duration 取 operator bound 与官方 adapter bound 的较小值。

Host 在 request body、resolver 或 provider I/O 前注册 watchdog。SSE request 在完成有界 body admission 后，把同一个 lease 从 request pool 原子 transfer 到 stream pool，不会同时占用两个 permit。正常完成可以提前 release；如果 workerd 没有投递 mid-response `AbortSignal`、stream cancellation 或 socket teardown，幂等 deadline 仍是最终 release/abort owner。超限的 non-streaming response、被拒绝的 WebSocket handshake body、redirect 和错误的 streaming content type 都会在释放 permit 前 abort provider I/O；body cancellation 只是次级 cleanup signal。SSE 必须看到 protocol terminal event（`response.completed`、`response.incomplete`、`response.failed`、`error` 或 Chat Completions `[DONE]`）；terminal event 前 EOF 是错误。Terminal frame 会在 stream permit 分别记录 `completed`、`provider_incomplete`、`provider_failed` 或 `provider_error` 前原样转发。Semantic terminal event 或 tenant cancellation 还会独立 abort provider fetch，不依赖 stream cancellation 能否传递。Provider I/O 可能产生 side effect 后，WDL 不做隐式 inference retry。

Client-originated WebSocket text frame 必须是 container 最多嵌套 128 层的 JSON。WDL 拒绝重复的 `type`、`model` 或 `session` 决策字段，只修改或新增 provider model 固定所需的 model field，并原样转发其它 JSON source token。WDL 同时执行 advertised binary-frame support，并传播有界 close code/reason。WDL 将 Gateway 原本视为可重连的 provider-loss close 转换成终止性的 `1013 AI provider connection lost`，因此 Gateway 不会静默替换 provider session。Initial upgrade 还会禁用 Gateway backend replacement，因此 runtime 丢失时 public session 会以 `1012 service restart` 关闭，不会在同一条 client connection 后面创建新的 provider session。这些保证适用于 AI binding 自己持有的 WebSocket。若 tenant 代码终结一个独立的 `WebSocketPair` 并桥接 AI socket，它必须在自己返回的 `101` 上保留 AI upgrade headers：

```js
const aiUpgrade = await env.AI.run(model, null, { websocket: true });
// ...把 aiUpgrade.webSocket 桥接到 client...
return new Response(null, {
  status: 101,
  webSocket: client,
  headers: aiUpgrade.headers,
});
```

Gateway 会消费并从公开响应中删除内部 policy header。若桥接层遗漏这些 headers，tenant WebSocket 会保留 Gateway 普通的有界 backend replacement 行为，runtime 丢失时可能创建新的 provider session。WDL 不重连 AI binding 持有的 WebSocket，也不恢复它的 provider session。

## 安全边界

- Credential 加密存储，不写入 bundle metadata 或 tenant env，也不会由 Control list/get route 返回。
- redis-proxy 是唯一 decrypting reader。其 `/ai/*` route 要求 internal auth token；host binding 只在本地 resolver response 中收到 plaintext。
- Runtime 在附加 credential 前再次校验 resolver destination，只接受精确的官方 HTTPS/WSS destination。
- Provider traffic 使用 public-only `AI_NETWORK` workerd network service，不使用 runtime 的 internal/private outbound path。
- Tenant 指定的 provider destination 和认证 header 不会被转发；原生 multimodal content field 仍属于 provider request。Redirect 会被拒绝。
- Namespace、worker、version、provider error 和 request id 可以进入 structured log；credential 和 tenant request body 不会进入日志。

## 可观测性

AI 调用复用 shared binding metrics：

- `wdl_binding_operations_total{service,binding="ai",operation,outcome}`
- `wdl_binding_operation_duration_ms{service,binding="ai",operation}`

Pool state 只使用有界 label：

- `wdl_ai_pool_in_use{service,pool}`
- `wdl_ai_pool_high_water{service,pool}`
- `wdl_ai_pool_events_total{service,pool,outcome}`

Host rejection 记录 `ai_binding_request_rejected`；被拒绝的 Control AI mutation 通过共享 Control request log 记录有界 structured event。Namespace、worker、version 和 raw error text 进入日志，不进入 metric label。

## 部署 / Rollout 注意事项

AI 的跨 tier 变化遵循 [infra rollout 注意事项](infra.zh.md#部署--rollout-注意事项)中的 receiver-before-sender 流程。Provider availability 必须从目标部署的真实 user-runtime 与 do-runtime 出口完成资格确认；local fake-provider integration 只能证明 protocol 与 secret boundary，不能证明区域 provider availability。

## 测试

- `tests/unit/ai-contract.test.js` 与 Rust `ai_contract_fixture_matches_rust_readers` 测试固定共享 persisted/resolver grammar 和 exact official destination。
- `tests/unit/control-ai-handler.test.js` 覆盖 revision CAS、encryption、canonical state、aggregate limit、credential index validation 和 residual cleanup。
- `tests/unit/runtime-ai-client.test.js` 与 `tests/unit/runtime-ai-binding.test.js` 覆盖 facade option、官方 destination、byte/frame bound、SSE terminal、慢上传 cancellation、watchdog、pool lease transfer 与 WebSocket model pinning。
- `tests/integration/ai-binding.test.js` 覆盖 positional 与 live imported env、显式禁用 importable env、loaded-module catalog 复用、credential 与 upstream model 轮换、zero-Worker recreation、JSON/SSE、Responses 与 Realtime WebSocket、OpenAI SDK、credential non-exposure、终止 user-runtime/do-runtime loss 和 DO caller teardown。

Integration 使用仓库内 fake official provider。真实 credential 绝不能提交或打印到测试输出。

## 已知约束与非目标

- 不接受任意 OpenAI-compatible endpoint。新增 provider 必须增加经过 review 的精确 destination adapter 和 conformance test。
- 没有 managed catalog、平台共享 credential、usage aggregation、billing、quota、distributed fairness 或自动 provider failover。
- 未实现 `toMarkdown()`、异步 batch inference、background Responses、stored file API、WebRTC、SIP 或自动 tool execution。
- DeepSeek non-text input 或 output、continuation（`previous_response_id`/conversation state）、stored responses、embeddings 与 WebSocket transport 会在 provider I/O 前被拒绝。
- Provider-native warning 与 semantic event 原样透传；WDL 不发明私有 provider event protocol。
- Durable Object 内的 AI WebSocket 仍是普通 outbound session。DO hibernation 不会让 provider socket durable 或可重连。
