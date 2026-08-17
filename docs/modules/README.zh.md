# 模块文档地图

模块文档是当前模块设计的入口。每个模块文档都应讲清楚当前实现，以及后续改动必须保留的约束。下表列出的源码、当前设计文档和测试是刷新/复核模块文档时的输入；更新模块文档前，任何结论都必须重新对照当前代码核实。

跨模块协议改动还应阅读 [`../protocol-contracts.zh.md`](../protocol-contracts.zh.md)。面向 contributor 的阅读路径见 [`../contributing.zh.md`](../contributing.zh.md)。

## 当前模块

| 模块 | 目标文档 | 当前主要来源 |
|---|---|---|
| Gateway routing | `gateway.md`, `gateway.zh.md` | `gateway/`、gateway 集成测试 |
| Runtime loader 和 bindings | `runtime.md`, `runtime.zh.md` | `runtime/`、`shared/`、runtime 单元/集成测试 |
| AI binding | `ai.md`, `ai.zh.md` | `runtime/bindings/ai.js`、`runtime/ai-client.js`、`control/handlers/ai.js`、`rust/redis-proxy/src/ai.rs`、AI 测试 |
| CLI 和 Wrangler 输入 | `cli.md`、`cli.zh.md` | 下游 standalone CLI、CLI integration tests、README Quick Start / Deploy A Worker |
| Control 和 auth | `control-auth.md`, `control-auth.zh.md` | `control/`、`auth/`、`shared/auth-*` |
| Durable Objects | `durable-objects.md`, `durable-objects.zh.md` | `do-runtime/`、`runtime/do-client.js`、DO 测试 |
| D1 | `d1.md`, `d1.zh.md` | `d1-runtime/`、`runtime/bindings/d1.js`、D1 测试 |
| Queues 和 cron | `queues-cron.md`, `queues-cron.zh.md` | `rust/scheduler/`、runtime queue binding、control routing |
| Workflows | `workflows.md`, `workflows.zh.md` | `rust/workflows/`、`runtime/dispatch/workflow-*.js` |
| Log tail 和 observability | `log-tail-observability.md`, `log-tail-observability.zh.md` | `runtime/tail-worker.js`、`control/handlers/logs-tail.js`、`shared/observability.js` |
| Infra 和 deployment | `infra.md`, `infra.zh.md` | `terraform/`、`deploy/kubernetes/`、`.github/workflows/` |

## 模块文档合同

每个模块文档应覆盖：

- 目的和范围
- 当前实现
- 对外和内部接口
- Redis 或 storage 合同
- ownership、并发和失败语义
- 安全边界
- 可观测性
- 部署和 rollout 注意事项
- 保护该模块的测试
- 已知约束和非目标

同一个模块的英文和中文文件应在同一个提交中落地，除非临时 stub 被明确标注。
