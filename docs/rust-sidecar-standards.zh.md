# Rust Service 和 Sidecar 标准

本文定义 WDL Rust service 和 sidecar 代码的默认标准，并补充 `project-standards.zh.md`。新增 Rust 代码和重构现有 Rust 代码都应遵循它。它不是按行数拆文件的规则，也不是按时间记录的 refactor log。

## 范围

这些标准覆盖：

- `scheduler`
- `workflows`
- `redis-proxy`
- `supervisor`
- `wdl-rust-common`（`rust/common/`）

Rust crate 位于 `rust/` 下的 Cargo workspace 中。`rust/Cargo.lock` 是共享 dependency lock；member crate 不应再增加自己的 lockfile。源码目录按服务命名（`rust/scheduler/`、`rust/workflows/`、`rust/redis-proxy/`、`rust/supervisor/`、`rust/common/`），service package / binary 名也与目录名一致；共享 helper crate 保留明确的 `wdl-rust-common` 名称。

每个 Rust crate 可以保留适合自身 runtime、shutdown、logging 或 protocol 语义的本地结构。一致性指 ownership 和 validation discipline 一致，不是强迫每个 crate 使用相同文件布局。

## 语言和工具链

所有 Rust crate 都使用 `edition = "2024"`。工具链 pin 在 `rust/rust-toolchain.toml`；当前基线是 Rust 1.96.0。应优先使用该基线上可用的现代语法和标准库 API；不要只是因为习惯而保留旧写法。新的语法或 API 只有在 CI / build image 能编译、且相关 runtime dependency 支持时才进入可 review 的代码。`rust-toolchain.toml` 是唯一 source of truth：本地与 CI 的 rust job 直接读它，两个 Dockerfile 也把它 copy 进 build，让 `rust:1-alpine` base 的 `rustup` 安装同一个 pin 的工具链，而不是跟着滚动镜像自带的版本走。因此 Rust 升级就是对 `rust-toolchain.toml` 的一次显式修改。

当前允许的 idiom：

- Edition 2024 语法和 module path。
- 用 `let ... else` 表达 parse / validation 的 early return。
- 用 `OnceLock`、atomic、mutex 和 task-owned state，避免 `static mut`。
- 当 validation predicate 更清楚时，可以用 `is_some_and` / `is_none_or`。
- 对已经做过 domain check 的 signedness conversion，可以用 `cast_signed()` / `cast_unsigned()` 代替不清楚的 `as`。
- 可能窄化的 numeric conversion 应使用显式 `TryFrom` / `try_from`。

Modern-by-default 不等于忽略兼容性。需要 review 特别注意的语法和 API：

- 不使用 nightly feature、`#![feature(...)]`、unstable cargo flag 或 edition preview 语法。
- Edition 2024 保留 `gen`；当 wire/storage field 字面就是 `gen` 时使用 `r#gen`，不要为了避开 raw identifier 而改外部字段名。
- 对 Rust 2024 的 `impl Trait` lifetime capture 要显式处理。如果 crate 内 public helper 的返回类型依赖微妙的推断 capture，优先使用具体返回类型、命名 generic 或本地 helper struct。
- 普通 service 逻辑不要引入 `unsafe`、unsafe attribute、raw pointer 或 `static mut`。如果平台边界确实需要 unsafe，应封装在很小的 module 内，并写 safety comment 和测试。
- 复杂 match 中不要依赖难读的 pattern ergonomics。若 edition 变化让 binding mode 不明显，就把引用/ownership pattern 写清楚。
- 当 CI 和 Docker build image 都支持时，应使用刚稳定的新标准库 API。如果某个 crate dependency 无法配合新写法，应在局部保留旧形式，并在 owning module 或 review notes 里说明原因。Redis access 是常见例子：周边 Rust 语法应尽量现代化，但不要强行采用当前 `redis` crate 版本无法清楚表达的 API 形状。

## 所有权

优先使用中等大小、owner 清晰的文件，而不是很多很小的 helper 文件。一个 module 应拥有 reviewer 能命名、能验证的行为。

合适的拆分边界包括：

- 有不同 request/response contract 的 protocol handler
- 有独立并发或 fence 规则的 state machine
- Redis key family 和 cursor envelope
- background task orchestration
- limits、stable error mapping、route name 等 policy surface
- 保护本地 invariant 的 module-specific tests

Tiny module 只有在命名稳定 owner 时才合适，例如 key family、limit surface、cursor envelope、error mapping 或目录级 `mod` glue。不要为了把长文件挪走几行而新建文件。反过来，如果一个大文件能让一个 state machine 或 protocol path 端到端可读，也可以保留。

同一标准适用于所有 service：

- `scheduler` 可以拆分 cron、queue、remote tick、registry、orphan 和 delivery 逻辑，因为它们是不同 background loop 或 queue state machine。
- `workflows` 可以拆分 create、execution、lifecycle、tick、replay/history、payload、identity、routing 和 schema 逻辑，因为它们分别拥有不同 API surface、Lua/fence path 或 Redis key family。
- `redis-proxy` 可以把 KV、queue、logs、runtime-load 和 secrets 保持为中等大小 handler；如果继续拆只会分散单个 HTTP protocol path，就不要拆。
- `supervisor` 可以保持紧凑，因为 config、process、renew、drain 和 logging 已经是清楚的 owner。
- `common` 应保持多个小 primitive owner；这些 module 是 shared contract，不是 service state machine。

不要只因为文件长而拆分。如果拆分会让 state transition、retry rule 或 protocol response 更难端到端阅读，就保留在同一个 owner 中。

## Import 和 Re-export

生产 leaf module 应显式 import 自己使用的名字。避免在 service 逻辑中使用 `use super::*`、`use crate::*` 或本地 broad glob import，因为它们会隐藏真实 dependency surface，并让 parent module 变化时更容易产生漂移。

Crate root 和目录级 `mod` glue 可以保留既有的显式本地 prelude，例如 `pub(crate) use config::*` 或 `pub(crate) use state::*`。这是 crate wiring convention，不代表 leaf module 可以依赖巨大隐式 parent scope。如果某个 module 只需要 sibling owner 的少量类型，应直接 import 这些类型。

当 colocated test 有意测试 private module item 时，可以使用 `use super::*`。不要只为了移除这个 idiom 而重写 colocated tests，除非它确实降低了 review 或 drift 风险。

## 测试

当测试保护一个 module 的行为时，应尽量 colocate。只有 crate-wide style contract 或跨模块 invariant 才放在 central tests。

例子：

- Scheduler 的 cron、queue、runtime-client、workflow-tick、state、observability 和 time 行为应放在对应 owner 附近。
- `redis-proxy` 的 KV、queue、logs、runtime-load 和 app error response 行为应放在对应 owner 附近。
- Supervisor 的 config、log、drain 和 shared helper 测试应放在这些小 owner 附近；production 文件不需要为了对称而拆。
- Workflows 可以保留 crate-wide tests 来保护跨模块 contract；local execution/history/payload invariant 在可行时应放到 owner 附近。

移动测试只有在降低后续 review 和 drift 风险时才有价值。不要为了增加测试数量而加测试。

## 共享代码

`wdl-rust-common`（`rust/common/`）是本批次唯一的 shared Rust helper crate。它只拥有必须跨 crate 保持一致的小 primitive，例如环境变量数字解析、日志等级解析、HTTP health probe、shutdown/in-flight tracking、通用 JSON log-line emission、wall-clock millisecond helper、短 non-cryptographic random hex suffix、稳定 non-cryptographic hash、queue Redis key builders、worker version / bundle-key parsing、Prometheus metric storage/formatting、Prometheus text response、结构化错误字段合并、internal-auth token/header matching、Redis command construction helper、中立的 Redis connection execution wrapper，以及 UTF-8 安全的字符串截断。它不能变成 service 行为的杂物箱。Axum-facing helper 必须 feature-gate，因此 D1/DO supervisor binaries 这类非 HTTP consumer 不需要付 HTTP stack 成本。

`test-support` feature 暴露 Rust service tests 共用的唯一 process-environment override helper。它在同一 test process 的 module 之间串行化 override，并在 unwind 时恢复全部值；production dependency build 不启用该 feature。

当 sidecar/service 在这些方面行为不同，本地显式代码仍然更好：

- service-specific shutdown/drain timing 和 shutdown log event
- logging call site、event name 和 request completion 语义
- Redis access pattern
- runtime dispatch 和 retry 语义
- protocol-specific error mapping

当更多共享代码确实有必要时，先定义 owner 和 contract。不要只为了减少几行本地代码而加 shared helper。

共享 primitive 的规则：

- 如果两个 crate 必须生成或解析同一个 Redis key shape，该 shape 应属于 `wdl-rust-common` 或一个明确命名的 owner。不要在多个 crate 中复制 FNV hash constants、queue key builders、worker bundle key parsing 或 version-tag grammar。
- Shared helper 应保持语义中性。例如 64-bit random hex suffix helper 如果也用于 pending-create token，就不应命名成 scheduler 或 workflows 专属 instance id helper。
- 不要把 service-specific lifecycle、retry、Redis transaction 或 protocol-response 行为放进 `wdl-rust-common`；这些逻辑应留在拥有该 state machine 的 service crate 中。
- `wdl-rust-common` 中的 Redis helper 只能从显式 keys/args 构造命令，或针对显式 `ConnectionManager` 运行调用方传入的 closure。script body、选择哪条 connection、retry/timeout 行为、error mapping 和 state ownership 仍由 service crate 拥有。
- `wdl-rust-common` 中的 HTTP framework helper 必须放在 crate 的 `axum` feature 后面。只使用非 HTTP primitive 的 sidecar 应关闭 default features。

## Redis、错误和 Schema Contract

Redis-facing 代码应显式表达 key ownership、error classification 和 schema 行为。

- 分类 Redis server error 时，应使用稳定 error code（`err.code()`），不要在格式化后的错误字符串上做 substring matching。测试可以搜索 Lua source string 来保护脚本内容，但 runtime 逻辑不应这么做。
- Service error type 应拥有 machine code、human message 和 HTTP status。不要在独立 server 层再从 string code 反向推导 HTTP status。
- Redis schema marker 不符合 service contract 时应 fail closed。维护窗口 migration 已完成后，不要继续保留假的兼容、markerless adoption 或 destructive reset command。未来如果需要 migration，应作为新的显式 migration path 设计。
- 如果某个 service 写 runtime claim、token、lease 或 generation fence，这些 fence fields 应由该 service 的 state machine 拥有，并由本地测试保护。避免在多个 module 中复制同一套 fence key derivation。

## 验证

修改 Rust 前，先识别什么行为应捕获 regression。如果覆盖不足，应在同一改动中补或重塑相关测试。

Workspace manifest 拥有每个 sidecar crate 使用的基础 Clippy lint groups。Service crate
应通过 `[lints] workspace = true` opt in；CI 仍把 Clippy warnings 提升为 errors，因此
manifest 是 lint scope 的来源，门禁仍保持严格。

每个被修改 Rust crate 的基础检查，从 `rust/` 目录运行：

```bash
cargo fmt --package <package> --check
cargo check --locked -p <package>
cargo test --locked -p <package>
cargo clippy --locked --all-targets -p <package> -- -D warnings
```

完整 Rust sweep 也从 `rust/` 目录运行同类命令，并按 cargo subcommand 使用 `--all` 或 `--workspace`。

可执行行为变化需要跑定向集成：

- `workflows` dispatch、lifecycle、execution 或 API contract 变化需要 workflows integration。
- `scheduler` cron、queue、workflow tick 或 shutdown/drain 变化需要对应 cron、queue、workflows 或 shutdown-drain integration。
- `redis-proxy` KV、log、queue 或 routing protocol 变化需要覆盖受影响 binding/runtime path 的 integration。
- `supervisor` drain、renew 或 process 行为变化需要定向 D1/DO rolling 或 drain 覆盖；如果没有可用集成测试，应说明为什么 unit coverage 是最强本地 gate。

纯测试 colocate 和 Rust module reshaping 需要相关 crate gates 和 `git diff --check`；除非改变可执行行为，否则不需要 integration。纯文档改动需要 `git diff --check`、必要的链接/路径检查；如果文档导航或 source-scan 规则变化，再跑 style-contract 测试。

## 例子

这些例子说明如何应用标准，不是完整的允许/禁止文件列表。

- Scheduler background task orchestration 可以从 server bootstrap 拆出，因为它是有 lifecycle 行为的命名 owner。
- Scheduler queue delivery、retry planning 和 runtime response handling 如果形成连续 delivery state machine，就应放在一起。
- KV cursor envelope 如果 cursor parsing 和 serialization 是稳定 protocol surface，可以从 `redis-proxy` handler 拆出。
- `redis-proxy` KV 和 queue protocol handler 如果继续拆会分散 request handling 且不产生新 owner，就应保持在一起。
- Workflow limit 或 route-name module 如果定义稳定 policy surface，短文件也可以保留。
- Supervisor 当前 production 文件已经清楚映射到 config、logging、drain、process 和 entrypoint ownership，不应为了对称而拆分。

## Refactor 纪律

本仓库 Rust service 和 sidecar refactor 遵循与其他 WDL refactor 相同的 staged-review 纪律：

- 编辑前定义一个可独立部署的边界
- 不把无关 cleanup 混入当前边界
- stage 完整边界供 review
- feedback fix 在被确认前保持 unstaged
- 只有相关 crate gate 和定向 integration 通过后才 commit

如果 refactor 同时修复行为 bug，要在改动中明确说明，并运行覆盖该行为的 integration。不要把行为修复描述成纯结构调整。
