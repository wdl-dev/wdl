# Test Worker Fixtures

`test-workers/` 保存 `tests/integration/` 拥有的 worker fixture。测试可以依赖这棵树里的文件名、manifest 形状、binding 名称和响应载荷，所以 fixture 变化应和消费它的测试放在同一个 commit。三种 fixture 结构并存是有意的。

## Full Workspace（`package.json` + `wrangler.toml`/`wrangler.jsonc` + `src/`）

当测试通过 CLI 部署时，使用 full workspace：

```js
runWdlCli(["deploy", "test-workers/<name>", "--ns", ns]);
```

CLI 会像读取外部项目一样读取 manifest 和 package metadata，所以这种形状覆盖完整 deploy path。当测试重点是 deploy/lifecycle 流程，或者你希望迭代时 fixture 仍可用 Wrangler 单独运行时，选择这种布局。

如果一个测试需要 CLI 覆盖、另一个测试需要直接 source input，full-workspace fixture 也可以被读取为源码；这种双重用途要在 owner test 中保持明确。

## Source-Only（只有 `src/index.js`）

当 integration helper 把 worker source 读成字符串并内联到 programmatic deploy payload 时，使用 source-only fixture：

```js
const SOURCE = readFileSync(
  new URL("../../../test-workers/<name>/src/index.js", import.meta.url),
  "utf8"
);
```

测试不走 CLI 时选择这种布局，通常是因为 helper 直接构造 deploy body，把 fixture 留在树内但不塞进测试文件。这里不需要 Wrangler config；不要添加。

## Embedded Workerd Service（`config.capnp` + `src/`）

当本地 workerd 配置需要一个不属于 tenant code 的 hermetic 协议 peer 时，使用 embedded service。local 配置导入 fixture 的 `config.capnp`；production 配置仍使用真实 network 或 service binding。`ai-provider` 使用这种形态覆盖本地集成测试中的 AI provider 协议。

## 添加或移动 Fixture

- 通过 `runWdlCli(["deploy", ...])` 部署，意味着 full workspace。
- helper 通过 `readFileSync` inline 读取，意味着 source-only。
- local workerd 配置将其作为协议 peer 导入，意味着 embedded service。
- 没有 owner test 理由时，不要在同一个 fixture 中混用布局。
- 不要把 `examples/` 当成隐藏测试依赖。如果某个 demo 变成测试合同，应把最小 fixture 移入或复制到 `test-workers/`。
- 本地 `node_modules/`、`.wrangler/` 和 `.deploy-dist/` 目录是 install/build output，不能变成 fixture 合同。
