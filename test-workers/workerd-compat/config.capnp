using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  # Test-process only: gc() is exposed to all services, but only gc.js calls it.
  # Keep lifecycle's allocation-driven reclamation tests free of forced GC.
  v8Flags = ["--expose-gc"],
  services = [
    (name = "lifecycle", worker = (
      compatibilityDate = "2026-04-24",
      modules = [(name = "lifecycle.js", esModule = embed "lifecycle.js")],
    )),
    (name = "streams", worker = (
      compatibilityDate = "2026-04-24",
      compatibilityFlags = ["nodejs_compat"],
      modules = [(name = "streams.js", esModule = embed "streams.js")],
      bindings = [(name = "SELF", service = "streams")],
    )),
    (name = "diagnostics", worker = (
      compatibilityDate = "2026-04-24",
      compatibilityFlags = ["nodejs_compat"],
      modules = [(name = "diagnostics.js", esModule = embed "diagnostics.js")],
      bindings = [
        (name = "SOURCE", service = "diagnostics-source"),
        (name = "SINK", service = "diagnostics-sink"),
      ],
    )),
    (name = "diagnostics-source", worker = (
      compatibilityDate = "2026-04-24",
      # A 4096-byte cut splits this three-byte UTF-8 code point.
      modules = [(name = "worker.js", esModule = "export default { fetch() { const error = new Error('\\u4e2d'.repeat(2000)); error.name = 'LongError'; throw error; } };")],
      tails = ["diagnostics-sink"],
    )),
    (name = "diagnostics-sink", worker = (
      compatibilityDate = "2026-04-24",
      modules = [(name = "worker.js", esModule = "let last = null; export default { tail(events) { last = events[0]?.exceptions[0] ?? null; }, fetch() { return Response.json(last); } };")],
    )),
    (name = "gc", worker = (
      compatibilityDate = "2026-04-24",
      compatibilityFlags = ["nodejs_compat"],
      modules = [(name = "gc.js", esModule = embed "gc.js")],
    )),
    (name = "modules", worker = (
      compatibilityDate = "2026-04-24",
      compatibilityFlags = ["nodejs_compat", "legacy_module_registry"],
      modules = [
        (name = "modules.js", esModule = embed "modules.js"),
        (name = "require-entry.js", esModule = "import './require-pump.cjs'; export default 1;"),
        (name = "require-pump.cjs", commonJsModule = "require('./require-cycle.js');"),
        (name = "require-cycle.js", esModule = "import './require-entry.js'; export const value = 1;"),
        (name = "dynamic-entry.js", esModule = "export const value = 1; export const chunk = import('./dynamic-chunk.js');"),
        (name = "dynamic-chunk.js", esModule = "import { value } from './dynamic-entry.js'; await 0; export const result = value + 1;"),
        (name = "invalid-root.js", esModule = "export * from './invalid-dep.js';"),
        (name = "invalid-sibling.js", esModule = "export * from './invalid-dep.js';"),
        (name = "invalid-dep.js", esModule = "export { default } from './healthy.js' with { invalid: 'value' };"),
        (name = "healthy.js", esModule = "export default 42;"),
      ],
    )),
    (name = "node-boundaries", worker = (
      compatibilityDate = "2026-04-24",
      compatibilityFlags = ["nodejs_compat"],
      modules = [(name = "node-boundaries.js", esModule = embed "node-boundaries.js")],
    )),
    (name = "rpc-streams", worker = (
      compatibilityDate = "2026-04-24",
      compatibilityFlags = ["nodejs_compat"],
      modules = [(name = "rpc-streams.js", esModule = embed "rpc-streams.js")],
      bindings = [
        (name = "SOURCE", service = "http-source"),
        (name = "SINK", service = "rpc-sink"),
      ],
    )),
    (name = "rpc-sink", worker = (
      compatibilityDate = "2026-04-24",
      compatibilityFlags = ["nodejs_compat"],
      modules = [(name = "rpc-stream-sink.js", esModule = embed "rpc-stream-sink.js")],
    )),
    (name = "http-source", external = (address = "127.0.0.1:8088", http = ())),
  ],
);
