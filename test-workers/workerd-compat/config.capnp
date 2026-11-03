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
    (name = "gc", worker = (
      compatibilityDate = "2026-04-24",
      compatibilityFlags = ["nodejs_compat"],
      modules = [(name = "gc.js", esModule = embed "gc.js")],
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
