using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
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
  ],
);
