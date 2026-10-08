using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    (name = "lifecycle", worker = (
      compatibilityDate = "2026-04-24",
      modules = [(name = "lifecycle.js", esModule = embed "lifecycle.js")],
    )),
  ],
);
