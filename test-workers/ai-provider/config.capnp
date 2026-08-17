using Workerd = import "/workerd/workerd.capnp";

const providerWorker :Workerd.Worker = (
  modules = [
    (name = "worker", esModule = embed "src/index.js"),
  ],
  compatibilityDate = "2026-04-24",
);
