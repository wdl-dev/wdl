using Workerd = import "/workerd/workerd.capnp";
using Base = import "config-user.capnp";
using AiTest = import "../test-workers/ai-provider/config.capnp";

const config :Workerd.Config = (
  services = [
    (name = "loader", worker = .Base.loaderWorker),
    (name = "tail-worker", worker = .Base.tailWorker),
    # Default D1/DO router traffic goes through local Envoy to match
    # production's Service Connect path. Binding-scoped DO host adapters use
    # the loader's internal network for learned owner endpoints.
    (name = "d1-runtime", external = (address = "envoy:18787", http = ())),
    (name = "do-runtime", external = (address = "envoy:18788", http = ())),
    (name = "workflows", external = (address = "workflows:9120", http = ())),

    (name = "internal-network", network = (
      allow = ["private", "public"],
      tlsOptions = (trustBrowserCas = true),
    )),

    (name = "public-network", network = (
      allow = ["public"],
      tlsOptions = (trustBrowserCas = true),
    )),
    (name = "ai-public-network", worker = .AiTest.providerWorker),
  ],

  sockets = [
    (name = "loader", address = "*:8081", http = (), service = "loader"),
    (name = "internal", address = "*:8088", http = (), service = (name = "loader", entrypoint = "internal")),
  ],
);
