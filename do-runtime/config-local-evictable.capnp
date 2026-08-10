using Workerd = import "/workerd/workerd.capnp";
using Base = import "config.capnp";

const config :Workerd.Config = (
  services = [
    (name = "do-runtime", worker = .Base.doRuntimeEvictableWorker),

    # Default D1 router traffic goes through local Envoy to match
    # production's Service Connect path. Learned owner endpoints remain
    # direct because they intentionally target a specific D1 runtime task.
    (name = "d1-runtime", external = (address = "envoy:18787", http = ())),
    (name = "workflows", external = (address = "workflows:9120", http = ())),
    (name = "do-storage", disk = (path = "/data/do", writable = true)),
    (name = "internal-network", network = (
      allow = ["private", "public"],
      tlsOptions = (trustBrowserCas = true),
    )),
    (name = "public-network", network = (
      allow = ["public"],
      tlsOptions = (trustBrowserCas = true),
    )),
  ],

  sockets = [
    (name = "http", address = "*:8788", http = (), service = "do-runtime"),
  ],
);
