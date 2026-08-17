using Workerd = import "/workerd/workerd.capnp";
using Base = import "config.capnp";

const config :Workerd.Config = (
  services = [
    (name = "do-runtime", worker = .Base.doRuntimeEvictableWorker),
    (name = "d1-runtime", external = (address = "d1-runtime:8787", http = ())),
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
    (name = "ai-public-network", network = (
      allow = ["public"],
      tlsOptions = (trustBrowserCas = true),
    )),
  ],

  sockets = [
    (name = "http", address = "*:8788", http = (), service = "do-runtime"),
  ],
);
