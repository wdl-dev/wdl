using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    (name = "spike", worker = .spikeWorker),
    (name = "do-storage", disk = (path = "state", writable = true)),
    # Mirrors do-runtime's public-network: tenant workers get public egress.
    (name = "public-network", network = (
      allow = ["public"],
      tlsOptions = (trustBrowserCas = true),
    )),
  ],
  sockets = [
    (name = "http", address = "127.0.0.1:18080", http = (), service = "spike"),
  ],
);

const spikeWorker :Workerd.Worker = (
  modules = [
    (name = "worker", esModule = embed "host.js"),
    (name = "tenant-src", text = embed "tenant.js"),
    (name = "gadget-v1-src", text = embed "gadget-v1.js"),
    (name = "gadget-v2-src", text = embed "gadget-v2.js"),
  ],
  compatibilityDate = "2026-04-24",
  durableObjectNamespaces = [
    (className = "WdlDoHostActor", uniqueKey = "spike-host-v1", enableSql = true, preventEviction = true),
    (className = "GadgetHostActor", uniqueKey = "spike-gadget-host-v1", enableSql = true, preventEviction = true),
  ],
  durableObjectStorage = (localDisk = "do-storage"),
  bindings = [
    (name = "DO_HOSTS", durableObjectNamespace = "WdlDoHostActor"),
    (name = "GADGET_HOSTS", durableObjectNamespace = "GadgetHostActor"),
    (name = "LOADER", workerLoader = (id = "spike-loader")),
    (name = "PUBLIC_NETWORK", service = "public-network"),
  ],
);
