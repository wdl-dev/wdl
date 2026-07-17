using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    (name = "gateway", worker = .gatewayWorker),

    # Loader pools — one external per runtime container. gateway picks
    # RUNTIME_USER vs RUNTIME_SYSTEM by namespace (`__system__` → system,
    # others → user) in index.js. Names match the SC DNS aliases set in
    # terraform / compose.
    (name = "runtime-user",   external = (address = "user-runtime:8081",   http = ())),
    (name = "runtime-system", external = (address = "system-runtime:8081", http = ())),

    # Control plane short-circuit: requests landing on ADMIN_HOST bypass
    # the ns routing tables entirely and hit the static control worker on
    # system-runtime's :8082 socket. Keeps admin traffic independent of
    # Redis (admin reachability survives Redis outages / FLUSHALL).
    (name = "control-ext", external = (address = "system-runtime:8082", http = ())),

    # Gateway's own outbound (Redis, SUBSCRIBE long-lived connection).
    (name = "network", network = (
      allow = ["private", "public"],
      tlsOptions = (trustBrowserCas = true),
    )),
  ],

  sockets = [
    # Single published socket — /healthz and /_metrics share this port.
    (name = "http", address = "*:8080", http = (), service = "gateway"),
  ],
);

const gatewayWorker :Workerd.Worker = (
  modules = [
    (name = "worker", esModule = embed "index.js"),
    (name = "gateway-holder", esModule = embed "holder.js"),
    (name = "gateway-dispatch", esModule = embed "dispatch.js"),
    (name = "gateway-lib", esModule = embed "lib.js"),
    (name = "gateway-runtime", esModule = embed "runtime.js"),
    (name = "gateway-websocket", esModule = embed "websocket.js"),
    (name = "shared-worker-id", esModule = embed "../shared/worker-id.js"),
    (name = "ns-pattern.js", esModule = embed "../shared/ns-pattern.js"),
    (name = "worker-contract.js", esModule = embed "../shared/worker-contract.js"),
    (name = "shared-redis", esModule = embed "../shared/redis.js"),
    (name = "shared-redis-command-client", esModule = embed "../shared/redis-command-client.js"),
    (name = "shared-redis-resp", esModule = embed "../shared/redis-resp.js"),
    (name = "shared-redis-session", esModule = embed "../shared/redis-session.js"),
    (name = "shared-redis-subscriber", esModule = embed "../shared/redis-subscriber.js"),
    (name = "hex.js", esModule = embed "../shared/hex.js"),
    (name = "errors.js", esModule = embed "../shared/errors.js"),
    (name = "shared-route-projection", esModule = embed "../shared/route-projection.js"),
    (name = "shared-worker-contract", esModule = embed "../shared/worker-contract.js"),
    (name = "shared-ns-pattern", esModule = embed "../shared/ns-pattern.js"),
    (name = "shared-observability", esModule = embed "../shared/observability.js"),
    (name = "shared-respond", esModule = embed "../shared/respond.js"),
    (name = "shared-request-scope", esModule = embed "../shared/request-scope.js"),
  ],
  compatibilityDate = "2026-04-24",
  globalOutbound = "network",
  # WS upgrades are dispatched into this DO so the 101 fetch handler runs
  # on an actor IoContext (skipped by `IoContext::abortFromHang`).
  # Do not set preventEviction here: gateway uses one holder actor per
  # session, and the accepted socket keeps the active actor alive while
  # allowing completed sessions to be collected.
  durableObjectNamespaces = [
    (className = "GatewayWsHolder", uniqueKey = "gateway-ws-holder-v1"),
  ],
  durableObjectStorage = (inMemory = void),
  bindings = [
    (name = "RUNTIME_USER",   service = "runtime-user"),
    (name = "RUNTIME_SYSTEM", service = "runtime-system"),
    (name = "CONTROL",        service = "control-ext"),
    (name = "WS_HOLDER", durableObjectNamespace = "GatewayWsHolder"),
    (name = "REDIS_ADDR",     fromEnvironment = "REDIS_ADDR"),
    (name = "PLATFORM_DOMAIN", fromEnvironment = "PLATFORM_DOMAIN"),
    (name = "ADMIN_HOST",     fromEnvironment = "ADMIN_HOST"),
    (name = "LOG_LEVEL",      fromEnvironment = "LOG_LEVEL"),
    (name = "WEBSOCKET_RECONNECT_DELAYS_MS", fromEnvironment = "WEBSOCKET_RECONNECT_DELAYS_MS"),
    (name = "WEBSOCKET_MAX_BUFFERED_MESSAGES", fromEnvironment = "WEBSOCKET_MAX_BUFFERED_MESSAGES"),
  ],
);
