using Workerd = import "/workerd/workerd.capnp";

# user-runtime: workerLoader for tenant workers plus the tail worker.
# The loader source parallels config-system.capnp; loaded tenant workers
# get `public-network`, while runtime/tail internals keep private reach
# through `internal-network`.

const config :Workerd.Config = (
  services = [
    (name = "loader", worker = .loaderWorker),
    (name = "tail-worker", worker = .tailWorker),
    (name = "do-owner-network", worker = .doOwnerNetworkWorker),
    (name = "d1-runtime", external = (address = "d1-runtime:8787", http = ())),
    (name = "do-runtime", external = (address = "do-runtime:8788", http = ())),
    (name = "workflows", external = (address = "workflows:9120", http = ())),

    # Runtime's own outbound — needs private reach for Redis / S3mock.
    (name = "internal-network", network = (
      allow = ["private", "public"],
      tlsOptions = (trustBrowserCas = true),
    )),

    # Loaded tenant workers' outbound (via env.PUBLIC_NETWORK). Explicit
    # allow=["public"] keeps the internal service mesh unreachable —
    # tenants fetch("http://user-runtime:8081/...") etc. are TCP-blocked.
    (name = "public-network", network = (
      allow = ["public"],
      tlsOptions = (trustBrowserCas = true),
    )),
  ],

  sockets = [
    (name = "loader", address = "*:8081", http = (), service = "loader"),
    (name = "internal", address = "*:8088", http = (), service = (name = "loader", entrypoint = "internal")),
  ],
);

const loaderWorker :Workerd.Worker = (
  modules = [
    (name = "worker", esModule = embed "index.js"),
    (name = "runtime-internal", esModule = embed "internal.js"),
    (name = "runtime-lib", esModule = embed "lib.js"),
    (name = "runtime-dispatch", esModule = embed "dispatch.js"),
    (name = "runtime-dispatch-workflow-json", esModule = embed "dispatch/workflow-json.js"),
    (name = "runtime-dispatch-workflow-replay-cache", esModule = embed "dispatch/workflow-replay-cache.js"),
    (name = "runtime-dispatch-workflow-step", esModule = embed "dispatch/workflow-step.js"),
    (name = "runtime-tail-forwarder", esModule = embed "tail-forwarder.js"),
    (name = "runtime-load", esModule = embed "load.js"),
    (name = "runtime-load-env-build", esModule = embed "load/env-build.js"),
    (name = "runtime-load-code-budget", esModule = embed "load/code-budget.js"),
    (name = "runtime-load-injection-sources", esModule = embed "load/injection-sources.js"),
    (name = "runtime-load-module-rewrite", esModule = embed "load/module-rewrite.js"),
    (name = "runtime-load-wrapper-generate", esModule = embed "load/wrapper-generate.js"),
    (name = "runtime-metrics", esModule = embed "metrics.js"),
    (name = "runtime-bindings-proxy", esModule = embed "bindings/proxy.js"),
    (name = "runtime-state", esModule = embed "runtime.js"),
    (name = "runtime-bindings-kv", esModule = embed "bindings/kv.js"),
    (name = "runtime-bindings-assets", esModule = embed "bindings/assets.js"),
    (name = "runtime-bindings-service", esModule = embed "bindings/service.js"),
    (name = "runtime-bindings-queue", esModule = embed "bindings/queue.js"),
    (name = "runtime-bindings-d1", esModule = embed "bindings/d1.js"),
    (name = "runtime-bindings-r2", esModule = embed "bindings/r2.js"),
    (name = "runtime-bindings-r2-metadata", esModule = embed "bindings/r2/metadata.js"),
    (name = "runtime-bindings-r2-xml", esModule = embed "bindings/r2/xml.js"),
    (name = "runtime-bindings-do", esModule = embed "bindings/do.js"),
    (name = "runtime-bindings-internal-auth-backend", esModule = embed "bindings/internal-auth-backend.js"),
    (name = "runtime-do-transport", esModule = embed "_wdl-do-transport.js"),
    (name = "_wdl-request-id.js", esModule = embed "_wdl-request-id.js"),
    (name = "shared-owner-endpoint", esModule = embed "../shared/owner-endpoint.js"),
    # Injected DO transport uses a relative module name; host modules use the
    # shared bare name. Both resolve to the same shared contract owner.
    (name = "_wdl-owner-endpoint.js", esModule = embed "../shared/owner-endpoint.js"),
    (name = "runtime-owner-hint-cache", esModule = embed "_wdl-owner-hint-cache.js"),
    (name = "shared-d1-data-field", esModule = embed "../shared/d1-data-field.js"),
    (name = "shared-d1-params", esModule = embed "../shared/d1-params.js"),
    (name = "shared-d1-query-wire", esModule = embed "../shared/d1-query-wire.js"),
    (name = "runtime-d1-client-source", text = embed "d1-client.js"),
    (name = "runtime-d1-data-field-source", text = embed "../shared/d1-data-field.js"),
    (name = "runtime-d1-params-source", text = embed "../shared/d1-params.js"),
    (name = "runtime-sql-splitter-source", text = embed "../shared/sql-splitter.js"),
    (name = "runtime-d1-transport-source", text = embed "../shared/d1-transport.js"),
    (name = "runtime-r2-client-source", text = embed "r2-client.js"),
    (name = "runtime-r2-utils-source", text = embed "r2-utils.js"),
    (name = "runtime-do-client-source", text = embed "do-client.js"),
    (name = "runtime-do-transport-source", text = embed "_wdl-do-transport.js"),
    (name = "runtime-owner-endpoint-source", text = embed "../shared/owner-endpoint.js"),
    (name = "runtime-owner-hint-cache-source", text = embed "_wdl-owner-hint-cache.js"),
    (name = "runtime-request-id-source", text = embed "_wdl-request-id.js"),
    (name = "runtime-workflows-client-source", text = embed "workflows-client.js"),
    (name = "runtime-r2-utils", esModule = embed "r2-utils.js"),
    (name = "hex.js", esModule = embed "../shared/hex.js"),
    (name = "errors.js", esModule = embed "../shared/errors.js"),
    (name = "shared-errors", esModule = embed "../shared/errors.js"),
    (name = "shared-base64", esModule = embed "../shared/base64.js"),
    (name = "shared-worker-contract", esModule = embed "../shared/worker-contract.js"),
    (name = "shared-observability", esModule = embed "../shared/observability.js"),
    (name = "shared-s3-xml", esModule = embed "../shared/s3-xml.js"),
    (name = "shared-ns-pattern", esModule = embed "../shared/ns-pattern.js"),
    (name = "shared-workerd-compat-flags", esModule = embed "../shared/workerd-compat-flags.js"),
    (name = "shared-respond", esModule = embed "../shared/respond.js"),
    (name = "shared-s3-retry", esModule = embed "../shared/s3-retry.js"),
    (name = "respond.js", esModule = embed "../shared/respond.js"),
    (name = "shared-bounded-body", esModule = embed "../shared/bounded-body.js"),
    (name = "shared-request-scope", esModule = embed "../shared/request-scope.js"),
    (name = "shared-env", esModule = embed "../shared/env.js"),
    (name = "shared-worker-id", esModule = embed "../shared/worker-id.js"),
    (name = "shared-internal-auth", esModule = embed "../shared/internal-auth.js"),
    (name = "ns-pattern.js", esModule = embed "../shared/ns-pattern.js"),
    (name = "worker-contract.js", esModule = embed "../shared/worker-contract.js"),
    (name = "shared-d1-timeout", esModule = embed "../shared/d1-timeout.js"),
    (name = "@wdl-dev/aws-sigv4", esModule = embed "../shared/vendor/aws-sigv4.js"),
  ],
  compatibilityDate = "2026-04-24",
  # service_binding_extra_handlers exposes stub.queue()/scheduled() on
  # Fetcher stubs returned by workerLoader.get(). Runtime-only flag.
  compatibilityFlags = ["nodejs_compat", "service_binding_extra_handlers"],
  globalOutbound = "internal-network",
  bindings = [
    (name = "SERVICE_NAME", text = "user-runtime"),
    (name = "REDIS_PROXY_URL", fromEnvironment = "REDIS_PROXY_URL"),
    (name = "WDL_INTERNAL_AUTH_TOKEN", fromEnvironment = "WDL_INTERNAL_AUTH_TOKEN"),
    (name = "WDL_INTERNAL_AUTH_PREVIOUS_TOKEN", fromEnvironment = "WDL_INTERNAL_AUTH_PREVIOUS_TOKEN"),
    (name = "ASSETS_CDN_BASE", fromEnvironment = "ASSETS_CDN_BASE"),
    (name = "LOG_LEVEL", fromEnvironment = "LOG_LEVEL"),
    (name = "LOADER", workerLoader = (id = "dynamic")),
    (name = "TAIL_WORKER", service = "tail-worker"),
    (name = "PUBLIC_NETWORK", service = "public-network"),
    (name = "D1_BACKEND", service = "d1-runtime"),
    (name = "DO_BACKEND", service = "do-runtime"),
    (name = "WORKFLOWS_BACKEND", service = "workflows"),
    (name = "DO_OWNER_NETWORK", service = "do-owner-network"),
    (name = "D1_QUERY_TIMEOUT_MS", fromEnvironment = "D1_QUERY_TIMEOUT_MS"),
    (name = "R2_S3_ENDPOINT", fromEnvironment = "R2_S3_ENDPOINT"),
    (name = "R2_S3_BUCKET", fromEnvironment = "R2_S3_BUCKET"),
    (name = "R2_S3_ACCESS_KEY_ID", fromEnvironment = "R2_S3_ACCESS_KEY_ID"),
    (name = "R2_S3_SECRET_ACCESS_KEY", fromEnvironment = "R2_S3_SECRET_ACCESS_KEY"),
    (name = "R2_S3_REGION", fromEnvironment = "R2_S3_REGION"),
  ],
);

const doOwnerNetworkWorker :Workerd.Worker = (
  modules = [
    (name = "worker", esModule = embed "do-owner-network.js"),
    (name = "shared-owner-endpoint", esModule = embed "../shared/owner-endpoint.js"),
    (name = "shared-internal-auth", esModule = embed "../shared/internal-auth.js"),
  ],
  compatibilityDate = "2026-04-24",
  globalOutbound = "internal-network",
  bindings = [
    (name = "WDL_INTERNAL_AUTH_TOKEN", fromEnvironment = "WDL_INTERNAL_AUTH_TOKEN"),
  ],
);

const tailWorker :Workerd.Worker = (
  modules = [
    (name = "worker", esModule = embed "tail-worker.js"),
    (name = "hex.js", esModule = embed "../shared/hex.js"),
    (name = "errors.js", esModule = embed "../shared/errors.js"),
    (name = "shared-observability", esModule = embed "../shared/observability.js"),
    (name = "runtime-tail-forwarder", esModule = embed "tail-forwarder.js"),
    (name = "runtime-bindings-proxy", esModule = embed "bindings/proxy.js"),
    (name = "shared-errors", esModule = embed "../shared/errors.js"),
    (name = "shared-respond", esModule = embed "../shared/respond.js"),
    (name = "shared-internal-auth", esModule = embed "../shared/internal-auth.js"),
  ],
  compatibilityDate = "2026-04-24",
  # tail-worker fetches the local Rust Redis proxy sidecar to forward
  # captured events under the activation gate. Reusing the runtime's
  # private-reach service keeps the loopback hop inside the container.
  globalOutbound = "internal-network",
  bindings = [
    (name = "SERVICE_NAME", text = "user-runtime-tail"),
    (name = "LOG_LEVEL", fromEnvironment = "LOG_LEVEL"),
    (name = "REDIS_PROXY_URL", fromEnvironment = "REDIS_PROXY_URL"),
    (name = "WDL_INTERNAL_AUTH_TOKEN", fromEnvironment = "WDL_INTERNAL_AUTH_TOKEN"),
  ],
);
