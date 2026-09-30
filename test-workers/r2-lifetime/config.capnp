using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    (name = "probe", worker = (
      modules = [
        (name = "worker", esModule = embed "probe.js"),
        (name = "shared-s3-request", esModule = embed "../../shared/s3-request.js"),
      ],
      compatibilityDate = "2026-04-24",
      compatibilityFlags = ["nodejs_compat"],
      bindings = [
        (name = "BUCKET", service = (name = "r2", entrypoint = "R2Bucket",
          props = (json = "{\"ns\":\"demo\",\"bucketName\":\"uploads\"}"))),
        (name = "BACKEND", service = "backend"),
      ],
    )),
    (name = "r2", worker = (
      modules = [
        (name = "worker", esModule = embed "../../runtime/bindings/r2.js"),
        (name = "@wdl-dev/aws-sigv4", esModule = embed "../../shared/vendor/aws-sigv4.js"),
        (name = "runtime-metrics", esModule = "export function recordBindingOperation(_service, _binding, _operation, callback) { return callback(); }"),
        (name = "runtime-bindings-proxy", esModule = "export function serviceNameFromEnv() { return 'user-runtime'; }"),
        (name = "runtime-r2-utils", esModule = embed "../../runtime/r2-utils.js"),
        (name = "runtime-bindings-r2-metadata", esModule = embed "../../runtime/bindings/r2/metadata.js"),
        (name = "runtime-bindings-r2-xml", esModule = embed "../../runtime/bindings/r2/xml.js"),
        (name = "shared-s3-xml", esModule = embed "../../shared/s3-xml.js"),
        (name = "shared-s3-request", esModule = embed "../../shared/s3-request.js"),
        (name = "shared-s3-retry", esModule = embed "../../shared/s3-retry.js"),
        (name = "shared-base64", esModule = embed "../../shared/base64.js"),
        (name = "shared-respond", esModule = embed "../../shared/respond.js"),
        (name = "respond.js", esModule = embed "../../shared/respond.js"),
        (name = "shared-bounded-body", esModule = embed "../../shared/bounded-body.js"),
      ],
      compatibilityDate = "2026-04-24",
      compatibilityFlags = ["nodejs_compat"],
      globalOutbound = "backend",
      bindings = [
        (name = "R2_S3_ENDPOINT", text = "http://s3.test"),
        (name = "R2_S3_BUCKET", text = "wdl-r2"),
        (name = "R2_S3_ACCESS_KEY_ID", text = "test"),
        (name = "R2_S3_SECRET_ACCESS_KEY", text = "test"),
      ],
    )),
    (name = "backend", worker = (
      modules = [(name = "worker", esModule = embed "backend.js")],
      compatibilityDate = "2026-04-24",
    )),
  ],
);
