/*
 * The experimental flags mirror workerd v1.20260701.1
 * src/workerd/io/compatibility-date.capnp. Refresh them on every workerd pin
 * bump from an upstream source checkout:
 *
 * node scripts/extract-workerd-experimental-compat-flags.mjs \
 *   /path/to/workerd/src/workerd/io/compatibility-date.capnp
 *
 * The dynamic-worker minimum/default dates and required error-serialization
 * behavior below are WDL policy rather than upstream mirror data.
 */

export const WORKERD_EXPERIMENTAL_COMPAT_FLAGS_SOURCE_VERSION = "1.20260701.1";

// WDL supports one forward-only dynamic-worker compatibility surface. Static
// platform workers keep their independently pinned workerd service dates.
export const MIN_DYNAMIC_WORKER_COMPATIBILITY_DATE = "2026-04-01";
export const DEFAULT_DYNAMIC_WORKER_COMPATIBILITY_DATE = "2026-04-24";
export const ENHANCED_ERROR_SERIALIZATION_DEFAULT_DATE = "2026-04-21";
export const ENHANCED_ERROR_SERIALIZATION_FLAG = "enhanced_error_serialization";
export const LEGACY_ERROR_SERIALIZATION_FLAG = "legacy_error_serialization";

export const WORKERD_EXPERIMENTAL_COMPAT_FLAGS = Object.freeze([
  "allow_insecure_inefficient_logged_eval",
  "allow_irrevocable_stub_storage",
  "auto_grpc_convert",
  "cache_reload_enabled",
  "connect_pass_through",
  "durable_object_get_existing",
  "durable_object_rename",
  "enable_abortsignal_rpc",
  "enable_ctx_version_metadata",
  "enable_d1_with_sessions_api",
  "enable_version_api",
  "enable_web_file_system",
  "experimental",
  "increase_websocket_message_size",
  "js_rpc",
  "kv_direct_binding",
  "memory_cache_delete",
  "new_module_registry",
  "precise_timers",
  "python_workers_development",
  "python_workers_durable_objects",
  "replica_routing",
  "rtti_api",
  "service_binding_extra_handlers",
  "spec_compliant_property_attributes",
  "streaming_tail_worker",
  "streams_no_default_auto_allocate_chunk_size",
  "tail_worker_user_spans",
  "typescript_strip_types",
  "unsafe_module",
  "unsupported_process_actual_platform",
  "webgpu",
  "workflows_step_rollback",
]);

const WORKERD_EXPERIMENTAL_COMPAT_FLAG_SET = new Set(WORKERD_EXPERIMENTAL_COMPAT_FLAGS);

/** @param {unknown} flag */
export function isWorkerdExperimentalCompatFlag(flag) {
  return typeof flag === "string" && WORKERD_EXPERIMENTAL_COMPAT_FLAG_SET.has(flag);
}

/** @param {unknown} flags */
export function firstWorkerdExperimentalCompatFlag(flags) {
  if (!Array.isArray(flags)) return null;
  for (const flag of flags) {
    if (isWorkerdExperimentalCompatFlag(flag)) return flag;
  }
  return null;
}
