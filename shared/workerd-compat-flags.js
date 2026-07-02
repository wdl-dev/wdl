/*
 * Mirrors workerd v1.20260701.1 src/workerd/io/compatibility-date.capnp.
 * Regenerate on every workerd pin bump from an upstream workerd source checkout:
 *
 * node --input-type=module -e 'import fs from "node:fs"; const src=fs.readFileSync("src/workerd/io/compatibility-date.capnp","utf8"); const blocks=[]; let cur=[]; for (const line of src.split(/\n/)) { if (/^\s*[A-Za-z][A-Za-z0-9_]*\s+@\d+\s+:Bool/.test(line)) { if (cur.length) blocks.push(cur.join("\n")); cur=[line]; } else if (cur.length) cur.push(line); } if (cur.length) blocks.push(cur.join("\n")); const out=new Set(); for (const b of blocks) { if (!b.includes("$experimental")) continue; for (const m of b.matchAll(/\$compatEnableFlag\("([^"]+)"\)/g)) out.add(m[1]); } console.log([...out].sort().join("\n"));'
 */

export const WORKERD_EXPERIMENTAL_COMPAT_FLAGS_SOURCE_VERSION = "1.20260701.1";

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
  "nonclass_entrypoint_reuses_ctx_across_invocations",
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
  "unique_ctx_per_invocation",
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
