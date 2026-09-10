import { importSpecifierReplacements, repositoryFileUrl, repositoryModuleDataUrl } from "./load-shared-module.js";
import { sharedRedisStubUrl } from "./mocks/fake-redis.js";

export const workflowDefinitionsUrl = repositoryModuleDataUrl("control/workflow-definitions.js", importSpecifierReplacements({
  "shared-redis": sharedRedisStubUrl(),
  "shared-utf8": repositoryFileUrl("shared/utf8.js"),
}));

const definitions = await import(workflowDefinitionsUrl);

/** @param {string} value */
const bytes = (value) => Buffer.byteLength(value, "utf8");

/** @param {string} script @param {string[]} keys @param {unknown[]} args @param {import("./mocks/fake-redis.js").FakeRedisState} state */
export async function workflowDefinitionRedisEval(script, keys, args, state) {
  if (script === definitions.WORKFLOW_DEFINITION_SNAPSHOT_SCRIPT &&
    state.hashes.get(keys[2])?.[String(args[3])] !== args[4]) {
    return [2, null, [], 0];
  }
  const hash = state.hashes.get(keys[0]) ?? {};
  const entries = Object.entries(hash);
  if (script === definitions.WORKFLOW_ROUTE_PAGE_SCRIPT) {
    const start = Number(args[0]);
    const page = entries.length <= 128 ? entries : entries.slice(start, start + 32);
    const next = entries.length <= 128 || start + page.length >= entries.length ? "0" : String(start + page.length);
    const flat = page.flat();
    return [1, next, flat];
  }
  const count = entries.length;
  const size = entries.reduce((total, [field, value]) => total + bytes(field) + bytes(value), 0);
  const fits = count <= Number(args[0]) && size <= Number(args[1]);
  if (script === definitions.WORKFLOW_DEFINITION_ADMISSION_SCRIPT) {
    return fits ? [1, count, size, args.slice(2).map((name) => Object.hasOwn(hash, String(name)) ? hash[String(name)] : null)] : [0, count, size, []];
  }
  if (script === definitions.WORKFLOW_DEFINITION_SNAPSHOT_SCRIPT) {
    if (!fits) return [0, null, [], 0];
    const meta = state.hashes.get(keys[1])?.__meta__ ?? null;
    const metaBytes = meta === null ? 0 : bytes(meta);
    if (metaBytes > Number(args[2])) return [-1, null, [], 0];
    return [1, meta, entries.flat(), size + metaBytes];
  }
  throw new Error("Unexpected Workflow Redis script");
}
