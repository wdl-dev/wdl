import { decodeBulk } from "shared-redis";
import { utf8ByteLength } from "shared-utf8";

export const WORKFLOW_DEFINITIONS_MAX_COUNT = 1024;
export const WORKFLOW_DEFINITIONS_MAX_BYTES = 1024 * 1024;
export const WORKFLOW_LIST_META_MAX_BYTES = 8 * 1024 * 1024;
export const WORKFLOW_DEFINITION_PAGE_MAX_BYTES = 8 * 1024 * 1024;
export const WORKFLOW_DEFINITION_PAGE_READ_MAX_BYTES = 16 * 1024 * 1024;
export const WORKFLOW_DEFINITION_PAGE_MAX_WORKERS = 16;
export const WORKFLOW_DEFINITION_CURSOR_MAX_BYTES = 2048;

export const WORKFLOW_DEFINITION_SNAPSHOT_SCRIPT = `
if redis.call('HGET', KEYS[3], ARGV[4]) ~= ARGV[5] then return {2, false, {}, 0} end
if redis.call('HLEN', KEYS[1]) > tonumber(ARGV[1]) then return {0, false, {}, 0} end
local fields = redis.call('HKEYS', KEYS[1])
local bytes = 0
for _, field in ipairs(fields) do
  bytes = bytes + #field + redis.call('HSTRLEN', KEYS[1], field)
  if bytes > tonumber(ARGV[2]) then return {0, false, {}, 0} end
end
local meta_bytes = redis.call('HSTRLEN', KEYS[2], '__meta__')
if meta_bytes > tonumber(ARGV[3]) then return {-1, false, {}, 0} end
local meta = redis.call('HGET', KEYS[2], '__meta__')
return {1, meta, redis.call('HGETALL', KEYS[1]), bytes + meta_bytes}
`;

export const WORKFLOW_DEFINITION_ADMISSION_SCRIPT = `
local count = redis.call('HLEN', KEYS[1])
if count > tonumber(ARGV[1]) then return {0, count, 0, {}} end
local bytes = 0
for _, field in ipairs(redis.call('HKEYS', KEYS[1])) do
  bytes = bytes + #field + redis.call('HSTRLEN', KEYS[1], field)
  if bytes > tonumber(ARGV[2]) then return {0, count, bytes, {}} end
end
local values = {}
for i = 3, #ARGV do values[#values + 1] = redis.call('HGET', KEYS[1], ARGV[i]) end
return {1, count, bytes, values}
`;

export const WORKFLOW_ROUTE_PAGE_SCRIPT = `
local page = redis.call('HSCAN', KEYS[1], ARGV[1], 'COUNT', 32)
if #page[2] > 1024 then return {0, false, {}} end
local bytes = 0
for _, value in ipairs(page[2]) do
  bytes = bytes + #value
  if bytes > 131072 then return {0, false, {}} end
end
return {1, page[1], page[2]}
`;

/** @param {unknown} raw @returns {Record<string, string>} */
function decodeHash(raw) {
  if (!Array.isArray(raw) || raw.length % 2 !== 0) throw new Error("Invalid Workflow hash snapshot");
  const result = Object.create(null);
  for (let index = 0; index < raw.length; index += 2) {
    const key = decodeBulk(raw[index]);
    const value = decodeBulk(raw[index + 1]);
    if (key == null || value == null) throw new Error("Invalid Workflow hash entry");
    result[key] = value;
  }
  return result;
}

/**
 * @param {import("shared-redis").RedisClient | import("shared-redis").RedisSession} redis
 * @param {string} defsKey
 * @param {string} metaKey
 * @param {{key: string, worker: string, version: string}} route
 */
export async function readWorkflowDefinitionSnapshot(redis, defsKey, metaKey, route) {
  const raw = await redis.eval(WORKFLOW_DEFINITION_SNAPSHOT_SCRIPT,
    [defsKey, metaKey, route.key],
    [String(WORKFLOW_DEFINITIONS_MAX_COUNT), String(WORKFLOW_DEFINITIONS_MAX_BYTES), String(WORKFLOW_LIST_META_MAX_BYTES), route.worker, route.version]);
  if (!Array.isArray(raw) || raw.length !== 4) throw new Error("Invalid Workflow definition snapshot");
  const metaRaw = decodeBulk(raw[1]);
  const defs = decodeHash(raw[2]);
  const bytes = Number(raw[3]);
  return { status: Number(raw[0]), metaRaw, defs, bytes };
}

/**
 * @param {import("shared-redis").RedisSession} redis
 * @param {string} key
 * @param {string[]} names
 */
export async function readWorkflowDefinitionAdmission(redis, key, names) {
  const raw = await redis.eval(WORKFLOW_DEFINITION_ADMISSION_SCRIPT, [key],
    [String(WORKFLOW_DEFINITIONS_MAX_COUNT), String(WORKFLOW_DEFINITIONS_MAX_BYTES), ...names]);
  if (!Array.isArray(raw) || raw.length !== 4 || !Array.isArray(raw[3])) throw new Error("Invalid Workflow admission snapshot");
  const values = raw[3].map((value) => {
    const decoded = decodeBulk(value);
    if (decoded === undefined) throw new Error("Missing Workflow admission reply");
    return decoded;
  });
  if (raw[0] === 1 && values.length !== names.length) throw new Error("Workflow admission reply count mismatch");
  return { status: Number(raw[0]), count: Number(raw[1]), bytes: Number(raw[2]), values };
}

/**
 * @param {import("shared-redis").RedisClient | import("shared-redis").RedisSession} redis
 * @param {string} key
 * @param {string} cursor
 */
export async function readWorkflowRoutePage(redis, key, cursor) {
  const raw = await redis.eval(WORKFLOW_ROUTE_PAGE_SCRIPT, [key], [cursor]);
  if (!Array.isArray(raw) || raw.length !== 3 || raw[0] !== 1) {
    throw new Error("Workflow route scan page exceeds its bounds");
  }
  const next = decodeBulk(raw[1]);
  if (next == null) throw new Error("Invalid Workflow route cursor");
  return { next, routes: decodeHash(raw[2]) };
}

/** @param {{count: number, bytes: number, values: (string | null)[]}} snapshot @param {Array<[string, string]>} updates */
export function workflowDefinitionUpdatesFit(snapshot, updates) {
  let { count, bytes } = snapshot;
  for (let index = 0; index < updates.length; index += 1) {
    const [name, value] = updates[index];
    const old = snapshot.values[index];
    if (old == null) { count += 1; bytes += utf8ByteLength(name); }
    else bytes -= utf8ByteLength(old);
    bytes += utf8ByteLength(value);
  }
  return count <= WORKFLOW_DEFINITIONS_MAX_COUNT && bytes <= WORKFLOW_DEFINITIONS_MAX_BYTES;
}

/** @param {unknown} declarations */
export function workflowDeclarationsFit(declarations) {
  return Array.isArray(declarations) && declarations.length <= WORKFLOW_DEFINITIONS_MAX_COUNT &&
    utf8ByteLength(JSON.stringify(declarations)) <= WORKFLOW_DEFINITIONS_MAX_BYTES;
}
