import {
  errorMessage,
  codedErrorLogFields,
  codedErrorResponse,
  ControlAbort,
  jsonError,
  jsonResponse,
  randomHex,
  readJsonBody,
  requireControlLog,
  requireControlRedis,
  runOptimistic,
  secretEnvelopeErrorResponse,
  stringEnv,
} from "control-shared";
import {
  AI_CREDENTIAL_MAX_BYTES,
  AI_NAMESPACE_MODEL_MAX_COUNT,
  AI_PROVIDER_MAX_COUNT,
  AI_PROVIDER_REVISION_RE,
  aiProviderCredentialsKey,
  aiProvidersKey,
  assertAiCredential,
  normalizeAiProviderRecord,
  normalizeAiProviderWrite,
} from "shared-ai-contract";
import { isValidAiProviderName } from "shared-ns-pattern";
import { encryptSecretValue, SecretEnvelopeError } from "shared-secret-envelope";

const AI_MUTATION_ATTEMPTS = 5;
const AI_PROVIDER_BODY_MAX_BYTES = 128 * 1024;
// A valid ASCII credential can use a six-byte \uXXXX escape for every byte.
const AI_CREDENTIAL_BODY_MAX_BYTES = AI_CREDENTIAL_MAX_BYTES * 6 + 1024;

class AiControlError extends ControlAbort {
  /** @param {number} status @param {string} code @param {string} message */
  constructor(status, code, message) {
    super(status, code, { message });
  }
}

/** @param {string} provider @param {unknown} raw */
function parseStoredProvider(provider, raw) {
  if (typeof raw !== "string") {
    throw new AiControlError(500, "ai_state_corrupt", `AI provider ${provider} is not a string`);
  }
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new AiControlError(500, "ai_state_corrupt", `AI provider ${provider} is invalid JSON`);
  }
  let record;
  try {
    record = normalizeAiProviderRecord(parsed);
  } catch (err) {
    throw new AiControlError(
      500,
      "ai_state_corrupt",
      `AI provider ${provider} is malformed: ${errorMessage(err)}`
    );
  }
  if (JSON.stringify(record) !== raw) {
    throw new AiControlError(500, "ai_state_corrupt", `AI provider ${provider} is not canonical`);
  }
  return record;
}

/** @param {Record<string, string | null | undefined>} raw */
function parseStoredProviders(raw) {
  const providers = new Map();
  for (const [name, value] of Object.entries(raw)) {
    if (!isValidAiProviderName(name)) {
      throw new AiControlError(500, "ai_state_corrupt", `AI provider name ${JSON.stringify(name)} is invalid`);
    }
    providers.set(name, parseStoredProvider(name, value));
  }
  if (providers.size > AI_PROVIDER_MAX_COUNT) {
    throw new AiControlError(500, "ai_state_corrupt", "AI provider count exceeds its bound");
  }
  if (providerModelCount(providers) > AI_NAMESPACE_MODEL_MAX_COUNT) {
    throw new AiControlError(500, "ai_state_corrupt", "AI model count exceeds its bound");
  }
  return providers;
}

/** @param {string[]} names */
function parseStoredCredentialNames(names) {
  if (names.length > AI_PROVIDER_MAX_COUNT) {
    throw new AiControlError(500, "ai_state_corrupt", "AI credential count exceeds its bound");
  }
  const credentials = new Set();
  for (const name of names) {
    if (!isValidAiProviderName(name)) {
      throw new AiControlError(
        500,
        "ai_state_corrupt",
        `AI credential provider name ${JSON.stringify(name)} is invalid`
      );
    }
    credentials.add(name);
  }
  return credentials;
}

/** @param {string} ns */
async function readProviderSnapshot(ns) {
  const { hashes, keyLists } = await requireControlRedis().hGetAllManyAndHKeysMany(
    [aiProvidersKey(ns)],
    [aiProviderCredentialsKey(ns)]
  );
  return {
    providers: parseStoredProviders(hashes[0]),
    credentialNames: parseStoredCredentialNames(keyLists[0]),
  };
}

/** @param {Map<string, ReturnType<typeof normalizeAiProviderRecord>>} providers */
function providerModelCount(providers) {
  let count = 0;
  for (const record of providers.values()) count += Object.keys(record.models).length;
  return count;
}

/** @param {Map<string, ReturnType<typeof normalizeAiProviderRecord>>} providers */
function assertProviderAggregate(providers) {
  if (providers.size > AI_PROVIDER_MAX_COUNT) {
    throw new AiControlError(409, "ai_provider_limit", `Namespace may have at most ${AI_PROVIDER_MAX_COUNT} AI providers`);
  }
  if (providerModelCount(providers) > AI_NAMESPACE_MODEL_MAX_COUNT) {
    throw new AiControlError(
      409,
      "ai_model_limit",
      `Namespace may have at most ${AI_NAMESPACE_MODEL_MAX_COUNT} AI models`
    );
  }
}

/** @param {string} name @param {ReturnType<typeof normalizeAiProviderRecord>} record @param {boolean} credentialConfigured */
function providerResponse(name, record, credentialConfigured) {
  return { name, ...record, credentialConfigured };
}

/** @param {string} provider */
function validateProviderName(provider) {
  if (!isValidAiProviderName(provider)) {
    throw new AiControlError(400, "invalid_ai_provider", "AI provider name is invalid");
  }
}

/**
 * @template T
 * @param {import("shared-redis").RedisClient} redis
 * @param {(session: import("shared-redis").RedisSession) => Promise<T>} fn
 * @returns {Promise<T>}
 */
async function mutate(redis, fn) {
  return await runOptimistic(redis, {
    attempts: AI_MUTATION_ATTEMPTS,
    onExhausted: () => {
      throw new AiControlError(503, "ai_mutation_contention", "AI provider changed concurrently; retry later");
    },
  }, fn);
}

/** @param {{ request: Request, ns: string, provider: string }} args */
async function putProvider({ request, ns, provider }) {
  validateProviderName(provider);
  const parsed = await readJsonBody(request, {
    requireObject: true,
    maxBytes: AI_PROVIDER_BODY_MAX_BYTES,
  });
  if (parsed.response) return parsed.response;
  const body = /** @type {Record<string, unknown>} */ (parsed.body);
  let record;
  try {
    record = normalizeAiProviderWrite(body, randomHex(16));
  } catch (err) {
    throw new AiControlError(400, "invalid_ai_provider", errorMessage(err));
  }
  const providersKey = aiProvidersKey(ns);
  const credentialsKey = aiProviderCredentialsKey(ns);
  const redis = requireControlRedis();
  const credentialConfigured = await mutate(redis, async (iso) => {
    await iso.watch(providersKey, credentialsKey);
    const raw = await iso.hGetAll(providersKey);
    const providers = parseStoredProviders(raw);
    const previous = providers.get(provider);
    const credentialExists = await iso.hExists(credentialsKey, provider);
    const preserveCredential = previous?.kind === record.kind && credentialExists;
    providers.set(provider, record);
    assertProviderAggregate(providers);
    const tx = iso.multi().hSet(providersKey, provider, JSON.stringify(record));
    if (!preserveCredential) tx.hDel(credentialsKey, provider);
    await tx.exec();
    return preserveCredential;
  });
  return jsonResponse(200, {
    provider: providerResponse(provider, record, credentialConfigured),
  });
}

/** @param {{ request: Request, env: Record<string, unknown>, ns: string, provider: string }} args */
async function putCredential({ request, env, ns, provider }) {
  validateProviderName(provider);
  const parsed = await readJsonBody(request, {
    requireObject: true,
    maxBytes: AI_CREDENTIAL_BODY_MAX_BYTES,
  });
  if (parsed.response) return parsed.response;
  const body = /** @type {Record<string, unknown>} */ (parsed.body);
  for (const key of Object.keys(body)) {
    if (key !== "revision" && key !== "credential") {
      throw new AiControlError(400, "invalid_request", `Body field ${key} is not supported`);
    }
  }
  if (typeof body.revision !== "string" || !AI_PROVIDER_REVISION_RE.test(body.revision)) {
    throw new AiControlError(400, "invalid_request", "revision must be 32 lowercase hex characters");
  }
  let credential;
  try {
    credential = assertAiCredential(body.credential);
  } catch (err) {
    throw new AiControlError(400, "invalid_request", errorMessage(err));
  }
  const providersKey = aiProvidersKey(ns);
  const credentialsKey = aiProviderCredentialsKey(ns);
  const encrypted = await encryptSecretValue(credential, {
    env: stringEnv(env),
    hashKey: credentialsKey,
    fieldName: provider,
  });
  await mutate(requireControlRedis(), async (iso) => {
    await iso.watch(providersKey, credentialsKey);
    const raw = await iso.hGet(providersKey, provider);
    if (raw === null) throw new AiControlError(404, "ai_provider_not_found", "AI provider not found");
    const record = parseStoredProvider(provider, raw);
    if (record.revision !== body.revision) {
      throw new AiControlError(409, "ai_provider_revision_mismatch", "AI provider revision changed");
    }
    await iso.multi().hSet(credentialsKey, provider, encrypted).exec();
  });
  return jsonResponse(200, {
    ok: true,
    provider,
    revision: body.revision,
    credentialConfigured: true,
  });
}

/** @param {{ ns: string, provider: string }} args */
async function deleteProvider({ ns, provider }) {
  validateProviderName(provider);
  const providersKey = aiProvidersKey(ns);
  const credentialsKey = aiProviderCredentialsKey(ns);
  const deleted = await mutate(requireControlRedis(), async (iso) => {
    await iso.watch(providersKey, credentialsKey);
    const raw = await iso.hGet(providersKey, provider);
    const credentialExists = await iso.hExists(credentialsKey, provider);
    if (raw === null && !credentialExists) return false;
    if (raw !== null) parseStoredProvider(provider, raw);
    await iso.multi().hDel(providersKey, provider).hDel(credentialsKey, provider).exec();
    return true;
  });
  return jsonResponse(200, { ok: true, deleted });
}

/** @param {{ ns: string, provider?: string }} args */
async function getProviders({ ns, provider }) {
  const { providers, credentialNames } = await readProviderSnapshot(ns);
  if (provider !== undefined) {
    validateProviderName(provider);
    const record = providers.get(provider);
    if (!record) return jsonError(404, "ai_provider_not_found", "AI provider not found");
    return jsonResponse(200, {
      provider: providerResponse(provider, record, credentialNames.has(provider)),
    });
  }
  return jsonResponse(200, {
    providers: [...providers.entries()]
      .toSorted(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)
      .map(([name, record]) => providerResponse(name, record, credentialNames.has(name))),
  });
}

/** @param {{ ns: string }} args */
async function getModels({ ns }) {
  const { providers } = await readProviderSnapshot(ns);
  const models = [];
  for (const [provider, record] of [...providers.entries()].toSorted(([a], [b]) =>
    a < b ? -1 : a > b ? 1 : 0)) {
    for (const [alias, descriptor] of Object.entries(record.models)) {
      models.push({ id: `${provider}/${alias}`, provider, alias, kind: record.kind, ...descriptor });
    }
  }
  models.sort((left, right) => left.id < right.id ? -1 : left.id > right.id ? 1 : 0);
  return jsonResponse(200, { models });
}

/**
 * @param {{ request: Request, env: Record<string, unknown>, method: string, ns: string, subPath: string[], requestId: string }} args
 */
export async function handle({ request, env, method, ns, subPath, requestId }) {
  const log = requireControlLog();
  try {
    if (subPath[0] === "providers" && subPath.length === 1 && method === "GET") {
      return await getProviders({ ns });
    }
    if (subPath[0] === "providers" && subPath.length === 2) {
      const provider = subPath[1];
      if (method === "GET") return await getProviders({ ns, provider });
      if (method === "PUT") return await putProvider({ request, ns, provider });
      if (method === "DELETE") return await deleteProvider({ ns, provider });
    }
    if (
      subPath[0] === "providers" &&
      subPath.length === 3 &&
      subPath[2] === "credential" &&
      method === "PUT"
    ) {
      return await putCredential({ request, env, ns, provider: subPath[1] });
    }
    if (subPath.length === 1 && subPath[0] === "models" && method === "GET") {
      return await getModels({ ns });
    }
    return jsonError(404, "not_found", "Not found");
  } catch (err) {
    if (err instanceof AiControlError) {
      log(err.status >= 500 ? "error" : "warn", "ai_request_rejected", {
        request_id: requestId,
        namespace: ns,
        ...codedErrorLogFields(err),
      });
      return codedErrorResponse(err, err.code);
    }
    if (err instanceof SecretEnvelopeError) {
      return secretEnvelopeErrorResponse({
        err,
        log,
        event: "ai_credential_rejected",
        fields: { request_id: requestId, namespace: ns },
      });
    }
    throw err;
  }
}
