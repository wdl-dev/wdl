// auth: JSRPC-only static worker. control reaches us via env.AUTH; we don't
// expose any socket. verify is the hot path; issue/list/revoke are the
// operator lifecycle.

import { WorkerEntrypoint } from "cloudflare:workers";
import {
  AuthPolicyError,
  BOOTSTRAP_TOKEN_ID,
  assertTenantNs,
  createDelegatedIssueTemplateMap,
  evaluateAccess,
  generatePlaintextToken,
  generateTokenId,
  hashToken,
  isCanonicalStoredTimestamp,
  isExpired,
  isRevoked,
  isValidTenantNs,
  parseStoredIssueTemplates,
  randomHex,
  recordToListEntry,
  renderDelegatedIssueLabel,
  resolveDelegatedIssueTemplate,
  validateExpiresAt,
  validateIssueTemplatesInput,
  validateIssueInput,
  validateRecordShape,
} from "auth-lib";
import {
  TOKEN_KEY_PREFIX,
  bindAuthRuntime,
  buildHsetCmd,
  hashKey,
  isRedisWatchError,
  normalizeRequestId,
  formatError,
  tokenKey,
} from "auth-runtime";
import { ROLES } from "shared-auth-roles";
import {
  acquireTokenLock,
  createTokenLock,
  releaseTokenLock,
} from "shared-redis-lock";
import { NAMESPACES_KEY } from "shared-worker-contract";

const utf8Decoder = new TextDecoder();

/**
 * @typedef {import("auth-lib").TokenRecord} TokenRecord
 * @typedef {{ requestId?: string }} AuthRequestInput
 * @typedef {AuthRequestInput & { token?: string, action?: string, ns?: string }} VerifyInput
 * @typedef {import("auth-lib").DelegatedIssueTemplate} DelegatedIssueTemplate
 * @typedef {AuthRequestInput & { kind?: string, ns?: string, label?: string, expiresAt?: string | number | null, issuerTokenId?: string, issueTemplates?: unknown, issue_templates?: unknown }} IssueInput
 * @typedef {AuthRequestInput & { issuerTokenId?: string, template?: unknown, kind?: unknown, ns?: unknown, label?: unknown, expiresAt?: unknown, issueTemplates?: unknown, issue_templates?: unknown }} DelegatedIssueInput
 * @typedef {AuthRequestInput & { ns?: string }} ListInput
 * @typedef {AuthRequestInput & { tokenId?: string }} RevokeInput
 */

const DELEGATED_ISSUE_NAMESPACE_RETRIES = 16;
const DELEGATED_ISSUE_LOCK_TTL_SECONDS = 30;
const DELEGATED_ISSUE_LOCK_KEY_PREFIX = "auth:delegated-issue-lock:";

/** @param {string} issuerTokenId @param {string} templateId */
function delegatedIssueLockKey(issuerTokenId, templateId) {
  return `${DELEGATED_ISSUE_LOCK_KEY_PREFIX}${encodeURIComponent(issuerTokenId)}:${encodeURIComponent(templateId)}`;
}

/**
 * @param {unknown} value
 * @param {string} field
 * @returns {string}
 */
function requireString(value, field) {
  if (typeof value !== "string" || !value) {
    throw new AuthPolicyError(400, "invalid_template_request", `${field} is required`);
  }
  return value;
}

/** @param {TokenRecord | null} issuer @param {string} templateId */
function validateIssuerForDelegatedIssue(issuer, templateId) {
  if (!issuer) {
    throw new AuthPolicyError(403, "delegated_issue_not_allowed",
      "delegated issue requires a token-issuer token");
  }
  const shape = validateRecordShape(issuer);
  if (!shape.ok) {
    throw new AuthPolicyError(503, shape.reason || "invalid_token_record",
      "issuer token record is invalid");
  }
  if (issuer.kind !== "token-issuer") {
    throw new AuthPolicyError(403, "issuer_not_token_issuer",
      "delegated issue requires a token-issuer token");
  }
  if (isRevoked(issuer) || isExpired(issuer)) {
    throw new AuthPolicyError(403, "issuer_not_active",
      "issuer token is not active");
  }
  const allowed = parseStoredIssueTemplates(issuer.issue_templates);
  if (!allowed.includes(templateId)) {
    throw new AuthPolicyError(403, "template_not_allowed",
      `issuer is not allowed to use template "${templateId}"`);
  }
}

/**
 * @param {ReturnType<typeof bindAuthRuntime>} runtime
 * @param {import("shared-redis").RedisSession} session
 */
async function scanTokenRecords(runtime, session) {
  /** @type {Array<{ tokenId: string, record: TokenRecord }>} */
  const records = [];
  const seen = new Set();
  let cursor = "0";
  do {
    const [next, keys] = await session.scan(cursor, `${TOKEN_KEY_PREFIX}*`, 200);
    cursor = next;
    const tokenIds = keys
      .filter((/** @type {string} */ key) => key.startsWith(TOKEN_KEY_PREFIX))
      .map((/** @type {string} */ key) => key.slice(TOKEN_KEY_PREFIX.length))
      .filter((/** @type {string} */ tokenId) => {
        if (seen.has(tokenId)) return false;
        seen.add(tokenId);
        return true;
      });
    const batch = await runtime.readRecords(session, tokenIds);
    for (const item of batch) {
      if (item.record) records.push(/** @type {{ tokenId: string, record: TokenRecord }} */ (item));
    }
  } while (cursor !== "0");
  return records;
}

/**
 * @param {import("shared-redis").RedisSession} session
 * @param {{ key: string, token: string }} lock
 * @param {ReturnType<typeof bindAuthRuntime>} runtime
 * @param {string | null} requestId
 */
async function releaseDelegatedIssueLock(session, lock, runtime, requestId) {
  await releaseTokenLock(session, lock, {
    onError: (err) => runtime.logAuth("warn", "auth_delegated_issue_lock_release_failed", requestId, {
      ...formatError(err),
    }),
  });
}

/**
 * @param {import("shared-redis").RedisSession} session
 * @param {{ key: string, token: string }} lock
 * @param {ReturnType<typeof bindAuthRuntime>} runtime
 * @param {string} issuerTokenId
 * @param {string} templateId
 */
async function watchDelegatedIssuePrerequisites(
  session,
  lock,
  runtime,
  issuerTokenId,
  templateId,
) {
  await session.watch(lock.key, tokenKey(issuerTokenId));
  const current = await session.get(lock.key);
  if (current !== lock.token) {
    throw new AuthPolicyError(409, "delegated_issue_busy",
      "delegated issue lock expired before the token could be issued; retry");
  }
  const issuer = await runtime.readRecord(session, issuerTokenId);
  validateIssuerForDelegatedIssue(issuer, templateId);
}

/** @param {number} lockAttemptStartedAtMs */
function assertDelegatedIssueLockLocalBudget(lockAttemptStartedAtMs) {
  const elapsedMs = Date.now() - lockAttemptStartedAtMs;
  if (elapsedMs >= (DELEGATED_ISSUE_LOCK_TTL_SECONDS - 1) * 1000) {
    throw new AuthPolicyError(409, "delegated_issue_busy",
      "delegated issue exceeded the local lock budget; retry");
  }
}

/**
 * @param {Array<{ tokenId: string, record: TokenRecord }>} records
 * @param {string} issuerTokenId
 * @param {string} templateId
 * @param {number} nowMs
 */
function countActiveDelegatedTokens(records, issuerTokenId, templateId, nowMs) {
  let active = 0;
  for (const { record } of records) {
    if (record.created_by !== issuerTokenId || record.issue_template !== templateId) continue;
    if (record.revoked_at) continue;
    if (!record.expires_at) {
      throw new AuthPolicyError(503, "delegated_issue_misconfigured",
        "delegated token record is missing expires_at");
    }
    if (!isCanonicalStoredTimestamp(record.expires_at)) {
      throw new AuthPolicyError(503, "delegated_issue_misconfigured",
        "delegated token record has invalid expires_at");
    }
    const expiresMs = Date.parse(record.expires_at);
    if (expiresMs > nowMs) active += 1;
  }
  return active;
}

/**
 * @param {Array<{ tokenId: string, record: TokenRecord }>} records
 * @param {string} ns
 */
function tokenNamespaceClaimExists(records, ns) {
  for (const { record } of records) {
    if (record.ns === ns) return true;
  }
  return false;
}

/**
 * @param {import("shared-redis").RedisSession} session
 * @param {Array<{ tokenId: string, record: TokenRecord }>} records
 * @param {DelegatedIssueTemplate} template
 */
async function chooseDelegatedNamespace(session, records, template) {
  for (let attempt = 0; attempt < DELEGATED_ISSUE_NAMESPACE_RETRIES; attempt += 1) {
    const ns = template.nsGenerator.prefix + randomHex(template.nsGenerator.randomHexBytes);
    if (!isValidTenantNs(ns)) continue;
    if (await session.sIsMember(NAMESPACES_KEY, ns)) continue;
    if (tokenNamespaceClaimExists(records, ns)) continue;
    const label = renderDelegatedIssueLabel(template, ns);
    return { ns, label };
  }
  throw new AuthPolicyError(409, "namespace_collision",
    "could not generate an available namespace");
}

export default class Auth extends WorkerEntrypoint {
  /** @param {VerifyInput | null | undefined} input */
  async verify(input) {
    const requestId = normalizeRequestId(input?.requestId);
    const startedAt = Date.now();
    const runtime = bindAuthRuntime(this.env);
    const action = input?.action;
    const ns = typeof input?.ns === "string" ? input.ns : undefined;
    const verifyLog = runtime.beginVerify(requestId, startedAt, action, ns);
    /** @type {string | undefined} */
    let principalKind;
    /** @type {string | undefined} */
    let principalNs;
    /** @type {string | undefined} */
    let tokenId;

    try {
      const token = typeof input?.token === "string" ? input.token : null;
      if (!token) {
        return verifyLog.finalize({
          ret: { ok: false, status: 401, reason: "missing_token" },
          reason: "missing_token",
        });
      }
      const hash = await hashToken(token);
      const redis = runtime.newRedis();
      const loaded = await redis.session(async (session) => {
        const bootstrap = await runtime.ensureBootstrapForVerify(session, requestId);
        let id = await session.get(hashKey(hash));
        if (!id && bootstrap.cached && hash === bootstrap.desiredHash) {
          // Cached bootstrap hash only gates self-heal; normal GET above still
          // accepts a newer bootstrap token if another isolate rotated Redis.
          await runtime.ensureBootstrap(session, requestId);
          id = await session.get(hashKey(hash));
        }
        if (!id) return null;
        const tokenId = typeof id === "string" ? id : utf8Decoder.decode(id);
        const record = await runtime.readRecord(session, tokenId);
        return { tokenId, record };
      });
      if (!loaded) {
        return verifyLog.finalize({
          ret: { ok: false, status: 401, reason: "unknown_token" },
          reason: "unknown_token",
        });
      }
      const loadedTokenId = loaded.tokenId;
      tokenId = loadedTokenId;
      const { record } = loaded;
      if (!record) {
        return verifyLog.finalize({
          ret: { ok: false, status: 401, reason: "unknown_token" },
          reason: "unknown_token",
        });
      }
      principalKind = record.kind;
      principalNs = record.ns;
      if (record.hash !== hash) {
        return verifyLog.finalize({
          ret: { ok: false, status: 503, reason: "invalid_record_hash", tokenId },
          reason: "invalid_record_hash",
          tokenId,
          principalKind,
          principalNs,
        });
      }
      // Persisted shape failures are auth contract violations, not policy
      // denies, so validate before revoked/expired checks can collapse
      // malformed timestamps into 401 outcomes.
      const shape = validateRecordShape(record);
      if (!shape.ok) {
        return verifyLog.finalize({
          ret: { ok: false, status: 503, reason: shape.reason, tokenId },
          reason: shape.reason,
          tokenId,
          principalKind,
          principalNs,
        });
      }
      if (isRevoked(record)) {
        return verifyLog.finalize({
          ret: { ok: false, status: 401, reason: "revoked", tokenId },
          reason: "revoked",
          tokenId,
          principalKind,
          principalNs,
        });
      }
      if (isExpired(record)) {
        // Lazy GC + tombstone, mirrors revoke's `revoked_at`. Idempotent
        // under concurrent expired verifies.
        if (record.hash) {
          await redis.multiExec([
            ["DEL", hashKey(record.hash)],
            buildHsetCmd(tokenKey(loadedTokenId), {
              expired_at: new Date().toISOString(),
            }),
          ]);
        }
        return verifyLog.finalize({
          ret: { ok: false, status: 401, reason: "expired", tokenId },
          reason: "expired",
          tokenId,
          principalKind,
          principalNs,
        });
      }

      const decision = evaluateAccess({
        action, ns, kind: record.kind, principalNs: record.ns,
      });
      if (!decision.ok) {
        const status = decision.reason === "unknown_role" ? 503 : 403;
        return verifyLog.finalize({
          ret: { ok: false, status, reason: decision.reason, tokenId },
          reason: decision.reason,
          tokenId,
          principalKind,
          principalNs,
        });
      }

      // Build principal by ROLES.boundNsKind, not by kind literal. None-bound
      // roles must not create an own `ns` key across the JSRPC boundary.
      const role = ROLES[record.kind];
      const principal = role.boundNsKind === "none"
        ? { kind: record.kind }
        : { kind: record.kind, ns: record.ns };
      return verifyLog.finalize({
        ret: { ok: true, status: 200, principal, tokenId },
        reason: "allow",
        tokenId,
        principalKind,
        principalNs,
      });
    } catch (err) {
      runtime.recordVerifyThrow(requestId, action, ns, err);
      throw err;
    }
  }

  // issuerTokenId comes from control's verify result; auth deliberately does
  // not re-derive it from the issuer's plaintext.
  /** @param {IssueInput | null | undefined} input */
  async issue(input) {
    const requestId = normalizeRequestId(input?.requestId);
    const runtime = bindAuthRuntime(this.env);
    const redis = runtime.newRedis();
    await runtime.ensureBootstrap(redis, requestId);

    let kind, ns, label, expiresAt, issuerTokenId;
    /** @type {string[] | undefined} */
    let issueTemplates;
    try {
      if (input?.issue_templates !== undefined) {
        throw new AuthPolicyError(400, "invalid_template_request",
          "issue_templates is a storage field; use issueTemplates");
      }
      ({ kind, ns } = validateIssueInput({ kind: input?.kind, ns: input?.ns }));
      if (kind === "token-issuer") {
        issueTemplates = validateIssueTemplatesInput(input?.issueTemplates);
        const configured = createDelegatedIssueTemplateMap();
        for (const templateId of issueTemplates) {
          resolveDelegatedIssueTemplate(templateId, configured);
        }
      } else if (input?.issueTemplates !== undefined) {
        throw new AuthPolicyError(400, "invalid_template_request",
          "issueTemplates is only valid for token-issuer tokens");
      }
      label = input?.label;
      if (label != null) {
        if (typeof label !== "string" || label.length > 128) {
          throw new AuthPolicyError(400, "invalid_label", "label must be string ≤128 chars");
        }
      }
      expiresAt = validateExpiresAt(input?.expiresAt);
      issuerTokenId = input?.issuerTokenId;
      if (typeof issuerTokenId !== "string" || !issuerTokenId) {
        throw new AuthPolicyError(400, "missing_issuer", "issuerTokenId required");
      }
    } catch (err) {
      return runtime.rethrowPolicy(requestId, "issue", err);
    }

    const tokenId = generateTokenId();
    const plaintext = generatePlaintextToken();
    const hash = await hashToken(plaintext);
    const now = new Date().toISOString();

    /** @type {Record<string, string | undefined>} */
    const recordFields = {
      hash,
      kind,
      created_at: now,
      created_by: issuerTokenId,
    };
    // Only write `ns` for bound roles; storing String(undefined) would corrupt
    // downstream filters and list output.
    if (ROLES[kind].boundNsKind !== "none") recordFields.ns = ns;
    if (typeof label === "string" && label) recordFields.label = label;
    if (expiresAt) recordFields.expires_at = expiresAt;
    if (issueTemplates) recordFields.issue_templates = JSON.stringify(issueTemplates);

    // Atomic two-key write: verify must not see a hash -> tokenId index before
    // the token record exists.
    await redis.multiExec([
      buildHsetCmd(tokenKey(tokenId), recordFields),
      ["SET", hashKey(hash), tokenId],
    ]);

    runtime.recordLifecycleOk("issue", requestId, {
      token_id: tokenId,
      target_ns: ns,
      principal_kind: kind,
      issue_template_count: issueTemplates?.length,
    });
    return { token: plaintext, tokenId };
  }

  /** @param {DelegatedIssueInput | null | undefined} input */
  async delegatedIssue(input) {
    const requestId = normalizeRequestId(input?.requestId);
    const runtime = bindAuthRuntime(this.env);
    const redis = runtime.newRedis();
    await runtime.ensureBootstrap(redis, requestId);

    let issuerTokenId;
    let templateId;
    let template;
    try {
      for (const field of Object.keys(input || {})) {
        if (field !== "issuerTokenId" && field !== "template" && field !== "requestId") {
          throw new AuthPolicyError(400, "invalid_template_request",
            `${field} is not accepted by delegated issue`);
        }
      }
      issuerTokenId = requireString(input?.issuerTokenId, "issuerTokenId");
      templateId = requireString(input?.template, "template");
      template = resolveDelegatedIssueTemplate(templateId);
    } catch (err) {
      return runtime.rethrowPolicy(requestId, "delegated_issue", err);
    }

    return await redis.session(async (session) => {
      const issuer = await runtime.readRecord(session, issuerTokenId);
      try {
        validateIssuerForDelegatedIssue(issuer, templateId);
      } catch (err) {
        return runtime.rethrowPolicy(requestId, "delegated_issue", err);
      }

      const lockKey = delegatedIssueLockKey(issuerTokenId, templateId);
      const issueLock = createTokenLock(lockKey);
      const lockAttemptStartedAtMs = Date.now();
      let locked;
      try {
        locked = await acquireTokenLock(session, issueLock, {
          ttlSeconds: DELEGATED_ISSUE_LOCK_TTL_SECONDS,
        });
      } catch (err) {
        // A failed SET reply has an unknown server-side outcome and leaves this
        // RESP session untrustworthy. The lock TTL is the recovery path.
        return runtime.rethrowPolicy(requestId, "delegated_issue", err);
      }
      if (!locked) {
        return runtime.rethrowPolicy(requestId, "delegated_issue",
          new AuthPolicyError(409, "delegated_issue_busy",
            "delegated issue is already in progress"));
      }

      let canReleaseIssueLock = true;
      try {
        const nowMs = Date.now();
        const tokenRecords = await scanTokenRecords(runtime, session);
        const active = countActiveDelegatedTokens(tokenRecords, issuerTokenId, templateId, nowMs);
        if (active >= template.activeQuota) {
          throw new AuthPolicyError(409, "active_quota_exceeded",
            "active token quota exceeded", {
              active,
              quota: template.activeQuota,
              available: 0,
            });
        }
        const namespaceChoice = await chooseDelegatedNamespace(session, tokenRecords, template);
        assertDelegatedIssueLockLocalBudget(lockAttemptStartedAtMs);
        await watchDelegatedIssuePrerequisites(
          session,
          issueLock,
          runtime,
          issuerTokenId,
          templateId,
        );
        const tokenId = generateTokenId();
        const { ns, label } = namespaceChoice;
        const expiresAt = new Date(nowMs + template.ttlSeconds * 1000).toISOString();
        const plaintext = generatePlaintextToken();
        const hash = await hashToken(plaintext);
        const now = new Date(nowMs).toISOString();
        /** @type {Record<string, string>} */
        const recordFields = {
          hash,
          kind: template.targetKind,
          ns,
          label,
          created_at: now,
          created_by: issuerTokenId,
          expires_at: expiresAt,
          issue_template: template.id,
          issue_template_version: template.version,
        };
        try {
          await session.multi()
            .hSet(tokenKey(tokenId), recordFields)
            .set(hashKey(hash), tokenId)
            .exec();
        } catch (err) {
          if (isRedisWatchError(err)) {
            throw new AuthPolicyError(409, "delegated_issue_busy",
              "delegated issue lock changed before the token could be issued; retry");
          }
          // Any other EXEC failure has an unknown transaction outcome. Keep
          // the lock and let its TTL bound recovery rather than retrying an
          // issuance that may already have committed.
          canReleaseIssueLock = false;
          throw err;
        }
        runtime.recordLifecycleOk("delegated_issue", requestId, {
          token_id: tokenId,
          issuer_token_id: issuerTokenId,
          issue_template: template.id,
          issue_template_version: template.version,
          target_ns: ns,
          principal_kind: template.targetKind,
        });
        return {
          token: plaintext,
          tokenId,
          kind: template.targetKind,
          ns,
          label,
          expiresAt,
          issueTemplate: template.id,
          issueTemplateVersion: template.version,
        };
      } catch (err) {
        return runtime.rethrowPolicy(requestId, "delegated_issue", err);
      } finally {
        if (canReleaseIssueLock) {
          await releaseDelegatedIssueLock(session, issueLock, runtime, requestId);
        }
      }
    });
  }

  // No secondary indexes yet: SCAN + batch read is acceptable at internal
  // rollout scale, and includes the infra-managed bootstrap record.
  /** @param {ListInput | null | undefined} input */
  async list(input) {
    const requestId = normalizeRequestId(input?.requestId);
    const runtime = bindAuthRuntime(this.env);
    const redis = runtime.newRedis();

    const filterNs = input?.ns;
    if (filterNs != null) {
      try {
        assertTenantNs(filterNs);
      } catch (err) {
        return runtime.rethrowPolicy(requestId, "list", err);
      }
    }

    const tokens = await redis.session(async (session) => {
      await runtime.ensureBootstrap(session, requestId);

      const records = await scanTokenRecords(runtime, session);
      const tokens = [];
      for (const { tokenId, record } of records) {
        if (filterNs && record.ns !== filterNs) continue;
        tokens.push(recordToListEntry(tokenId, record));
      }
      return tokens;
    });

    const sortedTokens = tokens.toSorted((
      /** @type {{ tokenId: string }} */ a,
      /** @type {{ tokenId: string }} */ b
    ) => a.tokenId.localeCompare(b.tokenId));
    runtime.recordLifecycleOk("list", requestId, {
      target_ns: filterNs || undefined,
      count: sortedTokens.length,
    });
    return { tokens: sortedTokens };
  }

  /** @param {RevokeInput | null | undefined} input */
  async revoke(input) {
    const requestId = normalizeRequestId(input?.requestId);
    const runtime = bindAuthRuntime(this.env);
    const redis = runtime.newRedis();
    return await redis.session(async (session) => {
      await runtime.ensureBootstrap(session, requestId);

      const tokenId = input?.tokenId;
      if (typeof tokenId !== "string" || !tokenId) {
        return runtime.rethrowPolicy(requestId, "revoke",
          new AuthPolicyError(400, "missing_token_id", "tokenId required"));
      }
      if (tokenId === BOOTSTRAP_TOKEN_ID) {
        return runtime.rethrowPolicy(requestId, "revoke",
          new AuthPolicyError(403, "bootstrap_protected",
            "bootstrap token is infra-managed; rotate via BOOTSTRAP_TOKEN env"));
      }

      const record = await runtime.readRecord(session, tokenId);
      if (!record) {
        runtime.recordLifecycleOk("revoke", requestId, {
          token_id: tokenId,
          result: "unknown",
        });
        return { revoked: false };
      }
      if (!record.hash) {
        runtime.recordLifecycleOk("revoke", requestId, {
          token_id: tokenId,
          result: "hash_missing",
        });
        return { revoked: false };
      }

      // Missing hash index means a previous revoke already collapsed the verify
      // path; keep the record untouched so this call reports no mutation.
      const indexed = await session.get(hashKey(record.hash));
      if (!indexed) {
        runtime.recordLifecycleOk("revoke", requestId, {
          token_id: tokenId,
          result: "already_revoked",
        });
        return { revoked: false };
      }
      if (record.revoked_at) {
        // The tombstone is already present but the hash index still points at it;
        // drop the dangling index so verify never re-resolves a revoked id.
        await session.del(hashKey(record.hash));
        runtime.recordLifecycleOk("revoke", requestId, {
          token_id: tokenId,
          result: "drop_dangling_index",
        });
        return { revoked: false };
      }

      const now = new Date().toISOString();
      // Two concurrent revokes can both pass the GET above; only the one whose
      // DEL returns 1 owns the state transition.
      const result = await session.multi()
        .del(hashKey(record.hash))
        .hSet(tokenKey(tokenId), { revoked_at: now })
        .exec();
      const delReply = Array.isArray(result) ? result[0] : null;
      // `revoked: true` means this call collapsed the hash index; the loser of a
      // concurrent revoke still writes an idempotent tombstone but reports false.
      const collapsed = Number(delReply) === 1;
      runtime.recordLifecycleOk("revoke", requestId, {
        token_id: tokenId,
        result: collapsed ? "ok" : "race_lost",
      });
      return { revoked: collapsed };
    });
  }
}
