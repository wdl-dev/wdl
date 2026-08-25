export const DO_ALARM_SHIM_SOURCE = `
const ALARM_HEADER = "x-wdl-do-internal-alarm";
const RPC_HEADER = "x-wdl-do-internal-rpc";
const ALARMS_BINDING = "__WDL_DO_ALARMS__";
const ALARM_TABLE = "_wdl_do_alarms";
const ALARM_DELETE_FENCE_PREFIX = "delete:";
const DELETE_ALL_KV_BATCH_SIZE = 128;

// This module is evaluated before tenant code. Keep the small set of intrinsics
// that controls alarm classification, state transitions, and facade installation
// stable after tenant top-level evaluation mutates the shared isolate realm.
const NativeDate = Date;
const NativeNumber = Number;
const NativePromise = Promise;
const NativeProxy = Proxy;
const NativeResponse = Response;
const NativeString = String;
const nativeCrypto = crypto;
const arrayAt = Array.prototype.at;
const arrayIsArray = Array.isArray;
const arrayPush = Array.prototype.push;
const cryptoRandomUUID = crypto.randomUUID;
const dateGetTime = Date.prototype.getTime;
const dateNow = Date.now;
const headersGet = Headers.prototype.get;
const mapForEach = Map.prototype.forEach;
const mathTrunc = Math.trunc;
const numberIsFinite = Number.isFinite;
const numberIsInteger = Number.isInteger;
const objectDefineProperty = Object.defineProperty;
const promiseResolve = Promise.resolve;
const promiseThen = Promise.prototype.then;
const promiseWithResolvers = Promise.withResolvers;
const reflectApply = Reflect.apply;
const reflectGet = Reflect.get;
const requestHeadersGetter = Object.getOwnPropertyDescriptor(Request.prototype, "headers").get;
const requestJson = Request.prototype.json;
const responseJson = Response.json;
const stringIndexOf = String.prototype.indexOf;
const stringReplaceAll = String.prototype.replaceAll;
const stringSlice = String.prototype.slice;
const stringStartsWith = String.prototype.startsWith;
const stringToLowerCase = String.prototype.toLowerCase;

function withoutInternalEnv(env) {
  if (!env || typeof env !== "object" || !(ALARMS_BINDING in env)) return env;
  const out = { ...env };
  delete out[ALARMS_BINDING];
  return out;
}

function objectNameFromCtx(ctx) {
  return NativeString(ctx.id);
}

function scheduledTimeFromInput(value) {
  let scheduledTime;
  try {
    scheduledTime = reflectApply(dateGetTime, value, []);
  } catch {
    scheduledTime = NativeNumber(value);
  }
  if (!numberIsFinite(scheduledTime) || scheduledTime <= 0) {
    throw new TypeError("setAlarm() cannot be called with an alarm time <= 0");
  }
  return scheduledTime;
}

function retryCountFromInput(value) {
  const retryCount = NativeNumber(value ?? 0);
  if (!numberIsInteger(retryCount) || retryCount < 0) {
    throw new TypeError("DO alarm retryCount must be a non-negative integer");
  }
  return retryCount;
}

function alarmFieldsFromRow(row) {
  const scheduledTime = NativeNumber(row.scheduled_time);
  const retryCount = NativeNumber(row.retry_count);
  if (!numberIsFinite(scheduledTime) || scheduledTime <= 0) return null;
  if (!numberIsInteger(retryCount) || retryCount < 0) return null;
  return { scheduledTime, retryCount };
}

function alarmToken() {
  return reflectApply(cryptoRandomUUID, nativeCrypto, []);
}

function isAlarmDeleteFenceToken(value) {
  return reflectApply(stringStartsWith, NativeString(value), [ALARM_DELETE_FENCE_PREFIX]);
}

function alarmTokenBeforeDelete(value) {
  const token = NativeString(value);
  if (!isAlarmDeleteFenceToken(token)) return token;
  const separator = reflectApply(stringIndexOf, token, [":", ALARM_DELETE_FENCE_PREFIX.length]);
  if (separator < 0) return token;
  const baseline = reflectApply(stringSlice, token, [separator + 1]);
  return baseline || token;
}

function alarmDeleteFenceToken(token) {
  return ALARM_DELETE_FENCE_PREFIX + alarmToken() + ":" + NativeString(token);
}

function enqueueAlarmMutation(owner, effect) {
  const result = reflectApply(promiseThen, owner.tail, [effect]);
  owner.tail = reflectApply(promiseThen, result, [() => undefined, () => undefined]);
  return result;
}

function alarmTransactionState(nested) {
  return {
    effects: [],
    alarmReservation: null,
    baselineAlarmRow: null,
    nested: nested ? true : false,
    rolledBack: false,
    closed: false,
  };
}

function reserveAlarmMutation(transactionState, owner) {
  if (transactionState.alarmReservation !== null) return;
  const deferred = reflectApply(promiseWithResolvers, NativePromise, []);
  const reservation = {
    effect: null,
    resolve: deferred.resolve,
    result: null,
    settled: false,
  };
  reservation.result = enqueueAlarmMutation(owner, () => reflectApply(
    promiseThen,
    deferred.promise,
    [() => reservation.effect ? reflectApply(reservation.effect, undefined, []) : undefined]
  ));
  transactionState.alarmReservation = reservation;
}

function appendAlarmSideEffect(transactionState, owner, effect) {
  reserveAlarmMutation(transactionState, owner);
  reflectApply(arrayPush, transactionState.effects, [effect]);
}

function settleAlarmMutationReservation(transactionState, effect = null) {
  const reservation = transactionState.alarmReservation;
  if (!reservation) return null;
  if (!reservation.settled) {
    reservation.settled = true;
    reservation.effect = effect;
    reflectApply(reservation.resolve, undefined, [undefined]);
  }
  return reservation.result;
}

function safeErrorField(err, field) {
  try {
    return err == null ? undefined : err[field];
  } catch {
    return undefined;
  }
}

function safeErrorString(value) {
  try {
    return value == null ? null : NativeString(value);
  } catch {
    return null;
  }
}

function formatWrappedError(err) {
  try {
    const out = {
      error_name: safeErrorString(safeErrorField(err, "name")) || "Error",
      error_message: safeErrorString(safeErrorField(err, "message")) || safeErrorString(err) || "Unknown error",
    };
    const code = safeErrorString(safeErrorField(err, "code"));
    if (code != null) out.error_code = code;
    return out;
  } catch {
    return { error_name: "Error", error_message: "Unknown error" };
  }
}

function logStructured(level, event, fields = {}) {
  try {
    const payload = {
      ts: new Date().toISOString(),
      service: "do-runtime",
      level,
      event,
      ...fields,
    };
    const line = JSON.stringify(payload);
    if (level === "error") console.error(line);
    else console.log(line);
  } catch {
    // Tenant mutations must not turn best-effort logging into storage behavior.
  }
}

function ensureAlarmTable(storage) {
  storage.sql.exec(
    "CREATE TABLE IF NOT EXISTS " + ALARM_TABLE + " (" +
      "id INTEGER PRIMARY KEY CHECK (id = 1), " +
      "scheduled_time INTEGER NOT NULL, " +
      "retry_count INTEGER NOT NULL DEFAULT 0, " +
      "in_flight INTEGER NOT NULL DEFAULT 0, " +
      "token TEXT NOT NULL, " +
      "last_error TEXT" +
    ")"
  );
}

function readAlarmRow(storage) {
  ensureAlarmTable(storage);
  const result = storage.sql.exec(
    "SELECT scheduled_time, retry_count, in_flight, token FROM " + ALARM_TABLE + " WHERE id = 1"
  );
  const rows = arrayIsArray(result) ? result : [...result];
  return rows[0] || null;
}

function writeAlarmRow(storage, row) {
  ensureAlarmTable(storage);
  storage.sql.exec(
    "INSERT INTO " + ALARM_TABLE + " (id, scheduled_time, retry_count, in_flight, token, last_error) " +
      "VALUES (1, ?, ?, ?, ?, ?) " +
      "ON CONFLICT(id) DO UPDATE SET " +
      "scheduled_time = excluded.scheduled_time, " +
      "retry_count = excluded.retry_count, " +
      "in_flight = excluded.in_flight, " +
      "token = excluded.token, " +
      "last_error = excluded.last_error",
    mathTrunc(scheduledTimeFromInput(row.scheduledTime)),
    retryCountFromInput(row.retryCount),
    row.inFlight ? 1 : 0,
    NativeString(row.token),
    row.lastError == null ? null : NativeString(row.lastError)
  );
}

function deleteAlarmRow(storage, token = null) {
  ensureAlarmTable(storage);
  if (token == null) {
    storage.sql.exec("DELETE FROM " + ALARM_TABLE + " WHERE id = 1");
    return;
  }
  storage.sql.exec("DELETE FROM " + ALARM_TABLE + " WHERE id = 1 AND token = ?", NativeString(token));
}

function deleteAlarmRowIfLineage(storage, token) {
  const row = readAlarmRow(storage);
  if (!row || alarmTokenBeforeDelete(row.token) !== NativeString(token)) return;
  deleteAlarmRow(storage, row.token);
}

function writeAlarmDeleteFence(storage, expectedToken, fenceToken) {
  ensureAlarmTable(storage);
  storage.sql.exec(
    "UPDATE " + ALARM_TABLE + " SET token = ?, in_flight = 1 WHERE id = 1 AND token = ?",
    NativeString(fenceToken),
    NativeString(expectedToken)
  );
}

function restoreAlarmRowIfDeleteFence(storage, fenceToken, row, token) {
  ensureAlarmTable(storage);
  storage.sql.exec(
    "UPDATE " + ALARM_TABLE + " SET " +
      "scheduled_time = ?, retry_count = ?, in_flight = ?, token = ?, last_error = ? " +
      "WHERE id = 1 AND token = ?",
    row.scheduled_time,
    row.retry_count,
    row.in_flight,
    NativeString(token),
    null,
    NativeString(fenceToken)
  );
}

async function flushAlarmSideEffects(transactionState) {
  // One object has one alarm row. Transactional alarm updates coalesce to the
  // final SQLite row, so only the final backend index side effect should run.
  const finalEffect = reflectApply(arrayAt, transactionState.effects, [-1]);
  const queued = settleAlarmMutationReservation(transactionState, finalEffect);
  if (queued) await queued;
  else if (finalEffect) await finalEffect();
}

async function setStorageAlarm(
  storage,
  alarmBinding,
  className,
  objectName,
  scheduledTime,
  transactionState,
  alarmMutations
) {
  const alarmTime = scheduledTimeFromInput(scheduledTime);
  const token = alarmToken();
  writeAlarmRow(storage, {
    scheduledTime: alarmTime,
    retryCount: 0,
    inFlight: false,
    token,
  });
  const effect = () => alarmBinding.setAlarmIndex({
    className,
    objectName,
    scheduledTime: alarmTime,
    retryCount: 0,
    token,
  });
  const commit = async () => {
    try {
      await effect();
    } catch (err) {
      // Token-exact rollback preserves a newer alarm if user code raced
      // another mutation while this backend request was in flight.
      deleteAlarmRow(storage, token);
      throw err;
    }
  };
  if (transactionState) {
    appendAlarmSideEffect(transactionState, alarmMutations, commit);
  } else {
    await enqueueAlarmMutation(alarmMutations, commit);
  }
}

async function deleteStorageAlarm(
  storage,
  alarmBinding,
  className,
  objectName,
  transactionState,
  alarmMutations
) {
  const currentRow = readAlarmRow(storage);
  const row = transactionState ? transactionState.baselineAlarmRow : currentRow;
  const rowToken = row?.token == null ? null : alarmTokenBeforeDelete(row.token);
  const fenceToken = rowToken == null ? null : alarmDeleteFenceToken(rowToken);
  if (row && fenceToken) {
    // Keep a tokenized tombstone across backend I/O. Any later alarm mutation replaces
    // it, so this delete can neither commit nor compensate over newer local state. Only
    // token/in-flight change: deleting a corrupt legacy row must not validate its payload.
    if (currentRow?.token != null) writeAlarmDeleteFence(storage, currentRow.token, fenceToken);
  } else {
    deleteAlarmRow(storage);
  }
  const token = rowToken;
  const effect = () => token
    ? alarmBinding.deleteAlarmIndex({ className, objectName, token })
    : reflectApply(promiseResolve, NativePromise, ["skipped"]);
  const commit = async () => {
    try {
      await effect();
      if (rowToken != null) deleteAlarmRowIfLineage(storage, rowToken);
    } catch (err) {
      if (row && fenceToken && rowToken != null) {
        restoreAlarmRowIfDeleteFence(storage, fenceToken, row, row.token);
      }
      throw err;
    }
  };
  if (transactionState) {
    appendAlarmSideEffect(transactionState, alarmMutations, commit);
  } else {
    await enqueueAlarmMutation(alarmMutations, commit);
  }
}

async function getStorageAlarm(
  storage,
  alarmBinding,
  className,
  objectName,
  repairBackend,
  alarmMutations
) {
  const row = readAlarmRow(storage);
  if (!row || isAlarmDeleteFenceToken(row.token) || NativeNumber(row.in_flight) === 1) return null;
  const fields = alarmFieldsFromRow(row);
  if (!fields) {
    deleteAlarmRow(storage);
    return null;
  }
  if (repairBackend) {
    try {
      await enqueueAlarmMutation(alarmMutations, () => alarmBinding.setAlarmIndex({
        className,
        objectName,
        scheduledTime: fields.scheduledTime,
        retryCount: fields.retryCount,
        token: NativeString(row.token),
      }));
    } catch (err) {
      logStructured("warn", "do_alarm_index_repair_failed", {
        class_name: className,
        object_name: objectName,
        ...formatWrappedError(err),
      });
    }
  }
  return fields.scheduledTime;
}

function assertAlarmTransactionOpen(transactionState) {
  if (transactionState?.rolledBack) {
    throw new TypeError("Alarm storage operations cannot be used after transaction rollback()");
  }
  if (transactionState?.closed) {
    throw new TypeError("Alarm storage operations cannot be used after transaction completion");
  }
}

function assertAlarmTransactionActive(transactionState) {
  assertAlarmTransactionOpen(transactionState);
  if (transactionState?.nested) {
    throw new TypeError("Alarm storage operations cannot be used inside nested transaction()");
  }
}

function claimStorageAlarm(storage, alarm) {
  const row = readAlarmRow(storage);
  if (!row || isAlarmDeleteFenceToken(row.token)) return null;
  const fields = alarmFieldsFromRow(row);
  if (!fields) {
    deleteAlarmRow(storage);
    return null;
  }
  const rowToken = NativeString(row.token);
  const alarmTokenValue = alarm?.token == null ? null : NativeString(alarm.token);
  if (alarmTokenValue && alarmTokenValue !== rowToken) return null;
  const retryCount = retryCountFromInput(alarm?.retryCount ?? fields.retryCount);
  if (NativeNumber(row.in_flight) !== 1 && fields.scheduledTime > reflectApply(dateNow, NativeDate, [])) return null;
  writeAlarmRow(storage, {
    scheduledTime: fields.scheduledTime,
    retryCount,
    inFlight: true,
    token: rowToken,
  });
  return { token: rowToken, retryCount };
}

function completeStorageAlarm(storage, token) {
  deleteAlarmRowIfLineage(storage, token);
}

function quoteSqlIdentifier(name) {
  return '"' + reflectApply(stringReplaceAll, NativeString(name), ['"', '""']) + '"';
}

function sqlObjectDropStatement(row) {
  const type = NativeString(row.type);
  const name = NativeString(row.name);
  const lowerName = reflectApply(stringToLowerCase, name, []);
  if (
    reflectApply(stringStartsWith, lowerName, ["sqlite_"]) ||
    reflectApply(stringStartsWith, lowerName, ["_cf_"])
  ) return null;
  if (type === "table") return "DROP TABLE IF EXISTS " + quoteSqlIdentifier(name);
  if (type === "view") return "DROP VIEW IF EXISTS " + quoteSqlIdentifier(name);
  if (type === "trigger") return "DROP TRIGGER IF EXISTS " + quoteSqlIdentifier(name);
  if (type === "index") return "DROP INDEX IF EXISTS " + quoteSqlIdentifier(name);
  return null;
}

async function deleteAllKvStorage(storage) {
  while (true) {
    const entries = await storage.list({ limit: DELETE_ALL_KV_BATCH_SIZE });
    const keys = [];
    reflectApply(mapForEach, entries, [(_value, key) => {
      reflectApply(arrayPush, keys, [key]);
    }]);
    if (!keys.length) return;
    await storage.delete(keys);
  }
}

function deleteAllSqlStorage(storage, deleteAlarm) {
  const result = storage.sql.exec(
    "SELECT type, name FROM sqlite_master " +
      "WHERE type IN ('trigger', 'view', 'table', 'index') " +
      "ORDER BY CASE type WHEN 'trigger' THEN 0 WHEN 'view' THEN 1 WHEN 'table' THEN 2 ELSE 3 END"
  );
  const rows = arrayIsArray(result) ? result : [...result];
  storage.sql.exec("PRAGMA foreign_keys = OFF");
  try {
    for (let i = 0; i < rows.length; i += 1) {
      const row = rows[i];
      if (NativeString(row.name) === ALARM_TABLE && !deleteAlarm) continue;
      const statement = sqlObjectDropStatement(row);
      if (statement) storage.sql.exec(statement);
    }
  } finally {
    storage.sql.exec("PRAGMA foreign_keys = ON");
  }
}

function wrapStorage(
  storage,
  alarmBinding,
  className,
  objectName,
  transactionState = null,
  alarmStorage = storage,
  sharedTransactionContext = null,
  sharedAlarmMutations = null
) {
  if (!storage || !alarmBinding) return storage;
  const transactionContext = sharedTransactionContext || { active: transactionState, syncDepth: 0 };
  const alarmMutations = sharedAlarmMutations || {
    tail: reflectApply(promiseResolve, NativePromise, [undefined]),
  };
  const activeTransactionState = () => {
    if (transactionState?.rolledBack || transactionState?.closed) return transactionState;
    return transactionContext.active || transactionState;
  };
  return new NativeProxy(storage, {
    get(target, prop, receiver) {
      if (prop === "setAlarm") {
        return (scheduledTime, _options = undefined) => {
          assertAlarmTransactionActive(activeTransactionState());
          if (transactionContext.syncDepth > 0) {
            throw new TypeError("setAlarm() cannot be used inside transactionSync(); use transaction()");
          }
          return setStorageAlarm(
            alarmStorage,
            alarmBinding,
            className,
            objectName,
            scheduledTime,
            activeTransactionState(),
            alarmMutations
          );
        };
      }
      if (prop === "getAlarm") {
        return (_options = undefined) => {
          const activeState = activeTransactionState();
          assertAlarmTransactionActive(activeState);
          return getStorageAlarm(
            alarmStorage,
            alarmBinding,
            className,
            objectName,
            transactionContext.syncDepth === 0 && !activeState,
            alarmMutations
          );
        };
      }
      if (prop === "deleteAlarm") {
        return (_options = undefined) => {
          assertAlarmTransactionActive(activeTransactionState());
          if (transactionContext.syncDepth > 0) {
            throw new TypeError("deleteAlarm() cannot be used inside transactionSync(); use transaction()");
          }
          return deleteStorageAlarm(
            alarmStorage,
            alarmBinding,
            className,
            objectName,
            activeTransactionState(),
            alarmMutations
          );
        };
      }
      if (prop === "transaction" && !transactionState) {
        return async (callback, ...rest) => {
          const parentState = transactionContext.active;
          const previousState = parentState;
          assertAlarmTransactionOpen(parentState);
          const txState = alarmTransactionState(parentState != null);
          const wrapped = typeof callback === "function"
            ? async (txn) => {
              txState.baselineAlarmRow = txState.nested
                ? null
                : readAlarmRow(alarmStorage);
              // SQLite-backed workerd transactions include owning ctx.storage calls.
              // Keep this fence through native commit/rollback, not only callback settle.
              transactionContext.active = txState;
              try {
                return await callback(
                  wrapStorage(
                    txn,
                    alarmBinding,
                    className,
                    objectName,
                    txState,
                    target,
                    transactionContext,
                    alarmMutations
                  )
                );
              } finally {
                txState.closed = true;
              }
            }
            : callback;
          let result;
          try {
            result = await reflectApply(
              reflectGet(target, prop, receiver),
              target,
              [wrapped, ...rest]
            );
          } catch (err) {
            transactionContext.active = previousState;
            if (!txState.nested) settleAlarmMutationReservation(txState);
            throw err;
          }
          transactionContext.active = previousState;
          try {
            if (!txState.nested) {
              if (txState.rolledBack) {
                settleAlarmMutationReservation(txState);
              } else {
                await flushAlarmSideEffects(txState);
              }
            }
            return result;
          } catch (err) {
            if (!txState.nested) settleAlarmMutationReservation(txState);
            throw err;
          } finally {
            txState.closed = true;
            if (transactionContext.active === txState) transactionContext.active = previousState;
          }
        };
      }
      if (prop === "transactionSync" && !transactionState) {
        return (callback, ...rest) => {
          const wrapped = typeof callback === "function" ? () => {
            transactionContext.syncDepth += 1;
            try {
              // workerd transactionSync() invokes closure() without a txn
              // parameter; storage operations on this proxy are the txn surface.
              return callback();
            } finally {
              transactionContext.syncDepth -= 1;
            }
          } : callback;
          const result = reflectApply(reflectGet(target, prop, receiver), target, [wrapped, ...rest]);
          return result;
        };
      }
      if (prop === "deleteAll" && !transactionState) {
        return (...args) => {
          const activeState = activeTransactionState();
          assertAlarmTransactionActive(activeState);
          if (transactionContext.syncDepth > 0) {
            throw new TypeError(
              "deleteAll() cannot be used inside transactionSync(); call it outside the transaction"
            );
          }
          if (activeState) {
            throw new TypeError("deleteAll() cannot be used inside transaction(); call it outside the transaction");
          }
          return (async () => {
            if (args.length > 1) throw new TypeError("deleteAll() accepts at most one options argument");
            const options = args[0];
            const deleteAlarm = options?.deleteAlarm !== false;
            const alarmRow = readAlarmRow(alarmStorage);
            await deleteAllKvStorage(target);
            deleteAllSqlStorage(target, deleteAlarm);
            if (deleteAlarm) {
              const token = alarmRow?.token == null
                ? null
                : alarmTokenBeforeDelete(alarmRow.token);
              await enqueueAlarmMutation(alarmMutations, () => token
                ? alarmBinding.deleteAlarmIndex({ className, objectName, token })
                : reflectApply(promiseResolve, NativePromise, ["skipped"]));
            }
          })();
        };
      }
      if (prop === "rollback" && transactionState) {
        return (...args) => {
          const result = reflectApply(reflectGet(target, prop, receiver), target, args);
          if (!transactionState.rolledBack) {
            transactionState.effects.length = 0;
            transactionState.rolledBack = true;
          }
          return result;
        };
      }
      const value = reflectGet(target, prop, receiver);
      return typeof value === "function" ? (...args) => reflectApply(value, target, args) : value;
    },
  });
}

function wrapCtx(ctx, alarmBinding, className, installedStorageProxy = null) {
  if (!ctx || !alarmBinding) return ctx;
  const objectName = objectNameFromCtx(ctx);
  let storageProxy = installedStorageProxy;
  return new NativeProxy(ctx, {
    get(target, prop, receiver) {
      if (prop === "storage") {
        const storage = reflectGet(target, prop, receiver);
        if (!storageProxy) storageProxy = wrapStorage(storage, alarmBinding, className, objectName);
        return storageProxy;
      }
      const value = reflectGet(target, prop, receiver);
      return typeof value === "function" ? (...args) => reflectApply(value, target, args) : value;
    },
  });
}

function installStorageProxy(ctx, alarmBinding, className) {
  if (!ctx || !alarmBinding) return null;
  const objectName = objectNameFromCtx(ctx);
  const storageProxy = wrapStorage(ctx.storage, alarmBinding, className, objectName);
  try {
    objectDefineProperty(ctx, "storage", {
      value: storageProxy,
      configurable: true,
    });
  } catch (err) {
    // Some future workerd build may make DurableObjectState.storage
    // non-configurable. In that case post-constructor this.ctx still gets the
    // proxy, but constructor-cached storage cannot be shimmed without upstream.
    logStructured("warn", "do_storage_proxy_constructor_install_failed", {
      class_name: className,
      ...formatWrappedError(err),
    });
  }
  return storageProxy;
}

export function wrapDurableObjectClass(Base, className) {
  return class extends Base {
    constructor(ctx, env) {
      const alarmBinding = env?.[ALARMS_BINDING];
      const constructorStorageProxy = installStorageProxy(ctx, alarmBinding, className);
      // The inner host-binding wrapper owns env facade materialization. Strip
      // only the alarm binding here so __WDL_HOST_BINDINGS_WRAPPED can survive
      // through the two-layer wrapper contract.
      super(ctx, withoutInternalEnv(env));
      // Resolve once through the host wrapper after construction so prototype
      // methods, class fields, and accessors retain the real instance receiver.
      const tenantFetch = reflectGet(this, "fetch", this);
      const wrappedCtx = wrapCtx(ctx, alarmBinding, className, constructorStorageProxy);
      try {
        objectDefineProperty(this, "ctx", {
          value: wrappedCtx,
          configurable: true,
          writable: true,
        });
      } catch {
        this.ctx = wrappedCtx;
      }
      // workerd wraps instance handlers after construction, so this platform
      // dispatch must remain writable and configurable after replacing class fields.
      objectDefineProperty(this, "fetch", {
        value: async function(request) {
          const headers = reflectApply(requestHeadersGetter, request, []);
          if (reflectApply(headersGet, headers, [ALARM_HEADER]) === "1") {
            const alarm = await reflectApply(requestJson, request, []);
            const claim = claimStorageAlarm(this.ctx.storage, alarm);
            if (!claim) return reflectApply(responseJson, NativeResponse, [{ ok: true, ignored: true }]);
            // Alarm accessors may depend on initialized instance state.
            const tenantAlarm = reflectGet(this, "alarm", this);
            if (typeof tenantAlarm === "function") {
              await reflectApply(tenantAlarm, this, [{
                retryCount: claim.retryCount,
                isRetry: claim.retryCount > 0,
              }]);
            }
            completeStorageAlarm(this.ctx.storage, claim.token);
            return reflectApply(responseJson, NativeResponse, [{ ok: true }]);
          }
          if (reflectApply(headersGet, headers, [RPC_HEADER]) === "1") {
            const rpc = await reflectApply(requestJson, request, []);
            try {
              const tenantMethod = reflectGet(this, rpc.method, this);
              if (typeof tenantMethod !== "function") {
                return reflectApply(responseJson, NativeResponse, [{
                  error: "do_rpc_method_not_found",
                  message: "Durable Object RPC method " + rpc.method + " was not found",
                }, { status: 404 }]);
              }
              const result = await reflectApply(tenantMethod, this, rpc.args);
              return reflectApply(responseJson, NativeResponse, [{ ok: true, result }]);
            } catch (err) {
              const formatted = formatWrappedError(err);
              const stack = safeErrorString(safeErrorField(err, "stack"));
              return reflectApply(responseJson, NativeResponse, [{
                error: "do_rpc_error",
                name: formatted.error_name,
                message: formatted.error_message,
                ...(stack ? { stack } : {}),
              }, { status: 500 }]);
            }
          }
          if (typeof tenantFetch !== "function") {
            return new NativeResponse("Durable Object class has no fetch handler", { status: 500 });
          }
          return await reflectApply(tenantFetch, this, [request]);
        },
        configurable: true,
        writable: true,
      });
    }
  };
}
`;
