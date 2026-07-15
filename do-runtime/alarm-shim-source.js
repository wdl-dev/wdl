export const DO_ALARM_SHIM_SOURCE = `
const ALARM_HEADER = "x-wdl-do-internal-alarm";
const RPC_HEADER = "x-wdl-do-internal-rpc";
const ALARMS_BINDING = "__WDL_DO_ALARMS__";
const ALARM_TABLE = "_wdl_do_alarms";

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
const reflectApply = Reflect.apply;
const reflectGet = Reflect.get;
const requestHeadersGetter = Object.getOwnPropertyDescriptor(Request.prototype, "headers").get;
const requestJson = Request.prototype.json;
const responseJson = Response.json;
const stringReplaceAll = String.prototype.replaceAll;
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

async function flushAlarmSideEffects(sideEffects) {
  // One object has one alarm row. Transactional alarm updates coalesce to the
  // final SQLite row, so only the final backend index side effect should run.
  const finalEffect = reflectApply(arrayAt, sideEffects, [-1]);
  if (finalEffect) await finalEffect();
}

async function setStorageAlarm(storage, alarmBinding, className, objectName, scheduledTime, sideEffects = null) {
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
  if (sideEffects) {
    reflectApply(arrayPush, sideEffects, [async () => {
      try {
        await effect();
      } catch (err) {
        // Transaction flushes only the final alarm side effect, so this rollback
        // is token-exact for the only setAlarm attempt that reached the backend.
        deleteAlarmRow(storage, token);
        throw err;
      }
    }]);
  } else {
    try {
      await effect();
    } catch (err) {
      // Non-transactional setAlarm writes the backend immediately; token-scoped
      // rollback preserves a newer alarm if user code raced another update.
      deleteAlarmRow(storage, token);
      throw err;
    }
  }
}

async function deleteStorageAlarm(storage, alarmBinding, className, objectName, sideEffects = null) {
  const row = readAlarmRow(storage);
  deleteAlarmRow(storage);
  const token = sideEffects?.baselineAlarmToken != null
    ? sideEffects.baselineAlarmToken
    : row?.token == null ? null : NativeString(row.token);
  const effect = () => token
    ? alarmBinding.deleteAlarmIndex({ className, objectName, token })
    : reflectApply(promiseResolve, NativePromise, ["skipped"]);
  if (sideEffects) {
    reflectApply(arrayPush, sideEffects, [async () => {
      try {
        await effect();
      } catch (err) {
        if (row) {
          writeAlarmRow(storage, {
            scheduledTime: row.scheduled_time,
            retryCount: row.retry_count,
            inFlight: NativeNumber(row.in_flight) === 1,
            token: row.token,
          });
        }
        throw err;
      }
    }]);
  } else {
    try {
      await effect();
    } catch (err) {
      if (row) {
        writeAlarmRow(storage, {
          scheduledTime: row.scheduled_time,
          retryCount: row.retry_count,
          inFlight: NativeNumber(row.in_flight) === 1,
          token: row.token,
        });
      }
      throw err;
    }
  }
}

async function getStorageAlarm(storage, alarmBinding, className, objectName) {
  const row = readAlarmRow(storage);
  if (!row || NativeNumber(row.in_flight) === 1) return null;
  const fields = alarmFieldsFromRow(row);
  if (!fields) {
    deleteAlarmRow(storage);
    return null;
  }
  try {
    await alarmBinding.setAlarmIndex({
      className,
      objectName,
      scheduledTime: fields.scheduledTime,
      retryCount: fields.retryCount,
      token: NativeString(row.token),
    });
  } catch (err) {
    logStructured("warn", "do_alarm_index_repair_failed", {
      class_name: className,
      object_name: objectName,
      ...formatWrappedError(err),
    });
  }
  return fields.scheduledTime;
}

function claimStorageAlarm(storage, alarm) {
  const row = readAlarmRow(storage);
  if (!row) return null;
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
  deleteAlarmRow(storage, token);
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
  const entries = await storage.list();
  const keys = [];
  reflectApply(mapForEach, entries, [(_value, key) => {
    reflectApply(arrayPush, keys, [key]);
  }]);
  if (keys.length) await storage.delete(keys);
}

function deleteAllSqlStorage(storage, deleteAlarm) {
  const result = storage.sql.exec(
    "SELECT type, name FROM sqlite_master " +
      "WHERE type IN ('trigger', 'view', 'table', 'index') " +
      "ORDER BY CASE type WHEN 'trigger' THEN 0 WHEN 'view' THEN 1 WHEN 'table' THEN 2 ELSE 3 END"
  );
  const rows = arrayIsArray(result) ? result : [...result];
  // workerd permits this connection-level PRAGMA; integration coverage pins it
  // because database-level PRAGMA writes are not part of the public DO SQL API.
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

function wrapStorage(storage, alarmBinding, className, objectName, sideEffects = null, alarmStorage = storage) {
  if (!storage || !alarmBinding) return storage;
  let syncTransactionSideEffects = null;
  const activeSideEffects = () => syncTransactionSideEffects || sideEffects;
  return new NativeProxy(storage, {
    get(target, prop, receiver) {
      if (prop === "setAlarm") {
        return (scheduledTime, _options = undefined) => {
          if (syncTransactionSideEffects) {
            throw new TypeError("setAlarm() cannot be used inside transactionSync(); use transaction()");
          }
          return setStorageAlarm(alarmStorage, alarmBinding, className, objectName, scheduledTime, activeSideEffects());
        };
      }
      if (prop === "getAlarm") {
        return (_options = undefined) => (
          getStorageAlarm(alarmStorage, alarmBinding, className, objectName)
        );
      }
      if (prop === "deleteAlarm") {
        return (_options = undefined) => {
          if (syncTransactionSideEffects) {
            throw new TypeError("deleteAlarm() cannot be used inside transactionSync(); use transaction()");
          }
          return deleteStorageAlarm(alarmStorage, alarmBinding, className, objectName, activeSideEffects());
        };
      }
      if (prop === "transaction") {
        return async (callback, ...rest) => {
          const txSideEffects = [];
          const baselineAlarm = readAlarmRow(alarmStorage);
          txSideEffects.baselineAlarmToken = baselineAlarm?.token == null ? null : NativeString(baselineAlarm.token);
          const wrapped = typeof callback === "function"
            ? (txn) => callback(wrapStorage(txn, alarmBinding, className, objectName, txSideEffects, target))
            : callback;
          const result = await reflectApply(reflectGet(target, prop, receiver), target, [wrapped, ...rest]);
          await flushAlarmSideEffects(txSideEffects);
          return result;
        };
      }
      if (prop === "transactionSync") {
        return (callback, ...rest) => {
          const previousSideEffects = syncTransactionSideEffects;
          const wrapped = typeof callback === "function" ? () => {
            syncTransactionSideEffects = true;
            try {
              // workerd transactionSync() invokes closure() without a txn
              // parameter; storage operations on this proxy are the txn surface.
              return callback();
            } finally {
              syncTransactionSideEffects = previousSideEffects;
            }
          } : callback;
          const result = reflectApply(reflectGet(target, prop, receiver), target, [wrapped, ...rest]);
          return result;
        };
      }
      if (prop === "deleteAll") {
        return (...args) => {
          if (syncTransactionSideEffects) {
            throw new TypeError("deleteAll() cannot be used inside transactionSync(); use transaction()");
          }
          return (async () => {
            if (args.length > 1) throw new TypeError("deleteAll() accepts at most one options argument");
            const options = args[0];
            const deleteAlarm = options?.deleteAlarm !== false;
            const alarmRow = readAlarmRow(alarmStorage);
            const preservedAlarm = deleteAlarm ? null : alarmRow;
            // Implement the supported deleteAll() surface through public storage
            // operations so the alarm shim and SQL object cleanup stay in sync.
            await deleteAllKvStorage(target);
            deleteAllSqlStorage(target, deleteAlarm);
            if (deleteAlarm) {
              const sideEffects = activeSideEffects();
              const token = sideEffects?.baselineAlarmToken != null
                ? sideEffects.baselineAlarmToken
                : alarmRow?.token == null ? null : NativeString(alarmRow.token);
              const effect = () => token
                ? alarmBinding.deleteAlarmIndex({ className, objectName, token })
                : reflectApply(promiseResolve, NativePromise, ["skipped"]);
              if (sideEffects) reflectApply(arrayPush, sideEffects, [effect]);
              else await effect();
            } else if (preservedAlarm) {
              writeAlarmRow(alarmStorage, {
                scheduledTime: preservedAlarm.scheduled_time,
                retryCount: preservedAlarm.retry_count,
                inFlight: NativeNumber(preservedAlarm.in_flight) === 1,
                token: preservedAlarm.token,
              });
            }
          })();
        };
      }
      const value = reflectGet(target, prop, receiver);
      return typeof value === "function" ? (...args) => reflectApply(value, target, args) : value;
    },
  });
}

function wrapCtx(ctx, alarmBinding, className) {
  if (!ctx || !alarmBinding) return ctx;
  const objectName = objectNameFromCtx(ctx);
  let storageProxy = null;
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
  if (!ctx || !alarmBinding) return ctx;
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
  return ctx;
}

export function wrapDurableObjectClass(Base, className) {
  return class extends Base {
    constructor(ctx, env) {
      const alarmBinding = env?.[ALARMS_BINDING];
      const constructorCtx = installStorageProxy(ctx, alarmBinding, className);
      // The inner host-binding wrapper owns env facade materialization. Strip
      // only the alarm binding here so __WDL_HOST_BINDINGS_WRAPPED can survive
      // through the two-layer wrapper contract.
      super(constructorCtx, withoutInternalEnv(env));
      // Resolve once through the host wrapper after construction so prototype
      // methods, class fields, and accessors retain the real instance receiver.
      const tenantFetch = reflectGet(this, "fetch", this);
      const wrappedCtx = wrapCtx(constructorCtx, alarmBinding, className);
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
