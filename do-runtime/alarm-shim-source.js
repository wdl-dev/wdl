export const DO_ALARM_SHIM_SOURCE = `
const ALARM_HEADER = "x-wdl-do-internal-alarm";
const ALARMS_BINDING = "__WDL_DO_ALARMS__";
const ALARM_TABLE = "_wdl_do_alarms";

function withoutInternalEnv(env) {
  if (!env || typeof env !== "object" || !(ALARMS_BINDING in env)) return env;
  const out = { ...env };
  delete out[ALARMS_BINDING];
  return out;
}

function objectNameFromCtx(ctx) {
  return String(ctx.id);
}

function scheduledTimeFromInput(value) {
  const scheduledTime = value instanceof Date ? value.getTime() : Number(value);
  if (!Number.isFinite(scheduledTime) || scheduledTime <= 0) {
    throw new TypeError("setAlarm() cannot be called with an alarm time <= 0");
  }
  return scheduledTime;
}

function retryCountFromInput(value) {
  const retryCount = Number(value ?? 0);
  if (!Number.isInteger(retryCount) || retryCount < 0) {
    throw new TypeError("DO alarm retryCount must be a non-negative integer");
  }
  return retryCount;
}

function alarmFieldsFromRow(row) {
  const scheduledTime = Number(row.scheduled_time);
  const retryCount = Number(row.retry_count);
  if (!Number.isFinite(scheduledTime) || scheduledTime <= 0) return null;
  if (!Number.isInteger(retryCount) || retryCount < 0) return null;
  return { scheduledTime, retryCount };
}

function alarmToken() {
  return crypto.randomUUID();
}

function formatWrappedError(err) {
  const out = {
    error_name: err?.name || "Error",
    error_message: err instanceof Error ? err.message : String(err),
  };
  if (err?.code != null) out.error_code = String(err.code);
  return out;
}

function logStructured(level, event, fields = {}) {
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
  return [...storage.sql.exec(
    "SELECT scheduled_time, retry_count, in_flight, token FROM " + ALARM_TABLE + " WHERE id = 1"
  )][0] || null;
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
    Math.trunc(scheduledTimeFromInput(row.scheduledTime)),
    retryCountFromInput(row.retryCount),
    row.inFlight ? 1 : 0,
    String(row.token),
    row.lastError == null ? null : String(row.lastError)
  );
}

function deleteAlarmRow(storage, token = null) {
  ensureAlarmTable(storage);
  if (token == null) {
    storage.sql.exec("DELETE FROM " + ALARM_TABLE + " WHERE id = 1");
    return;
  }
  storage.sql.exec("DELETE FROM " + ALARM_TABLE + " WHERE id = 1 AND token = ?", String(token));
}

async function flushAlarmSideEffects(sideEffects) {
  // One object has one alarm row. Transactional alarm updates coalesce to the
  // final SQLite row, so only the final backend index side effect should run.
  const finalEffect = sideEffects.at(-1);
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
    sideEffects.push(async () => {
      try {
        await effect();
      } catch (err) {
        // Transaction flushes only the final alarm side effect, so this rollback
        // is token-exact for the only setAlarm attempt that reached the backend.
        deleteAlarmRow(storage, token);
        throw err;
      }
    });
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
    : row?.token == null ? null : String(row.token);
  const effect = () => token
    ? alarmBinding.deleteAlarmIndex({ className, objectName, token })
    : Promise.resolve("skipped");
  if (sideEffects) {
    sideEffects.push(async () => {
      try {
        await effect();
      } catch (err) {
        if (row) {
          writeAlarmRow(storage, {
            scheduledTime: row.scheduled_time,
            retryCount: row.retry_count,
            inFlight: Number(row.in_flight) === 1,
            token: row.token,
          });
        }
        throw err;
      }
    });
  } else {
    try {
      await effect();
    } catch (err) {
      if (row) {
        writeAlarmRow(storage, {
          scheduledTime: row.scheduled_time,
          retryCount: row.retry_count,
          inFlight: Number(row.in_flight) === 1,
          token: row.token,
        });
      }
      throw err;
    }
  }
}

async function getStorageAlarm(storage, alarmBinding, className, objectName) {
  const row = readAlarmRow(storage);
  if (!row || Number(row.in_flight) === 1) return null;
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
      token: String(row.token),
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
  const rowToken = String(row.token);
  const alarmTokenValue = alarm?.token == null ? null : String(alarm.token);
  if (alarmTokenValue && alarmTokenValue !== rowToken) return null;
  const retryCount = retryCountFromInput(alarm?.retryCount ?? fields.retryCount);
  if (Number(row.in_flight) !== 1 && fields.scheduledTime > Date.now()) return null;
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
  return '"' + String(name).replaceAll('"', '""') + '"';
}

function sqlObjectDropStatement(row) {
  const type = String(row.type);
  const name = String(row.name);
  const lowerName = name.toLowerCase();
  if (lowerName.startsWith("sqlite_") || lowerName.startsWith("_cf_")) return null;
  if (type === "table") return "DROP TABLE IF EXISTS " + quoteSqlIdentifier(name);
  if (type === "view") return "DROP VIEW IF EXISTS " + quoteSqlIdentifier(name);
  if (type === "trigger") return "DROP TRIGGER IF EXISTS " + quoteSqlIdentifier(name);
  if (type === "index") return "DROP INDEX IF EXISTS " + quoteSqlIdentifier(name);
  return null;
}

async function deleteAllKvStorage(storage) {
  const entries = await storage.list();
  const keys = [...entries.keys()];
  if (keys.length) await storage.delete(keys);
}

function deleteAllSqlStorage(storage, deleteAlarm) {
  const rows = [...storage.sql.exec(
    "SELECT type, name FROM sqlite_master " +
      "WHERE type IN ('trigger', 'view', 'table', 'index') " +
      "ORDER BY CASE type WHEN 'trigger' THEN 0 WHEN 'view' THEN 1 WHEN 'table' THEN 2 ELSE 3 END"
  )];
  // workerd permits this connection-level PRAGMA; integration coverage pins it
  // because database-level PRAGMA writes are not part of the public DO SQL API.
  storage.sql.exec("PRAGMA foreign_keys = OFF");
  try {
    for (const row of rows) {
      if (String(row.name) === ALARM_TABLE && !deleteAlarm) continue;
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
  return new Proxy(storage, {
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
          txSideEffects.baselineAlarmToken = baselineAlarm?.token == null ? null : String(baselineAlarm.token);
          const wrapped = typeof callback === "function"
            ? (txn) => callback(wrapStorage(txn, alarmBinding, className, objectName, txSideEffects, target))
            : callback;
          const result = await Reflect.apply(Reflect.get(target, prop, receiver), target, [wrapped, ...rest]);
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
          const result = Reflect.apply(Reflect.get(target, prop, receiver), target, [wrapped, ...rest]);
          return result;
        };
      }
      if (prop === "deleteAll") {
        return (...args) => {
          if (syncTransactionSideEffects) {
            throw new TypeError("deleteAll() cannot be used inside transactionSync(); use transaction()");
          }
          return (async () => {
            const [options, ...rest] = args;
            if (rest.length) throw new TypeError("deleteAll() accepts at most one options argument");
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
                : alarmRow?.token == null ? null : String(alarmRow.token);
              const effect = () => token
                ? alarmBinding.deleteAlarmIndex({ className, objectName, token })
                : Promise.resolve("skipped");
              if (sideEffects) sideEffects.push(effect);
              else await effect();
            } else if (preservedAlarm) {
              writeAlarmRow(alarmStorage, {
                scheduledTime: preservedAlarm.scheduled_time,
                retryCount: preservedAlarm.retry_count,
                inFlight: Number(preservedAlarm.in_flight) === 1,
                token: preservedAlarm.token,
              });
            }
          })();
        };
      }
      const value = Reflect.get(target, prop, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

function wrapCtx(ctx, alarmBinding, className) {
  if (!ctx || !alarmBinding) return ctx;
  const objectName = objectNameFromCtx(ctx);
  let storageProxy = null;
  return new Proxy(ctx, {
    get(target, prop, receiver) {
      if (prop === "storage") {
        const storage = Reflect.get(target, prop, receiver);
        if (!storageProxy) storageProxy = wrapStorage(storage, alarmBinding, className, objectName);
        return storageProxy;
      }
      const value = Reflect.get(target, prop, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

function installStorageProxy(ctx, alarmBinding, className) {
  if (!ctx || !alarmBinding) return ctx;
  const objectName = objectNameFromCtx(ctx);
  const storageProxy = wrapStorage(ctx.storage, alarmBinding, className, objectName);
  try {
    Object.defineProperty(ctx, "storage", {
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
      const wrappedCtx = wrapCtx(constructorCtx, alarmBinding, className);
      try {
        Object.defineProperty(this, "ctx", {
          value: wrappedCtx,
          configurable: true,
          writable: true,
        });
      } catch {
        this.ctx = wrappedCtx;
      }
    }

    async fetch(request) {
      if (request.headers.get(ALARM_HEADER) === "1") {
        const alarm = await request.json();
        const claim = claimStorageAlarm(this.ctx.storage, alarm);
        if (!claim) return Response.json({ ok: true, ignored: true });
        if (typeof super.alarm === "function") {
          await super.alarm({
            retryCount: claim.retryCount,
            isRetry: claim.retryCount > 0,
          });
        }
        completeStorageAlarm(this.ctx.storage, claim.token);
        return Response.json({ ok: true });
      }
      if (typeof super.fetch !== "function") {
        return new Response("Durable Object class has no fetch handler", { status: 500 });
      }
      return await super.fetch(request);
    }
  };
}
`;
