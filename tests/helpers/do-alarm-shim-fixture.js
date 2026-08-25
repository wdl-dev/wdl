/** @typedef {{ scheduled_time: number, retry_count: number, in_flight: number, token: string, last_error?: string | null }} DoAlarmRow */
/** @typedef {{ row: DoAlarmRow | null }} DoAlarmStorageState */
/** @typedef {{ storage: any, state: DoAlarmStorageState, kv: Map<string, unknown> }} DoAlarmStorageFixture */

/** @param {DoAlarmRow | null} [row] @returns {DoAlarmStorageFixture} */
export function makeDoAlarmStorage(row = null) {
  /** @type {DoAlarmStorageState} */
  const state = { row };
  /** @type {Map<string, unknown>} */
  const kv = new Map();
  /** @type {Array<{ rolledBack: boolean, closed: boolean, activeChildren: number }>} */
  const transactionStack = [];
  const storage = {
    sql: {
      /** @param {string} statement @param {unknown[]} params */
      exec(statement, ...params) {
        if (statement.startsWith("CREATE TABLE")) return [];
        if (statement.startsWith("SELECT scheduled_time")) return state.row ? [state.row] : [];
        if (statement.startsWith("SELECT type, name FROM sqlite_master")) {
          return state.row ? [{ type: "table", name: "_wdl_do_alarms" }] : [];
        }
        if (statement.startsWith("PRAGMA foreign_keys")) return [];
        if (statement.startsWith("DROP TABLE") && statement.includes("_wdl_do_alarms")) {
          state.row = null;
          return [];
        }
        if (statement.startsWith("INSERT INTO")) {
          state.row = {
            scheduled_time: /** @type {number} */ (params[0]),
            retry_count: /** @type {number} */ (params[1]),
            in_flight: /** @type {number} */ (params[2]),
            token: /** @type {string} */ (params[3]),
            last_error: /** @type {string | null | undefined} */ (params[4]),
          };
          return [];
        }
        if (statement.startsWith("UPDATE ") && statement.includes("SET token = ?, in_flight = 1")) {
          if (state.row?.token === String(params[1])) {
            state.row = { ...state.row, token: String(params[0]), in_flight: 1 };
          }
          return [];
        }
        if (statement.startsWith("UPDATE ") && statement.includes("WHERE id = 1 AND token = ?")) {
          if (state.row?.token === String(params[5])) {
            state.row = {
              scheduled_time: /** @type {number} */ (params[0]),
              retry_count: /** @type {number} */ (params[1]),
              in_flight: /** @type {number} */ (params[2]),
              token: /** @type {string} */ (params[3]),
              last_error: /** @type {string | null | undefined} */ (params[4]),
            };
          }
          return [];
        }
        if (statement.startsWith("DELETE FROM") && statement.includes("AND token")) {
          if (state.row?.token === String(params[0])) state.row = null;
          return [];
        }
        if (statement.startsWith("DELETE FROM")) {
          state.row = null;
          return [];
        }
        throw new Error(`unexpected SQL: ${statement}`);
      },
    },
    /** @param {() => unknown} callback */
    transactionSync(callback) {
      const snapshot = state.row ? { ...state.row } : null;
      const kvSnapshot = new Map(kv);
      try {
        return callback();
      } catch (err) {
        state.row = snapshot;
        kv.clear();
        for (const [key, value] of kvSnapshot) kv.set(key, value);
        throw err;
      }
    },
    /** @param {(txn?: unknown) => unknown} callback */
    async transaction(callback) {
      const snapshot = state.row ? { ...state.row } : null;
      const kvSnapshot = new Map(kv);
      const parentContext = transactionStack[transactionStack.length - 1] || null;
      const context = { rolledBack: false, closed: false, activeChildren: 0 };
      if (parentContext) parentContext.activeChildren += 1;
      transactionStack.push(context);
      const restore = () => {
        state.row = snapshot;
        kv.clear();
        for (const [key, value] of kvSnapshot) kv.set(key, value);
      };
      const assertActive = () => {
        if (context.rolledBack) throw new Error("Transaction has already been rolled back");
        if (context.closed) throw new Error("Transaction has already completed");
      };
      const txn = {
        async list() {
          assertActive();
          return kv;
        },
        /** @param {string[] | string} keys */
        async delete(keys) {
          assertActive();
          for (const key of Array.isArray(keys) ? keys : [keys]) kv.delete(key);
        },
        /** @param {string} key @param {unknown} value */
        async put(key, value) {
          assertActive();
          kv.set(key, value);
        },
        /** @param {string} key */
        async get(key) {
          assertActive();
          return kv.get(key);
        },
        rollback() {
          if (context.rolledBack) return;
          if (context.closed) throw new Error("Transaction has already completed");
          if (context.activeChildren > 0) {
            throw new Error("Cannot rollback while a nested transaction is active");
          }
          context.rolledBack = true;
          restore();
        },
      };
      try {
        const result = await callback(txn);
        context.closed = true;
        if (context.rolledBack) restore();
        return result;
      } catch (err) {
        context.closed = true;
        restore();
        throw err;
      } finally {
        transactionStack.pop();
        if (parentContext) parentContext.activeChildren -= 1;
      }
    },
    /** @param {{ limit?: number }} [options] */
    async list(options = {}) {
      const limit = options.limit ?? kv.size;
      return new Map([...kv].slice(0, limit));
    },
    /** @param {string[] | string} keys */
    async delete(keys) {
      for (const key of Array.isArray(keys) ? keys : [keys]) kv.delete(key);
    },
    /** @param {string} key @param {unknown} value */
    async put(key, value) {
      kv.set(key, value);
    },
    /** @param {string} key */
    async get(key) {
      return kv.get(key);
    },
    deleteAll() {
      state.row = null;
      kv.clear();
      return Promise.resolve();
    },
  };
  return { storage, state, kv };
}

/** @param {unknown[][]} calls */
export function makeDoAlarmBinding(calls) {
  return {
    /** @param {unknown} input */
    async setAlarmIndex(input) {
      calls.push(["set", input]);
    },
    /** @param {unknown} input */
    async deleteAlarmIndex(input) {
      calls.push(["delete", input]);
    },
  };
}
