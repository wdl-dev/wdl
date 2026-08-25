import { DurableObject } from "cloudflare:workers";

export class AlarmCounter extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx = ctx;
    this.env = env;
    this.constructorStorage = ctx.storage;
  }

  ensureTable() {
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS alarms (name TEXT PRIMARY KEY, value INTEGER NOT NULL)"
    );
  }

  read(name) {
    this.ensureTable();
    const row = [...this.ctx.storage.sql.exec("SELECT value FROM alarms WHERE name = ?", name)][0];
    return row?.value ?? 0;
  }

  write(name, value) {
    this.ensureTable();
    this.ctx.storage.sql.exec(
      "INSERT INTO alarms (name, value) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET value = excluded.value",
      name,
      value
    );
  }

  async seedDeleteAllState() {
    await this.ctx.storage.put("kv-key", "kv-value");
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS user_rows (id INTEGER PRIMARY KEY, value TEXT)");
    this.ctx.storage.sql.exec(
      "INSERT INTO user_rows (id, value) VALUES (1, 'sql-value') ON CONFLICT(id) DO UPDATE SET value = excluded.value"
    );
    await this.ctx.storage.setAlarm(Date.now() + 60000);
  }

  async deleteAllSnapshot() {
    let sqlTableExists = true;
    try {
      [...this.ctx.storage.sql.exec("SELECT value FROM user_rows WHERE id = 1")];
    } catch {
      sqlTableExists = false;
    }
    const kv = await this.ctx.storage.get("kv-key");
    return {
      kv: kv ?? null,
      sqlTableExists,
      alarm: await this.ctx.storage.getAlarm(),
    };
  }

  seedDeleteAllSqlEdges() {
    const sql = this.ctx.storage.sql;
    const exec = (label, statement) => {
      try {
        sql.exec(statement);
      } catch (err) {
        throw new Error(label + ": " + (err instanceof Error ? err.message : String(err)));
      }
    };
    exec("create-edge-audit", "CREATE TABLE IF NOT EXISTS edge_audit (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT)");
    exec("create-edge-rows", "CREATE TABLE IF NOT EXISTS edge_rows (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT)");
    exec("create-index", "CREATE INDEX IF NOT EXISTS edge_rows_value ON edge_rows(value)");
    exec("create-view", "CREATE VIEW IF NOT EXISTS edge_view AS SELECT value FROM edge_rows");
    exec("create-trigger",
      "CREATE TRIGGER IF NOT EXISTS edge_rows_ai AFTER INSERT ON edge_rows " +
        "BEGIN INSERT INTO edge_audit(value) VALUES (new.value); END"
    );
    exec("create-fts", "CREATE VIRTUAL TABLE IF NOT EXISTS edge_fts USING fts5(content)");
    exec("insert-edge-row", "INSERT INTO edge_rows(value) VALUES ('row-value')");
    exec("insert-fts-row", "INSERT INTO edge_fts(content) VALUES ('full text value')");
  }

  deleteAllSqlEdgeSnapshot() {
    const names = [...this.ctx.storage.sql.exec(
      "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name"
    )].map((row) => String(row.type) + ":" + String(row.name));
    let sqliteSequenceRows = null;
    try {
      sqliteSequenceRows = [...this.ctx.storage.sql.exec("SELECT name FROM sqlite_sequence ORDER BY name")]
        .map((row) => row.name);
    } catch {}
    this.ctx.storage.sql.exec("CREATE TABLE recreated_ids (id INTEGER PRIMARY KEY AUTOINCREMENT)");
    this.ctx.storage.sql.exec("INSERT INTO recreated_ids DEFAULT VALUES");
    const recreatedId = [...this.ctx.storage.sql.exec("SELECT id FROM recreated_ids")][0]?.id ?? null;
    this.ctx.storage.sql.exec("DROP TABLE recreated_ids");
    return { names, sqliteSequenceRows, recreatedId };
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/schedule") {
      await this.ctx.storage.setAlarm(Date.now() - 1000);
      const alarm = await this.ctx.storage.getAlarm();
      return Response.json({
        pending: typeof alarm === "number",
        envHasInternalAlarmBinding: Object.hasOwn(this.env, "__WDL_DO_ALARMS__"),
      });
    }
    if (url.pathname === "/schedule-future") {
      await this.ctx.storage.setAlarm(Date.now() + 60000);
      return Response.json({ pending: typeof (await this.ctx.storage.getAlarm()) === "number" });
    }
    if (url.pathname === "/schedule-soon") {
      await this.ctx.storage.setAlarm(Date.now() + 1000);
      return Response.json({ pending: typeof (await this.ctx.storage.getAlarm()) === "number" });
    }
    if (url.pathname === "/schedule-failing") {
      await this.ctx.storage.put("fail-alarm", true);
      await this.ctx.storage.setAlarm(Date.now() - 1000);
      return Response.json({ pending: typeof (await this.ctx.storage.getAlarm()) === "number" });
    }
    if (url.pathname === "/schedule-repair") {
      await this.ctx.storage.setAlarm(Date.now() + 30000);
      return Response.json({ pending: typeof (await this.ctx.storage.getAlarm()) === "number" });
    }
    if (url.pathname === "/schedule-constructor-cache") {
      await this.constructorStorage.setAlarm(Date.now() + 60000);
      return Response.json({ pending: typeof (await this.constructorStorage.getAlarm()) === "number" });
    }
    if (url.pathname === "/schedule-transaction") {
      await this.ctx.storage.transaction(async (txn) => {
        await txn.setAlarm(Date.now() - 1000);
      });
      return Response.json({ pending: typeof (await this.ctx.storage.getAlarm()) === "number" });
    }
    if (url.pathname === "/schedule-transaction-twice") {
      await this.ctx.storage.transaction(async (txn) => {
        await txn.setAlarm(Date.now() + 60000);
        await txn.setAlarm(Date.now() - 1000);
      });
      return Response.json({ pending: typeof (await this.ctx.storage.getAlarm()) === "number" });
    }
    if (url.pathname === "/schedule-transaction-sync") {
      try {
        this.ctx.storage.transactionSync(() => {
          this.ctx.storage.setAlarm(Date.now() - 1000);
        });
      } catch (error) {
        return Response.json({
          rejected: true,
          message: error instanceof Error ? error.message : String(error),
          pending: await this.ctx.storage.getAlarm(),
        });
      }
      return Response.json({ rejected: false, pending: await this.ctx.storage.getAlarm() });
    }
    if (url.pathname === "/replace-future-with-due") {
      await this.ctx.storage.setAlarm(Date.now() + 1500);
      await this.ctx.storage.setAlarm(Date.now() - 1000);
      return Response.json({ pending: typeof (await this.ctx.storage.getAlarm()) === "number" });
    }
    if (url.pathname === "/schedule-rollback") {
      try {
        await this.ctx.storage.transaction(async (txn) => {
          await txn.setAlarm(Date.now() - 1000);
          throw new Error("rollback alarm transaction");
        });
      } catch {}
      return Response.json({ pending: await this.ctx.storage.getAlarm() });
    }
    if (url.pathname === "/delete") {
      await this.ctx.storage.setAlarm(Date.now() + 60000);
      await this.ctx.storage.deleteAlarm();
      return Response.json({ alarm: await this.ctx.storage.getAlarm() });
    }
    if (url.pathname === "/schedule-short-delete") {
      await this.ctx.storage.setAlarm(Date.now() + 1000);
      await this.ctx.storage.deleteAlarm();
      return Response.json({ alarm: await this.ctx.storage.getAlarm() });
    }
    if (url.pathname === "/delete-after-transaction-sync-set") {
      await this.ctx.storage.setAlarm(Date.now() + 60000);
      try {
        this.ctx.storage.transactionSync(() => {
          this.ctx.storage.deleteAlarm();
        });
      } catch (error) {
        await this.ctx.storage.deleteAlarm();
        return Response.json({
          rejected: true,
          message: error instanceof Error ? error.message : String(error),
          alarm: await this.ctx.storage.getAlarm(),
        });
      }
      return Response.json({ rejected: false, alarm: await this.ctx.storage.getAlarm() });
    }
    if (url.pathname === "/delete-all-default") {
      await this.seedDeleteAllState();
      await this.ctx.storage.deleteAll();
      return Response.json(await this.deleteAllSnapshot());
    }
    if (url.pathname === "/delete-all-empty-options") {
      await this.seedDeleteAllState();
      await this.ctx.storage.deleteAll({});
      return Response.json(await this.deleteAllSnapshot());
    }
    if (url.pathname === "/delete-all-explicit-true") {
      await this.seedDeleteAllState();
      await this.ctx.storage.deleteAll({ deleteAlarm: true });
      return Response.json(await this.deleteAllSnapshot());
    }
    if (url.pathname === "/delete-all-keep-alarm") {
      await this.seedDeleteAllState();
      await this.ctx.storage.deleteAll({ deleteAlarm: false });
      const snapshot = await this.deleteAllSnapshot();
      return Response.json({
        kv: snapshot.kv,
        sqlTableExists: snapshot.sqlTableExists,
        pending: typeof snapshot.alarm === "number",
      });
    }
    if (url.pathname === "/delete-all-transaction") {
      await this.seedDeleteAllState();
      let rejected = false;
      let message = null;
      await this.ctx.storage.transaction(async (txn) => {
        try {
          await this.ctx.storage.deleteAll();
        } catch (error) {
          rejected = true;
          message = error instanceof Error ? error.message : String(error);
        }
        txn.rollback();
      });
      const snapshot = await this.deleteAllSnapshot();
      return Response.json({
        rejected,
        message,
        kv: snapshot.kv,
        sqlTableExists: snapshot.sqlTableExists,
        pending: typeof snapshot.alarm === "number",
      });
    }
    if (url.pathname === "/transaction-reaction-window") {
      const callbackEntered = Promise.withResolvers();
      const callbackResult = Promise.withResolvers();
      const transaction = this.ctx.storage.transaction(() => {
        callbackEntered.resolve();
        return callbackResult.promise;
      });
      await callbackEntered.promise;
      const outsideReaction = callbackResult.promise.catch(() => {
        try {
          this.ctx.storage.setAlarm(Date.now() + 60000);
          return "fulfilled";
        } catch (error) {
          return error instanceof Error ? error.message : String(error);
        }
      });
      callbackResult.reject(new Error("transaction rollback"));
      let transactionMessage = null;
      try {
        await transaction;
      } catch (error) {
        transactionMessage = error instanceof Error ? error.message : String(error);
      }
      return Response.json({
        transactionMessage,
        outsideOutcome: await outsideReaction,
        alarm: await this.ctx.storage.getAlarm(),
      });
    }
    if (url.pathname === "/delete-all-sql-edges") {
      this.seedDeleteAllSqlEdges();
      await this.ctx.storage.deleteAll();
      return Response.json(this.deleteAllSqlEdgeSnapshot());
    }
    if (url.pathname === "/failure-status") {
      return Response.json({
        alarms: this.read("count"),
        retry: this.read("retry"),
        pending: await this.ctx.storage.getAlarm(),
      });
    }
    return Response.json({
      alarms: this.read("count"),
      alarmDuringHandlerWasNull: this.read("during-null"),
      pending: await this.ctx.storage.getAlarm(),
    });
  }

  async alarm(alarmInfo) {
    const current = this.read("count");
    this.write("count", current + 1);
    this.write("retry", alarmInfo.retryCount || 0);
    this.write("during-null", (await this.ctx.storage.getAlarm()) === null ? 1 : 0);
    if (await this.ctx.storage.get("fail-alarm")) {
      throw new Error("intentional alarm failure");
    }
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.ALARMS.idFromName(url.searchParams.get("name") || "main");
    const stub = env.ALARMS.get(id);
    if (url.pathname === "/spoof") {
      return await stub.fetch("https://do.internal/status", {
        headers: { "x-wdl-do-internal-alarm": "1" },
      });
    }
    return await stub.fetch("https://do.internal" + url.pathname);
  },
};
