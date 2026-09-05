import { DurableObject } from "cloudflare:workers";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** @param {URLSearchParams} params @param {string} name @param {number} fallback */
function numberParamOr(params, name, fallback) {
  const raw = params.get(name);
  const value = raw == null || raw === "" ? fallback : raw;
  return Number(value);
}

export class AlarmCounter extends DurableObject {
  ensureTable() {
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS alarm_crash (name TEXT PRIMARY KEY, value INTEGER NOT NULL)"
    );
  }

  read(name) {
    this.ensureTable();
    const row = [...this.ctx.storage.sql.exec("SELECT value FROM alarm_crash WHERE name = ?", name)][0];
    return row?.value ?? 0;
  }

  write(name, value) {
    this.ensureTable();
    this.ctx.storage.sql.exec(
      "INSERT INTO alarm_crash (name, value) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET value = excluded.value",
      name,
      value
    );
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/schedule-blocking") {
      await this.ctx.storage.put("block-once-ms", numberParamOr(url.searchParams, "ms", 300000));
      await this.ctx.storage.put("start-delay-ms", numberParamOr(url.searchParams, "startDelayMs", 0));
      await this.ctx.storage.setAlarm(Date.now() + 200);
      return Response.json({ pending: typeof (await this.ctx.storage.getAlarm()) === "number" });
    }
    const progress = {
      started: this.read("started"),
      alarms: this.read("alarms"),
    };
    if (url.pathname === "/progress") return Response.json(progress);
    return Response.json({
      ...progress,
      pending: await this.ctx.storage.getAlarm(),
    });
  }

  async alarm() {
    const startDelayMs = Number((await this.ctx.storage.get("start-delay-ms")) ?? 0);
    if (startDelayMs > 0) await sleep(startDelayMs);
    const blockMs = Number((await this.ctx.storage.get("block-once-ms")) ?? 0);
    if (blockMs > 0) {
      await this.ctx.storage.delete("block-once-ms");
      this.write("started", this.read("started") + 1);
      await sleep(blockMs);
    }
    this.write("alarms", this.read("alarms") + 1);
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const id = env.ALARMS.idFromName(url.searchParams.get("name") || "main");
    return await env.ALARMS.get(id).fetch("https://do.internal" + url.pathname + url.search);
  },
};
