export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const op = url.searchParams.get("op");
    const id = url.searchParams.get("id") || "m1";
    const body = url.searchParams.get("body") || "hello";
    if (op === "init") {
      return Response.json(await env.DB.exec("create table if not exists messages (id text, body text)"));
    }
    if (op === "exec-semicolon") {
      await env.DB.exec("insert into messages (id, body) values ('semi', 'a;b');");
      return Response.json(await env.DB.prepare("select * from messages where id = ?").bind("semi").first());
    }
    if (op === "insert") {
      return Response.json(await env.DB.prepare("insert into messages (id, body) values (?, ?)").bind(id, body).run());
    }
    if (op === "get") {
      return Response.json(await env.DB.prepare("select * from messages where id = ?").bind(id).first());
    }
    if (op === "raw") {
      return Response.json(await env.DB.prepare("select * from messages where id = ?").bind(id).raw({ columnNames: true }));
    }
    if (op === "raw-missing") {
      return Response.json(await env.DB.prepare("select id, body from messages where id = ?").bind(id).raw({ columnNames: true }));
    }
    if (op === "batch") {
      return Response.json(await env.DB.batch([
        env.DB.prepare("insert into messages (id, body) values (?, ?)").bind(id, body),
        env.DB.prepare("select * from messages where id = ?").bind(id),
      ]));
    }
    if (op === "batch-fail") {
      try {
        await env.DB.batch([
          env.DB.prepare("insert into messages (id, body) values (?, ?)").bind(id, body),
          env.DB.prepare("insert into missing_table (id) values (?)").bind(id),
        ]);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        return Response.json({
          error: "d1_batch_failed",
          message: message,
          row: await env.DB.prepare("select * from messages where id = ?").bind(id).first(),
        });
      }
      return Response.json({ ok: true, row: await env.DB.prepare("select * from messages where id = ?").bind(id).first() });
    }
    if (op === "session") {
      const session = env.DB.withSession("first-primary");
      const row = await session.prepare("select * from messages where id = ?").bind(id).first();
      return Response.json({ row, bookmark: session.getBookmark() });
    }
    if (op === "rtree") {
      await env.DB.exec("create virtual table if not exists spatial_index using rtree(id, min_x, max_x, min_y, max_y)");
      await env.DB.exec("delete from spatial_index");
      await env.DB.exec("insert into spatial_index values (1, -1, 1, -1, 1), (2, 10, 12, 10, 12)");
      const rows = await env.DB.prepare(
        "select id from spatial_index where min_x <= 2 and max_x >= -2 and min_y <= 2 and max_y >= -2 order by id"
      ).raw({ columnNames: true });
      const check = await env.DB.prepare("select rtreecheck('spatial_index') as result").first("result");
      return Response.json({ rows, check });
    }
    return new Response("bad op", { status: 400 });
  }
};
