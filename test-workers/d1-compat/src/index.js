async function resetTable(db, ddl, table) {
  await db.exec(`${ddl}; delete from ${table};`);
}

async function blobParams(env) {
  await resetTable(env.DB, "create table if not exists blobs (id text primary key, data blob)", "blobs");

  await env.DB.prepare("insert into blobs (id, data) values (?, ?)")
    .bind("array-buffer", new Uint8Array([0, 1, 2, 127, 255]).buffer)
    .run();
  const source = new Uint8Array([10, 20, 30, 40]);
  const typedArrayInsert = env.DB.prepare("insert into blobs (id, data) values (?, ?)")
    .bind("typed-array", source.subarray(1, 3));
  source.fill(99);
  await typedArrayInsert.run();

  const rows = await env.DB.prepare(`
    select id, hex(data) as hex, length(data) as size
    from blobs
    order by id
  `).all();
  const selected = await env.DB.prepare("select data from blobs where id = ?").bind("array-buffer").first("data");
  const raw = await env.DB.prepare("select data from blobs where id = ?").bind("typed-array").raw();
  return {
    rows: rows.results,
    selectedBlob: {
      isUint8Array: selected instanceof Uint8Array,
      bytes: Array.from(selected),
    },
    rawBlob: {
      isUint8Array: raw[0][0] instanceof Uint8Array,
      bytes: Array.from(raw[0][0]),
    },
  };
}

async function rawEdges(env) {
  await resetTable(env.DB, "create table if not exists raw_edges (id text primary key, value integer)", "raw_edges");
  await env.DB.batch([
    env.DB.prepare("insert into raw_edges (id, value) values (?, ?)").bind("r1", 1),
    env.DB.prepare("insert into raw_edges (id, value) values (?, ?)").bind("r2", null),
  ]);

  const statement = env.DB.prepare("select id, value from raw_edges order by id");
  return {
    defaultRows: await statement.raw(),
    namedRows: await statement.raw({ columnNames: true }),
    emptyRows: await env.DB.prepare("select id, value from raw_edges where id = ?").bind("missing").raw(),
    emptyNamedRows: await env.DB.prepare("select id, value from raw_edges where id = ?").bind("missing").raw({ columnNames: true }),
  };
}

async function batchError(env) {
  await resetTable(env.DB, "create table if not exists batch_items (id text primary key, body text)", "batch_items");
  try {
    await env.DB.batch([
      env.DB.prepare("insert into batch_items (id, body) values (?, ?)").bind("b1", "kept only if commit succeeds"),
      env.DB.prepare("insert into batch_items (id, body) values (?, ?)").bind("b1", "duplicate"),
      env.DB.prepare("insert into batch_items (id, body) values (?, ?)").bind("b2", "should not run"),
    ]);
  } catch (err) {
    const rows = await env.DB.prepare("select * from batch_items order by id").all();
    return {
      error: {
        name: err.name,
        message: err.message,
        code: err.code,
        category: err.category,
        retryable: err.retryable,
        statementIndex: err.statementIndex,
        causeCode: err.causeCode,
      },
      rows: rows.results,
    };
  }
  return { error: null, rows: (await env.DB.prepare("select * from batch_items").all()).results };
}

async function sqlDefaults(env) {
  await resetTable(env.DB,
    "create table if not exists allowed_defaults (id integer, stamp text default CURRENT_TIMESTAMP, value integer default 7)",
    "allowed_defaults");
  await env.DB.prepare("insert into allowed_defaults(id) values (1)").run();
  const allowed = await env.DB.prepare("select stamp, value from allowed_defaults").first();

  let error = null;
  try {
    await env.DB.prepare(
      "create table blocked_defaults (id integer, value text default (sqlite_version()))"
    ).run();
    await env.DB.prepare("insert into blocked_defaults(id) values (1)").run();
  } catch (err) {
    error = {
      name: err.name,
      code: err.code,
      category: err.category,
      retryable: err.retryable,
    };
  }
  const tableExists = await env.DB.prepare(
    "select count(*) as count from sqlite_master where name = ?"
  ).bind("blocked_defaults").first("count");
  const blockedRows = tableExists
    ? await env.DB.prepare("select count(*) as count from blocked_defaults").first("count")
    : 0;
  return { allowed, error, blockedRows };
}

const worker = {
  async fetch(request, env) {
    const url = new URL(request.url);
    const op = url.searchParams.get("op");
    if (op === "blob") return Response.json(await blobParams(env));
    if (op === "raw") return Response.json(await rawEdges(env));
    if (op === "batch-error") return Response.json(await batchError(env));
    if (op === "sql-defaults") return Response.json(await sqlDefaults(env));
    return new Response("unknown D1 compat op", { status: 400 });
  },
  get test() {
    if (this !== worker) throw new Error("default export accessor receiver changed");
    return undefined;
  },
  get unrelated() {
    throw new Error("unrelated default export getter evaluated");
  },
};

export default Object.freeze(worker);
