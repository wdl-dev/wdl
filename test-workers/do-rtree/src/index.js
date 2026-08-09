import { DurableObject } from "cloudflare:workers";

export class SpatialIndex extends DurableObject {
  fetch() {
    const sql = this.ctx.storage.sql;
    sql.exec(
      "CREATE VIRTUAL TABLE IF NOT EXISTS spatial_index USING rtree(id, min_x, max_x, min_y, max_y)"
    );
    sql.exec("DELETE FROM spatial_index");
    sql.exec(
      "INSERT INTO spatial_index VALUES (1, -1, 1, -1, 1), (2, 10, 12, 10, 12)"
    );
    const ids = [...sql.exec(
      "SELECT id FROM spatial_index WHERE min_x <= 2 AND max_x >= -2 AND min_y <= 2 AND max_y >= -2 ORDER BY id"
    )].map((row) => row.id);
    const check = [...sql.exec(
      "SELECT rtreecheck('spatial_index') AS result"
    )][0]?.result;
    return Response.json({ ids, check });
  }
}

export default {
  fetch(request, env) {
    const id = env.SPATIAL.idFromName("main");
    return env.SPATIAL.get(id).fetch(request);
  },
};
