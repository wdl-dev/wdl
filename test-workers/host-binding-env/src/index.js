import {
  DurableObject,
  WorkflowEntrypoint,
  env as importedEnv,
} from "cloudflare:workers";

const moduleScope = {
  room: importedEnv.ROOM,
  flow: importedEnv.FLOW,
  alarms: importedEnv.__WDL_DO_ALARMS__,
  doBackend: importedEnv.__WDL_DO_BACKEND__,
  ownerNetwork: importedEnv.__WDL_DO_OWNER_NETWORK__,
  workflowsBackend: importedEnv.__WDL_WORKFLOWS_BACKEND__,
};

async function abortStalledFetch(fetcher, url) {
  const controller = new AbortController();
  const reason = new DOMException("binding caller aborted", "AbortError");
  const request = new Request(url, {
    method: "POST",
    body: new ReadableStream({
      pull() {
        return new Promise(() => {});
      },
    }),
    signal: controller.signal,
  });
  const timeout = setTimeout(() => controller.abort(reason), 25);
  try {
    await fetcher(request);
    return { name: null, message: "resolved" };
  } catch (error) {
    return {
      name: error instanceof Error ? error.name : typeof error,
      message: error instanceof Error ? error.message : String(error),
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function doAbortResult(namespace, objectName) {
  const stub = namespace.get(namespace.idFromName(objectName));
  const error = await abortStalledFetch(
    (request) => stub.fetch(request),
    "https://do-binding.invalid/stalled"
  );
  const count = await stub.fetch("https://do-binding.invalid/__dispatch-count");
  return { ...error, dispatches: (await count.json()).dispatches };
}

async function moduleScopeFacadeCalls() {
  let room = moduleScope.room ? "resolved" : "missing";
  if (moduleScope.room) {
    try {
      await moduleScope.room.idFromName("module-scope");
    } catch {
      room = "rejected";
    }
  }
  let flow = moduleScope.flow ? "resolved" : "missing";
  if (moduleScope.flow) {
    try {
      await moduleScope.flow.create({ id: "module-scope" });
    } catch {
      flow = "rejected";
    }
  }
  return { room, flow };
}

async function surface(env) {
  return {
    positional: {
      room: typeof env.ROOM?.idFromName,
      flow: typeof env.FLOW?.create,
    },
    imported: {
      room: typeof importedEnv.ROOM?.idFromName,
      flow: typeof importedEnv.FLOW?.create,
    },
    moduleScope: {
      roomFacade: typeof moduleScope.room?.idFromName,
      roomTransport: typeof moduleScope.room?.fetch,
      flowFacade: typeof moduleScope.flow?.create,
      flowTransport: typeof moduleScope.flow?.fetch,
      alarmTransport: typeof moduleScope.alarms?.setAlarmIndex,
    },
    moduleScopeCalls: await moduleScopeFacadeCalls(),
    hidden: {
      doBackend: typeof moduleScope.doBackend,
      ownerNetwork: typeof moduleScope.ownerNetwork,
      workflowsBackend: typeof moduleScope.workflowsBackend,
    },
  };
}

export class Room extends DurableObject {
  async fetch(request) {
    const pathname = new URL(request.url).pathname;
    if (pathname === "/__dispatch-count") {
      return Response.json({ dispatches: (await this.ctx.storage.get("dispatches")) ?? 0 });
    }
    if (pathname === "/abort-nested") {
      return Response.json(await doAbortResult(this.env.ROOM, "nested-abort-target"));
    }
    const dispatches = (await this.ctx.storage.get("dispatches")) ?? 0;
    await this.ctx.storage.put("dispatches", dispatches + 1);
    return Response.json(await surface(this.env));
  }
}

export class Flow extends WorkflowEntrypoint {
  async run() {
    return null;
  }
}

export default {
  async fetch(request, env) {
    const pathname = new URL(request.url).pathname;
    if (pathname === "/abort-do") {
      return Response.json(await doAbortResult(env.ROOM, "user-abort-target"));
    }
    if (pathname === "/abort-do-nested") {
      return await env.ROOM.get(env.ROOM.idFromName("scope")).fetch(
        "https://do-binding.invalid/abort-nested"
      );
    }
    if (pathname === "/abort-workflow") {
      if (!moduleScope.flow || typeof moduleScope.flow.fetch !== "function") {
        return Response.json({ name: "Error", message: "missing Workflow transport" });
      }
      return Response.json(await abortStalledFetch(
        (scopedRequest) => moduleScope.flow.fetch(scopedRequest),
        "https://workflow-binding.invalid/internal/workflows/create"
      ));
    }
    if (pathname === "/do") {
      return await env.ROOM.get(env.ROOM.idFromName("scope")).fetch(request);
    }
    return Response.json(await surface(env));
  },
};
