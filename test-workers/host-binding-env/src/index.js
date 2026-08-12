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
  async fetch() {
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
    if (new URL(request.url).pathname === "/do") {
      return await env.ROOM.get(env.ROOM.idFromName("scope")).fetch(request);
    }
    return Response.json(await surface(env));
  },
};
