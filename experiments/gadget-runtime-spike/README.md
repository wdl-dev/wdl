# Gadget runtime spike

Throwaway harness answering one question: **can a WDL tenant worker load
AI-written code as a nested Dynamic Worker and isolate it in a Durable Object
facet — i.e. can WDL run the Cloudflare OS "gadget" model, and could wdl-chat
drop its per-session Lambda MicroVM?**

This directory owns no runtime or deployable service behavior and is therefore
outside `docs/source-map.md`. It exists to keep the reproduction and the
measured numbers alongside the conclusion. Nothing here should be merged into a
service tier as-is.

Measured against workerd `1.20260804.1` (the repository pin), standalone,
`workerd serve ... --experimental`.

```bash
npm install workerd@1.20260804.1
./run.sh          # full matrix
./run.sh --cpu    # also runs T-L/T-M, which wedge workerd and need SIGKILL
```

## What the harness models

Three layers, mirroring the real tiers:

| Harness | Real WDL |
|---|---|
| `host.js` → `WdlDoHostActor` | `do-runtime/actor.js`: owns `env.LOADER`, loads the tenant bundle, mounts the tenant DO as a level-1 facet |
| `tenant.js` → `ChatSessionDO` | wdl-chat's chat-worker: a *dynamically loaded* tenant worker whose DO already runs as a facet |
| `gadget-v1.js` / `gadget-v2.js` | AI-written gadget code, loaded at runtime, `globalOutbound: null` |

The layering matters: because a WDL tenant worker is itself loaded through
`workerLoader`, anything the tenant does with loaders or facets happens one
level deeper than anything WDL does today. That is precisely what is under test.

## Results

### The gadget model works, but only host-brokered

| ID | Question | Result |
|---|---|---|
| T-A | Can `env.LOADER` be cloned into a dynamic worker's env? | **No.** `DataCloneError: Could not serialize object of type "WorkerLoader"` |
| T-B | Can a dynamic worker's DO class cross JSRPC to another worker? | **No.** `DataCloneError: Entrypoints to dynamically-loaded workers cannot be transferred to other Workers` |
| T-C | Does a dynamically loaded worker get `ctx.exports` and `ctx.facets`? | **Yes**, both |
| T-D | Can a level-1 facet mount a level-2 facet of its *own* exported class? | **Yes** — facets nest fine |
| T-E | Host-brokered gadget: load, mount facet, SQLite | **Yes** — count 1 then 2 across calls |
| T-F | Gadget egress under `globalOutbound: null` | **Blocked**: *"This worker is not permitted to access the internet via global functions like fetch()"* |
| T-G | Per-gadget SQLite isolation | **Isolated** — two gadget ids, both count 1 |
| T-H | Code swap v1→v2→v1 via `facets.abort()` | **Works, storage preserved** — 3, then 13, then 24 |
| T-I | Can a tenant-chosen gadget name escape the host key prefix? | **No**, but see hardening note below |
| — | Does per-session state survive in the loader env? | **No** — loader memoization freezes it, see below |
| T-J | Does session-facet teardown cascade to gadget facets? | **No** — orphaned, see below |
| T-K | Cold start (fresh session + fresh gadget) | **25–50 ms**; warm 3–4 ms |
| T-L | Does `limits: { cpuMs: 200 }` stop a runaway gadget? | **No** — see below |
| T-M | Control: same runaway in ordinary tenant code | **Same outcome** — not gadget-specific |

### T-A/T-B force the architecture

Both tenant-side variants are closed by the runtime, not by policy. workerd's
own error message states the remedy: *"have the parent Worker expose an
entrypoint which constructs the dynamic worker and forwards to it."*

So gadget hosting **cannot be tenant-side in WDL**. It has to be a platform
tier that owns the loader and the facet, handing tenant code only a forwarding
RPC stub — the same shape as `runtime/bindings/kv.js`, and the same reason
`cloudflare-os`'s overseer is a static worker that does the loading, the
mounting, *and* the forwarding itself.

`host.js` implements that: `GadgetBridge` (a `WorkerEntrypoint` whose identity
comes from host-authored `ctx.props`, never from arguments) forwards into
`GadgetHostActor`, which owns `env.LOADER` and the gadget facets. The tenant
names a gadget id and a code version; the host resolves the source, so a tenant
cannot smuggle code under a key the host already resolved.

`GadgetHostActor` is deliberately a *separate* actor from the session host.
Routing gadget calls back into the session actor would re-enter a Durable
Object already blocked on the tenant facet.

### Loader memoization freezes anything baked into `env`

The first version of this harness put `session` into `GadgetBridge`'s props at
load time. T-J then reported session `z`'s gadget under scope `.../s1`: because
`LOADER.get(key, cb)` memoizes by key, the callback that builds `env` runs
**once per worker version**, so every later session inherited whichever session
loaded first. A silent, plausible-looking wrong answer.

The layering that works is the one WDL already uses: **per-load identity in
`env`, per-call identity in `getDurableObjectClass(className, { props })`.**
Only the second is evaluated per dispatch.

That constrains what the platform can attest. `ns` and `worker` are per-load and
genuinely host-authored, so the gadget key is scoped `gadget/<ns>/<worker>/…`.
A *session* is a tenant-level concept the platform has no independent view of,
so the session component rides inside the tenant-supplied gadget name and is
untrusted in exactly the way a KV key or a DO name is untrusted — a tenant can
address any gadget within its own namespace, and nothing outside it. The tenant
reads its own session id from `ctx.props`, which *is* per-facet and therefore
trustworthy on that side.

### T-J: teardown does not cascade

Aborting *and* hard-deleting the session facet left gadget storage fully intact
(the counter kept climbing 3 → 4). Because gadget facets live in a different
host actor, nothing cascades. The MicroVM model gets this for free — `/workspace`
dies with the VM. A gadget runtime needs explicit cascading GC keyed on session
close, or it inherits and widens wdl-chat's existing "closed sessions are not
reaped" limitation.

### T-L/T-M: `limits.cpuMs` did not save us

`limits: { cpuMs: 200 }` was accepted by the loader and **did not interrupt a
synchronous infinite loop**. The process stayed pegged, stopped serving
*unrelated* sessions in *other* actors, and ignored SIGTERM — it needed SIGKILL,
because a wedged JS loop never reaches the signal handler.

T-M is the control and matters for how this is read: ordinary tenant code with
the same loop does exactly the same thing. **This is a standalone-workerd
property, not something gadgets introduce.** WDL passes no `limits` to
`workerLoader` today and `docs/` does not discuss CPU starvation at all, so this
is a pre-existing gap worth its own look regardless of the gadget question.

What gadgets change is exposure, not mechanism: today reaching this state
requires a deploy — gated, versioned, attributable to a namespace. With gadgets,
unreviewed LLM-written code reaches the same runtime with no deploy step.

### T-I hardening note

The tenant-supplied name is concatenated into the loader key, so
`../../host-private` becomes the literal key
`gadget/tmp-demo/chat-worker/../../host-private/v1`. Loader keys are opaque
strings, not paths, so there is no traversal and no collision here. It stays a
hardening point rather than a bug only because the host prefix is host-authored
and every tenant-supplied byte lands strictly after it — encode or validate the
tenant component before this becomes load-bearing.

## Bottom line

The gadget model runs on WDL's exact workerd pin, boots ~100× faster than a
MicroVM, isolates storage per gadget, blocks egress completely, and hot-swaps
AI-edited code without a redeploy. It cannot be built inside chat-worker. It is
a platform tier — a `gadget-runtime` alongside `d1-runtime` and `do-runtime` —
and it needs two things this spike shows WDL does not have: cascading facet GC
on session close, and a real answer for runaway CPU.
