#!/usr/bin/env bash
# Reproduces the gadget-runtime spike end to end. Throwaway harness, not a service.
#
#   ./run.sh          full matrix, skipping the destructive CPU test
#   ./run.sh --cpu    also run the CPU-starvation test (wedges workerd; SIGKILL follows)
set -uo pipefail

cd "$(dirname "$0")"
PORT=18080
BASE="http://127.0.0.1:${PORT}"
WORKERD="${WORKERD:-./node_modules/.bin/workerd}"

if [ ! -x "$WORKERD" ]; then
  echo "workerd not found at $WORKERD" >&2
  echo "install it with: npm install workerd@1.20260804.1" >&2
  exit 1
fi

cleanup() { pkill -9 -x workerd 2>/dev/null; }
trap cleanup EXIT

start() {
  pkill -9 -x workerd 2>/dev/null
  sleep 1
  rm -rf state && mkdir -p state
  nohup "$WORKERD" serve workerd.capnp --experimental >server.log 2>&1 &
  sleep 3
}

hit() { curl -s -m "${2:-15}" "${BASE}${1}"; echo; }
say() { printf '\n\033[1m%s\033[0m\n' "$1"; }

start

say "T-A  WorkerLoader binding cloned into a dynamic worker's env?  (expect: no)"
hit "/host/raw-loader-env?session=s1"

say "T-B  dynamic worker's DO class transferred over JSRPC?  (expect: no)"
hit "/host/transfer-class?session=s1"

say "T-C  ctx.exports and ctx.facets inside a dynamically loaded worker  (expect: both present)"
hit "/tenant/ping?session=s1"

say "T-D  level-1 facet mounts a level-2 facet of its OWN class  (expect: works)"
hit "/tenant/subfacet?session=s1"

say "T-E  host-brokered gadget: load + facet + SQLite, twice  (expect: count 1 then 2)"
hit "/tenant/gadget?session=s1&gadget=g1&gv=1"
hit "/tenant/gadget?session=s1&gadget=g1&gv=1"

say "T-F  gadget egress under globalOutbound:null  (expect: escaped=false)"
hit "/tenant/egress?session=s1&gadget=g1&gv=1"

say "T-G  per-gadget SQLite isolation  (expect: both count 1)"
hit "/tenant/isolation?session=s1"

say "T-H  code swap v1 -> v2 -> v1, storage preserved  (expect: n, n+10, n+11)"
hit "/tenant/gadget?session=sw&gadget=g1&gv=1"
hit "/tenant/swap?session=sw&gadget=g1&gv=2"
hit "/tenant/swap?session=sw&gadget=g1&gv=1"

say "T-I  tenant-chosen gadget name stays inside the host key prefix"
hit "/tenant/steal?session=s1&key=../../host-private"

say "T-J  session facet teardown does NOT cascade to gadget facets  (expect: count keeps rising)"
hit "/tenant/gadget?session=z&gadget=g1&gv=1"
hit "/host/drop-session?session=z"
hit "/tenant/gadget?session=z&gadget=g1&gv=1"
hit "/host/drop-session?session=z&hard=1"
hit "/tenant/gadget?session=z&gadget=g1&gv=1"

say "T-K  cold start vs warm"
for i in 1 2 3; do
  curl -s -o /dev/null -w "  cold #$i  %{time_total}s\n" -m 20 \
    "${BASE}/tenant/gadget?session=cold$i&gadget=new$i&gv=1"
done
for i in 1 2 3; do
  curl -s -o /dev/null -w "  warm #$i  %{time_total}s\n" -m 20 \
    "${BASE}/tenant/gadget?session=cold1&gadget=new1&gv=1"
done

if [ "${1:-}" = "--cpu" ]; then
  say "T-L  runaway gadget under limits.cpuMs=200  (expect: NOT killed, whole process starves)"
  curl -s -m 8 "${BASE}/tenant/cpu?session=s1&gadget=spin&cpu=200" >/dev/null 2>&1 &
  sleep 3
  echo -n "  unrelated session during runaway: "
  curl -s -m 6 "${BASE}/tenant/gadget?session=other&gadget=g&gv=1" || echo "TIMED OUT (starved)"
  ps -o pid,pcpu,comm -p "$(pgrep -x workerd | head -1)"
  echo "  SIGTERM:"; pkill -x workerd; sleep 2
  pgrep -x workerd >/dev/null && echo "  still alive -- SIGTERM ignored, needs SIGKILL"

  say "T-M  control: same runaway in ORDINARY tenant code, no gadget"
  start
  curl -s -m 6 "${BASE}/tenant/spin-self?session=spinner" >/dev/null 2>&1 &
  sleep 3
  echo -n "  unrelated session during tenant-side runaway: "
  curl -s -m 6 "${BASE}/tenant/gadget?session=b&gadget=g&gv=1" || echo "TIMED OUT (starved)"
fi

say "done -- see README.md for what each result means"

# Not part of the matrix above: persistence is verified by hand, because it
# needs a SIGKILL and a restart against a *preserved* state/ directory, while
# start() deliberately wipes it.
#
#   nohup ./node_modules/.bin/workerd serve workerd.capnp --experimental &
#   curl ".../tenant/gadget?session=persist&gadget=g1&gv=1"   # x3 -> count 3
#   pkill -9 -x workerd && nohup ./node_modules/.bin/workerd serve ... &
#   curl ".../tenant/gadget?session=persist&gadget=g1&gv=1"   # -> count 4, durable
#   sed -i 's/version: 1/version: 99/' gadget-v1.js           # edit, restart, hit again
#   #                                  -> version 99, count 6: new code, old storage
