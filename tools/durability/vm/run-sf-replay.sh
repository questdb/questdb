#!/usr/bin/env bash
# run-sf-replay.sh [mode] [window_us]
#
# THE CLIENT IS ON A DIFFERENT MACHINE. That is the deployment QWP is used in, and it changes
# what a power cut means: the cut kills the SERVER, the client survives, and the client's
# store-and-forward buffer is what closes the server's RPO gap.
#
#   server : inside the VM, killed by SIGKILL to QEMU (a real power cut)
#   client : a HOST process, never killed, reconnecting on its own policy
#
# The guest's 9000 is forwarded to a host port, and the VM is rebooted on the REPLAYED disk with
# the SAME forward, so the client finds the server again at the address it already holds. No
# restart, no re-point: the client's own reconnect + cursor reposition does the work.
#
# WHAT THIS PROVES, and what it deliberately does not:
#   Under adaptive W>0 the server may legitimately discard txns above Wm. The claim under test is
#   that the CLIENT replays them, so nothing it accepted is lost end to end. That is proven
#   COMPARATIVELY -- the same crash boundary is verified twice, once WITHOUT letting the client
#   reconnect and once WITH -- because "all rows present" alone cannot distinguish "the client
#   replayed" from "the server never lost anything".
#
# sf_durability stays `memory` on purpose: the client did not crash, so its buffer never needed to
# survive a crash. (`flush`/`append` parse but are not implemented; `periodic` exists. Client-side
# disk durability matters only when the CLIENT crashes, which is a different scenario.)
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/qemu.sh
source "$HERE/lib/qemu.sh"
# shellcheck source=lib/verdict.sh
source "$HERE/lib/verdict.sh"

bash "$HERE/check-host.sh" >/dev/null || { bash "$HERE/check-host.sh"; exit 1; }

MODE="${1:-adaptive}"
WINDOW="${2:-50000}"
STATE_DIR="${QDB_VMCRASH_STATE:-/data/qdb-vmcrash}"
BASE="$STATE_DIR/base"
KEY="$BASE/id_ed25519"
RUN="$STATE_DIR/sfreplay-$MODE-w$WINDOW-$$"
JAR="$HERE/../../../benchmarks/target/benchmarks.jar"

# ONE LINE. Sent through ssh, embedded newlines make the shell execute each continuation as its
# own command ("--add-exports=...: No such file or directory") and the server never starts.
JVM="--enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.time.zone=ALL-UNNAMED --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"

CLIENT_PID=""
cleanup() {
    [ -n "$CLIENT_PID" ] && kill "$CLIENT_PID" 2>/dev/null
    vm_kill "$RUN" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

mkdir -p "$RUN"
qemu-img create -f qcow2 -b "$BASE/golden.qcow2" -F qcow2 "$RUN/overlay.qcow2" >/dev/null
truncate -s 40G "$RUN/data.raw"
truncate -s 60G "$RUN/log.raw"

# A HOST port, so it must be verified free. A server that loses its bind does not stop -- the next
# client talks to whatever already owns the port, which is how 20000 rows once landed in a live
# container's database.
QWP_PORT=$(vm_free_port)
if ss -ltn 2>/dev/null | grep -q ":$QWP_PORT "; then
    echo "LOUD_FAILURE: host port $QWP_PORT is already bound; refusing to forward onto it"
    exit 1
fi

echo "store-and-forward replay — client on the HOST, server in the VM"
echo "  mode=$MODE W=$WINDOW qwpPort=$QWP_PORT run=$RUN"

P=$(vm_free_port)
vm_boot "$RUN" "$RUN/overlay.qcow2" "$RUN/data.raw" "$P" "" "$RUN/log.raw" "$QWP_PORT"
vm_wait_ssh "$P" "$KEY" 240 || { echo "LOUD_FAILURE: guest never answered SSH"; exit 1; }
vm_scp "$P" "$KEY" "$JAR" /opt/vmcrash/benchmarks.jar
vm_scp_dir "$P" "$KEY" "$HERE/guest" /opt/vmcrash/
vm_ssh "$P" "$KEY" "sudo sync"
vm_ssh "$P" "$KEY" "bash /opt/vmcrash/guest/prepare-device.sh --mode=log-writes" >/dev/null \
    || { echo "LOUD_FAILURE: could not build the log-writes stack"; exit 1; }

start_server() {
    # prepare-device.sh leaves /mnt/qdb root-owned; the server runs as ubuntu and cannot create
    # its db/ or conf/ there. Silent without this -- the server exits and only the guest log says why.
    vm_ssh "$P" "$KEY" "sudo chown -R ubuntu /mnt/qdb" >/dev/null 2>&1 || true
    vm_ssh "$P" "$KEY" "setsid env QDB_CAIRO_COMMIT_MODE=$MODE \
        QDB_CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW=${WINDOW}us \
        java $JVM -cp /opt/vmcrash/benchmarks.jar io.questdb.ServerMain -d /mnt/qdb \
        </dev/null >/mnt/qdb/server.log 2>&1 & sleep 1; echo started" >/dev/null 2>&1 || true
    for _ in $(seq 1 120); do
        curl -s "http://127.0.0.1:$QWP_PORT/exec?query=select+1" >/dev/null 2>&1 && return 0
        sleep 1
    done
    return 1
}

start_server || {
    # CAPTURE THE REASON. A failure path that discards its own evidence costs a VM boot per guess.
    echo "LOUD_FAILURE: server never came up in the guest"
    vm_ssh "$P" "$KEY" "tail -25 /mnt/qdb/server.log 2>/dev/null; echo '--- mount ---'; mount | grep qdb; ls -ld /mnt/qdb" 2>&1 | sed 's/^/    /' | tail -20
    exit 1
}
# Assert the premise: a run that tests a different mode than it reports is worse than no run.
actual=$(curl -s -G "http://127.0.0.1:$QWP_PORT/exec" \
    --data-urlencode "query=select value from (show parameters) where property_path = 'cairo.commit.mode'" \
    2>/dev/null | grep -oE '\[\["[a-z]+"\]\]' | grep -oE '[a-z]+' | head -1)
echo "  server reports cairo.commit.mode=$actual (requested $MODE)"
[ "$actual" = "$MODE" ] || { echo "LOUD_FAILURE: server is in '$actual', run claims '$MODE'"; exit 1; }

# ---- the client: a HOST process that outlives the cut --------------------------------------
# reconnect_max_duration must exceed the whole outage (cut + replay + reboot + server start), or
# the client gives up before the server returns and the test measures the client's patience
# instead of its replay.
# The client writes its own watermark file; it is a HOST process, so this lives on the host --
# deliberately NOT on the crashed device, because the client's machine does not crash here.
CLIENT_DIR="$RUN/client-state"
mkdir -p "$CLIENT_DIR"
java $JVM -cp "$JAR" \
    -Dqwp.addr="127.0.0.1:$QWP_PORT" \
    -Dqwp.durable.ack=local \
    -Dqwp.reconnect.max.ms=600000 \
    -Dmax.rows=2000000000 \
    org.questdb.QwpCrashIngestClient "$CLIENT_DIR" > "$RUN/client.log" 2>&1 &
CLIENT_PID=$!
echo "  client pid=$CLIENT_PID (host process, survives the cut)"

for _ in $(seq 1 120); do
    grep -q "^qwp sent=" "$RUN/client.log" 2>/dev/null && break
    sleep 0.5
done
sleep 8
kill -0 "$CLIENT_PID" 2>/dev/null || { echo "LOUD_FAILURE: client died before the cut"; tail -5 "$RUN/client.log"; exit 1; }

SENT_BEFORE=$(grep -oE "^qwp sent=[0-9]+" "$RUN/client.log" | tail -1 | cut -d= -f2)
echo "  client had sent ${SENT_BEFORE:-0} rows when the power was cut"

echo "--- POWER CUT (server dies, client keeps running) ---"
vm_kill "$RUN"
kill -0 "$CLIENT_PID" 2>/dev/null && echo "  client survived the cut, as a remote client would" \
    || { echo "LOUD_FAILURE: client died with the server — it is not modelling a separate machine"; exit 1; }

echo "--- reboot on the REPLAYED disk, same forwarded port ---"
rm -f "$RUN/overlay.qcow2"
qemu-img create -f qcow2 -b "$BASE/golden.qcow2" -F qcow2 "$RUN/overlay.qcow2" >/dev/null
P2=$(vm_free_port)
vm_boot "$RUN" "$RUN/overlay.qcow2" "$RUN/data.raw" "$P2" "" "$RUN/log.raw" "$QWP_PORT"
vm_wait_ssh "$P2" "$KEY" 240 || { echo "LOUD_FAILURE: guest never rebooted"; exit 1; }
vm_scp "$P2" "$KEY" "$JAR" /opt/vmcrash/benchmarks.jar
vm_scp_dir "$P2" "$KEY" "$HERE/guest" /opt/vmcrash/
P="$P2"

nflush=$(vm_ssh "$P" "$KEY" "sudo python3 /opt/vmcrash/guest/replay-log.py --log /dev/vdc --list | head -1" \
    | grep -oE '[0-9]+ flushes' | grep -oE '^[0-9]+')
echo "  recorded $nflush flush boundaries; replaying to the last one"
vm_ssh "$P" "$KEY" "sudo umount /mnt/qdb 2>/dev/null; sudo dmsetup remove qdbdata 2>/dev/null; \
    sudo python3 /opt/vmcrash/guest/replay-log.py --log /dev/vdc --replay /dev/vdb --to-flush $nflush >/dev/null 2>&1; \
    sudo mkdir -p /mnt/qdb && sudo mount /dev/vdb /mnt/qdb && sudo chown -R ubuntu /mnt/qdb" >/dev/null

# ---- ARM A: what the SERVER alone kept, before the client is allowed to reconnect ------------
start_server || {
    echo "LOUD_FAILURE: server never came up after replay"
    vm_ssh "$P" "$KEY" "tail -20 /mnt/qdb/server.log 2>/dev/null" 2>&1 | sed 's/^/    /' | tail -12
    exit 1
}
# COUNT ONLY THE PRE-CUT ID RANGE. The client is long-running and keeps ingesting through the
# outage, so a raw count after reconnect mixes REPLAYED rows with NEWLY PRODUCED ones -- a first
# run reported "40,867,000 rows recovered" when only a fraction were replays and the rest were
# new data. Bounding both arms by id < SENT_BEFORE isolates what the replay actually restored.
count_precut() {
    curl -s -G "http://127.0.0.1:$QWP_PORT/exec" \
        --data-urlencode "query=select count() from t where id < $SENT_BEFORE" 2>/dev/null \
        | grep -oE '\[\[[0-9]+\]\]' | grep -oE '[0-9]+'
}
rows_server_only=$(count_precut)
echo "  ARM A (server alone, pre-cut ids only): rows=${rows_server_only:-?} of $SENT_BEFORE sent"

# ---- ARM B: let the client reconnect on its own and replay ------------------------------------
echo "--- waiting for the client to reconnect and replay (its own policy, no prompting) ---"
prev=-1
for _ in $(seq 1 90); do
    sleep 2
    now=$(count_precut)
    [ -z "$now" ] && continue
    [ "$now" = "$prev" ] && [ "$now" -gt "${rows_server_only:-0}" ] && break
    prev="$now"
done
rows_after_replay="$prev"
echo "  ARM B (after reconnect+replay, pre-cut ids only): rows=${rows_after_replay:-?} of $SENT_BEFORE sent"

# AT-LEAST-ONCE vs EXACTLY-ONCE. If the replay resends rows the server already committed and the
# table has no dedup keys, the pre-cut range ends up with MORE rows than were ever sent. Measure
# it rather than infer it: distinct ids vs total rows over the same range.
dup_total=$(curl -s -G "http://127.0.0.1:$QWP_PORT/exec" \
    --data-urlencode "query=select count() total, count_distinct(id) distinct_ids from t where id < $SENT_BEFORE" \
    2>/dev/null | grep -oE '\[\[[0-9]+,[0-9]+\]\]' | tr -d '[]')
echo "  duplicate check (pre-cut ids) total,distinct = ${dup_total:-unavailable}"

kill "$CLIENT_PID" 2>/dev/null; CLIENT_PID=""
recovered=$(( ${rows_after_replay:-0} - ${rows_server_only:-0} ))
echo
echo "  sent before cut : ${SENT_BEFORE:-?}"
echo "  server kept     : ${rows_server_only:-?}"
echo "  after replay    : ${rows_after_replay:-?}"
echo "  RECOVERED BY THE CLIENT: $recovered rows"
if [ "$recovered" -gt 0 ]; then
    echo "SF_REPLAY_PROVEN: of $SENT_BEFORE rows sent before the cut, the server kept"
    echo "  $rows_server_only and the client replayed $recovered more (pre-cut ids only, so"
    echo "  rows produced during/after the outage are excluded from this count)"
else
    echo "SF_REPLAY_NOT_DEMONSTRATED: the server lost nothing at this boundary, so the"
    echo "  replay had nothing to recover — this run does not prove the mechanism either way"
fi
vm_kill "$RUN"
