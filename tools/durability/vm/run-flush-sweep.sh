#!/usr/bin/env bash
# run-flush-sweep.sh [mode] [window_us] [max_points]
#
# THE ENUMERATED CRASH SWEEP, on a real kernel and a real filesystem.
#
# One workload run under dm-log-writes records every write and every flush. Each
# flush is then a crash point: replay the log to it and you hold exactly the
# device state a volatile write cache would have left behind. Run the oracle at
# each and you have enumerated real crash points -- not sampled ones.
#
# This is what run-fuzz.sh cannot do. That samples ONE wall-clock moment per VM
# boot (~1 min each) and, because dm-flakey only discards writes issued after
# arming, it cannot lose a write that reached the device unflushed -- proven by
# probe. Here the boundary is the FLUSH, which is the boundary real hardware
# uses.
#
#   Java forEachAdaptiveCrashPoint : every durability op, modelled filesystem
#   this                           : every flush,        real filesystem
#   run-fuzz.sh                    : random wall-clock moments, real filesystem
#
# Replay is deliberately NOT a fresh VM per point: one boot serves every
# boundary, so N crash points cost one workload run plus N replay+verify passes.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/qemu.sh
source "$HERE/lib/qemu.sh"
# shellcheck source=lib/verdict.sh
source "$HERE/lib/verdict.sh"

bash "$HERE/check-host.sh" >/dev/null || { bash "$HERE/check-host.sh"; exit 1; }

MODE="${1:-adaptive}"
WINDOW="${2:-0}"
MAX_POINTS="${3:-12}"
PROFILE="${QDB_SCHEMA_PROFILE:-bitmap}"
# EPOCH=-1 DISABLES the periodic durable epoch, so the table runs with a
# SUSTAINED LAZY GAP: columns applied lazily with no epoch cut behind them, and
# recovery must rebuild the whole tail from the durable WAL. That is the shape
# AdaptiveO3LazyGapCrashSweepTest and W2/W3/W5 are built around, and it is the
# path adaptive recovery exists for -- a run with epochs every second never
# builds a gap worth recovering from.
EPOCH="${QDB_EPOCH_MS:-1000}"

STATE_DIR="${QDB_VMCRASH_STATE:-/data/qdb-vmcrash}"
BASE="$STATE_DIR/base"
KEY="$BASE/id_ed25519"
LOG="$STATE_DIR/flush-sweep.log"
OUTDIR="$STATE_DIR/sweep-out/$MODE-w$WINDOW-$PROFILE-e${QDB_EPOCH_MS:-1000}-$$"
RUN="$STATE_DIR/sweep-$MODE-w$WINDOW-$PROFILE-e$EPOCH-$$"
STAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)

mkdir -p "$RUN"
qemu-img create -f qcow2 -b "$BASE/golden.qcow2" -F qcow2 "$RUN/overlay.qcow2" >/dev/null
truncate -s 40G "$RUN/data.raw"
truncate -s 60G "$RUN/log.raw"

echo "flush-boundary crash sweep — $STAMP"
echo "  mode=$MODE W=$WINDOW profile=$PROFILE epoch=${EPOCH}ms sibling=${QDB_SIBLING_TABLE:-false} recoverAs=${QDB_RECOVER_AS:-same} ddlEvery=${QDB_DDL_EVERY_ROWS:--1} matView=${QDB_MAT_VIEW:-false} rebaseAt=${QDB_REBASE_AT_ROWS:--1}"

keep() { echo "run state kept at $RUN" >&2; }

# Same rule as power-cut-vm.sh: the disks are kept on failure, the VM is not.
# Nine orphaned qemu processes accumulated in one session before this existed.
cleanup_vm() { vm_kill "$RUN" 2>/dev/null || true; }
trap cleanup_vm EXIT INT TERM

# ---- one workload run, fully recorded --------------------------------------
P=$(vm_free_port)
vm_boot "$RUN" "$RUN/overlay.qcow2" "$RUN/data.raw" "$P" "" "$RUN/log.raw"
vm_wait_ssh "$P" "$KEY" 240 || { keep; echo "LOUD_FAILURE: guest never answered SSH"; exit 1; }
vm_scp "$P" "$KEY" "$HERE/../../../benchmarks/target/benchmarks.jar" /opt/vmcrash/benchmarks.jar
vm_scp_dir "$P" "$KEY" "$HERE/guest" /opt/vmcrash/
vm_ssh "$P" "$KEY" "sudo sync"
vm_ssh "$P" "$KEY" "bash /opt/vmcrash/guest/prepare-device.sh --mode=log-writes" >/dev/null \
    || { keep; echo "LOUD_FAILURE: could not build the log-writes stack"; exit 1; }

vm_ssh "$P" "$KEY" "setsid env QDB_SCHEMA_PROFILE=$PROFILE QDB_SIBLING_TABLE=${QDB_SIBLING_TABLE:-false} QDB_DDL_EVERY_ROWS=${QDB_DDL_EVERY_ROWS:--1} QDB_MAT_VIEW=${QDB_MAT_VIEW:-false} QDB_REBASE_AT_ROWS=${QDB_REBASE_AT_ROWS:--1} bash /opt/vmcrash/guest/run-workload.sh --arm=reference --mode=$MODE \
    --window-us=$WINDOW --epoch-ms=$EPOCH </dev/null >/mnt/qdb/workload.out 2>&1 &" || true

# Let it build a real history: many commits means many flushes means many
# crash points. Anchor on the first commit so startup is not counted.
for _ in $(seq 1 120); do
    n=$(vm_ssh "$P" "$KEY" "head -1 /mnt/qdb/db/_progress 2>/dev/null | tr -dc '0-9'" 2>/dev/null || echo "")
    [ -n "$n" ] && [ "$n" -ge 1 ] 2>/dev/null && break
    sleep 0.2
done
sleep 8
vm_ssh "$P" "$KEY" "pgrep -f '[C]rashIngestWriter' >/dev/null" \
    || { keep; echo "LOUD_FAILURE: workload was not running when the recording stopped"; exit 1; }
vm_ssh "$P" "$KEY" "sleep 2" || true     # let the log kthread drain its queue
vm_kill "$RUN"

# ---- reboot once; replay many boundaries from the one recording -------------
rm -f "$RUN/overlay.qcow2"
qemu-img create -f qcow2 -b "$BASE/golden.qcow2" -F qcow2 "$RUN/overlay.qcow2" >/dev/null
P2=$(vm_free_port)
vm_boot "$RUN" "$RUN/overlay.qcow2" "$RUN/data.raw" "$P2" "" "$RUN/log.raw"
vm_wait_ssh "$P2" "$KEY" 240 || { keep; echo "LOUD_FAILURE: guest never rebooted"; exit 1; }
vm_scp "$P2" "$KEY" "$HERE/../../../benchmarks/target/benchmarks.jar" /opt/vmcrash/benchmarks.jar
vm_scp_dir "$P2" "$KEY" "$HERE/guest" /opt/vmcrash/

nflush=$(vm_ssh "$P2" "$KEY" "sudo python3 /opt/vmcrash/guest/replay-log.py --log /dev/vdc --list | head -1" \
    | grep -oE '[0-9]+ flushes' | grep -oE '^[0-9]+')
echo "  recorded $nflush flush boundaries"
[ "${nflush:-0}" -ge 2 ] || { keep; echo "LOUD_FAILURE: only ${nflush:-0} flushes recorded; nothing to sweep"; exit 1; }

# Sweep the LAST max_points boundaries: the early ones are mkfs and startup,
# the interesting states are where the workload was live.
first=$(( nflush - MAX_POINTS + 1 ))
[ "$first" -lt 1 ] && first=1

fails=0; checked=0
for n in $(seq "$first" "$nflush"); do
    out=$(vm_ssh "$P2" "$KEY" "sudo umount /mnt/qdb 2>/dev/null; sudo dmsetup remove qdbdata 2>/dev/null; \
        sudo python3 /opt/vmcrash/guest/replay-log.py --log /dev/vdc --replay /dev/vdb --to-flush $n 2>&1 | tail -1; \
        sudo mkdir -p /mnt/qdb; \
        if sudo mount /dev/vdb /mnt/qdb 2>/dev/null; then \
            bash /opt/vmcrash/guest/verify.sh --arm=reference --mode=$MODE --window-us=$WINDOW --epoch-ms=$EPOCH --sibling=${QDB_SIBLING_TABLE:-false} --recover-as=${QDB_RECOVER_AS:-} --mat-view=${QDB_MAT_VIEW:-false} --rebase=$([ "${QDB_REBASE_AT_ROWS:--1}" -gt 0 ] && echo true || echo false); \
        else echo 'MOUNT_FAILED'; fi")
    # Archive the FULL per-boundary output. The one-line verdict in $LOG is a summary,
    # not evidence: every time a result needed explaining, the explanation was in the
    # lines this used to throw away. Kept outside $RUN so it survives the success-path
    # `rm -rf "$RUN"`.
    mkdir -p "$OUTDIR"
    printf '%s\n' "$out" > "$OUTDIR/flush-$n.out"
    line=$(echo "$out" | grep -vE '^DETAIL' | tail -1)
    v=$(verdict_classify "$line")
    checked=$((checked + 1))
    echo "$STAMP sweep profile=$PROFILE epoch=$EPOCH mode=$MODE W=$WINDOW flush=$n/$nflush verdict=$v line=$line" >> "$LOG"
    printf '  flush %4d/%-4d -> %s\n' "$n" "$nflush" "$v"
    case "$v" in
        DURABLE|RPO_OK|NO_COMMIT) ;;
        *) if [ "$line" = "MOUNT_FAILED" ]; then
               # A filesystem that will not mount at a crash point is a real
               # outcome, not a harness error -- report it and keep going.
               echo "      (filesystem unmountable at this boundary)"
           fi
           fails=$((fails + 1)) ;;
    esac
done

vm_kill "$RUN"
if [ "$fails" -eq 0 ] && [ "${QDB_KEEP_RUN:-0}" != "1" ]; then
    rm -rf "$RUN"
    echo "sweep complete: $checked boundaries, 0 failures; log at $LOG"
    echo "  full per-boundary output: $OUTDIR"
elif [ "$fails" -eq 0 ]; then
    keep
    echo "sweep complete: $checked boundaries, 0 failures; log at $LOG"
else
    keep
    echo "sweep complete: $checked boundaries, $fails FAILURES; log at $LOG"
    echo "  full per-boundary output: $OUTDIR"
fi
[ "$fails" -eq 0 ]
