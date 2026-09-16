#!/usr/bin/env bash
# t07 — can the ORACLE fail?
#
# t04 proves the CUT can fail (arm it without drop_writes and the preflight must go red).
# Nothing proved the OTHER half: that CrashVerifier, shown corrupt data, reports it. Until
# that is shown, every DURABLE and RPO_OK this harness has ever printed is unproven — an
# oracle that always passes is indistinguishable from one that works.
#
# METHOD: one device state, TWO verdicts.
#
#   1. record a workload under dm-log-writes, reboot, replay to the last flush, mount
#   2. verify                       -> expect DURABLE / RPO_OK
#   3. overwrite 8 bytes of a COMMITTED column file, in place
#   4. verify the SAME mount again  -> expect SILENT_CORRUPTION
#
# Same boundary, same mount, same oracle, same JVM invocation. The only variable is the
# bytes on disk. If verdict 2 equals verdict 1, the oracle is blind.
#
# WHY CORRUPT THE DATA RATHER THAN THE ORACLE. Editing the expected-value formula tests
# that the comparison runs, which is trivially true. Corrupting the DEVICE tests the thing
# that matters: that a wrong byte written where QuestDB's data lives is noticed after a
# real recovery on a real filesystem. It also needs no source change, so it cannot drift
# out of sync with the oracle it is checking.
#
# WHY v.d. Every value is a deterministic function of the row id (v = id * 2654435761), so
# a single wrong long IS silent corruption by the oracle's own definition, and the verdict
# should name the exact row.
#
# SCOPE — read this before citing a green t07. This proves the oracle catches corrupt
# DATA. It does NOT prove it catches a missing DURABILITY BARRIER: a product that stops
# calling fdatasync fails in a completely different way — every byte that arrives is
# correct, there are simply fewer of them than were acknowledged.
#
# THAT CONTROL NOW EXISTS: t10-wal-barrier-control.sh, which takes the barrier away on the
# WAL path (WAL table + NOSYNC) and requires the sweep to go red. The set is now three:
# t04 the cut can fail, t07 the oracle can fail, t10 the barrier control. Cite all three or
# name which one you mean.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../lib/qemu.sh
source "$HERE/lib/qemu.sh"
# shellcheck source=../lib/verdict.sh
source "$HERE/lib/verdict.sh"

bash "$HERE/check-host.sh" >/dev/null || { bash "$HERE/check-host.sh"; exit 1; }

STATE_DIR="${QDB_VMCRASH_STATE:-/data/qdb-vmcrash}"
BASE="$STATE_DIR/base"
KEY="$BASE/id_ed25519"
RUN="$STATE_DIR/t07-$$"
MODE="${QDB_T07_MODE:-adaptive}"
WINDOW="${QDB_T07_WINDOW:-0}"
PROFILE=bitmap

[ -f "$BASE/golden.qcow2" ] || { echo "FAIL t07: no golden image; run build-image.sh first"; exit 1; }

mkdir -p "$RUN"
qemu-img create -f qcow2 -b "$BASE/golden.qcow2" -F qcow2 "$RUN/overlay.qcow2" >/dev/null
truncate -s 40G "$RUN/data.raw"
truncate -s 60G "$RUN/log.raw"

# Every exit path reaps the VM. Disks are kept only on failure, as everywhere else here.
cleanup() { vm_kill "$RUN" 2>/dev/null || true; }
trap cleanup EXIT INT TERM
keep() { echo "run state kept at $RUN" >&2; }

echo "t07 — oracle negative control (mode=$MODE W=$WINDOW)"

# ---- record a workload -------------------------------------------------------
P=$(vm_free_port)
vm_boot "$RUN" "$RUN/overlay.qcow2" "$RUN/data.raw" "$P" "" "$RUN/log.raw"
vm_wait_ssh "$P" "$KEY" 240 || { keep; echo "FAIL t07: guest never answered SSH"; exit 1; }
vm_scp "$P" "$KEY" "$HERE/../../../benchmarks/target/benchmarks.jar" /opt/vmcrash/benchmarks.jar \
    || { keep; echo "FAIL t07: could not ship benchmarks.jar"; exit 1; }
vm_scp_dir "$P" "$KEY" "$HERE/guest" /opt/vmcrash/
vm_ssh "$P" "$KEY" "sudo sync"
vm_ssh "$P" "$KEY" "bash /opt/vmcrash/guest/prepare-device.sh --mode=log-writes" >/dev/null \
    || { keep; echo "FAIL t07: could not build the log-writes stack"; exit 1; }

vm_ssh "$P" "$KEY" "setsid env QDB_SCHEMA_PROFILE=$PROFILE bash /opt/vmcrash/guest/run-workload.sh \
    --arm=reference --mode=$MODE --window-us=$WINDOW --epoch-ms=1000 </dev/null \
    >/mnt/qdb/workload.out 2>&1 &" || true

for _ in $(seq 1 120); do
    n=$(vm_ssh "$P" "$KEY" "head -1 /mnt/qdb/db/_progress 2>/dev/null | tr -dc '0-9'" 2>/dev/null || echo "")
    [ -n "$n" ] && [ "$n" -ge 1 ] 2>/dev/null && break
    sleep 0.2
done
sleep 8
# Same vacuity guard as everywhere else: a control run against a dead workload would
# verify an empty table and could not discriminate anything.
vm_ssh "$P" "$KEY" "pgrep -f '[C]rashIngestWriter' >/dev/null" \
    || { keep; echo "FAIL t07: workload was not running; the control would be vacuous"; exit 1; }
vm_ssh "$P" "$KEY" "sleep 2" || true     # let the log kthread drain its queue
vm_kill "$RUN"

# ---- reboot, replay to the last boundary, mount -------------------------------
rm -f "$RUN/overlay.qcow2"
qemu-img create -f qcow2 -b "$BASE/golden.qcow2" -F qcow2 "$RUN/overlay.qcow2" >/dev/null
P2=$(vm_free_port)
# The replay boot, and the only one that gets discard=unmap -- the recording boot above keeps
# the default, or an unmapping discard would land in the log as a DISCARD entry and change
# what is being reconstructed here.
QDB_VM_DATA_DISCARD=unmap vm_boot "$RUN" "$RUN/overlay.qcow2" "$RUN/data.raw" "$P2" "" "$RUN/log.raw"
vm_wait_ssh "$P2" "$KEY" 240 || { keep; echo "FAIL t07: guest never rebooted"; exit 1; }
vm_scp "$P2" "$KEY" "$HERE/../../../benchmarks/target/benchmarks.jar" /opt/vmcrash/benchmarks.jar
vm_scp_dir "$P2" "$KEY" "$HERE/guest" /opt/vmcrash/

# RESET THE DEVICE, even though this is a single replay to the LAST boundary.
#
# "Last boundary" is not "every byte". dm-log-writes passes writes through, so /dev/vdb still
# carries whatever the workload wrote AFTER the final flush entry -- and those bytes are
# precisely the ones the last boundary is supposed to exclude. Without the reset the clean
# state V1 is measured on a device that is part boundary and part crash tail.
#
# That matters more here than it looks. This control's whole claim rests on ONE variable
# changing between V1 and V2 -- eight bytes written by hand. Any other difference in the
# device under them weakens the claim, and a post-boundary tail is exactly such a difference.
replay_reset_assert "$P2" "$KEY" || { keep; echo "FAIL t07: the device reset is not real; the clean state cannot be trusted"; exit 1; }
vm_ssh "$P2" "$KEY" "sudo umount /mnt/qdb 2>/dev/null; sudo dmsetup remove qdbdata 2>/dev/null; \
    $(replay_reset_cmd); \
    sudo python3 /opt/vmcrash/guest/replay-log.py --log /dev/vdc --replay /dev/vdb --to-flush last 2>&1 | tail -1; \
    sudo mkdir -p /mnt/qdb && sudo mount /dev/vdb /mnt/qdb" \
    || { keep; echo "FAIL t07: could not replay and mount"; exit 1; }

verify_once() {
    vm_ssh "$P2" "$KEY" "bash /opt/vmcrash/guest/verify.sh --arm=reference --mode=$MODE \
        --window-us=$WINDOW --epoch-ms=1000 --profile=$PROFILE" 2>&1 | grep -vE '^DETAIL' | tail -1
}

# ---- VERDICT 1: honest state --------------------------------------------------
V1=$(verify_once)
echo "  clean     : $V1"

# ---- corrupt ONE committed value ----------------------------------------------
# Mid-file and 8-byte aligned, so it lands well inside the committed range rather than in
# an uncommitted tail, where its absence would prove nothing.
CORRUPT=$(vm_ssh "$P2" "$KEY" "
    f=\$(sudo find /mnt/qdb/db -path '*/t~*/*/v.d' | sort | head -1)
    [ -n \"\$f\" ] || { echo 'NO_COLUMN_FILE'; exit 0; }
    sz=\$(sudo stat -c %s \"\$f\")
    off=\$(( sz / 2 / 8 * 8 ))
    printf 'DEADBEEF' | sudo dd of=\"\$f\" bs=1 seek=\$off conv=notrunc 2>/dev/null
    sudo sync
    echo \"\$f @\$off (size \$sz)\"")
echo "  corrupted : $CORRUPT"
case "$CORRUPT" in
    NO_COLUMN_FILE) keep; echo "FAIL t07: no v.d column file found; nothing was corrupted, so the control is vacuous"; exit 1 ;;
esac

# ---- VERDICT 2: same state, one value wrong -----------------------------------
V2=$(verify_once)
echo "  corrupt   : $V2"
vm_kill "$RUN"

# ---- judge --------------------------------------------------------------------
C1=$(verdict_classify "$V1")
C2=$(verdict_classify "$V2")
if verdict_is_pass "$C1" && [ "$C2" = "SILENT_CORRUPTION" ]; then
    rm -rf "$RUN"
    echo "PASS t07 (oracle passes honest data and REPORTS corruption: $C1 -> $C2)"
    exit 0
fi
keep
if ! verdict_is_pass "$C1"; then
    echo "FAIL t07: the CLEAN state did not verify ($C1). The control cannot run; fix this first."
elif verdict_is_pass "$C2"; then
    echo "FAIL t07: corrupt data still verified as $C2 — THE ORACLE IS BLIND."
    echo "  No DURABLE or RPO_OK verdict from this harness means anything until this passes."
else
    echo "FAIL t07: corruption went red as $C2, not SILENT_CORRUPTION."
    echo "  The oracle discriminates, but does not name the fault correctly — check whether"
    echo "  the engine refused to open (LOUD_FAILURE) instead of bit-checking the row."
fi
exit 1
