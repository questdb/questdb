#!/usr/bin/env bash
# t07 — can the ORACLE fail?
#
# An oracle that always passes is indistinguishable from one that works, so every DURABLE and
# RPO_OK verdict is unproven until CrashVerifier is shown corrupt data and reports it.
#
# One device state, two verdicts: record a workload under dm-log-writes, reboot, replay to the
# last flush and mount, then verify (expect DURABLE or RPO_OK); overwrite 8 bytes of a
# committed column file in place and verify the same mount again (expect SILENT_CORRUPTION).
# Same boundary, same mount, same oracle, so the only variable is the bytes on disk -- and if
# the second verdict equals the first, the oracle is blind.
#
# Corrupting the device rather than the oracle's own formula is what makes the control
# meaningful: it needs no source change, so it cannot drift out of sync with the oracle it
# checks. Each value is a deterministic function of the row id, so one wrong long inside v.d is
# silent corruption by the oracle's own definition.
#
# Scope: this proves the oracle catches corrupt data. It does not prove it catches a missing
# durability barrier, where every byte that arrives is correct and there are simply fewer of
# them than were acknowledged. t10-wal-barrier-control.sh is that control.
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
replay_reset_assert_config || exit 64
trap cleanup EXIT
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM
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

# Anchor on the first commit. Exhausting this loop is a failure, not a timeout to shrug at:
# the control would then corrupt and verify a table with nothing committed in it, and pass
# for the wrong reason.
anchored=false
for _ in $(seq 1 120); do
    n=$(vm_ssh "$P" "$KEY" "head -1 /mnt/qdb/db/_progress 2>/dev/null | tr -dc '0-9'" 2>/dev/null || echo "")
    [ -n "$n" ] && [ "$n" -ge 1 ] 2>/dev/null && { anchored=true; break; }
    sleep 0.2
done
[ "$anchored" = true ] \
    || { keep; echo "FAIL t07: no commit reached in 24s; the control would be vacuous"; exit 1; }
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
# The replay boot, and the only one that gets discard=unmap: on the recording boot an unmapping
# discard would land in the log as a DISCARD entry and change what is reconstructed here.
QDB_VM_DATA_DISCARD=unmap vm_boot "$RUN" "$RUN/overlay.qcow2" "$RUN/data.raw" "$P2" "" "$RUN/log.raw"
vm_wait_ssh "$P2" "$KEY" 240 || { keep; echo "FAIL t07: guest never rebooted"; exit 1; }
vm_scp "$P2" "$KEY" "$HERE/../../../benchmarks/target/benchmarks.jar" /opt/vmcrash/benchmarks.jar
vm_scp_dir "$P2" "$KEY" "$HERE/guest" /opt/vmcrash/

# Reset the device even for a single replay to the last boundary. dm-log-writes passes writes
# through, so /dev/vdb still carries whatever the workload wrote after the final flush entry --
# exactly the bytes that boundary is supposed to exclude. The claim here rests on eight
# hand-written bytes being the only difference between V1 and V2, and a crash tail under both
# is another one.
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
