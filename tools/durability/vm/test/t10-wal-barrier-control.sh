#!/usr/bin/env bash
# t10 — would the harness notice if the WAL durability barrier VANISHED?
#
# The third member of the set, and the one that was missing:
#
#   t04  the CUT can fail        (arm it without drop_writes; the preflight must go red)
#   t07  the ORACLE can fail     (corrupt a committed value; it must report SILENT_CORRUPTION)
#   t10  the BARRIER control     (take the barrier away; the sweep must go red)   <- this file
#
# t07 proves a wrong BYTE is caught. A missing fsync fails in a completely different way:
# every byte that arrives is correct, there are simply fewer of them than were acknowledged.
# Until that is shown, every DURABLE and RPO_OK this harness prints on the WAL path is green
# for an unknown reason — and the WAL path is the one adaptive commit ships on.
#
# METHOD: the same workload twice, one variable changed.
#
#   arm A   WAL table + commitMode=SYNC     -> barriered   -> every boundary must be GREEN
#   arm B   WAL table + commitMode=NOSYNC   -> no barrier  -> every boundary must be RED
#
# Same table kind, same schema, same oracle, same replay machinery, same boundary selection.
# The ONLY difference is whether the commit path issues a durability barrier. If arm B comes
# back green, the sweep cannot detect a missing WAL fdatasync and nothing it says about the
# WAL path is evidence.
#
# WHY NOSYNC RATHER THAN A MUTATED PRODUCT. The ticket originally asked for a system property
# gating a real fdatasync (questdb.durability.omitWalFdatasync). That is production code whose
# only purpose is to be broken, and it has to be kept out of release builds forever.
# CommitMode.NOSYNC is the SAME mutation — WalWriter.syncIfRequired0 gates the barrier on
# commitMode != NOSYNC — and it is a supported product configuration, so this control is also
# coverage. No source change, so it cannot drift out of sync with the product it checks.
#
# WHY THIS NEEDED A HARNESS CHANGE AT ALL. The table kind used to be implied by the commit
# mode (SYNC/NOSYNC -> bypass wal, adaptive -> WAL), so "WAL table, no barrier" was not
# expressible. -Dwal.table (QDB_WAL_TABLE) decouples them. The non-WAL half of this control
# was already measured and discriminates completely: SYNC 9/9 DURABLE with count == watermark
# exactly, NOSYNC 9/9 SILENT_CORRUPTION with count=0. See phase-09-barrier-control.md.
#
# ANTI-VACUITY. A NOSYNC run that silently took the BYPASS-WAL path would go red for the
# already-covered reason and look like a passing control. Every verification below is required
# to report wal.table=true, from the verifier's own stdout, before its verdict is counted.
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
RUN="$STATE_DIR/t10-$$"
PROFILE=bitmap
# Boundaries per arm. Three is enough to tell "structural" from "one unlucky point" while
# keeping the test to two VM cycles; the real sweep is what runs hundreds.
POINTS="${QDB_T10_POINTS:-3}"

[ -f "$BASE/golden.qcow2" ] || { echo "FAIL t10: no golden image; run build-image.sh first"; exit 1; }

mkdir -p "$RUN"

cleanup() { vm_kill "$RUN" 2>/dev/null || true; }
trap cleanup EXIT INT TERM
keep() { echo "run state kept at $RUN" >&2; }

echo "t10 — WAL barrier control (SYNC must survive, NOSYNC must not)"

# run_arm MODE -> prints one "<verdict-token> <full-line>" per boundary on stdout, via $ARM_OUT.
#
# Records a workload on a WAL table at MODE under dm-log-writes, reboots, then replays to
# several strided flush boundaries and verifies each. Identical for both arms except MODE:
# anything that differed between them would confound the one variable under test.
run_arm() {
    local mode="$1"
    local logged=0

    rm -f "$RUN/overlay.qcow2" "$RUN/data.raw" "$RUN/log.raw"
    qemu-img create -f qcow2 -b "$BASE/golden.qcow2" -F qcow2 "$RUN/overlay.qcow2" >/dev/null
    truncate -s 40G "$RUN/data.raw"
    truncate -s 60G "$RUN/log.raw"

    local P
    P=$(vm_free_port)
    vm_boot "$RUN" "$RUN/overlay.qcow2" "$RUN/data.raw" "$P" "" "$RUN/log.raw"
    vm_wait_ssh "$P" "$KEY" 240 || { echo "ARM_FAIL guest never answered SSH"; return 1; }
    vm_scp "$P" "$KEY" "$HERE/../../../benchmarks/target/benchmarks.jar" /opt/vmcrash/benchmarks.jar \
        || { echo "ARM_FAIL could not ship benchmarks.jar"; return 1; }
    vm_scp_dir "$P" "$KEY" "$HERE/guest" /opt/vmcrash/
    vm_ssh "$P" "$KEY" "sudo sync"
    vm_ssh "$P" "$KEY" "bash /opt/vmcrash/guest/prepare-device.sh --mode=log-writes" >/dev/null \
        || { echo "ARM_FAIL could not build the log-writes stack"; return 1; }

    # QDB_WAL_TABLE=true is the whole point: at mode=NOSYNC it is the only thing that puts the
    # workload on the WAL path, which the harness could not express before this ticket.
    vm_ssh "$P" "$KEY" "setsid env QDB_WAL_TABLE=true QDB_SCHEMA_PROFILE=$PROFILE \
        bash /opt/vmcrash/guest/run-workload.sh --arm=reference --mode=$mode \
        --window-us=0 --epoch-ms=1000 </dev/null >/mnt/qdb/workload.out 2>&1 &" || true

    local n
    for _ in $(seq 1 120); do
        n=$(vm_ssh "$P" "$KEY" "head -1 /mnt/qdb/db/_progress 2>/dev/null | tr -dc '0-9'" 2>/dev/null || echo "")
        [ -n "$n" ] && [ "$n" -ge 1 ] 2>/dev/null && break
        sleep 0.2
    done
    sleep 8
    # The same vacuity guard every flow here carries: a control verified against a workload
    # that had already died would grade an idle database and could discriminate nothing.
    vm_ssh "$P" "$KEY" "pgrep -f '[C]rashIngestWriter' >/dev/null" \
        || { vm_ssh "$P" "$KEY" "tail -20 /mnt/qdb/writer.log 2>/dev/null" >&2
             echo "ARM_FAIL workload was not running; this arm would be vacuous"; return 1; }
    # PROVE THE WORKLOAD ITSELF TOOK THE WAL PATH, from its own log, before the cut. If
    # -Dwal.table never reached CrashIngestWriter the run is a bypass-WAL run wearing a WAL
    # label, and at NOSYNC it would go red for the reason t07 and phase-09 already covered.
    vm_ssh "$P" "$KEY" "grep -q '^wal.table=true' /mnt/qdb/writer.log" \
        || { echo "ARM_FAIL writer.log does not report wal.table=true; the workload was NOT on the WAL path"; return 1; }
    vm_ssh "$P" "$KEY" "sleep 2" || true     # let the log kthread drain its queue
    vm_kill "$RUN"

    # ---- reboot and replay ----------------------------------------------------
    rm -f "$RUN/overlay.qcow2"
    qemu-img create -f qcow2 -b "$BASE/golden.qcow2" -F qcow2 "$RUN/overlay.qcow2" >/dev/null
    local P2
    P2=$(vm_free_port)
    vm_boot "$RUN" "$RUN/overlay.qcow2" "$RUN/data.raw" "$P2" "" "$RUN/log.raw"
    vm_wait_ssh "$P2" "$KEY" 240 || { echo "ARM_FAIL guest never rebooted"; return 1; }
    vm_scp "$P2" "$KEY" "$HERE/../../../benchmarks/target/benchmarks.jar" /opt/vmcrash/benchmarks.jar
    vm_scp_dir "$P2" "$KEY" "$HERE/guest" /opt/vmcrash/

    local nflush
    nflush=$(vm_ssh "$P2" "$KEY" "sudo python3 /opt/vmcrash/guest/replay-log.py --log /dev/vdc --list | head -1" \
        | grep -oE '[0-9]+ flushes' | grep -oE '^[0-9]+')
    [ "${nflush:-0}" -ge 2 ] || { echo "ARM_FAIL only ${nflush:-0} flushes recorded; nothing to replay"; return 1; }

    # Strided across the live range, skipping the first 10%: those boundaries predate the
    # table and verify as NO_COMMIT, which measures nothing in either arm.
    local floor=$(( nflush / 10 )); [ "$floor" -lt 1 ] && floor=1
    local span=$(( nflush - floor ))
    local stride=$(( span / POINTS )); [ "$stride" -lt 1 ] && stride=1

    local b out line
    for b in $(seq "$floor" "$stride" "$nflush" | head -n "$POINTS"); do
        out=$(vm_ssh "$P2" "$KEY" "sudo umount /mnt/qdb 2>/dev/null; sudo dmsetup remove qdbdata 2>/dev/null; \
            sudo python3 /opt/vmcrash/guest/replay-log.py --log /dev/vdc --replay /dev/vdb --to-flush $b 2>&1 | tail -1; \
            sudo mkdir -p /mnt/qdb; \
            if sudo mount /dev/vdb /mnt/qdb 2>/dev/null; then \
                env QDB_WAL_TABLE=true bash /opt/vmcrash/guest/verify.sh --arm=reference --mode=$mode \
                    --window-us=0 --epoch-ms=1000 --profile=$PROFILE; \
            else echo 'MOUNT_FAILED'; fi" 2>&1)
        # The verifier ECHOES the table kind it graded under. Checked per boundary, not once:
        # a run whose verifier fell back to the non-WAL oracle would read a bare row count and
        # never run the recovery triple, and its verdict says nothing about the WAL path.
        if ! printf '%s\n' "$out" | grep -q 'wal.table=true'; then
            echo "ARM_FAIL boundary $b was not verified as a WAL table (verifier did not report wal.table=true)"
            return 1
        fi
        line=$(printf '%s\n' "$out" | grep -vE '^DETAIL' | grep -vE '^[[:space:]]*$' | tail -1)
        # The acknowledged watermark lives in the verifier's DETAIL output, not in the verdict
        # line, so it is captured here while the full output is still in hand. The SYNC arm's
        # exactness check below has nothing to compare against without it.
        local wm
        wm=$(printf '%s\n' "$out" | grep -oE 'watermark rows=[0-9]+' | head -1 | cut -d= -f2)
        printf '%s\t%s\t%s\t%s\n' "$b" "$(verdict_classify "$line")" "${wm:--1}" "$line" >> "$ARM_OUT"
        logged=$((logged + 1))
    done
    vm_kill "$RUN"
    [ "$logged" -ge 1 ] || { echo "ARM_FAIL no boundary produced a verdict"; return 1; }
    return 0
}

# ---- arm A: the barrier is present -------------------------------------------
ARM_OUT="$RUN/sync.tsv"; : > "$ARM_OUT"
echo "  arm A: WAL table, commitMode=SYNC (barriered) ..."
if ! run_arm SYNC; then
    keep
    echo "FAIL t10: the SYNC arm could not run (reason above)."
    echo "  The control cannot be judged without its positive side; fix this first."
    exit 1
fi
sync_pass=0; sync_fail=0
while IFS=$'\t' read -r b v wm line; do
    printf '    flush %-6s -> %s\n' "$b" "$v"
    if verdict_is_pass "$v"; then sync_pass=$((sync_pass + 1)); else sync_fail=$((sync_fail + 1)); fi
    # EXACTNESS, not merely green. "It passed" can be luck; a recovered count short of the
    # acknowledged watermark cannot be, and it is the same discrimination the non-WAL half of
    # this control showed (count == watermark exactly at 9/9 boundaries).
    #
    # The bar is count >= watermark rather than equality, and that is deliberate: on the WAL
    # path the recovered count may legally EXCEED the watermark, because _progress is written
    # after the commit it describes and a further commit can land before the cut. Demanding
    # equality would fail this arm on correct behaviour. Short of the watermark is always loss.
    cnt=$(printf '%s' "$line" | grep -oE 'count=[0-9]+' | head -1 | cut -d= -f2)
    if [ -n "${cnt:-}" ] && [ "${wm:--1}" -ge 0 ] 2>/dev/null && [ "$cnt" -lt "$wm" ] 2>/dev/null; then
        echo "      count=$cnt < watermark=$wm — acknowledged rows missing on the BARRIERED arm"
        sync_fail=$((sync_fail + 1))
    fi
done < "$ARM_OUT"

# ---- arm B: the barrier is gone ----------------------------------------------
ARM_OUT="$RUN/nosync.tsv"; : > "$ARM_OUT"
echo "  arm B: WAL table, commitMode=NOSYNC (no barrier) ..."
if ! run_arm NOSYNC; then
    keep
    echo "FAIL t10: the NOSYNC arm could not run (reason above)."
    exit 1
fi
nosync_red=0; nosync_green=0
while IFS=$'\t' read -r b v wm line; do
    printf '    flush %-6s -> %s\n' "$b" "$v"
    if verdict_is_pass "$v"; then nosync_green=$((nosync_green + 1)); else nosync_red=$((nosync_red + 1)); fi
done < "$ARM_OUT"

# ---- judge --------------------------------------------------------------------
echo
echo "  SYNC   : $sync_pass green, $sync_fail red   (expected: all green)"
echo "  NOSYNC : $nosync_red red, $nosync_green green (expected: all red)"

if [ "$sync_fail" -eq 0 ] && [ "$sync_pass" -ge 1 ] && [ "$nosync_green" -eq 0 ] && [ "$nosync_red" -ge 1 ]; then
    vm_kill "$RUN"
    rm -rf "$RUN"
    echo "PASS t10 (the harness detects a missing WAL durability barrier: SYNC all green, NOSYNC all red)"
    exit 0
fi

keep
if [ "$sync_fail" -gt 0 ]; then
    echo "FAIL t10: the BARRIERED arm lost data ($sync_fail of $((sync_pass + sync_fail)) boundaries red)."
    echo "  That is a durability finding about the WAL path itself, not a broken control."
    echo "  Investigate it before reading anything into the NOSYNC arm."
elif [ "$nosync_green" -gt 0 ] && [ "$nosync_red" -eq 0 ]; then
    echo "FAIL t10: NOSYNC on a WAL table lost NOTHING — THE BARRIER CONTROL DOES NOT DISCRIMINATE."
    echo "  A workload that issues no durability barrier survived a flush-boundary replay intact,"
    echo "  so this harness cannot tell a durable WAL commit path from one that never flushes."
    echo "  No DURABLE or RPO_OK verdict on the WAL path is evidence until this is explained."
    echo "  First thing to check: whether some OTHER flusher is carrying the data to the device"
    echo "  (the harness's own _progress fsync manufactures ~10.5% of boundaries — see issues/08)."
else
    echo "FAIL t10: the NOSYNC arm was only PARTIALLY red ($nosync_red red, $nosync_green green)."
    echo "  Do NOT relax the bar to accommodate this. A missing barrier should be uniformly"
    echo "  detectable, and a boundary-dependent result means something else is flushing the"
    echo "  data some of the time. Explain it with a measurement, then re-run."
fi
exit 1
