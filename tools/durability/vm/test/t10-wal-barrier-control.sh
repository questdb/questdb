#!/usr/bin/env bash
# t10 — would the harness notice if the WAL durability barrier vanished?
#
# t07 shows the oracle catches a wrong byte. A missing fsync fails differently: every byte that
# arrives is correct, there are simply fewer of them than were acknowledged. Without this
# control, a green verdict on the WAL path is green for an unknown reason.
#
# Method: the same workload twice, one variable changed.
#
#   arm A   WAL table + commitMode=SYNC     -> barriered   -> every boundary must be green
#   arm B   WAL table + commitMode=NOSYNC   -> no barrier  -> every boundary must be red
#
# Same table kind, schema, oracle, replay machinery and boundary selection; only the barrier
# differs. A green arm B means the sweep cannot detect a missing WAL fdatasync.
#
# NOSYNC stands in for a mutated product: WalWriter.syncIfRequired0 gates the barrier on
# commitMode != NOSYNC, so NOSYNC is the same mutation expressed as a supported configuration.
# No production code exists only to be broken, and nothing can drift out of sync with it.
#
# A NOSYNC run that silently took the bypass-WAL path would go red for an already-covered
# reason and look like a passing control. Every verification below must report wal.table=true,
# from the verifier's own stdout, before its verdict counts.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../lib/qemu.sh
source "$HERE/lib/qemu.sh"
# shellcheck source=../lib/verdict.sh
source "$HERE/lib/verdict.sh"
# shellcheck source=../lib/arms.sh
source "$HERE/lib/arms.sh"

# THE CONTROL IS JUDGED ON VERDICTS, NOT ON POLARITY. Counting "pass" and "not pass" lets the
# wrong evidence satisfy both arms: NO_COMMIT is a pass, so an arm that never committed reads as
# "SYNC survived", and every instrument fault is a non-pass, so a broken rig reads as "NOSYNC lost
# data". Either one certifies a control that observed nothing. Arm A must produce real durable
# boundaries and arm B must produce real product-side loss; a rig fault on either side is a t10
# failure, not evidence.
judge_sync() {   # TSV -> sets sync_durable sync_nocommit sync_fail sync_rig
    sync_durable=0; sync_nocommit=0; sync_fail=0; sync_rig=0
    local b v wm cnt line
    while IFS=$'\t' read -r b v wm cnt line; do
        printf '    flush %-6s -> %s\n' "$b" "$v"
        case "$v" in
            DURABLE|RPO_OK) sync_durable=$((sync_durable + 1)) ;;
            NO_COMMIT)      sync_nocommit=$((sync_nocommit + 1)) ;;
            *) if verdict_is_instrument_fault "$v"; then sync_rig=$((sync_rig + 1))
               else sync_fail=$((sync_fail + 1)); fi ;;
        esac
    done < "$1"
}

judge_nosync() { # TSV -> sets nosync_loss nosync_green nosync_nocommit nosync_rig nosync_other
    nosync_loss=0; nosync_green=0; nosync_nocommit=0; nosync_rig=0; nosync_other=0
    local b v wm cnt line
    while IFS=$'\t' read -r b v wm cnt line; do
        printf '    flush %-6s -> %s\n' "$b" "$v"
        case "$v" in
            # The shape a missing barrier makes: acknowledged rows are simply not there. Corruption
            # counts too -- a torn commit boundary is the same absent flush seen from the side.
            DURABILITY_FAILURE|SILENT_CORRUPTION) nosync_loss=$((nosync_loss + 1)) ;;
            DURABLE|RPO_OK)                       nosync_green=$((nosync_green + 1)) ;;
            NO_COMMIT)                            nosync_nocommit=$((nosync_nocommit + 1)) ;;
            *) if verdict_is_instrument_fault "$v"; then nosync_rig=$((nosync_rig + 1))
               else nosync_other=$((nosync_other + 1)); fi ;;
        esac
    done < "$1"
}

# 0 when the control discriminated: arm A shows real durability, arm B shows real loss, and
# neither arm was decided by a boundary that measured nothing or by a broken rig.
judge_verdict() {
    [ "$sync_durable" -ge 1 ] && [ "$sync_fail" -eq 0 ] && [ "$sync_rig" -eq 0 ] \
        && [ "$nosync_loss" -ge 1 ] && [ "$nosync_green" -eq 0 ] \
        && [ "$nosync_rig" -eq 0 ] && [ "$nosync_other" -eq 0 ]
}

# --self-test drives the judge with synthetic per-boundary verdicts and no VM, because the judge
# is the part that decides what this control certifies and a VM cycle is too expensive to use for
# checking it. Every case below is a run this test must refuse.
if [ "${1:-}" = "--self-test" ]; then
    rc=0
    tsv() { local f="$1"; shift; : > "$f"; local i=0
            for v in "$@"; do i=$((i + 1)); printf '%d\t%s\t-1\t0\t%s x\n' "$i" "$v" "$v" >> "$f"; done; }
    A="$(mktemp)"; B="$(mktemp)"; trap 'rm -f "$A" "$B"' EXIT
    case_is() {  # NAME EXPECTED(pass|fail)
        local name="$1" want="$2" got=fail
        judge_sync "$A" >/dev/null; judge_nosync "$B" >/dev/null
        judge_verdict && got=pass
        if [ "$got" = "$want" ]; then printf '  ok   %s -> %s\n' "$name" "$got"
        else printf '  FAIL %s -> %s (expected %s)\n' "$name" "$got" "$want"; rc=1; fi
    }

    tsv "$A" DURABLE DURABLE DURABLE;   tsv "$B" DURABILITY_FAILURE DURABILITY_FAILURE DURABILITY_FAILURE
    case_is "real discrimination"                         pass
    # The run that used to pass on polarity alone: NO_COMMIT is a pass token and every instrument
    # fault is a non-pass, so both arms were satisfied by boundaries that measured nothing.
    tsv "$A" NO_COMMIT NO_COMMIT NO_COMMIT; tsv "$B" NOT_EVALUATED UNPARSEABLE LOUD_FAILURE
    case_is "uninformative SYNC + rig-fault NOSYNC"       fail
    tsv "$A" NO_COMMIT NO_COMMIT NO_COMMIT; tsv "$B" DURABILITY_FAILURE DURABILITY_FAILURE
    case_is "SYNC never committed"                        fail
    tsv "$A" DURABLE DURABLE;               tsv "$B" NOT_EVALUATED NOT_EVALUATED
    case_is "NOSYNC decided by the rig"                   fail
    tsv "$A" DURABLE DURABLE;               tsv "$B" NO_COMMIT NO_COMMIT
    case_is "NOSYNC measured nothing"                     fail
    tsv "$A" DURABLE DURABLE;               tsv "$B" DURABLE DURABILITY_FAILURE
    case_is "NOSYNC partially durable"                    fail
    tsv "$A" DURABLE DURABILITY_FAILURE;    tsv "$B" DURABILITY_FAILURE
    case_is "SYNC lost data"                              fail
    tsv "$A" DURABLE UNPARSEABLE;           tsv "$B" DURABILITY_FAILURE
    case_is "SYNC hit a rig fault"                        fail
    tsv "$A" DURABLE NO_COMMIT DURABLE;     tsv "$B" DURABILITY_FAILURE NO_COMMIT
    case_is "a few uninformative boundaries are tolerated" pass
    tsv "$A" DURABLE;                       tsv "$B" SILENT_CORRUPTION
    case_is "torn commit counts as loss"                  pass

    [ "$rc" -eq 0 ] && echo "PASS t10 --self-test (the judge)" || echo "FAIL t10 --self-test"
    exit "$rc"
fi

bash "$HERE/check-host.sh" >/dev/null || { bash "$HERE/check-host.sh"; exit 1; }

STATE_DIR="${QDB_VMCRASH_STATE:-/data/qdb-vmcrash}"
BASE="$STATE_DIR/base"
KEY="$BASE/id_ed25519"
RUN="$STATE_DIR/t10-$$"
PROFILE=bitmap
# Boundaries per arm. Three tells "structural" from "one unlucky point" while keeping the test
# to two VM cycles; the sweep is what runs hundreds.
POINTS="${QDB_T10_POINTS:-3}"

# The control states its configuration and lets lib/arms.sh build both commands, so it grades
# under the same flags as the sweep it certifies.
#
# Exported rather than passed, because both builders read the environment. Pinned rather than
# inherited, so an ambient QDB_MAT_VIEW in someone's shell cannot change what is graded. Every
# value below is the builders' own default except QDB_WAL_TABLE, the one variable this control
# exists to set: harness_wal_table() otherwise derives the table kind from the commit mode
# (adaptive -> true, everything else -> false), which at SYNC and NOSYNC would silently grade
# the bypass-WAL path instead.
export QDB_WAL_TABLE=true
export QDB_SCHEMA_PROFILE="$PROFILE"
export QDB_SIBLING_TABLE=false
export QDB_DDL_EVERY_ROWS=-1
export QDB_MAT_VIEW=false
export QDB_REBASE_AT_ROWS=-1
export QDB_RECOVER_AS=
# The harness's own _progress fsync contributes flush boundaries of its own. Pinned so that a
# foreign flusher contributes the same share in both arms.
export QDB_WITNESS_FSYNC=true
# The reference arm has no ack channel; pinned so an ambient value cannot reach arm_qwp_tier and
# put a durable-ack bar on a run that has no client to satisfy it.
export QDB_QWP_DURABLE_ACK=off
export QDB_QWP_DEFANG_ACK=0
export QDB_EDITION=oss

# Assert the premise before booting anything, because the failure it guards against is silent:
# if harness_wal_table stops honouring an explicit QDB_WAL_TABLE, every verification runs the
# non-WAL oracle and t10 passes while measuring nothing about the WAL path. Both modes are
# checked, since the point is that they must not diverge here.
for _mode in SYNC NOSYNC; do
    case " $(harness_workload_env reference "$_mode") " in
        *" QDB_WAL_TABLE=true "*) ;;
        *) echo "FAIL t10: harness_workload_env does not put the workload on the WAL path at mode=$_mode"
           echo "  got: $(harness_workload_env reference "$_mode")"
           exit 1 ;;
    esac
    case " $(harness_verify_cmd reference "$_mode" 0 1000) " in
        *" QDB_WAL_TABLE=true "*) ;;
        *) echo "FAIL t10: harness_verify_cmd does not grade as a WAL table at mode=$_mode"
           echo "  got: $(harness_verify_cmd reference "$_mode" 0 1000)"
           exit 1 ;;
    esac
done
unset _mode

for _mode in SYNC NOSYNC; do
    harness_assert_config "$_mode" || exit 64
done
unset _mode
replay_reset_assert_config || exit 64

[ -f "$BASE/golden.qcow2" ] || { echo "FAIL t10: no golden image; run build-image.sh first"; exit 1; }

mkdir -p "$RUN"

cleanup() { vm_kill "$RUN" 2>/dev/null || true; }
trap cleanup EXIT
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM
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

    # ssh carries none of the caller's environment, so a variable run-workload.sh reads must be
    # named by harness_workload_env to arrive at all.
    vm_ssh "$P" "$KEY" "setsid env $(harness_workload_env reference "$mode") \
        bash /opt/vmcrash/guest/run-workload.sh --arm=reference --mode=$mode \
        --window-us=0 --epoch-ms=1000 </dev/null >/mnt/qdb/workload.out 2>&1 &" || true

    # Exhausting this loop is a failure: an arm whose workload never committed verifies an empty
    # table at every boundary, and NO_COMMIT on both sides would judge as "SYNC survived, NOSYNC
    # lost nothing" from a run that observed neither.
    local n anchored=false
    for _ in $(seq 1 120); do
        n=$(vm_ssh "$P" "$KEY" "head -1 /mnt/qdb/db/_progress 2>/dev/null | tr -dc '0-9'" 2>/dev/null || echo "")
        [ -n "$n" ] && [ "$n" -ge 1 ] 2>/dev/null && { anchored=true; break; }
        sleep 0.2
    done
    [ "$anchored" = true ] || { echo "ARM_FAIL no commit reached in 24s; this arm would be vacuous"; return 1; }
    sleep 8
    # A control verified against a workload that has already died grades an idle database and
    # discriminates nothing.
    vm_ssh "$P" "$KEY" "pgrep -f '[C]rashIngestWriter' >/dev/null" \
        || { vm_ssh "$P" "$KEY" "tail -20 /mnt/qdb/writer.log 2>/dev/null" >&2
             echo "ARM_FAIL workload was not running; this arm would be vacuous"; return 1; }
    # Prove the workload itself took the WAL path, from its own log, before the cut. If
    # -Dwal.table never reached CrashIngestWriter this is a bypass-WAL run wearing a WAL label.
    vm_ssh "$P" "$KEY" "grep -q '^wal.table=true' /mnt/qdb/writer.log" \
        || { echo "ARM_FAIL writer.log does not report wal.table=true; the workload was NOT on the WAL path"; return 1; }
    vm_ssh "$P" "$KEY" "sleep 2" || true     # let the log kthread drain its queue
    vm_kill "$RUN"

    # ---- reboot and replay ----------------------------------------------------
    rm -f "$RUN/overlay.qcow2"
    qemu-img create -f qcow2 -b "$BASE/golden.qcow2" -F qcow2 "$RUN/overlay.qcow2" >/dev/null
    local P2
    P2=$(vm_free_port)
    # discard=unmap on the replay boot only. The recording boot must not have it, or a discard
    # issued by the workload becomes a DISCARD entry in the dm-log-writes log and changes what
    # was recorded.
    QDB_VM_DATA_DISCARD=unmap vm_boot "$RUN" "$RUN/overlay.qcow2" "$RUN/data.raw" "$P2" "" "$RUN/log.raw"
    vm_wait_ssh "$P2" "$KEY" 240 || { echo "ARM_FAIL guest never rebooted"; return 1; }
    # Reset the data device between replays. dm-log-writes passes writes through, so /dev/vdb
    # still holds the final crashed state and a replay cannot revert writes issued after the
    # boundary; mounting then runs ext4 recovery, which writes again. Without the reset each
    # boundary inherits the previous one's state.
    local RESET_CMD
    RESET_CMD="$(replay_reset_cmd)"
    replay_reset_assert "$P2" "$KEY" || { echo "ARM_FAIL the device reset is a no-op; every boundary would inherit the last"; return 1; }
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

    local verify_cmd
    verify_cmd="$(harness_verify_cmd reference "$mode" 0 1000)"

    local b out line res
    for b in $(seq "$floor" "$stride" "$nflush" | head -n "$POINTS"); do
        out=$(vm_ssh "$P2" "$KEY" "sudo umount /mnt/qdb 2>/dev/null; sudo dmsetup remove qdbdata 2>/dev/null; \
            $RESET_CMD; \
            sudo python3 /opt/vmcrash/guest/replay-log.py --log /dev/vdc --replay /dev/vdb --to-flush $b 2>&1 | tail -1; \
            sudo mkdir -p /mnt/qdb; \
            if sudo mount /dev/vdb /mnt/qdb 2>/dev/null; then \
                $verify_cmd; \
            else echo 'MOUNT_FAILED'; fi" 2>&1)
        # The verifier echoes the table kind it graded under. Checked per boundary: a verifier
        # that fell back to the non-WAL oracle reads a bare row count, never runs the recovery
        # triple, and says nothing about the WAL path.
        if ! printf '%s\n' "$out" | grep -q 'wal.table=true'; then
            echo "ARM_FAIL boundary $b was not verified as a WAL table (verifier did not report wal.table=true)"
            return 1
        fi
        line=$(verdict_line "$out")

        # The recovered count comes from the result file, not the verdict line. CrashVerifier
        # writes count/F/C/Wm as anchored key=value pairs from a shutdown hook because the engine
        # logs to the same stdout, where a spliced line can parse as a plausible wrong number.
        # A separate ssh invocation, so no engine output shares the stream.
        local wm cnt
        res=$(vm_ssh "$P2" "$KEY" "cat /mnt/qdb/verify-armB.properties 2>/dev/null" 2>/dev/null)
        cnt=$(printf '%s\n' "$res" | grep -oE '^count=[0-9]+$' | head -1 | cut -d= -f2)

        # The acknowledged watermark is still read from stdout, a known gap. The result file's
        # Wm is localDurableSeqTxn, a sequencer txn, while this value is in rows, and
        # CrashVerifier only prints rowsWatermark rather than recording it. Anchored to ^ so a
        # spliced prefix is rejected as absent rather than parsed as a number.
        wm=$(printf '%s\n' "$out" | grep -oE '^watermark rows=[0-9]+' | head -1 | cut -d= -f2)
        printf '%s\t%s\t%s\t%s\t%s\n' "$b" "$(verdict_classify "$line")" "${wm:--1}" "${cnt:--1}" "$line" >> "$ARM_OUT"
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
judge_sync "$ARM_OUT"

# ---- arm B: the barrier is gone ----------------------------------------------
ARM_OUT="$RUN/nosync.tsv"; : > "$ARM_OUT"
echo "  arm B: WAL table, commitMode=NOSYNC (no barrier) ..."
if ! run_arm NOSYNC; then
    keep
    echo "FAIL t10: the NOSYNC arm could not run (reason above)."
    exit 1
fi
judge_nosync "$ARM_OUT"

# ---- judge --------------------------------------------------------------------
echo
echo "  SYNC   : $sync_durable durable, $sync_fail lost, $sync_rig rig-fault, $sync_nocommit uninformative"
echo "           (expected: at least one durable, nothing else; graded by the oracle against the"
echo "           writer's committed history, since arm=reference has no acknowledged watermark)"
echo "  NOSYNC : $nosync_loss lost, $nosync_green durable, $nosync_rig rig-fault, $nosync_other other,"
echo "           $nosync_nocommit uninformative   (expected: at least one lost, nothing else)"

if judge_verdict; then
    vm_kill "$RUN"
    rm -rf "$RUN"
    echo "PASS t10 (the harness detects a missing WAL durability barrier: SYNC all green, NOSYNC all red)"
    exit 0
fi

keep
if [ "$sync_rig" -gt 0 ] || [ "$nosync_rig" -gt 0 ] || [ "$nosync_other" -gt 0 ]; then
    echo "FAIL t10: a boundary was decided by the RIG, not by the product"
    echo "  (SYNC rig-faults=$sync_rig, NOSYNC rig-faults=$nosync_rig, NOSYNC other=$nosync_other)."
    echo "  An instrument fault is not evidence of a missing barrier. Fix the rig and re-run."
elif [ "$sync_durable" -eq 0 ]; then
    echo "FAIL t10: the BARRIERED arm produced no durable boundary at all"
    echo "  ($sync_nocommit of its boundaries committed nothing)."
    echo "  There is no positive side to the control, so the NOSYNC arm says nothing either."
elif [ "$nosync_loss" -eq 0 ]; then
    echo "FAIL t10: the NOSYNC arm lost nothing that the oracle could name"
    echo "  ($nosync_nocommit uninformative, $nosync_green durable)."
    echo "  A control that cannot produce the failure it exists to detect is not a control."
elif [ "$sync_fail" -gt 0 ]; then
    echo "FAIL t10: the BARRIERED arm lost data ($sync_fail of $((sync_durable + sync_fail)) boundaries red)."
    echo "  That is a durability finding about the WAL path itself, not a broken control."
    echo "  Investigate it before reading anything into the NOSYNC arm."
elif [ "$nosync_green" -gt 0 ] && [ "$nosync_loss" -eq 0 ]; then
    echo "FAIL t10: NOSYNC on a WAL table lost NOTHING — THE BARRIER CONTROL DOES NOT DISCRIMINATE."
    echo "  A workload that issues no durability barrier survived a flush-boundary replay intact,"
    echo "  so this harness cannot tell a durable WAL commit path from one that never flushes."
    echo "  No DURABLE or RPO_OK verdict on the WAL path is evidence until this is explained."
    echo "  First thing to check: whether some OTHER flusher is carrying the data to the device"
    echo "  (the harness's own _progress fsync manufactures ~10.5% of boundaries)."
else
    echo "FAIL t10: the NOSYNC arm was only PARTIALLY red ($nosync_loss lost, $nosync_green durable)."
    echo "  Do NOT relax the bar to accommodate this. A missing barrier should be uniformly"
    echo "  detectable, and a boundary-dependent result means something else is flushing the"
    echo "  data some of the time. Explain it with a measurement, then re-run."
fi
exit 1
