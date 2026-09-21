#!/usr/bin/env bash
# run-flush-sweep.sh [mode] [window_us] [max_points]
#
# The enumerated crash sweep, on a real kernel and a real filesystem.
#
# One workload run under dm-log-writes records every write and every flush. Each flush is then a
# crash point: replay the log to it and the device holds exactly the state a volatile write cache
# would have left behind. One boot serves every boundary, so N crash points cost one workload run
# plus N replay+verify passes rather than N VM boots.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/qemu.sh
source "$HERE/lib/qemu.sh"
# shellcheck source=lib/verdict.sh
source "$HERE/lib/verdict.sh"
# shellcheck source=lib/arms.sh
source "$HERE/lib/arms.sh"
# shellcheck source=lib/junit.sh
source "$HERE/lib/junit.sh"

bash "$HERE/check-host.sh" >/dev/null || { bash "$HERE/check-host.sh"; exit 1; }

MODE="${1:-adaptive}"
WINDOW="${2:-0}"
# Each extra point costs a replay plus a verifier JVM, not a boot. Per-point cost grows with table
# size, because the oracle scans every recovered row.
MAX_POINTS="${3:-${QDB_SWEEP_POINTS:-40}}"
# Derived once so the long ssh command lines below cannot drift apart. qwp-sf is a qwp run for
# everything the server-side oracle does, hence QWP_FLAG true for both.
#
# The arm name has to imply the tier, so QWP_TIER is defaulted here rather than left to the shared
# QDB_QWP_DURABLE_ACK default of `off`, which would launch qwp-sf with durable ack disabled. An
# explicit QDB_QWP_DURABLE_ACK still wins, and run-workload.sh rejects any value for this arm that
# does not include `local`.
case "${QDB_ARM:-reference}" in
    qwp)    QWP_FLAG=true;  QWP_SF_FLAG=false; SF_REPLAY="${QDB_SF_REPLAY:-false}"; QWP_TIER="${QDB_QWP_DURABLE_ACK:-off}"   ;;
    # `compare`, not `true`: verify each boundary twice, server alone and then after the client
    # replays, and report the delta. Measuring only after the replay makes `lost=0` ambiguous
    # between "the client refilled the gap" and "nothing was lost here".
    qwp-sf) QWP_FLAG=true;  QWP_SF_FLAG=true;  SF_REPLAY="${QDB_SF_REPLAY:-compare}"; QWP_TIER="${QDB_QWP_DURABLE_ACK:-local}" ;;
    # product is qwp-sf with the server artifact swapped; --server=product below puts the shipped
    # launcher and the JPMS module configuration it starts under test.
    #
    # WalWriter advances localDurableSeqTxn only under CommitMode.ADAPTIVE, so outside adaptive
    # there is no local durable-ack tier and the arm drops to the plain-qwp contract. The artifact
    # is still under test; the ack channel is not part of the claim in that mode.
    product) QWP_FLAG=true;
             if arm_sf_capable "$MODE"; then
                 QWP_SF_FLAG=true;  SF_REPLAY="${QDB_SF_REPLAY:-compare}"; QWP_TIER="${QDB_QWP_DURABLE_ACK:-local}"
             else
                 QWP_SF_FLAG=false; SF_REPLAY=false;                      QWP_TIER=off
             fi ;;
    *)      QWP_FLAG=false; QWP_SF_FLAG=false; SF_REPLAY="${QDB_SF_REPLAY:-false}"; QWP_TIER="${QDB_QWP_DURABLE_ACK:-off}"   ;;
esac
# qwp-sf is the tier, so outside adaptive there is no arm left. Refuse before any disk is created:
# otherwise the client refuses late, a full VM cycle later, and that surfaces to the caller as the
# misleading "workload was not running".
if [ "${QDB_ARM:-reference}" = qwp-sf ] && ! arm_sf_capable "$MODE"; then
    echo "LOUD_FAILURE: arm=qwp-sf cannot run at mode=$MODE -- the LOCAL durable-ack tier is advanced"
    echo "  only on the ADAPTIVE commit path (WalWriter: 'if (commitMode == CommitMode.ADAPTIVE)'),"
    echo "  so the server emits no STATUS_LOCAL_DURABLE_ACK frames. Use QDB_ARM=qwp for this mode."
    exit 1
fi
PROFILE="${QDB_SCHEMA_PROFILE:-bitmap}"
# EPOCH=-1 disables the periodic durable epoch, so the table runs with a sustained lazy gap:
# columns applied lazily with no epoch cut behind them, and recovery must rebuild the whole tail
# from the durable WAL. That is the path adaptive recovery exists for; a run with epochs every
# second never builds a gap worth recovering from.
EPOCH="${QDB_EPOCH_MS:-1000}"
# QDB_ARM=qwp swaps the embedded-engine writer for a real server plus a real WebSocket client, so
# the cut lands on the wire protocol's write path. Replay, verify and the oracle are arm-agnostic
# because both arms write the same deterministic payload.
ARM="${QDB_ARM:-reference}"
# Store-and-forward replay stays off here (opt in with QDB_SF_REPLAY=true). This sweep requires the
# server's recovered state to hold no duplicates, while an at-least-once client replay resends rows
# the server already committed. The end-to-end no-loss claim has its own driver, run-sf-replay.sh.
# QDB_EDITION=ent runs the enterprise server, qwp arm only, since the reference arm embeds the OSS
# engine in-process. The edition is asserted at runtime via build(), never assumed.
EDITION="${QDB_EDITION:-oss}"
# Derived, not hardcoded: the enterprise root is four levels up from this tree, but the OSS repo is
# also checked out standalone where that path is something else. Verify it looks like an ENT
# checkout, and demand QDB_ENT_ROOT explicitly when it does not.
ENT_ROOT="${QDB_ENT_ROOT:-$(cd "$HERE/../../../.." 2>/dev/null && pwd)}"
# The jar version moves with the POM, so glob rather than pin it, and require exactly one match.
ENT_JAR="${QDB_ENT_JAR:-}"
ENT_DEPS="${QDB_ENT_DEPS:-$ENT_ROOT/questdb-ent/target/deps}"

# The release tarball for the product arm, resolved on the host before any VM boots so a missing
# artifact costs a message rather than two boots and a guest-side failure.
#
# Never hand-assembled or rebuilt here: fidelity to the shipped artifact is the point of this arm,
# so the tarball must come from the real assembly (-P build-binaries).
DIST_TGZ="${QDB_PRODUCT_DIST_TGZ:-}"
if [ "${QDB_ARM:-reference}" = product ] && [ -z "$DIST_TGZ" ]; then
    mapfile -t _dists < <(find "$HERE/../../../core/target" -maxdepth 1 -name 'questdb-*-no-jre-bin.tar.gz' 2>/dev/null | sort)
    case ${#_dists[@]} in
        0) echo "LOUD_FAILURE: arm=product needs the release tarball, and core/target has none."
           echo "  Build it with the real assembly:"
           echo "    JAVA_HOME=<a stock JDK> mvn -pl core -am package -P build-binaries -Dmaven.test.skip=true"
           echo "  A Nix/flox JDK fails the jlink step with 'libmanagement_ext.so has been modified';"
           echo "  that is the JDK, not the product build."
           exit 1 ;;
        1) DIST_TGZ="${_dists[0]}" ;;
        *) echo "LOUD_FAILURE: ${#_dists[@]} candidate no-jre tarballs in core/target; set QDB_PRODUCT_DIST_TGZ to choose:"
           printf '    %s\n' "${_dists[@]}"
           exit 1 ;;
    esac
fi

STATE_DIR="${QDB_VMCRASH_STATE:-/data/qdb-vmcrash}"
BASE="$STATE_DIR/base"
KEY="$BASE/id_ed25519"
LOG="${QDB_SWEEP_LOG:-$STATE_DIR/flush-sweep.log}"
mkdir -p "$(dirname "$LOG")"
# The arm is part of the run's identity: without it, two arms at the same mode, window and profile
# land in directories distinguishable only by PID.
OUTDIR="$STATE_DIR/sweep-out/$ARM-$MODE-w$WINDOW-$PROFILE-e${QDB_EPOCH_MS:-1000}-$$"
RUN="$STATE_DIR/sweep-$ARM-$MODE-w$WINDOW-$PROFILE-e$EPOCH-$$"
STAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)

mkdir -p "$RUN"
qemu-img create -f qcow2 -b "$BASE/golden.qcow2" -F qcow2 "$RUN/overlay.qcow2" >/dev/null
truncate -s 40G "$RUN/data.raw"
truncate -s 60G "$RUN/log.raw"

echo "flush-boundary crash sweep — $STAMP"
echo "  arm=$ARM edition=$EDITION mode=$MODE W=$WINDOW profile=$PROFILE epoch=${EPOCH}ms sibling=${QDB_SIBLING_TABLE:-false} recoverAs=${QDB_RECOVER_AS:-same} ddlEvery=${QDB_DDL_EVERY_ROWS:--1} matView=${QDB_MAT_VIEW:-false} rebaseAt=${QDB_REBASE_AT_ROWS:--1}"
# Name the table kind: "mode=NOSYNC" alone does not say whether the run took the WAL path or the
# bypass-WAL one, and the barrier control is only a control on the WAL path. Flagged loudly when
# the run is that control, whose red is required rather than a regression.
WAL_TABLE="$(harness_wal_table "$MODE")"
echo "  walTable=$WAL_TABLE$([ "$WAL_TABLE" = true ] && [ "$(echo "$MODE" | tr 'A-Z' 'a-z')" = nosync ] \
    && echo "  *** BARRIER CONTROL: a WAL table with no durability barrier. This sweep MUST report failures. ***")"
{ [ "$ARM" = qwp-sf ] || [ "$ARM" = product ]; } && echo "  $ARM: tier=$QWP_TIER sfReplay=$SF_REPLAY sfDurability=${QDB_QWP_SF_DURABILITY:-periodic}"
# "arm=product" says which code path ran; only the tarball's name says which build was under test.
[ "$ARM" = product ] && echo "  product: dist=$(basename "$DIST_TGZ") recoveryPass=${QDB_PRODUCT_RECOVERY_PASS:-true}"
# A defanged run is required to fail: its red is not a regression, and a green one is the problem.
if [ "${QDB_QWP_DEFANG_ACK:-0}" = "1" ]; then
    echo "  *** NEGATIVE CONTROL: durable ack DEFANGED. This sweep MUST report failures. ***"
fi

keep() { echo "run state kept at $RUN" >&2; }

# The verify invocation, built once. Both call sites below, the main loop and the densify pass,
# must use this same string: neighbours checked under a different oracle bracket a failure with
# results that mean something else.
#
# --server is what makes this the product arm: the recovery pass and the replay server run the
# shipped artifact. QDB_PRODUCT_RECOVERY_PASS travels as an env var because ssh does not carry the
# caller's environment.
VERIFY_CMD="$(harness_verify_cmd "$ARM" "$MODE" "$WINDOW" "$EPOCH")"

# The disks are kept on failure, the VM is not; without this, killed runs leave orphaned qemu
# processes holding their pidfiles.
cleanup_vm() { vm_kill "$RUN" 2>/dev/null || true; }
# A SIGNAL MUST END THE RUN, not just kill the VM. A handler that returns hands control back to
# the boundary loop with the guest gone: every remaining vm_ssh comes back empty, classifies as
# UNPARSEABLE, and junit_case records it -- so a deadline-cut sweep reaches junit_finish and
# publishes a complete report full of instrument failures that never happened. Exiting from the
# handler leaves the temp file unrenamed and therefore nothing for a publisher to glob.
trap cleanup_vm EXIT
trap 'cleanup_vm; echo "INTERRUPTED: no report published"; exit 130' INT
trap 'cleanup_vm; echo "TERMINATED: deadline or operator; no report published"; exit 143' TERM

# Refuse an incoherent configuration before the first boot rather than after an hour of sweeping.
harness_assert_config "$MODE" || exit 64
replay_reset_assert_config || exit 64

# ---- one workload run, fully recorded --------------------------------------
P=$(vm_free_port)
vm_boot "$RUN" "$RUN/overlay.qcow2" "$RUN/data.raw" "$P" "" "$RUN/log.raw"
vm_wait_ssh "$P" "$KEY" 240 || { keep; echo "LOUD_FAILURE: guest never answered SSH"; exit 1; }
vm_scp "$P" "$KEY" "$HERE/../../../benchmarks/target/benchmarks.jar" /opt/vmcrash/benchmarks.jar
vm_scp_dir "$P" "$KEY" "$HERE/guest" /opt/vmcrash/
# The tarball goes in whole and is unpacked in the guest, so what runs there is the artifact a user
# downloads. Both boots need it: the first writes with the shipped server, the second recovers.
[ "$ARM" = product ] && vm_scp "$P" "$KEY" "$DIST_TGZ" /opt/vmcrash/questdb-dist.tar.gz
# ENT is not a fat jar: without its runtime deps in entlib/ the server dies with
# io/questdb/jar/jni/LoadException.
if [ "${QDB_EDITION:-oss}" = "ent" ]; then
    # Three distinguishable failures, because "the ent run did not work" is not a diagnosis: the
    # root is not an ENT checkout, the module was never built, or the target dir is ambiguous.
    [ -d "$ENT_ROOT/questdb-ent" ] || { keep; echo "LOUD_FAILURE: QDB_EDITION=ent but '$ENT_ROOT' is not an enterprise checkout (no questdb-ent/); set QDB_ENT_ROOT"; exit 1; }
    if [ -z "$ENT_JAR" ]; then
        # Exclude sources/javadoc/original- classifiers; only the runnable artifact counts.
        mapfile -t _ent_jars < <(find "$ENT_ROOT/questdb-ent/target" -maxdepth 1 -name 'questdb-enterprise-*.jar' \
            ! -name '*-sources.jar' ! -name '*-javadoc.jar' ! -name 'original-*' 2>/dev/null | sort)
        case ${#_ent_jars[@]} in
            0) keep; echo "LOUD_FAILURE: QDB_EDITION=ent but no questdb-enterprise-*.jar in $ENT_ROOT/questdb-ent/target (build questdb-ent first)"; exit 1 ;;
            1) ENT_JAR="${_ent_jars[0]}" ;;
            *) keep; echo "LOUD_FAILURE: ${#_ent_jars[@]} candidate ENT jars in $ENT_ROOT/questdb-ent/target; set QDB_ENT_JAR to choose:"; printf '    %s\n' "${_ent_jars[@]}"; exit 1 ;;
        esac
        echo "  ent jar: $ENT_JAR"
    fi
    [ -f "$ENT_JAR" ] || { keep; echo "LOUD_FAILURE: QDB_EDITION=ent but $ENT_JAR is missing (build questdb-ent first)"; exit 1; }
    vm_scp "$P" "$KEY" "$ENT_JAR" /opt/vmcrash/questdb-enterprise.jar
    vm_ssh "$P" "$KEY" "mkdir -p /opt/vmcrash/entlib"
    for j in "$ENT_DEPS"/*.jar; do vm_scp "$P" "$KEY" "$j" "/opt/vmcrash/entlib/$(basename "$j")"; done
fi
vm_ssh "$P" "$KEY" "sudo sync"
vm_ssh "$P" "$KEY" "QDB_FS_MOUNT_OPTS='${QDB_FS_MOUNT_OPTS:-}' bash /opt/vmcrash/guest/prepare-device.sh --mode=log-writes" >/dev/null \
    || { keep; echo "LOUD_FAILURE: could not build the log-writes stack"; exit 1; }

vm_ssh "$P" "$KEY" "setsid env $(harness_workload_env "$ARM" "$MODE") bash /opt/vmcrash/guest/run-workload.sh --arm=$ARM --mode=$MODE \
    --window-us=$WINDOW --epoch-ms=$EPOCH </dev/null >/mnt/qdb/workload.out 2>&1 &" || true

# Anchor on the first commit so startup is not counted, then let the workload build a history:
# many commits means many flushes means many crash points.
#
# Exhausting this loop is a failure, not a timeout to shrug at. A host that needs longer than 24s
# to reach its first commit would leave every boundary replaying to a point before any commit,
# verifying as NO_COMMIT, so the run measures nothing while reporting green.
anchored=false
for _ in $(seq 1 120); do
    n=$(vm_ssh "$P" "$KEY" "head -1 /mnt/qdb/db/$(arm_progress_file "$ARM") 2>/dev/null | tr -dc '0-9'" 2>/dev/null || echo "")
    [ -n "$n" ] && [ "$n" -ge 1 ] 2>/dev/null && { anchored=true; break; }
    sleep 0.2
done
if [ "$anchored" != true ]; then
    # Same evidence capture as the liveness assertion below: the reason is in writer.log, and a
    # failure path that discards it costs a VM boot to diagnose.
    mkdir -p "$OUTDIR"
    vm_ssh "$P" "$KEY" "tail -40 /mnt/qdb/writer.log 2>/dev/null; echo '--- workload.out ---'; tail -20 /mnt/qdb/workload.out 2>/dev/null" \
        > "$OUTDIR/anchor-failure.out" 2>&1 || true
    echo "  guest logs: $OUTDIR/anchor-failure.out"
    sed -n '1,12p' "$OUTDIR/anchor-failure.out" | sed 's/^/      /'
    # The head above is the arm's startup banner, so a crash lands below it and never reaches
    # the console. Three SLF4J warnings were enough to hide a fatal client error 15 lines in,
    # leaving only "no commit in 24s" -- which reads as host contention, the one explanation
    # this pool always makes plausible (build 270618). Name the fault where it is read.
    fatal=$(grep -m3 -E 'Exception in thread|FatalError|Caused by:|[A-Za-z]+(Exception|Error):' \
            "$OUTDIR/anchor-failure.out" 2>/dev/null || true)
    if [ -n "$fatal" ]; then
        echo "  the guest reported a fatal error, so this is NOT contention:"
        echo "$fatal" | sed 's/^/      /'
    fi
    keep; echo "LOUD_FAILURE: no commit in 24s; every boundary would measure nothing"; exit 1
fi
sleep 8
# The liveness assertion must name the arm's own process: each arm runs a different main class, so
# a shared pattern fails closed on a healthy run. The bracket idiom keeps pgrep from matching its
# own ssh cmdline.
LIVE_PAT="$(arm_live_pattern "$ARM")"
vm_ssh "$P" "$KEY" "pgrep -f '$LIVE_PAT' >/dev/null" || {
    # This fires when the workload died, and the reason is in writer.log inside the guest. Capture
    # it before the VM goes away, or diagnosing it costs another boot.
    mkdir -p "$OUTDIR"
    vm_ssh "$P" "$KEY" "tail -40 /mnt/qdb/writer.log 2>/dev/null; echo '--- workload.out ---'; tail -20 /mnt/qdb/workload.out 2>/dev/null" \
        > "$OUTDIR/liveness-failure.out" 2>&1 || true
    echo "  guest logs: $OUTDIR/liveness-failure.out"
    sed -n '1,12p' "$OUTDIR/liveness-failure.out" | sed 's/^/      /'
    keep; echo "LOUD_FAILURE: workload was not running when the recording stopped"; exit 1
}
vm_ssh "$P" "$KEY" "sleep 2" || true     # let the log kthread drain its queue
vm_kill "$RUN"

# ---- reboot once; replay many boundaries from the one recording -------------
rm -f "$RUN/overlay.qcow2"
qemu-img create -f qcow2 -b "$BASE/golden.qcow2" -F qcow2 "$RUN/overlay.qcow2" >/dev/null
P2=$(vm_free_port)
# The replay boot, and the only boot that gets discard=unmap. The recording boot must not have it:
# a discard issued by the workload would become a DISCARD entry in the dm-log-writes log and change
# what was recorded. Here nothing is recorded and the data device must be resettable between
# boundaries.
QDB_VM_DATA_DISCARD=unmap vm_boot "$RUN" "$RUN/overlay.qcow2" "$RUN/data.raw" "$P2" "" "$RUN/log.raw"
vm_wait_ssh "$P2" "$KEY" 240 || { keep; echo "LOUD_FAILURE: guest never rebooted"; exit 1; }
RESET_CMD="$(replay_reset_cmd)"
echo "  device reset between boundaries: ${QDB_REPLAY_RESET:-blkdiscard}"
replay_reset_assert "$P2" "$KEY" || { keep; exit 1; }
vm_scp "$P2" "$KEY" "$HERE/../../../benchmarks/target/benchmarks.jar" /opt/vmcrash/benchmarks.jar
vm_scp_dir "$P2" "$KEY" "$HERE/guest" /opt/vmcrash/
[ "$ARM" = product ] && vm_scp "$P2" "$KEY" "$DIST_TGZ" /opt/vmcrash/questdb-dist.tar.gz

nflush=$(vm_ssh "$P2" "$KEY" "sudo python3 /opt/vmcrash/guest/replay-log.py --log /dev/vdc --list | head -1" \
    | grep -oE '[0-9]+ flushes' | grep -oE '^[0-9]+')
echo "  recorded $nflush flush boundaries"
[ "${nflush:-0}" -ge 2 ] || { keep; echo "LOUD_FAILURE: only ${nflush:-0} flushes recorded; nothing to sweep"; exit 1; }

# Which boundaries to verify. The recording holds tens of thousands of crash points, and a defect
# that manifests mid-run, during an index rebuild, a partition switch or a mat-view refresh, is
# invisible to a sweep covering only the tail.
#
#   stride (default) : MAX_POINTS points spread across the whole live range
#   tail             : the last MAX_POINTS points (QDB_SWEEP_MODE=tail)
#
# The floor skips the first tenth: those boundaries predate the table, so they verify as NO_COMMIT
# and spend a VM round trip proving nothing.
SWEEP_MODE="${QDB_SWEEP_MODE:-stride}"
points=""
if [ "$SWEEP_MODE" = "tail" ]; then
    first=$(( nflush - MAX_POINTS + 1 ))
    [ "$first" -lt 1 ] && first=1
    points=$(seq "$first" "$nflush")
else
    floor=$(( nflush / 10 ))
    [ "$floor" -lt 1 ] && floor=1
    span=$(( nflush - floor ))
    if [ "$span" -lt "$MAX_POINTS" ]; then
        points=$(seq "$floor" "$nflush")
    else
        stride=$(( span / MAX_POINTS ))
        [ "$stride" -lt 1 ] && stride=1
        # QDB_SWEEP_OFFSET shifts the start within one stride, so repeated runs of the same
        # configuration sample DIFFERENT boundaries.
        #
        # This matters more than it looks. Nothing in this harness is random: the workload
        # writes v = id * 2654435761 with no seed (the identity oracle depends on it), and
        # the points below are a fixed arithmetic sequence from a fixed 10% floor. A nightly
        # job with a fixed offset therefore re-runs one experiment every night and calls the
        # result coverage. With the offset driven by something that changes per run -- CI
        # passes the build id -- night N samples a different residue class mod stride, and
        # the union over `stride` nights is every boundary in the span.
        #
        # Unset or 0 reproduces the previous selection exactly, which is what the controls
        # and any bisect of an old result need.
        off=$(( ${QDB_SWEEP_OFFSET:-0} % stride ))
        [ "$off" -lt 0 ] && off=$(( -off ))
        points=$(seq $(( floor + off )) "$stride" "$nflush")
    fi
fi
echo "  sweep mode=$SWEEP_MODE over $(echo "$points" | wc -w) boundaries: $(echo $points | cut -c1-100)..."

fails=0; checked=0
# A boundary that measured something. NO_COMMIT is a legitimate but empty sample and sits in the
# pass arm below, so without this counter an all-NO_COMMIT sweep exits 0. The gate is after the
# loop.
informative=0
failed_points=""
# Machine-readable output, alongside the text log. It lands in $OUTDIR, outside $RUN, so the
# success-path cleanup does not delete the report a green run produced.
JUNIT_XML="${QDB_JUNIT_XML:-$OUTDIR/junit.xml}"
mkdir -p "$(dirname "$JUNIT_XML")"
# The suite name is the identity a CI dashboard groups and trends by, and arm/mode/W/profile
# does not always distinguish a run: two sweeps that differ only by QDB_SIBLING_TABLE or
# QDB_DDL_EVERY_ROWS produce the same four fields, so their cases merge into one suite and the
# per-case names (flush-N) can collide outright. A caller that varies something not in the name
# passes QDB_JUNIT_SUITE_SUFFIX to keep the suites apart. Unset, the name is unchanged.
junit_begin "$JUNIT_XML" "durability.$ARM.$MODE.W$WINDOW.$PROFILE${QDB_JUNIT_SUITE_SUFFIX:+.$QDB_JUNIT_SUITE_SUFFIX}"
JUNIT_CLASS="durability.$ARM.$MODE.W$WINDOW.$PROFILE"
# The run's identity, in the machine-readable report: a dashboard has to say which build a trend
# belongs to, and the classname carries only four of these facts. Set from the values this script
# already holds, never re-derived inside lib/junit.sh.
junit_property arm          "$ARM"
junit_property edition      "$EDITION"
junit_property mode         "$MODE"
junit_property window_us    "$WINDOW"
junit_property profile      "$PROFILE"
junit_property epoch_ms     "$EPOCH"
junit_property wal_table    "$WAL_TABLE"
junit_property sweep_mode   "$SWEEP_MODE"
junit_property nflush       "$nflush"
junit_property points       "$(echo "$points" | wc -w)"
junit_property replay_reset "${QDB_REPLAY_RESET:-blkdiscard}"
junit_property fs_mount_opts "${QDB_FS_MOUNT_OPTS:-default}"
junit_property stamp        "$STAMP"
# Which instrument produced the numbers is part of the numbers: a trend that moves because the
# harness changed is not a product regression. Degrades to "unknown" outside a git checkout.
junit_property harness_commit "$(git -C "$HERE" rev-parse --short HEAD 2>/dev/null || echo unknown)"
# The ack channel, and whether the run could claim it. Outside adaptive the product provides no
# ack-channel coverage, so a degraded cell must not read as a full-claim run on a dashboard.
if [ "$ARM" = qwp-sf ] || [ "$ARM" = product ]; then
    junit_property qwp_tier  "$QWP_TIER"
    junit_property sf_replay "$SF_REPLAY"
fi
if [ "$ARM" = product ]; then
    junit_property product_dist "$(basename "$DIST_TGZ")"
    junit_property product_recovery_pass "${QDB_PRODUCT_RECOVERY_PASS:-true}"
    # Keyed on arm_sf_capable, the predicate that produced the degrade above, not on the resulting
    # flag values, which are also false for a deliberately defanged run.
    if arm_sf_capable "$MODE"; then
        junit_property degraded false
    else
        junit_property degraded true
        junit_property degraded_reason "no LOCAL durable-ack tier at mode=$MODE; the arm runs the plain-qwp contract and makes no ack-channel claim"
    fi
fi
# A run required to fail must say so where a machine can see it, or its red reads as a regression.
if [ "${QDB_QWP_DEFANG_ACK:-0}" = "1" ]; then
    junit_property negative_control true
fi
if [ "$WAL_TABLE" = true ] && [ "$(echo "$MODE" | tr 'A-Z' 'a-z')" = nosync ]; then
    junit_property barrier_control true
fi
for n in $points; do
    point_started=$(date +%s)
    out=$(vm_ssh "$P2" "$KEY" "sudo umount /mnt/qdb 2>/dev/null; sudo dmsetup remove qdbdata 2>/dev/null; \
        $RESET_CMD; \
        sudo python3 /opt/vmcrash/guest/replay-log.py --log /dev/vdc --replay /dev/vdb --to-flush $n 2>&1 | tail -1; \
        sudo mkdir -p /mnt/qdb; \
        if sudo mount ${QDB_FS_MOUNT_OPTS:+-o ${QDB_FS_MOUNT_OPTS}} /dev/vdb /mnt/qdb 2>/dev/null; then \
            $VERIFY_CMD; \
        else echo 'MOUNT_FAILED'; fi")
    # Archive the full per-boundary output: the one-line verdict in $LOG is a summary, not
    # evidence. Kept outside $RUN so it survives the success-path `rm -rf "$RUN"`.
    mkdir -p "$OUTDIR"
    printf '%s\n' "$out" > "$OUTDIR/flush-$n.out"
    line=$(verdict_line "$out")
    v=$(verdict_classify "$line")
    checked=$((checked + 1))
    [ "$v" = NO_COMMIT ] || informative=$((informative + 1))
    junit_case "$JUNIT_CLASS" "flush-$n" "$v" "$(( $(date +%s) - point_started ))" "$out"
    echo "$STAMP sweep profile=$PROFILE epoch=$EPOCH mode=$MODE W=$WINDOW flush=$n/$nflush verdict=$v line=$line" >> "$LOG"
    printf '  flush %4d/%-4d -> %s\n' "$n" "$nflush" "$v"
    case "$v" in
        DURABLE|RPO_OK|NO_COMMIT) ;;
        *) if [ "$v" = MOUNT_FAILED ]; then
               # A filesystem that will not mount at a crash point is a real outcome, not a harness
               # error, so report it and keep going. Compared as a token, not a raw string, because
               # verdict_classify owns the vocabulary.
               echo "      (filesystem unmountable at this boundary)"
           fi
           fails=$((fails + 1))
           failed_points="$failed_points $n" ;;
    esac
done

# Bisect around failures. A strided sweep says "it breaks somewhere in this gap", but the useful
# question is which boundary breaks first, because that names the operation that did it. Verifying
# the immediate neighbours turns a point into a bracket.
#
# This pass must verify exactly as the main loop does, through the same $VERIFY_CMD: neighbours
# checked under a different oracle bracket the failure with results that mean something else.
if [ -n "$failed_points" ] && [ "${QDB_SWEEP_DENSIFY:-true}" = "true" ]; then
    echo "  densifying around failures:$failed_points"
    for f in $failed_points; do
        for n in $(( f - 2 )) $(( f - 1 )) $(( f + 1 )); do
            [ "$n" -lt 1 ] && continue
            [ "$n" -gt "$nflush" ] && continue
            point_started=$(date +%s)
            # This pass replays backwards (f-2, f-1), so it needs the reset most: without it the
            # bracket is measured on top of the higher boundary just verified, and a bracket built
            # on blended state brackets nothing.
            out=$(vm_ssh "$P2" "$KEY" "sudo umount /mnt/qdb 2>/dev/null; sudo dmsetup remove qdbdata 2>/dev/null; \
                $RESET_CMD; \
                sudo python3 /opt/vmcrash/guest/replay-log.py --log /dev/vdc --replay /dev/vdb --to-flush $n 2>&1 | tail -1; \
                sudo mkdir -p /mnt/qdb; \
                if sudo mount ${QDB_FS_MOUNT_OPTS:+-o ${QDB_FS_MOUNT_OPTS}} /dev/vdb /mnt/qdb 2>/dev/null; then \
                    $VERIFY_CMD; \
                else echo 'MOUNT_FAILED'; fi")
            mkdir -p "$OUTDIR"
            printf '%s\n' "$out" > "$OUTDIR/flush-$n.out"
            line=$(verdict_line "$out")
            v=$(verdict_classify "$line")
            # The neighbours are cases too: they are the bracket around a failure, so leaving them
            # out of the report would hide the most informative points in the run.
            junit_case "$JUNIT_CLASS" "flush-$n-densify" "$v" "$(( $(date +%s) - point_started ))" "$out"
            echo "$STAMP sweep-densify profile=$PROFILE mode=$MODE W=$WINDOW flush=$n/$nflush verdict=$v line=$line" >> "$LOG"
            printf '    neighbour %4d -> %s\n' "$n" "$v"
        done
    done
fi

vm_kill "$RUN"
junit_finish
echo "  junit xml: $JUNIT_XML"
# A sweep that measured nothing is not a pass. Every verdict being NO_COMMIT means no boundary
# carried a committed transaction, so the run made no durability claim, and the exit code below
# counts only failures, of which there are none.
if [ "$informative" -eq 0 ]; then
    keep
    echo "LOUD_FAILURE: $checked boundaries verified, ALL NO_COMMIT -- this sweep measured nothing"
    echo "  full per-boundary output: $OUTDIR"
    echo "  log at $LOG"
    exit 1
fi
if [ "$fails" -eq 0 ] && [ "${QDB_KEEP_RUN:-0}" != "1" ]; then
    rm -rf "$RUN"
    echo "sweep complete: $checked boundaries ($informative informative), 0 failures; log at $LOG"
    echo "  full per-boundary output: $OUTDIR"
elif [ "$fails" -eq 0 ]; then
    keep
    echo "sweep complete: $checked boundaries ($informative informative), 0 failures; log at $LOG"
else
    keep
    echo "sweep complete: $checked boundaries, $fails FAILURES; log at $LOG"
    echo "  full per-boundary output: $OUTDIR"
fi
[ "$fails" -eq 0 ]
