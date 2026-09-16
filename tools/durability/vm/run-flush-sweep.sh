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
# shellcheck source=lib/arms.sh
source "$HERE/lib/arms.sh"
# shellcheck source=lib/junit.sh
source "$HERE/lib/junit.sh"

bash "$HERE/check-host.sh" >/dev/null || { bash "$HERE/check-host.sh"; exit 1; }

MODE="${1:-adaptive}"
WINDOW="${2:-0}"
# Points per run. The replay+verify loop reuses ONE booted VM, so each extra point costs a replay
# plus a verifier JVM -- not a boot. That makes a much larger default affordable: the expensive
# part (record the workload, reboot, rebuild the log-writes stack) is paid once per run, not per
# point. Raise freely for a thorough run; the per-point cost grows with table size because the
# oracle scans every recovered row.
MAX_POINTS="${3:-${QDB_SWEEP_POINTS:-40}}"
# Derived once, so the long ssh command lines below stay readable and cannot drift apart.
# qwp-sf is a qwp run for everything the SERVER-side oracle does -- hence QWP_FLAG true for both --
# and additionally turns on the client-side replay and its own duplicate-tolerant bar.
# QWP_TIER is derived here too. The arm NAME has to imply the tier: leaving it to the shared
# QDB_QWP_DURABLE_ACK default of `off` made the sweep launch qwp-sf with durable ack disabled,
# and the arm correctly refused to run -- a label promising a guarantee the configuration did not
# request. Defaulted, not forced: an explicit QDB_QWP_DURABLE_ACK still wins, and run-workload.sh
# rejects any value for this arm that does not include `local`.
case "${QDB_ARM:-reference}" in
    qwp)    QWP_FLAG=true;  QWP_SF_FLAG=false; SF_REPLAY="${QDB_SF_REPLAY:-false}"; QWP_TIER="${QDB_QWP_DURABLE_ACK:-off}"   ;;
    # `compare`, not `true`: verify each boundary TWICE -- server alone, then after the client
    # replays -- and report the delta. Measuring only after the replay makes `lost=0` ambiguous,
    # because it equally describes "the client refilled the gap" and "nothing was lost here".
    qwp-sf) QWP_FLAG=true;  QWP_SF_FLAG=true;  SF_REPLAY="${QDB_SF_REPLAY:-compare}"; QWP_TIER="${QDB_QWP_DURABLE_ACK:-local}" ;;
    # product is qwp-sf with the SERVER ARTIFACT SWAPPED, so every flag here is qwp-sf's. The one
    # difference is carried by --server=product below, which is what makes the shipped launcher --
    # and the JPMS module configuration it starts -- the thing under test.
    #
    # OUTSIDE ADAPTIVE there is no LOCAL durable-ack tier to have (WalWriter advances
    # localDurableSeqTxn only under CommitMode.ADAPTIVE), so the arm drops to the plain-qwp
    # contract. The artifact is still the thing under test; the ack channel simply is not part of
    # the claim in that mode, and the flags say so rather than asserting a guarantee the product
    # does not offer here.
    product) QWP_FLAG=true;
             if arm_sf_capable "$MODE"; then
                 QWP_SF_FLAG=true;  SF_REPLAY="${QDB_SF_REPLAY:-compare}"; QWP_TIER="${QDB_QWP_DURABLE_ACK:-local}"
             else
                 QWP_SF_FLAG=false; SF_REPLAY=false;                      QWP_TIER=off
             fi ;;
    *)      QWP_FLAG=false; QWP_SF_FLAG=false; SF_REPLAY="${QDB_SF_REPLAY:-false}"; QWP_TIER="${QDB_QWP_DURABLE_ACK:-off}"   ;;
esac
# qwp-sf IS the tier, so outside adaptive there is no arm left. Refuse before any disk is
# created; the alternative is the client's late "the channel is dead" refusal, 20,000 rows and
# one VM cycle later, which reaches the caller as the misleading "workload was not running".
if [ "${QDB_ARM:-reference}" = qwp-sf ] && ! arm_sf_capable "$MODE"; then
    echo "LOUD_FAILURE: arm=qwp-sf cannot run at mode=$MODE -- the LOCAL durable-ack tier is advanced"
    echo "  only on the ADAPTIVE commit path (WalWriter: 'if (commitMode == CommitMode.ADAPTIVE)'),"
    echo "  so the server emits no STATUS_LOCAL_DURABLE_ACK frames. Use QDB_ARM=qwp for this mode."
    exit 1
fi
PROFILE="${QDB_SCHEMA_PROFILE:-bitmap}"
# EPOCH=-1 DISABLES the periodic durable epoch, so the table runs with a
# SUSTAINED LAZY GAP: columns applied lazily with no epoch cut behind them, and
# recovery must rebuild the whole tail from the durable WAL. That is the shape
# AdaptiveO3LazyGapCrashSweepTest and W2/W3/W5 are built around, and it is the
# path adaptive recovery exists for -- a run with epochs every second never
# builds a gap worth recovering from.
EPOCH="${QDB_EPOCH_MS:-1000}"
# QDB_ARM=qwp swaps the embedded-engine writer for a REAL server + REAL WebSocket client, so the
# cut lands on the wire protocol's write path. Everything downstream -- replay, verify, oracle --
# is arm-agnostic because both arms write the same deterministic payload.
ARM="${QDB_ARM:-reference}"
# SF REPLAY IS OFF HERE BY DESIGN (opt in with QDB_SF_REPLAY=true). This sweep asserts that the
# SERVER's recovered state is internally consistent -- identity, contiguity, NO DUPLICATES. A
# store-and-forward replay is at-least-once: it resends rows the server already committed, which
# is correct behaviour for no-loss but violates the no-duplicates bar, and the sweep rightly
# reported SILENT_CORRUPTION when the two were combined. The end-to-end no-loss claim has its own
# driver, run-sf-replay.sh, which measures distinct ids and quantifies the duplicates.
# QDB_EDITION=ent runs the ENTERPRISE server (qwp arm only -- the reference arm embeds the OSS
# engine in-process). The edition is ASSERTED at runtime via build(), never assumed.
EDITION="${QDB_EDITION:-oss}"
# DERIVED, not hardcoded. This tree is questdb-enterprise/questdb/tools/durability/vm, so
# the enterprise root is four levels up -- but the OSS repo is ALSO checked out standalone,
# where that path is something else entirely. So derive, then VERIFY it looks like an ENT
# checkout, and demand QDB_ENT_ROOT explicitly when it does not. The previous default was
# one developer's home directory, which silently produced "jar is missing" for everyone else.
ENT_ROOT="${QDB_ENT_ROOT:-$(cd "$HERE/../../../.." 2>/dev/null && pwd)}"
# The jar version is not pinned here: it moves with the POM, and a hardcoded
# questdb-enterprise-4.0.2-SNAPSHOT.jar becomes wrong at the next version bump without
# anyone noticing until an ENT run fails. Glob, and require EXACTLY one match so an
# ambiguous target dir is reported rather than silently resolved.
ENT_JAR="${QDB_ENT_JAR:-}"
ENT_DEPS="${QDB_ENT_DEPS:-$ENT_ROOT/questdb-ent/target/deps}"

# THE RELEASE TARBALL, for the product arm. Resolved on the HOST, before any VM boots, so a
# missing artifact costs a message rather than two boots and a confusing guest-side failure.
#
# Globbed, not pinned: the version moves with the POM, and a hardcoded name silently becomes
# wrong at the next bump -- the same fault the ENT jar resolution above was written to avoid.
# Exactly one match is required; an ambiguous target/ is reported rather than resolved by luck.
#
# NOT hand-assembled from parts, and deliberately not rebuilt here either. Fidelity to the
# shipped artifact is the entire point of this arm, so the tarball must come from the real
# assembly (-P build-binaries) and the harness only ships what that produced.
DIST_TGZ="${QDB_PRODUCT_DIST_TGZ:-}"
if [ "${QDB_ARM:-reference}" = product ] && [ -z "$DIST_TGZ" ]; then
    mapfile -t _dists < <(find "$HERE/../../../core/target" -maxdepth 1 -name 'questdb-*-no-jre-bin.tar.gz' 2>/dev/null | sort)
    case ${#_dists[@]} in
        0) echo "LOUD_FAILURE: arm=product needs the release tarball, and core/target has none."
           echo "  Build it with the real assembly:"
           echo "    JAVA_HOME=<a stock JDK> mvn -pl core -am package -P build-binaries -Dmaven.test.skip=true"
           echo "  A Nix/flox JDK fails the jlink step with 'libmanagement_ext.so has been modified';"
           echo "  that is the JDK, not the product build. See issues/03."
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
LOG="$STATE_DIR/flush-sweep.log"
# THE ARM IS PART OF THE RUN'S IDENTITY. It was missing from both names, so a reference sweep
# and a qwp sweep at the same mode/window/profile landed in directories distinguishable only by
# PID -- and with a third arm that is worse. Evidence that cannot be attributed to the run that
# produced it is not evidence.
OUTDIR="$STATE_DIR/sweep-out/$ARM-$MODE-w$WINDOW-$PROFILE-e${QDB_EPOCH_MS:-1000}-$$"
RUN="$STATE_DIR/sweep-$ARM-$MODE-w$WINDOW-$PROFILE-e$EPOCH-$$"
STAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)

mkdir -p "$RUN"
qemu-img create -f qcow2 -b "$BASE/golden.qcow2" -F qcow2 "$RUN/overlay.qcow2" >/dev/null
truncate -s 40G "$RUN/data.raw"
truncate -s 60G "$RUN/log.raw"

echo "flush-boundary crash sweep — $STAMP"
echo "  arm=$ARM edition=$EDITION mode=$MODE W=$WINDOW profile=$PROFILE epoch=${EPOCH}ms sibling=${QDB_SIBLING_TABLE:-false} recoverAs=${QDB_RECOVER_AS:-same} ddlEvery=${QDB_DDL_EVERY_ROWS:--1} matView=${QDB_MAT_VIEW:-false} rebaseAt=${QDB_REBASE_AT_ROWS:--1}"
# NAME THE TABLE KIND. "mode=NOSYNC" alone does not say whether the run exercised the WAL path
# or the bypass-WAL one, and the barrier control is only a control on the WAL path -- archived
# evidence that cannot distinguish the two says nothing about which half of the product was
# tested. Flagged loudly when the run IS the control, so a reader never mistakes its required
# red for a regression.
WAL_TABLE="$(harness_wal_table "$MODE")"
echo "  walTable=$WAL_TABLE$([ "$WAL_TABLE" = true ] && [ "$(echo "$MODE" | tr 'A-Z' 'a-z')" = nosync ] \
    && echo "  *** BARRIER CONTROL: a WAL table with no durability barrier. This sweep MUST report failures. ***")"
{ [ "$ARM" = qwp-sf ] || [ "$ARM" = product ]; } && echo "  $ARM: tier=$QWP_TIER sfReplay=$SF_REPLAY sfDurability=${QDB_QWP_SF_DURABILITY:-periodic}"
# NAME THE ARTIFACT IN THE RUN'S OWN OUTPUT. "arm=product" says which code path ran; only the
# tarball's name says WHICH BUILD was under test, and a report that cannot say that is not
# evidence about a shipped artifact.
[ "$ARM" = product ] && echo "  product: dist=$(basename "$DIST_TGZ") recoveryPass=${QDB_PRODUCT_RECOVERY_PASS:-true}"
# A defanged run is REQUIRED to fail. Say so up front, so a reader of the log cannot mistake
# the red result for a regression -- and so a GREEN one is immediately visible as the real
# problem it would be.
if [ "${QDB_QWP_DEFANG_ACK:-0}" = "1" ]; then
    echo "  *** NEGATIVE CONTROL: durable ack DEFANGED. This sweep MUST report failures. ***"
fi

keep() { echo "run state kept at $RUN" >&2; }

# THE VERIFY INVOCATION, BUILT ONCE. Every flag in it is constant across boundaries, and the two
# call sites below -- the main loop and the densify pass -- MUST use the same one. They did not:
# the densify pass carried a hand-copied duplicate that never gained --qwp, --qwp-sf or
# --sf-replay, so the neighbours of a qwp-sf failure were verified under a different oracle and
# came back green for the wrong reason. A bracket that cannot bracket is worse than none. One
# string, referenced twice, so the next flag cannot drift either -- the same fix, and the same
# reason, as lib/arms.sh.
#
# --server is what makes this arm the product arm: the recovery pass and the replay server run
# the SHIPPED artifact. QDB_PRODUCT_RECOVERY_PASS must travel as an ENV VAR because ssh does not
# carry the caller's environment.
VERIFY_SERVER="$(arm_server_kind "$ARM")"
VERIFY_CMD="$(harness_verify_cmd "$ARM" "$MODE" "$WINDOW" "$EPOCH")"

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
# The release tarball goes in WHOLE and is unpacked in the guest, so what runs there is the
# artifact a user downloads, not a directory tree we assembled on the host and copied file by
# file. Both boots need it: the first writes with the shipped server, the second RECOVERS with it.
[ "$ARM" = product ] && vm_scp "$P" "$KEY" "$DIST_TGZ" /opt/vmcrash/questdb-dist.tar.gz
# ENT ships as a jar PLUS its runtime deps: it is not a fat jar, and without entlib/ the server
# dies with io/questdb/jar/jni/LoadException.
if [ "${QDB_EDITION:-oss}" = "ent" ]; then
    # Resolve the jar now, loudly. Three distinguishable failures, because "ent run did not
    # work" is not a diagnosis: the root is not an ENT checkout, the module was never built,
    # or the target dir holds more than one candidate jar.
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

# Let it build a real history: many commits means many flushes means many
# crash points. Anchor on the first commit so startup is not counted.
for _ in $(seq 1 120); do
    n=$(vm_ssh "$P" "$KEY" "head -1 /mnt/qdb/db/$(arm_progress_file "$ARM") 2>/dev/null | tr -dc '0-9'" 2>/dev/null || echo "")
    [ -n "$n" ] && [ "$n" -ge 1 ] 2>/dev/null && break
    sleep 0.2
done
sleep 8
# The liveness assertion must name the arm's OWN process: the qwp arm runs
# QwpCrashIngestClient, so the reference-arm pattern would never match and every qwp run
# would abort as "workload not running" -- a guard that fails closed on a healthy run is as
# useless as one that never fires. Bracket idiom avoids pgrep matching its own ssh cmdline.
LIVE_PAT="$(arm_live_pattern "$ARM")"
vm_ssh "$P" "$KEY" "pgrep -f '$LIVE_PAT' >/dev/null" || {
    # CAPTURE THE GUEST LOGS. This assertion fires when the workload died, and the
    # reason is always in writer.log -- which used to require booting the VM again to
    # read. A failure path that discards its own evidence costs three VM boots to
    # diagnose (measured: an empty main class from a mangled line continuation).
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
# THE REPLAY BOOT, and the only boot that gets discard=unmap. The recording boot above must not
# have it: an unmapping discard issued by the workload would become a DISCARD entry in the
# dm-log-writes log and change what was recorded. Here there is nothing left to record, and the
# data device has to be resettable between boundaries -- see replay_reset_cmd in lib/qemu.sh.
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

# WHICH boundaries to verify. The recording holds tens of thousands of crash points; verifying
# the last handful covers ~0.01% of them, and only the part of the workload that ran last. A
# defect that manifests mid-run -- during an index rebuild, a partition switch, a mat-view refresh
# -- is invisible to a tail sweep. The bitmap SIGSEGV was found at boundary 13776 of ~14000, near
# the tail, which was luck rather than method.
#
#   stride (default) : MAX_POINTS points spread across the whole live range
#   tail             : the last MAX_POINTS points (QDB_SWEEP_MODE=tail)
#
# The floor skips the first 10%: those boundaries predate the table, so they verify as NO_COMMIT
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
        points=$(seq "$floor" "$stride" "$nflush")
    fi
fi
echo "  sweep mode=$SWEEP_MODE over $(echo "$points" | wc -w) boundaries: $(echo $points | cut -c1-100)..."

fails=0; checked=0
failed_points=""
# MACHINE-READABLE OUTPUT, alongside the text log rather than instead of it (issues/06). Lands
# next to the per-boundary evidence in $OUTDIR, which is outside $RUN and therefore survives the
# success-path cleanup -- a report that a green run deletes is no use to a dashboard.
JUNIT_XML="${QDB_JUNIT_XML:-$OUTDIR/junit.xml}"
mkdir -p "$(dirname "$JUNIT_XML")"
junit_begin "$JUNIT_XML" "durability.$ARM.$MODE.W$WINDOW.$PROFILE"
JUNIT_CLASS="durability.$ARM.$MODE.W$WINDOW.$PROFILE"
# THE RUN'S IDENTITY, IN THE MACHINE-READABLE REPORT. The classname smuggles four of these
# into a dotted string; everything else lived only in the text log, so a dashboard could not
# say WHICH BUILD a trend belonged to -- and on a green product run the artifact name appeared
# nowhere in the XML at all. issues/06 asks for "boundary 13776 regressed between build N and
# N+1"; that question needs the build named.
#
# Set from the values this script already holds, never re-derived inside lib/junit.sh: a second
# derivation is a second source of truth, and this harness has paid for that four times.
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
# THE HARNESS COMMIT. Which instrument produced the numbers is part of the numbers: a trend
# that moves because the harness changed is not a product regression, and without this the two
# are indistinguishable after the fact. Degrades to "unknown" outside a git checkout rather
# than failing the sweep.
junit_property harness_commit "$(git -C "$HERE" rev-parse --short HEAD 2>/dev/null || echo unknown)"
# THE ACK CHANNEL, AND WHETHER THE RUN COULD CLAIM IT. 203f136a05 promises that outside
# adaptive "nothing reports ack-channel coverage the product cannot provide in that mode" --
# the text log honours that, the XML did not, so a DEGRADED product/SYNC cell was
# indistinguishable from a full-claim run and a dashboard would show "product arm: green" for
# coverage that was explicitly never provided.
if [ "$ARM" = qwp-sf ] || [ "$ARM" = product ]; then
    junit_property qwp_tier  "$QWP_TIER"
    junit_property sf_replay "$SF_REPLAY"
fi
if [ "$ARM" = product ]; then
    junit_property product_dist "$(basename "$DIST_TGZ")"
    junit_property product_recovery_pass "${QDB_PRODUCT_RECOVERY_PASS:-true}"
    # The degrade is the arm dropping to the plain-qwp contract because the LOCAL durable-ack
    # tier cannot exist outside adaptive. Keyed on arm_sf_capable, the same predicate that
    # produced the degrade above -- not on the resulting flag values, which would also be false
    # for a deliberately defanged run.
    if arm_sf_capable "$MODE"; then
        junit_property degraded false
    else
        junit_property degraded true
        junit_property degraded_reason "no LOCAL durable-ack tier at mode=$MODE; the arm runs the plain-qwp contract and makes no ack-channel claim"
    fi
fi
# A run that is REQUIRED to fail must say so where a machine can see it, or its red is
# indistinguishable from a regression.
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
    # Archive the FULL per-boundary output. The one-line verdict in $LOG is a summary,
    # not evidence: every time a result needed explaining, the explanation was in the
    # lines this used to throw away. Kept outside $RUN so it survives the success-path
    # `rm -rf "$RUN"`.
    mkdir -p "$OUTDIR"
    printf '%s\n' "$out" > "$OUTDIR/flush-$n.out"
    line=$(verdict_line "$out")
    v=$(verdict_classify "$line")
    checked=$((checked + 1))
    junit_case "$JUNIT_CLASS" "flush-$n" "$v" "$(( $(date +%s) - point_started ))" "$out"
    echo "$STAMP sweep profile=$PROFILE epoch=$EPOCH mode=$MODE W=$WINDOW flush=$n/$nflush verdict=$v line=$line" >> "$LOG"
    printf '  flush %4d/%-4d -> %s\n' "$n" "$nflush" "$v"
    case "$v" in
        DURABLE|RPO_OK|NO_COMMIT) ;;
        *) if [ "$v" = MOUNT_FAILED ]; then
               # A filesystem that will not mount at a crash point is a real
               # outcome, not a harness error -- report it and keep going.
               # Compared as a TOKEN, not as the raw string: verdict_classify owns
               # the vocabulary, and this line matching text the classifier knew
               # nothing about was two sources of truth for one concept.
               echo "      (filesystem unmountable at this boundary)"
           fi
           fails=$((fails + 1))
           failed_points="$failed_points $n" ;;
    esac
done

# BISECT AROUND FAILURES. A strided sweep says "it breaks somewhere in this gap"; the useful
# question is which boundary FIRST breaks, because that names the operation that did it. Verify
# the immediate neighbours of each failure so the report gives a bracket rather than a point.
# THE DENSIFY PASS MUST VERIFY THE SAME WAY THE MAIN LOOP DOES. It did not: its verify.sh
# invocation was a hand-copied duplicate that never gained --qwp, --qwp-sf or the arm's
# --sf-replay mode. So a qwp-sf failure's neighbours were verified under a DIFFERENT oracle,
# and came back green for the wrong reason -- a bracket that cannot bracket. Both call sites
# now build the same argument list; keep it that way, or the next flag will drift too.
if [ -n "$failed_points" ] && [ "${QDB_SWEEP_DENSIFY:-true}" = "true" ]; then
    echo "  densifying around failures:$failed_points"
    for f in $failed_points; do
        for n in $(( f - 2 )) $(( f - 1 )) $(( f + 1 )); do
            [ "$n" -lt 1 ] && continue
            [ "$n" -gt "$nflush" ] && continue
            point_started=$(date +%s)
            # The densify pass replays BACKWARDS (f-2, f-1), so it is the call site that needs
            # the reset most: without it the bracket around a failure is measured on the state
            # of the HIGHER boundary just verified, and a bracket built on a blended state
            # cannot bracket.
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
            # The densified neighbours are cases too. They are the BRACKET around a failure --
            # the boundary that first breaks is what names the operation that did it -- so
            # leaving them out of the report would hide the most informative points in the run.
            junit_case "$JUNIT_CLASS" "flush-$n-densify" "$v" "$(( $(date +%s) - point_started ))" "$out"
            echo "$STAMP sweep-densify profile=$PROFILE mode=$MODE W=$WINDOW flush=$n/$nflush verdict=$v line=$line" >> "$LOG"
            printf '    neighbour %4d -> %s\n' "$n" "$v"
        done
    done
fi

vm_kill "$RUN"
junit_finish
echo "  junit xml: $JUNIT_XML"
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
