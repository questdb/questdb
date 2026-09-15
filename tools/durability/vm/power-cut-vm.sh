#!/usr/bin/env bash
# power-cut-vm.sh --arm=reference|product --mode=MODE [--window-us=W] [--epoch-ms=N]
#
# One matrix cell, one arm, one cut, one verdict line on stdout.
#
# Exit 0 only on DURABLE / RPO_OK. Anything else exits non-zero AND KEEPS THE
# RUN'S DISKS, because a failure you cannot inspect afterwards is a failure you
# will re-run blind.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/qemu.sh
source "$HERE/lib/qemu.sh"
# shellcheck source=lib/verdict.sh
source "$HERE/lib/verdict.sh"
# shellcheck source=lib/arms.sh
source "$HERE/lib/arms.sh"

bash "$HERE/check-host.sh" >/dev/null || { bash "$HERE/check-host.sh"; exit 1; }

ARM=reference
MODE=adaptive
WINDOW=0
EPOCH=1000
MIN_ROWS=0          # 0 = do not wait for rows; the cut clock starts at workload launch
MAX_ROWS=""         # passthrough; empty = the guest default (unreachable in-window)
CUT_AFTER_MS=""     # empty = draw randomly from [CUT_MIN_MS, CUT_MAX_MS]
SEED=""             # empty = draw one; ALWAYS printed so a failure is replayable
# Delays are measured FROM THE FIRST COMMIT, not from workload launch. JVM and
# engine startup takes seconds, so a delay anchored to launch spends most of its
# early range before QuestDB has committed anything -- measured: 5 of 6
# log-uniform iterations came back NO_COMMIT, carrying no information at all.
# Anchoring to the first commit makes the whole range informative.
DEVICE_MODE=flakey  # flakey = live drop_writes cut; log-writes = record + replay
CUT_MIN_MS=50       # just after the first commit: maximum state in flight
CUT_MAX_MS=20000    # steady-state ingest

for a in "$@"; do
    case "$a" in
        --arm=*)       ARM="${a#*=}" ;;
        --mode=*)      MODE="${a#*=}" ;;
        --window-us=*) WINDOW="${a#*=}" ;;
        --epoch-ms=*)  EPOCH="${a#*=}" ;;
        --min-rows=*)  MIN_ROWS="${a#*=}" ;;
        --cut-after-ms=*) CUT_AFTER_MS="${a#*=}" ;;
        --seed=*)      SEED="${a#*=}" ;;
        --max-rows=*)  MAX_ROWS="${a#*=}" ;;
        --cut-min-ms=*) CUT_MIN_MS="${a#*=}" ;;
        --cut-max-ms=*) CUT_MAX_MS="${a#*=}" ;;
        --device=*)    DEVICE_MODE="${a#*=}" ;;
        *) echo "power-cut-vm: unknown argument $a" >&2; exit 64 ;;
    esac
done

# Reject an unrunnable arm HERE, before 100GB of disks and two VM boots. The list lives in
# lib/arms.sh so it cannot disagree with the arms the guest scripts actually implement.
if ! arm_is_known "$ARM"; then
    echo "power-cut-vm: arm '$ARM' is not runnable (known: reference, qwp, qwp-sf, product)" >&2
    exit 64
fi

# THE PRODUCT ARM ENFORCES THE RPO BAR AT W>0, like every other arm here.
#
# It could not while the client-side LOCAL durable-ack frontier was unbuilt: Wm was unobservable
# from outside the server, so this script downgraded the cell to RPO_UNVERIFIED and measured the
# gap instead of grading it. That tier landed with issues/17, and it was verified against a
# MODULE-LAUNCHED server -- not merely a classpath one -- before this arm was enabled:
# localAcks and trimAdvances both advance, and Wm tracks, through the shipped launcher.
#
# The downgrade is gone rather than left dormant. A latent "cannot enforce" branch on a path
# that now can is how a bar quietly stops being a bar.
RPO_ENFORCEABLE=1

# THE SF ARMS NEED THE ADAPTIVE PATH. Reject here, before 100GB of disks and two VM boots, for
# the same reason the unknown-arm check above is here. The product arm is NOT rejected: it
# degrades to the plain-qwp contract and still tests the artifact (see lib/arms.sh).
if [ "$ARM" = qwp-sf ] && ! arm_sf_capable "$MODE"; then
    echo "power-cut-vm: arm=qwp-sf cannot run at mode=$MODE -- the LOCAL durable-ack tier is" >&2
    echo "  advanced only on the ADAPTIVE commit path, so the server emits no durable-ack frames" >&2
    echo "  and the arm's bar cannot be evaluated. Use --arm=qwp for this mode." >&2
    exit 64
fi

# Randomised cut timing is the POINT of this harness, not a detail. The Java
# sweeps enumerate every durability op; this one samples real wall-clock moments
# on real hardware, so coverage comes from many iterations at DIFFERENT delays.
# A single fixed late cut proves almost nothing: by then the guest kernel has
# written nearly everything back and there is little left to lose.
[ -n "$SEED" ] || SEED=$(od -An -N4 -tu4 < /dev/urandom | tr -dc '0-9')
if [ -z "$CUT_AFTER_MS" ]; then
    # LOG-UNIFORM, not uniform. Deterministic from the seed either way, so a
    # failing run still replays exactly.
    #
    # Uniform over [250ms, 30s] puts ~90% of draws above 3s -- precisely the
    # region where the LEAST is at stake, because by then the guest kernel has
    # written nearly everything back and the cut has little left to take. It was
    # measured: six uniform draws landed at 13-28s and every one lost NOTHING,
    # not even un-acked data, despite an at-risk window of 8-11 txns.
    #
    # Log-uniform gives each decade equal weight (~250ms-750ms, 750ms-2.2s,
    # 2.2s-6.7s, 6.7s-20s, 20s-30s), so early cuts -- where in-flight state
    # actually exists -- are sampled as often as late ones.
    CUT_AFTER_MS=$(awk -v s="$SEED" -v lo="$CUT_MIN_MS" -v hi="$CUT_MAX_MS" \
        'BEGIN { u = (s % 1000000) / 1000000.0; printf "%d", lo * exp(u * log(hi/lo)) }')
fi

STATE_DIR="${QDB_VMCRASH_STATE:-/data/qdb-vmcrash}"
BASE="$STATE_DIR/base"
KEY="$BASE/id_ed25519"
RUN="$STATE_DIR/run-$ARM-$MODE-w$WINDOW-s$SEED-$$"

[ -f "$BASE/golden.qcow2" ] || { echo "LOUD_FAILURE: no golden image; run build-image.sh first"; exit 1; }

mkdir -p "$RUN"
qemu-img create -f qcow2 -b "$BASE/golden.qcow2" -F qcow2 "$RUN/overlay.qcow2" >/dev/null
truncate -s 40G "$RUN/data.raw"
# log-writes needs a separate log device. Sized generously: it records every
# write, so it grows with total traffic rather than with the dataset.
LOGDISK=""
if [ "$DEVICE_MODE" = "log-writes" ]; then
    truncate -s 60G "$RUN/log.raw"
    LOGDISK="$RUN/log.raw"
fi

keep_disks() { echo "run state kept at $RUN" >&2; }

# EVERY exit path must reap the VM. Without this, bail() leaves a daemonized
# qemu running forever: NINE accumulated during one session, holding ~10GB of
# RSS and breaching the one-VM-at-a-time rule this harness promises the host.
# The DISKS are deliberately kept on failure for inspection; the PROCESS never
# should be. `pgrep -f qemu.*qdb-vmcrash` to audit.
cleanup_vm() { vm_kill "$RUN" 2>/dev/null || true; }
trap cleanup_vm EXIT INT TERM

bail() { cleanup_vm; keep_disks; echo "$1"; exit 1; }

# ---- boot, prepare, ingest -------------------------------------------------
P=$(vm_free_port)
vm_boot "$RUN" "$RUN/overlay.qcow2" "$RUN/data.raw" "$P" "" "$LOGDISK"
vm_wait_ssh "$P" "$KEY" 240 || bail "LOUD_FAILURE: guest never answered SSH"

vm_scp "$P" "$KEY" "$HERE/../../../benchmarks/target/benchmarks.jar" /opt/vmcrash/benchmarks.jar \
    || bail "LOUD_FAILURE: could not ship benchmarks.jar into the guest"

# The product arm additionally needs the RELEASE TARBALL, which is what it exists to test.
# Same resolution rules as run-flush-sweep.sh: globbed for the version, exactly one match, and
# never rebuilt or assembled here -- the harness ships what the real assembly produced.
if [ "$ARM" = product ]; then
    DIST_TGZ="${QDB_PRODUCT_DIST_TGZ:-}"
    if [ -z "$DIST_TGZ" ]; then
        mapfile -t _dists < <(find "$HERE/../../../core/target" -maxdepth 1 -name 'questdb-*-no-jre-bin.tar.gz' 2>/dev/null | sort)
        case ${#_dists[@]} in
            0) bail "LOUD_FAILURE: arm=product needs the release tarball; build it with: JAVA_HOME=<a stock JDK> mvn -pl core -am package -P build-binaries -Dmaven.test.skip=true" ;;
            1) DIST_TGZ="${_dists[0]}" ;;
            *) bail "LOUD_FAILURE: ${#_dists[@]} candidate no-jre tarballs in core/target; set QDB_PRODUCT_DIST_TGZ to choose" ;;
        esac
    fi
    echo "  product dist: $(basename "$DIST_TGZ")"
    vm_scp "$P" "$KEY" "$DIST_TGZ" /opt/vmcrash/questdb-dist.tar.gz \
        || bail "LOUD_FAILURE: could not ship the release tarball into the guest"
fi

# Re-ship the guest scripts every run. They are baked into the golden image too,
# but re-shipping means editing one does not require an image rebuild — and it
# removes the class of confusion where the image silently carries a stale copy.
vm_scp_dir "$P" "$KEY" "$HERE/guest" /opt/vmcrash/ \
    || bail "LOUD_FAILURE: could not ship the guest scripts"

# Make the shipped artifacts DURABLE before anything can cut the machine down.
#
# They live on the boot disk, which is outside dm-flakey -- but it is still
# cache=none, so bytes sitting in the guest page cache die with the VMM exactly
# like any other unflushed write. Without this sync the jar is gone or truncated
# after the reboot and the verifier dies with ClassNotFoundException, which reads
# like a packaging fault rather than the lost write it actually is.
#
# The guest SCRIPTS happen to survive regardless, because the golden image also
# carries them and a lost re-ship silently falls back to the baked copy. The jar
# has no such fallback, so this is not optional.
vm_ssh "$P" "$KEY" "sudo sync" \
    || bail "LOUD_FAILURE: could not flush shipped artifacts to the guest boot disk"
vm_ssh "$P" "$KEY" "QDB_FS_MOUNT_OPTS='${QDB_FS_MOUNT_OPTS:-}' QDB_SUPPRESS_WRITEBACK='${QDB_SUPPRESS_WRITEBACK:-0}' bash /opt/vmcrash/guest/prepare-device.sh --mode=$DEVICE_MODE" >/dev/null \
    || bail "LOUD_FAILURE: could not prepare the guest device stack"

# setsid + full redirection so the SSH channel closes immediately instead of
# hanging on the workload's inherited stdout.
# THE ENVIRONMENT MUST BE NAMED ON THE REMOTE COMMAND LINE -- ssh does not carry the caller's.
# This script named nothing, so every QDB_* knob was silently dropped on the live-cut path:
# the DEFANGED negative control ran undefanged and went green, QDB_EDITION=ent ran OSS, and no
# schema dimension beyond the default was reachable. One definition, in lib/arms.sh.
vm_ssh "$P" "$KEY" "setsid env $(harness_workload_env "$ARM" "$MODE") bash /opt/vmcrash/guest/run-workload.sh \
    --arm=$ARM --mode=$MODE --window-us=$WINDOW --epoch-ms=$EPOCH ${MAX_ROWS:+--max-rows=$MAX_ROWS} \
    </dev/null >/mnt/qdb/workload.out 2>&1 &" \
    || bail "LOUD_FAILURE: could not start the workload"

# ---- anchor on the first commit, then wait the drawn interval ---------------
# This is an ANCHOR, not a lateness gate: the delay below can be as small as
# 50ms, so cuts still land with maximum state in flight. What it removes is the
# dead region before QuestDB has committed anything, where a cut yields
# NO_COMMIT and measures nothing.
gate=${MIN_ROWS:-0}
[ "$gate" -lt 1 ] && gate=1
anchored=0
for _ in $(seq 1 180); do
    n=$(vm_ssh "$P" "$KEY" "head -1 /mnt/qdb/db/$(arm_progress_file "$ARM") 2>/dev/null | tr -dc '0-9'" 2>/dev/null || echo "")
    if [ -n "$n" ] && [ "$n" -ge "$gate" ] 2>/dev/null; then anchored=1; break; fi
    sleep 0.2
done
[ "$anchored" -eq 1 ] || bail "LOUD_FAILURE: workload never reached its first commit (see $RUN)"

echo "cut scheduled: seed=$SEED delay=${CUT_AFTER_MS}ms after first commit" >&2
sleep "$(awk "BEGIN{printf \"%.3f\", $CUT_AFTER_MS/1000}")"

# LIVENESS: the workload must still be running AT THE MOMENT OF THE CUT.
# Cutting a finished, quiesced system has nothing in flight and yields a
# guaranteed pass that proves nothing -- a vacuous iteration. Fail it loudly
# instead of counting it as evidence.
# ARM-SPECIFIC, and it was not. This was hardcoded to the reference arm's process, so every
# qwp / qwp-sf run driven through this script aborted as "workload was not running" -- the
# pattern cannot match the process those arms actually start. run-flush-sweep.sh had fixed
# it locally and the fix was never shared, which is precisely how the two copies drifted.
# One definition now, in lib/arms.sh, carrying the bracket idiom and its history with it.
LIVE_PAT="$(arm_live_pattern "$ARM")"
if ! vm_ssh "$P" "$KEY" "pgrep -f '$LIVE_PAT' >/dev/null"; then
    # CAPTURE THE GUEST LOGS BEFORE THE VM DIES. This assertion fires when the workload is
    # already gone, and the reason is always in writer.log / server.log -- which used to require
    # booting the VM again to read, or were lost entirely once the disks were reaped.
    # run-flush-sweep.sh learned this the expensive way (issues/04): a failure path that
    # discards its own evidence costs three VM boots to diagnose. The same fix belongs here,
    # and its absence is why the first product-arm SYNC failure said only "not running".
    vm_ssh "$P" "$KEY" "tail -40 /mnt/qdb/writer.log 2>/dev/null; echo '--- workload.out ---'; tail -20 /mnt/qdb/workload.out 2>/dev/null; echo '--- server.log ---'; tail -20 /mnt/qdb/server.log 2>/dev/null" \
        > "$RUN/liveness-failure.out" 2>&1 || true
    echo "guest logs: $RUN/liveness-failure.out" >&2
    sed -n '1,12p' "$RUN/liveness-failure.out" | sed 's/^/    /' >&2
    bail "LOUD_FAILURE: workload ($LIVE_PAT) was not running at cut time (seed=$SEED delay=${CUT_AFTER_MS}ms) — iteration would be vacuous"
fi

# ---- THE CUT: arm the device first, then kill the machine ------------------
if [ "$DEVICE_MODE" = "log-writes" ]; then
    # Nothing to arm. The log already holds every write and every flush; the
    # discarding happens at REPLAY time against a chosen flush boundary, which
    # is what lets one run yield many enumerable crash points. Pause first so
    # the log kthread lands its queued entries -- entries still queued at the
    # kill are simply never recorded.
    vm_ssh "$P" "$KEY" "sleep 2" || true
    vm_kill "$RUN"
else
    vm_ssh "$P" "$KEY" "bash /opt/vmcrash/guest/arm-cut.sh" >/dev/null 2>&1 &
    arm_pid=$!
    vm_wait_console "$RUN" "CUT-ARMED" 60 || { kill "$arm_pid" 2>/dev/null; bail "LOUD_FAILURE: cut never armed"; }
    vm_kill "$RUN"
    wait "$arm_pid" 2>/dev/null || true
fi

# ---- reboot and run the oracle ---------------------------------------------
# FRESH BOOT DISK, SAME DATA DISK. The data disk is what is under test; the OS
# disk is scaffolding. It sits outside dm-flakey but is still cache=none, so the
# VMM kill leaves it dirty, and ext4 cannot always replay cleanly -- observed
# once as a guest dropping to EMERGENCY MODE, which surfaced as
# "guest never rebooted after the cut" and failed an otherwise fine iteration.
# Booting a clean overlay removes an irrelevant failure mode without touching
# the thing being measured. data.raw is carried over exactly as the cut left it.
rm -f "$RUN/overlay.qcow2"
qemu-img create -f qcow2 -b "$BASE/golden.qcow2" -F qcow2 "$RUN/overlay.qcow2" >/dev/null

P2=$(vm_free_port)
vm_boot "$RUN" "$RUN/overlay.qcow2" "$RUN/data.raw" "$P2" "" "$LOGDISK"
vm_wait_ssh "$P2" "$KEY" 240 || bail "LOUD_FAILURE: guest never rebooted after the cut"

# The fresh overlay carries the golden image's guest scripts but NOT the jar, so
# re-ship it. This also removes the run's dependence on the pre-cut `sudo sync`
# having made the jar durable.
vm_scp "$P2" "$KEY" "$HERE/../../../benchmarks/target/benchmarks.jar" /opt/vmcrash/benchmarks.jar \
    || bail "LOUD_FAILURE: could not re-ship benchmarks.jar after the cut"
# Re-ship the tarball for the same reason the jar is re-shipped: the post-cut boot must RECOVER
# with the shipped artifact, and whatever was unpacked before the cut may not have survived it.
[ "$ARM" = product ] && { vm_scp "$P2" "$KEY" "$DIST_TGZ" /opt/vmcrash/questdb-dist.tar.gz \
    || bail "LOUD_FAILURE: could not re-ship the release tarball after the cut"; }
vm_scp_dir "$P2" "$KEY" "$HERE/guest" /opt/vmcrash/ \
    || bail "LOUD_FAILURE: could not re-ship the guest scripts after the cut"

vm_ssh "$P2" "$KEY" "QDB_FS_MOUNT_OPTS='${QDB_FS_MOUNT_OPTS:-}' bash /opt/vmcrash/guest/prepare-device.sh --reattach --mode=$DEVICE_MODE" >/dev/null \
    || bail "LOUD_FAILURE: could not reattach the device after the cut"

LINE=$(vm_ssh "$P2" "$KEY" "$(harness_verify_cmd "$ARM" "$MODE" "$WINDOW" "$EPOCH")" 2>&1 || true)
vm_kill "$RUN"

if [ "${RPO_ENFORCEABLE:-1}" -eq 0 ]; then
    case "$LINE" in
        DURABLE*|RPO_OK*) LINE="RPO_UNVERIFIED ${LINE} (gap measured; RPO bar NOT enforced — client Wm unavailable)" ;;
    esac
fi
LINE="$LINE [seed=$SEED cutAfterMs=$CUT_AFTER_MS]"
# CLASSIFY THE VERDICT LINE, NOT THE WHOLE OUTPUT. verify.sh prints its evidence first and the
# verdict last, so `verdict_classify "$LINE"` matched a DETAIL line and returned UNPARSEABLE for
# every run once that evidence was added -- which silently turned the pass path off: disks kept
# on green runs, exit 1 on green runs. Invisible from outside, because both callers take
# `tail -1` and so reported the right verdict while this script was reporting the wrong one to
# itself. 27 directories and 28 GB from a single run-matrix.sh session is what surfaced it.
V=$(verdict_classify "$(verdict_line "$LINE")")
if verdict_is_pass "$V"; then
    echo "$LINE"
    rm -rf "$RUN"
    exit 0
fi
# Note to stderr FIRST, verdict to stdout LAST, so the verdict is the final line
# on both streams. Emitting the note afterwards let a caller merging stderr into
# stdout capture "run state kept at ..." with `tail -1` and discard the verdict
# sitting immediately above it -- which then classifies as UNPARSEABLE.
keep_disks
echo "$LINE"
exit 1
