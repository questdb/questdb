#!/usr/bin/env bash
# power-cut-dmflakey.sh
#
# POWER-CUT DURABILITY TEST FOR QUESTDB (HARNESS #2)
#
# WHAT THIS TESTS — POWER-LOSS DURABILITY, NOT JUST PROCESS-CRASH CONSISTENCY:
#   dm-flakey with the drop_writes feature discards writes at the block layer during
#   its "down" interval, exactly modelling a power cut: un-fsync'd data that lives
#   only in the OS page cache is LOST when the device switches to drop_writes and
#   the filesystem is unmounted (the umount's writeback is silently dropped).
#   This contrasts with harness #1 (crash-consistency-pkill.sh) which hard-kills the
#   JVM but leaves the page cache intact, so NOSYNC-committed data also survives there.
#
# THE CORE DISTINCTION:
#   SYNC mode:   fsync/msync pushes committed data to the block device BEFORE the commit
#                is acknowledged → data is already on disk when the power cut happens →
#                MUST survive (expected: count >= committed_before_cut).
#   NOSYNC mode: commits are acknowledged without flushing to the device → data sits in
#                the page cache only → LOST on power cut (expected: count < committed,
#                or partial/corrupt data).  Loss is expected and demonstrates why SYNC matters.
#
# TECHNIQUE (mirrors xfstests _flakey_drop_and_remount):
#   1. Create a dm-flakey device over a loop device (or real disk) in "always UP" mode.
#   2. Format and mount ext4 on the flakey device.
#   3. Run CrashIngestWriter until committed rows >= threshold.
#   4. Capture COMMITTED (from _progress) into a shell variable BEFORE the cut.
#   5. Kill the writer (page cache still intact here — we haven't cut power yet).
#   6. THE CUT: switch dm-flakey to "always DOWN + drop_writes" then unmount.
#      The umount triggers writeback; dm-flakey DROPS every write → un-fsync'd pages
#      never reach the image.  Switch back to UP and remount to reveal what survived.
#   7. Run CrashVerifier and interpret vs COMMITTED.
#
# MUST RUN AS ROOT:
#   sudo bash power-cut-dmflakey.sh
#
#   dm-flakey, losetup, dmsetup, mount, umount all require root.
#   The script uses a cleanup() EXIT trap to remove the dm device and loop device.
#
# PREREQUISITES:
#   - Linux kernel with dm-flakey module (modprobe dm-flakey; standard since ~4.0)
#   - device-mapper-event or dm-event package (dmsetup from device-mapper)
#   - e2fsprogs (mkfs.ext4) for FSTYPE=ext4 (default); xfsprogs (mkfs.xfs) for FSTYPE=xfs
#   - Java 21+, benchmarks.jar already built (or build with mvn first)
#   - Sufficient disk space at IMG path (4 GB per run)
#
# dm-flakey TABLE STRING SYNTAX:
#   "<start_sector> <num_sectors> flakey <real_dev> <offset> <up_interval> <down_interval> [<num_features> <features...>]"
#   UP (pass-through, always up):
#     "0 $SECTORS flakey $LOOP 0 180 0"
#     → up_interval=180s, down_interval=0s, no features → always in up phase
#   DOWN + drop_writes (always down, discard all writes):
#     "0 $SECTORS flakey $LOOP 0 0 180 1 drop_writes"
#     → up_interval=0s, down_interval=180s, 1 feature: drop_writes → always in down phase,
#       all writes silently discarded (reads return stale data)
#
# COMMIT MODES (parameterized — COMMIT_MODE ∈ {adaptive, SYNC, NOSYNC}, plus W for adaptive):
#   adaptive: the WAL path (CommitMode.ADAPTIVE). CrashVerifier runs the production recovery triple
#             (RecoveryCoordinator.recover → notifyWalTxnRepublisher → drainWalQueue) on reopen and
#             asserts the SP-D4 oracle against (C=committed seqTxn, Wm=localDurableSeqTxn):
#               W=0   → expect DURABLE (F >= C — zero loss, adaptive == SYNC)
#               W=50ms→ expect RPO_OK  (F >= Wm — every ACKED txn survives; unflushed loss bounded to
#                                       (Wm,C], RPO <= W). A missing ACKED txn (F < Wm) = DURABILITY_FAILURE.
#   SYNC / NOSYNC: the NON-WAL (bypass wal) path (the original harness / regression guard).
#
# INTERPRETATION:
#   adaptive + verifier prints DURABLE (W=0) or RPO_OK (W>0) → PASS (durable-ack contract upheld)
#   adaptive + DURABILITY_FAILURE / SILENT_CORRUPTION        → serious (acked txn lost / corruption)
#   SYNC + count >= COMMITTED  → DURABLE: fsync'd data survived the power cut (expected)
#   SYNC + count <  COMMITTED  → DURABILITY_FAILURE: SYNC-committed rows were LOST (serious!)
#   NOSYNC + any result        → informational; loss or corruption is expected and normal

set -euo pipefail

# ============================================================
# CONFIGURATION — adjust paths for your environment
# ============================================================
# Under `sudo`, $HOME is /root — derive the invoking user's home from SUDO_USER instead.
WT="${WT:-/home/${SUDO_USER:-$(id -un)}/claude/wt/oss/adaptive-commit}"
JAR="${JAR:-$WT/benchmarks/target/benchmarks.jar}"
# Prefer the lean exploded classpath: the FunctionFactory scan hits the ~10MB core jar (~0.5s) instead
# of the 44MB shaded fat jar (~2min per JVM start). Falls back to the self-contained fat jar if the
# classpath file is absent. Build it with: mvn -q -pl benchmarks dependency:build-classpath \
#   -Dmdep.outputFile="$PWD/benchmarks/target/bench-cp.txt"
if [ -d "$WT/benchmarks/target/classes" ] && [ -s "$WT/benchmarks/target/bench-cp.txt" ]; then
    CP="$WT/benchmarks/target/classes:$(cat "$WT/benchmarks/target/bench-cp.txt")"
else
    CP="$JAR"
fi
# QuestDB needs these JVM flags on JDK 21+ (same set as core/pom.xml argLine).
# WITHOUT --add-exports ...jdk.internal.vm=ALL-UNNAMED the worker continuation class
# fails to init and QuestDB runs DEGRADED (workers dead) — invalidating the test.
QDB_JVM="--enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.time.zone=ALL-UNNAMED --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"
# Every run owns uniquely named destructive resources. User overrides are allowed, but collisions are
# rejected rather than cleaned up: this script must never remove another run's mapper/image/mount.
RUN_ID="${RUN_ID:-$$}"
[[ "$RUN_ID" =~ ^[A-Za-z0-9_.-]+$ ]] || { echo "ERROR: invalid RUN_ID '$RUN_ID'" >&2; exit 1; }
if [ -d /data ] && [ -w /data ]; then
    IMG="${IMG:-/data/qdb-pcut-$RUN_ID.img}"
else
    IMG="${IMG:-$HOME/qdb-pcut-$RUN_ID.img}"
fi
MNT="${MNT:-/mnt/qdbpcut-$RUN_ID}"
DM_NAME="${DM_NAME:-qdbflakey-$RUN_ID}"
DM_DEV="/dev/mapper/$DM_NAME"

# Filesystem under test: ext4 (default) or xfs. Pass as the FIRST ARG so it survives
# sudo's env sanitizing:  sudo bash power-cut-dmflakey.sh xfs   (env FSTYPE= also works).
FSTYPE="${1:-${FSTYPE:-ext4}}"
case "$FSTYPE" in ext4|xfs) ;; *) echo "ERROR: FSTYPE must be ext4 or xfs (got '$FSTYPE')" >&2; exit 1 ;; esac
mkfs_dev() {  # $1 = device; format with the chosen FSTYPE
    case "$FSTYPE" in
        ext4) mkfs.ext4 -F -q "$1" >/dev/null 2>&1 ;;
        xfs)  mkfs.xfs -f "$1" >/dev/null 2>&1 ;;
        *) echo "ERROR: unknown FSTYPE=$FSTYPE (use ext4 or xfs)" >&2; exit 1 ;;
    esac
}

# Minimum rows committed before we trigger the cut (at least 50 commits × 1000 rows).
MIN_COMMITTED="${MIN_COMMITTED:-50000}"

# ============================================================
# ROOT CHECK
# ============================================================
if [ "$(id -u)" -ne 0 ]; then
    echo "ERROR: This script must run as root (dm-flakey, losetup, mount require root)." >&2
    echo "       Run: sudo bash $0" >&2
    exit 1
fi

# ============================================================
# CLEANUP TRAP
# ============================================================
LOOP=""
HAS_IMAGE=0
HAS_LOOP=0
HAS_DM=0
HAS_MOUNT=0
HAS_MNT_DIR=0
cleanup_cycle() {
    if [ -n "${WRITER_PID:-}" ] && kill -0 "$WRITER_PID" 2>/dev/null; then
        kill -9 "$WRITER_PID" 2>/dev/null || true
        wait "$WRITER_PID" 2>/dev/null || true
    fi
    WRITER_PID=""
    if [ "$HAS_MOUNT" -eq 1 ]; then umount -l "$MNT" 2>/dev/null || true; HAS_MOUNT=0; fi
    if [ "$HAS_DM" -eq 1 ]; then dmsetup remove "$DM_NAME" 2>/dev/null || true; HAS_DM=0; fi
    if [ "$HAS_LOOP" -eq 1 ] && [ -n "$LOOP" ]; then losetup -d "$LOOP" 2>/dev/null || true; HAS_LOOP=0; LOOP=""; fi
    if [ "$HAS_IMAGE" -eq 1 ]; then rm -f "$IMG" 2>/dev/null || true; HAS_IMAGE=0; fi
    if [ "$HAS_MNT_DIR" -eq 1 ]; then rmdir "$MNT" 2>/dev/null || true; HAS_MNT_DIR=0; fi
}
cleanup() { local rc=$?; cleanup_cycle; exit "$rc"; }
trap cleanup EXIT

reject_resource_collisions() {
    [ ! -e "$IMG" ] || { echo "ERROR: image already exists: $IMG" >&2; return 1; }
    ! dmsetup info "$DM_NAME" &>/dev/null || { echo "ERROR: mapper already exists: $DM_NAME" >&2; return 1; }
    ! mountpoint -q "$MNT" 2>/dev/null || { echo "ERROR: mountpoint already mounted: $MNT" >&2; return 1; }
    if [ -d "$MNT" ] && [ -n "$(find "$MNT" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]; then
        echo "ERROR: mount directory is not empty: $MNT" >&2
        return 1
    fi
}

create_cycle_resources() {
    local size="$1"
    reject_resource_collisions
    truncate -s "$size" "$IMG"; HAS_IMAGE=1
    LOOP=$(losetup -f --show "$IMG"); HAS_LOOP=1
    SECTORS=$(blockdev --getsz "$LOOP")
    mkdir -p "$MNT"; HAS_MNT_DIR=1
}

# ============================================================
# HELPERS
# ============================================================

# Ensure dm-flakey kernel module is loaded
ensure_dmflakey() {
    if ! lsmod | grep -q '^dm_flakey'; then
        echo "  loading dm-flakey kernel module..."
        modprobe dm-flakey || {
            echo "ERROR: could not load dm-flakey module." >&2
            echo "       Ensure your kernel has CONFIG_DM_FLAKEY=m or =y." >&2
            exit 1
        }
    fi
}

# Poll _progress until value >= threshold or timeout.
# Usage: wait_progress <file> <threshold> <timeout_secs>
# Prints the value read and returns 0 on success, 1 on timeout.
wait_progress() {
    local pfile="$1" threshold="$2" timeout_secs="$3"
    local waited=0 interval=0.5
    local max_polls=$(( timeout_secs * 2 ))
    local polls=0
    while true; do
        if [ -f "$pfile" ]; then
            local val
            # First line of _progress is the bare committed row count in ALL modes (adaptive appends
            # C=/Wm= lines below it), so head -1 works for both formats.
            val=$(head -1 "$pfile" 2>/dev/null || echo 0)
            if [ "${val:-0}" -ge "$threshold" ] 2>/dev/null; then
                echo "$val"
                return 0
            fi
        fi
        polls=$((polls + 1))
        if [ "$polls" -ge "$max_polls" ]; then
            echo "0"
            return 1
        fi
        sleep "$interval"
    done
}

# ============================================================
# run_one MODE — run a single power-cut cycle for given commit mode
# ============================================================
WRITER_PID=""

run_one() {
    local MODE="$1"
    local W="${2:-0}"          # cairo.adaptive.commit.group.window (only meaningful for adaptive; 0 for SYNC/NOSYNC)
    local BATCHED="${3:-true}"
    echo ""
    echo "======================================================"
    echo "  POWER-CUT CYCLE: commitMode=$MODE  W(group.window.us)=$W  batchedColumnSync=$BATCHED"
    echo "======================================================"

    # ---- 1/2. Create uniquely owned sparse image and loop device ----
    echo "  [1] creating 4G sparse image: $IMG"
    create_cycle_resources 4G
    echo "      LOOP=$LOOP  SECTORS=$SECTORS"

    # ---- 3. Create dm-flakey in UP (pass-through) mode ----
    # Table: "0 <sectors> flakey <dev> <offset> <up_interval> <down_interval>"
    # up_interval=180, down_interval=0 → always in up phase (pass-through)
    local UP_TABLE="0 $SECTORS flakey $LOOP 0 180 0"
    echo "  [3] creating dm-flakey (always UP / pass-through)"
    echo "      table: $UP_TABLE"
    dmsetup create "$DM_NAME" --table "$UP_TABLE"
    HAS_DM=1

    # ---- 4. Format and mount ----
    echo "  [4] mkfs.$FSTYPE on $DM_DEV"
    mkfs_dev "$DM_DEV"
    echo "  [4] mounting $DM_DEV → $MNT"
    mount "$DM_DEV" "$MNT"
    HAS_MOUNT=1

    # ---- 5. Start CrashIngestWriter ----
    local DBDIR="$MNT/db"
    mkdir -p "$DBDIR"
    echo "  [5] starting CrashIngestWriter (commitMode=$MODE) → $DBDIR"
    java $QDB_JVM -cp "$CP" \
        -DcommitMode="$MODE" \
        -Dgroup.window.us="$W" \
        -Dbatched="$BATCHED" \
        org.questdb.CrashIngestWriter "$DBDIR" \
        > "$DBDIR/../_writer.log" 2>&1 &
    WRITER_PID=$!
    echo "      writer PID=$WRITER_PID"

    # ---- 6. Wait for MIN_COMMITTED rows ----
    echo "  [6] waiting for _progress >= $MIN_COMMITTED rows (max 180s)..."
    local PROGRESS_FILE="$DBDIR/_progress"
    local PROGRESS_VAL
    if ! PROGRESS_VAL=$(wait_progress "$PROGRESS_FILE" "$MIN_COMMITTED" 180); then
        echo "  ERROR: writer did not reach $MIN_COMMITTED committed rows within 180s"
        cat "$DBDIR/../_writer.log" || true
        # cleanup trap will handle teardown
        exit 1
    fi
    echo "      _progress=$PROGRESS_VAL (>= $MIN_COMMITTED)"

    # Small extra sleep to let a few more commits land (we want the JVM to have committed
    # well ahead of the kill, so the COMMITTED value is stable and clearly > 0)
    sleep 2

    # ---- 7. Capture COMMITTED (must happen BEFORE kill + cut) ----
    # We read _progress NOW, while the page cache is still live and the file is intact.
    # After the power-cut, the file may or may not be on disk depending on commit mode.
    # We store COMMITTED in a shell variable so it survives regardless.
    local COMMITTED
    # First line = committed row count in all modes (adaptive appends C=/Wm= below); head -1 handles both.
    COMMITTED=$(head -1 "$PROGRESS_FILE" 2>/dev/null || echo 0)
    if ! [[ "$COMMITTED" =~ ^[1-9][0-9]*$ ]]; then
        echo "ERROR: invalid committed watermark '$COMMITTED'" >&2
        return 1
    fi
    echo "  [7] COMMITTED=$COMMITTED (captured before power cut)"

    # ---- 8. Kill the writer (page cache still intact) ----
    echo "  [8] kill -9 writer PID=$WRITER_PID (page cache still live)"
    kill -9 "$WRITER_PID" 2>/dev/null || true
    wait "$WRITER_PID" 2>/dev/null || true
    WRITER_PID=""
    echo "      writer gone"

    # ---- 9. THE POWER CUT ----
    # Switch dm-flakey to "always DOWN + drop_writes":
    #   Table: "0 <sectors> flakey <dev> <offset> 0 180 1 drop_writes"
    #   up_interval=0, down_interval=180 → always in down phase.
    #   Feature "drop_writes" → all writes are silently discarded (not failed).
    # Then unmount: the kernel's writeback of dirty pages hits dm-flakey which DROPS them.
    # Un-fsync'd data never reaches the loop device or image → exactly a power cut.
    local DROP_TABLE="0 $SECTORS flakey $LOOP 0 0 180 1 drop_writes"
    echo "  [9] --- POWER CUT ---"
    echo "      suspending dm-flakey (--nolockfs: do NOT sync the fs, else dirty data flushes before the cut)..."
    dmsetup suspend --nolockfs "$DM_NAME"
    echo "      loading drop_writes table: $DROP_TABLE"
    dmsetup load "$DM_NAME" --table "$DROP_TABLE"
    echo "      resuming (dm-flakey now drops all writes)..."
    dmsetup resume "$DM_NAME"
    echo "      umount $MNT (writeback is DROPPED — un-fsync'd pages lost)"
    umount "$MNT"
    HAS_MOUNT=0
    echo "      --- power cut complete ---"

    # ---- 10. Restore dm-flakey to UP mode and remount ----
    # Switch back to pass-through so the filesystem is readable.
    echo "  [10] restoring dm-flakey to UP (pass-through)..."
    dmsetup suspend "$DM_NAME"
    dmsetup load "$DM_NAME" --table "$UP_TABLE"
    dmsetup resume "$DM_NAME"
    echo "       remounting $DM_DEV → $MNT"
    mount "$DM_DEV" "$MNT"
    HAS_MOUNT=1

    # ---- 11. Verify ----
    echo ""
    echo "=== MODE=$MODE W=$W batched=$BATCHED committed_before_cut=$COMMITTED ==="
    local VERIFY_OUT VERIFY_EXIT
    VERIFY_EXIT=0
    # The verifier must be told the mode + W (mode is not stored on disk): adaptive runs the recovery
    # triple and the (C,Wm) oracle; SYNC/NOSYNC take the original bit-check path.
    VERIFY_OUT=$(java $QDB_JVM -cp "$CP" -DcommitMode="$MODE" -Dgroup.window.us="$W" \
        org.questdb.CrashVerifier "$DBDIR" 2>&1) || VERIFY_EXIT=$?
    echo "$VERIFY_OUT"
    if [ "$VERIFY_EXIT" -ne 0 ]; then
        echo "ERROR: verifier failed [exit=$VERIFY_EXIT]" >&2
        return 1
    fi

    # Extract count from verifier output
    local COUNT
    COUNT=$(echo "$VERIFY_OUT" | grep -oP '(?<=count=)\d+' | tail -1 || true)

    echo ""
    echo "--- INTERPRETATION (MODE=$MODE W=$W) ---"
    if [ "$MODE" = "adaptive" ]; then
        local EXPECT
        if [ "$W" -eq 0 ]; then EXPECT="DURABLE"; else EXPECT="RPO_OK"; fi
        if ! echo "$VERIFY_OUT" | grep -q "^$EXPECT"; then
            echo "ERROR: adaptive verifier did not produce exact expected verdict $EXPECT" >&2
            return 1
        fi
        echo "PASS: adaptive W=$W met its bar ($EXPECT)"
    elif [ "$MODE" = "SYNC" ]; then
        if ! echo "$VERIFY_OUT" | grep -q '^CONSISTENT' || ! [[ "$COUNT" =~ ^[0-9]+$ ]] || [ "$COUNT" -lt "$COMMITTED" ]; then
            echo "ERROR: SYNC durability failed (count=${COUNT:-unknown}, committed=$COMMITTED)" >&2
            return 1
        fi
        echo "DURABLE: SYNC committed data survived power cut"
    else
        if ! echo "$VERIFY_OUT" | grep -Eq '^(CONSISTENT|LOUD_FAILURE|SILENT_CORRUPTION)'; then
            echo "ERROR: NOSYNC verifier output is unparseable" >&2
            return 1
        fi
        echo "NOSYNC control result accepted; durability is not promised"
    fi

    # ---- 12. Teardown this iteration ----
    echo ""
    echo "  [12] tearing down iteration (mode=$MODE)..."
    cleanup_cycle
    echo "  [12] done"
}

# ============================================================
# prove_cut_drops_unsynced — deterministic, QuestDB-independent proof that the
#   power-cut model actually drops un-fsync'd data. Without this, a SYNC "survived"
#   result is meaningless (maybe the cut dropped nothing). A sync'd file MUST
#   survive; an un-sync'd file written immediately before the cut MUST vanish.
# ============================================================
prove_cut_drops_unsynced() {
    echo "======================================================"
    echo "  PREFLIGHT: proving the power cut actually DROPS un-fsync'd data"
    echo "  (control: sync'd file must survive; un-sync'd file must vanish)"
    echo "======================================================"
    create_cycle_resources 1G
    local UP_TABLE="0 $SECTORS flakey $LOOP 0 180 0"
    local DROP_TABLE="0 $SECTORS flakey $LOOP 0 0 180 1 drop_writes"
    dmsetup create "$DM_NAME" --table "$UP_TABLE"
    HAS_DM=1
    mkfs_dev "$DM_DEV"
    mount "$DM_DEV" "$MNT"
    HAS_MOUNT=1

    # Durable file: write then sync the WHOLE fs so data + dir entry are on the device.
    echo "DURABLE_MARKER_KEEPME" > "$MNT/durable.txt"
    sync

    # Un-sync'd data written immediately before the cut (no time for background writeback).
    echo "EPHEMERAL_MARKER_DROPME" > "$MNT/ephemeral.txt"
    dd if=/dev/zero of="$MNT/ephemeral.bin" bs=1M count=20 status=none 2>/dev/null || true

    # THE CUT (no sleep): drop_writes, umount (page-cache writeback DROPPED), restore, remount.
    # --nolockfs: suspend WITHOUT syncing the fs, else the dirty page cache flushes before the cut.
    dmsetup suspend --nolockfs "$DM_NAME"
    dmsetup load "$DM_NAME" --table "$DROP_TABLE"
    dmsetup resume "$DM_NAME"
    umount "$MNT"
    HAS_MOUNT=0
    dmsetup suspend "$DM_NAME"
    dmsetup load "$DM_NAME" --table "$UP_TABLE"
    dmsetup resume "$DM_NAME"
    mount "$DM_DEV" "$MNT"
    HAS_MOUNT=1

    local durable_ok=0 ephemeral_dropped=0
    if [ -f "$MNT/durable.txt" ] && grep -q DURABLE_MARKER_KEEPME "$MNT/durable.txt" 2>/dev/null; then durable_ok=1; fi
    if [ ! -f "$MNT/ephemeral.txt" ] || ! grep -q EPHEMERAL_MARKER_DROPME "$MNT/ephemeral.txt" 2>/dev/null; then ephemeral_dropped=1; fi

    echo "  sync'd durable.txt survived the cut: $([ $durable_ok = 1 ] && echo YES || echo 'NO  <-- PROBLEM')"
    echo "  un-sync'd ephemeral data dropped:    $([ $ephemeral_dropped = 1 ] && echo YES || echo 'NO  <-- PROBLEM')"
    if [ $durable_ok = 1 ] && [ $ephemeral_dropped = 1 ]; then
        echo "  ==> CUT VERIFIED: model keeps fsync'd data, drops un-fsync'd data."
        echo "      A SYNC 'DURABLE' result below is therefore CONCLUSIVE."
    else
        echo "  ==> CUT INEFFECTIVE on this stack" >&2
        cleanup_cycle
        return 1
    fi

    cleanup_cycle
    echo ""
    return 0
}

# ============================================================
# MAIN
# ============================================================
echo "======================================================"
echo "  QuestDB dm-flakey POWER-CUT DURABILITY HARNESS"
echo "  (harness #2 — simulates actual power loss)"
echo "======================================================"
echo "  WT:            $WT"
echo "  JAR:           $JAR"
echo "  IMG:           $IMG"
echo "  MNT:           $MNT"
echo "  MIN_COMMITTED: $MIN_COMMITTED"
echo "  DM device:     $DM_DEV"
echo "  FSTYPE:        $FSTYPE   <-- filesystem actually under test"
echo ""
echo "  Technique: dm-flakey drop_writes → umount writeback DROPPED"
echo "             = un-fsync'd page-cache data LOST (power cut model)"
echo "  adaptive W=0:    WAL fdatasync before commit returns → DURABLE (zero loss)"
echo "  adaptive W=50ms: batched WAL fdatasync → RPO_OK (every ACKED txn survives, RPO <= W)"
echo "  SYNC:  fsync before commit → data survives → DURABLE"
echo "  NOSYNC: no fsync → data lost → expected (shows why the durable modes matter)"
echo ""

# Complete non-destructive preflight before creating any resource.
[ "${EUID:-$(id -u)}" -eq 0 ] || { echo "ERROR: run as root" >&2; exit 1; }
for tool in dmsetup losetup blockdev mount umount mountpoint truncate find grep dd sync; do
    command -v "$tool" >/dev/null || { echo "ERROR: missing required tool: $tool" >&2; exit 1; }
done
IMG_PARENT=$(dirname "$IMG")
mkdir -p "$IMG_PARENT" || { echo "ERROR: cannot create image directory: $IMG_PARENT" >&2; exit 1; }
BACKING_FS=$(stat -f -c %T "$IMG_PARENT")
case "$BACKING_FS" in tmpfs|ramfs) echo "ERROR: image backing filesystem must be persistent, got $BACKING_FS" >&2; exit 1 ;; esac
reject_resource_collisions || exit 1

# Check JAR (do NOT build under root — maven would use /root/.m2 and re-download everything).
if [ ! -f "$JAR" ]; then
    echo "ERROR: benchmarks jar not found at: $JAR" >&2
    echo "Build it first AS YOUR NORMAL USER (not root):" >&2
    echo "  cd $WT && mvn install -pl core -am -DskipTests -q && mvn package -pl benchmarks -DskipTests -q" >&2
    echo "Then re-run this script with sudo." >&2
    exit 1
fi

ensure_dmflakey

# PREFLIGHT: prove the cut drops un-fsync'd data before trusting any durability result.
prove_cut_drops_unsynced || { echo "ERROR: power-cut preflight failed" >&2; exit 4; }

# Default run-set (run_one MODE W BATCHED):
#   1) adaptive W=0      — the zero-loss SUBJECT (adaptive == SYNC); expect DURABLE
#   2) adaptive W=50000  — the RPO SUBJECT (batched WAL fdatasync, 50ms); expect RPO_OK (acked survives)
#   3) SYNC batched on   — CONTROL / regression guard on the existing non-WAL path; expect DURABLE
#   4) SYNC batched off  — per-file msync(MS_SYNC) baseline
#   5) NOSYNC            — CONTROL; total loss expected (shows why the durable modes matter)
FAILURES=0
run_one adaptive 0     true  || { cleanup_cycle; FAILURES=$((FAILURES + 1)); }
run_one adaptive 50000 true  || { cleanup_cycle; FAILURES=$((FAILURES + 1)); }
run_one SYNC     0     true  || { cleanup_cycle; FAILURES=$((FAILURES + 1)); }
run_one SYNC     0     false || { cleanup_cycle; FAILURES=$((FAILURES + 1)); }
run_one NOSYNC   0     true  || { cleanup_cycle; FAILURES=$((FAILURES + 1)); }
if [ "$FAILURES" -ne 0 ]; then
    echo "ERROR: $FAILURES power-cut cycle(s) failed" >&2
    exit 5
fi

echo ""
echo "======================================================"
echo "  POWER-CUT HARNESS COMPLETE"
echo "======================================================"
echo "  adaptive W=0:     expected DURABLE (F >= C — zero loss, adaptive == SYNC)"
echo "  adaptive W=50ms:  expected RPO_OK  (F >= Wm — every acked txn survives, RPO <= W)"
echo "  SYNC:             expected DURABLE (count >= committed) — regression guard on the existing path"
echo "  NOSYNC:           expected data loss (count < committed, or corruption)"
echo "  See output above for per-mode INTERPRETATION lines."
echo ""
echo "  If SYNC shows DURABILITY_FAILURE, check:"
echo "    - Is the filesystem mounted with barrier=0 / nobarrier? (disables write ordering)"
echo "    - Is the loop device backed by a real disk (not tmpfs/ramfs)?"
echo "    - Is the block device itself a writeback cache without power-loss protection?"
echo "    - See QuestDB startup warnings about nobarrier mounts."
echo "======================================================"
