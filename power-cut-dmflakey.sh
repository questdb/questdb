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
#   - e2fsprogs (mkfs.ext4)
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
# INTERPRETATION:
#   SYNC + count >= COMMITTED  → DURABLE: fsync'd data survived the power cut (expected)
#   SYNC + count <  COMMITTED  → DURABILITY_FAILURE: SYNC-committed rows were LOST (serious!)
#   NOSYNC + any result        → informational; loss or corruption is expected and normal

set -euo pipefail

# ============================================================
# CONFIGURATION — adjust paths for your environment
# ============================================================
# Under `sudo`, $HOME is /root — derive the invoking user's home from SUDO_USER instead.
WT="${WT:-/home/${SUDO_USER:-$(id -un)}/claude/wt/oss/sync-batch}"
JAR="${JAR:-$WT/benchmarks/target/benchmarks.jar}"
# QuestDB needs these JVM flags on JDK 21+ (same set as core/pom.xml argLine).
# WITHOUT --add-exports ...jdk.internal.vm=ALL-UNNAMED the worker continuation class
# fails to init and QuestDB runs DEGRADED (workers dead) — invalidating the test.
QDB_JVM="--enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.time.zone=ALL-UNNAMED --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"
# IMG: the sparse image file that backs the loop device.
# Put this on a REAL block device (ext4 / xfs on a spinning disk or SSD) — NOT tmpfs.
# /data is preferred (real disk); fall back to HOME.
if [ -d /data ] && [ -w /data ]; then
    IMG="${IMG:-/data/qdb-pcut.img}"
else
    IMG="${IMG:-$HOME/qdb-pcut.img}"
fi
MNT="${MNT:-/mnt/qdbpcut}"
DM_NAME="qdbflakey"
DM_DEV="/dev/mapper/$DM_NAME"

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
cleanup() {
    local rc=$?
    echo "--- cleanup (exit code $rc) ---"
    # Kill any stray writer process
    if [ -n "${WRITER_PID:-}" ] && kill -0 "$WRITER_PID" 2>/dev/null; then
        kill -9 "$WRITER_PID" 2>/dev/null || true
        wait "$WRITER_PID" 2>/dev/null || true
    fi
    # Unmount
    if mountpoint -q "$MNT" 2>/dev/null; then
        umount -l "$MNT" 2>/dev/null || true
    fi
    # Remove dm device
    if dmsetup info "$DM_NAME" &>/dev/null; then
        dmsetup remove "$DM_NAME" 2>/dev/null || true
    fi
    # Detach loop device
    if [ -n "$LOOP" ] && losetup "$LOOP" &>/dev/null; then
        losetup -d "$LOOP" 2>/dev/null || true
    fi
    # Remove image
    rm -f "$IMG" 2>/dev/null || true
    echo "--- cleanup done ---"
}
trap cleanup EXIT

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
            val=$(cat "$pfile" 2>/dev/null || echo 0)
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
    local BATCHED="${2:-true}"
    echo ""
    echo "======================================================"
    echo "  POWER-CUT CYCLE: commitMode=$MODE  batchedColumnSync=$BATCHED"
    echo "======================================================"

    # ---- 1. Create 4 GB sparse image on a real disk ----
    echo "  [1] creating 4G sparse image: $IMG"
    # Remove any leftover from a previous failed run
    rm -f "$IMG"
    truncate -s 4G "$IMG"

    # ---- 2. Attach loop device ----
    echo "  [2] attaching loop device"
    LOOP=$(losetup -f --show "$IMG")
    echo "      LOOP=$LOOP"
    SECTORS=$(blockdev --getsz "$LOOP")
    echo "      SECTORS=$SECTORS"

    # ---- 3. Create dm-flakey in UP (pass-through) mode ----
    # Table: "0 <sectors> flakey <dev> <offset> <up_interval> <down_interval>"
    # up_interval=180, down_interval=0 → always in up phase (pass-through)
    local UP_TABLE="0 $SECTORS flakey $LOOP 0 180 0"
    echo "  [3] creating dm-flakey (always UP / pass-through)"
    echo "      table: $UP_TABLE"
    dmsetup create "$DM_NAME" --table "$UP_TABLE"

    # ---- 4. Format and mount ----
    echo "  [4] mkfs.ext4 on $DM_DEV"
    mkfs.ext4 -F -q "$DM_DEV"
    mkdir -p "$MNT"
    echo "  [4] mounting $DM_DEV → $MNT"
    mount "$DM_DEV" "$MNT"

    # ---- 5. Start CrashIngestWriter ----
    local DBDIR="$MNT/db"
    mkdir -p "$DBDIR"
    echo "  [5] starting CrashIngestWriter (commitMode=$MODE) → $DBDIR"
    java $QDB_JVM -cp "$JAR" \
        -DcommitMode="$MODE" \
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
    COMMITTED=$(cat "$PROGRESS_FILE" 2>/dev/null || echo 0)
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
    echo "      --- power cut complete ---"

    # ---- 10. Restore dm-flakey to UP mode and remount ----
    # Switch back to pass-through so the filesystem is readable.
    echo "  [10] restoring dm-flakey to UP (pass-through)..."
    dmsetup suspend "$DM_NAME"
    dmsetup load "$DM_NAME" --table "$UP_TABLE"
    dmsetup resume "$DM_NAME"
    echo "       remounting $DM_DEV → $MNT"
    mount "$DM_DEV" "$MNT"

    # ---- 11. Verify ----
    echo ""
    echo "=== MODE=$MODE batched=$BATCHED committed_before_cut=$COMMITTED ==="
    local VERIFY_OUT VERIFY_EXIT
    VERIFY_EXIT=0
    VERIFY_OUT=$(java $QDB_JVM -cp "$JAR" org.questdb.CrashVerifier "$DBDIR" 2>&1) || VERIFY_EXIT=$?
    echo "$VERIFY_OUT"

    # Extract count from verifier output
    local COUNT
    COUNT=$(echo "$VERIFY_OUT" | grep -oP '(?<=count=)\d+' | tail -1 || echo "unknown")

    echo ""
    echo "--- INTERPRETATION (MODE=$MODE) ---"
    if [ "$MODE" = "SYNC" ]; then
        if echo "$VERIFY_OUT" | grep -q '^CONSISTENT'; then
            if [ "$COUNT" != "unknown" ] && [ "$COUNT" -ge "$COMMITTED" ] 2>/dev/null; then
                echo "DURABLE: SYNC committed data survived power cut (count=$COUNT >= committed=$COMMITTED)"
            else
                # CONSISTENT but count < COMMITTED — partial loss
                echo "DURABILITY_FAILURE: SYNC-committed rows lost after power cut (count=$COUNT < committed=$COMMITTED)"
                echo "*** SERIOUS FINDING: SYNC mode is not delivering durability on this storage stack ***"
            fi
        elif echo "$VERIFY_OUT" | grep -q '^SILENT_CORRUPTION'; then
            echo "DURABILITY_FAILURE: SILENT_CORRUPTION after power cut (count=$COUNT, committed=$COMMITTED)"
            echo "*** SERIOUS FINDING: SYNC mode is not delivering durability on this storage stack ***"
        elif echo "$VERIFY_OUT" | grep -q '^LOUD_FAILURE'; then
            echo "LOUD_FAILURE: engine detected torn state (count=$COUNT, committed=$COMMITTED)"
            echo "    This may indicate un-fsync'd metadata survived but data did not — investigate."
            echo "    For SYNC mode a loud failure is unexpected; could be metadata journaling gap."
        else
            echo "UNKNOWN_RESULT: could not parse verifier output (exit=$VERIFY_EXIT)"
        fi
    else
        # NOSYNC — loss or corruption is expected; just report what happened
        if echo "$VERIFY_OUT" | grep -q '^CONSISTENT'; then
            echo "(NOSYNC: CONSISTENT count=$COUNT committed=$COMMITTED — this iteration the data happened to survive)"
            echo "(NOSYNC: loss is not promised but not guaranteed either; rerun to see variability)"
        else
            echo "(NOSYNC: data lost or corrupt after power cut — count=$COUNT, committed=$COMMITTED)"
        fi
        echo "(NOSYNC: loss is expected — durability not promised without SYNC mode)"
    fi

    # ---- 12. Teardown this iteration ----
    echo ""
    echo "  [12] tearing down iteration (mode=$MODE)..."
    umount "$MNT" 2>/dev/null || true
    dmsetup remove "$DM_NAME" 2>/dev/null || true
    losetup -d "$LOOP" 2>/dev/null || true
    LOOP=""
    rm -f "$IMG"
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
    rm -f "$IMG"
    truncate -s 1G "$IMG"
    LOOP=$(losetup -f --show "$IMG")
    SECTORS=$(blockdev --getsz "$LOOP")
    local UP_TABLE="0 $SECTORS flakey $LOOP 0 180 0"
    local DROP_TABLE="0 $SECTORS flakey $LOOP 0 0 180 1 drop_writes"
    dmsetup create "$DM_NAME" --table "$UP_TABLE"
    mkfs.ext4 -F -q "$DM_DEV"
    mkdir -p "$MNT"
    mount "$DM_DEV" "$MNT"

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
    dmsetup suspend "$DM_NAME"
    dmsetup load "$DM_NAME" --table "$UP_TABLE"
    dmsetup resume "$DM_NAME"
    mount "$DM_DEV" "$MNT"

    local durable_ok=0 ephemeral_dropped=0
    if [ -f "$MNT/durable.txt" ] && grep -q DURABLE_MARKER_KEEPME "$MNT/durable.txt" 2>/dev/null; then durable_ok=1; fi
    if [ ! -f "$MNT/ephemeral.txt" ] || ! grep -q EPHEMERAL_MARKER_DROPME "$MNT/ephemeral.txt" 2>/dev/null; then ephemeral_dropped=1; fi

    echo "  sync'd durable.txt survived the cut: $([ $durable_ok = 1 ] && echo YES || echo 'NO  <-- PROBLEM')"
    echo "  un-sync'd ephemeral data dropped:    $([ $ephemeral_dropped = 1 ] && echo YES || echo 'NO  <-- PROBLEM')"
    if [ $durable_ok = 1 ] && [ $ephemeral_dropped = 1 ]; then
        echo "  ==> CUT VERIFIED: model keeps fsync'd data, drops un-fsync'd data."
        echo "      A SYNC 'DURABLE' result below is therefore CONCLUSIVE."
    else
        echo "  ==> CUT INEFFECTIVE on this stack: the cut is NOT dropping un-fsync'd data,"
        echo "      so any SYNC 'survival' below is INCONCLUSIVE. Investigate the dm-flakey setup."
    fi

    umount "$MNT" 2>/dev/null || true
    dmsetup remove "$DM_NAME" 2>/dev/null || true
    losetup -d "$LOOP" 2>/dev/null || true
    LOOP=""
    rm -f "$IMG"
    echo ""
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
echo ""
echo "  Technique: dm-flakey drop_writes → umount writeback DROPPED"
echo "             = un-fsync'd page-cache data LOST (power cut model)"
echo "  SYNC:  fsync before commit → data survives → DURABLE"
echo "  NOSYNC: no fsync → data lost → expected (shows why SYNC matters)"
echo ""

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
prove_cut_drops_unsynced

# Attribution run:
#   1) SYNC + batched flush optimization (the path that showed SILENT_CORRUPTION)
#   2) SYNC + batched OFF = per-file msync(MS_SYNC) baseline (does the proven path survive?)
#   3) NOSYNC for reference (total loss expected)
run_one SYNC  true
run_one SYNC  false
run_one NOSYNC true

echo ""
echo "======================================================"
echo "  POWER-CUT HARNESS COMPLETE"
echo "======================================================"
echo "  SYNC:   expected DURABLE (count >= committed)"
echo "  NOSYNC: expected data loss (count < committed, or corruption)"
echo "  See output above for per-mode INTERPRETATION lines."
echo ""
echo "  If SYNC shows DURABILITY_FAILURE, check:"
echo "    - Is the filesystem mounted with barrier=0 / nobarrier? (disables write ordering)"
echo "    - Is the loop device backed by a real disk (not tmpfs/ramfs)?"
echo "    - Is the block device itself a writeback cache without power-loss protection?"
echo "    - See QuestDB startup warnings about nobarrier mounts."
echo "======================================================"
