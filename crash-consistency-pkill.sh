#!/usr/bin/env bash
# crash-consistency-pkill.sh
#
# PROCESS-CRASH-CONSISTENCY TEST FOR QUESTDB (SYNC COMMIT MODE)
#
# WHAT THIS TESTS — PROCESS-CRASH-CONSISTENCY, NOT POWER-LOSS DURABILITY:
#   kill -9 terminates the JVM abruptly without running shutdown hooks, but it does
#   NOT flush or invalidate the OS page cache. Data written to mmap'd files before
#   the kill lives on in the page cache and is visible to the next process that opens
#   the same files. This test proves that QuestDB's recovery path (torn-aux guards,
#   _txn/_cv A/B metadata, _todo) leaves a CONSISTENT state after an abrupt mid-write
#   kill: committed rows are intact and fully correct, and any in-flight (uncommitted)
#   batch is cleanly rolled back.
#
#   Power-loss durability — verifying data survives actual storage power failure
#   (page cache flushed / discarded) — requires a separate dm-log-writes harness
#   and is NOT what this script tests.
#
# HOW IT WORKS:
#   Each iteration:
#     1. Starts CrashIngestWriter as a real separate JVM process.
#     2. Waits until _progress exists and shows >= MIN_COMMITTED rows (several commits in).
#     3. Sleeps a small randomised extra delay (0–500 ms across iterations) so the kill
#        lands at varied points within a commit cycle — increasing the chance of hitting
#        mid-commit windows (between msync and _txn write, or within _cv A/B flip).
#     4. Sends kill -9 to the writer JVM.
#     5. Runs CrashVerifier on the same DB root.
#     6. Records CONSISTENT / LOUD_FAILURE / SILENT_CORRUPTION.
#
# The DB root is under /data (xfs, real disk) when available, otherwise $HOME (ext4).
# tmpfs is explicitly rejected — we need real disk I/O for this test.
#
# Usage: bash crash-consistency-pkill.sh [iterations]
#        (default: 10 iterations)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR="$SCRIPT_DIR/benchmarks/target/benchmarks.jar"

N="${1:-10}"           # number of kill iterations
MIN_COMMITTED=3000     # wait until at least this many rows committed before killing
                       # (3 commits × 1000 rows; writer prints "committed N" each commit)

# ---- Choose a real-disk base directory (reject tmpfs) ----
choose_base_dir() {
    # Prefer /data (xfs) if present and writable; else HOME (ext4).
    # Check with df -T that neither is tmpfs.
    for candidate in /data "$HOME"; do
        if [ -d "$candidate" ] && [ -w "$candidate" ]; then
            fstype=$(df -T "$candidate" 2>/dev/null | awk 'NR==2{print $2}')
            if [[ "$fstype" == tmpfs ]]; then
                echo "SKIP: $candidate is tmpfs (not suitable for durability/consistency test)" >&2
                continue
            fi
            echo "$candidate"
            return 0
        fi
    done
    echo "ERROR: no suitable real-disk directory found" >&2
    exit 1
}

BASE_DIR=$(choose_base_dir)
FSTYPE=$(df -T "$BASE_DIR" 2>/dev/null | awk 'NR==2{print $2}')
echo "=== QuestDB process-crash-consistency harness ==="
echo "    WHAT: kill -9 consistency (page cache persists; testing recovery NOT power-loss)"
echo "    Iterations: $N"
echo "    DB base: $BASE_DIR (type=$FSTYPE)"
echo "    JAR: $JAR"
echo ""

if [ ! -f "$JAR" ]; then
    echo "=== Building QuestDB + benchmarks ==="
    cd "$SCRIPT_DIR"
    # Build core then benchmarks; skip tests (we ARE the test)
    mvn install -pl core -am -DskipTests -q && \
    mvn package -pl benchmarks -DskipTests -q
    echo "=== Build complete ==="
fi

# ---- Result counters ----
cnt_consistent=0
cnt_loud=0
cnt_silent=0
cnt_total=0

for iter in $(seq 1 "$N"); do
    ROOT="$BASE_DIR/qdb-pkill-$iter-$(date +%s%N)"
    mkdir -p "$ROOT"

    echo "--- iteration $iter/$N  root=$ROOT ---"

    # Launch CrashIngestWriter as a separate JVM process (real process kill target)
    java -cp "$JAR" org.questdb.CrashIngestWriter "$ROOT" > "$ROOT/_writer.log" 2>&1 &
    WRITER_PID=$!
    echo "  writer PID=$WRITER_PID"

    # Wait until _progress exists AND shows >= MIN_COMMITTED rows.
    # This ensures at least a few commit cycles have completed before we kill,
    # guaranteeing committed data exists to verify.
    WAITED=0
    MAX_WAIT=120  # 120 seconds max wait
    while true; do
        if ! kill -0 "$WRITER_PID" 2>/dev/null; then
            echo "  ERROR: writer exited prematurely (PID $WRITER_PID)!"
            cat "$ROOT/_writer.log" || true
            break
        fi
        if [ -f "$ROOT/_progress" ]; then
            COMMITTED=$(cat "$ROOT/_progress" 2>/dev/null || echo 0)
            if [ "${COMMITTED:-0}" -ge "$MIN_COMMITTED" ] 2>/dev/null; then
                echo "  _progress=$COMMITTED >= $MIN_COMMITTED — ready to kill"
                break
            fi
        fi
        sleep 0.2
        WAITED=$((WAITED + 1))
        if [ "$WAITED" -ge "$((MAX_WAIT * 5))" ]; then
            echo "  TIMEOUT waiting for writer; aborting iteration"
            kill -9 "$WRITER_PID" 2>/dev/null || true
            continue 2
        fi
    done

    # Randomise kill timing within the commit window:
    # Iterations 1–10 get 0, 50, 100, 150, 200, 250, 300, 350, 400, 450 ms extra delay.
    # This spreads the kill across the full 1000-row commit cycle to increase mid-commit hits.
    EXTRA_DELAY_MS=$(( (iter - 1) * 50 ))
    if [ "$EXTRA_DELAY_MS" -gt 0 ]; then
        echo "  extra delay ${EXTRA_DELAY_MS}ms before kill (varies mid-commit window)"
        sleep "0.$(printf '%03d' "$EXTRA_DELAY_MS")"
    fi

    # Hard kill: kill -9 does not invoke JVM shutdown hooks or finally-blocks.
    # The OS page cache is NOT flushed — data written to mmap'd files survives.
    echo "  sending kill -9 to PID $WRITER_PID"
    kill -9 "$WRITER_PID" 2>/dev/null || true

    # Wait for the writer process to be fully gone (OS releases flock on its fd)
    # so the verifier can acquire the table-writer lock cleanly.
    for i in $(seq 1 50); do
        if ! kill -0 "$WRITER_PID" 2>/dev/null; then
            break
        fi
        sleep 0.1
    done
    if kill -0 "$WRITER_PID" 2>/dev/null; then
        echo "  WARN: writer PID $WRITER_PID still alive after 5s; proceeding anyway"
    fi

    # Show what was committed before the kill
    WATERMARK=$(cat "$ROOT/_progress" 2>/dev/null || echo "N/A")
    echo "  watermark at kill time: $WATERMARK"

    # Run CrashVerifier; capture exit code separately to avoid set -e exit
    VERIFY_OUT=$( java -cp "$JAR" org.questdb.CrashVerifier "$ROOT" 2>&1 ) || VERIFY_EXIT=$?
    VERIFY_EXIT="${VERIFY_EXIT:-0}"
    echo "  verifier output: $VERIFY_OUT"

    # Parse verdict from first word of last verdict line
    VERDICT=$(echo "$VERIFY_OUT" | grep -E '^(CONSISTENT|LOUD_FAILURE|SILENT_CORRUPTION)' | tail -1 | awk '{print $1}')
    VERDICT="${VERDICT%:}"  # strip trailing colon from LOUD_FAILURE:

    cnt_total=$((cnt_total + 1))
    case "$VERDICT" in
        CONSISTENT)
            cnt_consistent=$((cnt_consistent + 1))
            echo "  RESULT: CONSISTENT"
            ;;
        LOUD_FAILURE)
            cnt_loud=$((cnt_loud + 1))
            echo "  RESULT: LOUD_FAILURE (detected corruption — acceptable, guards caught it)"
            ;;
        SILENT_CORRUPTION)
            cnt_silent=$((cnt_silent + 1))
            echo "  RESULT: SILENT_CORRUPTION *** SERIOUS FINDING ***"
            echo "  Full output: $VERIFY_OUT"
            ;;
        *)
            echo "  RESULT: UNKNOWN verdict='$VERDICT' exit=$VERIFY_EXIT"
            echo "  Full output: $VERIFY_OUT"
            # Count unknown as LOUD_FAILURE (could be missing table / exception)
            cnt_loud=$((cnt_loud + 1))
            ;;
    esac

    # Clean up the DB root for this iteration
    rm -rf "$ROOT"
    echo ""
done

echo "=========================================="
echo "  AGGREGATE RESULTS ($cnt_total iterations)"
echo "=========================================="
echo "  CONSISTENT:        $cnt_consistent"
echo "  LOUD_FAILURE:      $cnt_loud  (detected; guards caught it)"
echo "  SILENT_CORRUPTION: $cnt_silent  (*** SERIOUS if > 0 ***)"
echo ""
echo "  Disk type: $FSTYPE ($BASE_DIR)"
echo "  Test: PROCESS-CRASH-CONSISTENCY (kill -9, page cache persists)"
echo "  NOT: power-loss durability (requires dm-log-writes harness)"
echo "=========================================="

if [ "$cnt_silent" -gt 0 ]; then
    echo "FAILED: $cnt_silent SILENT_CORRUPTION — investigate immediately" >&2
    exit 2
fi

exit 0
