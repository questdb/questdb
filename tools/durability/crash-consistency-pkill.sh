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
# QuestDB needs these JVM flags on JDK 21+ (same set as core/pom.xml argLine); without
# --add-exports ...jdk.internal.vm=ALL-UNNAMED the worker continuation class fails to init
# and QuestDB runs DEGRADED (workers dead), invalidating the test.
QDB_JVM="--enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.time.zone=ALL-UNNAMED --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"

N="${1:-10}"           # number of kill iterations
if [ "$N" = "--validate-iterations" ]; then
    value="${2:-}"
    [[ "$value" =~ ^[1-9][0-9]*$ ]] || { echo "ERROR: iterations must be a positive integer" >&2; exit 1; }
    exit 0
fi
if [ "$#" -gt 1 ] || ! [[ "$N" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: iterations must be a positive integer" >&2
    exit 1
fi
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
cnt_harness=0

for ((iter = 1; iter <= N; iter++)); do
    ROOT="$BASE_DIR/qdb-pkill-$iter-$(date +%s%N)"
    mkdir -p "$ROOT"
    echo "--- iteration $iter/$N  root=$ROOT ---"

    java $QDB_JVM -cp "$JAR" org.questdb.CrashIngestWriter "$ROOT" > "$ROOT/_writer.log" 2>&1 &
    WRITER_PID=$!
    echo "  writer PID=$WRITER_PID"
    WAITED=0
    MAX_WAIT=120
    ITER_VALID=1
    while true; do
        if ! kill -0 "$WRITER_PID" 2>/dev/null; then
            echo "  ERROR: writer exited prematurely (PID $WRITER_PID)!" >&2
            cat "$ROOT/_writer.log" || true
            ITER_VALID=0
            break
        fi
        if [ -f "$ROOT/_progress" ]; then
            COMMITTED=$(head -1 "$ROOT/_progress" 2>/dev/null || echo 0)
            if [[ "$COMMITTED" =~ ^[1-9][0-9]*$ ]] && [ "$COMMITTED" -ge "$MIN_COMMITTED" ]; then
                echo "  _progress=$COMMITTED >= $MIN_COMMITTED — ready to kill"
                break
            fi
        fi
        sleep 0.2
        WAITED=$((WAITED + 1))
        if [ "$WAITED" -ge "$((MAX_WAIT * 5))" ]; then
            echo "  ERROR: timeout waiting for writer" >&2
            kill -9 "$WRITER_PID" 2>/dev/null || true
            wait "$WRITER_PID" 2>/dev/null || true
            ITER_VALID=0
            break
        fi
    done
    if [ "$ITER_VALID" -eq 0 ]; then
        cnt_harness=$((cnt_harness + 1))
        echo "  retained failed iteration at $ROOT" >&2
        continue
    fi

    EXTRA_DELAY_MS=$(( (iter - 1) * 50 ))
    [ "$EXTRA_DELAY_MS" -eq 0 ] || sleep "0.$(printf '%03d' "$EXTRA_DELAY_MS")"
    echo "  sending kill -9 to PID $WRITER_PID"
    kill -9 "$WRITER_PID" 2>/dev/null || true
    for ((i = 0; i < 50; i++)); do
        kill -0 "$WRITER_PID" 2>/dev/null || break
        sleep 0.1
    done
    if kill -0 "$WRITER_PID" 2>/dev/null; then
        echo "  ERROR: writer still alive after kill deadline" >&2
        cnt_harness=$((cnt_harness + 1))
        echo "  retained failed iteration at $ROOT" >&2
        continue
    fi
    wait "$WRITER_PID" 2>/dev/null || true

    VERIFY_EXIT=0
    VERIFY_OUT=$(java $QDB_JVM -cp "$JAR" org.questdb.CrashVerifier "$ROOT" 2>&1) || VERIFY_EXIT=$?
    echo "  verifier output: $VERIFY_OUT"
    VERDICT=$(printf '%s\n' "$VERIFY_OUT" | grep -E '^(CONSISTENT|LOUD_FAILURE|SILENT_CORRUPTION)' | tail -1 | awk '{print $1}' || true)
    VERDICT="${VERDICT%:}"
    if [ "$VERIFY_EXIT" -ne 0 ] || [ -z "$VERDICT" ]; then
        echo "  ERROR: invalid verifier result [exit=$VERIFY_EXIT, verdict=$VERDICT]" >&2
        cnt_harness=$((cnt_harness + 1))
        echo "  retained failed iteration at $ROOT" >&2
        continue
    fi

    cnt_total=$((cnt_total + 1))
    case "$VERDICT" in
        CONSISTENT) cnt_consistent=$((cnt_consistent + 1)); echo "  RESULT: CONSISTENT" ;;
        LOUD_FAILURE) cnt_loud=$((cnt_loud + 1)); echo "  RESULT: LOUD_FAILURE (guarded failure)" ;;
        SILENT_CORRUPTION) cnt_silent=$((cnt_silent + 1)); echo "  RESULT: SILENT_CORRUPTION" ;;
        *) cnt_harness=$((cnt_harness + 1)); echo "  retained failed iteration at $ROOT" >&2; continue ;;
    esac
    rm -rf "$ROOT"
done

echo "=========================================="
echo "  AGGREGATE RESULTS ($cnt_total/$N valid iterations)"
echo "  CONSISTENT: $cnt_consistent  LOUD_FAILURE: $cnt_loud"
echo "  SILENT_CORRUPTION: $cnt_silent  HARNESS_FAILURE: $cnt_harness"
echo "=========================================="
if [ "$cnt_silent" -gt 0 ]; then exit 2; fi
if [ "$cnt_harness" -gt 0 ] || [ "$cnt_total" -ne "$N" ]; then exit 3; fi
exit 0
