#!/usr/bin/env bash
# run-fuzz.sh [iterations] [mode] [window_us]
#
# Randomised power-cut fuzz: many iterations, each cutting at a DIFFERENT
# randomly drawn wall-clock moment, every one checked by the same oracle.
#
# THIS IS THE E2E INSTRUMENT. The division of labour with the Java suite:
#
#   Java (AbstractAdaptiveCrashSweepTest#forEachAdaptiveCrashPoint)
#       ENUMERATES crash points -- every durability op of a commit phase --
#       against a MODELLED filesystem. Exhaustive, deterministic, fast, and only
#       as faithful as CrashFaultFilesFacade.
#
#   Here
#       SAMPLES crash moments at random against a REAL kernel, a REAL block
#       layer and a REAL page cache. Not exhaustive, but nothing is modelled,
#       and coverage accumulates across iterations and across runs.
#
# A single fixed cut would prove very little, and that is measured rather than
# assumed: cutting late, after the guest kernel has written most dirty pages
# back, even NOSYNC loses nothing (count==watermark at 312k/432k/974k rows).
# Sampling the delay -- including very EARLY cuts, before the first commit --
# is what gives this harness its reach.
#
# Every iteration prints and logs its SEED. A failure is replayable exactly:
#   bash power-cut-vm.sh --arm=reference --mode=adaptive --seed=<SEED>
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/qemu.sh
source "$HERE/lib/qemu.sh"
# shellcheck source=lib/verdict.sh
source "$HERE/lib/verdict.sh"
# shellcheck source=lib/preflight.sh
source "$HERE/lib/preflight.sh"

bash "$HERE/check-host.sh" >/dev/null || { bash "$HERE/check-host.sh"; exit 1; }

ITERATIONS="${1:-10}"
MODE="${2:-adaptive}"
WINDOW="${3:-0}"
ARM="${QDB_FUZZ_ARM:-reference}"

STATE_DIR="${QDB_VMCRASH_STATE:-/data/qdb-vmcrash}"
LOG="$STATE_DIR/fuzz.log"
mkdir -p "$STATE_DIR"
STAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)

echo "randomised power-cut fuzz — $STAMP"
echo "  mode=$MODE W=$WINDOW arm=$ARM iterations=$ITERATIONS"

# The standing gate, once per run: prove the cut still discards un-flushed
# device writes before trusting any verdict below it. test/t04 proves this
# check can fail, which is what stops a green fuzz run from being vacuous.
pf=$(run_preflight_cycle real "$HERE")
echo "$STAMP fuzz preflight verdict=$pf" >> "$LOG"
if [ "$pf" != "PREFLIGHT_OK" ]; then
    echo "ABORT: preflight returned $pf — the cut is not cutting" >&2
    exit 2
fi
echo "  preflight -> $pf"

pass=0
nocommit=0
for i in $(seq 1 "$ITERATIONS"); do
    seed=$(od -An -N4 -tu4 < /dev/urandom | tr -dc '0-9')
    line=$(bash "$HERE/power-cut-vm.sh" --arm="$ARM" --mode="$MODE" \
             --window-us="$WINDOW" --seed="$seed" 2>/dev/null | tail -1)
    v=$(verdict_classify "$line")
    echo "$STAMP fuzz iter=$i/$ITERATIONS mode=$MODE W=$WINDOW seed=$seed verdict=$v line=$line" >> "$LOG"
    printf '  [%2d/%2d] seed=%-12s -> %s\n' "$i" "$ITERATIONS" "$seed" "$v"

    # NOSYNC is sampled for information only -- it is expected to be lossless at
    # late cut times and lossy at early ones, and neither outcome is a verdict
    # about the code under test. See test/t05 for the measurements.
    if [ "$MODE" = "NOSYNC" ]; then
        pass=$((pass + 1))
        continue
    fi

    if [ "$v" = "NO_COMMIT" ]; then
        # Counts as a pass (it is not a failure) but is tracked separately: it
        # carries NO information about durability, and a summary that hides that
        # would report "6/6 passed" for a run where 5 measured nothing.
        nocommit=$((nocommit + 1))
        pass=$((pass + 1))
    elif verdict_is_pass "$v"; then
        pass=$((pass + 1))
    else
        echo
        echo "FAIL at iteration $i: $v"
        echo "  $line"
        echo "  replay exactly:"
        echo "    bash $HERE/power-cut-vm.sh --arm=$ARM --mode=$MODE --window-us=$WINDOW --seed=$seed"
        echo "$STAMP fuzz STOP iter=$i seed=$seed verdict=$v" >> "$LOG"
        exit 1
    fi
done

informative=$((pass - nocommit))
echo "fuzz complete: $pass/$ITERATIONS passed — $informative informative, $nocommit NO_COMMIT (measured nothing); log at $LOG"
if [ "$informative" -eq 0 ]; then
    echo "WARNING: no iteration carried information. Cut timing is mis-tuned; this run proves nothing." >&2
fi
