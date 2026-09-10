#!/usr/bin/env bash
#
# The performance acceptance matrix of the live-view checkpoint layout removal.
#
# Runs one revision's half of the matrix. Invoke it once per revision against that
# revision's own benchmarks jar, into that revision's own output directory, then compare
# the two with summarize-matrix.py.
#
#   ./run-matrix.sh <benchmarks.jar> <output-dir> [cardinalities] [runs] [steady|repair]
#
# MATRIX_SHAPES=<label>[,<label>...] in the environment restricts a run to the named rows,
# which is how the 1,000,000-key scaling run covers only the anchor-only and single-SUM
# shapes and how the baseline's half of the repair cell runs only the whole-range control.
#
# Every cell is one (shape, fusion mode, live-key count) triple, and every run of a cell is
# an independent JVM over a fresh database. A run seeds K rows into K round-robin accounts,
# so the view holds exactly K live keys and nothing ever falls behind the frontier, then
# ingests BATCHES commits of 1000 rows each. Consecutive rows take consecutive accounts, so
# a commit touches exactly 1000 distinct existing keys - the changed-key domain the matrix
# fixes at 1000 independently of the state size. --checkpoint-rows matches the commit, so
# each commit seals exactly one boundary and a batch row of the output is one seal.
#
# --restart=true adds the restore and its first reseal at the end of each run, which is a
# separate row of the matrix measured from the same state the steady rows leave behind.
#
# The repair mode is the closed-segment repair cell. A one-minute anchor with 1000 rows per
# minute makes every batch one closed anchor segment and one checkpoint boundary, and from
# batch 20 on every commit carries exactly one late row - every 1000th row, ten minutes
# behind its position - so each measured batch is a one-key correction inside a closed
# segment with ten checkpoints sealed above it. The two cells differ in the route: the
# whole-range replay of the corrected segment, which is the control both revisions run, and
# the keyed replay through the base's posting index, which the candidate runs beside it.
# Every repair run ends with the independent result oracle, since a repair that read too
# little is faster for the wrong reason.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR="${1:?usage: run-matrix.sh <benchmarks.jar> <output-dir> [cardinalities] [runs]}"
OUT="${2:?usage: run-matrix.sh <benchmarks.jar> <output-dir> [cardinalities] [runs]}"
CARDINALITIES="${3:-10000 100000}"
RUNS="${4:-5}"
MODE="${5:-steady}"
BATCHES=110
BATCH=1000

mkdir -p "$OUT"

# shape-label:extra-arguments. The labels are the matrix's rows.
SHAPES=(
    "anchor-only-decimal:--shape=decimal-sum"
    "anchor-only-unfused-control:--shape=sum-unfused"
    "narrow-sum:--shape=sum"
    "narrow-sum-avg-count:--shape=sum-avg-count"
    "narrow-count-star-key:--shape=count-star-key --null-key-percent=5"
    "wide-below-budget:--shape=sum --sum-columns=14"
    "wide-at-budget:--shape=count --sum-columns=15"
    "wide-above-budget:--shape=sum --sum-columns=15"
    "residual-heavy:--shape=residual"
)

REPAIR_COMMON="--shape=sum --index=true --anchor-period=1m --ts-step-us=60000 --o3-percent=0.1 --o3-lag=10m --o3-from-batch=20 --oracle=true"
REPAIR_SHAPES=(
    "repair-closed-whole:$REPAIR_COMMON --repair-keyed-replay=false"
    "repair-closed-keyed:$REPAIR_COMMON --repair-keyed-replay=true"
)

case "$MODE" in
    steady) ;;
    repair) SHAPES=("${REPAIR_SHAPES[@]}") ;;
    *) echo "unknown mode: $MODE (steady|repair)" >&2; exit 2 ;;
esac

is_selected() {
    [ -z "${MATRIX_SHAPES:-}" ] && return 0
    case ",$MATRIX_SHAPES," in
        *",$1,"*) return 0 ;;
        *) return 1 ;;
    esac
}

heap() {
    if [ "$1" -ge 1000000 ]; then echo "-Xmx24g"; else echo "-Xmx8g"; fi
}

for keys in $CARDINALITIES; do
    for fusion in true false; do
        for entry in "${SHAPES[@]}"; do
            label="${entry%%:*}"
            args="${entry#*:}"
            is_selected "$label" || continue
            for run in $(seq 1 "$RUNS"); do
                file="$OUT/${label}__fusion-${fusion}__keys-${keys}__run-${run}.tsv"
                if [ -s "$file" ]; then
                    echo "skip $(basename "$file")"
                    continue
                fi
                echo "run  $(basename "$file")"
                # shellcheck disable=SC2086
                # -Dout sends the server log to a file. The default configuration writes it
                # to stdout on a thread that does not share System.out's lock, so a log
                # record lands inside a printf'd report line often enough to corrupt a run.
                java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED "$(heap "$keys")" \
                    -Dout="$DIR/quiet-log.conf" \
                    -cp "$JAR" org.questdb.LiveViewSteadyStateBenchmark \
                    --seed="$keys" --recycle-accounts="$keys" \
                    --batch=$BATCH --batches=$BATCHES --checkpoint-rows=$BATCH \
                    --fusion="$fusion" --restart=true $args \
                    2>/dev/null > "$file" || { echo "FAILED $file"; rm -f "$file"; }
            done
        done
    done
done
