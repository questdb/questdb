#!/usr/bin/env bash
#
# The performance acceptance matrix of the live-view checkpoint layout removal.
#
# Runs one revision's half of the matrix. Invoke it once per revision against that
# revision's own benchmarks jar, into that revision's own output directory, then compare
# the two with summarize-matrix.py.
#
#   ./run-matrix.sh <benchmarks.jar> <output-dir> [cardinalities] [runs]
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
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR="${1:?usage: run-matrix.sh <benchmarks.jar> <output-dir> [cardinalities] [runs]}"
OUT="${2:?usage: run-matrix.sh <benchmarks.jar> <output-dir> [cardinalities] [runs]}"
CARDINALITIES="${3:-10000 100000}"
RUNS="${4:-5}"
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

heap() {
    if [ "$1" -ge 1000000 ]; then echo "-Xmx24g"; else echo "-Xmx8g"; fi
}

for keys in $CARDINALITIES; do
    for fusion in true false; do
        for entry in "${SHAPES[@]}"; do
            label="${entry%%:*}"
            args="${entry#*:}"
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
