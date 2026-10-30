#!/usr/bin/env bash
#
# The performance acceptance matrix of the live-view checkpoint layout removal.
#
# Runs one revision's half of the matrix. Invoke it once per revision against that
# revision's own benchmarks jar, into that revision's own output directory, then compare
# the two with summarize-matrix.py.
#
#   ./run-matrix.sh <benchmarks.jar> <output-dir> [cardinalities] [runs] [steady|repair|churn|cold-restore]
#
# MATRIX_SHAPES=<label>[,<label>...] in the environment restricts a run to the named rows,
# which is how the 1,000,000-key scaling run covers only the anchor-only and single-SUM
# shapes and how the baseline's half of the repair cell runs only the whole-range control.
# MATRIX_FUSION=true|false restricts it to one runtime mode, for re-measuring one cell.
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
#
# The churn mode is the add/remove-keys, cross-anchor-boundary run the matrix names beside
# the steady rows. Instead of K accounts recycled forever it runs a K-account window over an
# anchor bucket of exactly K rows - K minutes at 1000 rows per minute - which the harness
# slides forward by K/2 accounts every bucket: half of a bucket's accounts recur from the
# bucket before it and half are new, and the half left behind falls behind the frontier and
# is evicted by the sweep at the next bucket boundary. The compaction thresholds are lowered
# so that sweep actually fires (the defaults never do below 100,000 stale keys). Every
# steady seal of the run is therefore one of three kinds: over existing keys, over keys the
# batch just added, or - once per bucket - the seal after a sweep, which carries one removal
# per evicted key on top of the keys it imaged. The run is CHURN_BUCKETS anchor buckets long
# (six by default, which leaves five measured sweeps once the first boundary after the seed
# has gone into the warm-up), and ends with the result oracle, since eviction is state the
# view must not have needed. CHURN_BUCKETS=2 in the environment is how the residual-heavy
# cell runs at 100,000 keys: its ring-backed residual scans the whole map on every seal,
# which makes a bucket of 100 batches cost two minutes, and one measured sweep per run over
# five runs is what that cell affords.
#
# The cold-restore mode repeats the steady rows with --restart-cache=cold: the same run, but
# the restart at its end drops every file of the database from the page cache first, so the
# restore reads its checkpoint from disk. It is the cold-cache restore reading the matrix
# asks for beside the warm one, and only its restore figures are read.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR="${1:?usage: run-matrix.sh <benchmarks.jar> <output-dir> [cardinalities] [runs]}"
OUT="${2:?usage: run-matrix.sh <benchmarks.jar> <output-dir> [cardinalities] [runs]}"
CARDINALITIES="${3:-10000 100000}"
RUNS="${4:-5}"
MODE="${5:-steady}"
CHURN_BUCKETS="${CHURN_BUCKETS:-6}"
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

# The label suffix and the extra arguments a mode adds to every steady row.
SUFFIX=""
MODE_ARGS=""
case "$MODE" in
    steady) ;;
    repair) SHAPES=("${REPAIR_SHAPES[@]}") ;;
    churn) SUFFIX="-churn"; MODE_ARGS="--oracle=true" ;;
    cold-restore) SUFFIX="-cold-restore"; MODE_ARGS="--restart-cache=cold" ;;
    *) echo "unknown mode: $MODE (steady|repair|churn|cold-restore)" >&2; exit 2 ;;
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

# How a run picks its accounts, per mode and cardinality. The steady rows recycle K accounts
# so the live domain is exactly K and nothing ages out; the churn rows slide a K-account
# window over a K-row anchor bucket, so K/2 accounts are added and K/2 evicted per bucket.
account_args() {
    local keys="$1"
    if [ "$MODE" = churn ]; then
        # 1000 rows per minute, so a K-row bucket is a K/1000-minute anchor.
        echo "--account-window=$keys --anchor-period=$((keys / 1000))m --ts-step-us=60000 --compact-threshold=1000 --compact-stale-percent=25"
    else
        echo "--recycle-accounts=$keys"
    fi
}

# How many commits a run ingests. The churn rows cross one anchor boundary per K/1000
# batches and run CHURN_BUCKETS of them: the first boundary after the seed falls inside the
# warm-up, so six buckets give five measured sweeps. Never fewer than the steady 110.
batches() {
    local keys="$1"
    if [ "$MODE" = churn ]; then
        local per_bucket=$((keys / 1000))
        local cycles=$((CHURN_BUCKETS * per_bucket))
        if [ "$cycles" -lt 100 ]; then cycles=100; fi
        echo $((10 + cycles))
    else
        echo "$BATCHES"
    fi
}

for keys in $CARDINALITIES; do
    for fusion in true false; do
        [ -n "${MATRIX_FUSION:-}" ] && [ "$MATRIX_FUSION" != "$fusion" ] && continue
        for entry in "${SHAPES[@]}"; do
            label="${entry%%:*}$SUFFIX"
            args="${entry#*:} $MODE_ARGS"
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
                # stderr goes to a file beside the output, so a run that dies keeps its
                # stack trace, and a failed run's partial output is kept under .failed.tsv
                # rather than deleted: the aggregator ignores both.
                java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED "$(heap "$keys")" \
                    -Dout="$DIR/quiet-log.conf" \
                    -cp "$JAR" org.questdb.LiveViewSteadyStateBenchmark \
                    --seed="$keys" $(account_args "$keys") \
                    --batch=$BATCH --batches="$(batches "$keys")" --checkpoint-rows=$BATCH \
                    --fusion="$fusion" --restart=true $args \
                    2> "${file%.tsv}.err" > "$file" \
                    || { echo "FAILED $file (stderr in ${file%.tsv}.err)"; mv -f "$file" "${file%.tsv}.failed.tsv"; }
            done
        done
    done
done
