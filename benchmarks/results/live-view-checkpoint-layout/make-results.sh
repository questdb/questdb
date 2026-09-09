#!/usr/bin/env bash
# Builds RESULTS.md from the two matrix output directories.
set -euo pipefail
BASE="$1"; CAND="$2"; OUT="$3"
DIR="$(cd "$(dirname "$0")" && pwd)"
{
cat <<'HDR'
# Performance acceptance matrix: measured run

Baseline `6a2c656028` against the candidate at the head of `puzpuzpuz_live_view`.
Both revisions ran on the same machine, filesystem, JVM, heap, worker count, input and
maintenance settings, through `run-matrix.sh`, which fixes every one of those but the
machine.

HDR
echo "## Environment"
echo
echo '```'
echo "cpu:    $(nproc) logical cores"
echo "memory: $(free -g | awk '/^Mem:/{print $2}') GiB"
echo "os:     $(lsb_release -ds 2>/dev/null || uname -sr)"
echo "jvm:    $(java -version 2>&1 | head -1)"
echo "disk:   $(df -h --output=fstype,size . | tail -1 | tr -s ' ')"
echo '```'
echo
echo "## Protocol"
echo
cat <<'PROTO'
Per cell: 5 independent JVM runs over a fresh database, `--batches=110` commits of 1000
rows each into `K` round-robin accounts, `--checkpoint-rows=1000` so one commit seals one
boundary, the first 10 batches dropped as warm-up, `--restart=true` for the restore and
its first reseal. A run's figure is a median (or a p95) over its 100 measured seals; a
cell's figure is the median over its five runs.

Ratios below are candidate / baseline. `rows_per_sec_median` is the one row where higher
is better and its limit is a floor; every other limit is a ceiling.
PROTO
echo
echo "## Gates"
echo
"$DIR/summarize-matrix.py" "$BASE" "$CAND" --md
} > "$OUT"
echo "wrote $OUT"
