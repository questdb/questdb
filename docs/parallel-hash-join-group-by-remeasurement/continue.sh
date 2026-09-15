#!/usr/bin/env bash
set -euo pipefail
cd /home/puzpuzpuz/projects/questdb
remeasurement_output=/tmp/questdb-task9g
remeasurement_pattern="^($(tail -n +12 benchmarks/parallel-hash-join-group-by-recovery-cases.txt | paste -sd '|'))$"
date -u +'%Y-%m-%dT%H:%M:%SZ latency_continuation_start' >> "$remeasurement_output/timeline.txt"
CASE_PATTERN="$remeasurement_pattern" BENCHMARK_JAR="$remeasurement_output/candidate.jar" BENCHMARK_REVISION=4a05a7eb242e1225e57b3f1bd7d3798059a2820c BREAKER_MODE=active bash benchmarks/parallel-hash-join-group-by-v1.sh "$remeasurement_output/continuation" > "$remeasurement_output/continuation.txt" 2>&1
python3 - <<'MERGE'
from pathlib import Path
import shutil
r=Path('/tmp/questdb-task9g')
for p in (r/'continuation').glob('*.txt'):
 if p.name in {'environment.txt', 'commands.txt'}:
  shutil.copyfile(p, r/('continuation-'+p.name))
 else:
  shutil.copyfile(p, r/'candidate'/p.name)
with (r/'candidate/commands.txt').open('a') as out: out.write((r/'continuation/commands.txt').read_text())
MERGE
date -u +'%Y-%m-%dT%H:%M:%SZ latency_end' >> "$remeasurement_output/timeline.txt"
set +e
python3 benchmarks/compare-hash-join-group-by-recovery.py "$remeasurement_output/reference" "$remeasurement_output/candidate" "$remeasurement_output/comparison.csv" > "$remeasurement_output/comparison.txt" 2>&1
remeasurement_comparison_status=$?
set -e
printf '%s\n' "$remeasurement_comparison_status" > "$remeasurement_output/comparison.status"
date -u +'%Y-%m-%dT%H:%M:%SZ allocation_start' >> "$remeasurement_output/timeline.txt"
bash "$remeasurement_output/allocation.sh" "$remeasurement_output/allocation" > "$remeasurement_output/allocation.txt" 2>&1
date -u +'%Y-%m-%dT%H:%M:%SZ allocation_end' >> "$remeasurement_output/timeline.txt"
printf 'Finished continuation. Recovery comparison exit status: %s\n' "$remeasurement_comparison_status"
