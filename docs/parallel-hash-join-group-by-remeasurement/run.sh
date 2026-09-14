#!/usr/bin/env bash
set -euo pipefail
cd /home/puzpuzpuz/projects/questdb
remeasurement_output=/tmp/questdb-task9g
remeasurement_pattern="^($(paste -sd '|' benchmarks/parallel-hash-join-group-by-recovery-cases.txt))$"
date -u +'%Y-%m-%dT%H:%M:%SZ latency_start' >> "$remeasurement_output/timeline.txt"
CASE_PATTERN="$remeasurement_pattern" BENCHMARK_JAR="$remeasurement_output/candidate.jar" BENCHMARK_REVISION=4a05a7eb242e1225e57b3f1bd7d3798059a2820c BREAKER_MODE=active bash benchmarks/parallel-hash-join-group-by-v1.sh "$remeasurement_output/candidate" > "$remeasurement_output/candidate.txt" 2>&1
date -u +'%Y-%m-%dT%H:%M:%SZ latency_end' >> "$remeasurement_output/timeline.txt"
set +e
python3 benchmarks/compare-hash-join-group-by-recovery.py "$remeasurement_output/reference" "$remeasurement_output/candidate" "$remeasurement_output/comparison.csv" > "$remeasurement_output/comparison.txt" 2>&1
remeasurement_comparison_status=$?
set -e
printf '%s\n' "$remeasurement_comparison_status" > "$remeasurement_output/comparison.status"
date -u +'%Y-%m-%dT%H:%M:%SZ allocation_start' >> "$remeasurement_output/timeline.txt"
bash "$remeasurement_output/allocation.sh" "$remeasurement_output/allocation" > "$remeasurement_output/allocation.txt" 2>&1
date -u +'%Y-%m-%dT%H:%M:%SZ allocation_end' >> "$remeasurement_output/timeline.txt"
printf 'Finished. Recovery comparison exit status: %s\n' "$remeasurement_comparison_status"
