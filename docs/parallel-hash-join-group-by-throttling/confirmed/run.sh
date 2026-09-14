#!/usr/bin/env bash
set -euo pipefail
cd /home/puzpuzpuz/projects/questdb
confirmed_output=/tmp/questdb-throttle-confirmed
confirmed_pattern="^($(paste -sd '|' benchmarks/parallel-hash-join-group-by-recovery-cases.txt))$"
CASE_PATTERN="$confirmed_pattern" BENCHMARK_JAR="$confirmed_output/candidate.jar" BENCHMARK_REVISION=d2e59fc832 BREAKER_MODE=active bash benchmarks/parallel-hash-join-group-by-v1.sh "$confirmed_output/candidate" > "$confirmed_output/candidate.txt" 2>&1
set +e
python3 benchmarks/compare-hash-join-group-by-recovery.py "$confirmed_output/reference" "$confirmed_output/candidate" "$confirmed_output/comparison.csv" > "$confirmed_output/comparison.txt" 2>&1
confirmed_comparison_status=$?
set -e
printf '%s\n' "$confirmed_comparison_status" > "$confirmed_output/comparison.status"
bash "$confirmed_output/allocation.sh" "$confirmed_output/allocation" > "$confirmed_output/allocation.txt" 2>&1
printf 'Finished. Recovery comparison exit status: %s\n' "$confirmed_comparison_status"
