#!/bin/bash
################################################################################
#     ___                  _   ____  ____
#    / _ \ _   _  ___  ___| |_|  _ \| __ )
#   | | | | | | |/ _ \/ __| __| | | |  _ \
#   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
#    \__\_\\__,_|\___||___/\__|____/|____/
#
#  Copyright (c) 2014-2019 Appsicle
#  Copyright (c) 2019-2026 QuestDB
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
################################################################################

# RFC 130 task 10. Build the shaded benchmark jar before invoking.
# Usage: bash benchmarks/parallel-hash-join-group-by-v1.sh /tmp/new-results-directory
# Optional CASE_PATTERN selects cases for reproduction; only primary-w4 is the gate.
set -euo pipefail
if [[ $# != 1 ]]; then
    echo "Usage: $0 NEW_RESULTS_DIRECTORY" >&2
    exit 1
fi
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir "$1"
results_dir="$(cd "$1" && pwd)"
cd "$repo_dir"
revision="$(git rev-parse HEAD)"
if ! git diff --quiet HEAD -- core benchmarks; then
    revision+="+working-tree"
fi
java_command=(java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED
    --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow -Xmx8g
    -cp benchmarks/target/benchmarks.jar)
{
    date -u +'%Y-%m-%dT%H:%M:%SZ'
    git rev-parse HEAD
    git status --short
    sha256sum benchmarks/target/benchmarks.jar benchmarks/src/main/java/org/questdb/HashJoinGroupBy*Benchmark.java
    sha256sum benchmarks/parallel-hash-join-group-by-v1.sh benchmarks/parallel-hash-join-cold.py
    java -version
    uname -sr
    lscpu
    free -h
    df -h /tmp
    cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor
} > "$results_dir/environment.txt" 2>&1
run_case() {
    local name="$1" class="$2"
    shift 2
    if [[ ! "$name" =~ ${CASE_PATTERN:-.*} ]]; then
        return
    fi
    printf '%q ' "${java_command[@]}" "$class" "--revision=$revision" "$@" >> "$results_dir/commands.txt"
    printf '> %q 2>&1\n' "$results_dir/$name.txt" >> "$results_dir/commands.txt"
    echo "Running $name"
    "${java_command[@]}" "$class" "--revision=$revision" "$@" > "$results_dir/$name.txt" 2>&1
    sed -n '/^# .*repetition=/p; /^# result_checks=/p; /^# primary_gate=/p' "$results_dir/$name.txt"
}
primary() {
    run_case "$1" org.questdb.HashJoinGroupByBenchmark \
        '--candidate-compiler=org.questdb.HashJoinGroupByBenchmark$PlannerCandidateCompiler' "${@:2}"
}
v1() {
    run_case "$1" org.questdb.HashJoinGroupByV1Benchmark "${@:2}"
}
primary primary-w4 --require-primary-gate=true
primary primary-w1 --workers=1
primary primary-w2 --workers=2
v1 inner
v1 left --join=left
v1 right --join=right
v1 scalar-inner --groups=scalar
v1 scalar-left --groups=scalar --join=left
v1 scalar-right --groups=scalar --join=right
v1 inner-swapped --join=inner-swapped --rows=1000000 --plants=10000 --selected-keys=1000
v1 left-swapped --join=left-swapped --rows=1000000 --plants=10000 --selected-keys=1000
v1 right-swapped --join=right-swapped --rows=1000000 --plants=10000 --selected-keys=1000
v1 zero-matches --selected-keys=1 --key-domain=2147483647
v1 empty-build --selected-keys=0
v1 empty-build-left --selected-keys=0 --join=left
v1 singleton-build --selected-keys=1
v1 match-001pct --selected-keys=10
v1 match-1pct --selected-keys=1000
v1 match-50pct --selected-keys=50000
v1 match-100pct --selected-keys=100000
v1 build-beyond-cache --plants=2000000 --selected-keys=2000000
v1 build-source-10m --source-rows=10000000
v1 post-selective --join=left --post-filter=selective
v1 post-null-accepting --join=left --post-filter=null-accepting
v1 post-reject-all --join=left --post-filter=reject-all
v1 fanout-10 --fanout=10
v1 hot-key-90pct --hot-percent=90
v1 hot-chain --rows=100000 --plants=100 --selected-keys=1 --fanout=1000 --hot-percent=90
v1 high-cardinality --groups=high
v1 compressed-groups --groups=high --plants=100 --selected-keys=100
v1 near-input-groups --groups=high --rows=100000 --plants=100000 --selected-keys=100000
v1 small-input --rows=100000 --plants=1000 --selected-keys=100
v1 small-interval --interval=hour
v1 build-heavy --rows=100000 --plants=1000000 --selected-keys=100000
v1 concurrent-2 --concurrency=2
v1 concurrent-4 --concurrency=4
v1 concurrent-scalar-left --concurrency=4 --groups=scalar --join=left
v1 mixed --probe-storage=mixed --build-storage=mixed
v1 parquet --probe-storage=parquet --build-storage=parquet
# A matching native case isolates storage from the RIGHT normalization difference.
v1 scalar-right-1m --rows=1000000 --groups=scalar --join=right
v1 parquet-scalar-right --rows=1000000 --probe-storage=parquet --build-storage=parquet --groups=scalar --join=right
v1 cold-native --cold-helper=benchmarks/parallel-hash-join-cold.py
v1 cold-parquet --probe-storage=parquet --build-storage=parquet --cold-helper=benchmarks/parallel-hash-join-cold.py
v1 near-memory-limit --plants=1000000 --selected-keys=1000000 --memory-limit=92274688
