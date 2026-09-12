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

# RFC 130 task 7. Build benchmarks/target/benchmarks.jar before invoking.
# Usage: bash benchmarks/parallel-hash-join-group-by.sh /tmp/new-results-directory
# Runs sequentially; each JVM retains its fresh temporary database for inspection.
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
if ! git diff --quiet HEAD -- core benchmarks/src/main/java; then
    revision+="+working-tree"
fi

java_command=(java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED
    --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow -Xmx8g
    -cp benchmarks/target/benchmarks.jar org.questdb.HashJoinGroupByBenchmark
    "--revision=$revision"
    '--candidate-compiler=org.questdb.HashJoinGroupByBenchmark$PlannerCandidateCompiler')

{
    date -u +'%Y-%m-%dT%H:%M:%SZ'
    git rev-parse HEAD
    git status --short
    sha256sum benchmarks/target/benchmarks.jar benchmarks/src/main/java/org/questdb/HashJoinGroupByBenchmark.java
    java -version
    uname -sr
    lscpu
    free -h
    df -h /tmp
    cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor
} > "$results_dir/environment.txt" 2>&1

run_case() {
    local name="$1"
    shift
    printf '%q ' "${java_command[@]}" "$@" >> "$results_dir/commands.txt"
    printf '> %q 2>&1\n' "$results_dir/$name.txt" >> "$results_dir/commands.txt"
    echo "Running $name"
    "${java_command[@]}" "$@" > "$results_dir/$name.txt" 2>&1
    sed -n '/^# .*repetition=/p; /^# result_checks=/p; /^# primary_gate=/p' "$results_dir/$name.txt"
}

# The fixed primary case is the only acceptance gate; variants cannot replace it.
run_case primary-w4 --require-primary-gate=true
run_case primary-w1 --workers=1
run_case primary-w2 --workers=2
run_case build-10k --plants=10000
run_case build-1m --plants=1000000
run_case select-1pct --selected-percent=1
run_case select-50pct --selected-percent=50
run_case small-input --rows=100000 --plants=1000
run_case build-heavy --rows=100000 --plants=1000000
