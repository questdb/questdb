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

# RFC 130 task 9f. The first jar must be built from 58b1dc04cc, the original task 10.
# The second must be built from 12ff320ae7, before the recovery change.
# Build the candidate first. This script runs every JVM sequentially; do not run
# compilers, tests, allocation profiling, or other benchmarks concurrently.
set -euo pipefail
if [[ $# != 3 ]]; then
    echo "Usage: $0 REFERENCE_JAR PRECHANGE_JAR NEW_RESULTS_DIRECTORY" >&2
    exit 1
fi
reference_jar=$(realpath "$1")
prechange_jar=$(realpath "$2")
mkdir "$3"
recovery_dir=$(realpath "$3")
cd "$(dirname "$0")/.."
candidate_jar=$(realpath benchmarks/target/benchmarks.jar)
mkdir "$recovery_dir/controls" "$recovery_dir/harness"
javac -cp "$reference_jar" -d "$recovery_dir/harness" \
    benchmarks/src/main/java/org/questdb/HashJoinGroupByBenchmark.java \
    benchmarks/src/main/java/org/questdb/HashJoinGroupByV1Benchmark.java
{
    date -u
    git rev-parse HEAD
    git diff HEAD -- core benchmarks
    sha256sum "$reference_jar" "$prechange_jar" "$candidate_jar"
    sha256sum benchmarks/src/main/java/org/questdb/HashJoinGroupBy{,V1}Benchmark.java
    java -version
    uname -a
    lscpu
} > "$recovery_dir/environment.txt" 2>&1
java_command=(java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED
    --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow -Xmx8g)
control() {
    local name=$1 classpath=$2 revision=$3
    shift 3
    local command=("${java_command[@]}" -cp "$classpath" org.questdb.HashJoinGroupByBenchmark
        '--candidate-compiler=org.questdb.HashJoinGroupByBenchmark$PlannerCandidateCompiler'
        "--revision=$revision" "$@")
    printf '%q ' "${command[@]}" >> "$recovery_dir/commands.txt"
    printf '> %q 2>&1\n' "$recovery_dir/controls/$name.txt" >> "$recovery_dir/commands.txt"
    "${command[@]}" > "$recovery_dir/controls/$name.txt" 2>&1
    sed -n '/^# .*repetition=/p; /^# primary_gate=/p' "$recovery_dir/controls/$name.txt"
}
control reference-original "$reference_jar" 58b1dc04cc --require-primary-gate=true
control reference-active "$recovery_dir/harness:$reference_jar" 58b1dc04cc+common-harness --breaker=active --require-primary-gate=true
control prechange-noop "$recovery_dir/harness:$prechange_jar" 12ff320ae7+common-harness --breaker=noop
control prechange-active "$recovery_dir/harness:$prechange_jar" 12ff320ae7+common-harness --breaker=active --require-primary-gate=true
pattern="^($(paste -sd '|' benchmarks/parallel-hash-join-group-by-recovery-cases.txt))$"
CASE_PATTERN="$pattern" BENCHMARK_JAR="$reference_jar" \
    BENCHMARK_CLASSPATH="$recovery_dir/harness:$reference_jar" \
    BENCHMARK_REVISION=58b1dc04cc+common-harness BREAKER_MODE=noop \
    bash benchmarks/parallel-hash-join-group-by-v1.sh "$recovery_dir/reference"
CASE_PATTERN="$pattern" BENCHMARK_JAR="$candidate_jar" \
    BENCHMARK_REVISION="$(git rev-parse HEAD)+recovery" BREAKER_MODE=active \
    bash benchmarks/parallel-hash-join-group-by-v1.sh "$recovery_dir/candidate"
python3 benchmarks/compare-hash-join-group-by-recovery.py \
    "$recovery_dir/reference" "$recovery_dir/candidate" "$recovery_dir/comparison.csv"
