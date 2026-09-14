#!/usr/bin/env bash
set -euo pipefail
cd /home/puzpuzpuz/projects/questdb
allocation_output_dir=$(realpath -m "${1:-docs/parallel-hash-join-group-by-allocation}")
mkdir -p "$allocation_output_dir"
: > "$allocation_output_dir/commands.txt"
allocation_agent_dir=$(mktemp -d /tmp/questdb-allocation-agent.XXXXXX)
trap 'rm -rf "$allocation_agent_dir"' EXIT
allocation_asm_jar=${ALLOCATION_ASM_JAR:-$HOME/.m2/repository/org/ow2/asm/asm/9.9.1/asm-9.9.1.jar}
if [[ ! -f "$allocation_asm_jar" ]]; then
    echo 'ASM 9.9.1 is required: mvn dependency:get -Dartifact=org.ow2.asm:asm:9.9.1' >&2
    exit 1
fi
javac -cp "$allocation_asm_jar:/tmp/questdb-throttle-confirmed/candidate.jar" -d "$allocation_agent_dir" benchmarks/allocation/*.java
printf 'Premain-Class: org.questdb.AllocationAgent\n\n' > "$allocation_agent_dir/MANIFEST.MF"
jar cfm "$allocation_agent_dir/agent.jar" "$allocation_agent_dir/MANIFEST.MF" -C "$allocation_agent_dir" .
{
    java -version
    uname -a
    git rev-parse HEAD
    git diff HEAD -- '*.java' | sha256sum
    sha256sum /tmp/questdb-throttle-confirmed/candidate.jar benchmarks/allocation/*.java "$allocation_asm_jar"
} > "$allocation_output_dir/environment.txt" 2>&1
allocation_java_args=(-ea -Xbatch -XX:TieredStopAtLevel=1 -XX:-DoEscapeAnalysis -XX:-UseTLAB --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED
    --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow)
printf 'JVM_FLAGS=%s\n' "${allocation_java_args[*]}" >> "$allocation_output_dir/environment.txt"
printf 'case,storage,mode,rows,owners,join,conversion\n' > "$allocation_output_dir/cases.csv"
run_allocation_case() {
    local storage=$1 mode=$2 rows=$3 owners=$4 join=$5 conversion=$6
    local name="$storage-$mode-$rows-$owners-$join-$conversion"
    printf '%s,%s,%s,%s,%s,%s,%s\n' "$name" "$storage" "$mode" "$rows" "$owners" "$join" "$conversion" >> "$allocation_output_dir/cases.csv"
    for pass in bytes sites; do
        local agent_mode=census
        printf 'java ' >> "$allocation_output_dir/commands.txt"
        printf '%q ' "${allocation_java_args[@]}" "-javaagent:$allocation_agent_dir/agent.jar=$agent_mode" >> "$allocation_output_dir/commands.txt"
        printf '%q ' -cp "$allocation_agent_dir:$allocation_asm_jar:/tmp/questdb-throttle-confirmed/candidate.jar" org.questdb.HashJoinGroupByAllocationBenchmark "$storage" "$mode" "$pass" "$rows" "$owners" "$join" >> "$allocation_output_dir/commands.txt"
        printf '%q\n' "$conversion" >> "$allocation_output_dir/commands.txt"
        java "${allocation_java_args[@]}" "-javaagent:$allocation_agent_dir/agent.jar=$agent_mode" \
            -cp "$allocation_agent_dir:$allocation_asm_jar:/tmp/questdb-throttle-confirmed/candidate.jar" \
            org.questdb.HashJoinGroupByAllocationBenchmark "$storage" "$mode" "$pass" "$rows" "$owners" "$join" "$conversion" \
            > "$allocation_output_dir/$name-$pass.log" 2>&1
        gzip -n -f "$allocation_output_dir/$name-$pass.log"
    done
    echo "Passed $name"
}
for storage in native mixed parquet; do
    for mode in owner sharded scalar; do
        join=left
        if [[ "$mode" == owner ]]; then join=inner; fi
        if [[ "$mode" == scalar ]]; then join=right; fi
        run_allocation_case "$storage" "$mode" 8192 1 "$join" plain
        run_allocation_case "$storage" "$mode" 65536 2 "$join" plain
    done
done
for storage in mixed parquet; do
    for mode in owner sharded scalar; do
        run_allocation_case "$storage" "$mode" 8192 2 right converted
    done
done
python3 benchmarks/allocation/validate.py "$allocation_output_dir"
