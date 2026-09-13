#!/usr/bin/env bash
set -euo pipefail

# Build first, without another Maven build/test running in this checkout:
# mvn -pl benchmarks -am package -P build-rust-library,qdbr-release -DskipTests -Dmaven.test.skip=true
cd "$(dirname "$0")/.."
heap_output_dir=${1:-docs/parallel-hash-join-group-by-heap}
mkdir -p "$heap_output_dir"
heap_output_dir=$(realpath "$heap_output_dir")
heap_agent_dir=$(mktemp -d /tmp/questdb-heap-agent.XXXXXX)
trap 'rm -rf "$heap_agent_dir"' EXIT
cat > "$heap_agent_dir/MANIFEST.MF" <<'MANIFEST'
Manifest-Version: 1.0
Premain-Class: org.questdb.HashJoinGroupByHeapBenchmark

MANIFEST
jar cfm "$heap_agent_dir/agent.jar" "$heap_agent_dir/MANIFEST.MF" \
    -C benchmarks/target/classes org/questdb/HashJoinGroupByHeapBenchmark.class
{
    java -version
    uname -a
    git rev-parse HEAD
    git diff HEAD -- '*.java' | sha256sum
    sha256sum benchmarks/target/benchmarks.jar
} > "$heap_output_dir/environment.txt" 2>&1
heap_java_args=(
    -ea
    --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED
    --add-opens=java.base/java.lang=ALL-UNNAMED
    --add-opens=java.base/java.lang.ref=ALL-UNNAMED
    --add-opens=java.base/java.nio=ALL-UNNAMED
    --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED
    --add-opens=java.base/java.util=ALL-UNNAMED
    --add-opens=java.base/java.util.regex=ALL-UNNAMED
    --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
    --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
    --enable-native-access=ALL-UNNAMED
    --sun-misc-unsafe-memory-access=allow
    "-javaagent:$heap_agent_dir/agent.jar"
    -cp benchmarks/target/benchmarks.jar
    org.questdb.HashJoinGroupByHeapBenchmark
)
for heap_storage in native mixed parquet; do
    for heap_mode in owner sharded scalar; do
        java "${heap_java_args[@]}" "$heap_storage" "$heap_mode" \
            > "$heap_output_dir/$heap_storage-$heap_mode.log" 2>&1
    done
done
python3 - "$heap_output_dir" <<'PY'
import csv
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
header = 'storage,mode,dimension,cardinality,phase,heap_bytes,heap_objects,largest_array_bytes,query_native_bytes'
with (root / 'summary.csv').open('w') as summary, (root / 'classes.csv').open('w') as classes:
    summary.write(header + '\n')
    classes.write('storage,mode,dimension,cardinality,phase,class,objects,bytes\n')
    for storage in ('native', 'mixed', 'parquet'):
        for mode in ('owner', 'sharded', 'scalar'):
            rows = []
            with (root / f'{storage}-{mode}.log').open() as source:
                for line in source:
                    if line.startswith(storage + ','):
                        rows.append(line)
                        summary.write(line)
                    elif line.startswith('# class,'):
                        classes.write(line[len('# class,'):])
            if len(rows) != 96:
                raise SystemExit(f'incomplete run: {storage}-{mode}: {len(rows)} snapshots')
            seen = set()
            for row in csv.DictReader([header + '\n'] + rows):
                key = (row['dimension'], row['cardinality'], row['phase'])
                if key in seen:
                    raise SystemExit(f'duplicate sample: {storage}-{mode}: {key}')
                seen.add(key)
                if row['phase'].startswith('closed') and int(row['query_native_bytes']) != 0:
                    raise SystemExit(f'retained query memory: {storage}-{mode}: {key}')
print('Validated 864 heap snapshots and 288 candidate executions against 144 ordinary references.')
PY
# Keep exact plans and class totals without checking in repeated engine log text.
gzip -n -f "$heap_output_dir/classes.csv"
for heap_storage in native mixed parquet; do
    for heap_mode in owner sharded scalar; do
        gzip -n -f "$heap_output_dir/$heap_storage-$heap_mode.log"
    done
done
