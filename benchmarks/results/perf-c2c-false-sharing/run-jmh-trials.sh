#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 3 ]]; then
    echo "usage: $0 BASELINE_JAR FIXED_JAR OUTPUT_DIR" >&2
    exit 2
fi

baseline_jar=$(realpath "$1")
fixed_jar=$(realpath "$2")
output_dir=$(realpath -m "$3")
script_dir=$(cd "$(dirname "$0")" && pwd)
repo_root=$(realpath "$script_dir/../../..")
jemalloc_so=${JEMALLOC_SO:-$repo_root/core/src/main/bin/linux-x86-64/libjemalloc.so}
schedule=(baseline fixed fixed baseline baseline fixed fixed baseline baseline fixed fixed baseline)
baseline_n=0
fixed_n=0

if [[ ! -r "$jemalloc_so" ]]; then
    echo "jemalloc library is not readable: $jemalloc_so" >&2
    exit 2
fi

mkdir -p "$output_dir"
printf 'jemalloc=%s\n' "$(realpath "$jemalloc_so")" >"$output_dir/allocator.txt"
sha256sum "$jemalloc_so" >>"$output_dir/allocator.txt"

for variant in "${schedule[@]}"; do
    if [[ "$variant" == baseline ]]; then
        baseline_n=$((baseline_n + 1))
        trial=$baseline_n
        jar=$baseline_jar
    else
        fixed_n=$((fixed_n + 1))
        trial=$fixed_n
        jar=$fixed_jar
    fi

    printf 'JMH %s trial %d of 6\n' "$variant" "$trial"
    timeout 60s taskset -c 0-15 env LD_PRELOAD="$jemalloc_so" java \
        --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
        --add-opens=java.base/java.lang=ALL-UNNAMED \
        --sun-misc-unsafe-memory-access=allow \
        --enable-native-access=ALL-UNNAMED \
        -Dcovered.bench.skipBuild=true \
        -Dcovered.bench.rows=20000000 \
        -Dcovered.bench.workers=16 \
        -cp "$jar" \
        org.questdb.CoveredIndexDecodeBenchmark \
        CoveredIndexDecodeBenchmark.run \
        -p shape=sum \
        -p config=PARALLEL_REF \
        -p selectivity=50 \
        -wi 3 -w 1s -i 8 -r 1s \
        -rf json \
        -rff "$output_dir/jmh-$variant-$trial.json" \
        -o "$output_dir/jmh-$variant-$trial.log"
done

printf 'JMH schedule complete: baseline=%d fixed=%d\n' "$baseline_n" "$fixed_n"
