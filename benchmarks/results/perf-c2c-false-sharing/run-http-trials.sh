#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 4 ]]; then
    echo "usage: $0 BASELINE_JAR FIXED_JAR DATA_ROOT OUTPUT_DIR" >&2
    exit 2
fi

baseline_jar=$(realpath "$1")
fixed_jar=$(realpath "$2")
data_root=$(realpath "$3")
output_dir=$(realpath -m "$4")
script_dir=$(cd "$(dirname "$0")" && pwd)
repo_root=$(realpath "$script_dir/../../..")
jemalloc_so=${JEMALLOC_SO:-$repo_root/core/src/main/bin/linux-x86-64/libjemalloc.so}
schedule=(baseline fixed fixed baseline baseline fixed fixed baseline baseline fixed fixed baseline)
baseline_n=0
fixed_n=0
server_pid=

if [[ ! -r "$jemalloc_so" ]]; then
    echo "jemalloc library is not readable: $jemalloc_so" >&2
    exit 2
fi

mkdir -p "$output_dir"
printf 'jemalloc=%s\n' "$(realpath "$jemalloc_so")" >"$output_dir/allocator.txt"
sha256sum "$jemalloc_so" >>"$output_dir/allocator.txt"

stop_server() {
    if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
        kill -TERM "$server_pid"
        wait "$server_pid" || true
    fi
    server_pid=
}

trap stop_server EXIT INT TERM

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

    printf 'HTTP %s trial %d of 6\n' "$variant" "$trial"
    taskset -c 0-14 env LD_PRELOAD="$jemalloc_so" java \
        --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
        --add-opens=java.base/java.lang=ALL-UNNAMED \
        --sun-misc-unsafe-memory-access=allow \
        --enable-native-access=ALL-UNNAMED \
        -cp "$jar" \
        io.questdb.ServerMain \
        -d "$data_root" \
        >"$output_dir/http-server-$variant-$trial.log" 2>&1 &
    server_pid=$!

    ready=false
    for _ in {1..120}; do
        if taskset -c 15 curl -fsG \
            --data-urlencode "query=SELECT 1" \
            http://127.0.0.1:9000/exec \
            >/dev/null 2>&1; then
            ready=true
            break
        fi
        if ! kill -0 "$server_pid" 2>/dev/null; then
            tail -100 "$output_dir/http-server-$variant-$trial.log"
            exit 1
        fi
        sleep 0.1
    done
    if [[ "$ready" != true ]]; then
        echo "QuestDB did not become ready" >&2
        exit 1
    fi

    if ! rg -F "$jemalloc_so" "/proc/$server_pid/maps" \
        >"$output_dir/http-server-$variant-$trial-allocator-map.txt"; then
        echo "jemalloc is not mapped in server process $server_pid" >&2
        exit 1
    fi

    taskset -c 15 python3 "$script_dir/http_bench.py" \
        --warmups 100 \
        --requests 1000 \
        --output "$output_dir/http-$variant-$trial.csv"

    stop_server
done

printf 'HTTP schedule complete: baseline=%d fixed=%d\n' "$baseline_n" "$fixed_n"
