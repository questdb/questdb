#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 4 ]]; then
    echo "usage: $0 BASELINE_JAR FIXED_JAR DB_ROOT OUTPUT_DIR" >&2
    exit 2
fi

baseline_jar=$(realpath "$1")
fixed_jar=$(realpath "$2")
db_root=$(realpath "$3")
output_dir=$4
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(git -C "$script_dir" rev-parse --show-toplevel)
jemalloc=${JEMALLOC:-$repo_root/core/src/main/bin/linux-x86-64/libjemalloc.so}
server_cpus=${SERVER_CPUS:-0-14}
client_cpu=${CLIENT_CPU:-15}
warmups=${WARMUPS:-100}
requests=${REQUESTS:-500}
schedule=(baseline fixed fixed baseline baseline fixed fixed baseline baseline fixed fixed baseline)

mkdir -p "$output_dir"
output_dir=$(realpath "$output_dir")
server_pid=

if curl -fsS -G --data-urlencode "query=SELECT 1" \
    http://127.0.0.1:9000/exec >/dev/null 2>&1; then
    echo "port 9000 already serves a QuestDB instance" >&2
    exit 1
fi

stop_server() {
    if [[ -n "${server_pid:-}" ]] && kill -0 "$server_pid" 2>/dev/null; then
        kill -TERM "$server_pid"
        wait "$server_pid" || true
    fi
    server_pid=
}

trap stop_server EXIT INT TERM

start_server() {
    local jar=$1
    local log=$2
    env LD_PRELOAD="$jemalloc" taskset -c "$server_cpus" java \
        --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
        --add-opens=java.base/java.lang=ALL-UNNAMED \
        --sun-misc-unsafe-memory-access=allow \
        --enable-native-access=ALL-UNNAMED \
        -cp "$jar" io.questdb.ServerMain -d "$db_root" >"$log" 2>&1 &
    server_pid=$!

    local ready=false
    for _ in {1..120}; do
        if taskset -c "$client_cpu" curl -fsS -G \
            --data-urlencode "query=SELECT 1" \
            http://127.0.0.1:9000/exec >/dev/null 2>&1; then
            ready=true
            break
        fi
        if ! kill -0 "$server_pid" 2>/dev/null; then
            tail -100 "$log" >&2
            exit 1
        fi
        sleep 0.1
    done
    if [[ "$ready" != true ]]; then
        echo "QuestDB did not become ready" >&2
        exit 1
    fi
    kill -0 "$server_pid"
}

for index in "${!schedule[@]}"; do
    run=$((index + 1))
    variant=${schedule[$index]}
    if [[ "$variant" == baseline ]]; then
        jar=$baseline_jar
    else
        jar=$fixed_jar
    fi
    prefix=$(printf "%02d-%s" "$run" "$variant")
    echo "START $prefix"
    start_server "$jar" "$output_dir/$prefix-server.log"
    grep -F "$jemalloc" "/proc/$server_pid/maps" >"$output_dir/$prefix-jemalloc-map.txt"

    if [[ ! -f "$output_dir/plan-$variant.json" ]]; then
        query=$(tr '\n' ' ' <"$script_dir/query.sql")
        taskset -c "$client_cpu" curl -fsS -G \
            --data-urlencode "query=EXPLAIN $query" \
            http://127.0.0.1:9000/exec >"$output_dir/plan-$variant.json"
        grep -q 'Async Horizon Join workers: 15 offsets: 4' "$output_dir/plan-$variant.json"
    fi

    taskset -c "$client_cpu" python3 "$script_dir/query_client.py" \
        --query-file "$script_dir/query.sql" \
        --output "$output_dir/$prefix.json" \
        --warmups "$warmups" \
        --max-requests "$requests"
    echo "DONE  $prefix"
    stop_server
done

python3 "$script_dir/summarize.py" e2e "$output_dir"
