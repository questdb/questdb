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

mkdir -p "$output_dir"
output_dir=$(realpath "$output_dir")
server_pid=
perf_pid=

if curl -fsS -G --data-urlencode "query=SELECT 1" \
    http://127.0.0.1:9000/exec >/dev/null 2>&1; then
    echo "port 9000 already serves a QuestDB instance" >&2
    exit 1
fi

stop_all() {
    if [[ -n "${perf_pid:-}" ]] && kill -0 "$perf_pid" 2>/dev/null; then
        kill -INT "$perf_pid"
        wait "$perf_pid" || true
    fi
    perf_pid=
    if [[ -n "${server_pid:-}" ]] && kill -0 "$server_pid" 2>/dev/null; then
        kill -TERM "$server_pid"
        wait "$server_pid" || true
    fi
    server_pid=
}

trap stop_all EXIT INT TERM

for variant in baseline fixed; do
    if [[ "$variant" == baseline ]]; then
        jar=$baseline_jar
    else
        jar=$fixed_jar
    fi
    variant_dir=$output_dir/$variant
    mkdir -p "$variant_dir"
    echo "START $variant"

    env LD_PRELOAD="$jemalloc" taskset -c "$server_cpus" java \
        --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
        --add-opens=java.base/java.lang=ALL-UNNAMED \
        --sun-misc-unsafe-memory-access=allow \
        --enable-native-access=ALL-UNNAMED \
        -cp "$jar" io.questdb.ServerMain -d "$db_root" \
        >"$variant_dir/server.log" 2>&1 &
    server_pid=$!

    ready=false
    for _ in {1..120}; do
        if taskset -c "$client_cpu" curl -fsS -G \
            --data-urlencode "query=SELECT 1" \
            http://127.0.0.1:9000/exec >/dev/null 2>&1; then
            ready=true
            break
        fi
        if ! kill -0 "$server_pid" 2>/dev/null; then
            tail -100 "$variant_dir/server.log" >&2
            exit 1
        fi
        sleep 0.1
    done
    [[ "$ready" == true ]]
    grep -F "$jemalloc" "/proc/$server_pid/maps" >"$variant_dir/jemalloc-map.txt"

    taskset -c "$client_cpu" python3 "$script_dir/query_client.py" \
        --query-file "$script_dir/query.sql" \
        --output "$variant_dir/warmup.json" \
        --warmups 0 \
        --max-requests "$warmups"

    jcmd "$server_pid" Compiler.perfmap >"$variant_dir/jcmd-before.txt"
    perf c2c record -o "$variant_dir/perf.data" -p "$server_pid" \
        >"$variant_dir/record.stdout" 2>"$variant_dir/record.stderr" &
    perf_pid=$!
    sleep 0.5

    taskset -c "$client_cpu" python3 "$script_dir/query_client.py" \
        --query-file "$script_dir/query.sql" \
        --output "$variant_dir/client.json" \
        --warmups 0 \
        --max-requests "$requests"

    kill -INT "$perf_pid"
    wait "$perf_pid" || true
    perf_pid=
    jcmd "$server_pid" Compiler.perfmap >"$variant_dir/jcmd-after.txt"
    cp "/tmp/perf-$server_pid.map" "$variant_dir/perf.map"

    DEBUGINFOD_URLS= perf c2c report --stdio --stats \
        -i "$variant_dir/perf.data" \
        >"$variant_dir/c2c-stats.txt" \
        2>"$variant_dir/c2c-stats.stderr"
    DEBUGINFOD_URLS= perf c2c report --stdio --full-symbols --show-all \
        -i "$variant_dir/perf.data" \
        >"$variant_dir/c2c-report.txt" \
        2>"$variant_dir/c2c-report.stderr"
    DEBUGINFOD_URLS= perf script \
        -i "$variant_dir/perf.data" \
        -F comm,tid,cpu,addr,ip,sym,data_src \
        2>"$variant_dir/perf-script.stderr" \
        | awk '/SNP HitM/' >"$variant_dir/hitm.txt"
    python3 "$script_dir/group_hitm.py" "$variant_dir/hitm.txt" \
        --symbol AsyncHorizonJoinRecordCursorFactory.processHorizonTimestamps \
        >"$variant_dir/hot-cache-lines.csv"

    echo "DONE  $variant"
    stop_all
done

python3 "$script_dir/summarize.py" c2c "$output_dir"
