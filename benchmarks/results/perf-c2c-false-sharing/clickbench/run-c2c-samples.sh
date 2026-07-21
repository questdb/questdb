#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 5 ]]; then
    echo "usage: $0 BASELINE_JAR FIXED_JAR DB_ROOT QUERIES_SQL OUTPUT_DIR" >&2
    exit 2
fi

baseline_jar=$(realpath "$1")
fixed_jar=$(realpath "$2")
db_root=$(realpath "$3")
queries_sql=$(realpath "$4")
output_dir=$(realpath -m "$5")
script_dir=$(cd "$(dirname "$0")" && pwd)
repo_root=$(realpath "$script_dir/../../../..")
jemalloc_so=${JEMALLOC_SO:-$repo_root/core/src/main/bin/linux-x86-64/libjemalloc.so}
record_seconds=${RECORD_SECONDS:-5}
server_pid=
client_pid=

mkdir -p "$output_dir"
sha256sum "$baseline_jar" "$fixed_jar" "$jemalloc_so" > "$output_dir/artifact-sha256.txt"

stop_processes() {
    if [[ -n "$client_pid" ]] && kill -0 "$client_pid" 2>/dev/null; then
        kill -TERM "$client_pid" 2>/dev/null || true
        wait "$client_pid" 2>/dev/null || true
    fi
    client_pid=
    if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
        kill -TERM "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
    server_pid=
}

trap stop_processes EXIT INT TERM

query_at() {
    sed -n "$(( $1 + 1 ))p" "$queries_sql"
}

run_sample() {
    local name=$1
    local variant=$2
    local warmup_requests=$3
    local query=$4
    local allow_changing_result=${5:-false}
    local jar run_dir ready perf_map
    local -a result_args=()

    if [[ "$allow_changing_result" == true ]]; then
        result_args=(--allow-changing-result)
    fi

    if [[ "$variant" == baseline ]]; then
        jar=$baseline_jar
    else
        jar=$fixed_jar
    fi
    run_dir="$output_dir/$name"
    if [[ -s "$run_dir/c2c-full.txt" && -s "$run_dir/perf.data" ]]; then
        echo "$name: existing complete recording; skipping"
        return
    fi
    mkdir "$run_dir"
    printf '%s\n' "$query" > "$run_dir/query.sql"
    printf 'sample=%s\nvariant=%s\nwarmups=%s\nrecord_seconds=%s\n' \
        "$name" "$variant" "$warmup_requests" "$record_seconds" > "$run_dir/config.txt"

    echo "$name: starting $variant server"
    taskset -c 0-14 env LD_PRELOAD="$jemalloc_so" java \
        --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
        --add-opens=java.base/java.lang=ALL-UNNAMED \
        --sun-misc-unsafe-memory-access=allow \
        --enable-native-access=ALL-UNNAMED \
        -cp "$jar" io.questdb.ServerMain -d "$db_root" \
        > "$run_dir/server.log" 2>&1 &
    server_pid=$!
    printf '%s\n' "$server_pid" > "$run_dir/server.pid"

    ready=false
    for _ in {1..120}; do
        if taskset -c 15 curl -fsG --data-urlencode "query=SELECT 1" \
            http://127.0.0.1:9000/exec >/dev/null 2>&1; then
            ready=true
            break
        fi
        if ! kill -0 "$server_pid" 2>/dev/null; then
            tail -100 "$run_dir/server.log"
            exit 1
        fi
        sleep 0.1
    done
    if [[ "$ready" != true ]]; then
        echo "QuestDB did not become ready" >&2
        exit 1
    fi

    tr '\0' ' ' < "/proc/$server_pid/cmdline" > "$run_dir/cmdline.txt"
    grep -F "$jemalloc_so" "/proc/$server_pid/maps" > "$run_dir/jemalloc-map.txt"
    taskset -c 15 curl -fsG --data-urlencode "query=EXPLAIN $query" \
        http://127.0.0.1:9000/exec > "$run_dir/plan.json"

    taskset -c 15 python3 "$script_dir/c2c_query_client.py" \
        --query "$query" --warmups 0 --max-requests "$warmup_requests" \
        --output "$run_dir/warmup.json" "${result_args[@]}"
    jcmd "$server_pid" Compiler.perfmap > "$run_dir/jcmd-before-record.txt"

    taskset -c 15 python3 "$script_dir/c2c_query_client.py" \
        --query "$query" --warmups 0 --output "$run_dir/client.json" \
        "${result_args[@]}" &
    client_pid=$!
    sleep 0.2

    # perf resolves the portable c2c selector to ibs_op/ldlat=0/ on this AMD host.
    perf c2c record -e mem-ldst -F 4000 -C 0-15 \
        -o "$run_dir/perf.data" -- sleep "$record_seconds" \
        > "$run_dir/perf-record.stdout" 2> "$run_dir/perf-record.stderr"

    kill -TERM "$client_pid" 2>/dev/null || true
    wait "$client_pid"
    client_pid=
    jcmd "$server_pid" Compiler.perfmap > "$run_dir/jcmd-after-record.txt"
    perf_map="/tmp/perf-$server_pid.map"
    if [[ ! -s "$perf_map" ]]; then
        echo "JIT perf map was not created: $perf_map" >&2
        exit 1
    fi
    cp "$perf_map" "$run_dir/perf-$server_pid.map"
    stop_processes

    DEBUGINFOD_URLS= perf c2c report -i "$run_dir/perf.data" --stats \
        > "$run_dir/c2c-stats.txt" 2> "$run_dir/c2c-stats.stderr"
    DEBUGINFOD_URLS= perf c2c report -i "$run_dir/perf.data" --stdio \
        --show-all --full-symbols > "$run_dir/c2c-full.txt" 2> "$run_dir/c2c-full.stderr"
    DEBUGINFOD_URLS= perf report -i "$run_dir/perf.data" --header-only -I \
        > "$run_dir/perf-header.txt" 2> "$run_dir/perf-header.stderr"
    echo "$name: recording and reports complete"
}

refresh_query="SELECT sum(IsRefresh) FROM hits WHERE DontCountHits = 0"
run_sample calibration-refresh-baseline baseline 50 "$refresh_query"
run_sample calibration-refresh-fixed fixed 50 "$refresh_query"
run_sample q02-non-keyed-multi-aggregate fixed 100 "$(query_at 2)" true
run_sample q04-count-distinct-long fixed 3 "$(query_at 4)" true
run_sample q05-count-distinct-varchar fixed 3 "$(query_at 5)" true
run_sample q08-low-cardinality-count-distinct fixed 10 "$(query_at 8)" true
run_sample q09-low-cardinality-mixed-aggregates fixed 3 "$(query_at 9)" true
run_sample q10-varchar-count-distinct fixed 5 "$(query_at 10)" true
run_sample q11-composite-varchar-count-distinct fixed 5 "$(query_at 11)" true
run_sample q12-varchar-group-by fixed 10 "$(query_at 12)" true
run_sample q13-varchar-group-by-count-distinct fixed 3 "$(query_at 13)" true
run_sample q15-high-cardinality-group-by fixed 10 "$(query_at 15)" true
run_sample q16-high-cardinality-varchar-group-by fixed 1 "$(query_at 16)" true
run_sample q18-expression-varchar-group-by fixed 1 "$(query_at 18)" true
run_sample q20-like-filter-count fixed 3 "$(query_at 20)" true
run_sample q21-like-filter-varchar-group-by fixed 3 "$(query_at 21)" true
run_sample q22-like-filter-mixed-varchar-aggregates fixed 3 "$(query_at 22)" true
run_sample q23-like-filter-wide-top-k fixed 20 "$(query_at 23)" true
run_sample q25-varchar-sort fixed 10 "$(query_at 25)" true
run_sample q28-regex-varchar-group-by fixed 1 "$(query_at 28)" true
run_sample q29-wide-non-keyed-aggregate fixed 30 "$(query_at 29)" true
run_sample q30-filtered-composite-group-by fixed 3 "$(query_at 30)" true
run_sample q32-composite-high-cardinality-group-by fixed 5 "$(query_at 32)" true
run_sample q33-full-varchar-group-by fixed 1 "$(query_at 33)" true
run_sample q35-expression-keys-group-by fixed 3 "$(query_at 35)" true
run_sample q36-filtered-url-group-by fixed 30 "$(query_at 36)" true
run_sample q36-filtered-url-group-by-repeat fixed 30 "$(query_at 36)" true
run_sample q37-filtered-title-group-by fixed 20 "$(query_at 37)" true
run_sample q39-complex-varchar-group-by fixed 10 "$(query_at 39)" true
run_sample q42-sample-by fixed 30 "$(query_at 42)" true

echo "all c2c samples complete"
