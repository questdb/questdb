#!/usr/bin/env bash
#
# Copyright (c) 2014-2019 Appsicle
# Copyright (c) 2019-2026 QuestDB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "${script_dir}/../../../.." && pwd)

# shellcheck disable=SC1091
source "${script_dir}/versions.env"

java_cmd="${JAVA_HOME:+${JAVA_HOME}/bin/}java"
javac_cmd="${JAVA_HOME:+${JAVA_HOME}/bin/}javac"
curl_cmd=${CURL:-curl}
sha256_cmd=${SHA256SUM:-sha256sum}

for command_path in "$java_cmd" "$javac_cmd" "$curl_cmd" "$sha256_cmd" timeout; do
    if ! command -v "$command_path" >/dev/null 2>&1; then
        echo "required command not found: $command_path" >&2
        exit 1
    fi
done

maven_repo=${MAVEN_CACHE_FOLDER:-${HOME}/.m2/repository}
artifact_base=${QWP_COMPAT_ARTIFACT_BASE_URL:-https://repo.maven.apache.org/maven2}
work_root=${QWP_COMPAT_WORK_ROOT:-${repo_root}/compat/target/qwp-schema-compat}
artifact_dir=${QWP_COMPAT_ARTIFACT_DIR:-${repo_root}/compat/target/qwp-schema-compat-artifacts}
mkdir -p "$work_root"
run_dir=$(mktemp -d "${work_root}/run.XXXXXX")
classes_dir="${run_dir}/classes"
mkdir -p "$classes_dir"

legacy_client_jar="${artifact_dir}/questdb-client-${QWP_LEGACY_CLIENT_VERSION}.jar"
legacy_server_jar="${artifact_dir}/questdb-${QWP_LEGACY_SERVER_VERSION}.jar"
slf4j_jar="${artifact_dir}/slf4j-api-${QWP_SLF4J_VERSION}.jar"

download_verified() {
    local url=$1
    local path=$2
    local expected=$3
    local actual

    if [[ -f "$path" ]]; then
        actual=$("$sha256_cmd" "$path" | awk '{print $1}')
        if [[ "$actual" == "$expected" ]]; then
            return
        fi
        echo "cached artifact checksum mismatch, downloading a verified copy: $path" >&2
    fi

    mkdir -p "$(dirname "$path")"
    local partial="${path}.qwp-compat.$$"
    "$curl_cmd" --fail --location --silent --show-error \
        --connect-timeout 10 --max-time 120 --retry 3 --retry-delay 1 \
        --output "$partial" "$url"
    actual=$("$sha256_cmd" "$partial" | awk '{print $1}')
    if [[ "$actual" != "$expected" ]]; then
        echo "artifact checksum mismatch [url=$url, expected=$expected, actual=$actual]" >&2
        return 1
    fi
    mv -f "$partial" "$path"
}

download_verified \
    "${artifact_base}/org/questdb/questdb-client/${QWP_LEGACY_CLIENT_VERSION}/questdb-client-${QWP_LEGACY_CLIENT_VERSION}.jar" \
    "$legacy_client_jar" \
    "$QWP_LEGACY_CLIENT_SHA256"
download_verified \
    "${artifact_base}/org/questdb/questdb/${QWP_LEGACY_SERVER_VERSION}/questdb-${QWP_LEGACY_SERVER_VERSION}.jar" \
    "$legacy_server_jar" \
    "$QWP_LEGACY_SERVER_SHA256"
download_verified \
    "${artifact_base}/org/slf4j/slf4j-api/${QWP_SLF4J_VERSION}/slf4j-api-${QWP_SLF4J_VERSION}.jar" \
    "$slf4j_jar" \
    "$QWP_SLF4J_SHA256"

server_version=$(sed -n 's:.*<version>\([^<]*\)</version>.*:\1:p' "${repo_root}/core/pom.xml" | head -1)
client_version=$(sed -n 's:.*<questdb.client.version>\([^<]*\)</questdb.client.version>.*:\1:p' "${repo_root}/core/pom.xml" | head -1)
current_server_jar=${QWP_CURRENT_SERVER_JAR:-${repo_root}/core/target/questdb-${server_version}.jar}
if [[ -n "${QWP_CURRENT_CLIENT_JAR:-}" ]]; then
    current_client_jar=$QWP_CURRENT_CLIENT_JAR
elif [[ "$client_version" == *-SNAPSHOT ]]; then
    current_client_jar=${repo_root}/java-questdb-client/core/target/questdb-client-${client_version}.jar
else
    current_client_jar=${maven_repo}/org/questdb/questdb-client/${client_version}/questdb-client-${client_version}.jar
fi

if [[ ! -f "$current_server_jar" ]]; then
    echo "current server JAR not found: $current_server_jar" >&2
    echo "build it with: mvn -pl core -am -DskipTests package -P local-client" >&2
    exit 1
fi
if [[ ! -f "$current_client_jar" ]]; then
    echo "current client JAR not found: $current_client_jar" >&2
    exit 1
fi

"$javac_cmd" --release 11 \
    -cp "${legacy_client_jar}:${slf4j_jar}" \
    -d "$classes_dir" \
    "${script_dir}/QwpClientCompatibilityProbe.java"

legacy_port=${QWP_COMPAT_LEGACY_PORT:-19000}
current_port=${QWP_COMPAT_CURRENT_PORT:-19001}
server_pid=

assert_port_free() {
    local port=$1
    if (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then
        echo "compatibility-test port is already in use: $port" >&2
        return 1
    fi
}

stop_server() {
    if [[ -n "$server_pid" ]] && kill -0 "$server_pid" >/dev/null 2>&1; then
        kill "$server_pid" >/dev/null 2>&1 || true
        for _ in {1..40}; do
            if ! kill -0 "$server_pid" >/dev/null 2>&1; then
                break
            fi
            sleep 0.25
        done
        if kill -0 "$server_pid" >/dev/null 2>&1; then
            kill -KILL "$server_pid" >/dev/null 2>&1 || true
        fi
        wait "$server_pid" >/dev/null 2>&1 || true
    fi
    server_pid=
}

cleanup() {
    stop_server
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

wait_for_server() {
    local pid=$1
    local port=$2
    local log_file=$3
    local deadline=$((SECONDS + 30))

    while ((SECONDS < deadline)); do
        if ! kill -0 "$pid" >/dev/null 2>&1; then
            echo "server exited before becoming ready [port=$port]" >&2
            tail -200 "$log_file" >&2 || true
            return 1
        fi
        if "$curl_cmd" --fail --silent --show-error --max-time 2 \
            --get --data-urlencode 'query=select 1' \
            "http://127.0.0.1:${port}/exec" >/dev/null 2>&1; then
            return
        fi
        sleep 0.25
    done

    echo "server readiness timed out [port=$port]" >&2
    tail -200 "$log_file" >&2 || true
    return 1
}

start_server() {
    local jar=$1
    local port=$2
    local root=$3
    local log_file=$4

    mkdir -p "$root"
    QDB_HTTP_BIND_TO="127.0.0.1:${port}" \
    QDB_HTTP_MIN_ENABLED=false \
    QDB_LINE_TCP_ENABLED=false \
    QDB_LINE_UDP_ENABLED=false \
    QDB_METRICS_ENABLED=false \
    QDB_PG_ENABLED=false \
    QDB_TELEMETRY_ENABLED=false \
        "$java_cmd" -ea \
        --enable-native-access=ALL-UNNAMED \
        --add-opens=java.base/java.lang=ALL-UNNAMED \
        --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
        --add-opens=java.base/java.nio=ALL-UNNAMED \
        --add-opens=java.base/java.time.zone=ALL-UNNAMED \
        --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
        -jar "$jar" -d "$root" -f >"$log_file" 2>&1 &
    server_pid=$!
    wait_for_server "$server_pid" "$port" "$log_file"
}

exec_sql() {
    local port=$1
    local sql=$2
    local response

    response=$("$curl_cmd" --fail --silent --show-error --max-time 10 \
        --get --data-urlencode "query=$sql" \
        "http://127.0.0.1:${port}/exec")
    if [[ "$response" != *'"ddl":"OK"'* ]]; then
        echo "DDL failed [sql=$sql, response=$response]" >&2
        return 1
    fi
}

run_probe() {
    local client_jar=$1
    local port=$2
    local table=$3
    # Keep all three rows in one table block. The omitted value in the last row
    # is what makes legacy LONG_MIN handling observable; an earlier automatic
    # flush could remove that bitmap and make the gate nondeterministic.
    local config="ws::addr=127.0.0.1:${port};auto_flush_rows=2147483647;auto_flush_bytes=0;auto_flush_interval=2147483646;connect_timeout=5000;close_flush_timeout_millis=5000;"

    timeout --kill-after=5s 45s "$java_cmd" --enable-native-access=ALL-UNNAMED \
        -cp "${classes_dir}:${client_jar}:${slf4j_jar}" \
        io.questdb.compat.qwp.QwpClientCompatibilityProbe \
        "$config" "$table"
}

wait_for_csv() {
    local port=$1
    local sql=$2
    local expected=$3
    local label=$4
    local actual=
    local deadline=$((SECONDS + 30))

    while ((SECONDS < deadline)); do
        if [[ -n "$server_pid" ]] && ! kill -0 "$server_pid" >/dev/null 2>&1; then
            echo "server exited while waiting for rows [case=$label]" >&2
            return 1
        fi
        if actual=$("$curl_cmd" --fail --silent --show-error --max-time 2 \
            --get --data-urlencode "query=$sql" \
            "http://127.0.0.1:${port}/exp" | tr -d '\r'); then
            if [[ "$actual" == "$expected" ]]; then
                echo "PASS: $label"
                return
            fi
        fi
        sleep 0.25
    done

    echo "stored rows did not match [case=$label]" >&2
    echo "expected:" >&2
    printf '%s\n' "$expected" >&2
    echo "actual:" >&2
    printf '%s\n' "$actual" >&2
    return 1
}

run_case() {
    local label=$1
    local client_jar=$2
    local port=$3
    local table=$4
    local expected=$5

    exec_sql "$port" "create table ${table} (case_id long, value float, ts timestamp) timestamp(ts) partition by day wal"
    run_probe "$client_jar" "$port" "$table"
    wait_for_csv "$port" "select case_id, value from ${table} order by case_id" "$expected" "$label"
}

legacy_expected=$'"case_id","value"\n1,42.0\n2,-9.223372E18\n3,'
schema_expected=$'"case_id","value"\n1,42.0\n2,\n3,'

echo "QWP compatibility artifacts:"
echo "  legacy client ${QWP_LEGACY_CLIENT_VERSION} (${QWP_LEGACY_CLIENT_SHA256})"
echo "  legacy server ${QWP_LEGACY_SERVER_VERSION} (${QWP_LEGACY_SERVER_SHA256})"
echo "  current client ${client_version} ($("$sha256_cmd" "$current_client_jar" | awk '{print $1}'))"
echo "  current server ${server_version} ($("$sha256_cmd" "$current_server_jar" | awk '{print $1}'))"
echo "  logs ${run_dir}"

assert_port_free "$legacy_port"
if [[ "$current_port" != "$legacy_port" ]]; then
    assert_port_free "$current_port"
fi

start_server "$legacy_server_jar" "$legacy_port" "${run_dir}/legacy-server-root" "${run_dir}/legacy-server.log"
run_case \
    "current client -> legacy server uses legacy conversion" \
    "$current_client_jar" "$legacy_port" "qwp_new_client_old_server" "$legacy_expected"
stop_server

start_server "$current_server_jar" "$current_port" "${run_dir}/current-server-root" "${run_dir}/current-server.log"
run_case \
    "legacy client -> current server remains legacy" \
    "$legacy_client_jar" "$current_port" "qwp_old_client_new_server" "$legacy_expected"
run_case \
    "current client -> current server enables schema mode" \
    "$current_client_jar" "$current_port" "qwp_new_client_new_server" "$schema_expected"
stop_server

echo "QWP schema compatibility gate passed"
