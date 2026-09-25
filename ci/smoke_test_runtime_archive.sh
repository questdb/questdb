#!/usr/bin/env bash
#
# Start the packaged server from a QuestDB runtime archive and run one query.
#
# This exercises the exact artifact users download: the jlink runtime, the
# native libraries under <root>/lib/, and the launcher. On Linux and macOS the
# archive's bin/questdb.sh starts the server, which is how it points the JVM
# at lib/ through -Dquestdb.libs.dir. On Windows the archive ships questdb.exe
# (a service wrapper) instead of a shell launcher, so the script runs the
# embedded bin/java.exe with the same module options the wrapper passes; the
# JVM then finds lib/ by jlink-runtime detection, as it does in production.
#
# Fails when the server does not answer within the timeout, when the answer
# is not the smoke query's result, or when the server logged that it fell back
# to jar-embedded native libraries after failing to load the ones in lib/.
#
# Usage: smoke_test_runtime_archive.sh <questdb-<version>-rt-<platform>.tar.gz>
set -euo pipefail

archive="${1:?usage: smoke_test_runtime_archive.sh <runtime-archive.tar.gz>}"
[[ -f "${archive}" ]] || { echo "runtime archive not found: ${archive}" >&2; exit 2; }

work_dir="$(mktemp -d)"
server_pid=""
launcher=""

stop_server() {
    if [[ -n "${launcher}" ]]; then
        "${launcher}" stop > /dev/null 2>&1 || true
    elif [[ -n "${server_pid}" ]]; then
        kill "${server_pid}" > /dev/null 2>&1 || true
        wait "${server_pid}" 2>/dev/null || true
    fi
}
trap stop_server EXIT

# CI pipelines export QDB_LOG_* overrides for unit-test JVMs. They would
# replace the packaged log configuration, whose rolling-file writer rejects a
# location without a `$` and then keeps the server from starting.
for name in $(env | grep -o '^QDB_LOG_[A-Za-z0-9_]*' || true); do
    unset "${name}"
done

tar -xzf "${archive}" -C "${work_dir}"
dist_dir="$(find "${work_dir}" -mindepth 1 -maxdepth 1 -type d -name 'questdb-*-rt-*' | head -1)"
[[ -n "${dist_dir}" ]] || { echo "no questdb-*-rt-* directory inside ${archive}" >&2; exit 1; }
echo "Smoke-testing ${archive} (${dist_dir##*/})"

root="${work_dir}/smoke-root"
mkdir -p "${root}/log"

if [[ -x "${dist_dir}/bin/questdb.sh" ]]; then
    launcher="${dist_dir}/bin/questdb.sh"
    "${launcher}" start -d "${root}"
else
    java_binary="${dist_dir}/bin/java"
    [[ -x "${java_binary}" ]] || java_binary="${dist_dir}/bin/java.exe"
    [[ -x "${java_binary}" ]] || { echo "neither bin/questdb.sh nor an embedded java found in ${dist_dir}" >&2; exit 1; }
    root_native="${root}"
    if command -v cygpath > /dev/null 2>&1; then
        root_native="$(cygpath -w "${root}")"
    fi
    "${java_binary}" \
        -Dcontainerized=false \
        -ea \
        -XX:+UseParallelGC \
        --sun-misc-unsafe-memory-access=allow \
        --enable-native-access=io.questdb \
        --add-opens=java.base/java.lang=io.questdb \
        --add-opens=java.base/java.lang.reflect=io.questdb \
        --add-opens=java.base/java.nio=io.questdb \
        --add-opens=java.base/java.time.zone=io.questdb \
        --add-exports=java.base/jdk.internal.vm=io.questdb \
        -m io.questdb/io.questdb.ServerMain \
        -d "${root_native}" > "${root}/log/stdout-smoke.txt" 2>&1 &
    server_pid=$!
fi

response="${work_dir}/exp.out"
answered=0
for _ in $(seq 1 60); do
    if curl -fsS -G --data-urlencode "query=SELECT 1 AS smoke" "http://127.0.0.1:9000/exp" -o "${response}"; then
        answered=1
        break
    fi
    sleep 2
done

echo "--- /exp response ---"
cat "${response}" 2>/dev/null || true
echo
stop_server
trap - EXIT

if [[ "${answered}" -ne 1 ]]; then
    echo "ERROR: packaged QuestDB did not serve a query within the timeout" >&2
    echo "--- server logs ---"
    tail -n 200 "${root}"/log/* 2>/dev/null || true
    exit 1
fi
if ! grep -q '"smoke"' "${response}"; then
    echo "ERROR: /exp answered, but not with the smoke query's result" >&2
    exit 1
fi
# Os.tryLoadFromDistribution prints this before falling back to the
# jar-embedded copies, so a hit means the archive's lib/ did not load.
if grep -rl "Failed to load libraries from" "${root}/log" 2>/dev/null; then
    echo "ERROR: the packaged server fell back to jar-embedded native libraries" >&2
    grep -rn "Failed to load libraries from" "${root}/log" || true
    exit 1
fi
echo "Smoke test passed: ${dist_dir##*/} started from lib/ and served a query."
