#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

path_to_classname() {
    local f="$1"
    f="${f#./}"
    f="${f#core/}"
    f="${f#compat/}"
    f="${f#utils/}"
    f="${f#java-questdb-client/}"
    for prefix in src/test/java/ src/test/kotlin/ src/test/scala/ \
                  src/main/java/ src/main/kotlin/ src/main/scala/ \
                  src/it/java/ src/it/kotlin/ src/it/scala/; do
        f="${f#$prefix}"
    done
    for ext in .java .kt .scala .groovy .kts; do
        f="${f%$ext}"
    done
    echo "${f//\//.}"
}

if [[ $# -eq 0 || -z "${1:-}" ]]; then
    mvn -B -ntp test -pl '!benchmarks' \
        -Dspotless.check.skip=true \
        -Dcheckstyle.skip=true \
        -Dpmd.skip=true \
        -Dcpd.skip=true \
        -Denforcer.skip=true \
        -Dlicense.skip=true \
        -Dformatter.skip=true \
        -Dimpsort.skip=true \
        -Dsort.skip=true \
        -Djacoco.skip=true
else
    IFS=',' read -ra TEST_FILES <<< "$1"
    CLASSES=()
    for file in "${TEST_FILES[@]}"; do
        trimmed="${file#"${file%%[![:space:]]*}"}"
        trimmed="${trimmed%"${trimmed##*[![:space:]]}"}"
        [[ -z "$trimmed" ]] && continue
        CLASSES+=("$(path_to_classname "$trimmed")")
    done

    if [[ ${#CLASSES[@]} -eq 0 ]]; then
        echo "No valid test files were provided." >&2
        exit 1
    fi

    TEST_SELECTOR="$(IFS=,; echo "${CLASSES[*]}")"
    mvn -B -ntp test -pl '!benchmarks' -Dtest="$TEST_SELECTOR" \
        -Dspotless.check.skip=true \
        -Dcheckstyle.skip=true \
        -Dpmd.skip=true \
        -Dcpd.skip=true \
        -Denforcer.skip=true \
        -Dlicense.skip=true \
        -Dformatter.skip=true \
        -Dimpsort.skip=true \
        -Dsort.skip=true \
        -Djacoco.skip=true
fi
