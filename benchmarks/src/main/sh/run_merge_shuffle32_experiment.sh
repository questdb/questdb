#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/../../../.." && pwd)
HARNESS_SRC="${REPO_ROOT}/benchmarks/src/main/c/questdb_merge_shuffle32_profile.c"
HARNESS_BIN="${HARNESS_BIN:-/tmp/questdb-merge32-profile}"

BASELINE_LIB="${BASELINE_LIB:-/tmp/qdb-baseline/core/questdb/bin-local/libquestdb.so}"
HIGHWAY_LIB="${HIGHWAY_LIB:-${REPO_ROOT}/core/questdb/bin-local/libquestdb.so}"
OUTPUT_DIR="${OUTPUT_DIR:-/tmp/qdb-merge32-experiment}"
CORE="${CORE:-0}"
WARMUP="${WARMUP:-100}"
ITERATIONS="${ITERATIONS:-}"
TARGET_ROWS="${TARGET_ROWS:-2147483648}"
REPETITIONS="${REPETITIONS:-7}"
ROWS_LIST="${ROWS_LIST:-65536 1048576 4194304}"
OOO_LIST="${OOO_LIST:-1 25 50}"

usage() {
    cat <<EOF
Usage: $0 [options]

Options:
  --baseline-lib PATH    Baseline libquestdb.so
  --highway-lib PATH     Highway libquestdb.so
  --output-dir PATH      Output directory
  --core N               CPU core to pin runs to
  --warmup N             Warmup iterations per run
  --iterations N         Timed iterations per run
  --target-rows N        Total rows processed per timed run when iterations are not fixed
  --repetitions N        Repetitions per matrix point
  --rows "A B C"         Space-separated row counts
  --ooo "A B C"          Space-separated OOO percentages
  --harness-bin PATH     Benchmark harness binary path
EOF
}

while (($# > 0)); do
    case "$1" in
        --baseline-lib)
            BASELINE_LIB="$2"
            shift 2
            ;;
        --highway-lib)
            HIGHWAY_LIB="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --core)
            CORE="$2"
            shift 2
            ;;
        --warmup)
            WARMUP="$2"
            shift 2
            ;;
        --iterations)
            ITERATIONS="$2"
            shift 2
            ;;
        --target-rows)
            TARGET_ROWS="$2"
            shift 2
            ;;
        --repetitions)
            REPETITIONS="$2"
            shift 2
            ;;
        --rows)
            ROWS_LIST="$2"
            shift 2
            ;;
        --ooo)
            OOO_LIST="$2"
            shift 2
            ;;
        --harness-bin)
            HARNESS_BIN="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

mkdir -p "${OUTPUT_DIR}"

if [[ ! -x "${HARNESS_BIN}" || "${HARNESS_SRC}" -nt "${HARNESS_BIN}" ]]; then
    cc -O3 -g -fno-omit-frame-pointer -std=c11 -Wall -Wextra -Werror \
        "${HARNESS_SRC}" -ldl -o "${HARNESS_BIN}"
fi

if [[ ! -f "${BASELINE_LIB}" ]]; then
    echo "Missing baseline lib: ${BASELINE_LIB}" >&2
    exit 1
fi

if [[ ! -f "${HIGHWAY_LIB}" ]]; then
    echo "Missing Highway lib: ${HIGHWAY_LIB}" >&2
    exit 1
fi

RAW_CSV="${OUTPUT_DIR}/raw.csv"
SUMMARY_CSV="${OUTPUT_DIR}/summary.csv"
COMPARE_CSV="${OUTPUT_DIR}/compare.csv"

cat > "${RAW_CSV}" <<EOF
variant,rows,ooo_percent,rep,iterations,elapsed_ns,ns_per_call,rows_per_sec,checksum
EOF

iterations_for_rows() {
    local rows="$1"
    if [[ -n "${ITERATIONS}" ]]; then
        printf '%s\n' "${ITERATIONS}"
        return
    fi

    local derived=$((TARGET_ROWS / rows))
    if ((derived < 1)); then
        derived=1
    fi
    printf '%s\n' "${derived}"
}

run_one() {
    local variant="$1"
    local lib="$2"
    local rows="$3"
    local ooo="$4"
    local rep="$5"
    local iterations="$6"

    local tmp_file
    tmp_file=$(mktemp)
    echo "run variant=${variant} rows=${rows} ooo=${ooo} rep=${rep} iterations=${iterations}" >&2
    taskset -c "${CORE}" "${HARNESS_BIN}" \
        --lib "${lib}" \
        --rows "${rows}" \
        --ooo-percent "${ooo}" \
        --warmup "${WARMUP}" \
        --iterations "${iterations}" > "${tmp_file}"

    awk -F= -v variant="${variant}" -v rows="${rows}" -v ooo="${ooo}" -v rep="${rep}" -v iterations="${iterations}" '
        BEGIN {
            elapsed_ns = ""
            ns_per_call = ""
            rows_per_sec = ""
            checksum = ""
        }
        $1 == "elapsed_ns" { elapsed_ns = $2 }
        $1 == "ns_per_call" { ns_per_call = $2 }
        $1 == "rows_per_sec" { rows_per_sec = $2 }
        $1 == "checksum" { checksum = $2 }
        END {
            if (elapsed_ns == "" || ns_per_call == "" || rows_per_sec == "" || checksum == "") {
                exit 1
            }
            printf "%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
                   variant, rows, ooo, rep, iterations, elapsed_ns, ns_per_call, rows_per_sec, checksum
        }
    ' "${tmp_file}" >> "${RAW_CSV}"
    rm -f "${tmp_file}"
}

for rows in ${ROWS_LIST}; do
    for ooo in ${OOO_LIST}; do
        iterations=$(iterations_for_rows "${rows}")
        for ((rep = 1; rep <= REPETITIONS; rep++)); do
            if (((rep % 2) == 1)); then
                run_one baseline "${BASELINE_LIB}" "${rows}" "${ooo}" "${rep}" "${iterations}"
                run_one highway "${HIGHWAY_LIB}" "${rows}" "${ooo}" "${rep}" "${iterations}"
            else
                run_one highway "${HIGHWAY_LIB}" "${rows}" "${ooo}" "${rep}" "${iterations}"
                run_one baseline "${BASELINE_LIB}" "${rows}" "${ooo}" "${rep}" "${iterations}"
            fi
        done
    done
done

gawk -F, '
    NR == 1 {
        next
    }
    {
        key = $1 "," $2 "," $3
        count[key]++
        ns_per_call[key, count[key]] = $7 + 0
        rows_per_sec[key, count[key]] = $8 + 0
    }
    END {
        print "variant,rows,ooo_percent,samples,median_ns_per_call,min_ns_per_call,max_ns_per_call,mean_ns_per_call,median_rows_per_sec"
        nkeys = asorti(count, keys)
        for (ki = 1; ki <= nkeys; ki++) {
            key = keys[ki]
            n = count[key]
            delete ns_vals
            delete rps_vals
            sum = 0
            for (i = 1; i <= n; i++) {
                ns_vals[i] = ns_per_call[key, i]
                rps_vals[i] = rows_per_sec[key, i]
                sum += ns_per_call[key, i]
            }
            asort(ns_vals)
            asort(rps_vals)
            if ((n % 2) == 1) {
                median_ns = ns_vals[(n + 1) / 2]
                median_rps = rps_vals[(n + 1) / 2]
            } else {
                median_ns = (ns_vals[n / 2] + ns_vals[n / 2 + 1]) / 2.0
                median_rps = (rps_vals[n / 2] + rps_vals[n / 2 + 1]) / 2.0
            }
            min_ns = ns_vals[1]
            max_ns = ns_vals[n]
            mean_ns = sum / n
            printf "%s,%d,%.3f,%.3f,%.3f,%.3f,%.3f\n",
                   key, n, median_ns, min_ns, max_ns, mean_ns, median_rps
        }
    }
' "${RAW_CSV}" > "${SUMMARY_CSV}"

gawk -F, '
    NR == 1 {
        next
    }
    {
        key = $2 "," $3
        ns[$1, key] = $5 + 0
        rps[$1, key] = $9 + 0
    }
    END {
        print "rows,ooo_percent,baseline_median_ns_per_call,highway_median_ns_per_call,delta_pct,baseline_median_rows_per_sec,highway_median_rows_per_sec"
        nkeys = asorti(rps, keys)
        delete seen
        for (i = 1; i <= nkeys; i++) {
            split(keys[i], parts, SUBSEP)
            variant = parts[1]
            key = parts[2]
            if (variant != "baseline" || seen[key]) {
                continue
            }
            seen[key] = 1
            base_ns = ns["baseline", key]
            hw_ns = ns["highway", key]
            base_rps = rps["baseline", key]
            hw_rps = rps["highway", key]
            delta_pct = ((hw_ns - base_ns) / base_ns) * 100.0
            print key "," base_ns "," hw_ns "," delta_pct "," base_rps "," hw_rps
        }
    }
' "${SUMMARY_CSV}" > "${COMPARE_CSV}"

echo "raw_csv=${RAW_CSV}"
echo "summary_csv=${SUMMARY_CSV}"
echo "compare_csv=${COMPARE_CSV}"
