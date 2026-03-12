#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/../../../.." && pwd)
C_SRC="${REPO_ROOT}/benchmarks/src/main/c"

BASELINE_LIB="${BASELINE_LIB:-/tmp/qdb-baseline/core/questdb/bin-local/libquestdb.so}"
HIGHWAY_LIB="${HIGHWAY_LIB:-${REPO_ROOT}/core/questdb/bin-local/libquestdb.so}"
OUTPUT_DIR="${OUTPUT_DIR:-/tmp/qdb-ooo-benchmarks}"
CORE="${CORE:-0}"
WARMUP="${WARMUP:-100}"
ITERATIONS="${ITERATIONS:-}"
TARGET_ROWS="${TARGET_ROWS:-2147483648}"
REPETITIONS="${REPETITIONS:-7}"
ROWS_LIST="${ROWS_LIST:-65536 1048576 4194304}"
OOO_LIST="${OOO_LIST:-1 25 50}"

usage() {
    cat <<EOF
Usage: $0 [options] [kernel ...]

Run O3 kernel benchmarks comparing baseline vs Highway.

Kernels (default: all O3 kernels):
  mergeShuffle64Bit
  indexReshuffle32Bit
  indexReshuffle64Bit
  copyFromTimestampIndex
  oooCopyIndex
  shiftTimestampIndex
  flattenIndex

Vectorized aggregation kernels:
  sumDouble sumDoubleKahan sumDoubleNeumaier minDouble maxDouble countDouble
  sumInt minInt maxInt countInt
  sumLong minLong maxLong countLong
  sumShort minShort maxShort

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
  --ooo "A B C"          Space-separated OOO percentages (mergeShuffle only)
EOF
}

KERNELS=()

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
        -h|--help)
            usage
            exit 0
            ;;
        -*)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
        *)
            KERNELS+=("$1")
            shift
            ;;
    esac
done

if [[ ${#KERNELS[@]} -eq 0 ]]; then
    KERNELS=(
        mergeShuffle64Bit
        indexReshuffle32Bit
        indexReshuffle64Bit
        copyFromTimestampIndex
        oooCopyIndex
        shiftTimestampIndex
        flattenIndex
    )
fi

mkdir -p "${OUTPUT_DIR}"

if [[ ! -f "${BASELINE_LIB}" ]]; then
    echo "Missing baseline lib: ${BASELINE_LIB}" >&2
    exit 1
fi

if [[ ! -f "${HIGHWAY_LIB}" ]]; then
    echo "Missing Highway lib: ${HIGHWAY_LIB}" >&2
    exit 1
fi

# ---- compile harnesses ----

BIN_MERGE64="${OUTPUT_DIR}/questdb-merge64-profile"
BIN_RESHUFFLE="${OUTPUT_DIR}/questdb-reshuffle-profile"
BIN_TS_INDEX="${OUTPUT_DIR}/questdb-ts-index-profile"
BIN_VEC_AGG="${OUTPUT_DIR}/questdb-vec-agg-profile"

compile_if_needed() {
    local src="$1"
    local bin="$2"
    if [[ ! -x "${bin}" || "${src}" -nt "${bin}" ]]; then
        echo "compiling ${src} -> ${bin}" >&2
        cc -O3 -g -fno-omit-frame-pointer -std=c11 -Wall -Wextra -Werror \
            "${src}" -ldl -o "${bin}"
    fi
}

compile_if_needed "${C_SRC}/questdb_merge_shuffle64_profile.c" "${BIN_MERGE64}"
compile_if_needed "${C_SRC}/questdb_index_reshuffle_profile.c" "${BIN_RESHUFFLE}"
compile_if_needed "${C_SRC}/questdb_timestamp_index_profile.c" "${BIN_TS_INDEX}"
compile_if_needed "${C_SRC}/questdb_vec_agg_profile.c" "${BIN_VEC_AGG}"

# ---- common helpers ----

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

# Run a single benchmark point and append to a CSV
# run_point <csv> <variant> <lib> <harness_bin> <harness_args...>
run_point() {
    local csv="$1"
    local variant="$2"
    local lib="$3"
    local bin="$4"
    shift 4

    local tmp_file
    tmp_file=$(mktemp)

    echo "  ${variant}: ${bin##*/} --lib ${lib##*/} $*" >&2
    taskset -c "${CORE}" "${bin}" --lib "${lib}" "$@" > "${tmp_file}"

    # Parse key=value output into CSV columns
    local elapsed_ns ns_per_call rows_per_sec checksum
    elapsed_ns=$(awk -F= '$1=="elapsed_ns"{print $2}' "${tmp_file}")
    ns_per_call=$(awk -F= '$1=="ns_per_call"{print $2}' "${tmp_file}")
    rows_per_sec=$(awk -F= '$1=="rows_per_sec"{print $2}' "${tmp_file}")
    checksum=$(awk -F= '$1=="checksum"{print $2}' "${tmp_file}")

    if [[ -z "${elapsed_ns}" || -z "${ns_per_call}" || -z "${rows_per_sec}" || -z "${checksum}" ]]; then
        echo "ERROR: incomplete output from harness" >&2
        cat "${tmp_file}" >&2
        rm -f "${tmp_file}"
        return 1
    fi

    echo "${variant},${ns_per_call},${rows_per_sec},${checksum}" >> "${csv}"
    rm -f "${tmp_file}"
}

# Summarize a raw CSV into medians and produce a comparison
# summarize <raw_csv> <summary_csv> <compare_csv> <key_columns>
summarize() {
    local raw_csv="$1"
    local summary_csv="$2"
    local compare_csv="$3"

    # Compute medians per (variant, config) group
    # Raw CSV format: variant,ns_per_call,rows_per_sec,checksum
    # plus any config columns prepended before variant
    gawk -F, '
        NR == 1 { next }
        {
            # Everything before the last 4 fields is the config key
            nf = NF
            key = ""
            for (i = 1; i <= nf - 4; i++) {
                if (key != "") key = key ","
                key = key $i
            }
            variant = $(nf - 3)
            ns = $(nf - 2) + 0
            rps = $(nf - 1) + 0

            full_key = key "," variant
            count[full_key]++
            ns_arr[full_key, count[full_key]] = ns
            rps_arr[full_key, count[full_key]] = rps
        }
        END {
            print "config,variant,samples,median_ns_per_call,min_ns_per_call,max_ns_per_call,median_rows_per_sec"
            nkeys = asorti(count, keys)
            for (ki = 1; ki <= nkeys; ki++) {
                k = keys[ki]
                n = count[k]
                delete ns_vals
                delete rps_vals
                for (i = 1; i <= n; i++) {
                    ns_vals[i] = ns_arr[k, i]
                    rps_vals[i] = rps_arr[k, i]
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
                printf "%s,%d,%.3f,%.3f,%.3f,%.3f\n", k, n, median_ns, min_ns, max_ns, median_rps
            }
        }
    ' "${raw_csv}" > "${summary_csv}"

    # Produce baseline vs highway comparison.
    # The summary key is config + "," + variant, where config may itself
    # contain commas.  Find "baseline"/"highway" by scanning from the right:
    # the variant is always followed by samples, median_ns, min_ns, max_ns, median_rps
    # (5 trailing numeric fields).
    gawk -F, '
        NR == 1 { next }
        {
            nf = NF
            # variant is at position nf - 5 (6 fields from the end: variant,samples,median_ns,min_ns,max_ns,median_rps)
            variant_col = nf - 5
            config = ""
            for (i = 1; i < variant_col; i++) {
                if (config != "") config = config ","
                config = config $i
            }
            variant = $variant_col
            median_ns = $(variant_col + 2) + 0
            median_rps = $nf + 0
            ns[config, variant] = median_ns
            rps[config, variant] = median_rps
        }
        END {
            print "config,baseline_median_ns,highway_median_ns,delta_pct,baseline_median_rps,highway_median_rps"
            nkeys = asorti(ns, keys)
            delete seen
            for (i = 1; i <= nkeys; i++) {
                split(keys[i], parts, SUBSEP)
                config = parts[1]
                variant = parts[2]
                if (variant != "baseline" || seen[config]) continue
                seen[config] = 1
                b_ns = ns[config, "baseline"]
                h_ns = ns[config, "highway"]
                b_rps = rps[config, "baseline"]
                h_rps = rps[config, "highway"]
                delta = (b_ns > 0) ? ((h_ns - b_ns) / b_ns) * 100.0 : 0
                printf "%s,%.3f,%.3f,%.2f,%.3f,%.3f\n", config, b_ns, h_ns, delta, b_rps, h_rps
            }
        }
    ' "${summary_csv}" > "${compare_csv}"
}

# ---- kernel runners ----

run_merge_shuffle64() {
    local kernel_dir="${OUTPUT_DIR}/mergeShuffle64Bit"
    mkdir -p "${kernel_dir}"
    local raw="${kernel_dir}/raw.csv"
    echo "rows,ooo_percent,variant,ns_per_call,rows_per_sec,checksum" > "${raw}"

    echo "=== mergeShuffle64Bit ===" >&2
    for rows in ${ROWS_LIST}; do
        for ooo in ${OOO_LIST}; do
            local iters
            iters=$(iterations_for_rows "${rows}")
            for ((rep = 1; rep <= REPETITIONS; rep++)); do
                if (((rep % 2) == 1)); then
                    printf '%s,%s,' "${rows}" "${ooo}" >> "${raw}"
                    run_point "${raw}" baseline "${BASELINE_LIB}" "${BIN_MERGE64}" \
                        --rows "${rows}" --ooo-percent "${ooo}" --warmup "${WARMUP}" --iterations "${iters}"
                    printf '%s,%s,' "${rows}" "${ooo}" >> "${raw}"
                    run_point "${raw}" highway "${HIGHWAY_LIB}" "${BIN_MERGE64}" \
                        --rows "${rows}" --ooo-percent "${ooo}" --warmup "${WARMUP}" --iterations "${iters}"
                else
                    printf '%s,%s,' "${rows}" "${ooo}" >> "${raw}"
                    run_point "${raw}" highway "${HIGHWAY_LIB}" "${BIN_MERGE64}" \
                        --rows "${rows}" --ooo-percent "${ooo}" --warmup "${WARMUP}" --iterations "${iters}"
                    printf '%s,%s,' "${rows}" "${ooo}" >> "${raw}"
                    run_point "${raw}" baseline "${BASELINE_LIB}" "${BIN_MERGE64}" \
                        --rows "${rows}" --ooo-percent "${ooo}" --warmup "${WARMUP}" --iterations "${iters}"
                fi
            done
        done
    done

    summarize "${raw}" "${kernel_dir}/summary.csv" "${kernel_dir}/compare.csv"
    echo "  results: ${kernel_dir}/compare.csv" >&2
}

run_index_reshuffle() {
    local width="$1"
    local kernel_dir="${OUTPUT_DIR}/indexReshuffle${width}Bit"
    mkdir -p "${kernel_dir}"
    local raw="${kernel_dir}/raw.csv"
    echo "rows,variant,ns_per_call,rows_per_sec,checksum" > "${raw}"

    echo "=== indexReshuffle${width}Bit ===" >&2
    for rows in ${ROWS_LIST}; do
        local iters
        iters=$(iterations_for_rows "${rows}")
        for ((rep = 1; rep <= REPETITIONS; rep++)); do
            if (((rep % 2) == 1)); then
                printf '%s,' "${rows}" >> "${raw}"
                run_point "${raw}" baseline "${BASELINE_LIB}" "${BIN_RESHUFFLE}" \
                    --rows "${rows}" --width "${width}" --warmup "${WARMUP}" --iterations "${iters}"
                printf '%s,' "${rows}" >> "${raw}"
                run_point "${raw}" highway "${HIGHWAY_LIB}" "${BIN_RESHUFFLE}" \
                    --rows "${rows}" --width "${width}" --warmup "${WARMUP}" --iterations "${iters}"
            else
                printf '%s,' "${rows}" >> "${raw}"
                run_point "${raw}" highway "${HIGHWAY_LIB}" "${BIN_RESHUFFLE}" \
                    --rows "${rows}" --width "${width}" --warmup "${WARMUP}" --iterations "${iters}"
                printf '%s,' "${rows}" >> "${raw}"
                run_point "${raw}" baseline "${BASELINE_LIB}" "${BIN_RESHUFFLE}" \
                    --rows "${rows}" --width "${width}" --warmup "${WARMUP}" --iterations "${iters}"
            fi
        done
    done

    summarize "${raw}" "${kernel_dir}/summary.csv" "${kernel_dir}/compare.csv"
    echo "  results: ${kernel_dir}/compare.csv" >&2
}

run_ts_index_kernel() {
    local kernel_name="$1"
    local kernel_dir="${OUTPUT_DIR}/${kernel_name}"
    mkdir -p "${kernel_dir}"
    local raw="${kernel_dir}/raw.csv"
    echo "rows,variant,ns_per_call,rows_per_sec,checksum" > "${raw}"

    echo "=== ${kernel_name} ===" >&2
    for rows in ${ROWS_LIST}; do
        local iters
        iters=$(iterations_for_rows "${rows}")
        for ((rep = 1; rep <= REPETITIONS; rep++)); do
            if (((rep % 2) == 1)); then
                printf '%s,' "${rows}" >> "${raw}"
                run_point "${raw}" baseline "${BASELINE_LIB}" "${BIN_TS_INDEX}" \
                    --rows "${rows}" --kernel "${kernel_name}" --warmup "${WARMUP}" --iterations "${iters}"
                printf '%s,' "${rows}" >> "${raw}"
                run_point "${raw}" highway "${HIGHWAY_LIB}" "${BIN_TS_INDEX}" \
                    --rows "${rows}" --kernel "${kernel_name}" --warmup "${WARMUP}" --iterations "${iters}"
            else
                printf '%s,' "${rows}" >> "${raw}"
                run_point "${raw}" highway "${HIGHWAY_LIB}" "${BIN_TS_INDEX}" \
                    --rows "${rows}" --kernel "${kernel_name}" --warmup "${WARMUP}" --iterations "${iters}"
                printf '%s,' "${rows}" >> "${raw}"
                run_point "${raw}" baseline "${BASELINE_LIB}" "${BIN_TS_INDEX}" \
                    --rows "${rows}" --kernel "${kernel_name}" --warmup "${WARMUP}" --iterations "${iters}"
            fi
        done
    done

    summarize "${raw}" "${kernel_dir}/summary.csv" "${kernel_dir}/compare.csv"
    echo "  results: ${kernel_dir}/compare.csv" >&2
}

VEC_AGG_KERNELS="sumDouble sumDoubleKahan sumDoubleNeumaier minDouble maxDouble countDouble sumInt minInt maxInt countInt sumLong minLong maxLong countLong sumShort minShort maxShort"

is_vec_agg_kernel() {
    local name="$1"
    for k in ${VEC_AGG_KERNELS}; do
        if [[ "${k}" == "${name}" ]]; then
            return 0
        fi
    done
    return 1
}

run_vec_agg_kernel() {
    local kernel_name="$1"
    local kernel_dir="${OUTPUT_DIR}/${kernel_name}"
    mkdir -p "${kernel_dir}"
    local raw="${kernel_dir}/raw.csv"
    echo "rows,variant,ns_per_call,rows_per_sec,checksum" > "${raw}"

    echo "=== ${kernel_name} ===" >&2
    for rows in ${ROWS_LIST}; do
        local iters
        iters=$(iterations_for_rows "${rows}")
        for ((rep = 1; rep <= REPETITIONS; rep++)); do
            if (((rep % 2) == 1)); then
                printf '%s,' "${rows}" >> "${raw}"
                run_point "${raw}" baseline "${BASELINE_LIB}" "${BIN_VEC_AGG}" \
                    --rows "${rows}" --kernel "${kernel_name}" --warmup "${WARMUP}" --iterations "${iters}"
                printf '%s,' "${rows}" >> "${raw}"
                run_point "${raw}" highway "${HIGHWAY_LIB}" "${BIN_VEC_AGG}" \
                    --rows "${rows}" --kernel "${kernel_name}" --warmup "${WARMUP}" --iterations "${iters}"
            else
                printf '%s,' "${rows}" >> "${raw}"
                run_point "${raw}" highway "${HIGHWAY_LIB}" "${BIN_VEC_AGG}" \
                    --rows "${rows}" --kernel "${kernel_name}" --warmup "${WARMUP}" --iterations "${iters}"
                printf '%s,' "${rows}" >> "${raw}"
                run_point "${raw}" baseline "${BASELINE_LIB}" "${BIN_VEC_AGG}" \
                    --rows "${rows}" --kernel "${kernel_name}" --warmup "${WARMUP}" --iterations "${iters}"
            fi
        done
    done

    summarize "${raw}" "${kernel_dir}/summary.csv" "${kernel_dir}/compare.csv"
    echo "  results: ${kernel_dir}/compare.csv" >&2
}

# ---- main loop ----

for kernel in "${KERNELS[@]}"; do
    case "${kernel}" in
        mergeShuffle64Bit)
            run_merge_shuffle64
            ;;
        indexReshuffle32Bit)
            run_index_reshuffle 32
            ;;
        indexReshuffle64Bit)
            run_index_reshuffle 64
            ;;
        copyFromTimestampIndex|oooCopyIndex|shiftTimestampIndex|flattenIndex)
            run_ts_index_kernel "${kernel}"
            ;;
        *)
            if is_vec_agg_kernel "${kernel}"; then
                run_vec_agg_kernel "${kernel}"
            else
                echo "Unknown kernel: ${kernel}" >&2
                usage >&2
                exit 2
            fi
            ;;
    esac
done

echo ""
echo "All results in: ${OUTPUT_DIR}/"
echo "Per-kernel comparison CSVs:"
for kernel in "${KERNELS[@]}"; do
    echo "  ${OUTPUT_DIR}/${kernel}/compare.csv"
done
