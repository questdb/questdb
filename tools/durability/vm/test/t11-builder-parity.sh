#!/usr/bin/env bash
# t11 — the two environment builders agree, and an incoherent configuration is refused.
#
# NO VM: calls lib/arms.sh directly. Seconds.
#
# The recording and the replay are separate JVMs configured by two different builders,
# harness_workload_env and harness_verify_cmd. A setting that reaches only one of them does not
# fail loudly: the replay client looks in a store-and-forward directory the workload never wrote
# to, finds nothing, and the oracle reports that the product lost acknowledged data. That is a
# false durability failure produced entirely by the harness, so the pairing is pinned here.
#
# The refusals matter for the same reason. harness_wal_table's 64 and a defanged-but-armed
# negative control are both swallowed by command substitution at the call site, and both end in a
# long run whose label does not describe what it did.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/arms.sh
source "$HERE/../lib/arms.sh"
fails=0

ok()    { printf '  ok   %s\n' "$1"; }
bad()   { printf '  FAIL %s\n' "$1"; fails=$((fails + 1)); }
check() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected '$3', got '$2')"; fi; }

echo "t11 — builder parity and configuration refusal"

# KEY=VALUE tokens from a builder. harness_verify_cmd's env prefix ends at the `bash` that runs
# the oracle; everything after it is flags, not environment.
env_keys() {  # STRING -> "KEY=VALUE" per line
    local tok
    for tok in $1; do
        case "$tok" in
            bash) break ;;
            env) ;;
            *=*) printf '%s\n' "$tok" ;;
        esac
    done
}

# ---- 1. shared keys must carry identical values -------------------------------------------
# Settings only the oracle acts on: the recovery pass is a verification step, so the workload has
# nothing to do with it. Every other shared key must reach both JVMs.
VERIFY_ONLY="QDB_PRODUCT_RECOVERY_PASS"

parity() {  # LABEL ARM MODE
    local label="$1" arm="$2" mode="$3" k v w mismatch=""
    local w_env v_env
    w_env=$(env_keys "$(harness_workload_env "$arm" "$mode")")
    v_env=$(env_keys "$(harness_verify_cmd "$arm" "$mode" 0 1000)")
    while IFS= read -r kv; do
        k="${kv%%=*}"; v="${kv#*=}"
        # A key the verifier reads and the workload never sees is the defect itself, so absence
        # is a mismatch unless it is a setting only the oracle can act on.
        case " $VERIFY_ONLY " in *" $k "*) continue ;; esac
        if ! printf '%s\n' "$w_env" | grep -q "^$k="; then
            mismatch="$mismatch $k(missing from the workload)"
            continue
        fi
        w=$(printf '%s\n' "$w_env" | grep "^$k=" | head -1 | cut -d= -f2-)
        [ "$w" = "$v" ] || mismatch="$mismatch $k(workload='$w' verify='$v')"
    done <<< "$v_env"
    if [ -z "$mismatch" ]; then ok "$label: every shared key agrees"
    else bad "$label: builders disagree:$mismatch"; fi
}

# NO SUBSHELLS BELOW. A check inside ( ... ) increments a copy of $fails, so the test reports
# PASSED however many assertions failed -- the same guard-that-cannot-fire shape this file exists
# to catch. Variables are exported and unset in place instead.

# The defaults agree trivially; the overrides are where a one-sided builder shows up.
parity "defaults, product/adaptive" product adaptive

export QDB_QWP_SF_DIR=/mnt/qdb/custom-sf QDB_QWP_SF_DURABILITY=flush
export QDB_WAL_TABLE=false QDB_QWP_DURABLE_ACK=local
parity "overridden, product/adaptive" product adaptive
parity "overridden, qwp-sf/adaptive"  qwp-sf  adaptive

unset QDB_QWP_SF_DIR QDB_QWP_SF_DURABILITY QDB_WAL_TABLE QDB_QWP_DURABLE_ACK

# ---- 2. an invalid QDB_WAL_TABLE is refused, not defaulted ---------------------------------
# harness_wal_table returns 64, but its callers use it inside "$(...)", which discards the status
# and emits QDB_WAL_TABLE= -- so both JVMs fall back to the mode-derived default and the run
# silently tests a configuration nobody asked for.
export QDB_WAL_TABLE=yes
harness_assert_config adaptive >/dev/null 2>&1
check "QDB_WAL_TABLE=yes is refused" "$?" "64"
export QDB_WAL_TABLE=true
harness_assert_config adaptive >/dev/null 2>&1
check "QDB_WAL_TABLE=true is accepted" "$?" "0"
unset QDB_WAL_TABLE

# ---- 3. a defanged label must mean a defanged run ------------------------------------------
# The sweep prints "this sweep MUST report failures" and stamps negative_control=true from the
# defang flag alone, while the tier it actually runs comes from arm_qwp_tier.
export QDB_ARM=qwp-sf QDB_QWP_DEFANG_ACK=1
unset QDB_QWP_DURABLE_ACK
harness_assert_config adaptive >/dev/null 2>&1
check "defang with the tier still local is refused" "$?" "64"
export QDB_QWP_DURABLE_ACK=off
harness_assert_config adaptive >/dev/null 2>&1
check "defang with the tier off is accepted" "$?" "0"
export QDB_ARM=product
unset QDB_QWP_DURABLE_ACK
harness_assert_config adaptive >/dev/null 2>&1
check "defang on the product arm is refused too" "$?" "64"
export QDB_ARM=reference
harness_assert_config adaptive >/dev/null 2>&1
check "defang on an arm with no tier is not a contradiction" "$?" "0"
unset QDB_ARM QDB_QWP_DEFANG_ACK

echo
if [ "$fails" -eq 0 ]; then
    echo "t11 PASSED"
    exit 0
fi
echo "t11 FAILED: $fails assertion(s)"
exit 1
