#!/usr/bin/env bash
# t13 — an absent table is graded against the watermark of the workload that wrote the data.
#
# NO VM: runs the real guest/verify.sh against fake database roots, with a shim `java` standing in
# for CrashVerifier. Seconds.
#
# A cut before the table was durably created makes the oracle report "table does not exist", and
# verify.sh downgrades that to NO_COMMIT only when the workload's watermark says nothing was
# committed. Each arm's workload writes its own watermark file (lib/arms.sh arm_progress_file), and
# verify.sh, which cannot source lib/arms.sh inside the guest, keeps its own copy of that mapping.
# If the copy reads the wrong file, the watermark is always absent, a lost table with committed
# rows grades NO_COMMIT, NO_COMMIT counts as a pass, and a mixed sweep stays green.
#
# Every boundary here runs the command harness_verify_cmd builds for the arm, so a drift between
# the two mappings, or between arm_verify_flags and verify.sh's flag parsing, fails this test.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/arms.sh
source "$HERE/../lib/arms.sh"
# shellcheck source=../lib/verdict.sh
source "$HERE/../lib/verdict.sh"
fails=0
oks=0

ok()    { printf '  ok   %s\n' "$1"; oks=$((oks + 1)); }
bad()   { printf '  FAIL %s\n' "$1"; fails=$((fails + 1)); }
check() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected '$3', got '$2')"; fi; }

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

echo "t13 — absent-table watermark per arm"

# ---- the guest, relocated -------------------------------------------------------------------
# The guest paths become paths under $TMP, so nothing on the host is read or deleted. The
# substitution is checked: a guest path left in the copy would make every case below grade
# whatever the host happens to hold.
mkdir -p "$TMP/opt/guest" "$TMP/mnt" "$TMP/bin"
sed -e "s#/opt/vmcrash#$TMP/opt#g" -e "s#/mnt/qdb#$TMP/mnt#g" \
    "$HERE/../guest/verify.sh" > "$TMP/opt/guest/verify.sh"
check "the relocated verify.sh holds no guest path" \
    "$(grep -cE '/opt/vmcrash|/mnt/qdb' "$TMP/opt/guest/verify.sh")" "0"
# verify.sh refuses an empty jar before it runs the oracle.
echo "not a jar" > "$TMP/opt/benchmarks.jar"
# The product arm sources the distribution helpers. With the recovery pass off (below) the only
# one it calls is the unpack, and the artifact is not what this test measures.
echo 'product_dist_unpack() { return 0; }' > "$TMP/opt/guest/product-dist.sh"

# The oracle's own words for an absent table: CrashVerifier prints CairoException's message
# after "LOUD_FAILURE: " and exits 1. The shim records its arguments so the test can show which
# watermark file the oracle itself was told to read.
cat > "$TMP/bin/java" <<EOF
#!/usr/bin/env bash
printf '%s\n' "\$@" > "$TMP/java.args"
echo "LOUD_FAILURE: [-105] table does not exist [table=t]"
exit 1
EOF
chmod +x "$TMP/bin/java"
export PATH="$TMP/bin:$PATH"

# The store-and-forward replay starts a server and a client before the oracle runs; this test
# grades the oracle's line only. The product recovery pass would start the shipped server.
export QDB_SF_REPLAY=false
export QDB_PRODUCT_RECOVERY_PASS=false

# boundary ARM MODE FILE FIRST_LINE -> the token verify.sh grades, for a root holding no table
# and only FILE, whose first line is FIRST_LINE. FILE "none" means no watermark file at all.
boundary() {
    local arm="$1" mode="$2" file="$3" first="$4" cmd out
    rm -rf "$TMP/mnt/db" "$TMP/java.args"
    mkdir -p "$TMP/mnt/db"
    if [ "$file" != none ]; then
        printf '%s\nC=7\nWm=7\nrows=%s\nsent=20000\n' "$first" "$first" > "$TMP/mnt/db/$file"
    fi
    cmd=$(harness_verify_cmd "$arm" "$mode" 50000 1000 \
        | sed -e "s#/opt/vmcrash#$TMP/opt#g" -e "s#/mnt/qdb#$TMP/mnt#g")
    out=$(eval "$cmd" 2>&1)
    verdict_classify "$(verdict_line "$out")"
}

# oracle_qwp -> the -Dqwp value the oracle received on the last boundary.
oracle_qwp() {
    grep -m1 '^-Dqwp=' "$TMP/java.args" 2>/dev/null | cut -d= -f2
}

# ---- 1. committed rows and no table is a durability failure, in every arm -------------------
echo "  the arm's own watermark is positive: the lost table is a failure"
for spec in reference:adaptive qwp:adaptive qwp-sf:adaptive product:adaptive product:sync; do
    arm="${spec%%:*}"; mode="${spec#*:}"
    own=$(arm_progress_file "$arm")
    check "$arm/$mode, $own=7000 -> DURABILITY_FAILURE" \
        "$(boundary "$arm" "$mode" "$own" 7000)" DURABILITY_FAILURE
    case "$own" in
        _qwp_progress) want=true ;;
        *)             want=false ;;
    esac
    check "$arm/$mode, the oracle was told -Dqwp=$want, the same file" "$(oracle_qwp)" "$want"
done

# ---- 2. nothing committed is a legitimate early cut ------------------------------------------
echo "  the arm's own watermark is zero or absent: the cut came before any commit"
for spec in reference:adaptive qwp:adaptive qwp-sf:adaptive product:adaptive product:sync; do
    arm="${spec%%:*}"; mode="${spec#*:}"
    own=$(arm_progress_file "$arm")
    check "$arm/$mode, $own=0 -> NO_COMMIT" "$(boundary "$arm" "$mode" "$own" 0)" NO_COMMIT
    check "$arm/$mode, no watermark file -> NO_COMMIT" "$(boundary "$arm" "$mode" none 0)" NO_COMMIT
done

# ---- 3. the other arm's file does not count --------------------------------------------------
# A grader that read whichever file exists would pass sections 1 and 2. The workload writes one
# file only, so a positive count in the other one is not this arm's evidence of a commit.
echo "  only the other arm's watermark is positive: it is not this workload's"
check "reference, only _qwp_progress=7000 -> NO_COMMIT" \
    "$(boundary reference adaptive _qwp_progress 7000)" NO_COMMIT
check "qwp, only _progress=7000 -> NO_COMMIT" \
    "$(boundary qwp adaptive _progress 7000)" NO_COMMIT

echo
if [ "$fails" -eq 0 ]; then
    echo "t13 PASSED ($oks assertions)"
    exit 0
fi
echo "t13 FAILED: $fails assertion(s) of $((oks + fails))"
exit 1
