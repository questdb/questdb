#!/usr/bin/env bash
# t08 — the state reaper keeps what it must and deletes what it should.
#
# NO VM, NO ROOT, NO REAL DISKS: this builds a fake state directory in $TMPDIR and drives
# reap-state.sh against it. Seconds, not minutes.
#
# This test exists because the script it tests runs `rm -rf` on directories derived from an
# environment variable. Every guard below is one that, if it silently stopped working, would
# destroy something. The harness's rule for the cut and the oracle applies to the reaper too:
# a guard that has never been observed to fire is not a guard.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REAPER="$HERE/../reap-state.sh"
fails=0

ok()   { printf '  ok   %s\n' "$1"; }
bad()  { printf '  FAIL %s\n' "$1"; fails=$((fails + 1)); }
check() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected '$3', got '$2')"; fi; }

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"; [ -n "${SLEEP_PID:-}" ] && kill "$SLEEP_PID" 2>/dev/null' EXIT

mkstate() {  # DIR -> a directory that looks like a real state dir
    mkdir -p "$1/base"
    : > "$1/base/golden.qcow2"
    : > "$1/base/id_ed25519"
}

mkrun() {  # STATE NAME AGE_MINUTES
    local d="$1/$2"
    mkdir -p "$d"
    echo "qemu-system-x86_64 ... $2" > "$d/cmdline"
    echo "console output for $2"     > "$d/console.log"
    dd if=/dev/zero of="$d/data.raw" bs=1 count=0 seek=1M 2>/dev/null   # sparse, ~0 bytes
    touch -d "-$3 minutes" "$d"
}

echo "t08 — state reaper"

# ---- 1. refuses a directory that is not a state directory -------------------------------
# The first guard, and the one that matters most: this script deletes paths under whatever
# it is pointed at. A typo'd or empty QDB_VMCRASH_STATE must not be survivable.
NOTSTATE="$TMP/not-a-state-dir"
mkdir -p "$NOTSTATE/run-precious"
echo "important" > "$NOTSTATE/run-precious/data"
out=$(bash "$REAPER" --state="$NOTSTATE" --keep=0 --apply 2>&1); rc=$?
check "refuses a non-state directory (rc)" "$rc" "64"
[ -f "$NOTSTATE/run-precious/data" ] && ok "refused: the directory is untouched" \
    || bad "refused but deleted something anyway"
case "$out" in *"does not look like a vmcrash state directory"*) ok "refusal names the reason" ;;
               *) bad "refusal message unclear: $out" ;; esac

out=$(bash "$REAPER" --state="$TMP/does-not-exist" --apply 2>&1); rc=$?
check "refuses a missing directory (rc)" "$rc" "64"

# ---- 2. dry run deletes NOTHING ---------------------------------------------------------
S="$TMP/state"
mkstate "$S"
mkrun "$S" run-a 50
mkrun "$S" run-b 40
mkrun "$S" run-c 30
mkrun "$S" sweep-d 20
out=$(bash "$REAPER" --state="$S" --keep=1 2>&1)
check "dry run keeps every directory" "$(ls -d "$S"/run-* "$S"/sweep-* 2>/dev/null | wc -l)" "4"
case "$out" in *"would reap"*) ok "dry run says what it would do" ;;
               *) bad "dry run gave no preview: $out" ;; esac
case "$out" in *"re-run with --apply"*) ok "dry run tells you how to proceed" ;;
               *) bad "dry run does not mention --apply" ;; esac

# ---- 3. --apply keeps the N most RECENT, by mtime ----------------------------------------
out=$(bash "$REAPER" --state="$S" --keep=2 --apply 2>&1)
check "apply leaves exactly --keep directories" "$(ls -d "$S"/run-* "$S"/sweep-* 2>/dev/null | wc -l)" "2"
[ -d "$S/sweep-d" ] && ok "newest kept (sweep-d)"   || bad "newest was deleted"
[ -d "$S/run-c" ]   && ok "second newest kept (run-c)" || bad "second newest was deleted"
[ -d "$S/run-a" ]   && bad "oldest survived"        || ok "oldest reaped (run-a)"

# ---- 4. the cheap evidence is archived BEFORE the disks go ------------------------------
# A reaper that deletes first and uploads afterwards has already destroyed the thing the
# CI artifact was for.
[ -f "$S/evidence/run-a/console.log" ] && ok "console.log archived for a reaped run" \
    || bad "console.log lost when the run was reaped"
[ -f "$S/evidence/run-a/cmdline" ] && ok "cmdline archived for a reaped run" \
    || bad "cmdline lost when the run was reaped"

# ---- 5. a LIVE run is never reaped, however old -----------------------------------------
# The harness promises one VM at a time, but a human or a pipeline can start a run while
# this is sweeping. Deleting the disks under a live qemu corrupts the run it is measuring.
mkrun "$S" run-live 999
sleep 300 &
SLEEP_PID=$!
echo "$SLEEP_PID" > "$S/run-live/qemu.pid"
out=$(bash "$REAPER" --state="$S" --keep=0 --apply 2>&1)
[ -d "$S/run-live" ] && ok "live run skipped despite --keep=0 and being the oldest" \
    || bad "LIVE RUN WAS DELETED — this corrupts a run in progress"
case "$out" in *"LIVE"*) ok "live skip is reported, not silent" ;;
               *) bad "live skip was silent: $out" ;; esac
kill "$SLEEP_PID" 2>/dev/null; SLEEP_PID=""

# ---- 6. a STALE pid file does not protect a dead run ------------------------------------
# The mirror of 5: qemu.pid outlives the process it names, and a reaper that treats any
# pid file as "live" would never reclaim anything after an abnormal exit -- which is
# precisely the case this script exists for.
mkrun "$S" run-stale 998
echo "999999" > "$S/run-stale/qemu.pid"     # a pid that is not running
bash "$REAPER" --state="$S" --keep=0 --apply >/dev/null 2>&1
[ -d "$S/run-stale" ] && bad "stale pid file blocked reaping" || ok "stale pid file does not protect"

# ---- 7. base/, sweep-out/ and evidence/ are never candidates ----------------------------
mkdir -p "$S/sweep-out/some-cell"; echo "verdicts" > "$S/sweep-out/some-cell/flush-1.out"
bash "$REAPER" --state="$S" --keep=0 --apply >/dev/null 2>&1
[ -f "$S/base/golden.qcow2" ] && ok "base/ untouched" || bad "THE GOLDEN IMAGE WAS DELETED"
[ -f "$S/sweep-out/some-cell/flush-1.out" ] && ok "sweep-out/ untouched" \
    || bad "per-boundary evidence was deleted"
[ -d "$S/evidence" ] && ok "evidence/ untouched" || bad "archived evidence was deleted"

# ---- 8. an empty state directory is not an error ----------------------------------------
# CI calls this every run, including the first one.
S2="$TMP/empty"; mkstate "$S2"
out=$(bash "$REAPER" --state="$S2" --keep=5 --apply 2>&1); rc=$?
check "empty state dir exits 0" "$rc" "0"
case "$out" in *"nothing to reap"*) ok "empty state dir says so" ;;
               *) bad "unclear output for an empty state dir: $out" ;; esac

echo
if [ "$fails" -eq 0 ]; then
    echo "t08 PASSED"
    exit 0
fi
echo "t08 FAILED: $fails assertion(s)"
exit 1
