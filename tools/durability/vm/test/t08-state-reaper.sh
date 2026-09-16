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

# ---- 9. a SYMLINKED state directory is reaped, not silently skipped ---------------------
# `find DIR` does not descend a DIR that is a symlink, so the reaper used to print
# "nothing to reap" and exit 0 while the disk stayed full -- the exact outage it exists to
# prevent, wearing a green tick. Not hypothetical: this layout is symlink-heavy by design
# (docs/, scratch/, memory/) and the CI plan puts the state dir on a mounted volume.
S3="$TMP/linked-real"; mkstate "$S3"
mkrun "$S3" run-linked-a 50
mkrun "$S3" run-linked-b 40
ln -s "$S3" "$TMP/state-link"
out=$(bash "$REAPER" --state="$TMP/state-link" --keep=0 --apply 2>&1); rc=$?
check "symlinked state dir exits 0" "$rc" "0"
check "symlinked state dir actually reaps" "$(ls -d "$S3"/run-* 2>/dev/null | wc -l)" "0"
case "$out" in *"nothing to reap"*) bad "symlinked state dir reported nothing to reap" ;;
               *) ok "symlinked state dir did not claim an empty directory" ;; esac
# The banner must name the path that was operated on, or a job log cannot explain a reap.
case "$out" in *"$S3"*) ok "symlinked state dir logs the resolved path" ;;
               *) bad "banner still shows the link, not the target: $out" ;; esac

# ---- 10. every prefix this harness creates is a candidate -------------------------------
# Covering run-*/sweep-* alone left t07-*, t10-*, sfreplay-*, st8-* and preflight-* to
# accumulate untouched. t07 and t10 keep 40 G + 60 G EACH on failure and the CI plan runs
# both every night, so a week of red nights fills the agent through the one door the reaper
# never opened. One case per prefix, named after its creator.
S4="$TMP/prefixes"; mkstate "$S4"
for p in run-x sweep-x sfreplay-1 t07-2 t10-3 st8-4 preflight-real-5; do mkrun "$S4" "$p" 60; done
bash "$REAPER" --state="$S4" --keep=0 --apply >/dev/null 2>&1
for p in run-x sweep-x sfreplay-1 t07-2 t10-3 st8-4 preflight-real-5; do
    [ -d "$S4/$p" ] && bad "$p survived — its creator's disks are never reclaimed" \
        || ok "$p is a reap candidate"
done
# ...and widening the net must not have made base/ reachable.
[ -f "$S4/base/golden.qcow2" ] && ok "base/ still not a candidate under the wider prefix list" \
    || bad "THE GOLDEN IMAGE WAS DELETED"

# ---- 11. an rm that fails is a FAILED reap, not a green one -----------------------------
# The reaper used to exit 0 after a failed rm, which made it incapable of failing: the disk
# keeps filling, the nightly stays green, and the truth arrives days later as a check-host.sh
# free-space gate that reads as an infrastructure outage. 70 (EX_SOFTWARE) is deliberately
# distinct from the 64 refusal: "I could not" and "I refused" need different reactions.
S5="$TMP/rmfail"; mkstate "$S5"
mkrun "$S5" run-undeletable 60
echo "payload" > "$S5/run-undeletable/data"
chmod a-w "$S5/run-undeletable"                 # the file inside cannot be unlinked
out=$(bash "$REAPER" --state="$S5" --keep=0 --apply 2>&1); rc=$?
chmod u+w "$S5/run-undeletable"                 # restore, or the EXIT trap cannot clean up
check "a failed rm exits 70, not 0" "$rc" "70"
case "$out" in *"could not be removed"*) ok "the failure is named on stderr" ;;
               *) bad "a failed rm was silent: $out" ;; esac

# ---- 12. sweep-out/ and evidence/ are reclaimed BY AGE ----------------------------------
# Neither is run state, so neither is reaped by count -- and nothing reclaimed them at all,
# so both grew forever. sweep-out/<cell>/ gains one flush-N.out per boundary per sweep, and
# evidence/<run>/ is written by this script and excluded from its own candidate list, so the
# CI plan's "publish, then reap" would never reclaim what it had just published.
S6="$TMP/ageing"; mkstate "$S6"
mkdir -p "$S6/sweep-out/cell-ancient" "$S6/sweep-out/cell-stale" "$S6/sweep-out/cell-fresh"
mkdir -p "$S6/evidence/run-ancient" "$S6/evidence/run-fresh"
echo v > "$S6/sweep-out/cell-ancient/flush-1.out"
echo v > "$S6/sweep-out/cell-stale/flush-1.out"
echo v > "$S6/sweep-out/cell-fresh/flush-1.out"
echo c > "$S6/evidence/run-ancient/console.log"
echo c > "$S6/evidence/run-fresh/console.log"
touch -d '-90 days' "$S6/sweep-out/cell-ancient" "$S6/evidence/run-ancient"
touch -d '-30 days' "$S6/sweep-out/cell-stale"
out=$(bash "$REAPER" --state="$S6" --keep=0 --keep-days=14 2>&1)
case "$out" in *"would reap sweep-out/cell-ancient"*) ok "age reclaim previews in dry run" ;;
               *) bad "dry run gave no age preview: $out" ;; esac
[ -d "$S6/sweep-out/cell-ancient" ] && ok "age reclaim deletes nothing without --apply" \
    || bad "DRY RUN DELETED AN ARCHIVE"
out=$(bash "$REAPER" --state="$S6" --keep=0 --keep-days=14 --apply 2>&1); rc=$?
check "age reclaim exits 0" "$rc" "0"
[ -d "$S6/sweep-out/cell-ancient" ] && bad "90-day-old sweep-out archive survived" \
    || ok "sweep-out/ archive older than --keep-days is reaped"
[ -d "$S6/sweep-out/cell-stale" ] && bad "30-day-old sweep-out archive survived" \
    || ok "age reclaim is not limited to one entry per parent"
[ -d "$S6/evidence/run-ancient" ] && bad "90-day-old evidence archive survived" \
    || ok "evidence/ archive older than --keep-days is reaped"
# NEVER THE NEWEST, whatever its age: a quiet agent must keep one last-known-good sample.
[ -f "$S6/sweep-out/cell-fresh/flush-1.out" ] && ok "the newest sweep-out archive is kept" \
    || bad "the newest sweep-out archive was deleted"
[ -f "$S6/evidence/run-fresh/console.log" ] && ok "the newest evidence archive is kept" \
    || bad "the newest evidence archive was deleted"
# A sweep IN FLIGHT writes a flush-N.out per boundary, so its mtime is seconds old. There is
# no qemu.pid to consult here, and the threshold is what protects it -- prove that it does.
#
# THE IN-FLIGHT DIRECTORY MUST NOT BE THE NEWEST ONE. Two guards cover this directory, the age
# threshold and never-the-newest, and while the in-flight sweep was also the newest entry each
# one masked the other's mutation: deleting EITHER guard left t08 green. Adding a newer entry
# after it strips away the newest-guard and leaves the threshold alone under test.
mkdir -p "$S6/sweep-out/cell-inflight"; echo v > "$S6/sweep-out/cell-inflight/flush-1.out"
mkdir -p "$S6/sweep-out/cell-older"; touch -d '-40 days' "$S6/sweep-out/cell-older"
mkdir -p "$S6/sweep-out/cell-newest"; touch "$S6/sweep-out/cell-newest"
bash "$REAPER" --state="$S6" --keep=0 --keep-days=14 --apply >/dev/null 2>&1
[ -f "$S6/sweep-out/cell-inflight/flush-1.out" ] && ok "a sweep in flight is protected by its mtime" \
    || bad "AN IN-FLIGHT SWEEP'S OUTPUT WAS DELETED"

# ...and the mirror: when EVERY archive is older than the threshold, the newest is still kept.
# This is the only shape in which never-the-newest is the guard doing the work -- above, the age
# threshold would have spared a fresh entry anyway. A quiet agent, or one returning from a long
# gap, must not be left with nothing to compare against.
S7="$TMP/all-old"; mkstate "$S7"
mkdir -p "$S7/sweep-out/cell-90" "$S7/sweep-out/cell-80" "$S7/sweep-out/cell-70"
for a in 90 80 70; do echo v > "$S7/sweep-out/cell-$a/flush-1.out"; touch -d "-$a days" "$S7/sweep-out/cell-$a"; done
bash "$REAPER" --state="$S7" --keep=0 --keep-days=14 --apply >/dev/null 2>&1
[ -f "$S7/sweep-out/cell-70/flush-1.out" ] && ok "the newest archive survives even when all are stale" \
    || bad "THE LAST ARCHIVE WAS DELETED — nothing left to compare against"
check "all-stale parent keeps exactly one archive" \
    "$(find "$S7/sweep-out" -mindepth 1 -maxdepth 1 -type d | wc -l)" "1"
# --keep-days=0 means "everything but the newest", and must still spare the newest. The explicit
# mtimes are not decoration: with a 0-day cutoff of "now" and second-granularity mtimes, a
# directory this test created moments ago ties with the cutoff and makes the assertion a coin
# flip. Distinct ages also fix WHICH one is the newest, so the survivor is named, not whichever
# the filesystem happened to stamp last.
touch -d '-3 minutes' "$S6/sweep-out/cell-fresh"
touch -d '-2 minutes' "$S6/sweep-out/cell-inflight"
touch -d '-1 minute'  "$S6/sweep-out/cell-newest"
bash "$REAPER" --state="$S6" --keep=0 --keep-days=0 --apply >/dev/null 2>&1
check "--keep-days=0 leaves exactly the newest archive" \
    "$(find "$S6/sweep-out" -mindepth 1 -maxdepth 1 -type d | wc -l)" "1"
[ -d "$S6/sweep-out/cell-newest" ] && ok "--keep-days=0 spares the newest by name" \
    || bad "--keep-days=0 deleted the newest archive"
out=$(bash "$REAPER" --state="$S6" --keep-days=nonsense --apply 2>&1); rc=$?
check "--keep-days rejects a non-integer (rc)" "$rc" "64"

echo
if [ "$fails" -eq 0 ]; then
    echo "t08 PASSED"
    exit 0
fi
echo "t08 FAILED: $fails assertion(s)"
exit 1
