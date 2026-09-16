#!/usr/bin/env bash
# reap-state.sh [--keep N] [--keep-days D] [--apply] [--state DIR]
#
# Reclaim the run directories a persistent agent accumulates, KEEPING the evidence that
# matters and the N most recent runs.
#
# WHY THIS EXISTS. Every failed run keeps its disks on purpose -- 40 G data + 60 G log +
# an overlay, sparse but not small. On a developer box that is right: you want the
# evidence. On a persistent CI agent a run of red nights fills the disk, and then every
# SUBSEQUENT run fails at the check-host.sh free-space gate -- which reads as an
# infrastructure outage rather than as the cleanup problem it is.
#
# WHY IT IS A SEPARATE SCRIPT AND NOT PART OF TEARDOWN. The rule is explicit, and
# check-host.sh carries the same warning: teardown idioms that reap broadly (`losetup -D`,
# `dmsetup remove_all`, rm of a whole state dir) destroy real data on a shared box. This is
# a CI-side concern. It targets $QDB_VMCRASH_STATE BY NAME, it is never called by
# power-cut-vm.sh / run-flush-sweep.sh / run-matrix.sh, and it does nothing without --apply.
#
# ORDER IS LOAD-BEARING: archive the cheap evidence FIRST, then delete the disks. A reaper
# that deletes first and uploads afterwards has already destroyed the thing the CI artifact
# was for.
#
# EXIT CODES. 0 success (including "nothing to reap"), 64 refused-to-act (bad arguments, or a
# path that is not a state directory), 70 tried and failed (an rm that did not remove). 70 is
# sysexits' EX_SOFTWARE, and it is deliberately DISTINCT from 64: "I refused" and "I could not"
# need different reactions from a pipeline, and the old behaviour -- exit 0 after a failed rm --
# meant the reaper could not fail at all. A green reap every night while the disk fills arrives
# later as the check-host.sh free-space gate, which reads as an infrastructure outage.
set -uo pipefail

KEEP="${QDB_REAP_KEEP:-5}"
# Age threshold for the two ARCHIVE directories (sweep-out/, evidence/), which are not run
# state and are never reaped by count. Days, not runs: they accumulate per sweep rather than
# per run, and 14 days of them is megabytes -- correctness matters more than aggression here.
KEEP_DAYS="${QDB_REAP_KEEP_DAYS:-14}"
APPLY=0
STATE_DIR="${QDB_VMCRASH_STATE:-/data/qdb-vmcrash}"

for a in "$@"; do
    case "$a" in
        --keep=*)      KEEP="${a#*=}" ;;
        --keep-days=*) KEEP_DAYS="${a#*=}" ;;
        --state=*)     STATE_DIR="${a#*=}" ;;
        --apply)       APPLY=1 ;;
        -h|--help) sed -n '2,40p' "$0"; exit 0 ;;
        *) echo "reap-state: unknown argument $a" >&2; exit 64 ;;
    esac
done

case "$KEEP" in
    ''|*[!0-9]*) echo "reap-state: --keep must be a non-negative integer (got '$KEEP')" >&2; exit 64 ;;
esac
case "$KEEP_DAYS" in
    ''|*[!0-9]*) echo "reap-state: --keep-days must be a non-negative integer (got '$KEEP_DAYS')" >&2; exit 64 ;;
esac

# REFUSE ANYTHING THAT IS NOT A STATE DIRECTORY. The argument to this script is a path that
# gets rm -rf'd underneath, so a typo'd or empty QDB_VMCRASH_STATE must not be survivable.
# The golden image is the marker: a state dir always has one, and nothing else does.
[ -n "$STATE_DIR" ] || { echo "reap-state: state directory is empty" >&2; exit 64; }
[ -d "$STATE_DIR" ] || { echo "reap-state: $STATE_DIR does not exist" >&2; exit 64; }

# RESOLVE THE PATH BEFORE USING IT. `find DIR` does not descend a DIR that is a symlink --
# no -H, no -L, no traversal -- so a symlinked state directory made this script print
# "nothing to reap" and exit 0 while the disk stayed full: the precise failure it exists to
# prevent, now silent and green. This layout is symlink-heavy by design (docs/, scratch/ and
# memory/ are all symlinks) and the CI plan puts the state directory on a mounted volume
# outside the workspace.
#
# Resolved here rather than with `find -H` because STATE_DIR is also the root of every rm -rf
# path, the evidence/ archive target and the df argument: one resolution keeps all of them
# consistent, and the banner then prints the path that was actually operated on -- which is
# what you want in a job log when asking why a reap reclaimed nothing.
STATE_DIR="$(cd "$STATE_DIR" && pwd -P)" || { echo "reap-state: cannot resolve $STATE_DIR" >&2; exit 64; }
if [ ! -f "$STATE_DIR/base/golden.qcow2" ] && [ ! -f "$STATE_DIR/base/id_ed25519" ]; then
    echo "reap-state: $STATE_DIR does not look like a vmcrash state directory" >&2
    echo "  (no base/golden.qcow2 and no base/id_ed25519). Refusing to delete anything." >&2
    exit 64
fi

free_gb() { df -BG --output=avail "$1" 2>/dev/null | tail -1 | tr -dc '0-9'; }

BEFORE_FREE=$(free_gb "$STATE_DIR")
echo "reap-state: $STATE_DIR"
echo "  free before: ${BEFORE_FREE:-?} G, keeping the $KEEP most recent run directories"

# Newest first, by mtime. EVERY prefix under which this harness creates a per-run directory,
# each one verified against its creator -- covering only the two main flows meant a week of red
# nights filled the agent through a door the reaper never looked at, which is the same hazard
# arriving by another route:
#
#   run-*        power-cut-vm.sh      the live cut          40 G + 60 G kept on failure
#   sweep-*      run-flush-sweep.sh   the flush sweep       40 G + 60 G
#   sfreplay-*   run-sf-replay.sh     the sf replay         40 G + 60 G
#   t07-*        test/t07             oracle control        40 G + 60 G, runs nightly in CI
#   t10-*        test/t10             WAL barrier control   40 G + 60 G, runs nightly in CI
#   st8-*        run-st8-probe.sh     the ST8 probe          2 G + log
#   preflight-*  lib/preflight.sh     the preflight cycle     8 G, and it can be leaked
#
# An explicit prefix list, NOT a pattern wide enough to infer them. The script rm -rf's what
# this produces, so it must not be possible for a new directory name to become a candidate by
# accident. t01/t02/t03/t06 are deliberately absent: each is rm -rf'd at the start of its own
# run, so it is bounded to one copy and cannot accumulate.
#
# base/, sweep-out/ and evidence/ are NOT run state and are never candidates here -- sweep-out/
# in particular is the per-boundary evidence the sweep deliberately keeps OUTSIDE the run dir so
# it survives the success-path cleanup. `sweep-out` matches `sweep-*`, so that exclusion is
# load-bearing; `base` and `evidence` match no prefix and are excluded anyway, because the cost
# of the redundancy is one line and the cost of being wrong is the golden image.
mapfile -t candidates < <(find "$STATE_DIR" -mindepth 1 -maxdepth 1 -type d \
    \( -name 'run-*' -o -name 'sweep-*' -o -name 'sfreplay-*' -o -name 't07-*' \
       -o -name 't10-*' -o -name 'st8-*' -o -name 'preflight-*' \) \
    ! -name 'sweep-out' ! -name 'evidence' ! -name 'base' -printf '%T@ %p\n' 2>/dev/null \
    | sort -rn | cut -d' ' -f2-)

total=${#candidates[@]}
reclaimed=0
kept=0
skipped_live=0
deleted=0
idx=0
failed=0
[ "$total" -eq 0 ] && echo "  nothing to reap (0 run directories)"
for d in "${candidates[@]+"${candidates[@]}"}"; do
    idx=$((idx + 1))
    name=$(basename "$d")

    # NEVER REAP A RUNNING VM's DIRECTORY. The harness promises one VM at a time, but a
    # human or a pipeline can start a run while this is sweeping, and deleting the disks
    # under a live qemu corrupts the run it is measuring rather than merely losing it.
    if [ -f "$d/qemu.pid" ]; then
        pid=$(cat "$d/qemu.pid" 2>/dev/null)
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            echo "  LIVE  $name (qemu pid $pid) — skipped"
            skipped_live=$((skipped_live + 1))
            continue
        fi
    fi

    if [ "$idx" -le "$KEEP" ]; then
        kept=$((kept + 1))
        continue
    fi

    sz=$(du -sm "$d" 2>/dev/null | cut -f1)
    : "${sz:=0}"
    if [ "$APPLY" -eq 1 ]; then
        # ARCHIVE THE CHEAP EVIDENCE FIRST. These three files are kilobytes and answer most
        # questions a kept disk would have answered: the qemu command line, the guest serial
        # console, and the captured guest logs from a liveness bail.
        mkdir -p "$STATE_DIR/evidence/$name"
        for f in cmdline console.log liveness-failure.out; do
            [ -f "$d/$f" ] && cp -f "$d/$f" "$STATE_DIR/evidence/$name/" 2>/dev/null
        done
        rm -rf "$d" || { echo "  FAILED to remove $name" >&2; failed=$((failed + 1)); continue; }
        echo "  reaped   $name (${sz} MB)"
    else
        echo "  would reap $name (${sz} MB)"
    fi
    reclaimed=$((reclaimed + sz))
    deleted=$((deleted + 1))
done

# THE TWO ARCHIVE DIRECTORIES, WHICH NOTHING ELSE RECLAIMS. sweep-out/<cell>/ gains one
# flush-N.out per boundary per sweep (400 on a thorough run) plus a junit.xml, and evidence/<run>/
# is written by THIS script and excluded from its own candidate list -- so both grow forever, and
# the CI plan's "publish, then reap" step would never reclaim what it had just published.
#
# By AGE, not by count: these accumulate per sweep rather than per run, so "the 5 most recent"
# means something quite different here. Two guards:
#   * the newest entry in each parent is NEVER removed, whatever its age, so a quiet agent always
#     keeps one last-known-good sample to compare against;
#   * there is no qemu.pid to consult, and no live-pid check is meaningful -- but the mtime
#     threshold subsumes it. A sweep in flight writes a flush-N.out per boundary into its own
#     sweep-out directory, so its mtime is seconds old and a threshold measured in days cannot
#     reach it. Same for an evidence/ entry this run just wrote.
# Run AFTER the run-dir pass, so evidence archived a moment ago is present and visibly fresh.
archive_reclaim() {  # PARENT LABEL
    local parent="$1" label="$2" cutoff now d mt name sz
    [ -d "$parent" ] || return 0
    local all=()
    mapfile -t all < <(find "$parent" -mindepth 1 -maxdepth 1 -type d -printf '%T@ %p\n' 2>/dev/null \
        | sort -rn | cut -d' ' -f2-)
    [ "${#all[@]}" -le 1 ] && return 0          # nothing, or only the one that is always kept
    now=$(date +%s)
    cutoff=$(( now - KEEP_DAYS * 86400 ))
    for d in "${all[@]:1}"; do                  # [0] is the newest: never a candidate
        mt=$(stat -c %Y "$d" 2>/dev/null) || continue
        [ "$mt" -ge "$cutoff" ] && continue
        name=$(basename "$d")
        sz=$(du -sm "$d" 2>/dev/null | cut -f1); : "${sz:=0}"
        if [ "$APPLY" -eq 1 ]; then
            rm -rf "$d" || { echo "  FAILED to remove $label/$name" >&2; failed=$((failed + 1)); continue; }
            echo "  reaped   $label/$name (${sz} MB, older than ${KEEP_DAYS}d)"
        else
            echo "  would reap $label/$name (${sz} MB, older than ${KEEP_DAYS}d)"
        fi
        archived_reclaimed=$((archived_reclaimed + sz))
        archived_deleted=$((archived_deleted + 1))
    done
}

archived_reclaimed=0
archived_deleted=0
archive_reclaim "$STATE_DIR/sweep-out" sweep-out
archive_reclaim "$STATE_DIR/evidence" evidence

AFTER_FREE=$(free_gb "$STATE_DIR")
if [ "$total" -gt 0 ]; then
    echo "  $total run directories: $kept kept, $deleted $([ "$APPLY" -eq 1 ] && echo reaped || echo reapable), $skipped_live live"
    echo "  $([ "$APPLY" -eq 1 ] && echo reclaimed || echo reclaimable): ${reclaimed} MB"
fi
if [ "$archived_deleted" -gt 0 ]; then
    echo "  archives older than ${KEEP_DAYS}d: $archived_deleted $([ "$APPLY" -eq 1 ] && echo reaped || echo reapable), ${archived_reclaimed} MB"
fi
echo "  free after:  ${AFTER_FREE:-?} G"
# REPORT FREE SPACE EVEN WHEN NOTHING IS REAPED, so the trend is visible in the job log
# BEFORE it becomes the check-host.sh gate failing and looking like an outage.
if [ "$APPLY" -eq 0 ] && [ $((deleted + archived_deleted)) -gt 0 ]; then
    echo "  (dry run — re-run with --apply to delete)"
fi

# A FAILED rm IS A FAILED REAP. Exiting 0 here made the reaper incapable of failing: the disk
# keeps filling, the job stays green, and the truth arrives days later as a check-host.sh
# free-space gate that looks like an infrastructure outage.
if [ "$failed" -gt 0 ]; then
    echo "reap-state: $failed directory/directories could not be removed" >&2
    exit 70
fi
exit 0
