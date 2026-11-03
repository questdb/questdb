#!/usr/bin/env bash
# reap-state.sh [--keep N] [--keep-days D] [--apply] [--state DIR]
#
# Reclaim the run directories a persistent agent accumulates, keeping the evidence that matters
# and the N most recent runs.
#
# A failed run keeps its disks on purpose (40 G data + 60 G log + an overlay, sparse but not
# small). That is right on a developer box and fatal on a persistent agent, where a week of red
# nights fills the disk and every later run then fails at the check-host.sh free-space gate.
#
# No other script calls this one. Teardown idioms that reap broadly (`losetup -D`,
# `dmsetup remove_all`, rm of a whole state dir) destroy real data on a shared box, so this
# targets $QDB_VMCRASH_STATE by name and does nothing without --apply.
#
# Order is load-bearing: archive the cheap evidence first, then delete the disks. A reaper that
# deletes before it uploads has destroyed the thing the CI artifact was for.
#
# Exit codes: 0 success, including "nothing to reap"; 64 refused to act (bad arguments, or a path
# that is not a state directory); 70 tried and failed (an rm that did not remove). 64 and 70 are
# distinct because "I refused" and "I could not" need different reactions from a pipeline.
set -uo pipefail

KEEP="${QDB_REAP_KEEP:-5}"
# Age threshold for the two archive directories (sweep-out/, evidence/), which are not run state
# and are never reaped by count. They accumulate per sweep rather than per run, so a day count
# means something and a run count does not.
KEEP_DAYS="${QDB_REAP_KEEP_DAYS:-14}"
APPLY=0
STATE_DIR="${QDB_VMCRASH_STATE:-/data/qdb-vmcrash}"

for a in "$@"; do
    case "$a" in
        --keep=*)      KEEP="${a#*=}" ;;
        --keep-days=*) KEEP_DAYS="${a#*=}" ;;
        --state=*)     STATE_DIR="${a#*=}" ;;
        --apply)       APPLY=1 ;;
        -h|--help) awk 'NR > 1 { if (!/^#/) exit; print }' "$0"; exit 0 ;;
        *) echo "reap-state: unknown argument $a" >&2; exit 64 ;;
    esac
done

case "$KEEP" in
    ''|*[!0-9]*) echo "reap-state: --keep must be a non-negative integer (got '$KEEP')" >&2; exit 64 ;;
esac
case "$KEEP_DAYS" in
    ''|*[!0-9]*) echo "reap-state: --keep-days must be a non-negative integer (got '$KEEP_DAYS')" >&2; exit 64 ;;
esac

# Refuse anything that is not a state directory: this path gets rm -rf'd underneath, so a typo'd
# or empty QDB_VMCRASH_STATE must not be survivable. The golden image is the marker.
[ -n "$STATE_DIR" ] || { echo "reap-state: state directory is empty" >&2; exit 64; }
[ -d "$STATE_DIR" ] || { echo "reap-state: $STATE_DIR does not exist" >&2; exit 64; }

# Resolve the path before using it: `find DIR` does not descend a DIR that is a symlink, so a
# symlinked state directory would report "nothing to reap" and exit 0 while the disk stayed full.
# Resolved here rather than with `find -H` because STATE_DIR is also the root of every rm -rf
# path, the evidence/ archive target and the df argument, and one resolution keeps all of them
# consistent with the path the banner prints.
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

# Newest first, by mtime. Every prefix under which the harness creates a per-run directory is
# listed explicitly rather than inferred by a wider pattern, because this script rm -rf's what
# the list produces and a new directory name must not become a candidate by accident:
#
#   sweep-*      run-flush-sweep.sh   the flush sweep       40 G + 60 G kept on failure
#   sfreplay-*   run-sf-replay.sh     the sf replay         40 G + 60 G
#   t07-*        test/t07             oracle control        40 G + 60 G, runs nightly in CI
#   t10-*        test/t10             WAL barrier control   40 G + 60 G, runs nightly in CI
#
# t01 and t06 are absent because each is rm -rf'd at the start of its own run, so neither can
# accumulate past one copy.
#
# base/, sweep-out/ and evidence/ are not run state and are never candidates. sweep-out/ holds
# the per-boundary evidence the sweep keeps outside the run dir so it survives the success-path
# cleanup, and it matches `sweep-*`, so that exclusion is load-bearing.
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

    # Never reap a running VM's directory: a human or a pipeline can start a run while this is
    # sweeping, and deleting the disks under a live qemu corrupts the run it is measuring rather
    # than merely losing it.
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
        # Archive the cheap evidence first. These three files are kilobytes and answer most
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

# The two archive directories, which nothing else reclaims: sweep-out/<cell>/ gains a flush-N.out
# per boundary per sweep plus a junit.xml, and evidence/<run>/ is written by this script and
# excluded from its own candidate list, so both otherwise grow forever.
#
# Reaped by age rather than by count, with two guards:
#   * the newest entry in each parent is never removed, whatever its age, so a quiet agent keeps
#     one last-known-good sample to compare against;
#   * there is no qemu.pid here, but the mtime threshold subsumes a live check. A sweep in flight
#     writes a flush-N.out per boundary, so its mtime is seconds old and a threshold measured in
#     days cannot reach it.
# Runs after the run-dir pass, so evidence archived a moment ago is present and visibly fresh.
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
# Free space is reported even when nothing is reaped, so the trend is visible in the job log
# before it becomes the check-host.sh gate failing and looking like an outage.
if [ "$APPLY" -eq 0 ] && [ $((deleted + archived_deleted)) -gt 0 ]; then
    echo "  (dry run — re-run with --apply to delete)"
fi

# A failed rm is a failed reap. Exiting 0 here would make the reaper incapable of failing: the
# disk keeps filling while the job stays green.
if [ "$failed" -gt 0 ]; then
    echo "reap-state: $failed directory/directories could not be removed" >&2
    exit 70
fi
exit 0
