#!/usr/bin/env bash
# reap-state.sh [--keep N] [--apply] [--state DIR]
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
# WHY IT IS A SEPARATE SCRIPT AND NOT PART OF TEARDOWN. `issues/15` is explicit, and
# check-host.sh carries the same warning: teardown idioms that reap broadly (`losetup -D`,
# `dmsetup remove_all`, rm of a whole state dir) destroy real data on a shared box. This is
# a CI-side concern. It targets $QDB_VMCRASH_STATE BY NAME, it is never called by
# power-cut-vm.sh / run-flush-sweep.sh / run-matrix.sh, and it does nothing without --apply.
#
# ORDER IS LOAD-BEARING: archive the cheap evidence FIRST, then delete the disks. A reaper
# that deletes first and uploads afterwards has already destroyed the thing the CI artifact
# was for.
set -uo pipefail

KEEP="${QDB_REAP_KEEP:-5}"
APPLY=0
STATE_DIR="${QDB_VMCRASH_STATE:-/data/qdb-vmcrash}"

for a in "$@"; do
    case "$a" in
        --keep=*)  KEEP="${a#*=}" ;;
        --state=*) STATE_DIR="${a#*=}" ;;
        --apply)   APPLY=1 ;;
        -h|--help) sed -n '2,30p' "$0"; exit 0 ;;
        *) echo "reap-state: unknown argument $a" >&2; exit 64 ;;
    esac
done

case "$KEEP" in
    ''|*[!0-9]*) echo "reap-state: --keep must be a non-negative integer (got '$KEEP')" >&2; exit 64 ;;
esac

# REFUSE ANYTHING THAT IS NOT A STATE DIRECTORY. The argument to this script is a path that
# gets rm -rf'd underneath, so a typo'd or empty QDB_VMCRASH_STATE must not be survivable.
# The golden image is the marker: a state dir always has one, and nothing else does.
[ -n "$STATE_DIR" ] || { echo "reap-state: state directory is empty" >&2; exit 64; }
[ -d "$STATE_DIR" ] || { echo "reap-state: $STATE_DIR does not exist" >&2; exit 64; }
if [ ! -f "$STATE_DIR/base/golden.qcow2" ] && [ ! -f "$STATE_DIR/base/id_ed25519" ]; then
    echo "reap-state: $STATE_DIR does not look like a vmcrash state directory" >&2
    echo "  (no base/golden.qcow2 and no base/id_ed25519). Refusing to delete anything." >&2
    exit 64
fi

free_gb() { df -BG --output=avail "$1" 2>/dev/null | tail -1 | tr -dc '0-9'; }

BEFORE_FREE=$(free_gb "$STATE_DIR")
echo "reap-state: $STATE_DIR"
echo "  free before: ${BEFORE_FREE:-?} G, keeping the $KEEP most recent run directories"

# Newest first, by mtime. `run-*` (live cut) and `sweep-*` (flush sweep) are the only
# directories either flow creates for a run; base/, sweep-out/ and evidence/ are NOT run
# state and are never candidates -- sweep-out/ in particular is the per-boundary evidence
# the sweep deliberately keeps OUTSIDE the run dir so it survives the success-path cleanup.
mapfile -t candidates < <(find "$STATE_DIR" -mindepth 1 -maxdepth 1 -type d \
    \( -name 'run-*' -o -name 'sweep-*' \) ! -name 'sweep-out' -printf '%T@ %p\n' 2>/dev/null \
    | sort -rn | cut -d' ' -f2-)

total=${#candidates[@]}
if [ "$total" -eq 0 ]; then
    echo "  nothing to reap (0 run directories)"
    exit 0
fi

reclaimed=0
kept=0
skipped_live=0
deleted=0
idx=0
for d in "${candidates[@]}"; do
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
        rm -rf "$d" || { echo "  FAILED to remove $name" >&2; continue; }
        echo "  reaped   $name (${sz} MB)"
    else
        echo "  would reap $name (${sz} MB)"
    fi
    reclaimed=$((reclaimed + sz))
    deleted=$((deleted + 1))
done

AFTER_FREE=$(free_gb "$STATE_DIR")
echo "  $total run directories: $kept kept, $deleted $([ "$APPLY" -eq 1 ] && echo reaped || echo reapable), $skipped_live live"
echo "  $([ "$APPLY" -eq 1 ] && echo reclaimed || echo reclaimable): ${reclaimed} MB"
echo "  free after:  ${AFTER_FREE:-?} G"
# REPORT FREE SPACE EVEN WHEN NOTHING IS REAPED, so the trend is visible in the job log
# BEFORE it becomes the check-host.sh gate failing and looking like an outage.
if [ "$APPLY" -eq 0 ] && [ "$deleted" -gt 0 ]; then
    echo "  (dry run — re-run with --apply to delete)"
fi
exit 0
