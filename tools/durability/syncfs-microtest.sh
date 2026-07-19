#!/bin/bash
# ============================================================================
# syncfs micro-test (QuestDB-independent). RUN AS ROOT: sudo bash syncfs-microtest.sh
#
# Isolates ONE question: does syncfs(2) journal ext4's unwritten->written EXTENT
# conversion durably across a power cut?  (That is exactly what the batched flush
# optimization needs and what sync_file_range + a foreign _cv fsync failed to do.)
#
# Mechanism replicated with xfs_io:
#   falloc 0 N        -> allocate an UNWRITTEN extent (like QuestDB's append-page prealloc)
#   pwrite 0xAB 0 N   -> write data (converts unwritten->written; needs journaling)
#   sync_range -w -a  -> push the DATA BLOCKS to the device (NOT a journal commit)
#   <FLUSH>           -> the candidate durability op under test
# then the verified dm-flakey power cut (drop un-fsync'd writes), remount, read byte 0.
#   0xab  -> extent conversion was journaled  -> DURABLE
#   0x00  -> conversion lost -> extent reads unwritten -> ZEROS (the bug)
# ============================================================================
set -uo pipefail

IMG="${IMG:-/data/qdb-syncfs-micro.img}"
MNT="${MNT:-/mnt/qdbsfmicro}"
DM="${DM:-sfmicro}"
FSTYPE="${1:-${FSTYPE:-ext4}}"   # ext4 | xfs  — pass as FIRST ARG (survives sudo): sudo bash syncfs-microtest.sh xfs
case "$FSTYPE" in ext4|xfs) ;; *) echo "ERROR: FSTYPE must be ext4 or xfs (got '$FSTYPE')" >&2; exit 1 ;; esac
SZ=16m
LOOP=""

cleanup() {
    umount "$MNT" 2>/dev/null || true
    dmsetup remove "$DM" 2>/dev/null || true
    [ -n "$LOOP" ] && losetup -d "$LOOP" 2>/dev/null || true
    rm -f "$IMG"
}
trap cleanup EXIT

modprobe dm-flakey 2>/dev/null || true

mkfs_dev() {  # $1 = device; format with the chosen FSTYPE
    case "$FSTYPE" in
        ext4) mkfs.ext4 -F -q "$1" >/dev/null 2>&1 ;;
        xfs)  mkfs.xfs -f "$1" >/dev/null 2>&1 ;;
        *) echo "ERROR: unknown FSTYPE=$FSTYPE (use ext4 or xfs)" >&2; exit 1 ;;
    esac
}

# test_case "<xfs_io flush args>" "<label>" "<expect: SURVIVE|LOST>"
test_case() {
    local FLUSH="$1" LABEL="$2" EXPECT="$3"
    rm -f "$IMG"; truncate -s 1G "$IMG"
    LOOP=$(losetup -f --show "$IMG")
    local SECTORS; SECTORS=$(blockdev --getsz "$LOOP")
    dmsetup create "$DM" --table "0 $SECTORS flakey $LOOP 0 180 0"
    mkfs_dev /dev/mapper/"$DM"
    mkdir -p "$MNT"; mount /dev/mapper/"$DM" "$MNT"

    # falloc (unwritten extent) + write + sync_file_range(WRITE|WAIT_AFTER) + <FLUSH>
    # shellcheck disable=SC2086
    xfs_io -f \
        -c "falloc 0 $SZ" \
        -c "pwrite -S 0xab -b 1m 0 $SZ" \
        -c "sync_range -w -a 0 $SZ" \
        $FLUSH \
        "$MNT/sfile" >/dev/null 2>&1

    # THE CUT (verified mechanism): --nolockfs suspend, drop_writes, umount (writeback dropped)
    dmsetup suspend --nolockfs "$DM"
    dmsetup load "$DM" --table "0 $SECTORS flakey $LOOP 0 0 180 1 drop_writes"
    dmsetup resume "$DM"
    umount "$MNT"
    dmsetup suspend --nolockfs "$DM"
    dmsetup load "$DM" --table "0 $SECTORS flakey $LOOP 0 180 0"
    dmsetup resume "$DM"
    mount /dev/mapper/"$DM" "$MNT"

    # read byte 0 and a byte in the middle (offset 8m)
    local b0 bmid result
    b0=$(od -An -tx1 -N1 -j0 "$MNT/sfile" 2>/dev/null | tr -d ' ')
    bmid=$(od -An -tx1 -N1 -j8388608 "$MNT/sfile" 2>/dev/null | tr -d ' ')
    if [ "$b0" = "ab" ] && [ "$bmid" = "ab" ]; then result="SURVIVE"; else result="LOST"; fi
    local mark="  "; [ "$result" = "$EXPECT" ] && mark="ok" || mark="!!"
    printf "  [%s] %-46s bytes=0x%s,0x%s -> %-8s (expected %s)\n" "$mark" "$LABEL" "${b0:-??}" "${bmid:-??}" "$result" "$EXPECT"

    umount "$MNT" 2>/dev/null || true
    dmsetup remove "$DM" 2>/dev/null || true
    losetup -d "$LOOP" 2>/dev/null || true
    LOOP=""; rm -f "$IMG"
}

echo "======================================================================"
echo "  syncfs micro-test [$FSTYPE]: does syncfs journal the unwritten-extent"
echo "  conversion durably across a power cut?  (root + dm-flakey)"
echo "======================================================================"
test_case "-c syncfs"   "sync_range + SYNCFS        (THE CANDIDATE)"  "SURVIVE"
test_case "-c fsync"    "sync_range + FSYNC         (per-file baseline)" "SURVIVE"
test_case ""            "sync_range ONLY, no flush  (cut sanity control)" "LOST"
echo ""
echo "  VERDICT: syncfs is a viable trick IFF all three lines show 'ok'"
echo "  (SYNCFS=SURVIVE, FSYNC=SURVIVE, no-flush=LOST). If SYNCFS=LOST,"
echo "  syncfs does NOT journal the conversion either -> pivot to io_uring/WAL."
echo "======================================================================"
