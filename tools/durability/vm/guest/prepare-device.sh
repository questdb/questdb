#!/usr/bin/env bash
# guest/prepare-device.sh [--reattach]
#
# Builds the dm-flakey pass-through stack over the raw data disk and mounts it.
#   (no flag)   first boot: create the mapping, mkfs, mount
#   --reattach  after a cut: restore pass-through and mount WITHOUT mkfs, so
#               the filesystem's own journal recovery runs for real
#
# NO LOOP DEVICE, DELIBERATELY. /dev/vdb is already a raw block device, so
# losetup buys nothing — and losetup/`losetup -D` is precisely the idiom that,
# on the development host, would detach a live database's filesystem. Keeping
# it out of the harness entirely is cheaper than remembering not to misuse it.
set -euo pipefail

REATTACH=0
MODE="${QDB_DEVICE_MODE:-flakey}"
for a in "$@"; do
    case "$a" in
        --reattach) REATTACH=1 ;;
        --mode=*)   MODE="${a#*=}" ;;
    esac
done

DM=qdbdata
DEV=/dev/vdb
LOGDEV=/dev/vdc
MNT=/mnt/qdb

# log-writes mode: record every write AND every flush to a separate device, so
# the run can later be replayed to any flush boundary. Unlike flakey, this does
# NOT drop anything live -- the whole point is that the discarding happens at
# REPLAY time, against the flush boundary rather than against arming time.
if [ "$MODE" = "log-writes" ]; then
    [ -b "$LOGDEV" ] || { echo "ERROR: $LOGDEV absent; boot with a log disk" >&2; exit 1; }
    SECTORS=$(sudo blockdev --getsz "$DEV")
    sudo modprobe dm-log-writes
    if ! sudo dmsetup info "$DM" >/dev/null 2>&1; then
        sudo dmsetup create "$DM" --table "0 $SECTORS log-writes $DEV $LOGDEV"
    fi
    [ "$REATTACH" -eq 1 ] || sudo mkfs.ext4 -F -q "/dev/mapper/$DM"
    sudo mkdir -p "$MNT"
    mountpoint -q "$MNT" || sudo mount "/dev/mapper/$DM" "$MNT"
    sudo chown ubuntu:ubuntu "$MNT"
    echo "device ready: /dev/mapper/$DM (log-writes, log=$LOGDEV) -> $MNT"
    exit 0
fi

[ -b "$DEV" ] || { echo "ERROR: $DEV is not a block device" >&2; exit 1; }
SECTORS=$(sudo blockdev --getsz "$DEV")

sudo modprobe dm-flakey

# dm-flakey table syntax:
#   <start> <sectors> flakey <dev> <offset> <up_interval> <down_interval> [<nfeat> <feat>...]
# Pass-through is a long up interval with a zero down interval.
PASSTHROUGH="0 $SECTORS flakey $DEV 0 180 0"

if sudo dmsetup info "$DM" >/dev/null 2>&1; then
    sudo dmsetup suspend "$DM"
    sudo dmsetup load "$DM" --table "$PASSTHROUGH"
    sudo dmsetup resume "$DM"
else
    sudo dmsetup create "$DM" --table "$PASSTHROUGH"
fi

if [ "$REATTACH" -eq 0 ]; then
    sudo mkfs.ext4 -F -q "/dev/mapper/$DM"
fi

sudo mkdir -p "$MNT"
if ! mountpoint -q "$MNT"; then
    # QDB_FS_MOUNT_OPTS exists for one purpose: a single-variable A/B on ext4's
    # journalling mode.
    #
    # Under the default data=ordered, ANY fsync forces a journal commit that
    # writes back data blocks for every inode in the running transaction. So one
    # process fsyncing its own bookkeeping file can drag an unrelated process's
    # un-synced data to disk -- a FOREIGN FLUSH. That makes a no-sync workload
    # look durable for reasons that have nothing to do with the code under test.
    # data=writeback removes that coupling.
    if [ -n "${QDB_FS_MOUNT_OPTS:-}" ]; then
        echo "prepare-device: mounting with non-default options: $QDB_FS_MOUNT_OPTS" >&2
        sudo mount -o "$QDB_FS_MOUNT_OPTS" "/dev/mapper/$DM" "$MNT"
    else
        sudo mount "/dev/mapper/$DM" "$MNT"
    fi
fi
sudo chown ubuntu:ubuntu "$MNT"

# QDB_SUPPRESS_WRITEBACK=1 — DIAGNOSTIC ONLY, never a normal run.
#
# Stops the guest kernel from writing dirty pages back on its own schedule, so
# anything not explicitly fsync'd stays in the page cache and dies with the VMM.
#
# Why this exists: this harness has no VOLATILE DEVICE WRITE CACHE. On real
# hardware, kernel writeback lands in the disk's volatile cache and is lost on
# power loss unless a FLUSH moves it to platter. Here, writeback goes through
# virtio-blk with cache=none straight to host storage, and the host never lost
# power -- so it survives. dm-flakey drop_writes cannot help: it only discards
# writes issued AFTER arming, not ones already at the device but unflushed.
#
# Setting this isolates that difference: if loss appears only with writeback
# suppressed, then background writeback into a non-volatile virtual device is
# what has been masking the RPO gap.
if [ "${QDB_SUPPRESS_WRITEBACK:-0}" = "1" ]; then
    echo "prepare-device: SUPPRESSING guest writeback (diagnostic)" >&2
    sudo sysctl -q -w vm.dirty_background_ratio=95
    sudo sysctl -q -w vm.dirty_ratio=99
    sudo sysctl -q -w vm.dirty_expire_centisecs=360000
    sudo sysctl -q -w vm.dirty_writeback_centisecs=360000
fi

echo "device ready: /dev/mapper/$DM -> $MNT (reattach=$REATTACH, sectors=$SECTORS)"
