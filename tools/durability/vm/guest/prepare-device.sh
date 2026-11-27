#!/usr/bin/env bash
# guest/prepare-device.sh [--reattach] [--mode=log-writes]
#
# Builds the dm-log-writes stack over the raw data disk and mounts it.
#   (no flag)   first boot: create the mapping, mkfs, mount
#   --reattach  after a replay: mount WITHOUT mkfs, so the filesystem's own journal recovery
#               runs for real
#
# No loop device, deliberately. /dev/vdb is already a raw block device, so losetup buys nothing,
# and `losetup -D` on a shared host detaches every loop device including a live database's.
set -euo pipefail

REATTACH=0
MODE="${QDB_DEVICE_MODE:-log-writes}"
for a in "$@"; do
    case "$a" in
        --reattach) REATTACH=1 ;;
        --mode=*)   MODE="${a#*=}" ;;
    esac
done
[ "$MODE" = "log-writes" ] || { echo "ERROR: unsupported --mode=$MODE (log-writes only)" >&2; exit 1; }

DM=qdbdata
DEV=/dev/vdb
LOGDEV=/dev/vdc
MNT=/mnt/qdb

# log-writes records every write and every flush to a separate device, so the run can be replayed
# to any flush boundary afterwards. Nothing is dropped live: the discarding happens at replay
# time, against a flush boundary rather than against the moment of a cut.
[ -b "$LOGDEV" ] || { echo "ERROR: $LOGDEV absent; boot with a log disk" >&2; exit 1; }
SECTORS=$(sudo blockdev --getsz "$DEV")
sudo modprobe dm-log-writes
if ! sudo dmsetup info "$DM" >/dev/null 2>&1; then
    sudo dmsetup create "$DM" --table "0 $SECTORS log-writes $DEV $LOGDEV"
fi
[ "$REATTACH" -eq 1 ] || sudo mkfs.ext4 -F -q "/dev/mapper/$DM"
sudo mkdir -p "$MNT"
# QDB_FS_MOUNT_OPTS exists for a single-variable A/B on ext4's journalling mode. Under the default
# data=ordered any fsync forces a journal commit that writes back data blocks for every inode in
# the running transaction, so one process fsyncing its own bookkeeping file can drag an unrelated
# process's un-synced data to disk. data=writeback removes that coupling.
if ! mountpoint -q "$MNT"; then
    if [ -n "${QDB_FS_MOUNT_OPTS:-}" ]; then
        echo "prepare-device: mounting with non-default options: $QDB_FS_MOUNT_OPTS" >&2
        sudo mount -o "$QDB_FS_MOUNT_OPTS" "/dev/mapper/$DM" "$MNT"
    else
        sudo mount "/dev/mapper/$DM" "$MNT"
    fi
fi
sudo chown ubuntu:ubuntu "$MNT"
echo "device ready: /dev/mapper/$DM (log-writes, log=$LOGDEV, opts=${QDB_FS_MOUNT_OPTS:-default}) -> $MNT"
