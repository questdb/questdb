#!/usr/bin/env bash
# guest/arm-cut.sh — THE CUT, guest half.
#
# Switch dm-flakey to always-down + drop_writes so nothing further can reach the
# disk, THEN tell the host to kill the VMM.
#
# THE ORDER IS THE WHOLE POINT. The device must stop accepting writes BEFORE the
# machine dies. Reverse it and a write issued during the join window can reach
# durability, which is exactly the false-green this design exists to exclude.
#
# Why both halves are needed at all: killing the VMM discards the guest page
# cache (correct) but anything the guest kernel already wrote back has reached
# host storage and would survive — whereas on real hardware it would sit in the
# disk's volatile write cache and die. drop_writes closes that gap. See spec §2.
set -euo pipefail

DM=qdbdata
DEV=/dev/vdb
SECTORS=$(sudo blockdev --getsz "$DEV")

# QDB_CUT_DROP_WRITES=0 arms a cut that does NOT drop writes — a deliberately
# defanged cut, used ONLY by test/t04 as the negative control. It must make the
# preflight FAIL. Never set this in a real run.
#
# This is the correct mutation for that control because drop_writes is the
# mechanism that actually loses un-flushed data here. Mutating the QEMU cache
# mode instead does NOT discriminate: a post-arm write is discarded by dm-flakey
# inside the guest and never reaches QEMU's cache at all, and an un-fsync'd
# write that the guest already wrote back survives a VMM kill under every cache
# mode, because killing the VMM does not take the host's power with it.
if [ "${QDB_CUT_DROP_WRITES:-1}" = "0" ]; then
    echo "arm-cut: WARNING — arming WITHOUT drop_writes (negative control only)" >&2
    sudo dmsetup suspend "$DM"
    sudo dmsetup load "$DM" --table "0 $SECTORS flakey $DEV 0 180 0"
    sudo dmsetup resume "$DM"
else
    sudo dmsetup suspend "$DM"
    sudo dmsetup load "$DM" --table "0 $SECTORS flakey $DEV 0 0 180 1 drop_writes"
    sudo dmsetup resume "$DM"
fi

# The join. The host is tailing console.log for this token.
echo "CUT-ARMED" | sudo tee /dev/console >/dev/null
