#!/usr/bin/env bash
# guest/preflight.sh --phase=write|check
#
# The harness self-check, and the gate every run passes through first.
#
# THREE PROBES, because the cut has two independent layers and a probe that
# cannot tell them apart cannot gate either one:
#
#   pf_kept    fsync'd BEFORE the cut.            Must SURVIVE.
#              Catches a cut that is too aggressive and eats barriered data.
#
#   pf_lost    written after arming, page cache only, never written back.
#              Must be ABSENT — but this proves only that the VMM kill discarded
#              the GUEST PAGE CACHE. It says nothing about the device layer,
#              because at this timescale the bytes never reached it.
#
#   pf_ranged  pre-filled with a block of 'A' and fsync'd BEFORE arming, so its
#              blocks are allocated and its extent tree journaled. After arming,
#              overwritten IN PLACE with 'B' via O_DIRECT — straight to the
#              device, no page cache, no flush, no metadata change.
#              Must read back as 'A' (OLD). Reading 'B' (NEW) means un-flushed
#              device writes survived, i.e. drop_writes is not dropping.
#              This is the ONLY probe that exercises dm-flakey drop_writes, and
#              it is what makes test/t04's negative control discriminate.
#              In-place overwrite of pre-allocated blocks is load-bearing: on
#              ext4 with delayed allocation, writing into an empty file allocates
#              nothing until writeback and journals the extent only on a commit,
#              so data can reach the disk and still read back EMPTY — which is
#              indistinguishable from having been dropped.
#
# An earlier version had only the first two probes and could not fail under any
# mutation — pf_lost vanishes from page-cache loss alone, under a real cut and a
# defanged one alike. A control that cannot fail is decorative.
set -euo pipefail

PHASE="${1#--phase=}"
MNT=/mnt/qdb

case "$PHASE" in
    write)
        # Durable baseline: content AND directory entry forced out.
        printf 'kept' > "$MNT/pf_kept"
        sync "$MNT/pf_kept"

        # Pre-fill the ranged probe with a full block of 'A' and fsync it, so the
        # blocks are ALLOCATED and the extent tree is JOURNALED before the cut.
        # This matters: on ext4 with delayed allocation, writing into an empty
        # file allocates nothing until writeback, and the extent update is only
        # journaled by a commit. Without that, data can physically reach the disk
        # and STILL read back as an empty file after remount — indistinguishable
        # from having been dropped, which makes the probe useless.
        head -c 4096 /dev/zero | tr '\0' 'A' > "$MNT/pf_ranged"
        sync "$MNT/pf_ranged"
        sudo sync

        bash /opt/vmcrash/guest/arm-cut.sh

        # In-place O_DIRECT overwrite of those already-allocated blocks: straight
        # to the device, bypassing the page cache, with no flush and no metadata
        # change. Only drop_writes can lose this.
        #
        # O_DIRECT rather than sync_file_range deliberately — it needs no xfs_io
        # and cannot be silently skipped. Failure is REPORTED, not swallowed: a
        # masked failure here would leave the old content in place and look
        # exactly like a working cut.
        if ! head -c 4096 /dev/zero | tr '\0' 'B' \
                | dd of="$MNT/pf_ranged" bs=4096 count=1 conv=notrunc oflag=direct 2>/dev/null; then
            echo "PREFLIGHT-PROBE-FAILED: O_DIRECT overwrite did not run" | sudo tee /dev/console >/dev/null
        fi

        # Page cache only. Lost to the VMM kill regardless of the device layer.
        printf 'lost' > "$MNT/pf_lost" || true
        ;;

    check)
        kept=$(cat "$MNT/pf_kept" 2>/dev/null || echo MISSING)
        lost=$(cat "$MNT/pf_lost" 2>/dev/null || echo MISSING)
        # Classify the probe block by its first byte: A = pre-cut content held
        # (the O_DIRECT overwrite was dropped), B = the overwrite survived.
        case "$(head -c 1 "$MNT/pf_ranged" 2>/dev/null)" in
            A) ranged=OLD ;;
            B) ranged=NEW ;;
            "") ranged=EMPTY ;;
            *) ranged=UNEXPECTED ;;
        esac

        if [ "$kept" != "kept" ]; then
            # Too aggressive: it ate data that was properly barriered.
            echo "PREFLIGHT_FAILED fsynced-file-lost kept=$kept"
        elif [ "$lost" != "MISSING" ]; then
            # The guest page cache survived the kill. The VMM did not really die.
            echo "PREFLIGHT_FAILED unsynced-file-survived lost=$lost"
        elif [ "$ranged" = "NEW" ]; then
            # THE DEVICE-LAYER VERDICT. Bytes that reached the device without a
            # flush came back, so drop_writes is not dropping. Any durability
            # result from this configuration would be false green.
            echo "PREFLIGHT_FAILED device-write-survived ranged=$ranged"
        elif [ "$ranged" != "OLD" ]; then
            # The probe's PRE-CUT content was fsync'd, so losing it is the same
            # class of fault as pf_kept vanishing: the cut ate barriered data.
            echo "PREFLIGHT_FAILED ranged-probe-baseline-lost ranged=$ranged"
        else
            echo "PREFLIGHT_OK kept=$kept lost=$lost ranged=$ranged"
        fi
        ;;

    *)
        echo "PREFLIGHT_FAILED bad-phase=$PHASE"
        ;;
esac
