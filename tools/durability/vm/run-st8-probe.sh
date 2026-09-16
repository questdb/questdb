#!/usr/bin/env bash
# run-st8-probe.sh [--control] [--size N] [--lookback N]
#
# ST8 AGAINST A REAL KERNEL. One question, one run, one verdict line.
#
#   Does fdatasync(A) make file B's data durable, when B's extent conversion is still
#   pending and B is never fsynced?
#
# The Java model answers YES (CrashFaultFilesFacade.modelSharedJournal = true, the default):
# one filesystem-wide journal commit covers every inode's pending extent conversion. ~50 crash
# test classes and the batched-flush optimisation rest on that answer, and its ground truth is
# a reading of man pages rather than a kernel. If it is wrong, all ~50 are wrong in the SAME
# direction and every one of them stays green.
#
# THE EXPECTED OUTCOME IS "MODEL CONFIRMED", and that is stated here in advance so a confirming
# result cannot later be dressed up as a finding. The value of this run is INSURANCE: cheap to
# check, expensive to be wrong about. A refutation (ST8_LOST) is a PRODUCT finding about the
# batched-flush optimisation and outranks everything else in the durability-CI effort.
#
# WHY dm-log-writes AND NOT dm-flakey. The flakey path's cut boundary is ARMING TIME, and it
# can only discard writes issued after arming -- so data already at the device but unflushed
# survives it, which is exactly the state this probe creates. Recording with dm-log-writes and
# replaying to a chosen boundary reconstructs the device state a volatile write cache would
# have left. Same instrument the flush sweep uses. See spec.md 3.
#
# ============================================================================================
# A DEFINES THE BOUNDARY. Read this before changing the scan below.
# ============================================================================================
#
# The first working run of this probe replayed to "the last flush inside the fdatasync
# bracket" and got:
#
#     st8-verify: a.d LOST (all 65536 bytes are 0x00 (extent reads unwritten))
#     st8-verify: b.d LOST (all 65536 bytes are 0x00 (extent reads unwritten))
#     ST8_BOUNDARY 9  ST8_FLUSHTOTAL 9
#     replayed to flush 9/9 (entry 383): 371 writes applied, 2 entries after the boundary discarded
#
# A was fdatasync'd and came back unwritten, so the instrument -- not the model -- was broken.
# The arithmetic says why: the bracket was entries 378(pre-mark)..385(post-mark), the flush was
# entry 383, and the two discarded entries were 384 and 385. 385 is the post-mark, so ONE real
# write entry (384) sat between the flush and the moment fdatasync returned.
#
# That entry is the jbd2 COMMIT BLOCK. jbd2 writes the journal descriptor and data blocks,
# then issues the commit block with REQ_PREFLUSH|REQ_FUA; dm-log-writes records the preflush
# as its own FLUSH entry and the commit block as the NEXT entry, carrying FUA. The FLUSH entry
# therefore marks the point just BEFORE the transaction becomes valid. Replaying to it leaves
# a journal whose last transaction has no commit block, ext4's recovery discards it at mount,
# and the extent still reads unwritten -- for A and for B alike.
#
# Note the trap this would have been: "B reads zeros" is the model-refuted signature, and it
# was produced here by a boundary that was one entry early. Only the A-is-the-instrument check
# stopped it being reported as a product finding.
#
# So the boundary is no longer inferred from the log format at all. A is fdatasync'd, so "the
# boundary at which A becomes durable" IS the boundary at which A's commit took effect, by
# definition. The scan below replays each candidate boundary in chronological order, reads A
# at each, takes the FIRST boundary where A is fully 0xBB, and reports B FROM THAT SAME
# BOUNDARY. If no candidate makes A durable, there is no verdict about the model -- only
# ST8_INSTRUMENT_FAILED and the table.
#
# The mark bracket is kept: it is still what says WHERE to look, and its reasoning (a later
# jbd2 timer commit would journal B and fake a confirmation) is unchanged.
#
# SCOPE: ST8 ONLY. The ST1..ST10 battery is a separate decision (durability-ci issues/10).
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/qemu.sh
source "$HERE/lib/qemu.sh"
# shellcheck source=lib/verdict.sh
source "$HERE/lib/verdict.sh"

bash "$HERE/check-host.sh" >/dev/null || { bash "$HERE/check-host.sh"; exit 1; }

MODE=probe
SIZE="${QDB_ST8_SIZE:-65536}"
# How many flush boundaries BEFORE the bracket to include in the scan. The answer should be
# inside the bracket; scanning a little earlier costs one replay each and is what makes the
# table show that A was still lost before its own commit -- i.e. that the boundary found is
# the first one that works, not merely one that works.
LOOKBACK="${QDB_ST8_LOOKBACK:-3}"
for a in "$@"; do
    case "$a" in
        --control)    MODE=control ;;
        --size=*)     SIZE="${a#*=}" ;;
        --lookback=*) LOOKBACK="${a#*=}" ;;
        *) echo "run-st8-probe: unknown argument $a" >&2; exit 64 ;;
    esac
done

STATE_DIR="${QDB_VMCRASH_STATE:-/data/qdb-vmcrash}"
BASE="$STATE_DIR/base"
KEY="$BASE/id_ed25519"
LOG="$STATE_DIR/st8-probe.log"
RESULT="$STATE_DIR/st8-$MODE.result"
COUNTERPART_MODE=$([ "$MODE" = probe ] && echo control || echo probe)
COUNTERPART="$STATE_DIR/st8-$COUNTERPART_MODE.result"
STAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)

[ -f "$BASE/golden.qcow2" ] || { echo "LOUD_FAILURE: no golden image; run build-image.sh first"; exit 1; }

RUN="$STATE_DIR/st8-$MODE-$$"
mkdir -p "$RUN"
qemu-img create -f qcow2 -b "$BASE/golden.qcow2" -F qcow2 "$RUN/overlay.qcow2" >/dev/null

# 2 GiB, not the sweep's 40 GiB. The probe writes two small files, and the size USED to be
# load-bearing: the data device is cleared before EVERY replay in the scan (see below), and a
# dd zero costs time proportional to the device, multiplied by the number of candidates. The
# reset is now an unmapping discard -- 8 ms for the whole device regardless of size -- so that
# constraint is gone. The size is kept at 2 GiB anyway: nothing here needs more, and a small
# device also keeps the kept-on-failure run directory small.
DATA_MB=2048
truncate -s "${DATA_MB}M" "$RUN/data.raw"
truncate -s 2G "$RUN/log.raw"

echo "ST8 model-vs-kernel probe — $STAMP"
echo "  mode=$MODE size=$SIZE bytes lookback=$LOOKBACK flushes"
if [ "$MODE" = control ]; then
    echo "  *** CONTROL: fdatasync(A) is OMITTED. B MUST come back not-durable. ***"
    echo "  A control that reports B durable means the probe proves nothing at all."
fi

keep() { echo "run state kept at $RUN" >&2; }
cleanup_vm() { vm_kill "$RUN" 2>/dev/null || true; }
trap cleanup_vm EXIT INT TERM

# ---- boot, record, run the sequence ----------------------------------------------------
P=$(vm_free_port)
vm_boot "$RUN" "$RUN/overlay.qcow2" "$RUN/data.raw" "$P" "" "$RUN/log.raw"
vm_wait_ssh "$P" "$KEY" 240 || { keep; echo "LOUD_FAILURE: guest never answered SSH"; exit 1; }
vm_scp_dir "$P" "$KEY" "$HERE/guest" /opt/vmcrash/ \
    || { keep; echo "LOUD_FAILURE: could not ship the guest scripts"; exit 1; }
vm_ssh "$P" "$KEY" "sudo sync" \
    || { keep; echo "LOUD_FAILURE: could not flush shipped artifacts to the guest boot disk"; exit 1; }

vm_ssh "$P" "$KEY" "bash /opt/vmcrash/guest/prepare-device.sh --mode=log-writes" >/dev/null \
    || { keep; echo "LOUD_FAILURE: could not build the log-writes stack"; exit 1; }

# NAME THE KERNEL AND THE FILESYSTEM. The answer to this question is kernel-specific and
# filesystem-configuration-specific; a result that cannot say which kernel produced it is not
# evidence, it is an anecdote. Captured BEFORE the sequence so nothing here lands in the window
# between the sequence and the cut.
#
# The ext4 feature list matters more than it looks: ST7 (per-inode journaling, B LOST) is the
# ext4 FAST_COMMIT world and ST8 (shared jbd2 journal, B durable) is the classic one. If this
# guest's filesystem has fast_commit enabled, the scenario under test is ST7's world, not
# ST8's, and the result must be read accordingly. (Measured on this image: no fast_commit.)
ENVINFO=$(vm_ssh "$P" "$KEY" "echo kernel=\$(uname -r); \
    echo e2fsprogs=\$(mkfs.ext4 -V 2>&1 | head -1 | tr -d '\n'); \
    echo mount=\$(grep ' /mnt/qdb ' /proc/mounts | head -1); \
    echo features=\$(sudo tune2fs -l /dev/mapper/qdbdata 2>/dev/null | grep -i '^Filesystem features' | cut -d: -f2- | tr -s ' ')" 2>&1)
echo "$ENVINFO" | sed 's/^/  /'

PROBE_FLAGS="--size=$SIZE"
[ "$MODE" = control ] && PROBE_FLAGS="$PROBE_FLAGS --control"
# shellcheck disable=SC2086
vm_ssh "$P" "$KEY" "bash /opt/vmcrash/guest/st8-probe.sh $PROBE_FLAGS" \
    || { keep; echo "LOUD_FAILURE: the ST8 sequence did not complete in the guest"; exit 1; }

# Let the dm-log-writes kthread drain its queue to the log device. Without this the tail of
# the recording -- which is the only part that matters here, since it holds the fdatasync
# flush, the commit block and the post mark -- may never reach /dev/vdc.
#
# Two seconds, and no more: jbd2's periodic commit timer is 5 s, and every second spent here
# is a second in which an unrelated journal commit could append a flush after the bracket.
vm_ssh "$P" "$KEY" "sleep 2" || true

# THE CUT: kill the VMM with the filesystem STILL MOUNTED.
#
# Do NOT unmount first. A clean unmount flushes the page cache AND commits the journal, which
# would journal B's pending extent conversion as a side effect of the teardown -- and B would
# then come back durable no matter what fdatasync(A) did or did not do. That is the experiment
# answering itself. The same reasoning applies to `sync`, to a graceful shutdown, and to any
# command touching the tested filesystem after the sequence ends.
vm_kill "$RUN"

# ---- reboot and reconstruct ------------------------------------------------------------
rm -f "$RUN/overlay.qcow2"
qemu-img create -f qcow2 -b "$BASE/golden.qcow2" -F qcow2 "$RUN/overlay.qcow2" >/dev/null
P2=$(vm_free_port)
# The replay boot takes discard=unmap so the scan's per-candidate reset is a real unmap. The
# recording boot above deliberately does not: a discard issued while dm-log-writes is recording
# would be logged as a DISCARD entry, and this probe reasons about the log ENTRY BY ENTRY.
QDB_VM_DATA_DISCARD=unmap vm_boot "$RUN" "$RUN/overlay.qcow2" "$RUN/data.raw" "$P2" "" "$RUN/log.raw"
vm_wait_ssh "$P2" "$KEY" 240 || { keep; echo "LOUD_FAILURE: guest never rebooted"; exit 1; }
vm_scp_dir "$P2" "$KEY" "$HERE/guest" /opt/vmcrash/ \
    || { keep; echo "LOUD_FAILURE: could not re-ship the guest scripts after the cut"; exit 1; }
# This probe's answer turns on ONE SECTOR of extent metadata, so a reset that silently did
# nothing would not fail loudly -- it would return a confidently wrong durability boundary.
replay_reset_assert "$P2" "$KEY" || { keep; echo "LOUD_FAILURE: the device reset is not real; every candidate below would inherit the previous one"; exit 1; }
RESET_CMD="$(replay_reset_cmd)"

# WHERE TO LOOK, AND WHAT TO OFFER THE SCAN.
#
# The bracket still defines the window: the probe marks the log immediately before and after
# its fdatasync, so everything relevant lies between st8-pre-fdatasync and st8-post-fdatasync.
# What the bracket does NOT do any more is pick the answer -- it only produces CANDIDATES,
# which the scan then tests by reading A.
#
# Candidates, in chronological order:
#   * every flush boundary from (bracket flush - lookback) to the last flush in the log
#   * every entry inside the bracket, up to and including the post mark
# The second group is the one that matters and the one that needs --to-entry: the jbd2 commit
# block is written AFTER the last flush entry, so NO flush boundary in this recording can ever
# include it. That is exactly why run 2 failed.
#
# The scan below also dumps every bracket entry with its flags, so the FUA commit block is
# visible in the output rather than inferred from entry arithmetic.
#
# The locator duplicates replay-log.py's on-disk format constants, because replay-log.py
# exposes neither mark names nor a --to-mark, and this ticket is not licensed to change it.
# Duplication that cannot be avoided is made loud instead: the flush TOTAL from this scan is
# cross-checked against replay-log.py's own --list, and a disagreement fails the run.
LOCATOR_PY=$(cat <<'PYEOF'
import os
import struct
import sys

log_path, mode, lookback = sys.argv[1], sys.argv[2], int(sys.argv[3])

WRITE_LOG_MAGIC = 0x6A736677736872
LOG_FLUSH_FLAG = 1 << 0
LOG_FUA_FLAG = 1 << 1
LOG_DISCARD_FLAG = 1 << 2
LOG_MARK_FLAG = 1 << 3
LOG_METADATA_FLAG = 1 << 4
SUPER_FMT = "<QQQI"
ENTRY_FMT = "<QQQQ"
ENTRY_LEN = struct.calcsize(ENTRY_FMT)
BIO_SECTOR = 512

fd = os.open(log_path, os.O_RDONLY)
magic, version, nr_entries, sectorsize = struct.unpack(
    SUPER_FMT, os.pread(fd, struct.calcsize(SUPER_FMT), 0))
if magic != WRITE_LOG_MAGIC:
    sys.exit("ST8_BOUNDARY_ERROR not-a-log magic=0x%x" % magic)
log_bytes = os.lseek(fd, 0, os.SEEK_END)


def align_up(n, a):
    return (n + a - 1) // a * a


def describe(flags):
    names = []
    for bit, name in ((LOG_FLUSH_FLAG, "FLUSH"), (LOG_FUA_FLAG, "FUA"),
                      (LOG_DISCARD_FLAG, "DISCARD"), (LOG_MARK_FLAG, "MARK"),
                      (LOG_METADATA_FLAG, "META")):
        if flags & bit:
            names.append(name)
    return ",".join(names) if names else "-"


off = sectorsize
i = 0
hard_cap = max(nr_entries * 4, nr_entries + 4096)
flushes = []
marks = {}
entries = []
while i < hard_cap and off + ENTRY_LEN <= log_bytes:
    header_off = off
    raw = os.pread(fd, ENTRY_LEN, header_off)
    if len(raw) < ENTRY_LEN:
        break
    sector, nr_sectors, flags, data_len = struct.unpack(ENTRY_FMT, raw)
    if flags == 0 and sector == 0 and nr_sectors == 0 and data_len == 0:
        break
    if flags >> 5:
        break
    if data_len > sectorsize:
        break
    if nr_sectors > (1 << 32):
        break
    off += sectorsize
    name = ""
    if flags & LOG_MARK_FLAG:
        raw_name = os.pread(fd, data_len, header_off + ENTRY_LEN)
        name = raw_name.split(b"\x00")[0].decode("utf-8", "replace")
        marks.setdefault(name, i)
    elif not (flags & LOG_DISCARD_FLAG) and nr_sectors:
        off += align_up(nr_sectors * BIO_SECTOR, sectorsize)
    if flags & LOG_FLUSH_FLAG:
        flushes.append(i)
    entries.append((i, sector, nr_sectors, flags, name))
    i += 1
os.close(fd)

pre = marks.get("st8-pre-fdatasync")
post = marks.get("st8-post-fdatasync")
if pre is None or post is None:
    sys.exit("ST8_BOUNDARY_ERROR marks-missing pre=%s post=%s found=%s"
             % (pre, post, ",".join(sorted(marks)) or "none"))

inside = [(n, e) for n, e in enumerate(flushes, 1) if pre < e < post]
before = [(n, e) for n, e in enumerate(flushes, 1) if e < pre]

print("locator: entries=%d flushes=%d pre_entry=%d post_entry=%d inside=%d before=%d"
      % (i, len(flushes), pre, post, len(inside), len(before)))

# THE EVIDENCE DUMP. Run 2's diagnosis rested on entry arithmetic ("2 entries discarded, one
# of them the post mark"); printing the bracket makes the commit block and its FUA flag
# visible directly, so the next reader does not have to re-derive it.
print("locator: --- bracket entries %d..%d ---" % (pre, post))
for (idx, sector, nr_sectors, flags, name) in entries:
    if pre <= idx <= post:
        print("locator:   entry %d sector=%d nr_sectors=%d flags=%s%s"
              % (idx, sector, nr_sectors, describe(flags),
                 (" name=" + name) if name else ""))

# The bracket boundary as it used to be computed: still reported, now only as a reference
# point for the scan and for comparison with the boundary A actually turns out to need.
if inside:
    print("ST8_BRACKET_FLUSH %d" % inside[-1][0])
elif mode != "control":
    print("ST8_BRACKET_FLUSH 0")
    print("locator: NOTE no flush inside the bracket; fdatasync(A) recorded none")
else:
    print("ST8_BRACKET_FLUSH 0")

# CANDIDATES. Flush boundaries first (they work with an unpatched replay-log.py), then every
# entry in the bracket (these need --to-entry). Emitted as "<entry> <label>"; the driver sorts
# by entry index so the scan runs in chronological order regardless of which group they came
# from, and dedupes where a flush entry is also a bracket entry.
bracket_flush_ord = inside[-1][0] if inside else (before[-1][0] if before else 1)
lo = max(1, bracket_flush_ord - lookback)
for n, e in enumerate(flushes, 1):
    if lo <= n <= len(flushes):
        print("ST8_CANDIDATE %d flush%d" % (e, n))
for (idx, sector, nr_sectors, flags, name) in entries:
    if pre <= idx <= post:
        print("ST8_CANDIDATE %d entry%d" % (idx, idx))

print("ST8_FLUSHTOTAL %d" % len(flushes))
PYEOF
)

LOCOUT=$(vm_ssh "$P2" "$KEY" "sudo python3 - /dev/vdc $MODE $LOOKBACK <<'PYX'
$LOCATOR_PY
PYX" 2>&1)
echo "$LOCOUT" | sed 's/^/  /'
FLUSHTOTAL=$(echo "$LOCOUT" | grep -oE '^ST8_FLUSHTOTAL [0-9]+' | awk '{print $2}')
BRACKET_FLUSH=$(echo "$LOCOUT" | grep -oE '^ST8_BRACKET_FLUSH [0-9]+' | awk '{print $2}')
if [ -z "$FLUSHTOTAL" ]; then
    keep
    echo "$STAMP st8 mode=$MODE verdict=LOUD_FAILURE locator=failed" >> "$LOG"
    echo "LOUD_FAILURE: could not read the recording's mark bracket"
    exit 1
fi

# THE CROSS-CHECK. Two parsers, one log: if they do not agree on how many flushes the
# recording holds, the flush ordinals this script computed do not mean what replay-log.py will
# take them to mean, and the replay would silently reconstruct the wrong device state.
LISTOUT=$(vm_ssh "$P2" "$KEY" "sudo python3 /opt/vmcrash/guest/replay-log.py --log /dev/vdc --list | head -1" 2>&1)
LISTTOTAL=$(echo "$LISTOUT" | grep -oE '[0-9]+ flushes' | grep -oE '^[0-9]+')
if [ -z "$LISTTOTAL" ] || [ "$LISTTOTAL" != "$FLUSHTOTAL" ]; then
    keep
    echo "LOUD_FAILURE: flush-count disagreement — locator says ${FLUSHTOTAL:-?}, replay-log.py says ${LISTTOTAL:-?}"
    echo "  the two parsers number boundaries differently, so the replay would not land where this script thinks"
    exit 1
fi

# DOES THIS replay-log.py SUPPORT ENTRY-GRANULAR REPLAY?
#
# It must, for this experiment to be able to answer anything: the jbd2 commit block that makes
# A durable is written AFTER the last FLUSH entry, so no --to-flush boundary can include it.
# Without --to-entry the scan can still run -- and its table is real evidence for exactly that
# claim, since it will show A lost at every flush boundary -- but it cannot produce a model
# verdict. Say so loudly rather than letting the run look like a normal failure.
if vm_ssh "$P2" "$KEY" "sudo python3 /opt/vmcrash/guest/replay-log.py --help 2>&1 | grep -q -- '--to-entry'"; then
    HAVE_TO_ENTRY=1
else
    HAVE_TO_ENTRY=0
    echo
    echo "  NOTE: this replay-log.py has no --to-entry, so only FLUSH boundaries can be replayed."
    echo "  The commit block that makes A durable is recorded after the last flush, so the scan"
    echo "  below is expected to find no A-durable boundary. See the patch proposed in the"
    echo "  ticket: without it this experiment cannot reach the state it needs to read."
fi

# ---- THE SCAN: let A define the boundary ------------------------------------------------
#
# Chronological order, first A-durable boundary wins, and B is read AT THAT SAME BOUNDARY --
# never at a different one, which is why both columns come from a single --verify call.
CANDS=$(echo "$LOCOUT" | grep -oE '^ST8_CANDIDATE [0-9]+ [a-z0-9]+' | awk '{print $2" "$3}' | sort -k1,1n -u)

echo
printf '  %-10s %-10s %-10s %s\n' "boundary" "a.d" "b.d" "replay"
printf '  %-10s %-10s %-10s %s\n' "--------" "---" "---" "------"

TABLE=""
HIT_LABEL=""; HIT_A=""; HIT_B=""
CTRL_B_DURABLE=0
SCANNED=0
while read -r ENTRY LABEL; do
    [ -n "$ENTRY" ] || continue
    case "$LABEL" in
        flush*) REPLAY_ARG="--to-flush ${LABEL#flush}" ;;
        entry*) REPLAY_ARG="--to-entry ${LABEL#entry}"
                if [ "$HAVE_TO_ENTRY" -eq 0 ]; then
                    printf '  %-10s %-10s %-10s %s\n' "$LABEL" "-" "-" "skipped (no --to-entry)"
                    TABLE="$TABLE$LABEL SKIPPED SKIPPED no---to-entry"$'\n'
                    continue
                fi ;;
        *) continue ;;
    esac

    # Clear the data device before EVERY replay.
    #
    # Two independent reasons, both fatal if skipped. (1) dm-log-writes passes writes THROUGH
    # to the data device, so /dev/vdb still holds the final crashed state; replaying to a
    # boundary re-applies writes up to it but cannot revert later ones, and the one sector
    # that decides this experiment is the extent metadata. (2) mounting at the previous
    # candidate ran ext4 journal RECOVERY, which writes to the device -- carrying that into
    # the next replay would let an earlier boundary inherit a later one's recovered state.
    #
    # This was a full-device dd zero, and is now the harness-wide replay_reset_cmd -- an
    # unmapping discard. The SEMANTICS ARE THE ONES THIS SCRIPT ALREADY REQUIRED: both clear
    # the WHOLE device to zeros, which is what the two reasons above demand. What changes is
    # only the cost, ~8 ms against a multi-second dd per candidate. It also means this scan
    # and run-flush-sweep.sh now clear the device the same way, so a boundary means the same
    # thing in both -- the ST8 result and the sweep's verdicts are finally commensurable.
    # replay_reset_assert above proves the discard really zeroes before any of this runs.
    #
    # </dev/null IS LOAD-BEARING. vm_ssh runs ssh without -n, so ssh inherits this loop's
    # stdin and DRAINS THE CANDIDATE LIST: without it the scan silently tests exactly one
    # boundary and then exits the loop, which would read as "no boundary made A durable" --
    # an instrument failure invented by the harness. run-flush-sweep.sh never met this
    # because it iterates with `for n in $points`, not a while-read.
    OUT=$(vm_ssh "$P2" "$KEY" "sudo umount /mnt/qdb 2>/dev/null; \
        $RESET_CMD; \
        sudo python3 /opt/vmcrash/guest/replay-log.py --log /dev/vdc --replay /dev/vdb $REPLAY_ARG 2>&1 | tail -1; \
        sudo mkdir -p /mnt/qdb; \
        if sudo mount /dev/vdb /mnt/qdb 2>/dev/null; then \
            bash /opt/vmcrash/guest/st8-probe.sh --verify --size=$SIZE; \
        else echo 'ST8_READBACK a=MOUNT_FAILED b=MOUNT_FAILED'; fi" </dev/null 2>&1)

    RB=$(echo "$OUT" | grep -oE '^ST8_READBACK a=[A-Z_]+ b=[A-Z_]+')
    A=$(echo "$RB" | sed -nE 's/.*a=([A-Z_]+).*/\1/p')
    B=$(echo "$RB" | sed -nE 's/.*b=([A-Z_]+)$/\1/p')
    RSUM=$(echo "$OUT" | grep -oE 'replayed to [^:]*' | head -1)
    : "${A:=UNREADABLE}" "${B:=UNREADABLE}" "${RSUM:=-}"
    SCANNED=$((SCANNED + 1))

    printf '  %-10s %-10s %-10s %s\n' "$LABEL" "$A" "$B" "$RSUM"
    TABLE="$TABLE$LABEL $A $B"$'\n'

    [ "$B" = DURABLE ] && CTRL_B_DURABLE=1

    if [ -z "$HIT_LABEL" ] && [ "$A" = DURABLE ]; then
        HIT_LABEL="$LABEL"; HIT_A="$A"; HIT_B="$B"
        # FIRST A-durable boundary is the answer, by the definition at the top of this file.
        # The probe stops here; the control keeps scanning, because its question is "did B
        # EVER become durable without an fdatasync", which needs the whole window.
        [ "$MODE" = probe ] && break
    fi
done <<< "$CANDS"

vm_kill "$RUN"

if [ "$SCANNED" -eq 0 ]; then
    keep
    echo "$STAMP st8 mode=$MODE verdict=LOUD_FAILURE scan=empty" >> "$LOG"
    echo "LOUD_FAILURE: the scan tested no boundaries at all"
    exit 1
fi

# ---- the verdict -----------------------------------------------------------------------
VERDICT=""
if [ "$MODE" = probe ]; then
    if [ -z "$HIT_LABEL" ]; then
        # A never became durable anywhere in the window. The instrument did not work, so B
        # says nothing -- and "B reads zeros" is precisely the model-refuted signature, which
        # is why this must never be reported as a model verdict.
        VERDICT=ST8_INSTRUMENT_FAILED
    else
        case "$HIT_B" in
            DURABLE) VERDICT=ST8_DURABLE ;;
            LOST)    VERDICT=ST8_LOST ;;
            *)       VERDICT=ST8_INDETERMINATE ;;
        esac
    fi
else
    if [ "$CTRL_B_DURABLE" -eq 1 ]; then
        VERDICT=ST8_CONTROL_FAILED
    else
        VERDICT=ST8_CONTROL_OK
    fi
fi

{
    echo "mode=$MODE"
    echo "verdict=$VERDICT"
    echo "a=${HIT_A:-NONE}"
    echo "b=${HIT_B:-NONE}"
    echo "boundary=${HIT_LABEL:-NONE}"
    echo "bracket_flush=${BRACKET_FLUSH:-0}"
    echo "flushes=$FLUSHTOTAL"
    echo "to_entry_supported=$HAVE_TO_ENTRY"
    echo "size=$SIZE"
    echo "stamp=$STAMP"
    echo "$ENVINFO"
    echo "scan_table<<"
    printf '%s' "$TABLE"
    echo ">>"
} > "$RESULT"
echo "$STAMP st8 mode=$MODE verdict=$VERDICT boundary=${HIT_LABEL:-NONE} a=${HIT_A:-NONE} b=${HIT_B:-NONE} flushes=$FLUSHTOTAL" >> "$LOG"

echo
case "$VERDICT" in
    ST8_DURABLE)
        echo "ST8_DURABLE  boundary=$HIT_LABEL A=$HIT_A B=$HIT_B"
        echo "  At the first boundary where A is durable, B is durable too: fdatasync(A) carried"
        echo "  B's pending extent conversion. modelSharedJournal=true matches this kernel, the"
        echo "  batched-flush optimisation's assumption holds here, and the ~50 model-based crash"
        echo "  classes rest on a belief this kernel shares. This is the EXPECTED outcome."
        ;;
    ST8_LOST)
        echo "ST8_LOST  boundary=$HIT_LABEL A=$HIT_A B=$HIT_B"
        echo "  *** STOP AND REPORT. This is a PRODUCT finding, not a harness one. ***"
        echo "  At a boundary where A demonstrably became durable, B did not. modelSharedJournal=true"
        echo "  is wrong here, so ST8 and BatchedFlushSharedJournalDependencyTest encode a guarantee"
        echo "  this kernel does not give, the batched-flush optimisation is unsafe, and ~50 crash"
        echo "  classes are wrong in the same direction while staying green."
        ;;
    ST8_INDETERMINATE)
        echo "ST8_INDETERMINATE  boundary=$HIT_LABEL A=$HIT_A B=$HIT_B"
        echo "  B is neither fully durable nor fully lost. A partial conversion is a finding in its"
        echo "  own right and must not be rounded to either answer."
        ;;
    ST8_INSTRUMENT_FAILED)
        echo "ST8_INSTRUMENT_FAILED  no boundary in the scan made A durable"
        echo "  A was fdatasync'd, so this is the harness failing, not the model. B's state at these"
        echo "  boundaries says NOTHING about the shared journal -- note that 'B reads zeros' is also"
        echo "  the model-refuted signature, which is exactly why this check exists."
        if [ "$HAVE_TO_ENTRY" -eq 0 ]; then
            echo "  MOST LIKELY CAUSE HERE: replay-log.py has no --to-entry, so the scan could not"
            echo "  replay past the last FLUSH entry, and the jbd2 commit block is written after it."
        fi
        ;;
    ST8_CONTROL_OK)
        echo "ST8_CONTROL_OK  B was not durable at ANY boundary in the window"
        echo "  The probe's result is now falsifiable: without fdatasync(A), the sequence does"
        echo "  produce a lost B."
        ;;
    ST8_CONTROL_FAILED)
        echo "ST8_CONTROL_FAILED  B came back DURABLE with no fdatasync anywhere in the sequence"
        echo "  *** THE EXPERIMENT PROVES NOTHING. *** B's data is reaching durability by some other"
        echo "  route (writeback, a foreign commit, an un-excluded post-boundary write), so a durable"
        echo "  B in the probe run cannot be attributed to fdatasync(A). Fix this before reading any"
        echo "  probe result as evidence."
        ;;
esac

# BOTH DIRECTIONS OR NOTHING. A probe result with no control is an observation, not a
# measurement: 0xBB coming back could equally mean the data was durable for reasons unrelated
# to fdatasync(A). issues/08's first measurement fell into exactly that trap, and
# syncfs-microtest.sh carries a no-flush control line for the same reason.
echo
if [ -f "$COUNTERPART" ]; then
    CV=$(grep -m1 '^verdict=' "$COUNTERPART" | cut -d= -f2)
    CB=$(grep -m1 '^b=' "$COUNTERPART" | cut -d= -f2)
    echo "counterpart ($COUNTERPART_MODE): verdict=$CV b=$CB"
    if [ "$CV" = ST8_CONTROL_FAILED ] || { [ "${HIT_B:-}" = DURABLE ] && [ "$CB" = DURABLE ]; }; then
        echo "  *** THE CONTROL DID NOT LOSE B. THE EXPERIMENT PROVES NOTHING. ***"
        echo "  A durable B in the probe run is not evidence that fdatasync(A) did anything."
        VERDICT=ST8_NOT_FALSIFIABLE
        echo "$STAMP st8 mode=$MODE verdict=$VERDICT (control did not lose B)" >> "$LOG"
    fi
else
    echo "NO COUNTERPART RUN YET — this result is not yet falsifiable."
    echo "  run: bash $0 $([ "$MODE" = probe ] && echo --control)"
fi

echo
echo "result:  $RESULT"
echo "log:     $LOG"
if [ "${QDB_KEEP_RUN:-0}" != "1" ]; then
    rm -rf "$RUN"
else
    keep
fi

case "$VERDICT" in
    ST8_DURABLE|ST8_CONTROL_OK) exit 0 ;;
    *) exit 1 ;;
esac
