#!/usr/bin/env python3
"""Replay a dm-log-writes log onto a data device, up to a chosen flush boundary.

WHY THIS EXISTS
---------------
Killing the VMM cannot model a power cut below the hypervisor: unflushed writes
have already been handed to a host that is still powered, so they survive. Proven
by probe -- an O_DIRECT in-place overwrite with no flush reads back intact after
the cut. See the README's "no volatile device write cache" section.

dm-log-writes fixes that by recording every write AND every flush. Replaying the
log only as far as the Nth FLUSH reconstructs exactly the device state a volatile
cache would have left behind: everything before the flush is durable, everything
after it is gone.

It also turns crash points from SAMPLED into ENUMERABLE. One workload run yields
as many crash states as it had flushes, each independently verifiable -- the same
shape as the Java sweep's forEachAdaptiveCrashPoint, but on a real filesystem.

ON-DISK FORMAT (kernel drivers/md/dm-log-writes.c)
--------------------------------------------------
    superblock (1 block):  magic u64, version u64, nr_entries u64, sectorsize u32
    per entry:
        metadata block (1 block): sector u64, nr_sectors u64, flags u64,
                                  data_len u64, then inline data (MARK only)
        data (normal writes only): nr_sectors * 512 bytes, block-aligned

    flags: FLUSH 1<<0  FUA 1<<1  DISCARD 1<<2  MARK 1<<3  METADATA 1<<4

Usage:
    replay-log.py --log /dev/vdc --replay /dev/vdb --list
    replay-log.py --log /dev/vdc --replay /dev/vdb --to-flush N
    replay-log.py --log /dev/vdc --replay /dev/vdb --to-flush last
"""

import argparse
import os
import signal
import struct
import sys

# `--list` on a busy log prints thousands of lines and is routinely piped to
# `head`, which closes the pipe early. Default Python turns that into a noisy
# BrokenPipeError traceback that can bury a real error in the caller's output.
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

WRITE_LOG_MAGIC = 0x6A736677736872
WRITE_LOG_VERSION = 1

LOG_FLUSH_FLAG = 1 << 0
LOG_FUA_FLAG = 1 << 1
LOG_DISCARD_FLAG = 1 << 2
LOG_MARK_FLAG = 1 << 3
LOG_METADATA_FLAG = 1 << 4

SUPER_FMT = "<QQQI"
ENTRY_FMT = "<QQQQ"
ENTRY_LEN = struct.calcsize(ENTRY_FMT)
BIO_SECTOR = 512


def read_super(fd):
    raw = os.pread(fd, struct.calcsize(SUPER_FMT), 0)
    magic, version, nr_entries, sectorsize = struct.unpack(SUPER_FMT, raw)
    if magic != WRITE_LOG_MAGIC:
        sys.exit(f"not a dm-log-writes log: magic=0x{magic:x} "
                 f"(expected 0x{WRITE_LOG_MAGIC:x})")
    if version != WRITE_LOG_VERSION:
        sys.exit(f"unsupported log version {version}")
    if sectorsize == 0 or sectorsize % BIO_SECTOR:
        sys.exit(f"implausible sectorsize {sectorsize}")
    return nr_entries, sectorsize


def align_up(n, a):
    return (n + a - 1) // a * a


def scan(fd, nr_entries, sectorsize, log_bytes):
    """Walk the log. Yields (index, sector, nr_sectors, flags, data_off, data_len).

    DELIBERATELY DOES NOT TRUST nr_entries. The superblock's counter is updated
    lazily by the log kthread, so a crash -- precisely the case this tool exists
    for -- leaves it STALE. Trusting it silently truncated the tail of the log:
    a file that had been created and fsync'd went missing after replay, because
    its journal commit and the flush covering it were past the stale count.

    Instead, walk until an entry stops being plausible. A real crash tail is
    zeroes or garbage, which fails these checks; nr_entries is kept only as a
    sanity bound on how far past it we are willing to look.
    """
    off = sectorsize          # the superblock occupies the first block
    i = 0
    hard_cap = max(nr_entries * 4, nr_entries + 4096)
    while i < hard_cap and off + ENTRY_LEN <= log_bytes:
        raw = os.pread(fd, ENTRY_LEN, off)
        if len(raw) < ENTRY_LEN:
            break             # truncated log: stop cleanly rather than guess
        sector, nr_sectors, flags, data_len = struct.unpack(ENTRY_FMT, raw)

        # Plausibility gate marking the end of real log content.
        if flags == 0 and sector == 0 and nr_sectors == 0 and data_len == 0:
            break                       # zeroed tail
        if flags >> 5:
            break                       # undefined flag bits
        if data_len > sectorsize:
            break                       # inline data cannot exceed its block
        if nr_sectors > (1 << 32):
            break                       # implausible extent

        off += sectorsize     # metadata block, inline data included

        payload_off, payload_len = 0, 0
        if not (flags & LOG_MARK_FLAG) and not (flags & LOG_DISCARD_FLAG) and nr_sectors:
            payload_off = off
            payload_len = nr_sectors * BIO_SECTOR
            off += align_up(payload_len, sectorsize)

        yield i, sector, nr_sectors, flags, payload_off, payload_len
        i += 1


def describe(flags):
    names = []
    for bit, name in ((LOG_FLUSH_FLAG, "FLUSH"), (LOG_FUA_FLAG, "FUA"),
                      (LOG_DISCARD_FLAG, "DISCARD"), (LOG_MARK_FLAG, "MARK"),
                      (LOG_METADATA_FLAG, "META")):
        if flags & bit:
            names.append(name)
    return ",".join(names) if names else "-"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", required=True, help="the dm-log-writes LOG device")
    ap.add_argument("--replay", help="data device to reconstruct onto")
    ap.add_argument("--to-flush", default="last",
                    help="replay up to and including the Nth flush, or 'last'")
    ap.add_argument("--list", action="store_true", help="enumerate flush points and exit")
    ap.add_argument("--tail", type=int, default=0,
                    help="with --list, also dump the last N entries in order")
    args = ap.parse_args()

    logfd = os.open(args.log, os.O_RDONLY)
    nr_entries, sectorsize = read_super(logfd)
    log_bytes = os.lseek(logfd, 0, os.SEEK_END)

    entries = list(scan(logfd, nr_entries, sectorsize, log_bytes))
    flush_idx = [i for (i, _s, _n, f, _o, _l) in entries if f & LOG_FLUSH_FLAG]

    if args.list:
        print(f"log: superblock claims {nr_entries} entries, scan found {len(entries)}, "
              f"sectorsize={sectorsize}, {len(flush_idx)} flushes")
        if len(entries) > nr_entries:
            print(f"  NOTE: {len(entries) - nr_entries} entries past the superblock count "
                  f"— it was stale, as expected after a crash")
        # Cap the per-flush listing: a W=0 run records tens of thousands of
        # flushes and dumping them all is never what the caller wanted.
        show = flush_idx if len(flush_idx) <= 20 else flush_idx[:10] + flush_idx[-10:]
        for n, i in enumerate(flush_idx, 1):
            if i in show:
                print(f"  flush {n:5d} -> entry {i}")
            elif n == 11 and len(flush_idx) > 20:
                print(f"  ... {len(flush_idx) - 20} more flushes ...")
        if args.tail:
            print(f"  --- last {args.tail} entries ---")
            for i, sector, nr_sectors, flags, _o, dl in entries[-args.tail:]:
                print(f"  entry {i}: sector={sector} nr_sectors={nr_sectors} "
                      f"flags={describe(flags)} data_len={dl}")
        else:
            for i, sector, nr_sectors, flags, _o, dl in entries[:10]:
                print(f"  entry {i}: sector={sector} nr_sectors={nr_sectors} "
                      f"flags={describe(flags)} data_len={dl}")
        os.close(logfd)
        return

    if not args.replay:
        sys.exit("--replay is required unless --list is given")
    if not flush_idx:
        sys.exit("log contains no flushes; nothing is durable, refusing to replay")

    if args.to_flush == "last":
        stop_at = flush_idx[-1]
        which = len(flush_idx)
    else:
        n = int(args.to_flush)
        if n < 1 or n > len(flush_idx):
            sys.exit(f"--to-flush {n} out of range (log has {len(flush_idx)} flushes)")
        stop_at = flush_idx[n - 1]
        which = n

    # O_DIRECT deliberately avoided: correctness of the reconstruction matters
    # more than speed, and a final fsync makes it durable regardless.
    datafd = os.open(args.replay, os.O_WRONLY)
    applied = skipped = 0
    for i, sector, nr_sectors, flags, poff, plen in entries:
        if i > stop_at:
            skipped += 1
            continue
        if flags & (LOG_MARK_FLAG | LOG_DISCARD_FLAG) or plen == 0:
            continue
        data = os.pread(logfd, plen, poff)
        if len(data) != plen:
            sys.exit(f"log truncated inside entry {i}; refusing to write partial data")
        os.pwrite(datafd, data, sector * BIO_SECTOR)
        applied += 1

    os.fsync(datafd)
    os.close(datafd)
    os.close(logfd)
    print(f"replayed to flush {which}/{len(flush_idx)} (entry {stop_at}): "
          f"{applied} writes applied, {skipped} entries after the boundary discarded")


if __name__ == "__main__":
    main()
