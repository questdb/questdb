#!/usr/bin/env python3
"""Write bytes and push them to the DEVICE without any flush.

Usage: probe-write.py <file> <byte> <length>

WHY NOT O_DIRECT
----------------
The obvious probe -- an in-place O_DIRECT overwrite -- does NOT produce an
unflushed device write on ext4. Measured, via dm-log-writes marks bracketing it:

    entry 651: MARK          BEFORE_ODIRECT
    entry 652: MARK          AFTER_ODIRECT
    entry 653: sector=274432 nr_sectors=8      <- the O_DIRECT write
    entry 654: sector=4368   nr_sectors=2048   <- 1MB at a low sector: the journal
    entry 655: sector=6416   nr_sectors=2040   <- journal
    entry 656: FLUSH                           <- journal commit

ext4 forces a transaction for the O_DIRECT path, and that commit carries a FLUSH
which covers the very write meant to stay unflushed. Mounting with commit=3600
does not help: the commit is not time-driven.

WHAT THIS DOES INSTEAD
----------------------
Ordinary buffered write into an ALREADY-ALLOCATED range (so no metadata changes
and no transaction), then sync_file_range(SYNC_FILE_RANGE_WRITE|WAIT_AFTER),
which initiates writeback and waits for it to reach the device -- and issues no
FLUSH and no journal activity. That is precisely "at the device, not flushed".

The file must be pre-allocated and fsync'd at this length beforehand, or the
write will extend it and pull in the metadata transaction this avoids.
"""

import ctypes
import ctypes.util
import os
import sys

SYNC_FILE_RANGE_WAIT_BEFORE = 1
SYNC_FILE_RANGE_WRITE = 2
SYNC_FILE_RANGE_WAIT_AFTER = 4


def main():
    if len(sys.argv) != 4:
        sys.exit(__doc__)
    path, byte, length = sys.argv[1], sys.argv[2].encode()[:1], int(sys.argv[3])
    if not byte:
        sys.exit("byte must be a single character")

    libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
    libc.sync_file_range.argtypes = [ctypes.c_int, ctypes.c_long,
                                     ctypes.c_long, ctypes.c_uint]
    libc.sync_file_range.restype = ctypes.c_int

    st = os.stat(path)
    if st.st_size < length:
        sys.exit(f"{path} is {st.st_size} bytes, need >= {length} pre-allocated; "
                 "extending it here would pull in a metadata transaction")

    fd = os.open(path, os.O_WRONLY)
    try:
        os.pwrite(fd, byte * length, 0)
        # WRITE starts writeback; WAIT_AFTER blocks until it has been issued to
        # the device. Neither implies a flush.
        rc = libc.sync_file_range(fd, 0, length,
                                  SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER)
        if rc != 0:
            err = ctypes.get_errno()
            sys.exit(f"sync_file_range failed: errno={err} ({os.strerror(err)})")
    finally:
        os.close(fd)

    print(f"wrote {length} bytes of {byte.decode()!r} to {path}, "
          "pushed to device via sync_file_range, NO flush")


if __name__ == "__main__":
    main()
