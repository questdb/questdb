#!/usr/bin/env bash
# guest/st8-probe.sh [--control] [--verify] [--dir DIR] [--size N] [--dm NAME]
#
# ST8, AS A REAL SYSCALL SEQUENCE ON A REAL KERNEL.
#
# THE ONE QUESTION: does fdatasync(A) make file B's data durable, when B's extent
# conversion is still pending and B is never fsynced?
#
# The Java model says YES. CrashFaultFilesFacade.modelSharedJournal defaults to true, and
# CrashModelSelfCheckTest.test8_sharedJournalNewAllocationDurable asserts that a single
# filesystem-wide journal commit (A's fdatasync) also journals B's pending unwritten->written
# extent conversion, so B's at-device data becomes durable WITHOUT B's own fsync. ST7 is the
# same scenario with modelSharedJournal=false and asserts the opposite (B LOST).
#
# ~50 crash test classes and the batched-flush optimisation rest on that YES, and it has never
# been checked against a kernel. The model's ground truth is a reading of man pages.
#
# THE SEQUENCE, mirroring test8_sharedJournalNewAllocationDurable step for step:
#
#   ST8 (Java, via CrashFaultFilesFacade)          this probe (via libc, ctypes)
#   ------------------------------------------     --------------------------------------
#   openRW(a.d)                                    open(a.d, O_RDWR|O_CREAT)
#   ff.allocate(fd, SIZE)                          posix_fallocate(fd, 0, SIZE)
#   ff.mmap(fd, SIZE, 0, MAP_RW)                   mmap(NULL, SIZE, PROT_READ|PROT_WRITE,
#                                                       MAP_SHARED, fd, 0)
#   Unsafe.setMemory(addr, SIZE, 0xBB)             memset(addr, 0xBB, SIZE)
#   ... same for b.d ...                           ... same for b.d ...
#   ff.msync(addrA, SIZE, true)   [MS_ASYNC]       msync(addrA, SIZE, MS_ASYNC)
#   ff.msync(addrB, SIZE, true)   [MS_ASYNC]       msync(addrB, SIZE, MS_ASYNC)
#   ff.syncFileRange(fdA, 0, SIZE, WRITE|WAIT_AFTER)   sync_file_range(fdA, 0, SIZE, same)
#   ff.syncFileRange(fdB, 0, SIZE, WRITE|WAIT_AFTER)   sync_file_range(fdB, 0, SIZE, same)
#   ff.fdatasync(fdA)   -- A ONLY                  fdatasync(fdA)   -- A ONLY
#   munmap+close both                              munmap+close both
#   ff.crash(dir)                                  the driver kills the VMM and replays
#
# ff.allocate is posix_fallocate: core/src/main/c/linux/files.c,
# Java_io_questdb_std_Files_allocate calls posix_fallocate(fd, 0, len). Checked, not assumed.
#
# WHY libc/ctypes AND NOT xfs_io
# ------------------------------
# xfs_io CAN express every individual step (falloc / mmap / mwrite / msync -a / sync_range -w -a
# / fdatasync). What it cannot express CLEANLY is the thing ST8 is actually about: the
# INTERLEAVING of two files. ST8 requires both files mapped and written, then msync on BOTH,
# then sync_file_range on BOTH, and only then a single fdatasync aimed at A. In xfs_io that
# means juggling its "current file" (the `file N` command) and its "current mapping" (`mmap N`)
# across two open files, and the mapping-vs-file coupling is exactly the part that is easy to
# get subtly wrong and impossible to see afterwards -- the run would still print a verdict.
#
# This ticket exists because a plausible-looking substitute was trusted once before, so the
# rule is: a probe that runs a DIFFERENT sequence answers a DIFFERENT question. ctypes makes
# the syscall and its flags literal and auditable on the page -- msync(addr, len, MS_ASYNC=1)
# is either there or it is not -- and it needs no assumption about a tool's internal state
# machine. guest/probe-write.py already established this idiom in this harness, and for the
# same reason: it documents an xfs_io/O_DIRECT approach that was MEASURED to do something
# other than what it appeared to do.
#
# Both routes need the same thing to be true; only one of them can be checked by reading it.
#
# WHAT THIS SCRIPT DOES **NOT** DO: it does not cut the machine and it does not judge. The
# host driver (run-st8-probe.sh) records under dm-log-writes, replays to the boundary created
# by A's fdatasync, and reads the result back through --verify below.
set -euo pipefail

MODE=probe                       # probe | control | verify
DIR="${QDB_ST8_DIR:-/mnt/qdb/st8}"
SIZE="${QDB_ST8_SIZE:-65536}"
DM="${QDB_ST8_DM:-qdbdata}"

for a in "$@"; do
    case "$a" in
        --control) MODE=control ;;
        --verify)  MODE=verify ;;
        --dir=*)   DIR="${a#*=}" ;;
        --size=*)  SIZE="${a#*=}" ;;
        --dm=*)    DM="${a#*=}" ;;
        *) echo "st8-probe: unknown argument $a" >&2; exit 64 ;;
    esac
done

case "$SIZE" in
    ''|*[!0-9]*) echo "st8-probe: --size must be a positive integer (got '$SIZE')" >&2; exit 64 ;;
esac
[ "$SIZE" -gt 0 ] || { echo "st8-probe: --size must be > 0" >&2; exit 64; }

# ST8 uses SIZE=256 and states that a sub-page region is fine, because size is not the
# variable under test. 64 KiB is the default here so the read-back spans 16 filesystem
# blocks: a PARTIAL conversion then shows up as a partial result instead of being rounded
# to one of the two answers. --size=256 reproduces the Java test's exact extent.

# The whole probe runs as root: dm-log-writes marks are inserted with `dmsetup message`, and
# the marks have to be issued from INSIDE the sequence (between sync_file_range and fdatasync),
# while both files are still open and mapped. sudo, not a root check, so this matches how
# prepare-device.sh reaches dmsetup.
SUDO=""
[ "$(id -u)" -eq 0 ] || SUDO="sudo"

# ---------------------------------------------------------------------------------------
# The syscall sequence. Arguments: DIR SIZE DM MODE
#
# Every libc call below is checked; a silently-failing msync or sync_file_range would leave
# the probe measuring a sequence it did not run, which is the one failure mode this whole
# ticket is about. Failures abort rather than degrade.
# ---------------------------------------------------------------------------------------
run_sequence() {
    $SUDO python3 - "$DIR" "$SIZE" "$DM" "$MODE" <<'PY'
import ctypes
import ctypes.util
import os
import subprocess
import sys

directory, size, dm, mode = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]

PATTERN = 0xBB          # ST8's NEW marker: "a fresh write we are testing for survival"

MS_ASYNC = 1
SYNC_FILE_RANGE_WRITE = 2
SYNC_FILE_RANGE_WAIT_AFTER = 4
PROT_READ = 1
PROT_WRITE = 2
MAP_SHARED = 1
MAP_FAILED = ctypes.c_void_p(-1).value

libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)

libc.posix_fallocate.argtypes = [ctypes.c_int, ctypes.c_long, ctypes.c_long]
libc.posix_fallocate.restype = ctypes.c_int
libc.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int,
                      ctypes.c_int, ctypes.c_int, ctypes.c_long]
libc.mmap.restype = ctypes.c_void_p
libc.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
libc.munmap.restype = ctypes.c_int
libc.msync.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int]
libc.msync.restype = ctypes.c_int
libc.sync_file_range.argtypes = [ctypes.c_int, ctypes.c_long, ctypes.c_long, ctypes.c_uint]
libc.sync_file_range.restype = ctypes.c_int
libc.fdatasync.argtypes = [ctypes.c_int]
libc.fdatasync.restype = ctypes.c_int


def die(what):
    err = ctypes.get_errno()
    sys.exit("st8-probe: %s failed: errno=%d (%s)" % (what, err, os.strerror(err)))


def mark(name):
    """Insert a dm-log-writes MARK, so the driver can find A's fdatasync in the recording.

    WHY MARKS AND NOT "the last flush in the log". The cut has to be interpreted against a
    boundary, and 'last flush' is the wrong one: jbd2's periodic commit (5 s) or any writeback
    that happens between the sequence ending and the VMM dying would append a LATER flush. If
    that later commit journals B's extent conversion, replaying to it reports B durable -- a
    FALSE CONFIRMATION of exactly the belief under test, produced by a journal commit that had
    nothing to do with A's fdatasync.

    Bracketing the fdatasync with marks lets the driver take the last flush strictly inside the
    bracket, which is A's commit and nothing after it. probe-write.py's docstring shows the
    same technique already being used in this harness to attribute flushes to their cause.
    """
    subprocess.run(["dmsetup", "message", dm, "0", "mark", name], check=True)


class Mapped(object):
    def __init__(self, path, size):
        # openRW: ST8's ff.openRW(path, O_NONE)
        self.fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
        # ff.allocate(fd, SIZE) -> posix_fallocate(fd, 0, SIZE). This is what makes the
        # extent UNWRITTEN, which is the precondition the whole scenario rests on: the
        # unwritten->written conversion is metadata, and metadata needs a journal commit.
        rc = libc.posix_fallocate(self.fd, 0, size)
        if rc != 0:
            sys.exit("st8-probe: posix_fallocate(%s) failed: rc=%d (%s)"
                     % (path, rc, os.strerror(rc)))
        self.addr = libc.mmap(None, size, PROT_READ | PROT_WRITE, MAP_SHARED, self.fd, 0)
        if self.addr == MAP_FAILED or self.addr is None:
            die("mmap(%s)" % path)
        self.size = size
        self.path = path

    def fill(self):
        # Unsafe.setMemory(addr, SIZE, NEW): dirty the pages through the shared mapping,
        # exactly as the model does. A write(2) here would take a different kernel path.
        #
        # SEPARATE FROM THE CONSTRUCTOR ON PURPOSE. The setup below must be made durable
        # BEFORE a single 0xBB byte exists, and a durability step that ran after the fill
        # would commit the very thing under test.
        ctypes.memset(ctypes.c_void_p(self.addr), PATTERN, self.size)


os.makedirs(directory, exist_ok=True)
a = Mapped(os.path.join(directory, "a.d"), size)
b = Mapped(os.path.join(directory, "b.d"), size)

# ---- MAKE THE SETUP DURABLE, BEFORE ANY DATA EXISTS -------------------------------------
#
# WITHOUT THIS THE EXPERIMENT CANNOT RUN AT ALL, and it fails in a way that looks like a
# result. Measured on the first real run: replaying to A's fdatasync boundary produced
#
#     st8-verify: a.d ABSENT (file does not exist after replay)
#     st8-verify: b.d ABSENT (file does not exist after replay)
#
# The DIRECTORY ENTRIES had never been committed. `st8/`, `a.d` and `b.d` were created in a
# transaction that nothing forced to the device, so after the cut the filesystem had no such
# path -- and fdatasync(A) says nothing about a filename. The instrument check caught it
# (ST8_INSTRUMENT_FAILED: A was fdatasync'd and did not come back), which is the only reason
# this was not read as "B lost, model refuted".
#
# The Java model never meets this: CrashFaultFilesFacade.crash() drops unflushed CONTENT from
# a world where the files already exist. On a real kernel, existence is itself a journalled
# fact that has to be established first.
#
# So: commit the names, the inodes and the UNWRITTEN extents now, while the files are still
# empty. After this point the only thing not on the device is the 0xBB data and the
# unwritten->written conversion it requires -- which is exactly, and only, what ST8 asks about.
for m in (a, b):
    os.fsync(m.fd)
_dirfd = os.open(directory, os.O_RDONLY)
os.fsync(_dirfd)          # the directory entries: without this, a.d and b.d have no names
os.close(_dirfd)
os.sync()                 # and the mount's own metadata, so nothing from setup is left pending

# Only NOW does the data under test come into existence.
a.fill()
b.fill()

# msync(MS_ASYNC) on BOTH: starts writeback of the dirty mapped pages without waiting and
# without any journal activity. ST8 does this for A then B, in that order.
if libc.msync(ctypes.c_void_p(a.addr), size, MS_ASYNC) != 0:
    die("msync(a.d, MS_ASYNC)")
if libc.msync(ctypes.c_void_p(b.addr), size, MS_ASYNC) != 0:
    die("msync(b.d, MS_ASYNC)")

# sync_file_range(WRITE|WAIT_AFTER) on BOTH: pushes the DATA BLOCKS to the device and waits
# for them to be issued. It journals nothing and flushes nothing -- so after this line both
# files' bytes are at the device while both files' extent conversions are still pending.
# That is the precise state ST8 crashes from.
flags = SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER
if libc.sync_file_range(a.fd, 0, size, flags) != 0:
    die("sync_file_range(a.d)")
if libc.sync_file_range(b.fd, 0, size, flags) != 0:
    die("sync_file_range(b.d)")

# The bracket. In CONTROL mode the marks are still emitted, in the same place, with nothing
# between them -- so the driver applies an IDENTICAL boundary rule to both runs and the only
# difference between probe and control is the fdatasync itself.
mark("st8-pre-fdatasync")
if mode == "control":
    print("st8-probe: CONTROL -- fdatasync(a.d) DELIBERATELY OMITTED")
else:
    if libc.fdatasync(a.fd) != 0:
        die("fdatasync(a.d)")
    print("st8-probe: fdatasync(a.d) issued -- A only, B never fsynced")
mark("st8-post-fdatasync")

# ST8 unmaps and closes BOTH files after the fdatasync and before the crash. Neither munmap
# nor close forces writeback or a journal commit on ext4, so this does not disturb the state
# under test -- but the order is kept identical anyway, because "probably equivalent" is how
# a probe drifts into answering a different question.
for m in (a, b):
    if libc.munmap(ctypes.c_void_p(m.addr), m.size) != 0:
        die("munmap(%s)" % m.path)
    os.close(m.fd)

print("st8-probe: sequence complete mode=%s size=%d dir=%s" % (mode, size, directory))
print("st8-probe: a.d and b.d are at the device; only a.d was ever fdatasync'd")
PY
}

# ---------------------------------------------------------------------------------------
# Read-back. Deliberately lives in the SAME file as the writer so the byte pattern and the
# size have exactly one definition: a reader that disagreed with the writer about either
# would report LOST for a perfectly durable file, or DURABLE for a zeroed one.
# ---------------------------------------------------------------------------------------
verify() {
    $SUDO python3 - "$DIR" "$SIZE" <<'PY'
import os
import sys

directory, size = sys.argv[1], int(sys.argv[2])
PATTERN = 0xBB


def classify(path):
    """ABSENT / LOST / DURABLE / PARTIAL, plus the evidence for the call.

    LOST is specifically 'reads as zeros': when the extent conversion is not journaled, the
    extent reverts to UNWRITTEN and ext4 returns zeros regardless of what bytes physically sit
    in those blocks -- which is the mechanism that makes this test readable at all, since the
    replayed device still carries the data blocks either way.

    PARTIAL is neither answer and must never be rounded to one. It would mean some blocks
    converted and others did not, which is a finding in its own right.
    """
    if not os.path.exists(path):
        return "ABSENT", "file does not exist after replay"
    st = os.stat(path)
    if st.st_size < size:
        return "PARTIAL", "size=%d expected>=%d" % (st.st_size, size)
    with open(path, "rb") as f:
        data = f.read(size)
    if len(data) < size:
        return "PARTIAL", "read %d of %d bytes" % (len(data), size)
    pattern_bytes = sum(1 for c in data if c == PATTERN)
    zero_bytes = sum(1 for c in data if c == 0)
    if pattern_bytes == size:
        return "DURABLE", "all %d bytes are 0x%02x" % (size, PATTERN)
    if zero_bytes == size:
        return "LOST", "all %d bytes are 0x00 (extent reads unwritten)" % size
    return "PARTIAL", ("pattern=%d zero=%d other=%d of %d"
                       % (pattern_bytes, zero_bytes, size - pattern_bytes - zero_bytes, size))


results = {}
for name in ("a", "b"):
    verdict, why = classify(os.path.join(directory, name + ".d"))
    results[name] = verdict
    print("st8-verify: %s.d %s (%s)" % (name, verdict, why))

# One machine-readable line for the driver. Anchored with a fixed prefix so it cannot be
# confused with the human-readable lines above -- the same reason CrashVerifier was moved
# off the shared stdout after a spliced line corrupted a verdict.
print("ST8_READBACK a=%s b=%s" % (results["a"], results["b"]))
PY
}

case "$MODE" in
    verify)         verify ;;
    probe|control)  run_sequence ;;
esac
