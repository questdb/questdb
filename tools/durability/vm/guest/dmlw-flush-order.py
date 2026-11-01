#!/usr/bin/env python3
"""
dm-log-writes: two flushes that overlap and complete out of order put an
older write after a newer write of the same sector in the log.

Run by test/t14-log-writes-order.sh against the stock module (must reorder)
and against the patched one from kernel/dm-log-writes-flush-order.patch
(must not). Uses whichever dm-log-writes module is loaded.

Stack:
    /dev/mapper/lwrepro = log-writes /dev/nbd0 LOGDEV

/dev/nbd0 is served by this script over two connections, so the nbd queue
has two hardware queues and two flushes can be in flight at once.  The
server holds the first flush until the second one has completed, which
makes the interleaving deterministic.

Sequence, all on block 0 of the data device:

    1. write "OLD"   O_DIRECT, completes           -> unflushed = [OLD]
    2. fsync #1      flush F1 issued, server holds  -> F1 takes [OLD]
    3. write "NEW"   O_DIRECT, completes           -> unflushed = [NEW]
    4. fsync #2      flush F2 issued, completes     -> F2 takes [NEW]
                     fsync #2 returns
    5. server releases F1, fsync #1 returns
    6. fsync #3      flush F3, nothing pending

The device received OLD before NEW, and both writes completed before a
flush that then completed, so the device holds NEW.  Replaying the log must
give NEW too.  dm-log-writes logs a flush's batch when that flush
completes, so it logs [NEW] at step 4 and [OLD] at step 5, and the replay
ends with OLD: a state the device never held.

Usage (as root, in a VM; LOGDEV is overwritten):
    dmlw-flush-order.py LOGDEV [ITERATIONS]
Exit status: 0 if the log replays to what the device holds, 1 if not.
"""
import fcntl
import mmap
import os
import socket
import struct
import subprocess
import sys
import threading
import time

NBD_SET_SOCK, NBD_SET_BLKSIZE, NBD_SET_SIZE, NBD_DO_IT = 0xAB00, 0xAB01, 0xAB02, 0xAB03
NBD_CLEAR_SOCK, NBD_DISCONNECT, NBD_SET_FLAGS = 0xAB04, 0xAB08, 0xAB0A
NBD_FLAG_HAS_FLAGS, NBD_FLAG_SEND_FLUSH, NBD_FLAG_CAN_MULTI_CONN = 1 << 0, 1 << 2, 1 << 8
NBD_REQUEST_MAGIC, NBD_REPLY_MAGIC = 0x25609513, 0x67446698
NBD_CMD_READ, NBD_CMD_WRITE, NBD_CMD_DISC, NBD_CMD_FLUSH = 0, 1, 2, 3
BLKFLSBUF = 0x1261

LOG_FLUSH, LOG_FUA, LOG_DISCARD, LOG_MARK, LOG_META = 1, 2, 4, 8, 16
WRITE_LOG_MAGIC = 0x6A736677736872

NBD = "nbd0"
DM = "lwrepro"
BS = 4096
SIZE = 64 << 20
OLD, NEW = b"OLD!" * (BS // 4), b"NEW!" * (BS // 4)


def recvall(s, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = s.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return bytes(buf)


class Server:
    """An in-memory nbd server. `store` is what the device holds once every
    flush has completed."""

    def __init__(self, nconn):
        self.store = bytearray(SIZE)
        self.lock = threading.Lock()
        self.pairs = [socket.socketpair() for _ in range(nconn)]
        self.send_locks = [threading.Lock() for _ in range(nconn)]
        self.hold_next_flush = False
        self.held = None
        self.held_event = threading.Event()
        self.threads = [threading.Thread(target=self.serve, args=(i,), daemon=True)
                        for i in range(nconn)]
        for t in self.threads:
            t.start()

    def reply(self, i, handle, data=b""):
        with self.send_locks[i]:
            self.pairs[i][1].sendall(struct.pack(">II8s", NBD_REPLY_MAGIC, 0, handle) + data)

    def serve(self, i):
        s = self.pairs[i][1]
        while True:
            hdr = recvall(s, 28)
            if hdr is None:
                return
            magic, typ, handle, off, ln = struct.unpack(">II8sQI", hdr)
            assert magic == NBD_REQUEST_MAGIC, hex(magic)
            cmd = typ & 0xFFFF
            if cmd == NBD_CMD_WRITE:
                data = recvall(s, ln)
                with self.lock:
                    self.store[off:off + ln] = data
                self.reply(i, handle)
            elif cmd == NBD_CMD_READ:
                with self.lock:
                    data = bytes(self.store[off:off + ln])
                self.reply(i, handle, data)
            elif cmd == NBD_CMD_FLUSH:
                with self.lock:
                    hold, self.hold_next_flush = self.hold_next_flush, False
                    if hold:
                        self.held = (i, handle)
                if hold:
                    self.held_event.set()  # answered later by release()
                else:
                    self.reply(i, handle)
            elif cmd == NBD_CMD_DISC:
                return
            else:
                self.reply(i, handle)

    def release(self):
        i, handle = self.held
        self.held = None
        self.reply(i, handle)


def sh(cmd, **kw):
    return subprocess.run(cmd, shell=True, check=True, text=True, capture_output=True, **kw).stdout


def cpu_list(q):
    with open(f"/sys/block/{NBD}/mq/{q}/cpu_list") as f:
        return [int(c) for c in f.read().replace(",", " ").split()]


def on_cpu(cpu, fn):
    """Run fn in a new thread pinned to cpu. Returns (thread, result dict)."""
    res = {}

    def run():
        os.sched_setaffinity(0, {cpu})
        try:
            res["value"] = fn()
        except BaseException as e:  # noqa: BLE001 - reported by the caller
            res["error"] = e

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t, res


def nbd_connect(srv):
    fd = os.open(f"/dev/{NBD}", os.O_RDWR)
    fcntl.ioctl(fd, NBD_CLEAR_SOCK)
    fcntl.ioctl(fd, NBD_SET_BLKSIZE, BS)
    fcntl.ioctl(fd, NBD_SET_SIZE, SIZE)
    fcntl.ioctl(fd, NBD_SET_FLAGS, NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH | NBD_FLAG_CAN_MULTI_CONN)
    for kernel_end, _ in srv.pairs:
        fcntl.ioctl(fd, NBD_SET_SOCK, kernel_end.fileno())
    doit = threading.Thread(target=lambda: fcntl.ioctl(fd, NBD_DO_IT), daemon=True)
    doit.start()
    for _ in range(200):
        if os.path.exists(f"/sys/block/{NBD}/pid"):
            return fd, doit
        time.sleep(0.01)
    raise RuntimeError("nbd device never started")


def nbd_disconnect(fd, doit):
    fcntl.ioctl(fd, NBD_DISCONNECT)
    doit.join(10)
    fcntl.ioctl(fd, NBD_CLEAR_SOCK)
    os.close(fd)


def read_log(logdev):
    fd = os.open(logdev, os.O_RDONLY)
    try:
        fcntl.ioctl(fd, BLKFLSBUF, 0)
        magic, version, nr, ss = struct.unpack("<QQQI", os.pread(fd, 28, 0))
        assert magic == WRITE_LOG_MAGIC, f"no dm-log-writes super on {logdev}"
        off, entries = ss, []
        for i in range(nr):
            hdr = os.pread(fd, ss, off)
            sector, nsec, flags, dlen = struct.unpack("<QQQQ", hdr[:32])
            mark = hdr[32:32 + dlen].decode() if flags & LOG_MARK else ""
            data = b""
            if nsec and not flags & (LOG_DISCARD | LOG_MARK):
                data = os.pread(fd, nsec * ss, off + ss)
            entries.append((i, sector, nsec, flags, mark, data))
            off += ss * (1 + (0 if flags & LOG_DISCARD else nsec))
        return ss, entries
    finally:
        os.close(fd)


def tag(block):
    return {OLD[:4]: "OLD", NEW[:4]: "NEW", bytes(4): "zero"}.get(bytes(block[:4]), block[:4].hex())


def describe(flags):
    names = [n for b, n in ((LOG_FLUSH, "FLUSH"), (LOG_FUA, "FUA"), (LOG_DISCARD, "DISCARD"),
                            (LOG_MARK, "MARK"), (LOG_META, "META")) if flags & b]
    return ",".join(names) or "WRITE"


def run_once(logdev, verbose):
    sh(f"dd if=/dev/zero of={logdev} bs=1M count=16 oflag=direct status=none")
    srv = Server(2)
    nbd_fd, doit = nbd_connect(srv)
    dm_up = False
    try:
        q0, q1 = cpu_list(0), cpu_list(1)
        wc = open(f"/sys/block/{NBD}/queue/write_cache").read().strip()
        assert wc == "write back", f"{NBD} write_cache is '{wc}': flushes would never reach the device"
        sh(f"echo '0 {SIZE // 512} log-writes /dev/{NBD} {logdev}' | dmsetup create {DM}")
        dm_up = True
        sh("udevadm settle")
        c0, c1 = q0[0], q1[0]
        os.sched_setaffinity(0, {c1})
        fd = os.open(f"/dev/mapper/{DM}", os.O_RDWR | os.O_DIRECT)
        buf = mmap.mmap(-1, BS)

        def write(content):
            buf.seek(0)
            buf.write(content)
            assert os.pwrite(fd, buf, 0) == BS

        write(OLD)                                            # 1
        srv.hold_next_flush = True
        t1, r1 = on_cpu(c0, lambda: os.fsync(fd))             # 2
        if not srv.held_event.wait(5):
            raise RuntimeError("flush F1 never reached the server")
        write(NEW)                                            # 3
        t2, r2 = on_cpu(c1, lambda: os.fsync(fd))             # 4
        t2.join(5)
        if t2.is_alive():
            srv.release()
            raise RuntimeError("fsync #2 did not complete while F1 was held: the two flushes "
                               f"were serialized (cpu {c0} and cpu {c1} on one queue?)")
        sh(f"dmsetup message {DM} 0 mark fsync2-returned")
        srv.release()                                         # 5
        t1.join(5)
        assert not t1.is_alive() and "error" not in r1 and "error" not in r2, (r1, r2)
        os.fsync(fd)                                          # 6
        sh(f"dmsetup message {DM} 0 mark all-flushed")
        os.close(fd)
        sh(f"udevadm settle; dmsetup remove --retry {DM}")
        dm_up = False
    finally:
        if dm_up:
            subprocess.run(f"udevadm settle; dmsetup remove --retry {DM}", shell=True)
        nbd_disconnect(nbd_fd, doit)

    device = tag(srv.store[:BS])
    ss, entries = read_log(logdev)
    replay = {}
    rows = []
    for i, sector, nsec, flags, mark, data in entries:
        if data:
            for k in range(nsec):
                replay[sector + k] = data[k * ss:(k + 1) * ss]
        what = f"block {sector}  {tag(data)}" if data else mark
        rows.append(f"  #{i:<2} {describe(flags):<6} {what:<24} replay -> block 0 = "
                    f"{tag(replay.get(0, bytes(BS)))}")
    replayed = tag(replay.get(0, bytes(BS)))
    if verbose:
        print(f"nbd queues: q0 cpus {q0}, q1 cpus {q1}; F1 issued on cpu {c0}, F2 on cpu {c1}")
        print(f"kernel {os.uname().release}, dm-log-writes srcversion "
              f"{open('/sys/module/dm_log_writes/srcversion').read().strip()}")
        print("log, in the order dm-log-writes recorded it:")
        print("\n".join(rows))
        print(f"device block 0, all flushes completed: {device}")
        print(f"log replayed to the end, block 0:      {replayed}")
    return device, replayed


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    logdev, n = sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 1
    sh("modprobe nbd nbds_max=4 max_part=0")
    sh("modprobe dm-log-writes")
    bad = 0
    for it in range(n):
        device, replayed = run_once(logdev, verbose=(it == 0))
        bad += device != replayed
    print(f"{bad} of {n} runs: log replays to {'OLD' if bad else 'the device content'}"
          f"{' while the device holds NEW -> REORDERED' if bad else ' -> consistent'}")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
