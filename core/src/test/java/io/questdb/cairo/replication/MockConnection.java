package io.questdb.cairo.replication;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.Unsafe;

class MockConnection implements Closeable {
    private static final AtomicLong NEXT_FD = new AtomicLong(1_000_000);
    private static final LongObjHashMap<MockConnection> MOCK_CONNECTION_BY_FD = new LongObjHashMap<>();
    private static final int BUF_SZ = 2000;
    final long acceptorFd;
    final long connectorFd;
    private long buf1;
    private long bufLen1;
    private long bufSz1;
    private final ReentrantLock buf1Lock = new ReentrantLock();
    private long buf2;
    private long bufSz2;
    private long bufLen2;
    private final ReentrantLock buf2Lock = new ReentrantLock();
    private long nBytesSent; // From the perspective of the receiver
    private long nBytesReceived; // From the perspective of the receiver

    public static FilesFacade FILES_FACADE_INSTANCE = new FilesFacadeImpl() {
        @Override
        public long read(long fd, long buf, long len, long offset) {
            MockConnection conn = MOCK_CONNECTION_BY_FD.get((int) fd);
            if (null == conn) {
                return super.read(fd, buf, len, offset);
            }
            return conn.read(fd, buf, len, offset);
        }

        @Override
        public long write(long fd, long address, long len, long offset) {
            MockConnection conn = MOCK_CONNECTION_BY_FD.get((int) fd);
            if (null == conn) {
                return super.write(fd, address, len, offset);
            }
            return conn.write(fd, address, len, offset);
        }
    };

    MockConnection() {
        acceptorFd = NEXT_FD.incrementAndGet();
        connectorFd = NEXT_FD.incrementAndGet();
        bufSz1 = BUF_SZ;
        bufLen1 = 0;
        buf1 = Unsafe.malloc(bufSz1);
        bufSz2 = BUF_SZ;
        buf2 = Unsafe.malloc(bufSz2);
        bufLen2 = 0;
        synchronized (MOCK_CONNECTION_BY_FD) {
            MOCK_CONNECTION_BY_FD.put(acceptorFd, this);
            MOCK_CONNECTION_BY_FD.put(connectorFd, this);
        }
    }

    long read(long fd, long buf, long len, long offset) {
        if (fd == connectorFd) {
            buf1Lock.lock();
            try {
                long tranSz = bufLen1 < len ? bufLen1 : len;
                Unsafe.getUnsafe().copyMemory(null, buf1, null, buf + offset, tranSz);
                bufLen1 -= tranSz;
                if (bufLen1 > 0) {
                    Unsafe.getUnsafe().copyMemory(null, buf1 + tranSz, null, buf1, bufLen1);
                }
                return tranSz;
            } finally {
                buf1Lock.unlock();
            }
        } else if (fd == acceptorFd) {
            buf2Lock.lock();
            try {
                long tranSz = bufLen2 < len ? bufLen2 : len;
                Unsafe.getUnsafe().copyMemory(null, buf2, null, buf + offset, tranSz);
                bufLen2 -= tranSz;
                if (bufLen2 > 0) {
                    Unsafe.getUnsafe().copyMemory(null, buf2 + tranSz, null, buf2, bufLen2);
                }
                nBytesReceived += tranSz;
                return tranSz;
            } finally {
                buf2Lock.unlock();
            }
        } else {
            throw new IllegalArgumentException("fd=" + fd);
        }
    }

    long write(long fd, long buf, long len, long offset) {
        if (fd == acceptorFd) {
            buf1Lock.lock();
            try {
                long tranSz = bufSz1 - bufLen1;
                if (tranSz > len) {
                    tranSz = len;
                }
                Unsafe.getUnsafe().copyMemory(null, buf + offset, null, buf1 + bufLen1, tranSz);
                bufLen1 += tranSz;
                nBytesSent += tranSz;
                return tranSz;
            } finally {
                buf1Lock.unlock();
            }
        } else if (fd == connectorFd) {
            buf2Lock.lock();
            try {
                long tranSz = bufSz2 - bufLen2;
                if (tranSz > len) {
                    tranSz = len;
                }
                Unsafe.getUnsafe().copyMemory(null, buf + offset, null, buf2 + bufLen2, tranSz);
                bufLen2 += tranSz;
                return tranSz;
            } finally {
                buf2Lock.unlock();
            }
        } else {
            throw new IllegalArgumentException("fd=" + fd);
        }
    }

    void reset() {
        nBytesSent = 0;
        nBytesReceived = 0;
    }

    long getnBytesSent() {
        return nBytesSent;
    }

    long getnBytesReceived() {
        return nBytesReceived;
    }

    boolean isIdle() {
        return bufLen1 == 0 && bufLen2 == 0;
    }

    @Override
    public void close() {
        Unsafe.free(buf1, bufSz1);
        Unsafe.free(buf2, bufSz2);
    }
}