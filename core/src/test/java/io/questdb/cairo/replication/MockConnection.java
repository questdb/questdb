package io.questdb.cairo.replication;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.Unsafe;

class MockConnection implements Closeable {
    private static final AtomicLong NEXT_FD = new AtomicLong(1_000_000);
    private static final LongObjHashMap<MockConnection> MOCK_CONNECTION_BY_FD = new LongObjHashMap<>();
    private static final int BUF_SZ = 2000;
    final long acceptorFd;
    final long connectorFd;
    boolean closed;
    private long buf1;
    private int bufLen1;
    private int bufSz1;
    private final ReentrantLock buf1Lock = new ReentrantLock();
    private long buf2;
    private int bufSz2;
    private int bufLen2;
    private final ReentrantLock buf2Lock = new ReentrantLock();
    private long nBytesSent; // From the perspective of the receiver
    private long nBytesReceived; // From the perspective of the receiver

    public static NetworkFacade NETWORK_FACADE_INSTANCE = new NetworkFacadeImpl() {
        @Override
        public int recv(long fd, long buf, int len) {
            MockConnection conn = MOCK_CONNECTION_BY_FD.get((int) fd);
            if (null == conn) {
                return super.recv(fd, buf, len);
            }
            return conn.recv(fd, buf, len);
        }

        @Override
        public int send(long fd, long address, int len) {
            MockConnection conn = MOCK_CONNECTION_BY_FD.get((int) fd);
            if (null == conn) {
                return super.send(fd, address, len);
            }
            return conn.send(fd, address, len);
        }
    };

    MockConnection() {
        acceptorFd = NEXT_FD.incrementAndGet();
        connectorFd = NEXT_FD.incrementAndGet();
        closed = false;
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

    int recv(long fd, long buf, int len) {
        if (closed) {
            return -1;
        }

        if (fd == connectorFd) {
            buf1Lock.lock();
            try {
                int tranSz = bufLen1 < len ? bufLen1 : len;
                Unsafe.getUnsafe().copyMemory(null, buf1, null, buf, tranSz);
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
                int tranSz = bufLen2 < len ? bufLen2 : len;
                Unsafe.getUnsafe().copyMemory(null, buf2, null, buf, tranSz);
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

    int send(long fd, long buf, int len) {
        if (closed) {
            return -1;
        }

        if (fd == acceptorFd) {
            buf1Lock.lock();
            try {
                int tranSz = bufSz1 - bufLen1;
                if (tranSz > len) {
                    tranSz = len;
                }
                Unsafe.getUnsafe().copyMemory(null, buf, null, buf1 + bufLen1, tranSz);
                bufLen1 += tranSz;
                nBytesSent += tranSz;
                return tranSz;
            } finally {
                buf1Lock.unlock();
            }
        } else if (fd == connectorFd) {
            buf2Lock.lock();
            try {
                int tranSz = bufSz2 - bufLen2;
                if (tranSz > len) {
                    tranSz = len;
                }
                Unsafe.getUnsafe().copyMemory(null, buf, null, buf2 + bufLen2, tranSz);
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
        if (!closed) {
            closed = true;
            Unsafe.free(buf1, bufSz1);
            Unsafe.free(buf2, bufSz2);
        }
    }
}