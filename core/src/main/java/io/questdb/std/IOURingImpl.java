/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.std;

import io.questdb.cairo.CairoException;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.std.Files.toOsFd;
import static io.questdb.std.IOUringAccessor.*;

public class IOURingImpl implements IOURing {

    // Holds <id, res> tuples for recently consumed cqes.
    private final long[] cachedCqes;
    private final long cqKheadAddr;
    private final int cqKringMask;
    private final long cqKtailAddr;
    private final long cqesAddr;
    private final IOURingFacade facade;
    private final long ringAddr;
    private final long ringFd;
    private final long sqKheadAddr;
    private final int sqKringEntries;
    private final int sqKringMask;
    private final long sqesAddr;
    // Index of cached cqe tuple.
    private int cachedIndex;
    // Count of cached cqe tuples.
    private int cachedSize;
    private boolean closed = false;
    private long idSeq;

    public IOURingImpl(IOURingFacade facade, int capacity) {
        assert Numbers.isPow2(capacity);
        this.facade = facade;
        final long res = facade.create(capacity);
        if (res < 0) {
            throw CairoException.critical((int) -res).put("Cannot create io_uring instance");
        }
        this.ringAddr = res;

        int ringFd = Unsafe.getUnsafe().getInt(ringAddr + RING_FD_OFFSET);

        this.sqesAddr = Unsafe.getUnsafe().getLong(ringAddr + SQ_SQES_OFFSET);
        this.sqKheadAddr = Unsafe.getUnsafe().getLong(ringAddr + SQ_KHEAD_OFFSET);
        final long sqMaskAddr = Unsafe.getUnsafe().getLong(ringAddr + SQ_KRING_MASK_OFFSET);
        this.sqKringMask = Unsafe.getUnsafe().getInt(sqMaskAddr);
        final long sqEntriesAddr = Unsafe.getUnsafe().getLong(ringAddr + SQ_KRING_ENTRIES_OFFSET);
        this.sqKringEntries = Unsafe.getUnsafe().getInt(sqEntriesAddr);

        this.cqesAddr = Unsafe.getUnsafe().getLong(ringAddr + CQ_CQES_OFFSET);
        this.cqKheadAddr = Unsafe.getUnsafe().getLong(ringAddr + CQ_KHEAD_OFFSET);
        this.cqKtailAddr = Unsafe.getUnsafe().getLong(ringAddr + CQ_KTAIL_OFFSET);
        final long cqMaskAddr = Unsafe.getUnsafe().getLong(ringAddr + CQ_KRING_MASK_OFFSET);
        this.cqKringMask = Unsafe.getUnsafe().getInt(cqMaskAddr);
        final long cqEntriesAddr = Unsafe.getUnsafe().getLong(ringAddr + CQ_KRING_ENTRIES_OFFSET);
        int cqKringEntries = Unsafe.getUnsafe().getInt(cqEntriesAddr);
        cachedCqes = new long[2 * cqKringEntries];

        this.ringFd = Files.createUniqueFd(ringFd);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        Files.detach(ringFd);
        facade.close(ringAddr);
        closed = true;
    }

    @Override
    public long enqueueFsync(long fd) {
        return enqueueSqe(IORING_OP_FSYNC, fd, 0, 0, 0);
    }

    @Override
    public void enqueueFsync(long fd, long userData) {
        enqueueSqeWithUserData(IORING_OP_FSYNC, fd, 0, 0, 0, userData);
    }

    @Override
    @TestOnly
    public long enqueueNop() {
        return enqueueSqe(IORING_OP_NOP, -1, 0, 0, 0);
    }

    @Override
    public long enqueueRead(long fd, long offset, long bufAddr, int len) {
        return enqueueSqe(IORING_OP_READ, fd, offset, bufAddr, len);
    }

    @Override
    public long enqueueWrite(long fd, long offset, long bufAddr, int len) {
        return enqueueSqe(IORING_OP_WRITE, fd, offset, bufAddr, len);
    }

    @Override
    public void enqueueWrite(long fd, long offset, long bufAddr, int len, long userData) {
        enqueueSqeWithUserData(IORING_OP_WRITE, fd, offset, bufAddr, len, userData);
    }

    @Override
    public void enqueueWriteFixed(long fd, long offset, long bufAddr, int len, int bufIndex, long userData) {
        final long sqeAddr = nextSqe();
        if (sqeAddr == 0) {
            throw CairoException.critical(0).put("io_uring SQ full");
        }
        fillSqe(sqeAddr, IORING_OP_WRITE_FIXED, fd, offset, bufAddr, len, userData);
        Unsafe.getUnsafe().putShort(sqeAddr + SQE_BUF_INDEX_OFFSET, (short) bufIndex);
    }

    @Override
    public long getCqeId() {
        if (cachedIndex < cachedSize) {
            return cachedCqes[2 * cachedIndex];
        }
        return -1;
    }

    @Override
    public int getCqeRes() {
        if (cachedIndex < cachedSize) {
            return (int) cachedCqes[2 * cachedIndex + 1];
        }
        return -1;
    }

    @Override
    public int registerBuffers(long iovsAddr, int count) {
        return facade.registerBuffers(ringAddr, iovsAddr, count);
    }

    @Override
    public boolean nextCqe() {
        if (++cachedIndex < cachedSize) {
            return true;
        }
        // Consume all available cqes and store them in the cache.
        final int tail = Unsafe.getUnsafe().getInt(cqKtailAddr);
        Unsafe.getUnsafe().loadFence();
        final int head = Unsafe.getUnsafe().getInt(cqKheadAddr);
        if (tail == head) {
            return false;
        }
        for (int i = head; i < tail; i++) {
            final long cqeAddr = cqesAddr + (long) (i & cqKringMask) * SIZEOF_CQE;
            cachedCqes[2 * (i - head)] = Unsafe.getUnsafe().getLong(cqeAddr + CQE_USER_DATA_OFFSET);
            cachedCqes[2 * (i - head) + 1] = Unsafe.getUnsafe().getInt(cqeAddr + CQE_RES_OFFSET);
        }
        cachedSize = tail - head;
        cachedIndex = 0;
        Unsafe.getUnsafe().putInt(cqKheadAddr, tail);
        Unsafe.getUnsafe().storeFence();
        return true;
    }

    @Override
    public int submit() {
        return facade.submit(ringAddr);
    }

    @Override
    public int submitAndWait() {
        return facade.submitAndWait(ringAddr, 1);
    }

    @Override
    public int submitAndWait(int waitNr) {
        return facade.submitAndWait(ringAddr, waitNr);
    }

    @Override
    public int unregisterBuffers() {
        return facade.unregisterBuffers(ringAddr);
    }

    private long enqueueSqe(byte op, long fd, long offset, long bufAddr, int len) {
        final long sqeAddr = nextSqe();
        if (sqeAddr == 0) {
            return -1;
        }
        final long id = idSeq++;
        fillSqe(sqeAddr, op, fd, offset, bufAddr, len, id);
        return id;
    }

    private void enqueueSqeWithUserData(byte op, long fd, long offset, long bufAddr, int len, long userData) {
        final long sqeAddr = nextSqe();
        if (sqeAddr == 0) {
            throw CairoException.critical(0).put("io_uring SQ full");
        }
        fillSqe(sqeAddr, op, fd, offset, bufAddr, len, userData);
    }

    private static void fillSqe(long sqeAddr, byte op, long fd, long offset, long bufAddr, int len, long userData) {
        // Zero entire SQE to prevent stale flags/ioprio/buf_index/personality/rw_flags
        // from corrupting the operation. Matches liburing's io_uring_prep_* behavior.
        Unsafe.getUnsafe().setMemory(sqeAddr, SIZEOF_SQE, (byte) 0);
        Unsafe.getUnsafe().putByte(sqeAddr + SQE_OPCODE_OFFSET, op);
        Unsafe.getUnsafe().putInt(sqeAddr + SQE_FD_OFFSET, toOsFd(fd));
        Unsafe.getUnsafe().putLong(sqeAddr + SQE_OFF_OFFSET, offset);
        Unsafe.getUnsafe().putLong(sqeAddr + SQE_ADDR_OFFSET, bufAddr);
        Unsafe.getUnsafe().putInt(sqeAddr + SQE_LEN_OFFSET, len);
        Unsafe.getUnsafe().putLong(sqeAddr + SQE_USER_DATA_OFFSET, userData);
    }

    /**
     * Returns a pointer to sqe to fill. If there are sqes no available, returns 0.
     */
    private long nextSqe() {
        final int head = Unsafe.getUnsafe().getInt(sqKheadAddr);
        Unsafe.getUnsafe().loadFence();
        final int tail = Unsafe.getUnsafe().getInt(ringAddr + SQ_SQE_TAIL_OFFSET);
        if (tail - head < sqKringEntries) {
            final long addr = sqesAddr + (long) (tail & sqKringMask) * SIZEOF_SQE;
            Unsafe.getUnsafe().putInt(ringAddr + SQ_SQE_TAIL_OFFSET, tail + 1);
            return addr;
        }
        return 0;
    }

}
