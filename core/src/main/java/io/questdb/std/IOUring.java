/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import java.io.Closeable;

import static io.questdb.std.IOUringAccessor.*;

public final class IOUring implements Closeable {

    private final long ringPtr;
    private final int ringFd;

    private final long sqesPtr;
    private final long sqKheadPtr;
    private final int sqKringMask;
    private final int sqKringEntries;

    private final long cqesPtr;
    private final long cqKheadPtr;
    private final long cqKtailPtr;
    private final int cqKringMask;

    private long lastCqeId = -1;
    private int lastCqeRes;
    private long idSeq;
    private final IOUringFacade facade;
    private boolean closed = false;

    public IOUring(IOUringFacade facade, int capacity) {
        assert Numbers.isPow2(capacity);
        this.facade = facade;
        final long res = facade.create(capacity);
        if (res < 0) {
            throw CairoException.instance((int) -res).put("Cannot create io_uring instance");
        }
        this.ringPtr = res;

        this.ringFd = Unsafe.getUnsafe().getInt(ringPtr + RING_FD_OFFSET);

        this.sqesPtr = Unsafe.getUnsafe().getLong(ringPtr + SQ_SQES_OFFSET);
        this.sqKheadPtr = Unsafe.getUnsafe().getLong(ringPtr + SQ_KHEAD_OFFSET);
        final long sqMaskPtr = Unsafe.getUnsafe().getLong(ringPtr + SQ_KRING_MASK_OFFSET);
        this.sqKringMask = Unsafe.getUnsafe().getInt(sqMaskPtr);
        final long sqEntriesPtr = Unsafe.getUnsafe().getLong(ringPtr + SQ_KRING_ENTRIES_OFFSET);
        this.sqKringEntries = Unsafe.getUnsafe().getInt(sqEntriesPtr);

        this.cqesPtr = Unsafe.getUnsafe().getLong(ringPtr + CQ_CQES_OFFSET);
        this.cqKheadPtr = Unsafe.getUnsafe().getLong(ringPtr + CQ_KHEAD_OFFSET);
        this.cqKtailPtr = Unsafe.getUnsafe().getLong(ringPtr + CQ_KTAIL_OFFSET);
        final long cqMaskPtr = Unsafe.getUnsafe().getLong(ringPtr + CQ_KRING_MASK_OFFSET);
        this.cqKringMask = Unsafe.getUnsafe().getInt(cqMaskPtr);

//        Files.bumpFileCount(ringFd);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        facade.close(ringPtr);
//        Files.decrementFileCount(ringFd);
        closed = true;
    }

    /**
     * Submits pending sqes, if any.
     *
     * @return number of submitted sqes.
     */
    public int submit() {
        return facade.submit(ringPtr);
    }

    /**
     * Submits pending sqes, if any, and blocks until at least one operation result
     * becomes available as a cqe.
     *
     * @return number of submitted sqes.
     */
    public int submitAndWait() {
        return facade.submitAndWait(ringPtr, 1);
    }

    @TestOnly
    long enqueueNop() {
        return enqueueSqe(IORING_OP_NOP, 0, 0, 0, 0);
    }

    public long enqueueRead(long fd, long offset, long bufPtr, int len) {
        return enqueueSqe(IORING_OP_READ, fd, offset, bufPtr, len);
    }

    private long enqueueSqe(byte op, long fd, long offset, long bufPtr, int len) {
        final long sqePtr = nextSqe();
        if (sqePtr == 0) {
            return -1;
        }
        Unsafe.getUnsafe().putByte(sqePtr + SQE_OPCODE_OFFSET, op);
        Unsafe.getUnsafe().putInt(sqePtr + SQE_FD_OFFSET, (int) fd);
        Unsafe.getUnsafe().putLong(sqePtr + SQE_OFF_OFFSET, offset);
        Unsafe.getUnsafe().putLong(sqePtr + SQE_ADDR_OFFSET, bufPtr);
        Unsafe.getUnsafe().putInt(sqePtr + SQE_LEN_OFFSET, len);
        final long id = idSeq++;
        Unsafe.getUnsafe().putLong(sqePtr + SQE_USER_DATA_OFFSET, id);
        return id;
    }

    /**
     * Returns a pointer to sqe to fill. If there are sqes no available, returns 0.
     */
    private long nextSqe() {
        final int head = Unsafe.getUnsafe().getInt(sqKheadPtr);
        Unsafe.getUnsafe().loadFence();
        final int tail = Unsafe.getUnsafe().getInt(ringPtr + SQ_SQE_TAIL_OFFSET);
        if (tail - head < sqKringEntries) {
            final long ptr = sqesPtr + (long) (tail & sqKringMask) * SIZEOF_SQE;
            Unsafe.getUnsafe().putInt(ringPtr + SQ_SQE_TAIL_OFFSET, tail + 1);
            return ptr;
        }
        return 0;
    }

    /**
     * Checks if a cqe is ready and, if so, reads its data. Read data is
     * then available via {@link #getCqeId} and {@link #getCqeRes} methods.
     *
     * @return true - if cqe was read; false - otherwise.
     */
    public boolean nextCqe() {
        final int tail = Unsafe.getUnsafe().getInt(cqKtailPtr);
        Unsafe.getUnsafe().loadFence();
        final int head = Unsafe.getUnsafe().getInt(cqKheadPtr);
        if (tail == head) {
            lastCqeId = -1;
            lastCqeRes = 0;
            return false;
        }
        final long cqePtr = cqesPtr + (long) (head & cqKringMask) * SIZEOF_CQE;
        lastCqeId = Unsafe.getUnsafe().getLong(cqePtr + CQE_USER_DATA_OFFSET);
        lastCqeRes = Unsafe.getUnsafe().getInt(cqePtr + CQE_RES_OFFSET);
        Unsafe.getUnsafe().putInt(cqKheadPtr, head + 1);
        Unsafe.getUnsafe().storeFence();
        return true;
    }

    public long getCqeId() {
        return lastCqeId;
    }

    public int getCqeRes() {
        return lastCqeRes;
    }
}
