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

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoException;
import io.questdb.std.Files;
import io.questdb.std.IOURing;
import io.questdb.std.IOURingFacade;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

import java.io.Closeable;

public class WalWriterRingManager implements Closeable {

    public static final long OP_FSYNC = 2L;
    public static final long OP_SNAPSHOT = 1L;
    public static final long OP_SWAP_WRITE = 3L;
    public static final long OP_WRITE = 0L;
    private static final int COLUMN_SLOT_BITS = 18;
    private static final long COLUMN_SLOT_MASK = (1L << COLUMN_SLOT_BITS) - 1;
    private static final int MAX_COLUMN_SLOTS = 1 << COLUMN_SLOT_BITS;
    private static final int OP_KIND_BITS = 2;
    private static final long OP_KIND_MASK = (1L << OP_KIND_BITS) - 1;
    private static final int PAGE_ID_BITS = 44;
    private static final long PAGE_ID_MASK = (1L << PAGE_ID_BITS) - 1;
    private final ObjList<WalWriterRingColumn> columns = new ObjList<>();
    private final IntList registeredOsFds = new IntList();
    private final IOURing ring;
    private final int sqHighWatermark;
    private boolean closed;
    private boolean filesRegistered;
    private int inFlightCount;
    private int maxFileSlots;
    private int nextColumnSlot;
    private int pendingSubmitCount;
    private WalWriterBufferPool pool;

    public WalWriterRingManager(IOURingFacade facade, int ringCapacity) {
        int capacity = Numbers.ceilPow2(ringCapacity);
        this.ring = facade.newInstance(capacity);
        this.sqHighWatermark = capacity * 3 / 4;
    }

    public static long packFsyncId(int columnSlot) {
        return (OP_FSYNC << (COLUMN_SLOT_BITS + PAGE_ID_BITS))
                | ((long) columnSlot << PAGE_ID_BITS);
    }

    public static long packSnapshotId(int columnSlot) {
        return (OP_SNAPSHOT << (COLUMN_SLOT_BITS + PAGE_ID_BITS))
                | ((long) columnSlot << PAGE_ID_BITS);
    }

    public static long packSwapWriteId(int columnSlot, int bufferIndex) {
        return (OP_SWAP_WRITE << (COLUMN_SLOT_BITS + PAGE_ID_BITS))
                | ((long) columnSlot << PAGE_ID_BITS)
                | (bufferIndex & PAGE_ID_MASK);
    }

    public static long packWriteId(int columnSlot, long pageId) {
        return (OP_WRITE << (COLUMN_SLOT_BITS + PAGE_ID_BITS))
                | ((long) columnSlot << PAGE_ID_BITS)
                | (pageId & PAGE_ID_MASK);
    }

    public static int unpackColumnSlot(long packed) {
        return (int) ((packed >> PAGE_ID_BITS) & COLUMN_SLOT_MASK);
    }

    public static long unpackOpKind(long packed) {
        return (packed >> (COLUMN_SLOT_BITS + PAGE_ID_BITS)) & OP_KIND_MASK;
    }

    public static long unpackPageId(long packed) {
        return packed & PAGE_ID_MASK;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (filesRegistered) {
            ring.unregisterFiles();
            filesRegistered = false;
            registeredOsFds.clear();
        }
        ring.close();
    }

    public void drainCqes() {
        while (ring.nextCqe()) {
            dispatchCqe();
        }
    }

    public void enqueueFsync(int columnSlot, long fd) {
        long userData = packFsyncId(columnSlot);
        if (canUseFixedFile(columnSlot)) {
            if (!tryEnqueueFsyncFixedFile(columnSlot, userData)) {
                submitAndDrainAll();
                if (!tryEnqueueFsyncFixedFile(columnSlot, userData)) {
                    throw CairoException.critical(0).put("io_uring SQ full after drain");
                }
            }
        } else {
            if (!tryEnqueueFsync(fd, userData)) {
                submitAndDrainAll();
                if (!tryEnqueueFsync(fd, userData)) {
                    throw CairoException.critical(0).put("io_uring SQ full after drain");
                }
            }
        }
    }

    public void enqueueSnapshotWrite(int columnSlot, long fd, long fileOffset, long bufAddr, int len) {
        long userData = packSnapshotId(columnSlot);
        if (!tryEnqueueWrite(fd, fileOffset, bufAddr, len, userData)) {
            submitAndDrainAll();
            if (!tryEnqueueWrite(fd, fileOffset, bufAddr, len, userData)) {
                throw CairoException.critical(0).put("io_uring SQ full after drain");
            }
        }
    }

    public void enqueueSwapWrite(int columnSlot, int bufferIndex, long fd, long fileOffset, long bufAddr, int len) {
        long userData = packSwapWriteId(columnSlot, bufferIndex);
        boolean fixedBuf = pool != null && pool.isRegistered();
        boolean fixedFile = canUseFixedFile(columnSlot);
        if (fixedFile && fixedBuf) {
            if (!tryEnqueueWriteFixedFileBuf(columnSlot, fileOffset, bufAddr, len, bufferIndex, userData)) {
                submitAndDrainAll();
                if (!tryEnqueueWriteFixedFileBuf(columnSlot, fileOffset, bufAddr, len, bufferIndex, userData)) {
                    throw CairoException.critical(0).put("io_uring SQ full after drain");
                }
            }
        } else if (fixedFile) {
            if (!tryEnqueueWriteFixedFile(columnSlot, fileOffset, bufAddr, len, userData)) {
                submitAndDrainAll();
                if (!tryEnqueueWriteFixedFile(columnSlot, fileOffset, bufAddr, len, userData)) {
                    throw CairoException.critical(0).put("io_uring SQ full after drain");
                }
            }
        } else if (fixedBuf) {
            if (!tryEnqueueWriteFixed(fd, fileOffset, bufAddr, len, bufferIndex, userData)) {
                submitAndDrainAll();
                if (!tryEnqueueWriteFixed(fd, fileOffset, bufAddr, len, bufferIndex, userData)) {
                    throw CairoException.critical(0).put("io_uring SQ full after drain");
                }
            }
        } else {
            if (!tryEnqueueWrite(fd, fileOffset, bufAddr, len, userData)) {
                submitAndDrainAll();
                if (!tryEnqueueWrite(fd, fileOffset, bufAddr, len, userData)) {
                    throw CairoException.critical(0).put("io_uring SQ full after drain");
                }
            }
        }
    }

    public void enqueueWrite(int columnSlot, long pageId, long fd, long fileOffset, long bufAddr, int len) {
        long userData = packWriteId(columnSlot, pageId);
        if (canUseFixedFile(columnSlot)) {
            if (!tryEnqueueWriteFixedFile(columnSlot, fileOffset, bufAddr, len, userData)) {
                submitAndDrainAll();
                if (!tryEnqueueWriteFixedFile(columnSlot, fileOffset, bufAddr, len, userData)) {
                    throw CairoException.critical(0).put("io_uring SQ full after drain");
                }
            }
        } else {
            if (!tryEnqueueWrite(fd, fileOffset, bufAddr, len, userData)) {
                submitAndDrainAll();
                if (!tryEnqueueWrite(fd, fileOffset, bufAddr, len, userData)) {
                    throw CairoException.critical(0).put("io_uring SQ full after drain");
                }
            }
        }
    }

    public void enqueueWrite(int columnSlot, long pageId, int bufferIndex, long fd, long fileOffset, long bufAddr, int len) {
        long userData = packWriteId(columnSlot, pageId);
        boolean fixedBuf = pool != null && pool.isRegistered();
        boolean fixedFile = canUseFixedFile(columnSlot);
        if (fixedFile && fixedBuf) {
            if (!tryEnqueueWriteFixedFileBuf(columnSlot, fileOffset, bufAddr, len, bufferIndex, userData)) {
                submitAndDrainAll();
                if (!tryEnqueueWriteFixedFileBuf(columnSlot, fileOffset, bufAddr, len, bufferIndex, userData)) {
                    throw CairoException.critical(0).put("io_uring SQ full after drain");
                }
            }
        } else if (fixedFile) {
            if (!tryEnqueueWriteFixedFile(columnSlot, fileOffset, bufAddr, len, userData)) {
                submitAndDrainAll();
                if (!tryEnqueueWriteFixedFile(columnSlot, fileOffset, bufAddr, len, userData)) {
                    throw CairoException.critical(0).put("io_uring SQ full after drain");
                }
            }
        } else if (fixedBuf) {
            if (!tryEnqueueWriteFixed(fd, fileOffset, bufAddr, len, bufferIndex, userData)) {
                submitAndDrainAll();
                if (!tryEnqueueWriteFixed(fd, fileOffset, bufAddr, len, bufferIndex, userData)) {
                    throw CairoException.critical(0).put("io_uring SQ full after drain");
                }
            }
        } else {
            if (!tryEnqueueWrite(fd, fileOffset, bufAddr, len, userData)) {
                submitAndDrainAll();
                if (!tryEnqueueWrite(fd, fileOffset, bufAddr, len, userData)) {
                    throw CairoException.critical(0).put("io_uring SQ full after drain");
                }
            }
        }
    }

    public int getInFlightCount() {
        return inFlightCount;
    }

    public int registerBuffers(long iovsAddr, int count) {
        return ring.registerBuffers(iovsAddr, count);
    }

    public int registerColumn(WalWriterRingColumn column) {
        int slot = nextColumnSlot++;
        if (slot >= MAX_COLUMN_SLOTS) {
            throw CairoException.critical(0).put("too many columns for io_uring ring manager: ").put(slot);
        }
        columns.extendAndSet(slot, column);
        return slot;
    }

    public boolean registerFilesSparse(int maxSlots) {
        int ret = ring.registerFilesSparse(maxSlots);
        filesRegistered = ret >= 0;
        if (filesRegistered) {
            this.maxFileSlots = maxSlots;
            registeredOsFds.clear();
        }
        return filesRegistered;
    }

    public void setPool(WalWriterBufferPool pool) {
        this.pool = pool;
    }

    public void submitAndDrainAll() {
        pendingSubmitCount = 0;
        if (inFlightCount > 0) {
            submitAndWaitOrThrow(1);
            drainCqes();
        } else {
            submitOrThrow();
            drainCqes();
        }
    }

    public int unregisterBuffers() {
        return ring.unregisterBuffers();
    }

    public void unregisterColumn(int columnSlot) {
        columns.setQuick(columnSlot, null);
    }

    public void updateRegisteredFile(int columnSlot, long fd) {
        if (!filesRegistered) {
            return;
        }
        if (columnSlot >= maxFileSlots) {
            growFileTable(Math.max(columnSlot + 16, maxFileSlots * 2));
        }
        if (!filesRegistered) {
            // growFileTable failed and disabled file registration
            return;
        }
        int osFd = Files.toOsFd(fd);
        ring.updateRegisteredFile(columnSlot, osFd);
        while (registeredOsFds.size() <= columnSlot) {
            registeredOsFds.add(-1);
        }
        registeredOsFds.setQuick(columnSlot, osFd);
    }

    public void waitForAll() {
        if (inFlightCount == 0) {
            return;
        }
        pendingSubmitCount = 0;
        // Submit any pending SQEs first, then drain what's already available.
        submitOrThrow();
        drainCqes();
        while (inFlightCount > 0) {
            checkDistressed();
            submitAndWaitOrThrow(1);
            drainCqes();
        }
    }

    public void waitForPage(int columnSlot, long pageId) {
        pendingSubmitCount = 0;
        submitOrThrow();
        while (inFlightCount > 0) {
            drainCqes();
            WalWriterRingColumn column = columns.getQuick(columnSlot);
            if (column == null || column.isPageConfirmed(pageId)) {
                return;
            }
            if (inFlightCount > 0) {
                checkDistressed();
                submitAndWaitOrThrow(1);
            }
        }
    }

    private boolean canUseFixedFile(int columnSlot) {
        return filesRegistered && columnSlot < maxFileSlots;
    }

    private void checkDistressed() {
        for (int i = 0, n = columns.size(); i < n; i++) {
            WalWriterRingColumn column = columns.getQuick(i);
            if (column != null && column.isDistressed()) {
                throw CairoException.critical(0).put("io_uring write error on column slot ").put(i);
            }
        }
    }

    private void dispatchCqe() {
        long userData = ring.getCqeId();
        int cqeRes = ring.getCqeRes();
        inFlightCount--;

        long opKind = unpackOpKind(userData);
        int columnSlot = unpackColumnSlot(userData);

        switch ((int) opKind) {
            case (int) OP_WRITE -> {
                long pageId = unpackPageId(userData);
                WalWriterRingColumn column = columns.getQuick(columnSlot);
                if (column != null) {
                    column.onWriteCompleted(pageId, cqeRes);
                }
            }
            case (int) OP_SNAPSHOT -> {
                WalWriterRingColumn column = columns.getQuick(columnSlot);
                if (column != null) {
                    column.onSnapshotCompleted(cqeRes);
                }
            }
            case (int) OP_SWAP_WRITE -> {
                int bufIdx = (int) unpackPageId(userData);
                // NB: unlike OP_WRITE, we don't validate cqeRes against the submitted length.
                // Short positive writes don't occur on Linux local filesystems; if that changes,
                // the expected length would need to be tracked per swap buffer.
                if (cqeRes < 0) {
                    WalWriterRingColumn col = columns.getQuick(columnSlot);
                    if (col != null) {
                        col.onSwapWriteError(cqeRes);
                    }
                }
                pool.release(bufIdx);
            }
            case (int) OP_FSYNC -> {
                if (cqeRes < 0) {
                    WalWriterRingColumn column = columns.getQuick(columnSlot);
                    if (column != null) {
                        column.onFsyncCompleted(cqeRes);
                    }
                }
            }
        }
    }

    private void growFileTable(int newSize) {
        // Drain all in-flight ops — they may reference fixed-file slot indices
        // that become invalid after unregister.
        waitForAll();
        ring.unregisterFiles();
        int ret = ring.registerFilesSparse(newSize);
        if (ret < 0) {
            // Kernel rejected the new size; disable file registration entirely.
            filesRegistered = false;
            registeredOsFds.clear();
            return;
        }
        maxFileSlots = newSize;
        // Re-register all previously tracked fds.
        for (int i = 0, n = registeredOsFds.size(); i < n; i++) {
            int osFd = registeredOsFds.getQuick(i);
            if (osFd >= 0) {
                ring.updateRegisteredFile(i, osFd);
            }
        }
    }

    private void proactiveSubmitIfNeeded() {
        if (pendingSubmitCount >= sqHighWatermark) {
            submitOrThrow();
            drainCqes();
            pendingSubmitCount = 0;
        }
    }

    private void submitAndWaitOrThrow(int waitNr) {
        final int rc = ring.submitAndWait(waitNr);
        if (rc < 0) {
            throw CairoException.critical(-rc).put("io_uring submitAndWait failed [rc=").put(rc).put(']');
        }
    }

    private void submitOrThrow() {
        final int rc = ring.submit();
        if (rc < 0) {
            throw CairoException.critical(-rc).put("io_uring submit failed [rc=").put(rc).put(']');
        }
    }

    private boolean tryEnqueueFsync(long fd, long userData) {
        try {
            ring.enqueueFsync(fd, userData);
            inFlightCount++;
            pendingSubmitCount++;
            proactiveSubmitIfNeeded();
            return true;
        } catch (CairoException e) {
            return false;
        }
    }

    private boolean tryEnqueueFsyncFixedFile(int fileSlot, long userData) {
        try {
            ring.enqueueFsyncFixedFile(fileSlot, userData);
            inFlightCount++;
            pendingSubmitCount++;
            proactiveSubmitIfNeeded();
            return true;
        } catch (CairoException e) {
            return false;
        }
    }

    private boolean tryEnqueueWrite(long fd, long fileOffset, long bufAddr, int len, long userData) {
        try {
            ring.enqueueWrite(fd, fileOffset, bufAddr, len, userData);
            inFlightCount++;
            pendingSubmitCount++;
            proactiveSubmitIfNeeded();
            return true;
        } catch (CairoException e) {
            return false;
        }
    }

    private boolean tryEnqueueWriteFixed(long fd, long fileOffset, long bufAddr, int len, int bufIndex, long userData) {
        try {
            ring.enqueueWriteFixed(fd, fileOffset, bufAddr, len, bufIndex, userData);
            inFlightCount++;
            pendingSubmitCount++;
            proactiveSubmitIfNeeded();
            return true;
        } catch (CairoException e) {
            return false;
        }
    }

    private boolean tryEnqueueWriteFixedFile(int fileSlot, long fileOffset, long bufAddr, int len, long userData) {
        try {
            ring.enqueueWriteFixedFile(fileSlot, fileOffset, bufAddr, len, userData);
            inFlightCount++;
            pendingSubmitCount++;
            proactiveSubmitIfNeeded();
            return true;
        } catch (CairoException e) {
            return false;
        }
    }

    private boolean tryEnqueueWriteFixedFileBuf(int fileSlot, long fileOffset, long bufAddr, int len, int bufIndex, long userData) {
        try {
            ring.enqueueWriteFixedFileBuf(fileSlot, fileOffset, bufAddr, len, bufIndex, userData);
            inFlightCount++;
            pendingSubmitCount++;
            proactiveSubmitIfNeeded();
            return true;
        } catch (CairoException e) {
            return false;
        }
    }

    public interface WalWriterRingColumn {
        boolean isDistressed();

        boolean isPageConfirmed(long pageId);

        void onFsyncCompleted(int cqeRes);

        void onSnapshotCompleted(int cqeRes);

        void onSwapWriteError(int cqeRes);

        void onWriteCompleted(long pageId, int cqeRes);
    }
}
