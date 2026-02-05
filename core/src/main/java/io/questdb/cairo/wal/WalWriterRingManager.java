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
import io.questdb.std.IOURing;
import io.questdb.std.IOURingFacade;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;

import java.io.Closeable;

public class WalWriterRingManager implements Closeable {

    public static final long OP_FSYNC = 2L;
    public static final long OP_SNAPSHOT = 1L;
    public static final long OP_WRITE = 0L;
    private static final int COLUMN_SLOT_BITS = 18;
    private static final long COLUMN_SLOT_MASK = (1L << COLUMN_SLOT_BITS) - 1;
    private static final int MAX_COLUMN_SLOTS = 1 << COLUMN_SLOT_BITS;
    private static final int OP_KIND_BITS = 2;
    private static final long OP_KIND_MASK = (1L << OP_KIND_BITS) - 1;
    private static final int PAGE_ID_BITS = 44;
    private static final long PAGE_ID_MASK = (1L << PAGE_ID_BITS) - 1;
    private final ObjList<WalWriterRingColumn> columns = new ObjList<>();
    private final IOURing ring;
    private boolean closed;
    private int inFlightCount;
    private int nextColumnSlot;

    public WalWriterRingManager(IOURingFacade facade, int ringCapacity) {
        this.ring = facade.newInstance(Numbers.ceilPow2(ringCapacity));
    }

    public static long packFsyncId(int columnSlot) {
        return (OP_FSYNC << (COLUMN_SLOT_BITS + PAGE_ID_BITS))
                | ((long) columnSlot << PAGE_ID_BITS);
    }

    public static long packSnapshotId(int columnSlot) {
        return (OP_SNAPSHOT << (COLUMN_SLOT_BITS + PAGE_ID_BITS))
                | ((long) columnSlot << PAGE_ID_BITS);
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
        ring.close();
    }

    public void drainCqes() {
        while (ring.nextCqe()) {
            dispatchCqe();
        }
    }

    public void enqueueFsync(int columnSlot, long fd) {
        long userData = packFsyncId(columnSlot);
        if (!tryEnqueueFsync(fd, userData)) {
            submitAndDrainAll();
            if (!tryEnqueueFsync(fd, userData)) {
                throw CairoException.critical(0).put("io_uring SQ full after drain");
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

    public void enqueueWrite(int columnSlot, long pageId, long fd, long fileOffset, long bufAddr, int len) {
        long userData = packWriteId(columnSlot, pageId);
        if (!tryEnqueueWrite(fd, fileOffset, bufAddr, len, userData)) {
            submitAndDrainAll();
            if (!tryEnqueueWrite(fd, fileOffset, bufAddr, len, userData)) {
                throw CairoException.critical(0).put("io_uring SQ full after drain");
            }
        }
    }

    public int getInFlightCount() {
        return inFlightCount;
    }

    public int registerColumn(WalWriterRingColumn column) {
        int slot = nextColumnSlot++;
        if (slot >= MAX_COLUMN_SLOTS) {
            throw CairoException.critical(0).put("too many columns for io_uring ring manager: ").put(slot);
        }
        columns.extendAndSet(slot, column);
        return slot;
    }

    public void submitAndDrainAll() {
        if (inFlightCount > 0) {
            ring.submitAndWait();
            drainCqes();
        } else {
            ring.submit();
            drainCqes();
        }
    }

    public void unregisterColumn(int columnSlot) {
        columns.setQuick(columnSlot, null);
    }

    public void waitForAll() {
        while (inFlightCount > 0) {
            ring.submitAndWait();
            drainCqes();
            if (inFlightCount > 0) {
                checkDistressed();
            }
        }
    }

    public void waitForPage(int columnSlot, long pageId) {
        while (inFlightCount > 0) {
            ring.submitAndWait();
            drainCqes();
            // Check if the page CQE has been consumed by checking the column state.
            // The column's onWriteCompleted callback will have fired during drainCqes.
            WalWriterRingColumn column = columns.getQuick(columnSlot);
            if (column == null || column.isPageConfirmed(pageId)) {
                return;
            }
            checkDistressed();
        }
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
            case (int) OP_FSYNC -> {
                // Handled internally -- inFlightCount already decremented.
            }
        }
    }

    private boolean tryEnqueueFsync(long fd, long userData) {
        try {
            ring.enqueueFsync(fd, userData);
            inFlightCount++;
            return true;
        } catch (CairoException e) {
            return false;
        }
    }

    private boolean tryEnqueueWrite(long fd, long fileOffset, long bufAddr, int len, long userData) {
        try {
            ring.enqueueWrite(fd, fileOffset, bufAddr, len, userData);
            inFlightCount++;
            return true;
        } catch (CairoException e) {
            return false;
        }
    }

    public interface WalWriterRingColumn {
        boolean isDistressed();

        boolean isPageConfirmed(long pageId);

        void onSnapshotCompleted(int cqeRes);

        void onWriteCompleted(long pageId, int cqeRes);
    }
}
