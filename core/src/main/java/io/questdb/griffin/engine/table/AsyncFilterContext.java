/*+*****************************************************************************
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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameFilteredMemoryRecord;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * An {@link AsyncFilter} with the memory each worker slot filters its page frames in: a page frame memory pool, the
 * list of the row ids that pass, the column addresses a JIT-compiled filter reads, and, where requested, a filtered
 * memory record that reads the rows of a late-materialized frame.
 */
public class AsyncFilterContext extends AsyncFilter implements Mutable {
    private final ObjList<PageFrameFilteredMemoryRecord> frameFilteredMemoryRecords;
    private final DirectLongList ownerAuxAddresses;
    private final DirectLongList ownerDataAddresses;
    private final DirectLongList ownerFilteredRows;
    private final PageFrameMemoryPool ownerMemoryPool;
    private final PageFrameFilteredMemoryRecord ownerPageFrameFilteredMemoryRecord;
    private final ObjList<DirectLongList> perWorkerAuxAddresses;
    private final ObjList<DirectLongList> perWorkerDataAddresses;
    private final ObjList<DirectLongList> perWorkerFilteredRows;
    private final ObjList<PageFrameMemoryPool> perWorkerMemoryPools;

    /**
     * Takes the filter and bind variable resources of {@code resources}, as {@link AsyncFilter} does.
     */
    public AsyncFilterContext(
            CairoConfiguration configuration,
            AsyncFilterResources resources,
            int slotCount,
            int filteredMemoryRecordCount,
            long ownerMemoryPoolMaxBytes,
            long perWorkerMemoryPoolMaxBytes
    ) {
        super(resources, slotCount);
        final boolean isCompiled = getCompiledFilter() != null;
        try {
            ownerMemoryPool = new PageFrameMemoryPool(configuration, ownerMemoryPoolMaxBytes);
            ownerFilteredRows = new DirectLongList(configuration.getPageFrameReduceRowIdListCapacity(), MemoryTag.NATIVE_OFFLOAD);
            if (isCompiled) {
                ownerDataAddresses = new DirectLongList(configuration.getPageFrameReduceColumnListCapacity(), MemoryTag.NATIVE_OFFLOAD);
                ownerAuxAddresses = new DirectLongList(configuration.getPageFrameReduceColumnListCapacity(), MemoryTag.NATIVE_OFFLOAD);
            } else {
                ownerDataAddresses = null;
                ownerAuxAddresses = null;
            }

            perWorkerMemoryPools = new ObjList<>(slotCount);
            perWorkerFilteredRows = new ObjList<>(slotCount);
            perWorkerDataAddresses = new ObjList<>(slotCount);
            perWorkerAuxAddresses = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerMemoryPools.extendAndSet(i, new PageFrameMemoryPool(configuration, perWorkerMemoryPoolMaxBytes));
                perWorkerFilteredRows.extendAndSet(i, new DirectLongList(configuration.getPageFrameReduceRowIdListCapacity(), MemoryTag.NATIVE_OFFLOAD));
                if (isCompiled) {
                    perWorkerDataAddresses.extendAndSet(i, new DirectLongList(configuration.getPageFrameReduceColumnListCapacity(), MemoryTag.NATIVE_OFFLOAD));
                    perWorkerAuxAddresses.extendAndSet(i, new DirectLongList(configuration.getPageFrameReduceColumnListCapacity(), MemoryTag.NATIVE_OFFLOAD));
                }
            }

            if (filteredMemoryRecordCount > 0) {
                ownerPageFrameFilteredMemoryRecord = new PageFrameFilteredMemoryRecord();
                frameFilteredMemoryRecords = new ObjList<>(filteredMemoryRecordCount);
                for (int i = 0; i < filteredMemoryRecordCount; i++) {
                    frameFilteredMemoryRecords.extendAndSet(i, new PageFrameFilteredMemoryRecord());
                }
            } else {
                ownerPageFrameFilteredMemoryRecord = null;
                frameFilteredMemoryRecords = null;
            }
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public void clear() {
        final Throwable failure = cursorClosed(null);
        Misc.free(ownerMemoryPool);
        Misc.freeObjListAndKeepObjects(perWorkerMemoryPools);
        // Shrink the row-id and column-address lists back to initial capacity,
        // mirroring the per-task reset in PageFrameReduceTask.clear(). Under a JIT
        // filter the row-id lists grow to a full page frame (up to
        // cairo.sql.page.frame.max.rows longs = 8 MB each) and only ever grow, so an
        // idle or cached factory would otherwise pin peak-sized NATIVE_OFFLOAD buffers
        // until eviction.
        resetCapacity(ownerFilteredRows);
        resetCapacity(ownerDataAddresses);
        resetCapacity(ownerAuxAddresses);
        resetCapacity(perWorkerFilteredRows);
        resetCapacity(perWorkerDataAddresses);
        resetCapacity(perWorkerAuxAddresses);
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * Frees the filter, see {@link AsyncFilter#close(Throwable)}, and the memory of every worker slot, folding each
     * failure into {@code cleanupFailure}, which it returns.
     */
    @Override
    public Throwable close(@Nullable Throwable cleanupFailure) {
        cleanupFailure = super.close(cleanupFailure);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerMemoryPool);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerMemoryPools);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerFilteredRows);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerFilteredRows);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerDataAddresses);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerDataAddresses);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerAuxAddresses);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerAuxAddresses);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, frameFilteredMemoryRecords);
        return Misc.freeBestEffort(cleanupFailure, ownerPageFrameFilteredMemoryRecord);
    }

    public DirectLongList getAuxAddresses(int slotId) {
        if (slotId == -1) {
            return ownerAuxAddresses;
        }
        return perWorkerAuxAddresses.getQuick(slotId);
    }

    public DirectLongList getDataAddresses(int slotId) {
        if (slotId == -1) {
            return ownerDataAddresses;
        }
        return perWorkerDataAddresses.getQuick(slotId);
    }

    public DirectLongList getFilteredRows(int slotId) {
        if (slotId == -1) {
            return ownerFilteredRows;
        }
        return perWorkerFilteredRows.getQuick(slotId);
    }

    public PageFrameMemoryPool getMemoryPool(int slotId) {
        if (slotId == -1) {
            return ownerMemoryPool;
        }
        return perWorkerMemoryPools.getQuick(slotId);
    }

    public PageFrameMemoryPool getOwnerMemoryPool() {
        return ownerMemoryPool;
    }

    public PageFrameFilteredMemoryRecord getPageFrameFilteredMemoryRecord(int slotId) {
        if (slotId == -1) {
            return ownerPageFrameFilteredMemoryRecord;
        }
        return frameFilteredMemoryRecords.getQuick(slotId);
    }

    public ObjList<PageFrameMemoryPool> getPerWorkerMemoryPools() {
        return perWorkerMemoryPools;
    }

    public void initMemoryPools(PageFrameAddressCache pageFrameAddressCache, MemoryTracker memoryTracker) {
        initMemoryPools(pageFrameAddressCache, memoryTracker, ParquetDecodeHint.MONOTONIC);
    }

    public void initMemoryPools(PageFrameAddressCache pageFrameAddressCache, MemoryTracker memoryTracker, ParquetDecodeHint ownerHint) {
        ownerMemoryPool.setMemoryTracker(memoryTracker);
        ownerMemoryPool.of(pageFrameAddressCache, ownerHint);
        for (int i = 0, n = perWorkerMemoryPools.size(); i < n; i++) {
            final PageFrameMemoryPool pool = perWorkerMemoryPools.getQuick(i);
            pool.setMemoryTracker(memoryTracker);
            pool.of(pageFrameAddressCache, ParquetDecodeHint.MONOTONIC);
        }
    }

    private static void resetCapacity(@Nullable DirectLongList list) {
        // Skip closed lists: resetCapacity() on a capacity-0 list would re-malloc
        // (resurrect) it. clear() can run after close() on the horizon-join error path.
        if (list != null && list.getCapacity() > 0) {
            list.resetCapacity();
        }
    }

    private static void resetCapacity(@Nullable ObjList<DirectLongList> lists) {
        if (lists != null) {
            for (int i = 0, n = lists.size(); i < n; i++) {
                resetCapacity(lists.getQuick(i));
            }
        }
    }
}
