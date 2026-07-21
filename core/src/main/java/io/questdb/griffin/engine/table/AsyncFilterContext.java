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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameFilteredMemoryRecord;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

/**
 * Shared filter and memory-pool infrastructure used by {@link AsyncGroupByAtom},
 * {@link AsyncGroupByNotKeyedAtom}, and {@link AsyncTopKAtom}.
 */
public class AsyncFilterContext implements Closeable {
    private final ObjList<Function> bindVarFunctions;
    private final MemoryCARW bindVarMemory;
    private final CompiledFilter compiledFilter;
    private final IntHashSet filterUsedColumnIndexes;
    private final ObjList<PageFrameFilteredMemoryRecord> frameFilteredMemoryRecords;
    private final DirectLongList ownerAuxAddresses;
    private final DirectLongList ownerDataAddresses;
    private final Function ownerFilter;
    private final DirectLongList ownerFilteredRows;
    private final PageFrameMemoryPool ownerMemoryPool;
    private final PageFrameFilteredMemoryRecord ownerPageFrameFilteredMemoryRecord;
    private final SelectivityStats ownerSelectivityStats = new SelectivityStats();
    private final ObjList<DirectLongList> perWorkerAuxAddresses;
    private final ObjList<DirectLongList> perWorkerDataAddresses;
    private final ObjList<DirectLongList> perWorkerFilteredRows;
    private final ObjList<Function> perWorkerFilters;
    private final ObjList<PageFrameMemoryPool> perWorkerMemoryPools;
    private final ObjList<SelectivityStats> perWorkerSelectivityStats;

    AsyncFilterContext(
            CairoConfiguration configuration,
            AsyncHorizonJoinResources resources,
            int slotCount,
            int filteredMemoryRecordCount,
            long ownerMemoryPoolMaxBytes,
            long perWorkerMemoryPoolMaxBytes
    ) {
        this(
                configuration,
                resources.takeCompiledFilter(),
                resources.takeBindVarMemory(),
                resources.takeBindVarFunctions(),
                resources.takeFilter(),
                resources.getFilterUsedColumnIndexes(),
                resources.takePerWorkerFilters(),
                slotCount,
                filteredMemoryRecordCount,
                ownerMemoryPoolMaxBytes,
                perWorkerMemoryPoolMaxBytes
        );
    }

    public AsyncFilterContext(
            CairoConfiguration configuration,
            @Nullable CompiledFilter compiledFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function ownerFilter,
            @Nullable IntHashSet filterUsedColumnIndexes,
            @Nullable ObjList<Function> perWorkerFilters,
            int slotCount,
            int filteredMemoryRecordCount,
            long ownerMemoryPoolMaxBytes,
            long perWorkerMemoryPoolMaxBytes
    ) {
        this.compiledFilter = compiledFilter;
        this.bindVarMemory = bindVarMemory;
        this.bindVarFunctions = bindVarFunctions;
        this.ownerFilter = ownerFilter;
        this.filterUsedColumnIndexes = filterUsedColumnIndexes;
        this.perWorkerFilters = perWorkerFilters;

        try {
            ownerMemoryPool = new PageFrameMemoryPool(ownerMemoryPoolMaxBytes);
            ownerFilteredRows = new DirectLongList(configuration.getPageFrameReduceRowIdListCapacity(), MemoryTag.NATIVE_OFFLOAD);
            if (compiledFilter != null) {
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
            perWorkerSelectivityStats = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerMemoryPools.extendAndSet(i, new PageFrameMemoryPool(perWorkerMemoryPoolMaxBytes));
                perWorkerFilteredRows.extendAndSet(i, new DirectLongList(configuration.getPageFrameReduceRowIdListCapacity(), MemoryTag.NATIVE_OFFLOAD));
                if (compiledFilter != null) {
                    perWorkerDataAddresses.extendAndSet(i, new DirectLongList(configuration.getPageFrameReduceColumnListCapacity(), MemoryTag.NATIVE_OFFLOAD));
                    perWorkerAuxAddresses.extendAndSet(i, new DirectLongList(configuration.getPageFrameReduceColumnListCapacity(), MemoryTag.NATIVE_OFFLOAD));
                }
                perWorkerSelectivityStats.extendAndSet(i, new SelectivityStats());
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

    public void clear() {
        Misc.free(ownerMemoryPool);
        Misc.freeObjListAndKeepObjects(perWorkerMemoryPools);
        ownerSelectivityStats.clear();
        Misc.clearObjList(perWorkerSelectivityStats);
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
    }

    @Override
    public void close() {
        Throwable cleanupFailure = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, compiledFilter);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, bindVarMemory);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, bindVarFunctions);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerFilter);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerFilters);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerMemoryPool);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerMemoryPools);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerFilteredRows);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerFilteredRows);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerDataAddresses);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerDataAddresses);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerAuxAddresses);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerAuxAddresses);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, frameFilteredMemoryRecords);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerPageFrameFilteredMemoryRecord);
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    public DirectLongList getAuxAddresses(int slotId) {
        if (slotId == -1) {
            return ownerAuxAddresses;
        }
        return perWorkerAuxAddresses.getQuick(slotId);
    }

    public ObjList<Function> getBindVarFunctions() {
        return bindVarFunctions;
    }

    public MemoryCARW getBindVarMemory() {
        return bindVarMemory;
    }

    public CompiledFilter getCompiledFilter() {
        return compiledFilter;
    }

    public DirectLongList getDataAddresses(int slotId) {
        if (slotId == -1) {
            return ownerDataAddresses;
        }
        return perWorkerDataAddresses.getQuick(slotId);
    }

    public Function getFilter(int slotId) {
        if (slotId == -1 || perWorkerFilters == null) {
            return ownerFilter;
        }
        return perWorkerFilters.getQuick(slotId);
    }

    public @Nullable IntHashSet getFilterUsedColumnIndexes() {
        return filterUsedColumnIndexes;
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

    public SelectivityStats getSelectivityStats(int slotId) {
        if (slotId == -1) {
            return ownerSelectivityStats;
        }
        return perWorkerSelectivityStats.getQuick(slotId);
    }

    public void initFilters(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        if (ownerFilter != null) {
            ownerFilter.init(symbolTableSource, executionContext);
        }
        if (perWorkerFilters != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                Function.init(perWorkerFilters, symbolTableSource, executionContext, ownerFilter);
            } finally {
                executionContext.setCloneSymbolTables(current);
            }
        }
        if (bindVarFunctions != null) {
            Function.init(bindVarFunctions, symbolTableSource, executionContext, null);
            AsyncFilterUtils.prepareBindVarMemory(executionContext, symbolTableSource, bindVarFunctions, bindVarMemory);
        }
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

    public boolean shouldUseLateMaterialization(int slotId, boolean isParquetFrame) {
        if (!isParquetFrame) {
            return false;
        }
        if (filterUsedColumnIndexes == null || filterUsedColumnIndexes.size() == 0) {
            return false;
        }
        return getSelectivityStats(slotId).shouldUseLateMaterialization();
    }

    public void toPlan(PlanSink sink) {
        sink.val(ownerFilter);
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
