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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.orderby.LimitedSizeLongTreeChain;
import io.questdb.griffin.engine.orderby.RecordComparatorCompiler;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.IntHashSet;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


public class AsyncTopKAtom implements StatefulAtom, Reopenable, Plannable {
    private final AsyncFilterContext filterCtx;
    private final LimitedSizeLongTreeChain ownerChain;
    private final RecordComparator ownerComparator;
    private final PageFrameMemoryRecord ownerRecordA;
    private final PageFrameMemoryRecord ownerRecordB;
    private final ObjList<LimitedSizeLongTreeChain> perWorkerChains;
    private final ObjList<RecordComparator> perWorkerComparators;
    private final PerWorkerLocks perWorkerLocks;
    private final ObjList<PageFrameMemoryRecord> perWorkerRecordsB;
    private final int workerCount;

    public AsyncTopKAtom(
            @NotNull CairoConfiguration configuration,
            @Nullable Function ownerFilter,
            @Nullable IntHashSet filterUsedColumnIndexes,
            @Nullable ObjList<Function> perWorkerFilters,
            @Nullable CompiledFilter compiledFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @NotNull @Transient RecordComparatorCompiler recordComparatorCompiler,
            @NotNull @Transient ListColumnFilter orderByFilter,
            @NotNull @Transient RecordMetadata orderByMetadata,
            long lo,
            int workerCount
    ) throws SqlException {
        assert perWorkerFilters == null || perWorkerFilters.size() == workerCount;

        try {
            this.filterCtx = new AsyncFilterContext(
                    configuration,
                    compiledFilter,
                    bindVarMemory,
                    bindVarFunctions,
                    ownerFilter,
                    filterUsedColumnIndexes,
                    perWorkerFilters,
                    workerCount,
                    0,
                    configuration.getSqlParquetFrameCacheCapacity(),
                    2
            );

            final Class<RecordComparator> clazz = recordComparatorCompiler.compile(orderByMetadata, orderByFilter);
            this.ownerComparator = recordComparatorCompiler.newInstance(clazz);
            this.ownerRecordA = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
            this.ownerRecordB = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_B_LETTER);
            this.ownerChain = new LimitedSizeLongTreeChain(
                    configuration.getSqlSortKeyPageSize(),
                    configuration.getSqlSortKeyMaxPages(),
                    configuration.getSqlSortLightValuePageSize(),
                    configuration.getSqlSortLightValueMaxPages()
            );
            ownerChain.updateLimits(true, lo);

            this.workerCount = workerCount;
            this.perWorkerLocks = new PerWorkerLocks(configuration, workerCount);
            this.perWorkerComparators = new ObjList<>(workerCount);
            this.perWorkerChains = new ObjList<>(workerCount);
            this.perWorkerRecordsB = new ObjList<>(workerCount);

            for (int i = 0; i < workerCount; i++) {
                perWorkerComparators.extendAndSet(i, recordComparatorCompiler.newInstance(clazz));

                final LimitedSizeLongTreeChain chain = new LimitedSizeLongTreeChain(
                        configuration.getSqlSortKeyPageSize(),
                        configuration.getSqlSortKeyMaxPages(),
                        configuration.getSqlSortLightValuePageSize(),
                        configuration.getSqlSortLightValueMaxPages()
                );
                chain.updateLimits(true, lo);
                perWorkerChains.extendAndSet(i, chain);

                // We need to keep two records around.
                perWorkerRecordsB.extendAndSet(i, new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_B_LETTER));
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        Misc.free(ownerChain);
        Misc.free(ownerRecordA);
        Misc.free(ownerRecordB);
        freePerWorkerChainsAndPools();
        filterCtx.clear();
    }

    @Override
    public void close() {
        clear();
        Misc.free(filterCtx);
    }

    public void freePerWorkerChainsAndPools() {
        Misc.freeObjListAndKeepObjects(perWorkerChains);
        Misc.freeObjListAndKeepObjects(filterCtx.getPerWorkerMemoryPools());
        Misc.freeObjListAndKeepObjects(perWorkerRecordsB);
    }

    public RecordComparator getComparator(int slotId) {
        if (slotId == -1) {
            return ownerComparator;
        }
        return perWorkerComparators.getQuick(slotId);
    }

    public AsyncFilterContext getFilterContext() {
        return filterCtx;
    }

    public LimitedSizeLongTreeChain getOwnerChain() {
        return ownerChain;
    }

    public RecordComparator getOwnerComparator() {
        return ownerComparator;
    }

    public PageFrameMemoryRecord getOwnerRecordA() {
        return ownerRecordA;
    }

    public PageFrameMemoryRecord getOwnerRecordB() {
        return ownerRecordB;
    }

    // must not be used concurrently
    public ObjList<LimitedSizeLongTreeChain> getPerWorkerChains() {
        return perWorkerChains;
    }

    public PageFrameMemoryRecord getRecordB(int slotId) {
        if (slotId == -1) {
            return ownerRecordB;
        }
        return perWorkerRecordsB.getQuick(slotId);
    }

    public LimitedSizeLongTreeChain getTreeChain(int slotId) {
        if (slotId == -1) {
            return ownerChain;
        }
        return perWorkerChains.getQuick(slotId);
    }

    public int getWorkerCount() {
        return workerCount;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        filterCtx.initFilters(symbolTableSource, executionContext);

        ownerRecordA.of(symbolTableSource);
        ownerRecordB.of(symbolTableSource);
        for (int i = 0; i < workerCount; i++) {
            perWorkerRecordsB.getQuick(i).of(symbolTableSource);
        }
    }

    /**
     * Attempts to acquire a slot for the given worker thread.
     * On success, a {@link #release(int)} call must follow.
     *
     * @throws io.questdb.cairo.CairoException when circuit breaker has tripped
     */
    public int maybeAcquire(int workerId, boolean owner, SqlExecutionCircuitBreaker circuitBreaker) {
        if (workerId == -1 && owner) {
            // Owner thread is free to use the original functions anytime.
            return -1;
        }
        return perWorkerLocks.acquireSlot(workerId, circuitBreaker);
    }

    public void release(int slotId) {
        perWorkerLocks.releaseSlot(slotId);
    }

    @Override
    public void reopen() {
        ownerChain.reopen();
        for (int i = 0, n = perWorkerChains.size(); i < n; i++) {
            perWorkerChains.getQuick(i).reopen();
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        filterCtx.toPlan(sink);
    }
}
