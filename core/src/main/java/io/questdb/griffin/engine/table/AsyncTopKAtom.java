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

import io.questdb.PropertyKey;
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
import io.questdb.griffin.engine.PerWorkerLockOwner;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.orderby.EncodedTopKBuffer;
import io.questdb.griffin.engine.orderby.LimitedSizeLongTreeChain;
import io.questdb.griffin.engine.orderby.RecordComparatorCompiler;
import io.questdb.griffin.engine.orderby.SortKeyEncoder;
import io.questdb.griffin.engine.orderby.SortKeyType;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.DirectIntList;
import io.questdb.std.IntHashSet;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;


public class AsyncTopKAtom implements StatefulAtom, PerWorkerLockOwner, Reopenable, Plannable {
    private final IntHashSet encodedSkipColumnIndexes;
    private final AsyncFilterContext filterCtx;
    private final boolean isEncoded;
    private final long lo;
    private final LimitedSizeLongTreeChain ownerChain;
    private final RecordComparator ownerComparator;
    private final SortKeyEncoder ownerEncoder;
    private final PageFrameMemoryRecord ownerRecordA;
    private final PageFrameMemoryRecord ownerRecordB;
    private final EncodedTopKBuffer ownerTopK;
    private final ObjList<LimitedSizeLongTreeChain> perWorkerChains;
    private final ObjList<RecordComparator> perWorkerComparators;
    private final ObjList<SortKeyEncoder> perWorkerEncoders;
    private final PerWorkerLocks perWorkerLocks;
    private final ObjList<PageFrameMemoryRecord> perWorkerRecordsB;
    private final ObjList<EncodedTopKBuffer> perWorkerTopK;
    private final ObjList<DirectIntList> rankMaps;
    private final IntHashSet sortKeyColumnIndexes;
    private final int workerCount;
    // Per-query native memory tracker captured from SqlExecutionContext on init.
    // Null when no per-query limit applies. Workers and operator code feed it to
    // tracker-aware Unsafe overloads to charge allocations to the active workload.
    private MemoryTracker memoryTracker;
    private SortKeyType keyType;

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
                    configuration.getSqlParquetCacheMemorySize(),
                    0L
            );

            this.lo = lo;
            this.workerCount = workerCount;
            this.perWorkerLocks = new PerWorkerLocks(configuration, workerCount);
            this.ownerRecordA = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
            this.ownerRecordB = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_B_LETTER);
            this.perWorkerRecordsB = new ObjList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                perWorkerRecordsB.extendAndSet(i, new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_B_LETTER));
            }

            this.isEncoded = configuration.isSqlOrderBySortEnabled()
                    && SortKeyEncoder.isSupported(orderByMetadata, orderByFilter);
            if (isEncoded) {
                this.rankMaps = null;
                this.ownerComparator = null;
                this.ownerChain = null;
                this.perWorkerComparators = null;
                this.perWorkerChains = null;
                this.ownerEncoder = new SortKeyEncoder(orderByMetadata, orderByFilter);
                // Reduce runs on the shared worker pool; keep buffer sort/compaction
                // single-threaded so it does not nest parallelism onto those workers.
                this.ownerTopK = new EncodedTopKBuffer(configuration, false);
                this.perWorkerEncoders = new ObjList<>(workerCount);
                this.perWorkerTopK = new ObjList<>(workerCount);
                for (int i = 0; i < workerCount; i++) {
                    perWorkerEncoders.extendAndSet(i, new SortKeyEncoder(orderByMetadata, orderByFilter, ownerEncoder));
                    perWorkerTopK.extendAndSet(i, new EncodedTopKBuffer(configuration, false));
                }
                this.sortKeyColumnIndexes = SortKeyEncoder.extractSortKeyColumnIndexes(orderByFilter);
                if (filterUsedColumnIndexes != null) {
                    // Late materialization needs only the sort-key columns the filter
                    // pass did not decode; everything else is skipped.
                    final IntHashSet skipSet = new IntHashSet();
                    for (int i = 0, n = orderByMetadata.getColumnCount(); i < n; i++) {
                        if (!sortKeyColumnIndexes.contains(i) || filterUsedColumnIndexes.contains(i)) {
                            skipSet.add(i);
                        }
                    }
                    this.encodedSkipColumnIndexes = skipSet;
                } else {
                    this.encodedSkipColumnIndexes = null;
                }
            } else {
                this.ownerEncoder = null;
                this.ownerTopK = null;
                this.perWorkerEncoders = null;
                this.perWorkerTopK = null;
                this.sortKeyColumnIndexes = null;
                this.encodedSkipColumnIndexes = null;
                this.rankMaps = SortKeyEncoder.createRankMaps(orderByMetadata, orderByFilter);
                final Class<RecordComparator> clazz = recordComparatorCompiler.compile(orderByMetadata, orderByFilter);
                this.ownerComparator = recordComparatorCompiler.newInstance(clazz);
                // Lazy variant: the chain skeleton is constructed but the key/value
                // heaps are not allocated until the first cursor's reopen() binds a
                // MemoryTracker on the chain. This keeps malloc/free symmetric on
                // the per-query counter from the very first cursor.
                this.ownerChain = new LimitedSizeLongTreeChain(
                        configuration.getSqlSortKeyPageSize(),
                        configuration.getSqlSortKeyMaxBytes(),
                        configuration.getSqlSortLightValuePageSize(),
                        configuration.getSqlSortLightValueMaxBytes(),
                        PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath(),
                        PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES.getPropertyPath(),
                        false
                );
                ownerChain.updateLimits(true, lo);
                this.perWorkerComparators = new ObjList<>(workerCount);
                this.perWorkerChains = new ObjList<>(workerCount);
                for (int i = 0; i < workerCount; i++) {
                    perWorkerComparators.extendAndSet(i, recordComparatorCompiler.newInstance(clazz));
                    final LimitedSizeLongTreeChain chain = new LimitedSizeLongTreeChain(
                            configuration.getSqlSortKeyPageSize(),
                            configuration.getSqlSortKeyMaxBytes(),
                            configuration.getSqlSortLightValuePageSize(),
                            configuration.getSqlSortLightValueMaxBytes(),
                            PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath(),
                            PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES.getPropertyPath(),
                            false
                    );
                    chain.updateLimits(true, lo);
                    perWorkerChains.extendAndSet(i, chain);
                }
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        Misc.freeObjListAndKeepObjects(rankMaps);
        Misc.free(ownerChain);
        Misc.free(ownerTopK);
        Misc.free(ownerEncoder);
        Misc.free(ownerRecordA);
        Misc.free(ownerRecordB);
        freePerWorkerChainsAndPools();
        filterCtx.clear();
        memoryTracker = null;
    }

    @Override
    public void close() {
        clear();
        Misc.free(filterCtx);
    }

    public void freePerWorkerChainsAndPools() {
        Misc.freeObjListAndKeepObjects(perWorkerChains);
        Misc.freeObjListAndKeepObjects(perWorkerTopK);
        Misc.freeObjListAndKeepObjects(perWorkerEncoders);
        Misc.freeObjListAndKeepObjects(filterCtx.getPerWorkerMemoryPools());
        Misc.freeObjListAndKeepObjects(perWorkerRecordsB);
    }

    public RecordComparator getComparator(int slotId) {
        if (slotId == -1) {
            return ownerComparator;
        }
        return perWorkerComparators.getQuick(slotId);
    }

    public IntHashSet getEncodedSkipColumnIndexes() {
        return encodedSkipColumnIndexes;
    }

    public SortKeyEncoder getEncoder(int slotId) {
        if (slotId == -1) {
            return ownerEncoder;
        }
        return perWorkerEncoders.getQuick(slotId);
    }

    public AsyncFilterContext getFilterContext() {
        return filterCtx;
    }

    public MemoryTracker getMemoryTracker() {
        return memoryTracker;
    }

    public SortKeyType getKeyType() {
        return keyType;
    }

    public long getLo() {
        return lo;
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

    @Override
    @TestOnly
    public PerWorkerLocks getPerWorkerLocks() {
        return perWorkerLocks;
    }

    public PageFrameMemoryRecord getRecordB(int slotId) {
        if (slotId == -1) {
            return ownerRecordB;
        }
        return perWorkerRecordsB.getQuick(slotId);
    }

    public IntHashSet getSortKeyColumnIndexes() {
        return sortKeyColumnIndexes;
    }

    public EncodedTopKBuffer getTopK(int slotId) {
        if (slotId == -1) {
            return ownerTopK;
        }
        return perWorkerTopK.getQuick(slotId);
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
        memoryTracker = executionContext.getMemoryTracker();
        try {
            filterCtx.initFilters(symbolTableSource, executionContext);
            if (isEncoded) {
                keyType = ownerEncoder.init(symbolTableSource);
                assert keyType != SortKeyType.UNSUPPORTED;
                // Bind the tracker before of() presizes entryMem, else the alloc is
                // uncharged while close() still debits it: the counter goes negative and,
                // under -ea, the assert aborts the JVM mid-free (double free). reopen()
                // binds too late - it no-ops once entryMem is allocated.
                ownerTopK.setMemoryTracker(memoryTracker);
                ownerTopK.of(keyType, true, lo);
                // Fixed-width keys encode inline; variable-length keys spill into a
                // per-buffer key heap that the encoder must write into.
                final boolean isVariable = keyType.isVariable();
                if (isVariable) {
                    ownerEncoder.setKeyHeap(ownerTopK.getKeyHeap());
                }
                for (int i = 0; i < workerCount; i++) {
                    // Rank map building sorts the whole symbol dictionary; workers
                    // borrow the owner's maps instead of rebuilding identical ones.
                    final SortKeyEncoder workerEncoder = perWorkerEncoders.getQuick(i);
                    final EncodedTopKBuffer workerTopK = perWorkerTopK.getQuick(i);
                    workerEncoder.initFrom(ownerEncoder);
                    workerTopK.setMemoryTracker(memoryTracker);
                    workerTopK.of(keyType, true, lo);
                    if (isVariable) {
                        workerEncoder.setKeyHeap(workerTopK.getKeyHeap());
                    }
                }
            } else {
                buildRankMaps(symbolTableSource);
            }

            ownerRecordA.of(symbolTableSource);
            ownerRecordB.of(symbolTableSource);
            for (int i = 0; i < workerCount; i++) {
                perWorkerRecordsB.getQuick(i).of(symbolTableSource);
            }
        } catch (Throwable th) {
            // A per-query limit breach while presizing the buffers leaves some allocated and
            // charged and the rest not. getCursor() guards only cursor.of()/reopen(), so an
            // init-time throw escapes that close(): the partial allocations would leak and the
            // next open would free buffers the new tracker never charged, underflowing it into
            // a double free. Roll back here, mirroring the ctor, to keep the counter symmetric.
            clear();
            throw th;
        }
    }

    public boolean isEncoded() {
        return isEncoded;
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
        if (isEncoded) {
            // Bind the tracker captured in init() on every encoded buffer before reopen,
            // so each buffer's alloc/free is charged symmetrically to the per-query counter.
            ownerTopK.setMemoryTracker(memoryTracker);
            ownerTopK.reopen();
            for (int i = 0; i < workerCount; i++) {
                final EncodedTopKBuffer topK = perWorkerTopK.getQuick(i);
                topK.setMemoryTracker(memoryTracker);
                topK.reopen();
            }
        } else {
            // Propagate the tracker captured in init() to every chain before reopen,
            // so each chain's malloc is charged to the active workload's per-query
            // counter. The matching close at workload end charges to the same tracker
            // because the chain holds the reference for the duration of the cursor.
            ownerChain.setMemoryTracker(memoryTracker);
            ownerChain.reopen();
            for (int i = 0, n = perWorkerChains.size(); i < n; i++) {
                final LimitedSizeLongTreeChain chain = perWorkerChains.getQuick(i);
                chain.setMemoryTracker(memoryTracker);
                chain.reopen();
            }
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        filterCtx.toPlan(sink);
    }

    private void buildRankMaps(SymbolTableSource symbolTableSource) {
        SortKeyEncoder.buildRankMaps(symbolTableSource, rankMaps, ownerComparator);
        if (rankMaps != null) {
            for (int w = 0; w < workerCount; w++) {
                perWorkerComparators.getQuick(w).setRankMaps(rankMaps);
            }
        }
    }
}
