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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLockOwner;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.PerWorkerFunctionList;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

/**
 * Base class for HORIZON JOIN atoms that manages common per-worker resources.
 * <p>
 * This class holds:
 * 1. Per-worker time frame helpers for ASOF JOIN lookups via ConcurrentTimeFrameCursor
 * 2. Per-worker group by functions and updaters
 * 3. Per-worker ASOF join maps for symbol -> rowId mappings (when keyed join)
 * 4. Filter resources (compiled and Java filters)
 */
public abstract class BaseAsyncHorizonJoinAtom implements StatefulAtom, PerWorkerLockOwner, Closeable, Reopenable, Plannable {
    protected final long bwdScanAbsoluteThreshold;
    protected final long bwdScanMinGap;
    protected final long bwdScanSwitchFactor;
    protected final AsyncFilterContext filterCtx;
    protected final int masterTimestampColumnIndex;
    protected final long masterTimestampScale;
    // Per-query native memory tracker captured from the execution context in init() and
    // bound on the allocators and ASOF lookup maps in reopen(). Null when no per-query
    // limit applies, in which case the tracker-aware Unsafe overloads degrade to global-only.
    protected MemoryTracker memoryTracker;
    protected final long offsetCount;
    protected final long[] offsets;
    protected final GroupByAllocator ownerAllocator;
    protected final Map ownerAsOfJoinMap;
    protected final HorizonJoinRecord ownerCombinedRecord;
    protected final GroupByFunctionsUpdater ownerFunctionUpdater;
    protected final ObjList<GroupByFunction> ownerGroupByFunctions;
    // Per-worker horizon timestamp iterators for sorted processing
    protected final AsyncHorizonTimestampIterator ownerHorizonIterator;
    protected final RecordSink ownerMasterAsOfJoinMapSink;
    protected final RecordSink ownerSlaveAsOfJoinMapSink;
    protected final ConcurrentTimeFrameCursor ownerSlaveTimeFrameCursor;
    protected final HorizonJoinTimeFrameHelper ownerSlaveTimeFrameHelper;
    protected final SymbolTranslatingRecord ownerSymbolTranslatingRecord;
    protected final ObjList<GroupByAllocator> perWorkerAllocators;
    protected final ObjList<Map> perWorkerAsOfJoinMaps;
    protected final ObjList<HorizonJoinRecord> perWorkerCombinedRecords;
    protected final ObjList<GroupByFunctionsUpdater> perWorkerFunctionUpdaters;
    protected final ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions;
    protected final ObjList<AsyncHorizonTimestampIterator> perWorkerHorizonIterators;
    protected final PerWorkerLocks perWorkerLocks;
    protected final ObjList<RecordSink> perWorkerMasterAsOfJoinMapSinks;
    protected final ObjList<RecordSink> perWorkerSlaveAsOfJoinMapSinks;
    protected final ObjList<ConcurrentTimeFrameCursor> perWorkerSlaveTimeFrameCursors;
    protected final ObjList<HorizonJoinTimeFrameHelper> perWorkerSlaveTimeFrameHelpers;
    // Per-worker symbol translating records for integer-based symbol key comparison.
    // Null when there are no symbol columns in the ASOF join key.
    protected final ObjList<SymbolTranslatingRecord> perWorkerSymbolTranslatingRecords;
    private final HorizonJoinSymbolTableSource horizonJoinSymbolTableSource;

    protected BaseAsyncHorizonJoinAtom(
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull RecordCursorFactory slaveFactory,
            int masterTimestampColumnIndex,
            long @NotNull [] offsets,
            @Nullable ColumnTypes asOfJoinKeyTypes,
            @Nullable Class<RecordSink> masterAsOfJoinMapSinkClass,
            @Nullable Class<RecordSink> slaveAsOfJoinMapSinkClass,
            int masterColumnCount,
            int @Nullable [] masterSymbolKeyColumnIndices,
            int @Nullable [] slaveSymbolKeyColumnIndices,
            int @NotNull [] columnSources,
            int @NotNull [] columnIndexes,
            @NotNull ObjList<GroupByFunction> ownerGroupByFunctions,
            AsyncHorizonJoinResources resources,
            long masterTimestampScale,
            long slaveTsScale,
            int workerCount
    ) {
        assert slaveFactory.supportsTimeFrameCursor();
        assert resources.getPerWorkerFilters() == null || resources.getPerWorkerFilters().size() == workerCount;

        // Adopt the worker function views before configuration or allocation can fail. The
        // factory's transfer holder retains everything that this constructor has not adopted yet.
        this.ownerGroupByFunctions = ownerGroupByFunctions;
        this.perWorkerGroupByFunctions = resources.takePerWorkerGroupByFunctions();
        assert perWorkerGroupByFunctions == null || perWorkerGroupByFunctions.size() == workerCount;

        try {
            this.bwdScanAbsoluteThreshold = configuration.getSqlHorizonJoinBwdScanAbsoluteThreshold();
            this.bwdScanMinGap = configuration.getSqlHorizonJoinBwdScanMinGap();
            this.bwdScanSwitchFactor = configuration.getSqlHorizonJoinBwdScanSwitchFactor();
            this.masterTimestampColumnIndex = masterTimestampColumnIndex;
            this.offsets = offsets;
            this.offsetCount = offsets.length;
            this.horizonJoinSymbolTableSource = new HorizonJoinSymbolTableSource(columnSources, columnIndexes);

            // AsyncFilterContext adopts all filter and bind resources directly from the holder.
            this.filterCtx = new AsyncFilterContext(
                    configuration,
                    resources,
                    workerCount,
                    workerCount,
                    0L, // owner memory pool budget (single-buffer effective behavior)
                    0L  // per-worker memory pool budget
            );
            // Per-worker ASOF join map sinks (each worker needs its own sink for thread safety with DECIMAL types)
            if (masterAsOfJoinMapSinkClass != null || slaveAsOfJoinMapSinkClass != null) {
                this.ownerMasterAsOfJoinMapSink = RecordSinkFactory.getInstance(masterAsOfJoinMapSinkClass, null, null, null, null, null, null, null);
                this.ownerSlaveAsOfJoinMapSink = RecordSinkFactory.getInstance(slaveAsOfJoinMapSinkClass, null, null, null, null, null, null, null);
                this.perWorkerMasterAsOfJoinMapSinks = new ObjList<>(workerCount);
                this.perWorkerSlaveAsOfJoinMapSinks = new ObjList<>(workerCount);
                for (int i = 0; i < workerCount; i++) {
                    perWorkerMasterAsOfJoinMapSinks.add(RecordSinkFactory.getInstance(masterAsOfJoinMapSinkClass, null, null, null, null, null, null, null));
                    perWorkerSlaveAsOfJoinMapSinks.add(RecordSinkFactory.getInstance(slaveAsOfJoinMapSinkClass, null, null, null, null, null, null, null));
                }
            } else {
                this.ownerMasterAsOfJoinMapSink = null;
                this.ownerSlaveAsOfJoinMapSink = null;
                this.perWorkerMasterAsOfJoinMapSinks = null;
                this.perWorkerSlaveAsOfJoinMapSinks = null;
            }

            // Timestamp scale factor for cross-resolution support (1 if same type, otherwise scale to nanos)
            this.masterTimestampScale = masterTimestampScale;

            // Per-worker locks
            this.perWorkerLocks = new PerWorkerLocks(configuration, workerCount);

            // Create time frame cursors from slave factory - one per worker + owner
            final long lookahead = configuration.getSqlAsOfJoinLookAhead();
            this.ownerSlaveTimeFrameCursor = slaveFactory.newTimeFrameCursor();
            this.ownerSlaveTimeFrameHelper = new HorizonJoinTimeFrameHelper(
                    lookahead,
                    slaveTsScale,
                    bwdScanAbsoluteThreshold,
                    bwdScanMinGap,
                    bwdScanSwitchFactor
            );
            this.perWorkerSlaveTimeFrameCursors = new ObjList<>(workerCount);
            this.perWorkerSlaveTimeFrameHelpers = new ObjList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                perWorkerSlaveTimeFrameCursors.add(slaveFactory.newTimeFrameCursor());
                perWorkerSlaveTimeFrameHelpers.add(new HorizonJoinTimeFrameHelper(
                        lookahead,
                        slaveTsScale,
                        bwdScanAbsoluteThreshold,
                        bwdScanMinGap,
                        bwdScanSwitchFactor
                ));
            }

            // Per-worker ASOF maps and SingleRecordSink targets for key comparison.
            // openOnInit=false: the backing is allocated lazily by reopen() (in
            // initTimeFrameCursors()) under the per-query tracker bound in the atom's reopen(),
            // keeping malloc (reopen) and free (clear) symmetric on the per-query counter.
            if (asOfJoinKeyTypes != null) {
                this.perWorkerAsOfJoinMaps = new ObjList<>(workerCount);
                final SingleColumnType asOfValueTypes = new SingleColumnType(ColumnType.LONG);
                for (int i = 0; i < workerCount; i++) {
                    perWorkerAsOfJoinMaps.add(MapFactory.createUnorderedMap(configuration, asOfJoinKeyTypes, asOfValueTypes, false, false));
                }
                this.ownerAsOfJoinMap = MapFactory.createUnorderedMap(configuration, asOfJoinKeyTypes, asOfValueTypes, false, false);
            } else {
                this.perWorkerAsOfJoinMaps = null;
                this.ownerAsOfJoinMap = null;
            }

            // Per-worker symbol translating records for integer-based symbol key comparison
            if (masterSymbolKeyColumnIndices != null) {
                this.ownerSymbolTranslatingRecord = new SymbolTranslatingRecord(masterColumnCount, masterSymbolKeyColumnIndices, slaveSymbolKeyColumnIndices);
                this.perWorkerSymbolTranslatingRecords = new ObjList<>(workerCount);
                for (int i = 0; i < workerCount; i++) {
                    perWorkerSymbolTranslatingRecords.add(new SymbolTranslatingRecord(masterColumnCount, masterSymbolKeyColumnIndices, slaveSymbolKeyColumnIndices));
                }
            } else {
                this.ownerSymbolTranslatingRecord = null;
                this.perWorkerSymbolTranslatingRecords = null;
            }

            // Group by updaters
            final Class<? extends GroupByFunctionsUpdater> updaterClass = GroupByFunctionsUpdaterFactory.getInstanceClass(asm, ownerGroupByFunctions.size());
            this.ownerFunctionUpdater = GroupByFunctionsUpdaterFactory.getInstance(updaterClass, ownerGroupByFunctions);
            this.perWorkerFunctionUpdaters = new ObjList<>(workerCount);
            if (perWorkerGroupByFunctions != null) {
                for (int i = 0; i < workerCount; i++) {
                    perWorkerFunctionUpdaters.add(GroupByFunctionsUpdaterFactory.getInstance(updaterClass, perWorkerGroupByFunctions.getQuick(i)));
                }
            } else {
                for (int i = 0; i < workerCount; i++) {
                    perWorkerFunctionUpdaters.add(ownerFunctionUpdater);
                }
            }

            // Allocators. Lazy variant (openOnInit=false): the chunk index is global-counter
            // bookkeeping; only the data chunks it hands out are charged to the per-query tracker.
            this.ownerAllocator = GroupByAllocatorFactory.createAllocator(configuration, false);
            GroupByUtils.setAllocator(ownerGroupByFunctions, ownerAllocator);
            if (perWorkerGroupByFunctions != null) {
                this.perWorkerAllocators = new ObjList<>(workerCount);
                for (int i = 0; i < workerCount; i++) {
                    GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration, false);
                    perWorkerAllocators.add(allocator);
                    GroupByUtils.setAllocator(perWorkerGroupByFunctions.getQuick(i), allocator);
                }
            } else {
                perWorkerAllocators = null;
            }

            // Per-worker combined records
            this.ownerCombinedRecord = new HorizonJoinRecord();
            ownerCombinedRecord.init(columnSources, columnIndexes);
            this.perWorkerCombinedRecords = new ObjList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                HorizonJoinRecord record = new HorizonJoinRecord();
                record.init(columnSources, columnIndexes);
                perWorkerCombinedRecords.add(record);
            }

            // Per-worker horizon timestamp iterators for sorted processing
            this.ownerHorizonIterator = new AsyncHorizonTimestampIterator(offsets);
            this.perWorkerHorizonIterators = new ObjList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                perWorkerHorizonIterators.add(new AsyncHorizonTimestampIterator(offsets));
            }
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public void clear() {
        // Clear group by functions
        Misc.clearObjList(ownerGroupByFunctions);
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                PerWorkerFunctionList.clear(perWorkerGroupByFunctions.getQuick(i));
            }
        }
        Misc.clear(ownerAllocator);
        Misc.clearObjList(perWorkerAllocators);

        // Clear ASOF join maps
        Misc.free(ownerAsOfJoinMap);
        Misc.freeObjListAndKeepObjects(perWorkerAsOfJoinMaps);

        // Clear filter context (memory pools, etc.)
        filterCtx.clear();

        // Clear symbol translating records
        Misc.clear(ownerSymbolTranslatingRecord);
        Misc.clearObjList(perWorkerSymbolTranslatingRecords);

        // Clear time frame cursors
        Misc.free(ownerSlaveTimeFrameCursor);
        Misc.freeObjListAndKeepObjects(perWorkerSlaveTimeFrameCursors);

        // Let subclass clear its resources
        clearAggregationState();
        memoryTracker = null;
    }

    @Override
    public void close() {
        // clear() already freed the data chunks and ASOF maps under the bound tracker (the index
        // is on the global counter), so close() has nothing tracked to free. Nulling is defensive:
        // any stray free hits the global counter and cannot underflow an already-recycled block.
        if (ownerAllocator != null) {
            ownerAllocator.setMemoryTracker(null);
        }
        if (perWorkerAllocators != null) {
            for (int i = 0, n = perWorkerAllocators.size(); i < n; i++) {
                final GroupByAllocator allocator = perWorkerAllocators.getQuick(i);
                if (allocator != null) {
                    allocator.setMemoryTracker(null);
                }
            }
        }
        Throwable cleanupFailure = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerAllocator);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerAllocators);
        // ownerGroupByFunctions are freed by the owning factory via
        // recordFunctions/groupByFunctions field, so we only free per-worker clones here.
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                final ObjList<GroupByFunction> workerFunctions = perWorkerGroupByFunctions.getQuick(i);
                perWorkerGroupByFunctions.setQuick(i, null);
                try {
                    PerWorkerFunctionList.close(workerFunctions);
                } catch (Throwable th) {
                    if (cleanupFailure == null) {
                        cleanupFailure = th;
                    } else if (cleanupFailure != th) {
                        cleanupFailure.addSuppressed(th);
                    }
                }
            }
        }
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerAsOfJoinMap);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerAsOfJoinMaps);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerSlaveTimeFrameCursor);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerSlaveTimeFrameCursors);
        // Horizon timestamp iterators
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerHorizonIterator);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerHorizonIterators);
        // Filter and memory pool resources
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, filterCtx);
        // Symbol translating records
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerSymbolTranslatingRecord);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerSymbolTranslatingRecords);

        // Let subclass close its resources
        try {
            closeAggregationState();
        } catch (Throwable th) {
            if (cleanupFailure == null) {
                cleanupFailure = th;
            } else if (cleanupFailure != th) {
                cleanupFailure.addSuppressed(th);
            }
        }
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    public Map getAsOfJoinMap(int slotId) {
        if (slotId == -1) {
            return ownerAsOfJoinMap;
        }
        return perWorkerAsOfJoinMaps != null ? perWorkerAsOfJoinMaps.getQuick(slotId) : null;
    }

    public AsyncFilterContext getFilterContext() {
        return filterCtx;
    }

    public GroupByFunctionsUpdater getFunctionUpdater(int slotId) {
        if (slotId == -1) {
            return ownerFunctionUpdater;
        }
        return perWorkerFunctionUpdaters.getQuick(slotId);
    }

    /**
     * Get the horizon timestamp iterator for the given slot.
     * Used for sorted processing of horizon timestamps within a page frame.
     */
    public AsyncHorizonTimestampIterator getHorizonIterator(int slotId) {
        if (slotId == -1) {
            return ownerHorizonIterator;
        }
        return perWorkerHorizonIterators.getQuick(slotId);
    }

    public HorizonJoinRecord getHorizonJoinRecord(int slotId) {
        if (slotId == -1) {
            return ownerCombinedRecord;
        }
        return perWorkerCombinedRecords.getQuick(slotId);
    }

    public RecordSink getMasterAsOfJoinSink(int slotId) {
        if (slotId == -1) {
            return ownerMasterAsOfJoinMapSink;
        }
        return perWorkerMasterAsOfJoinMapSinks != null ? perWorkerMasterAsOfJoinMapSinks.getQuick(slotId) : null;
    }

    public Record getMasterKeyRecord(int slotId, Record masterRecord) {
        final SymbolTranslatingRecord translatingRecord = (slotId == -1)
                ? ownerSymbolTranslatingRecord
                : (perWorkerSymbolTranslatingRecords != null ? perWorkerSymbolTranslatingRecords.getQuick(slotId) : null);
        if (translatingRecord != null) {
            translatingRecord.of(masterRecord);
            return translatingRecord;
        }
        return masterRecord;
    }

    public int getMasterTimestampColumnIndex() {
        return masterTimestampColumnIndex;
    }

    public long getMasterTimestampScale() {
        return masterTimestampScale;
    }

    /**
     * Get the offset value at the given index. Offsets are in master's scale.
     */
    public long getOffset(int index) {
        return offsets[index];
    }

    public long getOffsetCount() {
        return offsetCount;
    }

    public ObjList<GroupByFunction> getOwnerGroupByFunctions() {
        return ownerGroupByFunctions;
    }

    @Override
    @TestOnly
    public PerWorkerLocks getPerWorkerLocks() {
        return perWorkerLocks;
    }

    public RecordSink getSlaveAsOfJoinMapSink(int slotId) {
        if (slotId == -1) {
            return ownerSlaveAsOfJoinMapSink;
        }
        return perWorkerSlaveAsOfJoinMapSinks != null ? perWorkerSlaveAsOfJoinMapSinks.getQuick(slotId) : null;
    }

    /**
     * Get the time frame helper for the given slot.
     */
    public HorizonJoinTimeFrameHelper getSlaveTimeFrameHelper(int slotId) {
        if (slotId == -1) {
            return ownerSlaveTimeFrameHelper;
        }
        return perWorkerSlaveTimeFrameHelpers.getQuick(slotId);
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        memoryTracker = executionContext.getMemoryTracker();
        filterCtx.initFilters(symbolTableSource, executionContext);
        // Note: group by functions are initialized in initTimeFrameCursors() where we have
        // access to both master and slave symbol table sources
    }

    /**
     * Initialize all time frame cursors with shared state.
     * Must be called after {@link ConcurrentTimeFrameState#of} has been called.
     */
    public void initTimeFrameCursors(
            SqlExecutionContext executionContext,
            SymbolTableSource masterSymbolTableSource,
            TablePageFrameCursor slavePageFrameCursor,
            ConcurrentTimeFrameState sharedState
    ) throws SqlException {
        // Initialize owner cursor
        final int timestampIndex = ownerSlaveTimeFrameCursor.getTimestampIndex();
        ownerSlaveTimeFrameCursor.of(sharedState, slavePageFrameCursor, timestampIndex);
        ownerSlaveTimeFrameCursor.setParquetDecodeHint(ParquetDecodeHint.MONOTONIC);
        ownerSlaveTimeFrameHelper.of(ownerSlaveTimeFrameCursor);

        // Initialize per-worker cursors with the same shared state
        for (int i = 0, n = perWorkerSlaveTimeFrameCursors.size(); i < n; i++) {
            final ConcurrentTimeFrameCursor workerCursor = perWorkerSlaveTimeFrameCursors.getQuick(i);
            workerCursor.of(sharedState, slavePageFrameCursor, timestampIndex);
            workerCursor.setParquetDecodeHint(ParquetDecodeHint.MONOTONIC);
            perWorkerSlaveTimeFrameHelpers.getQuick(i).of(workerCursor);
        }

        // Initialize group by functions with combined symbol table source
        horizonJoinSymbolTableSource.of(masterSymbolTableSource, slavePageFrameCursor);
        for (int i = 0, n = ownerGroupByFunctions.size(); i < n; i++) {
            ownerGroupByFunctions.getQuick(i).init(horizonJoinSymbolTableSource, executionContext);
        }
        if (perWorkerGroupByFunctions != null) {
            final boolean current = executionContext.getCloneSymbolTables();
            executionContext.setCloneSymbolTables(true);
            try {
                for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                    PerWorkerFunctionList.init(
                            perWorkerGroupByFunctions.getQuick(i),
                            ownerGroupByFunctions,
                            horizonJoinSymbolTableSource,
                            executionContext
                    );
                }
            } finally {
                executionContext.setCloneSymbolTables(current);
            }
        }

        // Reopen ASOF maps
        if (ownerAsOfJoinMap != null) {
            ownerAsOfJoinMap.reopen();
        }
        if (perWorkerAsOfJoinMaps != null) {
            for (int i = 0, n = perWorkerAsOfJoinMaps.size(); i < n; i++) {
                Map map = perWorkerAsOfJoinMaps.getQuick(i);
                if (map != null) {
                    map.reopen();
                }
            }
        }

        // Initialize symbol translating records with symbol table sources for lazy resolution
        if (ownerSymbolTranslatingRecord != null) {
            ownerSymbolTranslatingRecord.initSources(masterSymbolTableSource, slavePageFrameCursor);
            for (int i = 0, n = perWorkerSymbolTranslatingRecords.size(); i < n; i++) {
                perWorkerSymbolTranslatingRecords.getQuick(i).initSources(masterSymbolTableSource, slavePageFrameCursor);
            }
        }

    }

    public int maybeAcquire(int workerId, boolean owner, SqlExecutionCircuitBreaker circuitBreaker) {
        if (workerId == -1 && owner) {
            return -1;
        }
        return perWorkerLocks.acquireSlot(workerId, circuitBreaker);
    }

    public void release(int slotId) {
        perWorkerLocks.releaseSlot(slotId);
    }

    @Override
    public void reopen() {
        // init() runs before reopen() (frameSequence.of() -> atom.init(), then cursor.of()
        // -> atom.reopen()), so memoryTracker is available here to bind on the allocators and
        // ASOF lookup maps before any backing is allocated. The allocators' chunk index is
        // lazy (openOnInit=false) and reopened here; the ASOF maps are reopened later in
        // initTimeFrameCursors(). Both pick up the bound tracker so their growth counts
        // against the per-query limit.
        ownerAllocator.setMemoryTracker(memoryTracker);
        ownerAllocator.reopen();
        if (perWorkerAllocators != null) {
            for (int i = 0, n = perWorkerAllocators.size(); i < n; i++) {
                final GroupByAllocator allocator = perWorkerAllocators.getQuick(i);
                allocator.setMemoryTracker(memoryTracker);
                allocator.reopen();
            }
        }
        if (ownerAsOfJoinMap != null) {
            ownerAsOfJoinMap.setMemoryTracker(memoryTracker);
        }
        if (perWorkerAsOfJoinMaps != null) {
            for (int i = 0, n = perWorkerAsOfJoinMaps.size(); i < n; i++) {
                final Map map = perWorkerAsOfJoinMaps.getQuick(i);
                if (map != null) {
                    map.setMemoryTracker(memoryTracker);
                }
            }
        }
    }

    public void toTop() {
        if (perWorkerGroupByFunctions != null) {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                GroupByUtils.toTop(perWorkerGroupByFunctions.getQuick(i));
            }
        }
    }


    /**
     * Clear aggregation-specific state. Called by {@link #clear()}.
     */
    protected abstract void clearAggregationState();

    /**
     * Close aggregation-specific resources. Called by {@link #close()}.
     */
    protected abstract void closeAggregationState();

    // package-private to make linter happy
    HorizonJoinSymbolTableSource getSymbolTableSource() {
        return horizonJoinSymbolTableSource;
    }
}
