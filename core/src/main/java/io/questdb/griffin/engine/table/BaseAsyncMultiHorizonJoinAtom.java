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
 * Base class for multi-slave HORIZON JOIN atoms that manages per-worker x per-slave resources.
 * <p>
 * This class holds:
 * 1. Per-worker x per-slave time frame helpers for ASOF JOIN lookups via ConcurrentTimeFrameCursor
 * 2. Per-worker group by functions and updaters (shared across slaves)
 * 3. Per-worker x per-slave ASOF join maps for symbol -> rowId mappings (when keyed join)
 * 4. Per-worker x per-slave RecordSinks (RecordSink instances have mutable state and must not be shared)
 * 5. Filter resources (compiled and Java filters, shared across slaves)
 */
public abstract class BaseAsyncMultiHorizonJoinAtom implements StatefulAtom, PerWorkerLockOwner, Closeable, Reopenable, Plannable {
    protected final long bwdScanAbsoluteThreshold;
    protected final long bwdScanMinGap;
    protected final long bwdScanSwitchFactor;
    protected final AsyncFilterContext filterCtx;
    protected final int masterTimestampColumnIndex;
    // Per-query native memory tracker captured from the execution context in init() and
    // bound on the allocators and ASOF lookup maps in reopen(). Null when no per-query
    // limit applies, in which case the tracker-aware Unsafe overloads degrade to global-only.
    protected MemoryTracker memoryTracker;
    protected final long offsetCount;
    protected final long[] offsets;
    protected final GroupByAllocator ownerAllocator;
    protected final ObjList<Map> ownerAsOfJoinMaps;
    protected final MultiHorizonJoinRecord ownerCombinedRecord;
    protected final GroupByFunctionsUpdater ownerFunctionUpdater;
    protected final ObjList<GroupByFunction> ownerGroupByFunctions;
    protected final AsyncHorizonTimestampIterator ownerHorizonIterator;
    protected final ObjList<RecordSink> ownerMasterAsOfJoinSinks;
    protected final ObjList<RecordSink> ownerSlaveAsOfJoinSinks;
    protected final ObjList<ConcurrentTimeFrameCursor> ownerSlaveTimeFrameCursors;
    protected final ObjList<HorizonJoinTimeFrameHelper> ownerSlaveTimeFrameHelpers;
    protected final ObjList<SymbolTranslatingRecord> ownerSymbolTranslatingRecords;
    protected final long[] perSlaveMasterTsScales;
    protected final ObjList<GroupByAllocator> perWorkerAllocators;
    protected final ObjList<Map> perWorkerAsOfJoinMaps;
    protected final ObjList<MultiHorizonJoinRecord> perWorkerCombinedRecords;
    protected final ObjList<GroupByFunctionsUpdater> perWorkerFunctionUpdaters;
    protected final ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions;
    protected final ObjList<AsyncHorizonTimestampIterator> perWorkerHorizonIterators;
    protected final PerWorkerLocks perWorkerLocks;
    protected final ObjList<ObjList<RecordSink>> perWorkerMasterAsOfJoinSinks;
    protected final ObjList<ObjList<RecordSink>> perWorkerSlaveAsOfJoinSinks;
    protected final ObjList<ConcurrentTimeFrameCursor> perWorkerSlaveTimeFrameCursors;
    protected final ObjList<HorizonJoinTimeFrameHelper> perWorkerSlaveTimeFrameHelpers;
    protected final ObjList<SymbolTranslatingRecord> perWorkerSymbolTranslatingRecords;
    protected final int slaveCount;
    protected final int workerCount;
    private final MultiHorizonJoinSymbolTableSource horizonJoinSymbolTableSource;
    // Pre-allocated per-worker lists for matched slave records (avoids allocations on the data path)
    private final ObjList<Record> ownerMatchedSlaveRecords;
    private final ObjList<ObjList<Record>> perWorkerMatchedSlaveRecords;

    protected BaseAsyncMultiHorizonJoinAtom(
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull ObjList<HorizonJoinSlaveState> slaveStates,
            @Nullable ColumnTypes[] perSlaveAsOfJoinKeyTypes,
            @Nullable Class<RecordSink> @NotNull [] masterAsOfJoinMapSinkClasses,
            @Nullable Class<RecordSink> @NotNull [] slaveAsOfJoinMapSinkClasses,
            int masterTimestampColumnIndex,
            long @NotNull [] offsets,
            int @NotNull [] columnSources,
            int @NotNull [] columnIndexes,
            @NotNull ObjList<GroupByFunction> ownerGroupByFunctions,
            AsyncHorizonJoinResources resources,
            int workerCount
    ) {
        assert slaveStates.size() > 0;
        assert resources.getPerWorkerFilters() == null || resources.getPerWorkerFilters().size() == workerCount;

        // Adopt the worker function views before configuration or allocation can fail. The
        // factory's transfer holder retains everything that this constructor has not adopted yet.
        this.ownerGroupByFunctions = ownerGroupByFunctions;
        this.perWorkerGroupByFunctions = resources.takePerWorkerGroupByFunctions();
        assert perWorkerGroupByFunctions == null || perWorkerGroupByFunctions.size() == workerCount;

        try {
            this.slaveCount = slaveStates.size();
            this.workerCount = workerCount;
            this.masterTimestampColumnIndex = masterTimestampColumnIndex;
            this.offsets = offsets;
            this.offsetCount = offsets.length;
            this.bwdScanAbsoluteThreshold = configuration.getSqlHorizonJoinBwdScanAbsoluteThreshold();
            this.bwdScanMinGap = configuration.getSqlHorizonJoinBwdScanMinGap();
            this.bwdScanSwitchFactor = configuration.getSqlHorizonJoinBwdScanSwitchFactor();
            this.horizonJoinSymbolTableSource = new MultiHorizonJoinSymbolTableSource(columnSources, columnIndexes, slaveCount);

            // AsyncFilterContext adopts all filter and bind resources directly from the holder.
            this.filterCtx = new AsyncFilterContext(
                    configuration,
                    resources,
                    workerCount,
                    workerCount,
                    0L, // owner memory pool budget (single-buffer effective behavior)
                    0L  // per-worker memory pool budget
            );
            // Per-worker locks
            this.perWorkerLocks = new PerWorkerLocks(configuration, workerCount);

            // Per-slave master timestamp scales and per-worker sinks
            this.perSlaveMasterTsScales = new long[slaveCount];
            for (int s = 0; s < slaveCount; s++) {
                perSlaveMasterTsScales[s] = slaveStates.getQuick(s).getMasterTsScale();
            }
            this.ownerMasterAsOfJoinSinks = new ObjList<>(slaveCount);
            this.ownerSlaveAsOfJoinSinks = new ObjList<>(slaveCount);
            this.perWorkerMasterAsOfJoinSinks = new ObjList<>(workerCount);
            this.perWorkerSlaveAsOfJoinSinks = new ObjList<>(workerCount);
            for (int w = 0; w < workerCount; w++) {
                perWorkerMasterAsOfJoinSinks.add(new ObjList<>(slaveCount));
                perWorkerSlaveAsOfJoinSinks.add(new ObjList<>(slaveCount));
            }
            for (int s = 0; s < slaveCount; s++) {
                if (masterAsOfJoinMapSinkClasses[s] != null) {
                    ownerMasterAsOfJoinSinks.add(RecordSinkFactory.getInstance(masterAsOfJoinMapSinkClasses[s], null, null, null, null, null, null, null));
                    ownerSlaveAsOfJoinSinks.add(RecordSinkFactory.getInstance(slaveAsOfJoinMapSinkClasses[s], null, null, null, null, null, null, null));
                    for (int w = 0; w < workerCount; w++) {
                        perWorkerMasterAsOfJoinSinks.getQuick(w).add(RecordSinkFactory.getInstance(masterAsOfJoinMapSinkClasses[s], null, null, null, null, null, null, null));
                        perWorkerSlaveAsOfJoinSinks.getQuick(w).add(RecordSinkFactory.getInstance(slaveAsOfJoinMapSinkClasses[s], null, null, null, null, null, null, null));
                    }
                } else {
                    ownerMasterAsOfJoinSinks.add(null);
                    ownerSlaveAsOfJoinSinks.add(null);
                    for (int w = 0; w < workerCount; w++) {
                        perWorkerMasterAsOfJoinSinks.getQuick(w).add(null);
                        perWorkerSlaveAsOfJoinSinks.getQuick(w).add(null);
                    }
                }
            }

            // Create time frame cursors from slave factories - one per worker + owner per slave
            final long lookahead = configuration.getSqlAsOfJoinLookAhead();
            this.ownerSlaveTimeFrameCursors = new ObjList<>(slaveCount);
            this.ownerSlaveTimeFrameHelpers = new ObjList<>(slaveCount);
            this.perWorkerSlaveTimeFrameCursors = new ObjList<>(workerCount * slaveCount);
            this.perWorkerSlaveTimeFrameHelpers = new ObjList<>(workerCount * slaveCount);
            for (int s = 0; s < slaveCount; s++) {
                HorizonJoinSlaveState state = slaveStates.getQuick(s);
                ownerSlaveTimeFrameCursors.add(state.getFactory().newTimeFrameCursor());
                ownerSlaveTimeFrameHelpers.add(new HorizonJoinTimeFrameHelper(
                        lookahead,
                        state.getSlaveTsScale(),
                        bwdScanAbsoluteThreshold,
                        bwdScanMinGap,
                        bwdScanSwitchFactor
                ));
            }
            // Per-worker flat lists use worker-major order so that element at
            // index (slotId * slaveCount + slaveIndex) belongs to the correct worker+slave pair.
            for (int w = 0; w < workerCount; w++) {
                for (int s = 0; s < slaveCount; s++) {
                    HorizonJoinSlaveState state = slaveStates.getQuick(s);
                    perWorkerSlaveTimeFrameCursors.add(state.getFactory().newTimeFrameCursor());
                    perWorkerSlaveTimeFrameHelpers.add(new HorizonJoinTimeFrameHelper(
                            lookahead,
                            state.getSlaveTsScale(),
                            bwdScanAbsoluteThreshold,
                            bwdScanMinGap,
                            bwdScanSwitchFactor
                    ));
                }
            }

            // Per-worker x per-slave ASOF maps.
            // openOnInit=false: the backing is allocated lazily by reopen() (in
            // initSlaveTimeFrameCursors()) under the per-query tracker bound in the atom's
            // reopen(), keeping malloc (reopen) and free (clear) symmetric on the per-query counter.
            final SingleColumnType asOfValueTypes = new SingleColumnType(ColumnType.LONG);
            this.ownerAsOfJoinMaps = new ObjList<>(slaveCount);
            this.perWorkerAsOfJoinMaps = new ObjList<>(workerCount * slaveCount);
            for (int s = 0; s < slaveCount; s++) {
                if (perSlaveAsOfJoinKeyTypes != null && perSlaveAsOfJoinKeyTypes[s] != null) {
                    ownerAsOfJoinMaps.add(MapFactory.createUnorderedMap(configuration, perSlaveAsOfJoinKeyTypes[s], asOfValueTypes, false, false));
                } else {
                    ownerAsOfJoinMaps.add(null);
                }
            }
            // Worker-major order: index = slotId * slaveCount + slaveIndex
            for (int w = 0; w < workerCount; w++) {
                for (int s = 0; s < slaveCount; s++) {
                    if (perSlaveAsOfJoinKeyTypes != null && perSlaveAsOfJoinKeyTypes[s] != null) {
                        perWorkerAsOfJoinMaps.add(MapFactory.createUnorderedMap(configuration, perSlaveAsOfJoinKeyTypes[s], asOfValueTypes, false, false));
                    } else {
                        perWorkerAsOfJoinMaps.add(null);
                    }
                }
            }

            // Per-worker x per-slave symbol translating records
            this.ownerSymbolTranslatingRecords = new ObjList<>(slaveCount);
            this.perWorkerSymbolTranslatingRecords = new ObjList<>(workerCount * slaveCount);
            for (int s = 0; s < slaveCount; s++) {
                HorizonJoinSlaveState state = slaveStates.getQuick(s);
                if (state.getMasterSymbolKeyColumnIndices() != null) {
                    ownerSymbolTranslatingRecords.add(new SymbolTranslatingRecord(
                            state.getMasterColumnCount(),
                            state.getMasterSymbolKeyColumnIndices(),
                            state.getSlaveSymbolKeyColumnIndices()
                    ));
                } else {
                    ownerSymbolTranslatingRecords.add(null);
                }
            }
            // Worker-major order: index = slotId * slaveCount + slaveIndex
            for (int w = 0; w < workerCount; w++) {
                for (int s = 0; s < slaveCount; s++) {
                    HorizonJoinSlaveState state = slaveStates.getQuick(s);
                    if (state.getMasterSymbolKeyColumnIndices() != null) {
                        perWorkerSymbolTranslatingRecords.add(new SymbolTranslatingRecord(
                                state.getMasterColumnCount(),
                                state.getMasterSymbolKeyColumnIndices(),
                                state.getSlaveSymbolKeyColumnIndices()
                        ));
                    } else {
                        perWorkerSymbolTranslatingRecords.add(null);
                    }
                }
            }

            // Group by updaters (shared across slaves)
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

            // Allocators (shared across slaves). Lazy variant (openOnInit=false): the chunk index
            // is global-counter bookkeeping; only the data chunks are charged to the per-query tracker.
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

            // Per-worker combined records (shared across slaves)
            this.ownerCombinedRecord = new MultiHorizonJoinRecord(slaveCount);
            ownerCombinedRecord.init(columnSources, columnIndexes);
            this.perWorkerCombinedRecords = new ObjList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                MultiHorizonJoinRecord record = new MultiHorizonJoinRecord(slaveCount);
                record.init(columnSources, columnIndexes);
                perWorkerCombinedRecords.add(record);
            }

            // Per-worker matched slave record lists (shared across slaves, avoids per-frame allocation)
            this.ownerMatchedSlaveRecords = new ObjList<>(slaveCount);
            ownerMatchedSlaveRecords.setPos(slaveCount);
            this.perWorkerMatchedSlaveRecords = new ObjList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                ObjList<Record> list = new ObjList<>(slaveCount);
                list.setPos(slaveCount);
                perWorkerMatchedSlaveRecords.add(list);
            }

            // Per-worker horizon timestamp iterators (shared across slaves)
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

        // Clear ASOF join maps (per-slave)
        Misc.freeObjListAndKeepObjects(ownerAsOfJoinMaps);
        Misc.freeObjListAndKeepObjects(perWorkerAsOfJoinMaps);

        // Clear filter context (memory pools, etc.)
        filterCtx.clear();

        // Clear symbol translating records (per-slave)
        Misc.clearObjList(ownerSymbolTranslatingRecords);
        Misc.clearObjList(perWorkerSymbolTranslatingRecords);

        // Clear time frame cursors (per-slave)
        Misc.freeObjListAndKeepObjects(ownerSlaveTimeFrameCursors);
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
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, ownerAsOfJoinMaps);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerAsOfJoinMaps);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, ownerSlaveTimeFrameCursors);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerSlaveTimeFrameCursors);
        // Horizon timestamp iterators
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, ownerHorizonIterator);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, perWorkerHorizonIterators);
        // Filter and memory pool resources
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, filterCtx);
        // Symbol translating records (per-slave)
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, ownerSymbolTranslatingRecords);
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

    public Map getAsOfJoinMap(int slotId, int slaveIndex) {
        if (slotId == -1) {
            return ownerAsOfJoinMaps.getQuick(slaveIndex);
        }
        return perWorkerAsOfJoinMaps.getQuick(slotId * slaveCount + slaveIndex);
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

    public MultiHorizonJoinRecord getHorizonJoinRecord(int slotId) {
        if (slotId == -1) {
            return ownerCombinedRecord;
        }
        return perWorkerCombinedRecords.getQuick(slotId);
    }

    public RecordSink getMasterAsOfJoinSink(int slotId, int slaveIndex) {
        if (slotId == -1) {
            return ownerMasterAsOfJoinSinks.getQuick(slaveIndex);
        }
        return perWorkerMasterAsOfJoinSinks.getQuick(slotId).getQuick(slaveIndex);
    }

    public Record getMasterKeyRecord(int slotId, int slaveIndex, Record masterRecord) {
        final SymbolTranslatingRecord translatingRecord;
        if (slotId == -1) {
            translatingRecord = ownerSymbolTranslatingRecords.getQuick(slaveIndex);
        } else {
            translatingRecord = perWorkerSymbolTranslatingRecords.getQuick(slotId * slaveCount + slaveIndex);
        }
        if (translatingRecord != null) {
            translatingRecord.of(masterRecord);
            return translatingRecord;
        }
        return masterRecord;
    }

    public int getMasterTimestampColumnIndex() {
        return masterTimestampColumnIndex;
    }

    public long getMasterTimestampScale(int slaveIndex) {
        return perSlaveMasterTsScales[slaveIndex];
    }

    public ObjList<Record> getMatchedSlaveRecords(int slotId) {
        if (slotId == -1) {
            return ownerMatchedSlaveRecords;
        }
        return perWorkerMatchedSlaveRecords.getQuick(slotId);
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

    public RecordSink getSlaveAsOfJoinMapSink(int slotId, int slaveIndex) {
        if (slotId == -1) {
            return ownerSlaveAsOfJoinSinks.getQuick(slaveIndex);
        }
        return perWorkerSlaveAsOfJoinSinks.getQuick(slotId).getQuick(slaveIndex);
    }

    public int getSlaveCount() {
        return slaveCount;
    }

    /**
     * Get the time frame helper for the given slot and slave.
     */
    public HorizonJoinTimeFrameHelper getSlaveTimeFrameHelper(int slotId, int slaveIndex) {
        if (slotId == -1) {
            return ownerSlaveTimeFrameHelpers.getQuick(slaveIndex);
        }
        return perWorkerSlaveTimeFrameHelpers.getQuick(slotId * slaveCount + slaveIndex);
    }

    public MultiHorizonJoinSymbolTableSource getSymbolTableSource() {
        return horizonJoinSymbolTableSource;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        memoryTracker = executionContext.getMemoryTracker();
        filterCtx.initFilters(symbolTableSource, executionContext);
        // Note: group by functions are initialized in initGroupByFunctions() where we have
        // access to both master and slave symbol table sources
    }

    /**
     * Initialize group by functions with combined symbol table source from all slaves.
     * Must be called after all slaves have been initialized via initSlaveTimeFrameCursors.
     */
    public void initGroupByFunctions(
            SqlExecutionContext executionContext,
            SymbolTableSource masterSource,
            ObjList<SymbolTableSource> slaveSources
    ) throws SqlException {
        horizonJoinSymbolTableSource.of(masterSource, slaveSources);
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
    }

    /**
     * Initialize time frame cursors for a single slave with shared state.
     * Must be called after {@link ConcurrentTimeFrameState#of} has been called.
     */
    public void initSlaveTimeFrameCursors(
            int slaveIndex,
            SymbolTableSource masterSymbolTableSource,
            TablePageFrameCursor slavePageFrameCursor,
            ConcurrentTimeFrameState sharedState
    ) throws SqlException {
        // Initialize owner cursor for this slave. Each owner and per-worker pool is sized to the
        // configured budget, so a fan-out over a parquet slave would multiply peak RSS by the pool
        // count (here workerCount * slaveCount); MONOTONIC caps each effective budget to a quarter.
        int tsIndex = ownerSlaveTimeFrameCursors.getQuick(slaveIndex).getTimestampIndex();
        ownerSlaveTimeFrameCursors.getQuick(slaveIndex).of(sharedState, slavePageFrameCursor, tsIndex);
        ownerSlaveTimeFrameCursors.getQuick(slaveIndex).setParquetDecodeHint(ParquetDecodeHint.MONOTONIC);
        ownerSlaveTimeFrameHelpers.getQuick(slaveIndex).of(ownerSlaveTimeFrameCursors.getQuick(slaveIndex));

        // Initialize per-worker cursors for this slave
        for (int w = 0; w < workerCount; w++) {
            int idx = w * slaveCount + slaveIndex;
            ConcurrentTimeFrameCursor c = perWorkerSlaveTimeFrameCursors.getQuick(idx);
            c.of(sharedState, slavePageFrameCursor, tsIndex);
            c.setParquetDecodeHint(ParquetDecodeHint.MONOTONIC);
            perWorkerSlaveTimeFrameHelpers.getQuick(idx).of(c);
        }

        // Initialize symbol translating records for this slave
        if (ownerSymbolTranslatingRecords.getQuick(slaveIndex) != null) {
            ownerSymbolTranslatingRecords.getQuick(slaveIndex).initSources(masterSymbolTableSource, slavePageFrameCursor);
            for (int w = 0; w < workerCount; w++) {
                SymbolTranslatingRecord r = perWorkerSymbolTranslatingRecords.getQuick(w * slaveCount + slaveIndex);
                if (r != null) {
                    r.initSources(masterSymbolTableSource, slavePageFrameCursor);
                }
            }
        }

        // Reopen ASOF maps for this slave
        if (ownerAsOfJoinMaps.getQuick(slaveIndex) != null) {
            ownerAsOfJoinMaps.getQuick(slaveIndex).reopen();
        }
        for (int w = 0; w < workerCount; w++) {
            Map m = perWorkerAsOfJoinMaps.getQuick(w * slaveCount + slaveIndex);
            if (m != null) {
                m.reopen();
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
        // per-slave ASOF lookup maps before any backing is allocated. The allocators' chunk
        // index is lazy (openOnInit=false) and reopened here; the ASOF maps are reopened later
        // in initSlaveTimeFrameCursors(). Both pick up the bound tracker so their growth counts
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
        if (ownerAsOfJoinMaps != null) {
            for (int i = 0, n = ownerAsOfJoinMaps.size(); i < n; i++) {
                final Map map = ownerAsOfJoinMaps.getQuick(i);
                if (map != null) {
                    map.setMemoryTracker(memoryTracker);
                }
            }
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
}
