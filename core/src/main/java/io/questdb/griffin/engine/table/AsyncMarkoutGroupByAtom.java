/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordArray;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.MasterRowBatch;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * Atom that manages per-worker resources for parallel markout query execution.
 * <p>
 * This class holds:
 * 1. Shared (read-only) sequence record array (offset records from the horizons cursor)
 * 2. Per-worker price cursors for ASOF JOIN lookups
 * 3. Per-worker aggregation maps and group by functions
 * 4. Per-worker iterator block memory for the k-way merge algorithm
 */
public class AsyncMarkoutGroupByAtom implements StatefulAtom, Closeable, Reopenable, Plannable {
    // ASOF JOIN lookup resources (null if no join key)
    private final ColumnTypes asofJoinKeyTypes;
    private final CairoConfiguration configuration;
    private final RecordSink masterKeyCopier;
    private final RecordSink masterRecordSink;
    // Master row batch pool for reuse
    private final ObjList<MasterRowBatch> masterRowBatchPool;
    // Master row timestamp column index
    private final int masterTimestampColumnIndex;
    private final GroupByAllocator ownerAllocator;
    private final GroupByFunctionsUpdater ownerFunctionUpdater;
    private final ObjList<GroupByFunction> ownerGroupByFunctions;
    private final Map ownerMap;
    private final ObjList<GroupByAllocator> perWorkerAllocators;
    private final ObjList<Map> perWorkerAsofJoinMaps;                     // Per-slot ASOF maps
    private final ObjList<GroupByFunctionsUpdater> perWorkerFunctionUpdaters;
    private final ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions;
    // Per-worker resources
    private final PerWorkerLocks perWorkerLocks;
    private final ObjList<Map> perWorkerMaps;
    private final ObjList<RecordCursor> perWorkerPricesCursors;           // Per-slot cursors (lazily created)
    // Per-slot prices factories - each worker needs its own factory for independent cursors
    private final ObjList<RecordCursorFactory> pricesFactories;
    private final RecordSink pricesKeyCopier;
    private final int pricesTimestampIndex;
    private final int sequenceColumnIndex;
    // Shared (read-only) sequence data - materialized offset records
    private final RecordArray sequenceRecordArray;
    private final LongList sequenceRecordOffsets = new LongList();
    private long firstSequenceTimeOffset;
    private Map ownerAsofJoinMap;
    // Owner resources (for work stealing)
    private RecordCursor ownerPricesCursor;
    private long sequenceRowCount;
    // Stored execution context for lazy cursor creation
    private SqlExecutionContext storedExecutionContext;

    public AsyncMarkoutGroupByAtom(
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull ObjList<RecordCursorFactory> pricesFactories,
            @NotNull RecordCursorFactory sequenceCursorFactory,
            @NotNull RecordSink sequenceRecordSink,
            @NotNull RecordSink masterRecordSink,
            int masterTimestampColumnIndex,
            int sequenceColumnIndex,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            ColumnTypes asofJoinKeyTypes,
            RecordSink masterKeyCopier,
            RecordSink pricesKeyCopier,
            int pricesTimestampIndex,
            @NotNull ObjList<GroupByFunction> ownerGroupByFunctions,
            @NotNull ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            int workerCount
    ) {
        final int slotCount = Math.min(workerCount, configuration.getPageFrameReduceQueueCapacity());

        try {
            this.configuration = configuration;
            this.pricesFactories = pricesFactories;
            this.masterTimestampColumnIndex = masterTimestampColumnIndex;
            this.sequenceColumnIndex = sequenceColumnIndex;
            this.masterRecordSink = masterRecordSink;

            // Initialize ASOF join lookup resources
            this.asofJoinKeyTypes = asofJoinKeyTypes;
            this.masterKeyCopier = masterKeyCopier;
            this.pricesKeyCopier = pricesKeyCopier;
            this.pricesTimestampIndex = pricesTimestampIndex;

            // Initialize shared sequence record array
            this.sequenceRecordArray = new RecordArray(
                    sequenceCursorFactory.getMetadata(),
                    sequenceRecordSink,
                    configuration.getSqlHashJoinValuePageSize(),
                    configuration.getSqlHashJoinValueMaxPages()
            );

            // Initialize per-worker locks
            this.perWorkerLocks = new PerWorkerLocks(configuration, slotCount);

            // Initialize per-worker cursors (will be populated lazily)
            this.perWorkerPricesCursors = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerPricesCursors.add(null);
            }

            // Initialize per-worker ASOF maps (if join key exists)
            this.perWorkerAsofJoinMaps = new ObjList<>(slotCount);
            if (asofJoinKeyTypes != null) {
                ArrayColumnTypes asofValueTypes = new ArrayColumnTypes();
                asofValueTypes.add(io.questdb.cairo.ColumnType.LONG);
                for (int i = 0; i < slotCount; i++) {
                    perWorkerAsofJoinMaps.add(MapFactory.createUnorderedMap(configuration, asofJoinKeyTypes, asofValueTypes));
                }
                // Also create owner ASOF map
                this.ownerAsofJoinMap = MapFactory.createUnorderedMap(configuration, asofJoinKeyTypes, asofValueTypes);
            } else {
                for (int i = 0; i < slotCount; i++) {
                    perWorkerAsofJoinMaps.add(null);
                }
                this.ownerAsofJoinMap = null;
            }

            // Initialize per-worker maps
            this.perWorkerMaps = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerMaps.add(MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes));
            }

            // Initialize per-worker allocators and functions
            this.ownerGroupByFunctions = ownerGroupByFunctions;
            this.perWorkerGroupByFunctions = perWorkerGroupByFunctions;

            final Class<GroupByFunctionsUpdater> updaterClass = GroupByFunctionsUpdaterFactory.getInstanceClass(asm, ownerGroupByFunctions.size());
            this.ownerFunctionUpdater = GroupByFunctionsUpdaterFactory.getInstance(updaterClass, ownerGroupByFunctions);
            this.perWorkerFunctionUpdaters = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerFunctionUpdaters.add(GroupByFunctionsUpdaterFactory.getInstance(updaterClass, perWorkerGroupByFunctions.getQuick(i)));
            }

            this.ownerAllocator = GroupByAllocatorFactory.createAllocator(configuration);
            GroupByUtils.setAllocator(ownerGroupByFunctions, ownerAllocator);
            this.perWorkerAllocators = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration);
                perWorkerAllocators.add(allocator);
                GroupByUtils.setAllocator(perWorkerGroupByFunctions.getQuick(i), allocator);
            }

            // Initialize owner map
            this.ownerMap = MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes);

            // Initialize per-worker iterator block addresses
            // Per-worker iterator block memory addresses
            LongList perWorkerFirstIteratorBlockAddr = new LongList(slotCount);
            LongList perWorkerLastIteratorBlockAddr = new LongList(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerFirstIteratorBlockAddr.add(0);
                perWorkerLastIteratorBlockAddr.add(0);
            }

            // Initialize master row batch pool
            this.masterRowBatchPool = new ObjList<>(slotCount + 1);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        // Clear resources for potential reuse
        Misc.free(ownerMap);
        Misc.freeObjListAndKeepObjects(perWorkerMaps);
        Misc.clearObjList(ownerGroupByFunctions);
        for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
            Misc.clearObjList(perWorkerGroupByFunctions.getQuick(i));
        }
        Misc.free(ownerAllocator);
        Misc.freeObjListAndKeepObjects(perWorkerAllocators);
        // Clear per-slot ASOF join maps
        if (ownerAsofJoinMap != null) {
            ownerAsofJoinMap.clear();
        }
        for (int i = 0, n = perWorkerAsofJoinMaps.size(); i < n; i++) {
            Map asofMap = perWorkerAsofJoinMaps.getQuick(i);
            if (asofMap != null) {
                asofMap.clear();
            }
        }
        // Close per-slot prices cursors
        if (ownerPricesCursor != null) {
            ownerPricesCursor.close();
            ownerPricesCursor = null;
        }
        for (int i = 0, n = perWorkerPricesCursors.size(); i < n; i++) {
            RecordCursor cursor = perWorkerPricesCursors.getQuick(i);
            if (cursor != null) {
                cursor.close();
                perWorkerPricesCursors.setQuick(i, null);
            }
        }
        // Release factories to free memory - cursor won't be reopened after close
        for (int i = 0, n = pricesFactories.size(); i < n; i++) {
            Misc.free(pricesFactories.getQuick(i));
        }
        pricesFactories.clear();
    }

    @Override
    public void close() {
        Misc.free(sequenceRecordArray);
        Misc.free(ownerMap);
        Misc.freeObjList(perWorkerMaps);
        Misc.free(ownerAllocator);
        Misc.freeObjList(perWorkerAllocators);
        Misc.freeObjList(ownerGroupByFunctions);
        for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
            Misc.freeObjList(perWorkerGroupByFunctions.getQuick(i));
        }
        // Free per-slot ASOF maps
        Misc.free(ownerAsofJoinMap);
        Misc.freeObjList(perWorkerAsofJoinMaps);
        // Free per-slot prices cursors (must close each cursor)
        for (int i = 0, n = perWorkerPricesCursors.size(); i < n; i++) {
            Misc.free(perWorkerPricesCursors.getQuick(i));
        }
        perWorkerPricesCursors.clear();
        Misc.free(ownerPricesCursor);
        // Close any remaining prices factories (may have been cleared by clear())
        for (int i = 0, n = pricesFactories.size(); i < n; i++) {
            Misc.free(pricesFactories.getQuick(i));
        }
        pricesFactories.clear();
        Misc.freeObjList(masterRowBatchPool);
    }

    /**
     * Get the ASOF join map for the given slot.
     * Each worker has its own map to avoid contention.
     */
    public Map getAsofJoinMap(int slotId) {
        if (slotId == -1) {
            return ownerAsofJoinMap;
        }
        return perWorkerAsofJoinMaps.getQuick(slotId);
    }

    public CairoConfiguration getConfiguration() {
        return configuration;
    }

    public long getFirstSequenceTimeOffset() {
        return firstSequenceTimeOffset;
    }

    /**
     * Get the function updater for the given slot.
     */
    public GroupByFunctionsUpdater getFunctionUpdater(int slotId) {
        if (slotId == -1) {
            return ownerFunctionUpdater;
        }
        return perWorkerFunctionUpdaters.getQuick(slotId);
    }

    /**
     * Get the aggregation map for the given slot.
     */
    public Map getMap(int slotId) {
        if (slotId == -1) {
            return ownerMap;
        }
        return perWorkerMaps.getQuick(slotId);
    }

    public RecordSink getMasterKeyCopier() {
        return masterKeyCopier;
    }

    public RecordSink getMasterRecordSink() {
        return masterRecordSink;
    }

    public int getMasterTimestampColumnIndex() {
        return masterTimestampColumnIndex;
    }

    /**
     * Get the prices cursor for the given slot.
     * Creates the cursor lazily from the per-slot factory.
     * Workers should reuse the cursor by calling toTop() between batches.
     */
    public RecordCursor getPricesCursor(int slotId) throws SqlException {
        if (slotId == -1) {
            if (ownerPricesCursor == null) {
                // Owner uses the last factory in the list (index = slotCount)
                int ownerFactoryIndex = pricesFactories.size() - 1;
                ownerPricesCursor = pricesFactories.getQuick(ownerFactoryIndex).getCursor(storedExecutionContext);
            }
            return ownerPricesCursor;
        }
        RecordCursor cursor = perWorkerPricesCursors.getQuick(slotId);
        if (cursor == null) {
            cursor = pricesFactories.getQuick(slotId).getCursor(storedExecutionContext);
            perWorkerPricesCursors.setQuick(slotId, cursor);
        }
        return cursor;
    }

    public RecordSink getPricesKeyCopier() {
        return pricesKeyCopier;
    }

    public int getPricesTimestampIndex() {
        return pricesTimestampIndex;
    }

    public int getSequenceColumnIndex() {
        return sequenceColumnIndex;
    }

    // Shared sequence data accessors
    public RecordArray getSequenceRecordArray() {
        return sequenceRecordArray;
    }

    public LongList getSequenceRecordOffsets() {
        return sequenceRecordOffsets;
    }

    public long getSequenceRowCount() {
        return sequenceRowCount;
    }

    // ASOF join lookup accessors

    public boolean hasAsofJoinKey() {
        return asofJoinKeyTypes != null;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        // Initialize group by functions
        for (int i = 0, n = ownerGroupByFunctions.size(); i < n; i++) {
            ownerGroupByFunctions.getQuick(i).init(symbolTableSource, executionContext);
        }
        final boolean current = executionContext.getCloneSymbolTables();
        executionContext.setCloneSymbolTables(true);
        try {
            for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
                ObjList<GroupByFunction> functions = perWorkerGroupByFunctions.getQuick(i);
                for (int j = 0, m = functions.size(); j < m; j++) {
                    functions.getQuick(j).init(symbolTableSource, executionContext);
                }
            }
        } finally {
            executionContext.setCloneSymbolTables(current);
        }
    }

    /**
     * Store the execution context for lazy cursor creation.
     * Must be called from the main thread before dispatching tasks.
     */
    public void initPricesCursors(SqlExecutionContext executionContext) throws SqlException {
        this.storedExecutionContext = executionContext;
        // Cursors will be created lazily in getPricesCursor(slotId)
    }

    /**
     * Materialize the sequence cursor (offset records) into the shared RecordArray.
     * Must be called once before dispatching tasks.
     */
    public void materializeSequenceCursor(RecordCursor sequenceCursor, SqlExecutionCircuitBreaker circuitBreaker) {
        sequenceRecordArray.clear();
        sequenceRecordOffsets.clear();

        Record sequenceRecord = sequenceCursor.getRecord();
        while (sequenceCursor.hasNext()) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            long offset = sequenceRecordArray.put(sequenceRecord);
            sequenceRecordOffsets.add(offset);
        }
        sequenceRecordArray.toTop();
        sequenceRowCount = sequenceRecordOffsets.size();

        // Cache the first sequence's offset value
        if (sequenceRowCount > 0) {
            long firstSequenceRecordOffset = sequenceRecordOffsets.getQuick(0);
            Record firstSequenceRecord = sequenceRecordArray.getRecordAt(firstSequenceRecordOffset);
            firstSequenceTimeOffset = firstSequenceRecord.getLong(sequenceColumnIndex);
        } else {
            firstSequenceTimeOffset = 0;
        }
    }

    /**
     * Acquire a slot for parallel execution.
     */
    public int maybeAcquire(int workerId, boolean owner, SqlExecutionCircuitBreaker circuitBreaker) {
        if (workerId == -1 && owner) {
            return -1;
        }
        return perWorkerLocks.acquireSlot(workerId, circuitBreaker);
    }

    /**
     * Merge all per-worker maps into the owner map.
     */
    public Map mergeWorkerMaps() {
        Map destMap = ownerMap;
        if (!destMap.isOpen()) {
            destMap.reopen();
        }
        GroupByFunctionsUpdater functionUpdater = ownerFunctionUpdater;

        for (int i = 0, n = perWorkerMaps.size(); i < n; i++) {
            Map srcMap = perWorkerMaps.getQuick(i);
            if (srcMap.isOpen() && srcMap.size() > 0) {
                destMap.merge(srcMap, functionUpdater);
                srcMap.close();
            }
        }

        return destMap;
    }

    /**
     * Release a slot after parallel execution.
     */
    public void release(int slotId) {
        perWorkerLocks.releaseSlot(slotId);
    }

    @Override
    public void reopen() {
        // Maps will be opened lazily by worker threads
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("AsyncMarkoutGroupByAtom");
    }
}
