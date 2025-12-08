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
import io.questdb.cairo.sql.RecordMetadata;
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
 * 2. Per-worker slave cursors for ASOF JOIN lookups
 * 3. Per-worker aggregation maps and group by functions
 * 4. Per-worker iterator block memory for the k-way merge algorithm
 */
public class AsyncMarkoutGroupByAtom implements StatefulAtom, Closeable, Reopenable, Plannable {
    private final int[] columnIndices;
    private final int[] columnSources;
    private final CairoConfiguration configuration;
    private final RecordSink groupByKeyCopier;
    private final RecordSink masterKeyCopier;
    private final RecordSink masterRecordSink;
    private final ObjList<MasterRowBatch> masterRowBatchPool;
    private final int masterTimestampColumnIndex;
    private final GroupByAllocator ownerAllocator;
    private final Map ownerAsofJoinMap;
    private final GroupByFunctionsUpdater ownerFunctionUpdater;
    private final ObjList<GroupByFunction> ownerGroupByFunctions;
    private final Map ownerMap;
    private final ObjList<GroupByAllocator> perWorkerAllocators;
    private final ObjList<Map> perWorkerAsofJoinMaps;
    private final ObjList<GroupByFunctionsUpdater> perWorkerFunctionUpdaters;
    private final ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions;
    private final PerWorkerLocks perWorkerLocks;
    private final ObjList<Map> perWorkerMaps;
    private final ObjList<RecordCursor> perWorkerSlaveCursors;
    private final int sequenceColumnIndex;
    private final RecordMetadata sequenceMetadata;
    private final LongList sequenceRecordOffsets = new LongList();
    private final RecordSink sequenceRecordSink;
    // Per-worker slave factories - each worker needs its own factory for independent cursors
    private final ObjList<RecordCursorFactory> slaveFactories;
    private final RecordSink slaveKeyCopier;
    private final int slaveTimestampIndex;
    private long firstSequenceTimeOffset;
    // Owner resources (for work stealing)
    private RecordCursor ownerSlaveCursor;
    private RecordArray sequenceRecordArray;
    private long sequenceRowCount;
    // Stored execution context for lazy cursor creation
    private SqlExecutionContext storedExecutionContext;

    public AsyncMarkoutGroupByAtom(
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull ObjList<RecordCursorFactory> slaveFactories,
            @NotNull RecordCursorFactory sequenceCursorFactory,
            @NotNull RecordSink sequenceRecordSink,
            @NotNull RecordSink masterRecordSink,
            int masterTimestampColumnIndex,
            int sequenceColumnIndex,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            ColumnTypes asofJoinKeyTypes,
            RecordSink masterKeyCopier,
            RecordSink slaveKeyCopier,
            int slaveTimestampIndex,
            @NotNull RecordSink groupByKeyCopier,
            @NotNull int[] columnSources,
            @NotNull int[] columnIndices,
            @NotNull ObjList<GroupByFunction> ownerGroupByFunctions,
            @NotNull ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            int workerCount
    ) {
        final int slotCount = Math.min(workerCount, configuration.getPageFrameReduceQueueCapacity());

        try {
            this.configuration = configuration;
            this.slaveFactories = slaveFactories;
            this.masterTimestampColumnIndex = masterTimestampColumnIndex;
            this.sequenceColumnIndex = sequenceColumnIndex;
            this.masterRecordSink = masterRecordSink;

            // Initialize ASOF join lookup resources
            // ASOF JOIN lookup resources (null if no join key)
            this.masterKeyCopier = masterKeyCopier;
            this.slaveKeyCopier = slaveKeyCopier;
            this.slaveTimestampIndex = slaveTimestampIndex;

            // Store GROUP BY key copier and column mappings for CombinedRecord
            this.groupByKeyCopier = groupByKeyCopier;
            this.columnSources = columnSources;
            this.columnIndices = columnIndices;

            // Store metadata/sink for recreating sequence record array after clear()
            this.sequenceMetadata = sequenceCursorFactory.getMetadata();
            this.sequenceRecordSink = sequenceRecordSink;

            // Initialize shared sequence record array
            this.sequenceRecordArray = new RecordArray(
                    sequenceMetadata,
                    sequenceRecordSink,
                    configuration.getSqlHashJoinValuePageSize(),
                    configuration.getSqlHashJoinValueMaxPages()
            );

            // Initialize per-worker locks
            this.perWorkerLocks = new PerWorkerLocks(configuration, slotCount);

            // Initialize per-worker cursors (will be populated lazily)
            this.perWorkerSlaveCursors = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerSlaveCursors.add(null);
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
        // Free sequence record array - will be recreated in materializeSequenceCursor()
        Misc.free(sequenceRecordArray);
        sequenceRecordArray = null;
        sequenceRecordOffsets.clear();

        // Clear aggregation resources for potential reuse
        Misc.free(ownerMap);
        Misc.freeObjListAndKeepObjects(perWorkerMaps);
        Misc.clearObjList(ownerGroupByFunctions);
        for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
            Misc.clearObjList(perWorkerGroupByFunctions.getQuick(i));
        }
        Misc.free(ownerAllocator);
        Misc.freeObjListAndKeepObjects(perWorkerAllocators);
        // Free per-slot ASOF join maps (will be reopened lazily)
        if (ownerAsofJoinMap != null) {
            Misc.free(ownerAsofJoinMap);
        }
        Misc.freeObjListAndKeepObjects(perWorkerAsofJoinMaps);
        // Close per-slot slave cursors (but NOT the factories - they're owned by the factory class)
        if (ownerSlaveCursor != null) {
            ownerSlaveCursor.close();
            ownerSlaveCursor = null;
        }
        for (int i = 0, n = perWorkerSlaveCursors.size(); i < n; i++) {
            RecordCursor cursor = perWorkerSlaveCursors.getQuick(i);
            if (cursor != null) {
                cursor.close();
                perWorkerSlaveCursors.setQuick(i, null);
            }
        }
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
        // Free per-slot slave cursors (must close each cursor)
        for (int i = 0, n = perWorkerSlaveCursors.size(); i < n; i++) {
            Misc.free(perWorkerSlaveCursors.getQuick(i));
        }
        perWorkerSlaveCursors.clear();
        Misc.free(ownerSlaveCursor);
        // Close any remaining slave factories (may have been cleared by clear())
        for (int i = 0, n = slaveFactories.size(); i < n; i++) {
            Misc.free(slaveFactories.getQuick(i));
        }
        slaveFactories.clear();
        Misc.freeObjList(masterRowBatchPool);
    }

    /**
     * Get the ASOF join map for the given slot.
     * Each worker has its own map to avoid contention.
     * Reopens the map if it was closed by clear().
     */
    public Map getAsofJoinMap(int slotId) {
        Map map;
        if (slotId == -1) {
            map = ownerAsofJoinMap;
        } else {
            map = perWorkerAsofJoinMaps.getQuick(slotId);
        }
        if (map != null && !map.isOpen()) {
            map.reopen();
        }
        return map;
    }

    public CairoConfiguration getConfiguration() {
        return configuration;
    }

    public long getFirstSequenceTimeOffset() {
        return firstSequenceTimeOffset;
    }

    /**
     * Get the GROUP BY key copier for populating map keys from CombinedRecord.
     */
    public RecordSink getGroupByKeyCopier() {
        return groupByKeyCopier;
    }

    /**
     * Get the column indices mapping (baseMetadata column index -> source record column index).
     */
    public int[] getColumnIndices() {
        return columnIndices;
    }

    /**
     * Get the column sources mapping (baseMetadata column index -> source identifier).
     * Values: 0=SOURCE_MASTER, 1=SOURCE_SEQUENCE, 2=SOURCE_SLAVE
     */
    public int[] getColumnSources() {
        return columnSources;
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
     * Reopens the map if it was closed by clear().
     */
    public Map getMap(int slotId) {
        Map map;
        if (slotId == -1) {
            map = ownerMap;
        } else {
            map = perWorkerMaps.getQuick(slotId);
        }
        if (!map.isOpen()) {
            map.reopen();
        }
        return map;
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

    /**
     * Get the slave cursor for the given slot.
     * Creates the cursor lazily from the per-slot factory.
     * Workers should reuse the cursor by calling toTop() between batches.
     */
    public RecordCursor getSlaveCursor(int slotId) throws SqlException {
        if (slotId == -1) {
            if (ownerSlaveCursor == null) {
                // Owner uses the last factory in the list (index = slotCount)
                int ownerFactoryIndex = slaveFactories.size() - 1;
                ownerSlaveCursor = slaveFactories.getQuick(ownerFactoryIndex).getCursor(storedExecutionContext);
            }
            return ownerSlaveCursor;
        }
        RecordCursor cursor = perWorkerSlaveCursors.getQuick(slotId);
        if (cursor == null) {
            cursor = slaveFactories.getQuick(slotId).getCursor(storedExecutionContext);
            perWorkerSlaveCursors.setQuick(slotId, cursor);
        }
        return cursor;
    }

    public RecordSink getSlaveKeyCopier() {
        return slaveKeyCopier;
    }

    public int getSlaveTimestampIndex() {
        return slaveTimestampIndex;
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
    public void initSlaveCursors(SqlExecutionContext executionContext) throws SqlException {
        this.storedExecutionContext = executionContext;
    }

    /**
     * Materialize the sequence cursor (offset records) into the shared RecordArray.
     * Must be called once before dispatching tasks.
     */
    public void materializeSequenceCursor(RecordCursor sequenceCursor, SqlExecutionCircuitBreaker circuitBreaker) {
        if (sequenceRecordArray == null) {
            sequenceRecordArray = new RecordArray(
                    sequenceMetadata,
                    sequenceRecordSink,
                    configuration.getSqlHashJoinValuePageSize(),
                    configuration.getSqlHashJoinValueMaxPages()
            );
        } else {
            sequenceRecordArray.clear();
        }
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

        for (int i = 0, n = perWorkerMaps.size(); i < n; i++) {
            Map srcMap = perWorkerMaps.getQuick(i);
            if (srcMap.isOpen() && srcMap.size() > 0) {
                destMap.merge(srcMap, ownerFunctionUpdater);
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
