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
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
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
    // Native memory layout for iterator blocks (duplicated from MarkoutHorizonRecordCursor)
    private static final int BLOCK_HEADER_SIZE = 16;
    private static final int BLOCK_OFFSET_NEXT_BLOCK_ADDR = 0;    // long (8 bytes)
    private static final int BLOCK_OFFSET_NEXT_FREE_SLOT = 8;     // int (4 bytes)
    private static final int BLOCK_OFFSET_USED_SLOT_COUNT = 12;   // int (4 bytes)
    private static final int ITERATORS_PER_BLOCK = 1024;
    private static final int ITERATOR_OFFSET_TRADE_INDEX = 0;             // int (4 bytes)
    private static final int ITERATOR_OFFSET_MASTER_TIMESTAMP = 8;        // long (8 bytes)
    private static final int ITERATOR_OFFSET_NEXT_ITER_ADDR = 16;         // long (8 bytes)
    private static final int ITERATOR_OFFSET_NEXT_SEQUENCE_ROW_NUM = 32;  // int (4 bytes)
    private static final int ITERATOR_OFFSET_NEXT_TIMESTAMP = 24;         // long (8 bytes)
    private static final int ITERATOR_OFFSET_OFFSET_FROM_BLOCK_START = 36; // int (4 bytes)
    private static final int ITERATOR_SIZE = 40;
    private static final long BLOCK_SIZE = BLOCK_HEADER_SIZE + (ITERATORS_PER_BLOCK * ITERATOR_SIZE);

    private final CairoConfiguration configuration;

    // Shared (read-only) sequence data - materialized offset records
    private final RecordArray sequenceRecordArray;
    private final LongList sequenceRecordOffsets = new LongList();
    private final int sequenceColumnIndex;
    private long sequenceRowCount;
    private long firstSequenceTimeOffset;

    // Master row timestamp column index
    private final int masterTimestampColumnIndex;

    // Prices factory for creating per-worker cursors
    private final RecordCursorFactory pricesFactory;

    // Key and value types for the aggregation map
    private final ColumnTypes keyTypes;
    private final ColumnTypes valueTypes;

    // Per-worker resources
    private final PerWorkerLocks perWorkerLocks;
    private final ObjList<RecordCursor> perWorkerPricesCursors;
    private final ObjList<Map> perWorkerMaps;
    private final ObjList<GroupByAllocator> perWorkerAllocators;
    private final ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions;
    private final ObjList<GroupByFunctionsUpdater> perWorkerFunctionUpdaters;
    private final ObjList<RecordSink> perWorkerMapSinks;
    // Per-worker iterator block memory addresses
    private final LongList perWorkerFirstIteratorBlockAddr;
    private final LongList perWorkerLastIteratorBlockAddr;

    // Owner resources (for work stealing)
    private RecordCursor ownerPricesCursor;
    private final Map ownerMap;
    private final GroupByAllocator ownerAllocator;
    private final ObjList<GroupByFunction> ownerGroupByFunctions;
    private final GroupByFunctionsUpdater ownerFunctionUpdater;
    private final RecordSink ownerMapSink;
    // Owner iterator block memory
    private long ownerFirstIteratorBlockAddr;
    private long ownerLastIteratorBlockAddr;

    // Master row batch pool for reuse
    private final ObjList<MasterRowBatch> masterRowBatchPool;
    private final RecordSink masterRecordSink;

    public AsyncMarkoutGroupByAtom(
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull RecordCursorFactory pricesFactory,
            @NotNull RecordCursorFactory sequenceCursorFactory,
            @NotNull RecordSink sequenceRecordSink,
            @NotNull RecordSink masterRecordSink,
            int masterTimestampColumnIndex,
            int sequenceColumnIndex,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            @NotNull ObjList<GroupByFunction> ownerGroupByFunctions,
            @NotNull ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            @NotNull RecordSink ownerMapSink,
            @NotNull ObjList<RecordSink> perWorkerMapSinks,
            int workerCount
    ) {
        final int slotCount = Math.min(workerCount, configuration.getPageFrameReduceQueueCapacity());

        try {
            this.configuration = configuration;
            this.pricesFactory = pricesFactory;
            this.masterTimestampColumnIndex = masterTimestampColumnIndex;
            this.sequenceColumnIndex = sequenceColumnIndex;
            this.keyTypes = new ArrayColumnTypes().addAll(keyTypes);
            this.valueTypes = new ArrayColumnTypes().addAll(valueTypes);
            this.masterRecordSink = masterRecordSink;

            // Initialize shared sequence record array
            this.sequenceRecordArray = new RecordArray(
                    sequenceCursorFactory.getMetadata(),
                    sequenceRecordSink,
                    configuration.getSqlHashJoinValuePageSize(),
                    configuration.getSqlHashJoinValueMaxPages()
            );

            // Initialize per-worker locks
            this.perWorkerLocks = new PerWorkerLocks(configuration, slotCount);

            // Initialize per-worker cursors (will be populated during init)
            this.perWorkerPricesCursors = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerPricesCursors.add(null);
            }

            // Initialize per-worker maps
            this.perWorkerMaps = new ObjList<>(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerMaps.add(MapFactory.createUnorderedMap(configuration, this.keyTypes, this.valueTypes));
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

            // Initialize owner and per-worker map sinks
            this.ownerMapSink = ownerMapSink;
            this.perWorkerMapSinks = perWorkerMapSinks;

            // Initialize owner map
            this.ownerMap = MapFactory.createUnorderedMap(configuration, this.keyTypes, this.valueTypes);

            // Initialize per-worker iterator block addresses
            this.perWorkerFirstIteratorBlockAddr = new LongList(slotCount);
            this.perWorkerLastIteratorBlockAddr = new LongList(slotCount);
            for (int i = 0; i < slotCount; i++) {
                perWorkerFirstIteratorBlockAddr.add(0);
                perWorkerLastIteratorBlockAddr.add(0);
            }
            this.ownerFirstIteratorBlockAddr = 0;
            this.ownerLastIteratorBlockAddr = 0;

            // Initialize master row batch pool
            this.masterRowBatchPool = new ObjList<>(slotCount + 1);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void clear() {
        Misc.free(ownerMap);
        Misc.freeObjListAndKeepObjects(perWorkerMaps);
        Misc.clearObjList(ownerGroupByFunctions);
        for (int i = 0, n = perWorkerGroupByFunctions.size(); i < n; i++) {
            Misc.clearObjList(perWorkerGroupByFunctions.getQuick(i));
        }
        Misc.free(ownerAllocator);
        Misc.freeObjListAndKeepObjects(perWorkerAllocators);
        freeAllIteratorBlocks(-1);
        for (int i = 0, n = perWorkerFirstIteratorBlockAddr.size(); i < n; i++) {
            freeAllIteratorBlocks(i);
        }
        Misc.freeObjListAndKeepObjects(perWorkerPricesCursors);
        Misc.free(ownerPricesCursor);
        ownerPricesCursor = null;
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
        freeAllIteratorBlocks(-1);
        for (int i = 0, n = perWorkerFirstIteratorBlockAddr.size(); i < n; i++) {
            freeAllIteratorBlocks(i);
        }
        Misc.freeObjListAndKeepObjects(perWorkerPricesCursors);
        Misc.free(ownerPricesCursor);
        Misc.freeObjList(masterRowBatchPool);
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

    @Override
    public void reopen() {
        // Maps will be opened lazily by worker threads
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
     * Create or acquire a prices cursor for the given slot.
     */
    public RecordCursor getPricesCursor(int slotId, SqlExecutionContext executionContext) throws SqlException {
        if (slotId == -1) {
            if (ownerPricesCursor == null) {
                ownerPricesCursor = pricesFactory.getCursor(executionContext);
            }
            return ownerPricesCursor;
        }
        RecordCursor cursor = perWorkerPricesCursors.getQuick(slotId);
        if (cursor == null) {
            cursor = pricesFactory.getCursor(executionContext);
            perWorkerPricesCursors.setQuick(slotId, cursor);
        }
        return cursor;
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
     * Get the map sink for the given slot.
     */
    public RecordSink getMapSink(int slotId) {
        if (slotId == -1) {
            return ownerMapSink;
        }
        return perWorkerMapSinks.getQuick(slotId);
    }

    /**
     * Get the group by functions for the given slot.
     */
    public ObjList<GroupByFunction> getGroupByFunctions(int slotId) {
        if (slotId == -1) {
            return ownerGroupByFunctions;
        }
        return perWorkerGroupByFunctions.getQuick(slotId);
    }

    // Shared sequence data accessors
    public RecordArray getSequenceRecordArray() {
        return sequenceRecordArray;
    }

    public LongList getSequenceRecordOffsets() {
        return sequenceRecordOffsets;
    }

    public int getSequenceColumnIndex() {
        return sequenceColumnIndex;
    }

    public long getSequenceRowCount() {
        return sequenceRowCount;
    }

    public long getFirstSequenceTimeOffset() {
        return firstSequenceTimeOffset;
    }

    public int getMasterTimestampColumnIndex() {
        return masterTimestampColumnIndex;
    }

    public CairoConfiguration getConfiguration() {
        return configuration;
    }

    public RecordSink getMasterRecordSink() {
        return masterRecordSink;
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
     * Release a slot after parallel execution.
     */
    public void release(int slotId) {
        perWorkerLocks.releaseSlot(slotId);
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

    // ==================== Iterator Block Management ====================
    // These methods duplicate the native memory management from MarkoutHorizonRecordCursor
    // for the per-worker k-way merge algorithm

    public long allocIteratorBlock(int slotId) {
        long blockAddr = Unsafe.malloc(BLOCK_SIZE, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().setMemory(blockAddr, BLOCK_HEADER_SIZE, (byte) 0);

        if (slotId == -1) {
            if (ownerFirstIteratorBlockAddr == 0) {
                ownerFirstIteratorBlockAddr = blockAddr;
            } else {
                long lastBlock = ownerLastIteratorBlockAddr;
                Unsafe.getUnsafe().putLong(lastBlock + BLOCK_OFFSET_NEXT_BLOCK_ADDR, blockAddr);
            }
            ownerLastIteratorBlockAddr = blockAddr;
        } else {
            long firstBlock = perWorkerFirstIteratorBlockAddr.getQuick(slotId);
            if (firstBlock == 0) {
                perWorkerFirstIteratorBlockAddr.setQuick(slotId, blockAddr);
            } else {
                long lastBlock = perWorkerLastIteratorBlockAddr.getQuick(slotId);
                Unsafe.getUnsafe().putLong(lastBlock + BLOCK_OFFSET_NEXT_BLOCK_ADDR, blockAddr);
            }
            perWorkerLastIteratorBlockAddr.setQuick(slotId, blockAddr);
        }

        return blockAddr;
    }

    public void freeAllIteratorBlocks(int slotId) {
        long blockAddr;
        if (slotId == -1) {
            blockAddr = ownerFirstIteratorBlockAddr;
            ownerFirstIteratorBlockAddr = 0;
            ownerLastIteratorBlockAddr = 0;
        } else {
            blockAddr = perWorkerFirstIteratorBlockAddr.getQuick(slotId);
            perWorkerFirstIteratorBlockAddr.setQuick(slotId, 0);
            perWorkerLastIteratorBlockAddr.setQuick(slotId, 0);
        }

        while (blockAddr != 0) {
            long nextBlockAddr = Unsafe.getUnsafe().getLong(blockAddr + BLOCK_OFFSET_NEXT_BLOCK_ADDR);
            Unsafe.free(blockAddr, BLOCK_SIZE, MemoryTag.NATIVE_DEFAULT);
            blockAddr = nextBlockAddr;
        }
    }

    public long getLastIteratorBlockAddr(int slotId) {
        if (slotId == -1) {
            return ownerLastIteratorBlockAddr;
        }
        return perWorkerLastIteratorBlockAddr.getQuick(slotId);
    }

    public long getFirstIteratorBlockAddr(int slotId) {
        if (slotId == -1) {
            return ownerFirstIteratorBlockAddr;
        }
        return perWorkerFirstIteratorBlockAddr.getQuick(slotId);
    }

    public void setLastIteratorBlockAddr(int slotId, long addr) {
        if (slotId == -1) {
            ownerLastIteratorBlockAddr = addr;
        } else {
            perWorkerLastIteratorBlockAddr.setQuick(slotId, addr);
        }
    }

    public void setFirstIteratorBlockAddr(int slotId, long addr) {
        if (slotId == -1) {
            ownerFirstIteratorBlockAddr = addr;
        } else {
            perWorkerFirstIteratorBlockAddr.setQuick(slotId, addr);
        }
    }

    // Static helper constants for iterator layout
    public static int getBlockHeaderSize() {
        return BLOCK_HEADER_SIZE;
    }

    public static int getIteratorsPerBlock() {
        return ITERATORS_PER_BLOCK;
    }

    public static int getIteratorSize() {
        return ITERATOR_SIZE;
    }

    public static long getBlockSize() {
        return BLOCK_SIZE;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("AsyncMarkoutGroupByAtom");
    }
}
