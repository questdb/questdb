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

import io.questdb.MessageBus;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.ShardedMapCursor;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.std.DirectIntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.griffin.engine.table.ConcurrentTimeFrameCursor.populatePartitionTimestamps;

class AsyncHorizonJoinRecordCursor implements RecordCursor {
    private final MessageBus messageBus;
    private final AtomicBooleanCircuitBreaker postAggregationCircuitBreaker;
    private final SOUnboundedCountDownLatch postAggregationDoneLatch = new SOUnboundedCountDownLatch();
    private final AtomicInteger postAggregationStartedCounter = new AtomicInteger();
    private final VirtualRecord recordA;
    private final VirtualRecord recordB;
    private final ObjList<Function> recordFunctions;
    private final ShardedMapCursor shardedCursor = new ShardedMapCursor();
    private final RecordCursorFactory slaveFactory;
    private final LongList slavePartitionCeilings = new LongList();
    private final LongList slavePartitionTimestamps = new LongList();
    // Slave time frame cache data
    private final PageFrameAddressCache slaveTimeFrameAddressCache;
    private final DirectIntList slaveTimeFramePartitionIndexes;
    private final LongList slaveTimeFrameRowCounts = new LongList();
    private SqlExecutionCircuitBreaker circuitBreaker;
    private SqlExecutionContext executionContext;
    private UnorderedPageFrameSequence<AsyncHorizonJoinAtom> frameSequence;
    private boolean isDataMapBuilt;
    private boolean isOpen;
    private boolean isSlaveTimeFrameCacheBuilt;
    private MapRecordCursor mapCursor;
    private TablePageFrameCursor slaveFrameCursor;

    public AsyncHorizonJoinRecordCursor(
            CairoEngine engine,
            MessageBus messageBus,
            ObjList<Function> recordFunctions,
            RecordCursorFactory slaveFactory
    ) {
        try {
            this.isOpen = true;
            this.messageBus = messageBus;
            this.postAggregationCircuitBreaker = new AtomicBooleanCircuitBreaker(engine);
            this.recordFunctions = recordFunctions;
            this.slaveFactory = slaveFactory;
            this.recordA = new VirtualRecord(recordFunctions);
            this.recordB = new VirtualRecord(recordFunctions);
            this.slaveTimeFrameAddressCache = new PageFrameAddressCache();
            this.slaveTimeFramePartitionIndexes = new DirectIntList(64, MemoryTag.NATIVE_DEFAULT, true);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        buildSlaveTimeFrameCacheConditionally();
        buildMapConditionally();
        mapCursor.calculateSize(circuitBreaker, counter);
    }

    @Override
    public void close() {
        if (isOpen) {
            try {
                if (frameSequence != null) {
                    frameSequence.await();
                    frameSequence.reset();
                }
            } finally {
                // Free shared resources only after workers have finished
                mapCursor = Misc.free(mapCursor);
                slaveFrameCursor = Misc.free(slaveFrameCursor);
                Misc.free(slaveTimeFrameAddressCache);
                Misc.free(slaveTimeFramePartitionIndexes);
                isOpen = false;
            }
        }
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public Record getRecordB() {
        return recordB;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return (SymbolTable) recordFunctions.getQuick(columnIndex);
    }

    @Override
    public boolean hasNext() {
        buildSlaveTimeFrameCacheConditionally();
        buildMapConditionally();
        return mapCursor.hasNext();
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return ((SymbolFunction) recordFunctions.getQuick(columnIndex)).newSymbolTable();
    }

    @Override
    public long preComputedStateSize() {
        return isDataMapBuilt ? 1 : 0;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        if (mapCursor != null) {
            mapCursor.recordAt(((VirtualRecord) record).getBaseRecord(), atRowId);
        }
    }

    @Override
    public long size() {
        if (!isDataMapBuilt) {
            return -1;
        }
        return mapCursor != null ? mapCursor.size() : -1;
    }

    @Override
    public void toTop() {
        if (mapCursor != null) {
            mapCursor.toTop();
            GroupByUtils.toTop(recordFunctions);
            frameSequence.getAtom().toTop();
        }
    }

    private void buildMap() {
        frameSequence.prepareForDispatch();
        frameSequence.getAtom().getFilterContext().initMemoryPools(frameSequence.getPageFrameAddressCache());
        frameSequence.dispatchAndAwait();

        final AsyncHorizonJoinAtom atom = frameSequence.getAtom();
        final GroupByShardingContext shardingCtx = atom.getShardingContext();

        if (!atom.isSharded()) {
            final Map dataMap = shardingCtx.mergeOwnerMap();
            mapCursor = dataMap.getCursor();
        } else {
            final ObjList<Map> shards = shardingCtx.mergeShards(
                    messageBus,
                    frameSequence.getWorkStealingStrategy(),
                    circuitBreaker,
                    postAggregationCircuitBreaker,
                    postAggregationDoneLatch,
                    postAggregationStartedCounter
            );
            if (postAggregationCircuitBreaker.checkIfTripped()) {
                throwTimeoutException();
            }
            shardedCursor.of(shards);
            mapCursor = shardedCursor;
        }

        recordA.of(mapCursor.getRecord());
        recordB.of(mapCursor.getRecordB());
        isDataMapBuilt = true;
    }

    private void buildMapConditionally() {
        if (!isDataMapBuilt) {
            buildMap();
        }
    }

    private void buildSlaveTimeFrameCacheConditionally() {
        if (!isSlaveTimeFrameCacheBuilt) {
            final int frameCount = initializeSlaveTimeFrameCache();
            populatePartitionTimestamps(slaveFrameCursor, slavePartitionTimestamps, slavePartitionCeilings);
            initializeTimeFrameCursors(frameCount);
            isSlaveTimeFrameCacheBuilt = true;
        }
    }

    private int initializeSlaveTimeFrameCache() {
        RecordMetadata slaveMetadata = slaveFactory.getMetadata();
        slaveTimeFrameAddressCache.of(slaveMetadata, slaveFrameCursor.getColumnIndexes(), slaveFrameCursor.isExternal());
        slaveTimeFramePartitionIndexes.reopen();
        slaveTimeFramePartitionIndexes.clear();
        slaveTimeFrameRowCounts.clear();

        int frameCount = 0;
        PageFrame frame;
        while ((frame = slaveFrameCursor.next()) != null) {
            slaveTimeFramePartitionIndexes.add(frame.getPartitionIndex());
            slaveTimeFrameRowCounts.add(frame.getPartitionHi() - frame.getPartitionLo());
            slaveTimeFrameAddressCache.add(frameCount++, frame);
        }
        return frameCount;
    }

    private void initializeTimeFrameCursors(int frameCount) {
        try {
            frameSequence.getAtom().initTimeFrameCursors(
                    executionContext,
                    frameSequence.getSymbolTableSource(),
                    slaveFrameCursor,
                    slaveTimeFrameAddressCache,
                    slaveTimeFramePartitionIndexes,
                    slaveTimeFrameRowCounts,
                    slavePartitionTimestamps,
                    slavePartitionCeilings,
                    frameCount
            );
        } catch (SqlException e) {
            throw CairoException.nonCritical().put(e.getFlyweightMessage());
        }
    }

    private void throwTimeoutException() {
        if (frameSequence.getCancelReason() == SqlExecutionCircuitBreaker.STATE_CANCELLED) {
            throw CairoException.queryCancelled();
        } else {
            throw CairoException.queryTimedOut();
        }
    }

    void of(UnorderedPageFrameSequence<AsyncHorizonJoinAtom> frameSequence, SqlExecutionContext executionContext) throws SqlException {
        final AsyncHorizonJoinAtom atom = frameSequence.getAtom();
        if (!isOpen) {
            isOpen = true;
            atom.reopen();
        }
        this.frameSequence = frameSequence;
        this.executionContext = executionContext;
        this.circuitBreaker = executionContext.getCircuitBreaker();

        // Get slave page frame cursor for time frame initialization
        this.slaveFrameCursor = (TablePageFrameCursor) slaveFactory.getPageFrameCursor(executionContext, ORDER_ASC);

        // Initialize record functions with a symbol table source that routes lookups
        // to the correct source (master or slave) based on column mappings
        final HorizonJoinSymbolTableSource symbolTableSource = atom.getSymbolTableSource();
        symbolTableSource.of(frameSequence.getSymbolTableSource(), slaveFrameCursor);
        Function.init(recordFunctions, symbolTableSource, executionContext, null);

        isDataMapBuilt = false;
        isSlaveTimeFrameCacheBuilt = false;
    }
}
