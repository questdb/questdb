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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

/**
 * Async cursor for keyed multi-slave HORIZON JOIN.
 * Manages per-slave time frame cache initialization.
 */
class AsyncMultiHorizonJoinRecordCursor implements RecordCursor {
    private final MessageBus messageBus;
    private final AtomicBooleanCircuitBreaker postAggregationCircuitBreaker;
    private final SOUnboundedCountDownLatch postAggregationDoneLatch = new SOUnboundedCountDownLatch();
    private final AtomicInteger postAggregationStartedCounter = new AtomicInteger();
    private final VirtualRecord recordA;
    private final VirtualRecord recordB;
    private final ObjList<Function> recordFunctions;
    private final ShardedMapCursor shardedCursor = new ShardedMapCursor();
    private final int slaveCount;
    private final ObjList<RecordCursorFactory> slaveFactories;
    private final ObjList<TablePageFrameCursor> slaveFrameCursors;
    private final ObjList<SymbolTableSource> slaveSources;
    private final ObjList<ConcurrentTimeFrameState> slaveTimeFrameStates;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private SqlExecutionContext executionContext;
    private UnorderedPageFrameSequence<AsyncMultiHorizonJoinAtom> frameSequence;
    private boolean isDataMapBuilt;
    private boolean isOpen;
    private boolean isSlaveTimeFrameCacheBuilt;
    private MapRecordCursor mapCursor;

    public AsyncMultiHorizonJoinRecordCursor(
            CairoEngine engine,
            MessageBus messageBus,
            ObjList<Function> recordFunctions,
            ObjList<RecordCursorFactory> slaveFactories
    ) {
        try {
            this.isOpen = true;
            this.messageBus = messageBus;
            this.postAggregationCircuitBreaker = new AtomicBooleanCircuitBreaker(engine);
            this.recordFunctions = recordFunctions;
            this.slaveFactories = slaveFactories;
            this.slaveCount = slaveFactories.size();
            this.slaveFrameCursors = new ObjList<>(slaveCount);
            this.slaveFrameCursors.setPos(slaveCount);
            this.slaveSources = new ObjList<>(slaveCount);
            this.slaveSources.setPos(slaveCount);
            this.recordA = new VirtualRecord(recordFunctions);
            this.recordB = new VirtualRecord(recordFunctions);

            this.slaveTimeFrameStates = new ObjList<>(slaveCount);
            for (int s = 0; s < slaveCount; s++) {
                slaveTimeFrameStates.add(new ConcurrentTimeFrameState());
            }
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
                mapCursor = Misc.free(mapCursor);
                Misc.freeObjListAndKeepObjects(slaveFrameCursors);
                Misc.freeObjListAndKeepObjects(slaveTimeFrameStates);
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

        final AsyncMultiHorizonJoinAtom atom = frameSequence.getAtom();
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
            final AsyncMultiHorizonJoinAtom atom = frameSequence.getAtom();
            final SymbolTableSource masterSource = frameSequence.getSymbolTableSource();

            for (int s = 0; s < slaveCount; s++) {
                TablePageFrameCursor cursor = slaveFrameCursors.getQuick(s);
                slaveTimeFrameStates.getQuick(s).of(
                        cursor,
                        slaveFactories.getQuick(s).getMetadata(),
                        cursor.getColumnMapping(),
                        cursor.isExternal(),
                        executionContext.getPageFrameMinRows(),
                        executionContext.getPageFrameMaxRows(),
                        executionContext.getSharedQueryWorkerCount()
                );
                try {
                    atom.initSlaveTimeFrameCursors(
                            s,
                            masterSource,
                            cursor,
                            slaveTimeFrameStates.getQuick(s)
                    );
                } catch (SqlException e) {
                    throw CairoException.nonCritical().put(e.getFlyweightMessage());
                }
                slaveSources.setQuick(s, cursor);
            }

            // Initialize group by functions after all slaves are set up
            try {
                atom.initGroupByFunctions(executionContext, masterSource, slaveSources);
            } catch (SqlException e) {
                throw CairoException.nonCritical().put(e.getFlyweightMessage());
            }

            isSlaveTimeFrameCacheBuilt = true;
        }
    }

    private void throwTimeoutException() {
        if (frameSequence.getCancelReason() == SqlExecutionCircuitBreaker.STATE_CANCELLED) {
            throw CairoException.queryCancelled();
        } else {
            throw CairoException.queryTimedOut();
        }
    }

    void of(UnorderedPageFrameSequence<AsyncMultiHorizonJoinAtom> frameSequence, SqlExecutionContext executionContext) throws SqlException {
        final AsyncMultiHorizonJoinAtom atom = frameSequence.getAtom();
        if (!isOpen) {
            isOpen = true;
            atom.reopen();
        }
        this.frameSequence = frameSequence;
        this.executionContext = executionContext;
        this.circuitBreaker = executionContext.getCircuitBreaker();

        // Open per-slave page frame cursors and initialize functions
        try {
            for (int s = 0; s < slaveCount; s++) {
                slaveFrameCursors.setQuick(s, (TablePageFrameCursor) slaveFactories.getQuick(s).getPageFrameCursor(executionContext, ORDER_ASC));
            }

            // Initialize symbol table source with master and all slave sources
            final MultiHorizonJoinSymbolTableSource symbolTableSource = atom.getSymbolTableSource();
            for (int s = 0; s < slaveCount; s++) {
                slaveSources.setQuick(s, slaveFrameCursors.getQuick(s));
            }
            symbolTableSource.of(frameSequence.getSymbolTableSource(), slaveSources);
            Function.init(recordFunctions, symbolTableSource, executionContext, null);
        } catch (Throwable th) {
            Misc.freeObjList(slaveFrameCursors);
            throw th;
        }

        isDataMapBuilt = false;
        isSlaveTimeFrameCacheBuilt = false;
    }
}
