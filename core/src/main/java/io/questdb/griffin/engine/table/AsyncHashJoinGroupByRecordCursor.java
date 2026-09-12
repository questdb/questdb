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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.ShardedMapCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.HashJoinGroupByFunctions;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.PostAggregationCircuitBreaker;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

import java.util.concurrent.atomic.AtomicInteger;

/** Keeps probe/build symbols alive until output and parent consumers finish. */
final class AsyncHashJoinGroupByRecordCursor implements RecordCursor {
    private final CairoEngine engine;
    private final UnorderedPageFrameSequence<AsyncHashJoinGroupByAtom> frameSequence;
    private final HashJoinGroupByFunctions functions;
    private final PostAggregationCircuitBreaker mergeCircuitBreaker;
    private final SOUnboundedCountDownLatch mergeDoneLatch = new SOUnboundedCountDownLatch();
    private final AtomicInteger mergeStartedCounter = new AtomicInteger();
    private final VirtualRecord recordA;
    private final VirtualRecord recordB;
    private final ShardedMapCursor shardedCursor = new ShardedMapCursor();
    private SqlExecutionCircuitBreaker circuitBreaker;
    private boolean isOpen;
    private MapRecordCursor mapCursor;

    AsyncHashJoinGroupByRecordCursor(CairoEngine engine, UnorderedPageFrameSequence<AsyncHashJoinGroupByAtom> frameSequence,
                                    HashJoinGroupByFunctions functions) {
        this.engine = engine;
        this.mergeCircuitBreaker = new PostAggregationCircuitBreaker(engine);
        this.frameSequence = frameSequence;
        this.functions = functions;
        recordA = new VirtualRecord(functions.getOutputFunctions());
        recordB = new VirtualRecord(functions.getOutputFunctions());
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker breaker, Counter counter) {
        buildMap();
        mapCursor.calculateSize(breaker, counter);
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            frameSequence.cancel(SqlExecutionCircuitBreaker.STATE_CANCELLED);
            // await() drains publication even when dispatchAndAwait() failed midway.
            frameSequence.await();
            try {
                mapCursor = Misc.free(mapCursor);
            } finally {
                recordA.of(null);
                recordB.of(null);
                circuitBreaker = null;
                frameSequence.reset();
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
        return functions.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        buildMap();
        return mapCursor.hasNext();
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return functions.newSymbolTable(columnIndex);
    }

    @Override
    public long preComputedStateSize() {
        return mapCursor == null ? 0 : 1;
    }

    @Override
    public void recordAt(Record record, long rowId) {
        mapCursor.recordAt(((VirtualRecord) record).getBaseRecord(), rowId);
    }

    @Override
    public long size() {
        return mapCursor == null ? -1 : mapCursor.size();
    }

    @Override
    public void toTop() {
        if (mapCursor != null) {
            mapCursor.toTop();
            GroupByUtils.toTop(functions.getOutputFunctions());
        }
    }

    private void buildMap() {
        if (mapCursor == null) {
            try {
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                AsyncHashJoinGroupByAtom atom = frameSequence.getAtom();
                if (atom.shouldProbe()) {
                    frameSequence.prepareForDispatch();
                    atom.getFilterContext().initMemoryPools(frameSequence.getPageFrameAddressCache(), frameSequence.getMemoryTracker());
                    frameSequence.dispatchAndAwait();
                }
                final GroupByShardingContext sharding = atom.getShardingContext();
                if (sharding.isSharded()) {
                    // mergeShards drains every published task before returning or throwing.
                    // Only then may close() release fragments and shared build backing.
                    final ObjList<Map> shards = sharding.mergeShards(engine.getMessageBus(), frameSequence.getWorkStealingStrategy(),
                            circuitBreaker, mergeCircuitBreaker, mergeDoneLatch, mergeStartedCounter);
                    if (mergeCircuitBreaker.checkIfTripped()) {
                        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
                        if (mergeCircuitBreaker.hasError()) {
                            throw mergeCircuitBreaker.buildError();
                        }
                        throw frameSequence.buildInterruptionException();
                    }
                    shardedCursor.of(shards);
                    mapCursor = shardedCursor;
                } else {
                    mapCursor = sharding.mergeOwnerMap(circuitBreaker).getCursor();
                }
                recordA.of(mapCursor.getRecord());
                recordB.of(mapCursor.getRecordB());
            } catch (Throwable th) {
                Misc.free(this, th);
                throw th;
            }
        }
    }

    void open(SqlExecutionCircuitBreaker circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
        isOpen = true;
    }
}
