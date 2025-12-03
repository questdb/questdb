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

import io.questdb.MessageBus;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.MarkoutReduceJob;
import io.questdb.griffin.engine.groupby.MasterRowBatch;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MCSequence;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.tasks.MarkoutReduceTask;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Record cursor for parallel markout query execution.
 * <p>
 * This cursor:
 * 1. Reads master rows from the master cursor and batches them into MasterRowBatches
 * 2. Dispatches tasks to the MarkoutReduceQueue
 * 3. Waits for all workers to complete
 * 4. Merges partial maps from workers
 * 5. Iterates over the merged results
 */
public class AsyncMarkoutGroupByRecordCursor implements RecordCursor {
    private static final Log LOG = LogFactory.getLog(AsyncMarkoutGroupByRecordCursor.class);

    private final MessageBus messageBus;
    private final AsyncMarkoutGroupByAtom atom;
    private final ObjList<Function> recordFunctions;
    private final VirtualRecord recordA;
    private final VirtualRecord recordB;
    private final CairoConfiguration configuration;
    private final RecordSink masterRecordSink;
    private final RecordMetadata masterMetadata;

    // Task coordination
    private final AtomicBooleanCircuitBreaker taskCircuitBreaker;
    private final SOUnboundedCountDownLatch taskDoneLatch = new SOUnboundedCountDownLatch();
    private final AtomicInteger taskStartedCounter = new AtomicInteger();

    // MasterRowBatch pool for reuse
    private final ObjList<MasterRowBatch> masterRowBatchPool = new ObjList<>();
    private int nextPoolIndex = 0;

    // State
    private SqlExecutionCircuitBreaker circuitBreaker;
    private RecordCursor masterCursor;
    private MapRecordCursor mapCursor;
    private boolean isDataMapBuilt;
    private boolean isOpen;
    private int dispatchedBatchCount;

    public AsyncMarkoutGroupByRecordCursor(
            CairoConfiguration configuration,
            MessageBus messageBus,
            AsyncMarkoutGroupByAtom atom,
            ObjList<Function> recordFunctions,
            RecordSink masterRecordSink,
            RecordMetadata masterMetadata
    ) {
        this.configuration = configuration;
        this.messageBus = messageBus;
        this.atom = atom;
        this.recordFunctions = recordFunctions;
        this.masterRecordSink = masterRecordSink;
        this.masterMetadata = masterMetadata;
        this.recordA = new VirtualRecord(recordFunctions);
        this.recordB = new VirtualRecord(recordFunctions);
        this.taskCircuitBreaker = new AtomicBooleanCircuitBreaker(null);
        this.isOpen = true;
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            mapCursor = Misc.free(mapCursor);
            Misc.free(masterCursor);
            masterCursor = null;
            // Free master row batches in pool
            for (int i = 0, n = masterRowBatchPool.size(); i < n; i++) {
                Misc.free(masterRowBatchPool.getQuick(i));
            }
            masterRowBatchPool.clear();
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
    public SymbolTable newSymbolTable(int columnIndex) {
        return ((SymbolFunction) recordFunctions.getQuick(columnIndex)).newSymbolTable();
    }

    @Override
    public boolean hasNext() {
        buildMapConditionally();
        return mapCursor.hasNext();
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
        }
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        buildMapConditionally();
        mapCursor.calculateSize(circuitBreaker, counter);
    }

    @Override
    public long preComputedStateSize() {
        return isDataMapBuilt ? 1 : 0;
    }

    void of(
            RecordCursor masterCursor,
            RecordCursor sequenceCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (!isOpen) {
            isOpen = true;
            atom.reopen();
        }
        this.masterCursor = masterCursor;
        this.circuitBreaker = executionContext.getCircuitBreaker();
        Function.init(recordFunctions, null, executionContext, null);
        isDataMapBuilt = false;
        dispatchedBatchCount = 0;
        nextPoolIndex = 0;

        // Materialize sequence cursor into shared RecordArray
        atom.materializeSequenceCursor(sequenceCursor, circuitBreaker);
    }

    private void buildMapConditionally() {
        if (!isDataMapBuilt) {
            buildMap();
        }
    }

    private void buildMap() {
        // Reset coordination primitives
        taskCircuitBreaker.reset();
        taskStartedCounter.set(0);
        taskDoneLatch.reset();

        final RingQueue<MarkoutReduceTask> queue = messageBus.getMarkoutReduceQueue();
        final MPSequence pubSeq = messageBus.getMarkoutReducePubSeq();
        final MCSequence subSeq = messageBus.getMarkoutReduceSubSeq();

        int queuedCount = 0;

        try {
            // Read master rows and dispatch batches
            Record masterRecord = masterCursor.getRecord();
            MasterRowBatch currentBatch = getOrCreateMasterRowBatch();

            while (masterCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();

                if (!currentBatch.add(masterRecord)) {
                    // Batch is full, dispatch it
                    queuedCount += dispatchBatch(currentBatch, queue, pubSeq, subSeq, queuedCount);
                    currentBatch = getOrCreateMasterRowBatch();
                    currentBatch.add(masterRecord);
                }
            }

            // Dispatch remaining batch if not empty
            if (currentBatch.size() > 0) {
                queuedCount += dispatchBatch(currentBatch, queue, pubSeq, subSeq, queuedCount);
            }

        } catch (Throwable th) {
            taskCircuitBreaker.cancel();
            throw th;
        }

        // Wait for all workers to complete
        awaitCompletion(queuedCount, queue, subSeq);

        if (taskCircuitBreaker.checkIfTripped()) {
            throwTimeoutException();
        }

        // Merge partial maps
        Map mergedMap = atom.mergeWorkerMaps();
        mapCursor = mergedMap.getCursor();

        recordA.of(mapCursor.getRecord());
        recordB.of(mapCursor.getRecordB());
        isDataMapBuilt = true;

        LOG.debug().$("map built [batches=").$(dispatchedBatchCount)
                .$(", queuedCount=").$(queuedCount).I$();
    }

    private int dispatchBatch(
            MasterRowBatch batch,
            RingQueue<MarkoutReduceTask> queue,
            MPSequence pubSeq,
            MCSequence subSeq,
            int currentQueuedCount
    ) {
        while (true) {
            long cursor = pubSeq.next();
            if (cursor >= 0) {
                MarkoutReduceTask task = queue.get(cursor);
                task.of(
                        taskCircuitBreaker,
                        taskStartedCounter,
                        taskDoneLatch,
                        atom,
                        dispatchedBatchCount,
                        batch
                );
                pubSeq.done(cursor);
                dispatchedBatchCount++;
                return 1;
            }

            // Queue is full, try work stealing
            circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();

            // Try to steal work from the queue
            long stealCursor = subSeq.next();
            if (stealCursor >= 0) {
                MarkoutReduceTask task = queue.get(stealCursor);
                MarkoutReduceJob.run(-1, task, subSeq, stealCursor, atom);
            } else {
                Os.pause();
            }
        }
    }

    private void awaitCompletion(int queuedCount, RingQueue<MarkoutReduceTask> queue, MCSequence subSeq) {
        while (!taskDoneLatch.done(queuedCount)) {
            if (circuitBreaker.checkIfTripped()) {
                taskCircuitBreaker.cancel();
            }

            // Try to steal work
            long cursor = subSeq.next();
            if (cursor >= 0) {
                MarkoutReduceTask task = queue.get(cursor);
                MarkoutReduceJob.run(-1, task, subSeq, cursor, atom);
            } else {
                Os.pause();
            }
        }
    }

    private MasterRowBatch getOrCreateMasterRowBatch() {
        if (nextPoolIndex < masterRowBatchPool.size()) {
            MasterRowBatch batch = masterRowBatchPool.getQuick(nextPoolIndex++);
            batch.clear();
            return batch;
        }
        MasterRowBatch batch = new MasterRowBatch(configuration, masterMetadata, masterRecordSink);
        masterRowBatchPool.add(batch);
        nextPoolIndex++;
        return batch;
    }

    private void throwTimeoutException() {
        throw CairoException.queryTimedOut();
    }
}
