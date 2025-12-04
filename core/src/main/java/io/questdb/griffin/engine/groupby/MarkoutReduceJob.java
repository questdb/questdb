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

package io.questdb.griffin.engine.groupby;

import io.questdb.MessageBus;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.griffin.engine.table.AsyncMarkoutGroupByAtom;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.CountDownLatchSPI;
import io.questdb.mp.Sequence;
import io.questdb.tasks.MarkoutReduceTask;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Worker job that consumes MarkoutReduceTask from the queue.
 * Each task contains a batch of master rows to process through:
 * MarkoutHorizon expansion -> ASOF JOIN -> GROUP BY aggregation
 */
public class MarkoutReduceJob extends AbstractQueueConsumerJob<MarkoutReduceTask> {
    private static final Log LOG = LogFactory.getLog(MarkoutReduceJob.class);
    // Thread-local reducer instance to avoid creating garbage
    private static final ThreadLocal<MarkoutReducer> REDUCER = ThreadLocal.withInitial(MarkoutReducer::new);

    public MarkoutReduceJob(MessageBus messageBus) {
        super(messageBus.getMarkoutReduceQueue(), messageBus.getMarkoutReduceSubSeq());
    }

    /**
     * Process a markout reduce task.
     * This method can be called by worker threads (stealingAtom=null) or
     * by the query owner thread for work stealing (stealingAtom!=null).
     */
    public static void run(
            int workerId,
            MarkoutReduceTask task,
            Sequence subSeq,
            long cursor,
            AsyncMarkoutGroupByAtom stealingAtom
    ) {
        final AtomicBooleanCircuitBreaker circuitBreaker = task.getCircuitBreaker();
        final AtomicInteger startedCounter = task.getStartedCounter();
        final CountDownLatchSPI doneLatch = task.getDoneLatch();
        final AsyncMarkoutGroupByAtom atom = task.getAtom();
        final int batchIndex = task.getBatchIndex();
        final MasterRowBatch masterRowBatch = task.getMasterRowBatch();

        // Clear task and release queue slot
        task.clear();
        subSeq.done(cursor);

        startedCounter.incrementAndGet();
        LOG.info().$("run: started [batchIndex=").$(batchIndex).$(", workerId=").$(workerId)
                .$(", circuitBreaker=").$(circuitBreaker)
                .$(", atom=").$(atom)
                .$(", masterRowBatch.size=").$(masterRowBatch != null ? masterRowBatch.size() : -1)
                .$("]").$();

        try {
            if (circuitBreaker.checkIfTripped()) {
                return;
            }

            // Acquire a slot for this worker
            boolean owner = stealingAtom != null && stealingAtom == atom;
            int slotId = atom.maybeAcquire(workerId, owner, circuitBreaker);

            try {
                // Delegate to MarkoutReducer for the actual work
                MarkoutReducer reducer = REDUCER.get();
                reducer.reduce(atom, masterRowBatch, slotId, circuitBreaker);
            } finally {
                atom.release(slotId);
            }

        } catch (Throwable th) {
            LOG.error().$("markout reduce failed [batchIndex=").$(batchIndex)
                    .$(", error=").$(th)
                    .$(", message=").$(th.getMessage())
                    .I$();
            circuitBreaker.cancel();
        } finally {
            doneLatch.countDown();
        }
    }

    @Override
    protected boolean doRun(int workerId, long cursor, RunStatus runStatus) {
        final MarkoutReduceTask task = queue.get(cursor);
        run(workerId, task, subSeq, cursor, null);
        return true;
    }
}
