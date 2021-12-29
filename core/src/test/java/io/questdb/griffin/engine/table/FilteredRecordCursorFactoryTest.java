/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.sql.ExecutionToken;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.async.PageFrameCleanupJob;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.cairo.sql.async.PageFrameDispatchJob;
import io.questdb.cairo.sql.async.PageFrameReduceJob;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import org.junit.Test;

public class FilteredRecordCursorFactoryTest extends AbstractGriffinTest {

    @Test
    public void testSimple() throws SqlException {

        WorkerPool pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public int[] getWorkerAffinity() {
                return new int[]{ -1, -1, -1, -1};
            }

            @Override
            public int getWorkerCount() {
                return 4;
            }

            @Override
            public boolean haltOnError() {
                return false;
            }
        });

        pool.assign(new PageFrameDispatchJob(engine.getMessageBus(), pool.getWorkerCount()));
        for (int i = 0, n = pool.getWorkerCount(); i < n; i++) {
            pool.assign(i, new PageFrameReduceJob(
                    engine.getMessageBus(),
                    sqlExecutionContext.getRandom(),
                    pool.getWorkerCount())
            );
        }
        pool.assign(0, new PageFrameCleanupJob(engine.getMessageBus(), sqlExecutionContext.getRandom()));

        pool.start(null);

        try {
            compiler.compile("create table x as (select rnd_double() a, timestamp_sequence(20000000, 100000) t from long_sequence(20000000)) timestamp(t) partition by hour", sqlExecutionContext);
            try (RecordCursorFactory f = compiler.compile("x where a > 0.34", sqlExecutionContext).getRecordCursorFactory()) {
                SCSequence subSeq = new SCSequence();
                ExecutionToken executionToken = f.execute(sqlExecutionContext, subSeq);

                RingQueue<PageFrameReduceTask> queue = executionToken.getQueue();
                long producerId = executionToken.getProducerId();

                int frameCount = 0;

                while (true) {
                    long cursor = subSeq.nextBully();
                    PageFrameReduceTask task = queue.get(cursor);
                    if (task.getProducerId() == producerId) {
                        frameCount++;
                        task.getFramesCollectedCounter().incrementAndGet();
                        if (frameCount == task.getFrameCount()) {
                            subSeq.done(cursor);
                            break;
                        }
                    }
                    subSeq.done(cursor);
                }
            }
        } finally {
            pool.halt();
        }
    }
}