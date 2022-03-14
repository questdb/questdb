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

import io.questdb.Metrics;
import io.questdb.cairo.O3Utils;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.jit.JitUtil;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.Misc;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class AsyncFilteredRecordCursorFactoryTest extends AbstractGriffinTest {

    @Test
    public void testSimpleJitDisabled() throws Exception {
        testSimple(SqlJitMode.JIT_MODE_DISABLED, AsyncFilteredRecordCursorFactory.class);
    }

    @Test
    public void testSimpleJitEnabled() throws Exception {
        // Disable the test on ARM64.
        Assume.assumeTrue(JitUtil.isJitSupported());

        testSimple(SqlJitMode.JIT_MODE_ENABLED, AsyncJitFilteredRecordCursorFactory.class);
    }

    private void testSimple(int jitMode, Class<?> expectedFactoryClass) throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setJitMode(jitMode);

            WorkerPool pool = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public int[] getWorkerAffinity() {
                    return new int[]{-1, -1, -1, -1};
                }

                @Override
                public int getWorkerCount() {
                    return 4;
                }

                @Override
                public boolean haltOnError() {
                    return false;
                }
            }, Metrics.disabled());

            O3Utils.setupWorkerPool(
                    pool,
                    messageBus,
                    null
            );
            pool.start(null);

            try {
                compiler.compile("create table x as (select rnd_double() a, timestamp_sequence(20000000, 100000) t from long_sequence(5000000)) timestamp(t) partition by day", sqlExecutionContext);
                try (RecordCursorFactory f = compiler.compile("x where a > 0.34", sqlExecutionContext).getRecordCursorFactory()) {

                    Assert.assertEquals(expectedFactoryClass, f.getClass());
                    SCSequence subSeq = new SCSequence();
                    PageFrameSequence<?> frameSequence = f.execute(sqlExecutionContext, subSeq);

                    final RingQueue<PageFrameReduceTask> queue = frameSequence.getPageFrameReduceQueue();
                    int frameCount = 0;

                    while (true) {
                        long cursor = subSeq.nextBully();
                        PageFrameReduceTask task = queue.get(cursor);
                        if (task.getFrameSequence() == frameSequence) {
                            frameCount++;
                            task.collected();
                            if (frameCount == task.getFrameSequence().getFrameCount()) {
                                subSeq.done(cursor);
                                break;
                            }
                        }
                        subSeq.done(cursor);
                    }
                    frameSequence.await();
                    Misc.free(frameSequence.getSymbolTableSource());
                }
            } catch (Throwable e) {
                e.printStackTrace();
                throw e;
            } finally {
                pool.halt();
            }
        });
    }
}
