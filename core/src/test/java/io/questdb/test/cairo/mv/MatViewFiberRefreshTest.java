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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.mp.continuation.ContinuationQueue;
import io.questdb.mp.continuation.QueryFiber;
import io.questdb.mp.continuation.QueryFiberPool;
import io.questdb.mp.continuation.WorkerContinuation;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Validates fiber-mode materialized view refresh: the refresh job launches each
 * refresh as a QueryTask on a pooled fiber instead of executing inline. The test
 * drains the fibers' resume queue on the test thread (a plain, cont-free carrier,
 * standing in for a worker's outer driver) and asserts the refreshed view matches
 * the legacy path's results, with a single fiber reused across all refreshes.
 */
public class MatViewFiberRefreshTest extends AbstractCairoTest {

    @Test
    public void testFiberModeIncrementalAndFullRefresh() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            execute(
                    "create materialized view price_1h as " +
                            "select sym, last(price) as price, ts from base_price sample by 1h"
            );
            execute(
                    "insert into base_price (sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2024-09-10T12:02')"
            );
            drainWalQueue();

            final ContinuationQueue fiberQueue = new ContinuationQueue();
            try (
                    // capacity 1 exercises the launch throttle: each job.run() call
                    // dispatches one refresh and leaves the rest queued in the state
                    // store, so the whole test provably runs on a single fiber
                    QueryFiberPool fiberPool = new QueryFiberPool(1, fiberQueue);
                    MatViewRefreshJob job = new MatViewRefreshJob(engine, 1, fiberPool)
            ) {
                // incremental refresh, executed on a fiber
                driveFiberRefresh(job, fiberQueue);
                assertQuery("price_1h").noLeakCheck().expectSize().timestamp("ts").returns(
                        "sym\tprice\tts\n" +
                                "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n"
                );

                // second incremental round reuses the same fiber
                execute("insert into base_price (sym, price, ts) values('gbpusd', 1.5, '2024-09-10T13:01')");
                drainWalQueue();
                driveFiberRefresh(job, fiberQueue);
                assertQuery("price_1h").noLeakCheck().expectSize().timestamp("ts").returns(
                        "sym\tprice\tts\n" +
                                "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                                "gbpusd\t1.5\t2024-09-10T13:00:00.000000Z\n"
                );

                // full refresh goes through the fiber path as well
                execute("refresh materialized view price_1h full;");
                driveFiberRefresh(job, fiberQueue);
                assertQuery("price_1h").noLeakCheck().expectSize().timestamp("ts").returns(
                        "sym\tprice\tts\n" +
                                "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                                "gbpusd\t1.5\t2024-09-10T13:00:00.000000Z\n"
                );

                // every refresh in this test ran on one pooled, reused fiber
                Assert.assertEquals(1, fiberPool.getCreatedCount());
                Assert.assertEquals(0, fiberPool.getBusyCount());
                Assert.assertEquals(1, fiberPool.getPooledCount());
            }
        });
    }

    private static int drainFiberQueue(ContinuationQueue queue) {
        final ContinuationQueue.ResumeTask scratch = new ContinuationQueue.ResumeTask();
        int count = 0;
        WorkerContinuation cont;
        while ((cont = queue.tryDequeue(scratch)) != null) {
            cont.run();
            QueryFiber.reclaimIfIdle(cont);
            count++;
        }
        return count;
    }

    private static void driveFiberRefresh(MatViewRefreshJob job, ContinuationQueue fiberQueue) {
        boolean useful = true;
        while (useful) {
            useful = job.run();
            useful |= drainFiberQueue(fiberQueue) > 0;
            drainWalQueue();
        }
    }
}
