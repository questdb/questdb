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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.HashMap;

/**
 * Guards why {@code SampleByNanoTimestampConfigTest} and {@code SampleByConfigTest} drive their
 * live-{@link ServerMain} SAMPLE BY assertions with {@code .noMemoryUsageCheck()}.
 * <p>
 * {@link QueryAssertion}'s post-close check ({@code assertFactoryMemoryUsage}) bounds the delta of
 * {@code getMemUsedByFactories()} -- a sum of process-global per-tag counters -- to 64 KiB. On a
 * live server that delta is not attributable to the cursor: any factory-tagged native memory a
 * concurrent query/background component holds across the post-close reading inflates it. This test
 * holds such memory across the assertion window; it passes because {@code .noMemoryUsageCheck()}
 * skips the bound, and it turns red if that opt-out is ever dropped from this pattern.
 */
public class SampleByNanoMemUsageCheckTest extends AbstractBootstrapTest {

    private static final CharSequence DDL = "CREATE TABLE sensors (\n" +
            "ts TIMESTAMP_NS,\n" +
            "val INT\n" +
            ") TIMESTAMP(ts) PARTITION BY DAY WAL";
    private static final CharSequence DML = "INSERT INTO sensors (ts, val) VALUES \n" +
            "('2021-05-31T23:10:00.000000001Z', 10),\n" +
            "('2021-06-01T01:10:00.000000001Z', 80),\n" +
            "('2021-06-01T07:20:00.000000001Z', 15),\n" +
            "('2021-06-01T13:20:00.000000001Z', 10),\n" +
            "('2021-06-01T19:20:00.000000001Z', 40),\n" +
            "('2021-06-02T01:10:00.000000001Z', 90),\n" +
            "('2021-06-02T07:20:00.000000001Z', 30)";
    private static final CharSequence EXPECTED_CALENDAR = "ts\tcount\n" +
            "2021-05-31T00:00:00.000000000Z\t1\n" +
            "2021-06-01T00:00:00.000000000Z\t4\n" +
            "2021-06-02T00:00:00.000000000Z\t2\n";
    private static final CharSequence QUERY = "SELECT ts, count from sensors\n" +
            "SAMPLE BY 1d";

    @Test
    public void testNoMemoryUsageCheckToleratesConcurrentFactoryAllocation() throws Exception {
        try (final ServerMain serverMain = ServerMain.createWithoutWalApplyJob(root, new HashMap<>())) {
            serverMain.start();
            final CairoEngine engine = serverMain.getEngine();
            try (SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine)) {
                engine.execute(DDL, ctx);
                engine.execute(DML, ctx);
                drainWalQueue(engine);

                final FactoryTagHolder holder = new FactoryTagHolder();
                try {
                    // prepareHook runs immediately before QueryAssertion takes its baseline snapshot,
                    // so the holder's allocations land after the snapshot and are still outstanding
                    // at the post-close reading -- like concurrent work on a shared server. Without
                    // .noMemoryUsageCheck() the check would attribute the held memory to this (clean)
                    // cursor and fail with "cursor kept N KiB".
                    new QueryAssertion(engine, ctx, holder::start, QUERY)
                            .noLeakCheck()
                            .noMemoryUsageCheck()
                            .timestamp("ts")
                            .supportsRandomAccess(true)
                            .expectSize(true)
                            .returns(EXPECTED_CALENDAR);
                } finally {
                    holder.stopAndFree();
                }
            }
        }
    }

    // Simulates a concurrent live-server component (e.g. another GROUP BY query) holding
    // factory-tagged native memory. Starts after the assertion's baseline snapshot and keeps adding
    // 128 KiB blocks (one per ms, capped at 8 MiB) so the global NATIVE_GROUP_BY_FUNCTION counter is
    // elevated by far more than 64 KiB for the whole assertion window. stopAndFree() releases it all.
    private static final class FactoryTagHolder {
        private static final long BLOCK = 128 * 1024;
        private static final int MAX_BLOCKS = 64; // backstop: at most 8 MiB
        private final LongList ptrs = new LongList();
        private volatile boolean running;
        private Thread thread;

        void start() {
            running = true;
            thread = new Thread(() -> {
                int n = 0;
                while (running && n < MAX_BLOCKS) {
                    try {
                        // sleep first so the first block lands strictly after the baseline snapshot
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        break;
                    }
                    long p = Unsafe.malloc(BLOCK, MemoryTag.NATIVE_GROUP_BY_FUNCTION);
                    ptrs.add(p);
                    n++;
                }
            }, "factory-tag-holder");
            thread.setDaemon(true);
            thread.start();
        }

        void stopAndFree() {
            running = false;
            if (thread != null) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            // thread is dead here, so ptrs is stable
            for (int i = 0, n = ptrs.size(); i < n; i++) {
                Unsafe.free(ptrs.getQuick(i), BLOCK, MemoryTag.NATIVE_GROUP_BY_FUNCTION);
            }
            ptrs.clear();
        }
    }
}
