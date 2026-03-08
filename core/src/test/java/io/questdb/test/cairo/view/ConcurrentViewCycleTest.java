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

package io.questdb.test.cairo.view;

import io.questdb.cairo.CairoException;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Chars;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ConcurrentViewCycleTest extends AbstractViewTest {

    /**
     * Two threads race to create a mutual cycle: v1 -> v2 and v2 -> v1.
     * Both operations would create a cycle, but only one can succeed at a time.
     * The other must detect the cycle and fail.
     */
    @Test
    public void testConcurrentAlterRaceToCycle() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            // Both views initially depend on table1 (independent of each other)
            execute("CREATE VIEW " + VIEW1 + " AS (select * from " + TABLE1 + ")");
            execute("CREATE VIEW " + VIEW2 + " AS (select * from " + TABLE1 + ")");
            drainWalAndViewQueues();

            final int iterations = 50;
            final CyclicBarrier barrier = new CyclicBarrier(3);
            final AtomicReference<Throwable> error = new AtomicReference<>();
            final AtomicInteger cycleDetectedCount = new AtomicInteger();
            final AtomicInteger queryCount = new AtomicInteger();
            final AtomicBoolean done = new AtomicBoolean(false);

            // Thread 1: alternates v1 between depending on v2 and table1
            Thread thread1 = new Thread(() -> {
                try (SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine)) {
                    barrier.await();
                    for (int i = 0; i < iterations && error.get() == null; i++) {
                        try {
                            if (i % 2 == 0) {
                                // Try to make v1 -> v2 (cycle if v2 -> v1)
                                execute("ALTER VIEW " + VIEW1 + " AS (select * from " + VIEW2 + ")", ctx);
                            } else {
                                // Reset v1 -> table1 (no cycle)
                                execute("ALTER VIEW " + VIEW1 + " AS (select * from " + TABLE1 + ")", ctx);
                            }
                        } catch (SqlException | CairoException e) {
                            if (Chars.contains(e.getFlyweightMessage(), "circular dependency detected")) {
                                cycleDetectedCount.incrementAndGet();
                            } else {
                                throw e;
                            }
                        }
                    }
                } catch (Throwable e) {
                    error.set(e);
                } finally {
                    Path.clearThreadLocals();
                }
            }, "alter-v1-thread");

            // Thread 2: alternates v2 between depending on v1 and table1
            Thread thread2 = new Thread(() -> {
                try (SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine)) {
                    barrier.await();
                    for (int i = 0; i < iterations && error.get() == null; i++) {
                        try {
                            if (i % 2 == 0) {
                                // Try to make v2 -> v1 (cycle if v1 -> v2)
                                execute("ALTER VIEW " + VIEW2 + " AS (select * from " + VIEW1 + ")", ctx);
                            } else {
                                // Reset v2 -> table1 (no cycle)
                                execute("ALTER VIEW " + VIEW2 + " AS (select * from " + TABLE1 + ")", ctx);
                            }
                        } catch (SqlException | CairoException e) {
                            if (Chars.contains(e.getFlyweightMessage(), "circular dependency detected")) {
                                cycleDetectedCount.incrementAndGet();
                            } else {
                                throw e;
                            }
                        }
                    }
                } catch (Throwable e) {
                    error.set(e);
                } finally {
                    Path.clearThreadLocals();
                }
            }, "alter-v2-thread");

            // Thread 3: continuously queries both views - should always work
            Thread reader = new Thread(() -> {
                try (SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine)) {
                    barrier.await();
                    String expected = """
                            QUERY PLAN
                            PageFrame
                            &nbsp;&nbsp;&nbsp;&nbsp;Row forward scan
                            &nbsp;&nbsp;&nbsp;&nbsp;Frame forward scan on: table1
                            """;
                    StringSink sink = new StringSink();
                    while (!done.get() && error.get() == null) {
                        sink.clear();
                        engine.print("EXPLAIN SELECT * FROM " + VIEW1, sink, ctx);
                        TestUtils.assertEquals(expected, sink);

                        sink.clear();
                        engine.print("EXPLAIN SELECT * FROM " + VIEW2, sink, ctx);
                        TestUtils.assertEquals(expected, sink);

                        queryCount.incrementAndGet();
                    }
                } catch (SqlException e) {
                    // Circular reference can be detected at query time if ALTER VIEW created a cycle
                    // between when cycle detection ran and when the view graph was updated
                    if (!Chars.contains(e.getFlyweightMessage(), "circular view reference detected")) {
                        error.set(e);
                    }
                } catch (Throwable e) {
                    error.set(e);
                } finally {
                    Path.clearThreadLocals();
                }
            }, "reader-thread");

            thread1.start();
            thread2.start();
            reader.start();

            thread1.join();
            thread2.join();
            done.set(true);
            reader.join();

            if (error.get() != null) {
                throw new RuntimeException(error.get());
            }

            drainWalAndViewQueues();

            // Verify views are still queryable (no corrupted state)
            assertSql(
                    """
                            column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey
                            ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse
                            k\tSYMBOL\tfalse\t0\tfalse\t0\t0\tfalse\tfalse
                            k2\tSYMBOL\tfalse\t0\tfalse\t0\t0\tfalse\tfalse
                            v\tLONG\tfalse\t0\tfalse\t0\t0\tfalse\tfalse
                            """,
                    "SHOW COLUMNS FROM " + VIEW1
            );

            LOG.info().$("Cycles detected: ").$(cycleDetectedCount.get())
                    .$(", queries executed: ").$(queryCount.get()).$();
        });
    }
}
