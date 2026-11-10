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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.O3PartitionJob;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * HORIZON JOIN on a single SYMBOL key whose right-hand column is indexed looks rare keys up in the
 * symbol index (EXPLAIN: "lookup: index"). Every query here must return what the scan returns
 * (cairo.sql.horizon.join.index.scan.rows=-1), over bitmap, posting and covering posting indexes,
 * serial and parallel, with rare, dense, NULL and missing keys across partitions.
 */
public class HorizonJoinIndexedLookupTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(HorizonJoinIndexedLookupTest.class);
    private static final String[] QUERIES = {
            // keyed GROUP BY offset over one rare master key
            "SELECT offset, count() c, count(bid) n, sum(bid) s FROM (SELECT * FROM trades WHERE sym = 'R1') t " +
                    "HORIZON JOIN quotes q ON (sym) LIST (0s, 1s, 30s, 2h) AS h GROUP BY offset ORDER BY offset",
            // keyed by the join key over a mix of rare and dense keys, NULL and a key the quotes lack
            "SELECT t.sym, count() c, count(bid) n, sum(bid) s FROM (SELECT * FROM trades WHERE sym IN ('R0', 'R2', 'D1', 'MISSING')) t " +
                    "HORIZON JOIN quotes q ON (sym) RANGE FROM -1h TO 1h STEP 30m AS h GROUP BY t.sym ORDER BY t.sym",
            // every master row: NULL keys included
            "SELECT t.sym, offset, count() c, count(bid) n, sum(bid) s FROM trades t " +
                    "HORIZON JOIN quotes q ON (sym) LIST (-10s, 0s, 1m) AS h GROUP BY t.sym, offset ORDER BY t.sym, offset",
            // not keyed
            "SELECT count() c, count(bid) n, sum(bid) s FROM trades t HORIZON JOIN quotes q ON (sym) LIST (0s, 5m) AS h",
            // the right-hand side bounded by an interval: its first frame starts above row 0
            "SELECT t.sym, count() c, count(bid) n, sum(bid) s FROM trades t " +
                    "HORIZON JOIN (SELECT * FROM quotes WHERE ts >= '2024-01-02T07:00:00') q ON (sym) LIST (0s, 1h) AS h " +
                    "GROUP BY t.sym ORDER BY t.sym",
    };
    private static final String[] SLAVE_INDEXES = {
            "INDEX",
            "INDEX TYPE POSTING",
            "INDEX TYPE POSTING INCLUDE (bid)",
    };

    @Test
    public void testIndexedLookupMatchesScan() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        for (String slaveIndex : SLAVE_INDEXES) {
            for (boolean parallel : new boolean[]{false, true}) {
                assertMemoryLeak(() -> {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HORIZON_JOIN_ENABLED, String.valueOf(parallel));
                    createTables(rnd, slaveIndex);
                    for (String query : QUERIES) {
                        setProperty(PropertyKey.CAIRO_SQL_HORIZON_JOIN_INDEX_SCAN_ROWS, -1);
                        final StringSink expected = new StringSink();
                        printSql(query, expected);
                        assertPlanHasIndexLookup(query, false);

                        // 0: every key whose frame the index can count reads the index;
                        // a large budget: only the rarest keys do
                        for (long budget : new long[]{0, 1 + rnd.nextInt(64), 1 + rnd.nextInt(4096)}) {
                            setProperty(PropertyKey.CAIRO_SQL_HORIZON_JOIN_INDEX_SCAN_ROWS, budget);
                            assertPlanHasIndexLookup(query, true);
                            final StringSink actual = new StringSink();
                            printSql(query, actual);
                            TestUtils.assertEquals(slaveIndex + ", parallel=" + parallel + ", budget=" + budget + ": " + query, expected, actual);
                        }
                    }
                    execute("DROP TABLE trades");
                    execute("DROP TABLE quotes");
                });
            }
        }
    }

    @Test
    public void testIndexedLookupOnWorkersUnderConcurrentIngest() throws Exception {
        // Workers read the index from table readers of their own, at the shared reader's version or
        // not at all. A writer appends quotes a day past the last trade, which moves the version but
        // not the result, while readers run the parallel join on worker threads.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        // round(): the parallel sum adds the doubles in a varying order
        final String query = "SELECT t.sym, offset, count() c, count(bid) n, round(sum(bid), 6) s FROM trades t " +
                "HORIZON JOIN quotes q ON (sym) LIST (-10s, 0s, 1m, 2h) AS h GROUP BY t.sym, offset ORDER BY t.sym, offset";
        final int readerCount = 3;
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(rnd));
            try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, pool.getWorkerCount())) {
                TestUtils.setupWorkerPool(pool, engine);
                pool.start(LOG);
                try {
                    createTables(rnd, "INDEX TYPE POSTING");
                    execute("ALTER TABLE quotes SET TYPE WAL");
                    engine.releaseInactive();
                    TestUtils.drainWalQueue(engine);
                    context.setParallelHorizonJoinEnabled(true);
                    setProperty(PropertyKey.CAIRO_SQL_HORIZON_JOIN_INDEX_SCAN_ROWS, -1);
                    final StringSink expected = new StringSink();
                    TestUtils.printSql(engine, context, query, expected);
                    setProperty(PropertyKey.CAIRO_SQL_HORIZON_JOIN_INDEX_SCAN_ROWS, rnd.nextInt(256));

                    final ConcurrentHashMap<Integer, Throwable> errors = new ConcurrentHashMap<>();
                    final AtomicBoolean isWriterDone = new AtomicBoolean();
                    final CyclicBarrier barrier = new CyclicBarrier(readerCount + 1);
                    final SOCountDownLatch haltLatch = new SOCountDownLatch(readerCount + 1);
                    final int workerCount = context.getSharedQueryWorkerCount();
                    new Thread(() -> {
                        try (SqlExecutionContextImpl writerContext = TestUtils.createSqlExecutionCtx(engine, workerCount)) {
                            TestUtils.await(barrier);
                            for (int i = 0; i < 20; i++) {
                                engine.execute(
                                        "INSERT INTO quotes SELECT '2024-01-06'::TIMESTAMP + (" + i * 100 + " + x) * 1_000_000L, " +
                                                "rnd_symbol('R0', 'R1', 'D0', 'D1'), rnd_double() FROM long_sequence(100)",
                                        writerContext
                                );
                                TestUtils.drainWalQueue(engine);
                            }
                        } catch (Throwable th) {
                            errors.put(-1, th);
                        } finally {
                            isWriterDone.set(true);
                            Path.clearThreadLocals();
                            Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
                            haltLatch.countDown();
                        }
                    }).start();
                    for (int i = 0; i < readerCount; i++) {
                        final int readerId = i;
                        new Thread(() -> {
                            final StringSink actual = new StringSink();
                            try (SqlExecutionContextImpl readerContext = TestUtils.createSqlExecutionCtx(engine, workerCount)) {
                                readerContext.setParallelHorizonJoinEnabled(true);
                                TestUtils.await(barrier);
                                for (int runs = 0; !isWriterDone.get() || runs < 5; runs++) {
                                    TestUtils.printSql(engine, readerContext, query, actual);
                                    TestUtils.assertEquals(expected, actual);
                                }
                            } catch (Throwable th) {
                                errors.put(readerId, th);
                            } finally {
                                Path.clearThreadLocals();
                                haltLatch.countDown();
                            }
                        }).start();
                    }
                    haltLatch.await();
                    if (!errors.isEmpty()) {
                        final Throwable first = errors.values().iterator().next();
                        throw new AssertionError("errors in " + errors.size() + " thread(s), first: " + first, first);
                    }
                    final StringSink plan = new StringSink();
                    TestUtils.printSql(engine, context, "EXPLAIN " + query, plan);
                    Assert.assertTrue(plan.toString(), plan.toString().contains("Async Horizon Join") && plan.toString().contains("lookup: index"));
                } finally {
                    pool.haltAndAssertCleanForTest(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
                }
            }
        });
    }

    @Test
    public void testNoIndexHintKeepsScan() throws Exception {
        assertMemoryLeak(() -> {
            createTables(TestUtils.generateRandom(LOG), "INDEX TYPE POSTING");
            final String query = "SELECT /*+ no_index */ offset, count() c FROM trades t HORIZON JOIN quotes q ON (sym) LIST (0s) AS h GROUP BY offset";
            assertPlanHasIndexLookup(query, false);
        });
    }

    @Test
    public void testNotIndexedKeepsScan() throws Exception {
        assertMemoryLeak(() -> {
            createTables(TestUtils.generateRandom(LOG), "");
            assertPlanHasIndexLookup(QUERIES[0], false);
        });
    }

    @Test
    public void testSlaveFilterKeepsScan() throws Exception {
        assertMemoryLeak(() -> {
            createTables(TestUtils.generateRandom(LOG), "INDEX TYPE POSTING");
            assertPlanHasIndexLookup(
                    "SELECT offset, count() c FROM trades t HORIZON JOIN (SELECT * FROM quotes WHERE bid > 0.5) q ON (sym) LIST (0s) AS h GROUP BY offset",
                    false
            );
        });
    }

    private void assertPlanHasIndexLookup(String query, boolean expected) throws Exception {
        final StringSink plan = new StringSink();
        printSql("EXPLAIN " + query, plan);
        Assert.assertEquals(plan.toString(), expected, plan.toString().contains("lookup: index"));
    }

    // Quotes over four daily partitions: five dense keys, three rare ones (R0 only in the first
    // partition), NULLs; trades of the same keys plus one the quotes lack.
    private void createTables(Rnd rnd, String slaveIndex) throws Exception {
        execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL " + slaveIndex + ", bid DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
        final long quoteRows = 20_000 + rnd.nextInt(20_000);
        final long quoteStepMicros = 4L * 24 * 3_600_000_000L / quoteRows;
        execute(
                "INSERT INTO quotes SELECT timestamp_sequence('2024-01-01', " + quoteStepMicros + ") ts, " +
                        "CASE WHEN x % 1_499 = 0 AND x < " + quoteRows / 4 + " THEN 'R0' " +
                        "WHEN x % 2_003 = 0 THEN 'R1' " +
                        "WHEN x % 997 = 0 THEN 'R2' " +
                        "WHEN x % 41 = 0 THEN NULL " +
                        "ELSE 'D' || (x % 5) END sym, " +
                        "rnd_double() bid FROM long_sequence(" + quoteRows + ")"
        );
        execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
        final long tradeRows = 1_000 + rnd.nextInt(2_000);
        final long tradeStepMicros = 4L * 24 * 3_600_000_000L / tradeRows;
        execute(
                "INSERT INTO trades SELECT timestamp_sequence('2024-01-01T00:00:01', " + tradeStepMicros + ") ts, " +
                        "rnd_symbol('R0', 'R1', 'R2', 'D0', 'D1', 'D4', 'MISSING', NULL) sym, " +
                        "rnd_double() price FROM long_sequence(" + tradeRows + ")"
        );
    }
}
