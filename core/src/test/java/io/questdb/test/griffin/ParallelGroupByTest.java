/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;

import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class ParallelGroupByTest extends AbstractCairoTest {
    private static final int PAGE_FRAME_COUNT = 4; // also used to set queue size, so must be a power of 2
    private static final int PAGE_FRAME_MAX_ROWS = 100;
    private static final int ROW_COUNT = PAGE_FRAME_COUNT * PAGE_FRAME_MAX_ROWS;

    private final boolean enableParallelGroupBy;

    public ParallelGroupByTest(boolean enableParallelGroupBy) {
        this.enableParallelGroupBy = enableParallelGroupBy;
    }

    @Parameterized.Parameters(name = "parallel={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {true},
                {false}
        });
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        pageFrameMaxRows = PAGE_FRAME_MAX_ROWS;
        // We intentionally use small values for shard count and reduce
        // queue capacity to exhibit various edge cases.
        pageFrameReduceShardCount = 2;
        pageFrameReduceQueueCapacity = PAGE_FRAME_COUNT;

        AbstractCairoTest.setUpStatic();
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        configOverrideParallelGroupByEnabled(enableParallelGroupBy);
    }

    @Test
    public void testGroupByOverJoin() throws Exception {
        // Parallel GROUP BY shouldn't kick in on this query, yet we want
        // to make sure the result correctness.
        Assume.assumeTrue(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            ddl(
                    "CREATE TABLE t (\n" +
                            "  created timestamp,\n" +
                            "  event short,\n" +
                            "  origin short\n" +
                            ") TIMESTAMP(created) PARTITION BY DAY;"
            );
            insert("INSERT INTO t VALUES ('2023-09-21T10:00:00.000000Z', 1, 1);");
            insert("INSERT INTO t VALUES ('2023-09-21T11:00:00.000000Z', 1, 1);");

            assertQuery(
                    "count\n" +
                            "2\n",
                    "SELECT count(1)\n" +
                            "FROM t as T1 JOIN t as T2 ON T1.created = T2.created\n" +
                            "WHERE T1.event = 1.0",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testGroupByOverLatestBy() throws Exception {
        // Parallel GROUP BY shouldn't kick in on this query, yet we want
        // to make sure the result correctness.
        Assume.assumeTrue(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            ddl(
                    "CREATE TABLE t (\n" +
                            "  created timestamp,\n" +
                            "  event symbol,\n" +
                            "  origin symbol\n" +
                            ") TIMESTAMP(created) PARTITION BY DAY;"
            );
            insert("INSERT INTO t VALUES ('2023-09-21T10:00:00.000000Z', 'a', 'c');");
            insert("INSERT INTO t VALUES ('2023-09-21T10:00:01.000000Z', 'a', 'c');");
            insert("INSERT INTO t VALUES ('2023-09-21T10:00:02.000000Z', 'a', 'd');");
            insert("INSERT INTO t VALUES ('2023-09-21T10:00:00.000000Z', 'b', 'c');");
            insert("INSERT INTO t VALUES ('2023-09-21T10:00:01.000000Z', 'b', 'c');");

            assertQuery(
                    "count\n" +
                            "2\n",
                    "SELECT count()\n" +
                            "FROM t\n" +
                            "WHERE origin = 'c'\n" +
                            "LATEST ON created PARTITION BY event",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testGroupByOverUnion() throws Exception {
        // Parallel GROUP BY shouldn't kick in on this query, yet we want
        // to make sure the result correctness.
        Assume.assumeTrue(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            ddl(
                    "CREATE TABLE t1 (\n" +
                            "  created timestamp,\n" +
                            "  event short,\n" +
                            "  origin short\n" +
                            ") TIMESTAMP(created) PARTITION BY DAY;"
            );
            insert("INSERT INTO t1 VALUES ('2023-09-21T10:00:00.000000Z', 1, 1);");
            insert("INSERT INTO t1 VALUES ('2023-09-21T10:00:01.000000Z', 2, 2);");

            ddl(
                    "CREATE TABLE t2 (\n" +
                            "  created timestamp,\n" +
                            "  event short,\n" +
                            "  origin short\n" +
                            ") TIMESTAMP(created) PARTITION BY DAY;"
            );
            insert("INSERT INTO t2 VALUES ('2023-09-21T10:00:02.000000Z', 3, 1);");
            insert("INSERT INTO t2 VALUES ('2023-09-21T10:00:00.000000Z', 4, 2);");

            assertQuery(
                    "event\tcount\n" +
                            "1\t1\n" +
                            "3\t1\n",
                    "SELECT event, count()\n" +
                            "FROM (t1 UNION t2) WHERE origin = 1",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testNonKeyedGroupByEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool((() -> 4));
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                        ddl(
                                compiler,
                                "CREATE TABLE tab (" +
                                        "  ts TIMESTAMP," +
                                        "  price DOUBLE," +
                                        "  quantity LONG) timestamp (ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        assertQueries(
                                engine,
                                sqlExecutionContext,
                                "select vwap(price, quantity) from tab",
                                "vwap\n" +
                                        "NaN\n"
                        );
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testParallelFunctionKeyedExplicitGroupBy() throws Exception {
        testParallelSymbolKeyedGroupBy(
                "SELECT day_of_week(ts) day, key, vwap(price, quantity), sum(colTop) FROM tab GROUP BY day, key ORDER BY day",
                "day\tkey\tvwap\tsum\n" +
                        "1\tk0\t423.57142857142856\t4675.0\n" +
                        "1\tk1\t423.987012987013\t4235.0\n" +
                        "1\tk2\t424.98586572438165\t4245.0\n" +
                        "1\tk3\t425.9847238542891\t4255.0\n" +
                        "1\tk4\t426.9835873388042\t4265.0\n" +
                        "4\tk1\t70.23753665689149\t735.0\n" +
                        "4\tk2\t71.00576368876081\t745.0\n" +
                        "4\tk3\t71.78186968838527\t755.0\n" +
                        "4\tk4\t72.56545961002786\t765.0\n" +
                        "4\tk0\t70.07692307692308\t675.0\n" +
                        "5\tk0\t153.135593220339\t2950.0\n" +
                        "5\tk1\t154.0976430976431\t2970.0\n" +
                        "5\tk2\t155.06020066889633\t2990.0\n" +
                        "5\tk3\t156.02325581395348\t3010.0\n" +
                        "5\tk4\t156.98679867986797\t3030.0\n" +
                        "6\tk0\t250.85858585858585\t4950.0\n" +
                        "6\tk1\t251.8450704225352\t4970.0\n" +
                        "6\tk2\t252.8316633266533\t4990.0\n" +
                        "6\tk3\t253.8183632734531\t5010.0\n" +
                        "6\tk4\t254.8051689860835\t5030.0\n" +
                        "7\tk0\t349.89208633093523\t6950.0\n" +
                        "7\tk1\t350.8852223816356\t6970.0\n" +
                        "7\tk2\t351.87839771101574\t6990.0\n" +
                        "7\tk3\t352.8716119828816\t7010.0\n" +
                        "7\tk4\t353.86486486486484\t7030.0\n"
        );
    }

    @Test
    public void testParallelFunctionKeyedGroupByMultipleKeys() throws Exception {
        testParallelSymbolKeyedGroupBy(
                "SELECT vwap(price, quantity), day_of_week(ts) day, sum(colTop), concat(key, 'abc')::symbol key FROM tab ORDER BY day, key",
                "vwap\tday\tsum\tkey\n" +
                        "423.57142857142856\t1\t4675.0\tk0abc\n" +
                        "423.987012987013\t1\t4235.0\tk1abc\n" +
                        "424.98586572438165\t1\t4245.0\tk2abc\n" +
                        "425.9847238542891\t1\t4255.0\tk3abc\n" +
                        "426.9835873388042\t1\t4265.0\tk4abc\n" +
                        "70.07692307692308\t4\t675.0\tk0abc\n" +
                        "70.23753665689149\t4\t735.0\tk1abc\n" +
                        "71.00576368876081\t4\t745.0\tk2abc\n" +
                        "71.78186968838527\t4\t755.0\tk3abc\n" +
                        "72.56545961002786\t4\t765.0\tk4abc\n" +
                        "153.135593220339\t5\t2950.0\tk0abc\n" +
                        "154.0976430976431\t5\t2970.0\tk1abc\n" +
                        "155.06020066889633\t5\t2990.0\tk2abc\n" +
                        "156.02325581395348\t5\t3010.0\tk3abc\n" +
                        "156.98679867986797\t5\t3030.0\tk4abc\n" +
                        "250.85858585858585\t6\t4950.0\tk0abc\n" +
                        "251.8450704225352\t6\t4970.0\tk1abc\n" +
                        "252.8316633266533\t6\t4990.0\tk2abc\n" +
                        "253.8183632734531\t6\t5010.0\tk3abc\n" +
                        "254.8051689860835\t6\t5030.0\tk4abc\n" +
                        "349.89208633093523\t7\t6950.0\tk0abc\n" +
                        "350.8852223816356\t7\t6970.0\tk1abc\n" +
                        "351.87839771101574\t7\t6990.0\tk2abc\n" +
                        "352.8716119828816\t7\t7010.0\tk3abc\n" +
                        "353.86486486486484\t7\t7030.0\tk4abc\n"
        );
    }

    @Test
    public void testParallelFunctionKeyedGroupByThreadSafe() throws Exception {
        testParallelSymbolKeyedGroupBy(
                "SELECT day_of_week(ts) day, key, vwap(price, quantity), sum(colTop) FROM tab ORDER BY day",
                "day\tkey\tvwap\tsum\n" +
                        "1\tk0\t423.57142857142856\t4675.0\n" +
                        "1\tk1\t423.987012987013\t4235.0\n" +
                        "1\tk2\t424.98586572438165\t4245.0\n" +
                        "1\tk3\t425.9847238542891\t4255.0\n" +
                        "1\tk4\t426.9835873388042\t4265.0\n" +
                        "4\tk1\t70.23753665689149\t735.0\n" +
                        "4\tk2\t71.00576368876081\t745.0\n" +
                        "4\tk3\t71.78186968838527\t755.0\n" +
                        "4\tk4\t72.56545961002786\t765.0\n" +
                        "4\tk0\t70.07692307692308\t675.0\n" +
                        "5\tk0\t153.135593220339\t2950.0\n" +
                        "5\tk1\t154.0976430976431\t2970.0\n" +
                        "5\tk2\t155.06020066889633\t2990.0\n" +
                        "5\tk3\t156.02325581395348\t3010.0\n" +
                        "5\tk4\t156.98679867986797\t3030.0\n" +
                        "6\tk0\t250.85858585858585\t4950.0\n" +
                        "6\tk1\t251.8450704225352\t4970.0\n" +
                        "6\tk2\t252.8316633266533\t4990.0\n" +
                        "6\tk3\t253.8183632734531\t5010.0\n" +
                        "6\tk4\t254.8051689860835\t5030.0\n" +
                        "7\tk0\t349.89208633093523\t6950.0\n" +
                        "7\tk1\t350.8852223816356\t6970.0\n" +
                        "7\tk2\t351.87839771101574\t6990.0\n" +
                        "7\tk3\t352.8716119828816\t7010.0\n" +
                        "7\tk4\t353.86486486486484\t7030.0\n"
        );
    }

    @Test
    public void testParallelFunctionKeyedGroupByThreadUnsafe() throws Exception {
        testParallelSymbolKeyedGroupBy(
                "SELECT concat(key, 'abc')::symbol key, vwap(price, quantity), sum(colTop) FROM tab ORDER BY key",
                "key\tvwap\tsum\n" +
                        "k0abc\t288.84615384615387\t20200.0\n" +
                        "k1abc\t285.9440715883669\t19880.0\n" +
                        "k2abc\t286.6659242761693\t19960.0\n" +
                        "k3abc\t287.390243902439\t20040.0\n" +
                        "k4abc\t288.1169977924945\t20120.0\n"
        );
    }

    @Test
    public void testParallelMultiKeyedGroupBy() throws Exception {
        testParallelMultiKeyedGroupBy(
                "SELECT key1, key2, avg(value), sum(colTop) FROM tab",
                "key1\tkey2\tavg\tsum\n" +
                        "k1\tk1\t223.5\t19880.0\n" +
                        "k2\tk2\t224.5\t19960.0\n" +
                        "k3\tk3\t225.5\t20040.0\n" +
                        "k4\tk4\t226.5\t20120.0\n" +
                        "k0\tk0\t227.5\t20200.0\n"
        );
    }

    @Test
    public void testParallelMultiKeyedGroupBySubQuery() throws Exception {
        testParallelMultiKeyedGroupBy(
                "SELECT key1, key2, avg + sum from (" +
                        "  SELECT key1, key2, avg(value), sum(colTop) FROM tab" +
                        ")",
                "key1\tkey2\tcolumn\n" +
                        "k1\tk1\t20103.5\n" +
                        "k2\tk2\t20184.5\n" +
                        "k3\tk3\t20265.5\n" +
                        "k4\tk4\t20346.5\n" +
                        "k0\tk0\t20427.5\n"
        );
    }

    @Test
    public void testParallelMultiKeyedGroupByWithFilter() throws Exception {
        testParallelMultiKeyedGroupBy(
                "SELECT key1, key2, avg(value), sum(colTop) FROM tab WHERE value < 80",
                "key1\tkey2\tavg\tsum\n" +
                        "k1\tk1\t45.31818181818182\t381.0\n" +
                        "k2\tk2\t46.31818181818182\t387.0\n" +
                        "k3\tk3\t47.31818181818182\t393.0\n" +
                        "k4\tk4\t48.31818181818182\t399.0\n" +
                        "k0\tk0\t46.25\t325.0\n"
        );
    }

    @Test
    public void testParallelMultiKeyedGroupByWithLimit() throws Exception {
        testParallelMultiKeyedGroupBy(
                "SELECT key1, key2, avg(value), sum(colTop) FROM tab LIMIT 3",
                "key1\tkey2\tavg\tsum\n" +
                        "k1\tk1\t223.5\t19880.0\n" +
                        "k2\tk2\t224.5\t19960.0\n" +
                        "k3\tk3\t225.5\t20040.0\n",
                "SELECT key1, key2, avg(value), sum(colTop) FROM tab LIMIT -3",
                "key1\tkey2\tavg\tsum\n" +
                        "k3\tk3\t225.5\t20040.0\n" +
                        "k4\tk4\t226.5\t20120.0\n" +
                        "k0\tk0\t227.5\t20200.0\n"
        );
    }

    @Test
    public void testParallelMultiKeyedGroupByWithNestedFilter() throws Exception {
        testParallelMultiKeyedGroupBy(
                "SELECT avg(v), sum(ct), k1, k2 " +
                        "FROM (SELECT value v, colTop ct, key2 k2, key1 k1 FROM tab WHERE value < 80)",
                "avg\tsum\tk1\tk2\n" +
                        "45.31818181818182\t381.0\tk1\tk1\n" +
                        "46.31818181818182\t387.0\tk2\tk2\n" +
                        "47.31818181818182\t393.0\tk3\tk3\n" +
                        "48.31818181818182\t399.0\tk4\tk4\n" +
                        "46.25\t325.0\tk0\tk0\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupBy() throws Exception {
        testParallelNonKeyedGroupBy(
                "SELECT vwap(price, quantity), sum(colTop) FROM tab",
                "vwap\tsum\n" +
                        "289.3066666666667\t100200.0\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByConcurrent() throws Exception {
        final int numOfThreads = 8;
        final int numOfIterations = 50;
        final String query = "SELECT avg(value), sum(colTop) FROM tab";
        final String expected = "avg\tsum\n" +
                "225.5\t100200.0\n";

        final ConcurrentHashMap<Integer, Throwable> errors = new ConcurrentHashMap<>();
        final WorkerPool pool = new WorkerPool((() -> 4));
        TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    ddl(
                            compiler,
                            "CREATE TABLE tab (" +
                                    "  ts TIMESTAMP," +
                                    "  value DOUBLE) timestamp (ts) PARTITION BY DAY",
                            sqlExecutionContext
                    );
                    insert(
                            compiler,
                            "insert into tab select (x * 864000000)::timestamp, x from long_sequence(" + ROW_COUNT + ")",
                            sqlExecutionContext
                    );
                    ddl(compiler, "ALTER TABLE tab ADD COLUMN colTop DOUBLE", sqlExecutionContext);
                    insert(
                            compiler,
                            "insert into tab " +
                                    "select ((50 + x) * 864000000)::timestamp, 50 + x, 50 + x " +
                                    "from long_sequence(" + ROW_COUNT + ")",
                            sqlExecutionContext
                    );

                    final CyclicBarrier barrier = new CyclicBarrier(numOfThreads);
                    final SOCountDownLatch haltLatch = new SOCountDownLatch(numOfThreads);

                    for (int i = 0; i < numOfThreads; i++) {
                        final int threadId = i;
                        new Thread(() -> {
                            final StringSink sink = new StringSink();
                            TestUtils.await(barrier);
                            try {
                                for (int j = 0; j < numOfIterations; j++) {
                                    assertQueries(engine, sqlExecutionContext, sink, query, expected);
                                }
                            } catch (Throwable e) {
                                e.printStackTrace();
                                errors.put(threadId, e);
                            } finally {
                                haltLatch.countDown();
                            }
                        }).start();
                    }
                    haltLatch.await();
                },
                configuration,
                LOG
        );

        if (!errors.isEmpty()) {
            for (Map.Entry<Integer, Throwable> entry : errors.entrySet()) {
                LOG.error().$("Error in thread [id=").$(entry.getKey()).$("] ").$(entry.getValue()).$();
            }
            fail("Error in threads");
        }
    }

    @Test
    public void testParallelNonKeyedGroupByConstant() throws Exception {
        testParallelNonKeyedGroupBy(
                "SELECT count(*) FROM tab GROUP BY 1+2",
                "count\n" +
                        "800\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithNestedFilter() throws Exception {
        testParallelNonKeyedGroupBy(
                "SELECT vwap(p, q), sum(ct) " +
                        "FROM (SELECT colTop ct, quantity q, price p FROM tab WHERE quantity < 80)",
                "vwap\tsum\n" +
                        "185.23063683304647\t1885.0\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithReadThreadSafeFilter() throws Exception {
        testParallelNonKeyedGroupBy(
                "SELECT vwap(price, quantity), sum(colTop) FROM tab WHERE quantity < 80",
                "vwap\tsum\n" +
                        "185.23063683304647\t1885.0\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithReadThreadUnsafeFilter() throws Exception {
        testParallelSymbolKeyedGroupBy(
                "SELECT vwap(price, quantity), sum(colTop) FROM tab WHERE key = 'k1'",
                "vwap\tsum\n" +
                        "285.9440715883669\t19880.0\n"
        );
    }

    @Test
    public void testParallelOperationKeyedGroupBy() throws Exception {
        testParallelSymbolKeyedGroupBy(
                "SELECT ((key is not null) and (colTop is not null)) key, sum(colTop) FROM tab",
                "key\tsum\n" +
                        "false\tNaN\n" +
                        "true\t100200.0\n"
        );
    }

    @Test
    public void testParallelSingleKeyedGroupByConcurrent() throws Exception {
        final int numOfThreads = 8;
        final int numOfIterations = 50;
        final String query = "SELECT key, avg + sum from (" +
                "  SELECT key, avg(value), sum(colTop) FROM tab" +
                ")";
        final String expected = "key\tcolumn\n" +
                "k1\t20103.5\n" +
                "k2\t20184.5\n" +
                "k3\t20265.5\n" +
                "k4\t20346.5\n" +
                "k0\t20427.5\n";

        final ConcurrentHashMap<Integer, Throwable> errors = new ConcurrentHashMap<>();
        final WorkerPool pool = new WorkerPool((() -> 4));
        TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    ddl(
                            compiler,
                            "CREATE TABLE tab (" +
                                    "  ts TIMESTAMP," +
                                    "  key STRING," +
                                    "  value DOUBLE) timestamp (ts) PARTITION BY DAY",
                            sqlExecutionContext
                    );
                    insert(
                            compiler,
                            "insert into tab select (x * 864000000)::timestamp, 'k' || (x % 5), x from long_sequence(" + ROW_COUNT + ")",
                            sqlExecutionContext
                    );
                    ddl(compiler, "ALTER TABLE tab ADD COLUMN colTop DOUBLE", sqlExecutionContext);
                    insert(
                            compiler,
                            "insert into tab " +
                                    "select ((50 + x) * 864000000)::timestamp, 'k' || ((50 + x) % 5), 50 + x, 50 + x " +
                                    "from long_sequence(" + ROW_COUNT + ")",
                            sqlExecutionContext
                    );

                    final CyclicBarrier barrier = new CyclicBarrier(numOfThreads);
                    final SOCountDownLatch haltLatch = new SOCountDownLatch(numOfThreads);

                    for (int i = 0; i < numOfThreads; i++) {
                        final int threadId = i;
                        new Thread(() -> {
                            final StringSink sink = new StringSink();
                            TestUtils.await(barrier);
                            try {
                                for (int j = 0; j < numOfIterations; j++) {
                                    assertQueries(engine, sqlExecutionContext, sink, query, expected);
                                }
                            } catch (Throwable e) {
                                e.printStackTrace();
                                errors.put(threadId, e);
                            } finally {
                                haltLatch.countDown();
                            }
                        }).start();
                    }
                    haltLatch.await();
                },
                configuration,
                LOG
        );

        if (!errors.isEmpty()) {
            for (Map.Entry<Integer, Throwable> entry : errors.entrySet()) {
                LOG.error().$("Error in thread [id=").$(entry.getKey()).$("] ").$(entry.getValue()).$();
            }
            fail("Error in threads");
        }
    }

    @Test
    public void testParallelSingleKeyedGroupBySubQuery() throws Exception {
        testParallelStringKeyedGroupBy(
                "SELECT key, avg + sum from (" +
                        "SELECT key, avg(value), sum(colTop) FROM tab" +
                        ")",
                "key\tcolumn\n" +
                        "k1\t20103.5\n" +
                        "k2\t20184.5\n" +
                        "k3\t20265.5\n" +
                        "k4\t20346.5\n" +
                        "k0\t20427.5\n"
        );
    }

    @Test
    public void testParallelSingleKeyedGroupByWithFilter() throws Exception {
        testParallelStringKeyedGroupBy(
                "SELECT key, avg(value), sum(colTop), count() FROM tab WHERE value < 80",
                "key\tavg\tsum\tcount\n" +
                        "k1\t45.31818181818182\t381.0\t22\n" +
                        "k2\t46.31818181818182\t387.0\t22\n" +
                        "k3\t47.31818181818182\t393.0\t22\n" +
                        "k4\t48.31818181818182\t399.0\t22\n" +
                        "k0\t46.25\t325.0\t20\n"
        );
    }

    @Test
    public void testParallelStringKeyedGroupBy() throws Exception {
        testParallelStringKeyedGroupBy(
                "SELECT key, avg(value), sum(colTop), count() FROM tab",
                "key\tavg\tsum\tcount\n" +
                        "k1\t223.5\t19880.0\t160\n" +
                        "k2\t224.5\t19960.0\t160\n" +
                        "k3\t225.5\t20040.0\t160\n" +
                        "k4\t226.5\t20120.0\t160\n" +
                        "k0\t227.5\t20200.0\t160\n"
        );
    }

    @Test
    public void testParallelStringKeyedGroupByWithLimit() throws Exception {
        testParallelStringKeyedGroupBy(
                "SELECT key, avg(value), sum(colTop) FROM tab LIMIT 3",
                "key\tavg\tsum\n" +
                        "k1\t223.5\t19880.0\n" +
                        "k2\t224.5\t19960.0\n" +
                        "k3\t225.5\t20040.0\n",
                "SELECT key, avg(value), sum(colTop) FROM tab LIMIT -3",
                "key\tavg\tsum\n" +
                        "k3\t225.5\t20040.0\n" +
                        "k4\t226.5\t20120.0\n" +
                        "k0\t227.5\t20200.0\n"
        );
    }

    @Test
    public void testParallelStringKeyedGroupByWithNestedFilter() throws Exception {
        testParallelStringKeyedGroupBy(
                "SELECT avg(v), k, sum(ct) " +
                        "FROM (SELECT colTop ct, value v, key k FROM tab WHERE value < 80)",
                "avg\tk\tsum\n" +
                        "45.31818181818182\tk1\t381.0\n" +
                        "46.31818181818182\tk2\t387.0\n" +
                        "47.31818181818182\tk3\t393.0\n" +
                        "48.31818181818182\tk4\t399.0\n" +
                        "46.25\tk0\t325.0\n"
        );
    }

    @Test
    public void testParallelSymbolKeyedGroupBy() throws Exception {
        testParallelSymbolKeyedGroupBy(
                "SELECT key, vwap(price, quantity), sum(colTop) FROM tab",
                "key\tvwap\tsum\n" +
                        "k1\t285.9440715883669\t19880.0\n" +
                        "k2\t286.6659242761693\t19960.0\n" +
                        "k3\t287.390243902439\t20040.0\n" +
                        "k4\t288.1169977924945\t20120.0\n" +
                        "k0\t288.84615384615387\t20200.0\n"
        );
    }

    @Test
    public void testParallelSymbolKeyedGroupByFilterWithSubQuery() throws Exception {
        testParallelSymbolKeyedGroupBy(
                "SELECT key, vwap(price, quantity), sum(colTop) FROM tab where key in (select key from tab where key in ('k1','k3'))",
                "key\tvwap\tsum\n" +
                        "k1\t285.9440715883669\t19880.0\n" +
                        "k3\t287.390243902439\t20040.0\n"
        );
    }

    @Test
    public void testParallelSymbolKeyedGroupBySubQuery() throws Exception {
        testParallelSymbolKeyedGroupBy(
                "SELECT key, vwap + sum from (" +
                        "SELECT key, vwap(price, quantity), sum(colTop) FROM tab" +
                        ")",
                "key\tcolumn\n" +
                        "k1\t20165.94407158837\n" +
                        "k2\t20246.66592427617\n" +
                        "k3\t20327.39024390244\n" +
                        "k4\t20408.116997792495\n" +
                        "k0\t20488.846153846152\n"
        );
    }

    @Test
    public void testParallelSymbolKeyedGroupByWithLimit() throws Exception {
        testParallelSymbolKeyedGroupBy(
                "SELECT key, vwap(price, quantity), sum(colTop) FROM tab LIMIT 3",
                "key\tvwap\tsum\n" +
                        "k1\t285.9440715883669\t19880.0\n" +
                        "k2\t286.6659242761693\t19960.0\n" +
                        "k3\t287.390243902439\t20040.0\n",
                "SELECT key, vwap(price, quantity), sum(colTop) FROM tab LIMIT -3",
                "key\tvwap\tsum\n" +
                        "k3\t287.390243902439\t20040.0\n" +
                        "k4\t288.1169977924945\t20120.0\n" +
                        "k0\t288.84615384615387\t20200.0\n"
        );
    }

    @Test
    public void testParallelSymbolKeyedGroupByWithNestedFilter() throws Exception {
        testParallelSymbolKeyedGroupBy(
                "SELECT vwap(p, q), k, sum(ct) " +
                        "FROM (SELECT colTop ct, price p, quantity q, key k FROM tab WHERE quantity < 80)",
                "vwap\tk\tsum\n" +
                        "57.01805416248746\tk1\t381.0\n" +
                        "57.76545632973504\tk2\t387.0\n" +
                        "58.52353506243996\tk3\t393.0\n" +
                        "59.29162746942615\tk4\t399.0\n" +
                        "56.62162162162162\tk0\t325.0\n"
        );
    }

    @Test
    public void testParallelSymbolKeyedGroupByWithWithReadThreadSafeFilter() throws Exception {
        testParallelSymbolKeyedGroupBy(
                "SELECT key, vwap(price, quantity), sum(colTop) FROM tab WHERE quantity < 80",
                "key\tvwap\tsum\n" +
                        "k1\t57.01805416248746\t381.0\n" +
                        "k2\t57.76545632973504\t387.0\n" +
                        "k3\t58.52353506243996\t393.0\n" +
                        "k4\t59.29162746942615\t399.0\n" +
                        "k0\t56.62162162162162\t325.0\n"
        );
    }

    @Test
    public void testParallelSymbolKeyedGroupByWithWithReadThreadUnsafeFilter() throws Exception {
        testParallelSymbolKeyedGroupBy(
                "SELECT key, vwap(price, quantity), sum(colTop) FROM tab WHERE key in ('k1','k2')",
                "key\tvwap\tsum\n" +
                        "k1\t285.9440715883669\t19880.0\n" +
                        "k2\t286.6659242761693\t19960.0\n"
        );
    }

    @Test
    public void testStringKeyedGroupByEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool((() -> 4));
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                        ddl(
                                compiler,
                                "CREATE TABLE tab (" +
                                        "  ts TIMESTAMP," +
                                        "  key STRING," +
                                        "  value DOUBLE) timestamp (ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        assertQueries(
                                engine,
                                sqlExecutionContext,
                                "select key, sum(value) from tab",
                                "key\tsum\n"
                        );
                    },
                    configuration,
                    LOG
            );
        });
    }

    private static void assertQueries(CairoEngine engine, SqlExecutionContext sqlExecutionContext, String... queriesAndExpectedResults) throws SqlException {
        assertQueries(engine, sqlExecutionContext, sink, queriesAndExpectedResults);
    }

    private static void assertQueries(CairoEngine engine, SqlExecutionContext sqlExecutionContext, StringSink sink, String... queriesAndExpectedResults) throws SqlException {
        for (int i = 0, n = queriesAndExpectedResults.length; i < n; i += 2) {
            final String query = queriesAndExpectedResults[i];
            final String expected = queriesAndExpectedResults[i + 1];
            TestUtils.assertSql(
                    engine,
                    sqlExecutionContext,
                    query,
                    sink,
                    expected
            );
        }
    }

    private void testParallelMultiKeyedGroupBy(String... queriesAndExpectedResults) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool((() -> 4));
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                        ddl(
                                compiler,
                                "CREATE TABLE tab (\n" +
                                        "  ts TIMESTAMP," +
                                        "  key1 SYMBOL," +
                                        "  key2 SYMBOL," +
                                        "  value DOUBLE) timestamp (ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        insert(
                                compiler,
                                "insert into tab select (x * 864000000)::timestamp, 'k' || (x % 5), 'k' || (x % 5), x from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        ddl(compiler, "ALTER TABLE tab ADD COLUMN colTop DOUBLE", sqlExecutionContext);
                        insert(
                                compiler,
                                "insert into tab " +
                                        "select ((50 + x) * 864000000)::timestamp, 'k' || ((50 + x) % 5), 'k' || ((50 + x) % 5), 50 + x, 50 + x " +
                                        "from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        assertQueries(engine, sqlExecutionContext, queriesAndExpectedResults);
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void testParallelNonKeyedGroupBy(String... queriesAndExpectedResults) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool((() -> 4));
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                        ddl(
                                compiler,
                                "CREATE TABLE tab (" +
                                        "  ts TIMESTAMP," +
                                        "  price DOUBLE," +
                                        "  quantity DOUBLE) timestamp (ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        insert(
                                compiler,
                                "insert into tab select (x * 864000000)::timestamp, x, x % 100 from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        ddl(compiler, "ALTER TABLE tab ADD COLUMN colTop DOUBLE", sqlExecutionContext);
                        insert(
                                compiler,
                                "insert into tab " +
                                        "select ((50 + x) * 864000000)::timestamp, 50 + x, 50 + x, 50 + x " +
                                        "from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        assertQueries(engine, sqlExecutionContext, queriesAndExpectedResults);
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void testParallelStringKeyedGroupBy(String... queriesAndExpectedResults) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool((() -> 4));
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                        ddl(
                                compiler,
                                "CREATE TABLE tab (" +
                                        "  ts TIMESTAMP," +
                                        "  key STRING," +
                                        "  value DOUBLE) timestamp (ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        insert(
                                compiler,
                                "insert into tab select (x * 864000000)::timestamp, 'k' || (x % 5), x from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        ddl(compiler, "ALTER TABLE tab ADD COLUMN colTop DOUBLE", sqlExecutionContext);
                        insert(
                                compiler,
                                "insert into tab " +
                                        "select ((50 + x) * 864000000)::timestamp, 'k' || ((50 + x) % 5), 50 + x, 50 + x " +
                                        "from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        assertQueries(engine, sqlExecutionContext, queriesAndExpectedResults);
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void testParallelSymbolKeyedGroupBy(String... queriesAndExpectedResults) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool((() -> 4));
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                        ddl(
                                compiler,
                                "CREATE TABLE tab (" +
                                        "  ts TIMESTAMP," +
                                        "  key SYMBOL," +
                                        "  price DOUBLE," +
                                        "  quantity LONG) timestamp (ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        insert(
                                compiler,
                                "insert into tab select (x * 864000000)::timestamp, 'k' || (x % 5), x, x from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        ddl(compiler, "ALTER TABLE tab ADD COLUMN colTop DOUBLE", sqlExecutionContext);
                        insert(
                                compiler,
                                "insert into tab " +
                                        "select ((50 + x) * 864000000)::timestamp, " +
                                        "  'k' || ((50 + x) % 5), 50 + x, 50 + x, 50 + x " +
                                        "from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        assertQueries(engine, sqlExecutionContext, queriesAndExpectedResults);
                    },
                    configuration,
                    LOG
            );
        });
    }
}
