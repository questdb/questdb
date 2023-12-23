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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.groupby.GroupByMergeShardJob;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
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
    private static final int ROW_COUNT = 10 * PAGE_FRAME_COUNT * PAGE_FRAME_MAX_ROWS;

    private final boolean enableParallelGroupBy;

    public ParallelGroupByTest(boolean enableParallelGroupBy) {
        this.enableParallelGroupBy = enableParallelGroupBy;
    }

    @Parameterized.Parameters(name = "parallel={0} threshold={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {true},
                {false}
        });
    }

    @Override
    @Before
    public void setUp() {
        pageFrameMaxRows = PAGE_FRAME_MAX_ROWS;
        // We intentionally use small values for shard count and reduce
        // queue capacity to exhibit various edge cases.
        pageFrameReduceShardCount = 2;
        pageFrameReduceQueueCapacity = PAGE_FRAME_COUNT;
        // Set the sharding threshold to a small value to test sharding.
        groupByShardingThreshold = 2;
        super.setUp();
        configOverrideParallelGroupByEnabled(enableParallelGroupBy);
    }

    @Test
    public void testGroupByOverJoin() throws Exception {
        // Parallel GROUP BY shouldn't kick in on this query, yet we want
        // to make sure result correctness.
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
    public void testParallelCountOverMultiKeyGroupBy() throws Exception {
        testParallelMultiKeyGroupBy(
                "SELECT count(*) FROM (SELECT key1, key2 FROM tab GROUP BY key1, key2 ORDER BY key1, key2)",
                "count\n" +
                        "5\n"
        );
    }

    @Test
    public void testParallelCountOverSingleKeyGroupBy() throws Exception {
        testParallelStringKeyGroupBy(
                "SELECT count(*) FROM (SELECT key FROM tab WHERE key IS NOT NULL GROUP BY key ORDER BY key)",
                "count\n" +
                        "5\n"
        );
    }

    @Test
    public void testParallelFunctionKeyExplicitGroupBy() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT day_of_week(ts) day, key, vwap(price, quantity), sum(colTop) FROM tab GROUP BY day, key ORDER BY day, key",
                "day\tkey\tvwap\tsum\n" +
                        "1\tk0\t2848.23852863102\t263700.0\n" +
                        "1\tk1\t2848.94253657797\t263820.0\n" +
                        "1\tk2\t2849.6468136697736\t263940.0\n" +
                        "1\tk3\t2850.3513595394984\t264060.0\n" +
                        "1\tk4\t2851.05617382088\t264180.0\n" +
                        "2\tk0\t2624.4694763291645\t239025.0\n" +
                        "2\tk1\t2598.96097084443\t235085.0\n" +
                        "2\tk2\t2599.691650489951\t235195.0\n" +
                        "2\tk3\t2600.4225929755667\t235305.0\n" +
                        "2\tk4\t2601.153797916691\t235415.0\n" +
                        "3\tk0\t2526.5384615384614\t204750.0\n" +
                        "3\tk1\t2527.3046131315596\t204850.0\n" +
                        "3\tk2\t2528.070992925104\t204950.0\n" +
                        "3\tk3\t2528.8376005852233\t205050.0\n" +
                        "3\tk4\t2529.6044357786986\t205150.0\n" +
                        "4\tk0\t2594.679907219484\t215425.0\n" +
                        "4\tk1\t2595.0011126435716\t215585.0\n" +
                        "4\tk2\t2595.617813662006\t215695.0\n" +
                        "4\tk3\t2596.234922950459\t215805.0\n" +
                        "4\tk4\t2596.8524398569757\t215915.0\n" +
                        "5\tk0\t2651.1220904699167\t227700.0\n" +
                        "5\tk1\t2651.7251338776227\t227820.0\n" +
                        "5\tk2\t2652.3285952443625\t227940.0\n" +
                        "5\tk3\t2652.9324739103745\t228060.0\n" +
                        "5\tk4\t2653.5367692172845\t228180.0\n" +
                        "6\tk0\t2713.3938256153524\t239700.0\n" +
                        "6\tk1\t2714.035610040864\t239820.0\n" +
                        "6\tk2\t2714.6777527715262\t239940.0\n" +
                        "6\tk3\t2715.3202532700157\t240060.0\n" +
                        "6\tk4\t2715.9631110000832\t240180.0\n" +
                        "7\tk0\t2779.263011521653\t251700.0\n" +
                        "7\tk1\t2779.938130410611\t251820.0\n" +
                        "7\tk2\t2780.6135587838376\t251940.0\n" +
                        "7\tk3\t2781.2892961993175\t252060.0\n" +
                        "7\tk4\t2781.9653422158776\t252180.0\n"
        );
    }

    @Test
    public void testParallelFunctionKeyGroupByMultipleKeys() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT vwap(price, quantity), day_of_week(ts) day, sum(colTop), concat(key, 'abc')::symbol key " +
                        "FROM tab ORDER BY day, key",
                "vwap\tday\tsum\tkey\n" +
                        "2848.23852863102\t1\t263700.0\tk0abc\n" +
                        "2848.94253657797\t1\t263820.0\tk1abc\n" +
                        "2849.6468136697736\t1\t263940.0\tk2abc\n" +
                        "2850.3513595394984\t1\t264060.0\tk3abc\n" +
                        "2851.05617382088\t1\t264180.0\tk4abc\n" +
                        "2624.4694763291645\t2\t239025.0\tk0abc\n" +
                        "2598.96097084443\t2\t235085.0\tk1abc\n" +
                        "2599.691650489951\t2\t235195.0\tk2abc\n" +
                        "2600.4225929755667\t2\t235305.0\tk3abc\n" +
                        "2601.153797916691\t2\t235415.0\tk4abc\n" +
                        "2526.5384615384614\t3\t204750.0\tk0abc\n" +
                        "2527.3046131315596\t3\t204850.0\tk1abc\n" +
                        "2528.070992925104\t3\t204950.0\tk2abc\n" +
                        "2528.8376005852233\t3\t205050.0\tk3abc\n" +
                        "2529.6044357786986\t3\t205150.0\tk4abc\n" +
                        "2594.679907219484\t4\t215425.0\tk0abc\n" +
                        "2595.0011126435716\t4\t215585.0\tk1abc\n" +
                        "2595.617813662006\t4\t215695.0\tk2abc\n" +
                        "2596.234922950459\t4\t215805.0\tk3abc\n" +
                        "2596.8524398569757\t4\t215915.0\tk4abc\n" +
                        "2651.1220904699167\t5\t227700.0\tk0abc\n" +
                        "2651.7251338776227\t5\t227820.0\tk1abc\n" +
                        "2652.3285952443625\t5\t227940.0\tk2abc\n" +
                        "2652.9324739103745\t5\t228060.0\tk3abc\n" +
                        "2653.5367692172845\t5\t228180.0\tk4abc\n" +
                        "2713.3938256153524\t6\t239700.0\tk0abc\n" +
                        "2714.035610040864\t6\t239820.0\tk1abc\n" +
                        "2714.6777527715262\t6\t239940.0\tk2abc\n" +
                        "2715.3202532700157\t6\t240060.0\tk3abc\n" +
                        "2715.9631110000832\t6\t240180.0\tk4abc\n" +
                        "2779.263011521653\t7\t251700.0\tk0abc\n" +
                        "2779.938130410611\t7\t251820.0\tk1abc\n" +
                        "2780.6135587838376\t7\t251940.0\tk2abc\n" +
                        "2781.2892961993175\t7\t252060.0\tk3abc\n" +
                        "2781.9653422158776\t7\t252180.0\tk4abc\n"
        );
    }

    @Test
    public void testParallelFunctionKeyGroupByThreadSafe() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT day_of_week(ts) day, key, vwap(price, quantity), sum(colTop) FROM tab ORDER BY day, key",
                "day\tkey\tvwap\tsum\n" +
                        "1\tk0\t2848.23852863102\t263700.0\n" +
                        "1\tk1\t2848.94253657797\t263820.0\n" +
                        "1\tk2\t2849.6468136697736\t263940.0\n" +
                        "1\tk3\t2850.3513595394984\t264060.0\n" +
                        "1\tk4\t2851.05617382088\t264180.0\n" +
                        "2\tk0\t2624.4694763291645\t239025.0\n" +
                        "2\tk1\t2598.96097084443\t235085.0\n" +
                        "2\tk2\t2599.691650489951\t235195.0\n" +
                        "2\tk3\t2600.4225929755667\t235305.0\n" +
                        "2\tk4\t2601.153797916691\t235415.0\n" +
                        "3\tk0\t2526.5384615384614\t204750.0\n" +
                        "3\tk1\t2527.3046131315596\t204850.0\n" +
                        "3\tk2\t2528.070992925104\t204950.0\n" +
                        "3\tk3\t2528.8376005852233\t205050.0\n" +
                        "3\tk4\t2529.6044357786986\t205150.0\n" +
                        "4\tk0\t2594.679907219484\t215425.0\n" +
                        "4\tk1\t2595.0011126435716\t215585.0\n" +
                        "4\tk2\t2595.617813662006\t215695.0\n" +
                        "4\tk3\t2596.234922950459\t215805.0\n" +
                        "4\tk4\t2596.8524398569757\t215915.0\n" +
                        "5\tk0\t2651.1220904699167\t227700.0\n" +
                        "5\tk1\t2651.7251338776227\t227820.0\n" +
                        "5\tk2\t2652.3285952443625\t227940.0\n" +
                        "5\tk3\t2652.9324739103745\t228060.0\n" +
                        "5\tk4\t2653.5367692172845\t228180.0\n" +
                        "6\tk0\t2713.3938256153524\t239700.0\n" +
                        "6\tk1\t2714.035610040864\t239820.0\n" +
                        "6\tk2\t2714.6777527715262\t239940.0\n" +
                        "6\tk3\t2715.3202532700157\t240060.0\n" +
                        "6\tk4\t2715.9631110000832\t240180.0\n" +
                        "7\tk0\t2779.263011521653\t251700.0\n" +
                        "7\tk1\t2779.938130410611\t251820.0\n" +
                        "7\tk2\t2780.6135587838376\t251940.0\n" +
                        "7\tk3\t2781.2892961993175\t252060.0\n" +
                        "7\tk4\t2781.9653422158776\t252180.0\n"
        );
    }

    @Test
    public void testParallelFunctionKeyGroupByThreadUnsafe() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT concat(key, 'abc')::symbol key, vwap(price, quantity), sum(colTop) FROM tab ORDER BY key",
                "key\tvwap\tsum\n" +
                        "k0abc\t2685.431565967941\t1642000.0\n" +
                        "k1abc\t2682.7321472695826\t1638800.0\n" +
                        "k2abc\t2683.4065201284266\t1639600.0\n" +
                        "k3abc\t2684.081214514935\t1640400.0\n" +
                        "k4abc\t2684.756229953121\t1641200.0\n"
        );
    }

    @Test
    public void testParallelMultiKeyGroupBy() throws Exception {
        testParallelMultiKeyGroupBy(
                "SELECT key1, key2, avg(value), sum(colTop) FROM tab ORDER BY key1, key2",
                "key1\tkey2\tavg\tsum\n" +
                        "k0\tk0\t2027.5\t1642000.0\n" +
                        "k1\tk1\t2023.5\t1638800.0\n" +
                        "k2\tk2\t2024.5\t1639600.0\n" +
                        "k3\tk3\t2025.5\t1640400.0\n" +
                        "k4\tk4\t2026.5\t1641200.0\n"
        );
    }

    @Test
    public void testParallelMultiKeyGroupBySubQuery() throws Exception {
        testParallelMultiKeyGroupBy(
                "SELECT key1, key2, avg + sum from (" +
                        "  SELECT key1, key2, avg(value), sum(colTop) FROM tab" +
                        ") ORDER BY key1, key2",
                "key1\tkey2\tcolumn\n" +
                        "k0\tk0\t1644027.5\n" +
                        "k1\tk1\t1640823.5\n" +
                        "k2\tk2\t1641624.5\n" +
                        "k3\tk3\t1642425.5\n" +
                        "k4\tk4\t1643226.5\n"
        );
    }

    @Test
    public void testParallelMultiKeyGroupByWithFilter() throws Exception {
        testParallelMultiKeyGroupBy(
                "SELECT key1, key2, avg(value), sum(colTop) FROM tab WHERE value < 80 ORDER BY key1, key2",
                "key1\tkey2\tavg\tsum\n" +
                        "k0\tk0\t46.25\t325.0\n" +
                        "k1\tk1\t45.31818181818182\t381.0\n" +
                        "k2\tk2\t46.31818181818182\t387.0\n" +
                        "k3\tk3\t47.31818181818182\t393.0\n" +
                        "k4\tk4\t48.31818181818182\t399.0\n"
        );
    }

    @Test
    public void testParallelMultiKeyGroupByWithLimit() throws Exception {
        testParallelMultiKeyGroupBy(
                "SELECT key1, key2, avg(value), sum(colTop) FROM tab ORDER BY key1, key2 LIMIT 3",
                "key1\tkey2\tavg\tsum\n" +
                        "k0\tk0\t2027.5\t1642000.0\n" +
                        "k1\tk1\t2023.5\t1638800.0\n" +
                        "k2\tk2\t2024.5\t1639600.0\n",
                "SELECT key1, key2, avg(value), sum(colTop) FROM tab ORDER BY key1, key2 LIMIT -3",
                "key1\tkey2\tavg\tsum\n" +
                        "k2\tk2\t2024.5\t1639600.0\n" +
                        "k3\tk3\t2025.5\t1640400.0\n" +
                        "k4\tk4\t2026.5\t1641200.0\n"
        );
    }

    @Test
    public void testParallelMultiKeyGroupByWithNestedFilter() throws Exception {
        testParallelMultiKeyGroupBy(
                "SELECT avg(v), sum(ct), k1, k2 " +
                        "FROM (SELECT value v, colTop ct, key2 k2, key1 k1 FROM tab WHERE value < 80) ORDER BY k1, k2",
                "avg\tsum\tk1\tk2\n" +
                        "46.25\t325.0\tk0\tk0\n" +
                        "45.31818181818182\t381.0\tk1\tk1\n" +
                        "46.31818181818182\t387.0\tk2\tk2\n" +
                        "47.31818181818182\t393.0\tk3\tk3\n" +
                        "48.31818181818182\t399.0\tk4\tk4\n"
        );
    }

    @Test
    public void testParallelMultiKeyGroupByWithNoFunctions() throws Exception {
        testParallelMultiKeyGroupBy(
                "SELECT key1, key2 FROM tab GROUP BY key1, key2 ORDER BY key1, key2",
                "key1\tkey2\n" +
                        "k0\tk0\n" +
                        "k1\tk1\n" +
                        "k2\tk2\n" +
                        "k3\tk3\n" +
                        "k4\tk4\n"
        );
    }

    @Test
    public void testParallelMultiKeyGroupByWithNoFunctionsAndFilter() throws Exception {
        testParallelMultiKeyGroupBy(
                "SELECT key1, key2 FROM tab WHERE key1 != 'k1' and key2 != 'k2' GROUP BY key1, key2 ORDER BY key1, key2",
                "key1\tkey2\n" +
                        "k0\tk0\n" +
                        "k3\tk3\n" +
                        "k4\tk4\n"
        );
    }

    @Test
    public void testParallelMultiKeyGroupByWithNoFunctionsAndTooStrictFilter() throws Exception {
        testParallelMultiKeyGroupBy(
                "SELECT key1, key2 FROM tab WHERE value < 0 GROUP BY key1, key2 ORDER BY key1, key2",
                "key1\tkey2\n"
        );
    }

    @Test
    public void testParallelMultiKeyGroupByWithTooStrictFilter() throws Exception {
        testParallelMultiKeyGroupBy(
                "SELECT key1, key2, avg(value), sum(colTop) FROM tab WHERE value < 0 ORDER BY key1, key2",
                "key1\tkey2\tavg\tsum\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupBy() throws Exception {
        testParallelNonKeyedGroupBy(
                "SELECT vwap(price, quantity), sum(colTop) FROM tab",
                "vwap\tsum\n" +
                        "2684.615238095238\t8202000.0\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByConcurrent() throws Exception {
        final int numOfThreads = 8;
        final int numOfIterations = 50;
        final String query = "SELECT avg(value), sum(colTop) FROM tab";
        final String expected = "avg\tsum\n" +
                "2025.5\t8202000.0\n";

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
                        "8000\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByFaultTolerance() throws Exception {
        testParallelGroupByFaultTolerance("select vwap(price, quantity) from tab where npe();");
    }

    @Test
    public void testParallelNonKeyedGroupByThrowsOnTimeout() throws Exception {
        testParallelGroupByThrowsOnTimeout("select vwap(price, quantity) from tab");
    }

    @Test
    public void testParallelNonKeyedGroupByWithFilter() throws Exception {
        testParallelNonKeyedGroupBy(
                "SELECT vwap(price, quantity), sum(colTop) FROM tab WHERE quantity < 100",
                "vwap\tsum\n" +
                        "1981.006198090988\t3675.0\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithNestedFilter() throws Exception {
        testParallelNonKeyedGroupBy(
                "SELECT vwap(p, q), sum(ct) " +
                        "FROM (SELECT colTop ct, quantity q, price p FROM tab WHERE quantity < 80)",
                "vwap\tsum\n" +
                        "1974.5391511088592\t1885.0\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithReadThreadSafeFilter() throws Exception {
        testParallelNonKeyedGroupBy(
                "SELECT vwap(price, quantity), sum(colTop) FROM tab WHERE quantity < 80",
                "vwap\tsum\n" +
                        "1974.5391511088592\t1885.0\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithReadThreadUnsafeFilter() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT vwap(price, quantity), sum(colTop) FROM tab WHERE key = 'k1'",
                "vwap\tsum\n" +
                        "2682.7321472695826\t1638800.0\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithTooStrictFilter() throws Exception {
        testParallelNonKeyedGroupBy(
                "SELECT vwap(price, quantity), sum(colTop) FROM tab WHERE quantity < 0",
                "vwap\tsum\n" +
                        "NaN\tNaN\n"
        );
    }

    @Test
    public void testParallelOperationKeyGroupBy() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT ((key is not null) and (colTop is not null)) key, sum(colTop) FROM tab ORDER BY key",
                "key\tsum\n" +
                        "false\tNaN\n" +
                        "true\t8202000.0\n"
        );
    }

    @Test
    public void testParallelSingleKeyGroupByConcurrent() throws Exception {
        final int numOfThreads = 8;
        final int numOfIterations = 50;
        final String query = "SELECT key, avg + sum from (" +
                "  SELECT key, avg(value), sum(colTop) FROM tab" +
                ") ORDER BY key";
        final String expected = "key\tcolumn\n" +
                "k0\t1644027.5\n" +
                "k1\t1640823.5\n" +
                "k2\t1641624.5\n" +
                "k3\t1642425.5\n" +
                "k4\t1643226.5\n";

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
    public void testParallelSingleKeyGroupByFaultTolerance() throws Exception {
        testParallelGroupByFaultTolerance(
                "select case when quantity > 100 then 'a lot' else 'a few' end, vwap(price, quantity) " +
                        "from tab " +
                        "where npe();"
        );
    }

    @Test
    public void testParallelSingleKeyGroupBySubQuery() throws Exception {
        testParallelStringKeyGroupBy(
                "SELECT key, avg + sum from (" +
                        "SELECT key, avg(value), sum(colTop) FROM tab" +
                        ") ORDER BY key",
                "key\tcolumn\n" +
                        "k0\t1644027.5\n" +
                        "k1\t1640823.5\n" +
                        "k2\t1641624.5\n" +
                        "k3\t1642425.5\n" +
                        "k4\t1643226.5\n"
        );
    }

    @Test
    public void testParallelSingleKeyGroupByThrowsOnTimeout() throws Exception {
        testParallelGroupByThrowsOnTimeout("select quantity % 100, vwap(price, quantity) from tab");
    }

    @Test
    public void testParallelSingleKeyGroupByWithFilter() throws Exception {
        testParallelStringKeyGroupBy(
                "SELECT key, avg(value), sum(colTop), count() FROM tab WHERE value < 80 ORDER BY key",
                "key\tavg\tsum\tcount\n" +
                        "k0\t46.25\t325.0\t20\n" +
                        "k1\t45.31818181818182\t381.0\t22\n" +
                        "k2\t46.31818181818182\t387.0\t22\n" +
                        "k3\t47.31818181818182\t393.0\t22\n" +
                        "k4\t48.31818181818182\t399.0\t22\n"
        );
    }

    @Test
    public void testParallelSingleKeyGroupByWithNoFunctions() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key FROM tab GROUP BY key ORDER BY key",
                "key\n" +
                        "k0\n" +
                        "k1\n" +
                        "k2\n" +
                        "k3\n" +
                        "k4\n"
        );
    }

    @Test
    public void testParallelSingleKeyGroupByWithNoFunctionsAndFilter() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key FROM tab WHERE key != 'k1' GROUP BY key ORDER BY key",
                "key\n" +
                        "k0\n" +
                        "k2\n" +
                        "k3\n" +
                        "k4\n"
        );
    }

    @Test
    public void testParallelSingleKeyGroupByWithNoFunctionsAndTooStrictFilter() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key FROM tab WHERE quantity < 0 GROUP BY key ORDER BY key",
                "key\n"
        );
    }

    @Test
    public void testParallelSingleKeyGroupByWithTooStrictFilter() throws Exception {
        testParallelStringKeyGroupBy(
                "SELECT key, avg(value), sum(colTop), count() FROM tab WHERE value < 0 ORDER BY key",
                "key\tavg\tsum\tcount\n"
        );
    }

    @Test
    public void testParallelStringKeyGroupBy() throws Exception {
        testParallelStringKeyGroupBy(
                "SELECT key, avg(value), sum(colTop), count() FROM tab ORDER BY key",
                "key\tavg\tsum\tcount\n" +
                        "k0\t2027.5\t1642000.0\t1600\n" +
                        "k1\t2023.5\t1638800.0\t1600\n" +
                        "k2\t2024.5\t1639600.0\t1600\n" +
                        "k3\t2025.5\t1640400.0\t1600\n" +
                        "k4\t2026.5\t1641200.0\t1600\n"
        );
    }

    @Test
    public void testParallelStringKeyGroupByWithLimit() throws Exception {
        testParallelStringKeyGroupBy(
                "SELECT key, avg(value), sum(colTop) FROM tab ORDER BY key LIMIT 3",
                "key\tavg\tsum\n" +
                        "k0\t2027.5\t1642000.0\n" +
                        "k1\t2023.5\t1638800.0\n" +
                        "k2\t2024.5\t1639600.0\n",
                "SELECT key, avg(value), sum(colTop) FROM tab ORDER BY key LIMIT -3",
                "key\tavg\tsum\n" +
                        "k2\t2024.5\t1639600.0\n" +
                        "k3\t2025.5\t1640400.0\n" +
                        "k4\t2026.5\t1641200.0\n"
        );
    }

    @Test
    public void testParallelStringKeyGroupByWithNestedFilter() throws Exception {
        testParallelStringKeyGroupBy(
                "SELECT avg(v), k, sum(ct) " +
                        "FROM (SELECT colTop ct, value v, key k FROM tab WHERE value < 80) ORDER BY k",
                "avg\tk\tsum\n" +
                        "46.25\tk0\t325.0\n" +
                        "45.31818181818182\tk1\t381.0\n" +
                        "46.31818181818182\tk2\t387.0\n" +
                        "47.31818181818182\tk3\t393.0\n" +
                        "48.31818181818182\tk4\t399.0\n"
        );
    }

    @Test
    public void testParallelSymbolKeyGroupBy() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key, vwap(price, quantity), sum(colTop) FROM tab ORDER BY key",
                "key\tvwap\tsum\n" +
                        "k0\t2685.431565967941\t1642000.0\n" +
                        "k1\t2682.7321472695826\t1638800.0\n" +
                        "k2\t2683.4065201284266\t1639600.0\n" +
                        "k3\t2684.081214514935\t1640400.0\n" +
                        "k4\t2684.756229953121\t1641200.0\n"
        );
    }

    @Test
    public void testParallelSymbolKeyGroupByFilterWithSubQuery() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key, vwap(price, quantity), sum(colTop) FROM tab " +
                        "where key in (select key from tab where key in ('k1','k3')) ORDER BY key",
                "key\tvwap\tsum\n" +
                        "k1\t2682.7321472695826\t1638800.0\n" +
                        "k3\t2684.081214514935\t1640400.0\n"
        );
    }

    @Test
    public void testParallelSymbolKeyGroupBySubQuery() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key, vwap + sum from (" +
                        "SELECT key, vwap(price, quantity), sum(colTop) FROM tab" +
                        ") ORDER BY key",
                "key\tcolumn\n" +
                        "k0\t1644685.4315659679\n" +
                        "k1\t1641482.7321472696\n" +
                        "k2\t1642283.4065201285\n" +
                        "k3\t1643084.081214515\n" +
                        "k4\t1643884.7562299531\n"
        );
    }

    @Test
    public void testParallelSymbolKeyGroupByWithLimit() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key, vwap(price, quantity), sum(colTop) FROM tab ORDER BY key LIMIT 3",
                "key\tvwap\tsum\n" +
                        "k0\t2685.431565967941\t1642000.0\n" +
                        "k1\t2682.7321472695826\t1638800.0\n" +
                        "k2\t2683.4065201284266\t1639600.0\n",
                "SELECT key, vwap(price, quantity), sum(colTop) FROM tab ORDER BY key LIMIT -3",
                "key\tvwap\tsum\n" +
                        "k2\t2683.4065201284266\t1639600.0\n" +
                        "k3\t2684.081214514935\t1640400.0\n" +
                        "k4\t2684.756229953121\t1641200.0\n"
        );
    }

    @Test
    public void testParallelSymbolKeyGroupByWithNestedFilter() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT vwap(p, q), k, sum(ct) " +
                        "FROM (SELECT colTop ct, price p, quantity q, key k FROM tab WHERE quantity < 80) ORDER BY k",
                "vwap\tk\tsum\n" +
                        "56.62162162162162\tk0\t325.0\n" +
                        "57.01805416248746\tk1\t381.0\n" +
                        "57.76545632973504\tk2\t387.0\n" +
                        "58.52353506243996\tk3\t393.0\n" +
                        "59.29162746942615\tk4\t399.0\n"
        );
    }

    @Test
    public void testParallelSymbolKeyGroupByWithWithReadThreadSafeFilter() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key, vwap(price, quantity), sum(colTop) FROM tab WHERE quantity < 80 ORDER BY key",
                "key\tvwap\tsum\n" +
                        "k0\t56.62162162162162\t325.0\n" +
                        "k1\t57.01805416248746\t381.0\n" +
                        "k2\t57.76545632973504\t387.0\n" +
                        "k3\t58.52353506243996\t393.0\n" +
                        "k4\t59.29162746942615\t399.0\n"
        );
    }

    @Test
    public void testParallelSymbolKeyGroupByWithWithReadThreadUnsafeFilter() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key, vwap(price, quantity), sum(colTop) FROM tab WHERE key in ('k1','k2') ORDER BY key",
                "key\tvwap\tsum\n" +
                        "k1\t2682.7321472695826\t1638800.0\n" +
                        "k2\t2683.4065201284266\t1639600.0\n"
        );
    }

    @Test
    public void testStringKeyGroupByEmptyTable() throws Exception {
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
                                "select key, sum(value) from tab ORDER BY key",
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

    private void testParallelGroupByFaultTolerance(String query) throws Exception {
        Assume.assumeTrue(enableParallelGroupBy);
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

                        try {
                            try (final RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                                try (final RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                    //noinspection StatementWithEmptyBody
                                    while (cursor.hasNext()) {
                                    } // drain cursor until exception
                                    Assert.fail();
                                }
                            }
                        } catch (Throwable e) {
                            TestUtils.assertContains(e.getMessage(), "unexpected filter error");
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void testParallelGroupByThrowsOnTimeout(String query) throws Exception {
        Assume.assumeTrue(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
            currentMicros = 0;
            NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(
                    new DefaultSqlExecutionCircuitBreakerConfiguration() {
                        @Override
                        @NotNull
                        public MillisecondClock getClock() {
                            return () -> Long.MAX_VALUE;
                        }

                        @Override
                        public long getQueryTimeout() {
                            return 1;
                        }
                    },
                    MemoryTag.NATIVE_DEFAULT
            );

            ddl(
                    "CREATE TABLE tab (" +
                            "  ts TIMESTAMP," +
                            "  price DOUBLE," +
                            "  quantity DOUBLE) timestamp (ts) PARTITION BY DAY"
            );
            insert("insert into tab select (x * 864000000)::timestamp, x, x % 100 from long_sequence(" + ROW_COUNT + ")");

            context.with(
                    context.getSecurityContext(),
                    context.getBindVariableService(),
                    context.getRandom(),
                    context.getRequestFd(),
                    circuitBreaker
            );

            try {
                assertSql("", query);
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "timeout, query aborted");
            } finally {
                context.with(
                        context.getSecurityContext(),
                        context.getBindVariableService(),
                        context.getRandom(),
                        context.getRequestFd(),
                        null
                );
                Misc.free(circuitBreaker);
            }
        });
    }

    private void testParallelMultiKeyGroupBy(String... queriesAndExpectedResults) throws Exception {
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

    private void testParallelStringKeyGroupBy(String... queriesAndExpectedResults) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool((() -> 4));
            TestUtils.execute(pool,
                    (engine) -> pool.assign(new GroupByMergeShardJob(engine.getMessageBus())),
                    (engine, compiler, sqlExecutionContext) -> {
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

    private void testParallelSymbolKeyGroupBy(String... queriesAndExpectedResults) throws Exception {
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
