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

package io.questdb.test.cairo.fuzz;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.groupby.vect.GroupByRecordCursorFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.fail;

// This is not a fuzz test in traditional sense, but it's multithreaded, and we want to run it
// in CI frequently along with other fuzz tests.
public class ParallelGroupByFuzzTest extends AbstractCairoTest {
    private static final int PAGE_FRAME_COUNT = 4; // also used to set queue size, so must be a power of 2
    private static final int PAGE_FRAME_MAX_ROWS = 100;
    private static final int ROW_COUNT = 10 * PAGE_FRAME_COUNT * PAGE_FRAME_MAX_ROWS;
    private final boolean convertToParquet;
    private final boolean enableJitCompiler;
    private final boolean enableParallelGroupBy;

    public ParallelGroupByFuzzTest() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        this.enableParallelGroupBy = rnd.nextBoolean();
        this.enableJitCompiler = rnd.nextBoolean();
        this.convertToParquet = rnd.nextBoolean();
    }

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, PAGE_FRAME_MAX_ROWS);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, PAGE_FRAME_MAX_ROWS);
        // We intentionally use small values for shard count and reduce
        // queue capacity to exhibit various edge cases.
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 2);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, PAGE_FRAME_COUNT);
        // Set the sharding threshold to a small value to test sharding.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 2);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, String.valueOf(enableParallelGroupBy));
        super.setUp();
    }

    @Test
    public void testGroupByOverJoin() throws Exception {
        // Parallel GROUP BY shouldn't kick in on this query, yet we want
        // to validate the result correctness.
        Assume.assumeTrue(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE t (
                              created timestamp,
                              event short,
                              origin short
                            ) TIMESTAMP(created) PARTITION BY DAY;"""
            );
            execute("INSERT INTO t VALUES ('2023-09-21T10:00:00.000000Z', 1, 1);");
            execute("INSERT INTO t VALUES ('2023-09-22T11:00:00.000000Z', 1, 1);");

            if (convertToParquet) {
                execute("alter table t convert partition to parquet where created >= 0");
            }

            assertQuery(
                    """
                            count
                            2
                            """,
                    """
                            SELECT count(1)
                            FROM t as T1 JOIN t as T2 ON T1.created = T2.created
                            WHERE T1.event = 1.0""",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testGroupByOverLatestBy() throws Exception {
        // Parallel GROUP BY shouldn't kick in on this query, yet we want
        // to validate the result correctness.
        Assume.assumeTrue(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE t (
                              created timestamp,
                              event symbol,
                              origin symbol
                            ) TIMESTAMP(created) PARTITION BY DAY;"""
            );
            execute("INSERT INTO t VALUES ('2023-09-21T10:00:00.000000Z', 'a', 'c');");
            execute("INSERT INTO t VALUES ('2023-09-21T10:00:01.000000Z', 'a', 'c');");
            execute("INSERT INTO t VALUES ('2023-09-22T10:00:02.000000Z', 'a', 'd');");
            execute("INSERT INTO t VALUES ('2023-09-22T10:00:00.000000Z', 'b', 'c');");
            execute("INSERT INTO t VALUES ('2023-09-23T10:00:01.000000Z', 'b', 'c');");

            if (convertToParquet) {
                execute("alter table t convert partition to parquet where created >= 0");
            }

            assertQuery(
                    """
                            count
                            2
                            """,
                    """
                            SELECT count()
                            FROM t
                            WHERE origin = 'c'
                            LATEST ON created PARTITION BY event""",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testGroupByOverUnion() throws Exception {
        // Parallel GROUP BY shouldn't kick in on this query, yet we want
        // to validate the result correctness.
        Assume.assumeTrue(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE t1 (
                              created timestamp,
                              event short,
                              origin short
                            ) TIMESTAMP(created) PARTITION BY DAY;"""
            );
            execute("INSERT INTO t1 VALUES ('2023-09-21T10:00:00.000000Z', 1, 1);");
            execute("INSERT INTO t1 VALUES ('2023-09-22T10:00:01.000000Z', 2, 2);");

            execute(
                    """
                            CREATE TABLE t2 (
                              created timestamp,
                              event short,
                              origin short
                            ) TIMESTAMP(created) PARTITION BY DAY;"""
            );
            execute("INSERT INTO t2 VALUES ('2023-09-21T10:00:02.000000Z', 3, 1);");
            execute("INSERT INTO t2 VALUES ('2023-09-22T10:00:00.000000Z', 4, 2);");

            if (convertToParquet) {
                execute("alter table t1 convert partition to parquet where created >= 0");
                execute("alter table t2 convert partition to parquet where created >= 0");
            }

            assertQuery(
                    """
                            event\tcount
                            1\t1
                            3\t1
                            """,
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
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        // The table is empty.
        Assume.assumeFalse(convertToParquet);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        execute(
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
                                """
                                        vwap
                                        null
                                        """
                        );
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testParallelCaseExpressionKeyGroupBy1() throws Exception {
        testParallelStringAndVarcharKeyGroupBy(
                "SELECT CASE WHEN (key = 'k0') THEN 'foo' ELSE 'bar' END AS key, count(*) " +
                        "FROM tab GROUP BY key ORDER BY key",
                """
                        key\tcount
                        bar\t3200
                        foo\t800
                        """
        );
    }

    @Test
    public void testParallelCaseExpressionKeyGroupBy2() throws Exception {
        // Parallel GROUP BY shouldn't kick in on this query due to ::symbol cast,
        // yet we want to validate the result correctness.
        testParallelStringAndVarcharKeyGroupBy(
                "SELECT CASE WHEN (key::symbol = 'k0') THEN 'foo' ELSE 'bar' END AS key, count(*) " +
                        "FROM tab GROUP BY key ORDER BY key",
                """
                        key\tcount
                        bar\t3200
                        foo\t800
                        """
        );
    }

    @Test
    public void testParallelCaseExpressionKeyGroupBy3() throws Exception {
        testParallelStringAndVarcharKeyGroupBy(
                "SELECT CASE WHEN (value > 2023.5) THEN key ELSE '' END AS key, avg(value) " +
                        "FROM tab GROUP BY key ORDER BY key",
                """
                        key\tavg
                        \t1018.6259753335012
                        k0\t2037.5
                        k1\t2036.0
                        k2\t2037.0
                        k3\t2038.0
                        k4\t2036.5
                        """
        );
    }

    @Test
    public void testParallelCountDistinctFuzz() throws Exception {
        // With this test, we aim to verify correctness of merge() method
        // implementation in count_distinct functions.

        // This test controls sets enable parallel GROUP BY flag on its own.
        Assume.assumeTrue(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);

                        execute(
                                compiler,
                                "create table tab as (" +
                                        "  select" +
                                        "    rnd_int() anint," +
                                        "    rnd_ipv4() anipv4," +
                                        "    rnd_symbol(100,4,4,2) asymbol," +
                                        "    rnd_long() along," +
                                        "    rnd_uuid4() auuid," +
                                        "    rnd_long256() along256," +
                                        "    timestamp_sequence(0, 86400000000) ts" +  // 1 day per row
                                        "  from long_sequence(1)" +
                                        ") timestamp(ts) partition by day",
                                sqlExecutionContext
                        );

                        // Now insert varying amounts of data per partition to cover various branches.
                        long timestamp = 86400000000L; // Start from day 2

                        for (int i = 0; i < 50; i++) {
                            final int prob = rnd.nextInt(100);
                            if (prob < 25) {
                                // Add partition with 1 row (for inlined value branch).
                                execute(
                                        compiler,
                                        "insert into tab values(rnd_int(), rnd_ipv4(), rnd_symbol(100,4,4,2), " +
                                                "rnd_long(), rnd_uuid4(), rnd_long256(), " + timestamp + "::timestamp)",
                                        sqlExecutionContext
                                );
                            } else if (prob < 50) {
                                // Add partition with a varying row counts.
                                final int rows = rnd.nextInt(100) + 1;
                                execute(
                                        compiler,
                                        "insert into tab " +
                                                "select rnd_int(), rnd_ipv4(), rnd_symbol(100,4,4,2), " +
                                                "rnd_long(), rnd_uuid4(), rnd_long256(), " + timestamp + "::timestamp " +
                                                "from long_sequence(" + rows + ")",
                                        sqlExecutionContext
                                );
                            } else if (prob < 75) {
                                // Add partition with exactly PAGE_FRAME_MAX_ROWS.
                                execute(
                                        compiler,
                                        "insert into tab " +
                                                "select rnd_int(), rnd_ipv4(), rnd_symbol(100,4,4,2), " +
                                                "rnd_long(), rnd_uuid4(), rnd_long256(), " + timestamp + "::timestamp " +
                                                "from long_sequence(" + PAGE_FRAME_MAX_ROWS + ")",
                                        sqlExecutionContext
                                );
                            } else {
                                // Add partition with PAGE_FRAME_MAX_ROWS + 1 rows.
                                execute(
                                        compiler,
                                        "insert into tab " +
                                                "select rnd_int(), rnd_ipv4(), rnd_symbol(100,4,4,2), " +
                                                "rnd_long(), rnd_uuid4(), rnd_long256(), " + timestamp + "::timestamp " +
                                                "from long_sequence(" + (PAGE_FRAME_MAX_ROWS + 1) + ")",
                                        sqlExecutionContext
                                );
                            }
                            timestamp += 86400000000L; // next day
                        }

                        if (convertToParquet) {
                            execute(compiler, "alter table tab convert partition to parquet where ts >= 0", sqlExecutionContext);
                        }

                        final String query = "SELECT " +
                                "count_distinct(anint) cd_int, " +
                                "count_distinct(anipv4) cd_ipv4, " +
                                "count_distinct(asymbol) cd_symbol, " +
                                "count_distinct(along) cd_long, " +
                                "count_distinct(auuid) cd_uuid, " +
                                "count_distinct(along256) cd_long256 " +
                                "FROM tab";

                        // Run with single-threaded GROUP BY.
                        sqlExecutionContext.setParallelGroupByEnabled(false);
                        try {
                            TestUtils.printSql(
                                    engine,
                                    sqlExecutionContext,
                                    query,
                                    sink
                            );
                        } finally {
                            sqlExecutionContext.setParallelGroupByEnabled(engine.getConfiguration().isSqlParallelGroupByEnabled());
                        }

                        // Run with parallel GROUP BY.
                        sqlExecutionContext.setParallelGroupByEnabled(true);
                        final StringSink sinkB = new StringSink();
                        try {
                            TestUtils.printSql(
                                    engine,
                                    sqlExecutionContext,
                                    query,
                                    sinkB
                            );
                        } finally {
                            sqlExecutionContext.setParallelGroupByEnabled(engine.getConfiguration().isSqlParallelGroupByEnabled());
                        }

                        // Compare the results.
                        TestUtils.assertEquals(sink, sinkB);
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testParallelCountOverMultiKeyGroupBy() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelMultiSymbolKeyGroupBy(
                "SELECT count(*) FROM (SELECT key1, key2 FROM tab GROUP BY key1, key2 ORDER BY key1, key2)",
                """
                        count
                        20
                        """
        );
    }

    @Test
    public void testParallelCountOverStringKeyGroupBy() throws Exception {
        testParallelStringAndVarcharKeyGroupBy(
                "SELECT count(*) FROM (SELECT key FROM tab WHERE key IS NOT NULL GROUP BY key ORDER BY key)",
                """
                        count
                        5
                        """
        );
    }

    @Test
    public void testParallelDecimalKeyGroupBy() throws Exception {
        testParallelDecimalKeyGroupBy(
                "SELECT key, " +
                        "sum(d8) sum_d8,     avg(d8) avg_d8,     avg(d8,1) avg2_d8,     min(d8) min_d8,     max(d8) max_d8,     first(d8) first_d8,     last(d8) last_d8,     first_not_null(d8) firstn_d8,     last_not_null(d8) lastn_d8, " +
                        "sum(d16) sum_d16,   avg(d16) avg_d16,   avg(d16,2) avg2_d16,   min(d16) min_d16,   max(d16) max_d16,   first(d16) first_d16,   last(d16) last_d16,   first_not_null(d16) firstn_d16,   last_not_null(d16) lastn_d16, " +
                        "sum(d32) sum_d32,   avg(d32) avg_d32,   avg(d32,3) avg2_d32,   min(d32) min_d32,   max(d32) max_d32,   first(d32) first_d32,   last(d32) last_d32,   first_not_null(d32) firstn_d32,   last_not_null(d32) lastn_d32, " +
                        "sum(d64) sum_d64,   avg(d64) avg_d64,   avg(d64,5) avg2_d64,   min(d64) min_d64,   max(d64) max_d64,   first(d64) first_d64,   last(d64) last_d64,   first_not_null(d64) firstn_d64,   last_not_null(d64) lastn_d64, " +
                        "sum(d128) sum_d128, avg(d128) avg_d128, avg(d128,1) avg2_d128, min(d128) min_d128, max(d128) max_d128, first(d128) first_d128, last(d128) last_d128, first_not_null(d128) firstn_d128, last_not_null(d128) lastn_d128, " +
                        "sum(d256) sum_d256, avg(d256) avg_d256, avg(d256,1) avg2_d256, min(d256) min_d256, max(d256) max_d256, first(d256) first_d256, last(d256) last_d256, first_not_null(d256) firstn_d256, last_not_null(d256) lastn_d256 " +
                        "FROM tab " +
                        "ORDER BY key",
                """
                        key\tsum_d8\tavg_d8\tavg2_d8\tmin_d8\tmax_d8\tfirst_d8\tlast_d8\tfirstn_d8\tlastn_d8\tsum_d16\tavg_d16\tavg2_d16\tmin_d16\tmax_d16\tfirst_d16\tlast_d16\tfirstn_d16\tlastn_d16\tsum_d32\tavg_d32\tavg2_d32\tmin_d32\tmax_d32\tfirst_d32\tlast_d32\tfirstn_d32\tlastn_d32\tsum_d64\tavg_d64\tavg2_d64\tmin_d64\tmax_d64\tfirst_d64\tlast_d64\tfirstn_d64\tlastn_d64\tsum_d128\tavg_d128\tavg2_d128\tmin_d128\tmax_d128\tfirst_d128\tlast_d128\tfirstn_d128\tlastn_d128\tsum_d256\tavg_d256\tavg2_d256\tmin_d256\tmax_d256\tfirst_d256\tlast_d256\tfirstn_d256\tlastn_d256
                        0\t400\t0\t0.5\t0\t1\t1\t0\t1\t0\t2000.0\t2.5\t2.50\t0.0\t5.0\t5.0\t0.0\t5.0\t0.0\t6000.0\t7.5\t7.500\t0.0\t15.0\t5.0\t0.0\t5.0\t0.0\t9990.00\t12.49\t12.48750\t0.00\t25.00\t5.00\t10.00\t5.00\t10.00\t14000.000\t17.500\t17.5\t0.000\t35.000\t5.000\t0.000\t5.000\t0.000\t38000.000000\t47.500000\t47.5\t0.000000\t95.000000\t5.000000\t0.000000\t5.000000\t0.000000
                        1\t400\t0\t0.5\t0\t1\t1\t0\t1\t0\t2800.0\t3.5\t3.50\t1.0\t6.0\t1.0\t6.0\t1.0\t6.0\t6800.0\t8.5\t8.500\t1.0\t16.0\t1.0\t16.0\t1.0\t16.0\t10780.00\t13.48\t13.47500\t1.00\t26.00\t1.00\t6.00\t1.00\t6.00\t14800.000\t18.500\t18.5\t1.000\t36.000\t1.000\t36.000\t1.000\t36.000\t38800.000000\t48.500000\t48.5\t1.000000\t96.000000\t1.000000\t96.000000\t1.000000\t96.000000
                        2\t400\t0\t0.5\t0\t1\t0\t1\t0\t1\t3600.0\t4.5\t4.50\t2.0\t7.0\t2.0\t7.0\t2.0\t7.0\t7600.0\t9.5\t9.500\t2.0\t17.0\t2.0\t17.0\t2.0\t17.0\t11580.00\t14.48\t14.47500\t2.00\t27.00\t2.00\t7.00\t2.00\t7.00\t15600.000\t19.500\t19.5\t2.000\t37.000\t2.000\t37.000\t2.000\t37.000\t39600.000000\t49.500000\t49.5\t2.000000\t97.000000\t2.000000\t97.000000\t2.000000\t97.000000
                        3\t400\t0\t0.5\t0\t1\t1\t0\t1\t0\t4400.0\t5.5\t5.50\t3.0\t8.0\t3.0\t8.0\t3.0\t8.0\t8400.0\t10.5\t10.500\t3.0\t18.0\t3.0\t18.0\t3.0\t18.0\t12380.00\t15.48\t15.47500\t3.00\t28.00\t3.00\t8.00\t3.00\t8.00\t16400.000\t20.500\t20.5\t3.000\t38.000\t3.000\t38.000\t3.000\t38.000\t40400.000000\t50.500000\t50.5\t3.000000\t98.000000\t3.000000\t98.000000\t3.000000\t98.000000
                        4\t400\t0\t0.5\t0\t1\t0\t1\t0\t1\t5200.0\t6.5\t6.50\t4.0\t9.0\t4.0\t9.0\t4.0\t9.0\t9200.0\t11.5\t11.500\t4.0\t19.0\t4.0\t19.0\t4.0\t19.0\t13180.00\t16.48\t16.47500\t4.00\t29.00\t4.00\t9.00\t4.00\t9.00\t17200.000\t21.500\t21.5\t4.000\t39.000\t4.000\t39.000\t4.000\t39.000\t41200.000000\t51.500000\t51.5\t4.000000\t99.000000\t4.000000\t99.000000\t4.000000\t99.000000
                        """
        );
    }

    @Test
    public void testParallelFunctionKeyExplicitGroupBy() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelSymbolKeyGroupBy(
                "SELECT day_of_week(ts) day, key, vwap(price, quantity), sum(colTop) FROM tab GROUP BY day, key ORDER BY day, key",
                """
                        day\tkey\tvwap\tsum
                        1\tk0\t2848.23852863102\t263700.0
                        1\tk1\t2848.94253657797\t263820.0
                        1\tk2\t2849.6468136697736\t263940.0
                        1\tk3\t2850.3513595394984\t264060.0
                        1\tk4\t2851.05617382088\t264180.0
                        2\tk0\t2624.4694763291645\t239025.0
                        2\tk1\t2598.96097084443\t235085.0
                        2\tk2\t2599.691650489951\t235195.0
                        2\tk3\t2600.4225929755667\t235305.0
                        2\tk4\t2601.153797916691\t235415.0
                        3\tk0\t2526.5384615384614\t204750.0
                        3\tk1\t2527.3046131315596\t204850.0
                        3\tk2\t2528.070992925104\t204950.0
                        3\tk3\t2528.8376005852233\t205050.0
                        3\tk4\t2529.6044357786986\t205150.0
                        4\tk0\t2594.679907219484\t215425.0
                        4\tk1\t2595.0011126435716\t215585.0
                        4\tk2\t2595.617813662006\t215695.0
                        4\tk3\t2596.234922950459\t215805.0
                        4\tk4\t2596.8524398569757\t215915.0
                        5\tk0\t2651.1220904699167\t227700.0
                        5\tk1\t2651.7251338776227\t227820.0
                        5\tk2\t2652.3285952443625\t227940.0
                        5\tk3\t2652.9324739103745\t228060.0
                        5\tk4\t2653.5367692172845\t228180.0
                        6\tk0\t2713.3938256153524\t239700.0
                        6\tk1\t2714.035610040864\t239820.0
                        6\tk2\t2714.6777527715262\t239940.0
                        6\tk3\t2715.3202532700157\t240060.0
                        6\tk4\t2715.9631110000832\t240180.0
                        7\tk0\t2779.263011521653\t251700.0
                        7\tk1\t2779.938130410611\t251820.0
                        7\tk2\t2780.6135587838376\t251940.0
                        7\tk3\t2781.2892961993175\t252060.0
                        7\tk4\t2781.9653422158776\t252180.0
                        """,
                null
        );
    }

    @Test
    public void testParallelFunctionKeyGroupByMultipleKeys1() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelSymbolKeyGroupBy(
                "SELECT vwap(price, quantity), day_of_week(ts) day, hour(ts) hour, sum(colTop) " +
                        "FROM tab ORDER BY day, hour",
                """
                        vwap\tday\thour\tsum
                        2816.111833952912\t1\t0\t64560.0
                        2819.2256743179537\t1\t1\t51756.0
                        2821.9986885751755\t1\t2\t51852.0
                        2824.776237776238\t1\t3\t51948.0
                        2827.5582968257627\t1\t4\t52044.0
                        2830.3448408131953\t1\t5\t52140.0
                        2833.485377430715\t1\t6\t65310.0
                        2836.630835052334\t1\t7\t52356.0
                        2839.4317852512772\t1\t8\t52452.0
                        2842.2371165410673\t1\t9\t52548.0
                        2845.046804954031\t1\t10\t52644.0
                        2847.860826697004\t1\t11\t52740.0
                        2851.0320920375416\t1\t12\t66060.0
                        2854.208097288315\t1\t13\t52956.0
                        2857.0360401115886\t1\t14\t53052.0
                        2859.8682170542634\t1\t15\t53148.0
                        2862.704605213733\t1\t16\t53244.0
                        2865.5451818522683\t1\t17\t53340.0
                        2868.746145786559\t1\t18\t66810.0
                        2871.9516767495707\t1\t19\t53556.0
                        2874.805710877507\t1\t20\t53652.0
                        2877.6638386544614\t1\t21\t53748.0
                        2880.5260381843846\t1\t22\t53844.0
                        2883.3922877271043\t1\t23\t53940.0
                        2736.632776425153\t2\t0\t67560.0
                        2695.944281906248\t2\t1\t54156.0
                        2698.804979342865\t2\t2\t54252.0
                        2701.6700058291412\t2\t3\t54348.0
                        2704.539336737992\t2\t4\t54444.0
                        2707.412947628777\t2\t5\t54540.0
                        2710.6511997252865\t2\t6\t68310.0
                        2713.8940954746963\t2\t7\t54756.0
                        2716.7814497338663\t2\t8\t54852.0
                        2719.6729821417143\t2\t9\t54948.0
                        2722.568669207999\t2\t10\t55044.0
                        2725.4684876182378\t2\t11\t55140.0
                        2517.6369896704377\t2\t12\t52850.0
                        2457.3950932788143\t2\t13\t39130.0
                        2460.37311910227\t2\t14\t39210.0
                        2463.3553066938152\t2\t15\t39290.0
                        2466.3416306832614\t2\t16\t39370.0
                        2469.3320659062106\t2\t17\t39450.0
                        2472.7015680323725\t2\t18\t49425.0
                        2476.0754478930103\t2\t19\t39630.0
                        2479.0790732812893\t2\t20\t39710.0
                        2482.0867052023123\t2\t21\t39790.0
                        2485.0983195385\t2\t22\t39870.0
                        2488.1138923654566\t2\t23\t39950.0
                        2491.5114885114886\t3\t0\t50050.0
                        2494.9132818340395\t3\t1\t40130.0
                        2497.94155682666\t3\t2\t40210.0
                        2500.9736907421197\t3\t3\t40290.0
                        2504.0096606390885\t3\t4\t40370.0
                        2507.0494437577254\t3\t5\t40450.0
                        2510.474099654662\t3\t6\t50675.0
                        2513.9027811961605\t3\t7\t40630.0
                        2516.954802259887\t3\t8\t40710.0
                        2520.0105417994605\t3\t9\t40790.0
                        2523.0699779789575\t3\t10\t40870.0
                        2526.133089133089\t3\t11\t40950.0
                        2529.583820662768\t3\t12\t51300.0
                        2533.0384147823975\t3\t13\t41130.0
                        2536.113322009221\t3\t14\t41210.0
                        2539.191813998547\t3\t15\t41290.0
                        2542.273869954073\t3\t16\t41370.0
                        2545.3594692400484\t3\t17\t41450.0
                        2548.835339431873\t3\t18\t51925.0
                        2552.314917127072\t3\t19\t41630.0
                        2555.411891632702\t3\t20\t41710.0
                        2558.5123235223737\t3\t21\t41790.0
                        2561.616192978266\t3\t22\t41870.0
                        2564.7234803337305\t3\t23\t41950.0
                        2567.979545238322\t4\t0\t52550.0
                        2570.9360273355005\t4\t1\t42130.0
                        2573.570434041344\t4\t2\t42210.0
                        2576.2105200973556\t4\t3\t42290.0
                        2578.856250147381\t4\t4\t42370.0
                        2581.5075891281326\t4\t5\t42450.0
                        2584.4973939991546\t4\t6\t53175.0
                        2587.4934298362728\t4\t7\t42630.0
                        2590.1627591687898\t4\t8\t42710.0
                        2592.837551610721\t4\t9\t42790.0
                        2595.517773587541\t4\t10\t42870.0
                        2598.20339179928\t4\t11\t42950.0
                        2596.278893309892\t4\t12\t54010.0
                        2597.6253344404467\t4\t13\t43356.0
                        2599.8774739942924\t4\t14\t43452.0
                        2602.1373197391385\t4\t15\t43548.0
                        2604.4048208230224\t4\t16\t43644.0
                        2606.6799268404206\t4\t17\t43740.0
                        2609.2488596971357\t4\t18\t54810.0
                        2611.826462826463\t4\t19\t43956.0
                        2614.1259420684646\t4\t20\t44052.0
                        2616.4328168886473\t4\t21\t44148.0
                        2618.747039146551\t4\t22\t44244.0
                        2621.0685611186286\t4\t23\t44340.0
                        2623.689344852412\t5\t0\t55560.0
                        2626.3184307388456\t5\t1\t44556.0
                        2628.6633521454805\t5\t2\t44652.0
                        2631.0153749888264\t5\t3\t44748.0
                        2633.3744536615823\t5\t4\t44844.0
                        2635.7405429461505\t5\t5\t44940.0
                        2638.411117030723\t5\t6\t56310.0
                        2641.089644786961\t5\t7\t45156.0
                        2643.478210907805\t5\t8\t45252.0
                        2645.873599717738\t5\t9\t45348.0
                        2648.2757679781707\t5\t10\t45444.0
                        2650.6846728151077\t5\t11\t45540.0
                        2653.4030844724853\t5\t12\t57060.0
                        2656.1291196782936\t5\t13\t45756.0
                        2658.5596266247926\t5\t14\t45852.0
                        2660.996691912597\t5\t15\t45948.0
                        2663.440274520024\t5\t16\t46044.0
                        2665.8903337667966\t5\t17\t46140.0
                        2668.654731015395\t5\t18\t57810.0
                        2671.4264388644406\t5\t19\t46356.0
                        2673.897270300525\t5\t20\t46452.0
                        2676.3744092119964\t5\t21\t46548.0
                        2678.857816653803\t5\t22\t46644.0
                        2681.3474540008556\t5\t23\t46740.0
                        2684.156079234973\t6\t0\t58560.0
                        2686.9717182042764\t6\t1\t46956.0
                        2689.4813397942703\t6\t2\t47052.0
                        2691.997030626962\t6\t3\t47148.0
                        2694.5187537041743\t6\t4\t47244.0
                        2697.046472327841\t6\t5\t47340.0
                        2699.897656381723\t6\t6\t59310.0
                        2702.7555723778282\t6\t7\t47556.0
                        2705.302526651557\t6\t8\t47652.0
                        2707.855323783195\t6\t9\t47748.0
                        2710.4139286012874\t6\t10\t47844.0
                        2712.9783062161036\t6\t11\t47940.0
                        2715.8704628704627\t6\t12\t60060.0
                        2718.769083810948\t6\t13\t48156.0
                        2721.3519854099313\t6\t14\t48252.0
                        2723.9405146024656\t6\t15\t48348.0
                        2726.5346379324583\t6\t16\t48444.0
                        2729.134322208488\t6\t17\t48540.0
                        2732.0659431014637\t6\t18\t60810.0
                        2735.003773894495\t6\t19\t48756.0
                        2737.621305166626\t6\t20\t48852.0
                        2740.2442592138596\t6\t21\t48948.0
                        2742.872604192154\t6\t22\t49044.0
                        2745.5063085063084\t6\t23\t49140.0
                        2748.475958414555\t7\t0\t61560.0
                        2751.45157630278\t7\t1\t49356.0
                        2754.102483216048\t7\t2\t49452.0
                        2756.758617905869\t7\t3\t49548.0
                        2759.4199500443156\t7\t4\t49644.0
                        2762.0864495375954\t7\t5\t49740.0
                        2765.092761996469\t7\t6\t62310.0
                        2768.1048122347665\t7\t7\t49956.0
                        2770.7879005833934\t7\t8\t50052.0
                        2773.4760309483927\t7\t9\t50148.0
                        2776.1691744287873\t7\t10\t50244.0
                        2778.8673023440606\t7\t11\t50340.0
                        2781.9089755788136\t7\t12\t63060.0
                        2784.9561674183083\t7\t13\t50556.0
                        2787.670299297165\t7\t14\t50652.0
                        2790.3892961298966\t7\t15\t50748.0
                        2793.113130359531\t7\t16\t50844.0
                        2795.8417746368277\t7\t17\t50940.0
                        2798.917567779345\t7\t18\t63810.0
                        2801.9986707326607\t7\t19\t51156.0
                        2804.7427612580973\t7\t20\t51252.0
                        2807.4915478694397\t7\t21\t51348.0
                        2810.245004276495\t7\t22\t51444.0
                        2813.0031043849435\t7\t23\t51540.0
                        """,
                null
        );
    }

    @Test
    public void testParallelFunctionKeyGroupByMultipleKeys2() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelSymbolKeyGroupBy(
                "SELECT vwap(price, quantity), day_of_week(ts) day, sum(colTop), regexp_replace(key, 'k0', 'k42') key " +
                        "FROM tab " +
                        "ORDER BY day, key",
                """
                        vwap\tday\tsum\tkey
                        2848.94253657797\t1\t263820.0\tk1
                        2849.6468136697736\t1\t263940.0\tk2
                        2850.3513595394984\t1\t264060.0\tk3
                        2851.05617382088\t1\t264180.0\tk4
                        2848.23852863102\t1\t263700.0\tk42
                        2598.96097084443\t2\t235085.0\tk1
                        2599.691650489951\t2\t235195.0\tk2
                        2600.4225929755667\t2\t235305.0\tk3
                        2601.153797916691\t2\t235415.0\tk4
                        2624.4694763291645\t2\t239025.0\tk42
                        2527.3046131315596\t3\t204850.0\tk1
                        2528.070992925104\t3\t204950.0\tk2
                        2528.8376005852233\t3\t205050.0\tk3
                        2529.6044357786986\t3\t205150.0\tk4
                        2526.5384615384614\t3\t204750.0\tk42
                        2595.0011126435716\t4\t215585.0\tk1
                        2595.617813662006\t4\t215695.0\tk2
                        2596.234922950459\t4\t215805.0\tk3
                        2596.8524398569757\t4\t215915.0\tk4
                        2594.679907219484\t4\t215425.0\tk42
                        2651.7251338776227\t5\t227820.0\tk1
                        2652.3285952443625\t5\t227940.0\tk2
                        2652.9324739103745\t5\t228060.0\tk3
                        2653.5367692172845\t5\t228180.0\tk4
                        2651.1220904699167\t5\t227700.0\tk42
                        2714.035610040864\t6\t239820.0\tk1
                        2714.6777527715262\t6\t239940.0\tk2
                        2715.3202532700157\t6\t240060.0\tk3
                        2715.9631110000832\t6\t240180.0\tk4
                        2713.3938256153524\t6\t239700.0\tk42
                        2779.938130410611\t7\t251820.0\tk1
                        2780.6135587838376\t7\t251940.0\tk2
                        2781.2892961993175\t7\t252060.0\tk3
                        2781.9653422158776\t7\t252180.0\tk4
                        2779.263011521653\t7\t251700.0\tk42
                        """,
                null
        );
    }

    @Test
    public void testParallelFunctionKeyGroupByThreadSafe() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelSymbolKeyGroupBy(
                "SELECT day_of_week(ts) day, key, vwap(price, quantity), sum(colTop) FROM tab ORDER BY day, key",
                """
                        day\tkey\tvwap\tsum
                        1\tk0\t2848.23852863102\t263700.0
                        1\tk1\t2848.94253657797\t263820.0
                        1\tk2\t2849.6468136697736\t263940.0
                        1\tk3\t2850.3513595394984\t264060.0
                        1\tk4\t2851.05617382088\t264180.0
                        2\tk0\t2624.4694763291645\t239025.0
                        2\tk1\t2598.96097084443\t235085.0
                        2\tk2\t2599.691650489951\t235195.0
                        2\tk3\t2600.4225929755667\t235305.0
                        2\tk4\t2601.153797916691\t235415.0
                        3\tk0\t2526.5384615384614\t204750.0
                        3\tk1\t2527.3046131315596\t204850.0
                        3\tk2\t2528.070992925104\t204950.0
                        3\tk3\t2528.8376005852233\t205050.0
                        3\tk4\t2529.6044357786986\t205150.0
                        4\tk0\t2594.679907219484\t215425.0
                        4\tk1\t2595.0011126435716\t215585.0
                        4\tk2\t2595.617813662006\t215695.0
                        4\tk3\t2596.234922950459\t215805.0
                        4\tk4\t2596.8524398569757\t215915.0
                        5\tk0\t2651.1220904699167\t227700.0
                        5\tk1\t2651.7251338776227\t227820.0
                        5\tk2\t2652.3285952443625\t227940.0
                        5\tk3\t2652.9324739103745\t228060.0
                        5\tk4\t2653.5367692172845\t228180.0
                        6\tk0\t2713.3938256153524\t239700.0
                        6\tk1\t2714.035610040864\t239820.0
                        6\tk2\t2714.6777527715262\t239940.0
                        6\tk3\t2715.3202532700157\t240060.0
                        6\tk4\t2715.9631110000832\t240180.0
                        7\tk0\t2779.263011521653\t251700.0
                        7\tk1\t2779.938130410611\t251820.0
                        7\tk2\t2780.6135587838376\t251940.0
                        7\tk3\t2781.2892961993175\t252060.0
                        7\tk4\t2781.9653422158776\t252180.0
                        """,
                null
        );
    }

    @Test
    public void testParallelFunctionKeyGroupByThreadUnsafe() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelSymbolKeyGroupBy(
                "SELECT regexp_replace(key, 'k0', 'k42') key, vwap(price, quantity), sum(colTop) FROM tab ORDER BY key",
                """
                        key\tvwap\tsum
                        k1\t2682.7321472695826\t1638800.0
                        k2\t2683.4065201284266\t1639600.0
                        k3\t2684.081214514935\t1640400.0
                        k4\t2684.756229953121\t1641200.0
                        k42\t2685.431565967941\t1642000.0
                        """,
                null
        );
    }

    @Test
    public void testParallelFunctionKeyGroupByThreadUnsafe2() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        // This query shouldn't be executed in parallel,
        // so this test verifies that nothing breaks.
        testParallelSymbolKeyGroupBy(
                "SELECT concat(key, 'abc')::symbol key, vwap(price, quantity), sum(colTop) FROM tab ORDER BY key",
                """
                        key\tvwap\tsum
                        k0abc\t2685.431565967941\t1642000.0
                        k1abc\t2682.7321472695826\t1638800.0
                        k2abc\t2683.4065201284266\t1639600.0
                        k3abc\t2684.081214514935\t1640400.0
                        k4abc\t2684.756229953121\t1641200.0
                        """,
                null
        );
    }

    @Test
    public void testParallelFunctionKeyGroupByThreadUnsafe3() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        // This query shouldn't be executed in parallel,
        // so this test verifies that nothing breaks.
        testParallelStringAndVarcharKeyGroupBy(
                "SELECT key::symbol key, avg(value), sum(colTop) FROM tab ORDER BY key",
                """
                        key\tavg\tsum
                        k0\t1027.5\t421000.0
                        k1\t1023.5\t419400.0
                        k2\t1024.5\t419800.0
                        k3\t1025.5\t420200.0
                        k4\t1026.5\t420600.0
                        """
        );
    }

    @Test
    public void testParallelGroupByArray() throws Exception {
        Assume.assumeFalse(convertToParquet);
        testParallelGroupByArray(
                "SELECT first(darr), last(darr), key FROM tab order by key",
                """
                        first\tlast\tkey
                        [[null,null,null],[null,0.7883065830055033,null]]\t[[0.8522582952903538,0.6179906752583175],[null,null],[null,null]]\tk0
                        [[null,0.20447441837877756],[null,null]]\t[[null,null],[null,0.9164539569237466],[null,null]]\tk1
                        [[0.3491070363730514,0.7611029514995744],[0.4217768841969397,null],[0.7261136209823622,0.4224356661645131]]\t[[0.47845408543565093,null,0.19197284817490712],[null,null,0.21496623812935467]]\tk2
                        [[null,0.33608255572515877],[0.690540444367637,null]]\t[[0.7339245159010606,null],[0.39425956944686746,0.55078841544971]]\tk3
                        [[null,null],[0.12503042190293423,null]]\t[[null,0.6489095881388134],[0.280119654942501,null],[0.5379723582047159,null]]\tk4
                        """
        );
    }

    @Test
    public void testParallelGroupByArrayFunction() throws Exception {
        Assume.assumeTrue(!convertToParquet && enableParallelGroupBy);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);

                        execute(compiler,
                                "create table tango (ts timestamp, a double, arr double[]) timestamp(ts) partition by DAY",
                                sqlExecutionContext);
                        execute(compiler,
                                "insert into tango values " +
                                        "('2025-06-26', 1.0, ARRAY[1.0,2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0])," +
                                        "('2025-06-26', 10.0, null)," +
                                        "('2025-06-27', 18.0, ARRAY[11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0])," +
                                        "('2025-06-27', 25.0, ARRAY[21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0])," +
                                        "('2025-06-28', 38.0, ARRAY[11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0])," +
                                        "('2025-06-28', 35.0, ARRAY[21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0])," +
                                        "('2025-06-29', 48.0, ARRAY[11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0])," +
                                        "('2025-06-29', 45.0, ARRAY[21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0])," +
                                        "('2025-06-30', 58.0, ARRAY[11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0])," +
                                        "('2025-06-30', 55.0, ARRAY[21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0])",
                                sqlExecutionContext);

                        assertQueries(engine, sqlExecutionContext,
                                "explain select ts, max(array_position(arr, a)) as v from tango sample by 1d",
                                """
                                        QUERY PLAN
                                        Radix sort light
                                          keys: [ts]
                                            Async Group By workers: 4
                                              keys: [ts]
                                              values: [max(array_position(arr, a))]
                                              filter: null
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tango
                                        """,
                                "select ts, max(array_position(arr, a)) as v from tango sample by 1d",
                                """
                                        ts\tv
                                        2025-06-26T00:00:00.000000Z\t1
                                        2025-06-27T00:00:00.000000Z\t8
                                        2025-06-28T00:00:00.000000Z\tnull
                                        2025-06-29T00:00:00.000000Z\tnull
                                        2025-06-30T00:00:00.000000Z\tnull
                                        """
                        );

                        assertQueries(engine, sqlExecutionContext,
                                "explain select ts, max(array_position(arr, a)) as v from tango sample by 1d",
                                """
                                        QUERY PLAN
                                        Radix sort light
                                          keys: [ts]
                                            Async Group By workers: 4
                                              keys: [ts]
                                              values: [max(array_position(arr, a))]
                                              filter: null
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tango
                                        """,
                                "select ts, max(array_position(arr, a)) as v from tango sample by 1d",
                                """
                                        ts\tv
                                        2025-06-26T00:00:00.000000Z\t1
                                        2025-06-27T00:00:00.000000Z\t8
                                        2025-06-28T00:00:00.000000Z\tnull
                                        2025-06-29T00:00:00.000000Z\tnull
                                        2025-06-30T00:00:00.000000Z\tnull
                                        """
                        );

                        assertQueries(engine, sqlExecutionContext,
                                "explain select ts, min(insertion_point(arr, a)) as v from tango sample by 1d",
                                """
                                        QUERY PLAN
                                        Radix sort light
                                          keys: [ts]
                                            Async Group By workers: 4
                                              keys: [ts]
                                              values: [min(insertion_point(arr,a))]
                                              filter: null
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tango
                                        """,
                                "select ts, min(insertion_point(arr, a)) as v from tango sample by 1d",
                                """
                                        ts\tv
                                        2025-06-26T00:00:00.000000Z\t2
                                        2025-06-27T00:00:00.000000Z\t6
                                        2025-06-28T00:00:00.000000Z\t11
                                        2025-06-29T00:00:00.000000Z\t11
                                        2025-06-30T00:00:00.000000Z\t11
                                        """
                        );

                        assertQueries(engine, sqlExecutionContext,
                                "explain select ts, sum(array_count(arr)) as v from tango sample by 1d",
                                """
                                        QUERY PLAN
                                        Radix sort light
                                          keys: [ts]
                                            Async Group By workers: 4
                                              keys: [ts]
                                              values: [sum(array_count(arr))]
                                              filter: null
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tango
                                        """,
                                "select ts, sum(array_count(arr)) as v from tango sample by 1d",
                                """
                                        ts\tv
                                        2025-06-26T00:00:00.000000Z\t10
                                        2025-06-27T00:00:00.000000Z\t20
                                        2025-06-28T00:00:00.000000Z\t20
                                        2025-06-29T00:00:00.000000Z\t20
                                        2025-06-30T00:00:00.000000Z\t20
                                        """
                        );

                        assertQueries(engine, sqlExecutionContext,
                                "explain select ts, sum(array_avg(arr)) as v from tango sample by 1d",
                                """
                                        QUERY PLAN
                                        Radix sort light
                                          keys: [ts]
                                            Async Group By workers: 4
                                              keys: [ts]
                                              values: [sum(array_avg(arr))]
                                              filter: null
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tango
                                        """,
                                "select ts, sum(array_avg(arr)) as v from tango sample by 1d",
                                """
                                        ts\tv
                                        2025-06-26T00:00:00.000000Z\t5.5
                                        2025-06-27T00:00:00.000000Z\t41.0
                                        2025-06-28T00:00:00.000000Z\t41.0
                                        2025-06-29T00:00:00.000000Z\t41.0
                                        2025-06-30T00:00:00.000000Z\t41.0
                                        """
                        );

                        assertQueries(engine, sqlExecutionContext,
                                "explain select ts, array_sum(array_cum_sum(arr)), sum(a) from tango sample by 1d order by ts, array_sum",
                                """
                                        QUERY PLAN
                                        Sort light
                                          keys: [ts, array_sum]
                                            Async Group By workers: 4
                                              keys: [ts,array_sum]
                                              values: [sum(a)]
                                              filter: null
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tango
                                        """,
                                "select ts, array_sum(array_cum_sum(arr)), sum(a) from tango sample by 1d order by ts, array_sum",
                                """
                                        ts\tarray_sum\tsum
                                        2025-06-26T00:00:00.000000Z\t220.0\t1.0
                                        2025-06-26T00:00:00.000000Z\tnull\t10.0
                                        2025-06-27T00:00:00.000000Z\t770.0\t18.0
                                        2025-06-27T00:00:00.000000Z\t1320.0\t25.0
                                        2025-06-28T00:00:00.000000Z\t770.0\t38.0
                                        2025-06-28T00:00:00.000000Z\t1320.0\t35.0
                                        2025-06-29T00:00:00.000000Z\t770.0\t48.0
                                        2025-06-29T00:00:00.000000Z\t1320.0\t45.0
                                        2025-06-30T00:00:00.000000Z\t770.0\t58.0
                                        2025-06-30T00:00:00.000000Z\t1320.0\t55.0
                                        """
                        );

                        assertQueries(engine, sqlExecutionContext,
                                "explain select ts, dot_product(arr, 2), first(a) from tango sample by 1d",
                                """
                                        QUERY PLAN
                                        Radix sort light
                                          keys: [ts]
                                            Async Group By workers: 4
                                              keys: [ts,dot_product]
                                              values: [first(a)]
                                              filter: null
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tango
                                        """,
                                "select ts, dot_product(arr, 2), first(a) from tango sample by 1d order by ts, dot_product",
                                """
                                        ts\tdot_product\tfirst
                                        2025-06-26T00:00:00.000000Z\t110.0\t1.0
                                        2025-06-26T00:00:00.000000Z\tnull\t10.0
                                        2025-06-27T00:00:00.000000Z\t310.0\t18.0
                                        2025-06-27T00:00:00.000000Z\t510.0\t25.0
                                        2025-06-28T00:00:00.000000Z\t310.0\t38.0
                                        2025-06-28T00:00:00.000000Z\t510.0\t35.0
                                        2025-06-29T00:00:00.000000Z\t310.0\t48.0
                                        2025-06-29T00:00:00.000000Z\t510.0\t45.0
                                        2025-06-30T00:00:00.000000Z\t310.0\t58.0
                                        2025-06-30T00:00:00.000000Z\t510.0\t55.0
                                        """
                        );

                        assertQueries(engine, sqlExecutionContext,
                                "explain select ts, sum(array_sum((arr * 5 + 3 - 1)/2)) from tango sample by 1d",
                                """
                                        QUERY PLAN
                                        Radix sort light
                                          keys: [ts]
                                            Async Group By workers: 4
                                              keys: [ts]
                                              values: [sum(array_sum(arr*5+3-1/2))]
                                              filter: null
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tango
                                        """,
                                "select ts, sum(array_sum((arr * 5 + 3 - 1)/2)) from tango sample by 1d",
                                """
                                        ts\tsum
                                        2025-06-26T00:00:00.000000Z\t147.5
                                        2025-06-27T00:00:00.000000Z\t1045.0
                                        2025-06-28T00:00:00.000000Z\t1045.0
                                        2025-06-29T00:00:00.000000Z\t1045.0
                                        2025-06-30T00:00:00.000000Z\t1045.0
                                        """
                        );

                        assertQueries(engine, sqlExecutionContext,
                                "explain select ts, sum(array_sum(arr[a::int:a::int + 2])) from tango sample by 1d",
                                """
                                        QUERY PLAN
                                        Radix sort light
                                          keys: [ts]
                                            Async Group By workers: 4
                                              keys: [ts]
                                              values: [sum(array_sum(arr[a::int:a::int+2]))]
                                              filter: null
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tango
                                        """,
                                "select ts, sum(array_sum(arr[a::int:a::int + 2])) from tango sample by 1d",
                                """
                                        ts\tsum
                                        2025-06-26T00:00:00.000000Z\t3.0
                                        2025-06-27T00:00:00.000000Z\tnull
                                        2025-06-28T00:00:00.000000Z\tnull
                                        2025-06-29T00:00:00.000000Z\tnull
                                        2025-06-30T00:00:00.000000Z\tnull
                                        """
                        );
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testParallelGroupByArrayNull() throws Exception {
        Assume.assumeFalse(convertToParquet);

        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);

                        execute(
                                compiler,
                                "create table tab as (select" +
                                        " 'k' || ((50 + x) % 5) key," +
                                        " timestamp_sequence(400000000000, 500000000) ts" +
                                        " from long_sequence(" + ROW_COUNT + ")) timestamp(ts) partition by day",
                                sqlExecutionContext
                        );
                        // Add double[], all values should be null
                        execute(
                                compiler,
                                "alter table tab add column darr double[]",
                                sqlExecutionContext
                        );

                        assertQueries(engine, sqlExecutionContext, "SELECT first(darr), key FROM tab order by key", """
                                first\tkey
                                null\tk0
                                null\tk1
                                null\tk2
                                null\tk3
                                null\tk4
                                """);
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testParallelGroupByCastToSymbol() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        // The table is non-partitioned.
        Assume.assumeFalse(convertToParquet);
        // This query shouldn't be executed in parallel,
        // so this test verifies that nothing breaks.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        execute(
                                compiler,
                                "create table x as (select * from (select rnd_symbol('a','b','c') a, 'x' || x b from long_sequence(" + ROW_COUNT + ")))",
                                sqlExecutionContext
                        );
                        assertQueries(
                                engine,
                                sqlExecutionContext,
                                """
                                        select a, ct, c
                                        from\s
                                        (
                                          select a, cast(b as SYMBOL) as ct, count(*) c
                                          from x
                                        ) order by a, ct limit 10""",
                                """
                                        a\tct\tc
                                        a\tx1\t1
                                        a\tx100\t1
                                        a\tx1001\t1
                                        a\tx1005\t1
                                        a\tx1007\t1
                                        a\tx1008\t1
                                        a\tx1009\t1
                                        a\tx101\t1
                                        a\tx1010\t1
                                        a\tx1011\t1
                                        """
                        );
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testParallelGroupByCorrelation() throws Exception {
        Assume.assumeTrue(enableParallelGroupBy);
        testParallelGroupByAllTypes(
                "SELECT round(corr(adouble, along), 14) FROM tab", """
                        round
                        -0.01506463207666
                        """
        );
    }

    @Test
    public void testParallelGroupByCovariance() throws Exception {
        Assume.assumeTrue(enableParallelGroupBy);
        testParallelGroupByAllTypes(
                "SELECT round(covar_samp(adouble, along), 14) FROM tab", """
                        round
                        -92233.72036854776
                        """,
                "SELECT round(covar_pop(adouble, along), 13) FROM tab", """
                        round
                        -922337.2036854776
                        """
        );
    }

    @Test
    public void testParallelGroupByRegrIntercept() throws Exception {
        Assume.assumeTrue(enableParallelGroupBy);
        testParallelGroupByAllTypes("SELECT round(regr_intercept(adouble, along), 14) FROM tab", """
                round
                0.50356769718027
                """);
    }

    @Test
    public void testParallelGroupByStdDev() throws Exception {
        Assume.assumeTrue(enableParallelGroupBy);
        testParallelGroupByAllTypes(
                "SELECT round(stddev_samp(adouble), 14) FROM tab", """
                        round
                        0.2851973374189
                        """,
                "SELECT round(stddev(adouble), 14) FROM tab", """
                        round
                        0.2851973374189
                        """,
                "SELECT round(stddev_pop(adouble), 13) FROM tab", """
                        round
                        0.28515456316480003
                        """
        );
    }

    @Test
    public void testParallelGroupByVariance() throws Exception {
        Assume.assumeTrue(enableParallelGroupBy);
        testParallelGroupByAllTypes(
                "SELECT round(var_samp(adouble), 14) FROM tab", """
                        round
                        0.08133752127083
                        """,
                "SELECT round(variance(adouble), 14) FROM tab", """
                        round
                        0.08133752127083
                        """,
                "SELECT round(var_pop(adouble), 13) FROM tab", """
                        round
                        0.0813131248937
                        """
        );
    }

    @Test
    public void testParallelIPv4CastToVarcharKeyGroupBy() throws Exception {
        testParallelIPv4KeyGroupBy(
                "SELECT key::varchar key, max(value) FROM tab WHERE key > '0.0.0.101' ORDER BY key LIMIT 5",
                """
                        key\tmax
                        0.0.0.102\t3902.0
                        0.0.0.103\t3903.0
                        0.0.0.104\t3904.0
                        0.0.0.105\t3905.0
                        0.0.0.106\t3906.0
                        """
        );
    }

    @Test
    public void testParallelJsonKeyGroupBy() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelJsonKeyGroupBy(
                "SELECT json_extract(key, '.key')::varchar key, max(price) FROM tab ORDER BY key",
                """
                        key\tmax
                        k0\t4000.0
                        k1\t3996.0
                        k2\t3997.0
                        k3\t3998.0
                        k4\t3999.0
                        """
        );
    }

    @Test
    public void testParallelJsonKeyGroupByWithFilter() throws Exception {
        testParallelJsonKeyGroupBy(
                "SELECT json_extract(key, '.key')::varchar key, max(price) FROM tab WHERE json_extract(key, '.foo')::varchar = 'bar' ORDER BY key",
                """
                        key\tmax
                        k0\t4000.0
                        k1\t3996.0
                        k2\t3997.0
                        k3\t3998.0
                        k4\t3999.0
                        """
        );
    }

    @Test
    public void testParallelKSumNSum() throws Exception {
        testParallelStringAndVarcharKeyGroupBy(
                "SELECT key, round(ksum(value)) ksum, round(nsum(value)) nsum " +
                        "FROM tab " +
                        "ORDER BY key",
                """
                        key\tksum\tnsum
                        k0\t822000.0\t822000.0
                        k1\t818800.0\t818800.0
                        k2\t819600.0\t819600.0
                        k3\t820400.0\t820400.0
                        k4\t821200.0\t821200.0
                        """
        );
    }

    @Test
    public void testParallelKSumNSum2() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT key, round(ksum(adouble),2) ksum, round(nsum(adouble),2) nsum FROM tab ORDER BY key DESC",
                """
                        key\tksum\tnsum
                        k4\t324.15000000000003\t324.15000000000003
                        k3\t338.48\t338.48
                        k2\t338.86\t338.86
                        k1\t350.17\t350.17
                        k0\t327.49\t327.49
                        """
        );
    }

    @Test
    public void testParallelKeyedGroupByWithLongTopK() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT quantity, max(price) FROM tab ORDER BY quantity ASC LIMIT 10",
                """
                        quantity\tmax
                        1\t1.0
                        2\t2.0
                        3\t3.0
                        4\t4.0
                        5\t5.0
                        6\t6.0
                        7\t7.0
                        8\t8.0
                        9\t9.0
                        10\t10.0
                        """,
                "Long Top K lo: 10"
        );
    }

    @Test
    public void testParallelKeyedGroupByWithLongTopK2() throws Exception {
        // Verifies outer selected factory scenario.
        testParallelSymbolKeyGroupBy(
                "SELECT max_p, sum_q FROM (SELECT concat(key,'_42'), max(price) max_p, sum(quantity) sum_q FROM tab) ORDER BY sum_q DESC LIMIT 10",
                """
                        max_p\tsum_q
                        4050.0\t3244000
                        4049.0\t3242400
                        4048.0\t3240800
                        4047.0\t3239200
                        4046.0\t3237600
                        """,
                "Long Top K lo: 10"
        );
    }

    @Test
    public void testParallelKeyedGroupByWithLongTopK3() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key, last(ts) last_ts FROM tab ORDER BY last_ts DESC LIMIT 10",
                """
                        key\tlast_ts
                        k0\t1970-02-10T12:00:00.000000Z
                        k4\t1970-02-10T11:45:36.000000Z
                        k3\t1970-02-10T11:31:12.000000Z
                        k2\t1970-02-10T11:16:48.000000Z
                        k1\t1970-02-10T11:02:24.000000Z
                        """,
                "Long Top K lo: 10"
        );
    }

    @Test
    public void testParallelKeyedGroupByWithLongTopK4() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key, 'foobar' key2, last(ts) last_ts FROM tab ORDER BY last_ts DESC LIMIT 10",
                """
                        key\tkey2\tlast_ts
                        k0\tfoobar\t1970-02-10T12:00:00.000000Z
                        k4\tfoobar\t1970-02-10T11:45:36.000000Z
                        k3\tfoobar\t1970-02-10T11:31:12.000000Z
                        k2\tfoobar\t1970-02-10T11:16:48.000000Z
                        k1\tfoobar\t1970-02-10T11:02:24.000000Z
                        """,
                "Long Top K lo: 10"
        );
    }

    @Test
    public void testParallelKeyedGroupByWithLongTopK5() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key, sum(quantity) s FROM tab ORDER BY s ASC LIMIT 5",
                """
                        key\ts
                        k1\t3237600
                        k2\t3239200
                        k3\t3240800
                        k4\t3242400
                        k0\t3244000
                        """,
                "Long Top K lo: 5"
        );
    }

    @Test
    public void testParallelMultiJsonKeyGroupBy() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelJsonKeyGroupBy(
                "SELECT json_extract(key, '.key')::varchar key, date_trunc('month', ts) ts, max(price) FROM tab ORDER BY key, ts",
                """
                        key\tts\tmax
                        k0\t1970-01-01T00:00:00.000000Z\t3095.0
                        k0\t1970-02-01T00:00:00.000000Z\t4000.0
                        k1\t1970-01-01T00:00:00.000000Z\t3096.0
                        k1\t1970-02-01T00:00:00.000000Z\t3996.0
                        k2\t1970-01-01T00:00:00.000000Z\t3097.0
                        k2\t1970-02-01T00:00:00.000000Z\t3997.0
                        k3\t1970-01-01T00:00:00.000000Z\t3098.0
                        k3\t1970-02-01T00:00:00.000000Z\t3998.0
                        k4\t1970-01-01T00:00:00.000000Z\t3099.0
                        k4\t1970-02-01T00:00:00.000000Z\t3999.0
                        """
        );
    }

    @Test
    public void testParallelMultiKeyGroupBy1() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelMultiSymbolKeyGroupBy(
                "SELECT key1, key2, avg(value), sum(colTop) FROM tab ORDER BY key1, key2",
                """
                        key1\tkey2\tavg\tsum
                        k0\tk0\t2030.0\t410000.0
                        k0\tk1\t2025.0\t411000.0
                        k0\tk2\t2030.0\t412000.0
                        k0\tk3\t2025.0\t409000.0
                        k1\tk0\t2026.0\t409200.0
                        k1\tk1\t2021.0\t410200.0
                        k1\tk2\t2026.0\t411200.0
                        k1\tk3\t2021.0\t408200.0
                        k2\tk0\t2022.0\t408400.0
                        k2\tk1\t2027.0\t409400.0
                        k2\tk2\t2022.0\t410400.0
                        k2\tk3\t2027.0\t411400.0
                        k3\tk0\t2028.0\t411600.0
                        k3\tk1\t2023.0\t408600.0
                        k3\tk2\t2028.0\t409600.0
                        k3\tk3\t2023.0\t410600.0
                        k4\tk0\t2024.0\t410800.0
                        k4\tk1\t2029.0\t411800.0
                        k4\tk2\t2024.0\t408800.0
                        k4\tk3\t2029.0\t409800.0
                        """
        );
    }

    @Test
    public void testParallelMultiKeyGroupBy2() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelMultiSymbolKeyGroupBy(
                "SELECT key1, key2, key3, avg(value), sum(colTop) FROM tab ORDER BY key1, key2, key3",
                """
                        key1\tkey2\tkey3\tavg\tsum
                        k0\tk0\tk0\t2025.1127819548872\t136680.0
                        k0\tk0\tk1\t2034.8872180451128\t135300.0
                        k0\tk0\tk2\t2030.0\t138020.0
                        k0\tk1\tk0\t2025.0\t135630.0
                        k0\tk1\tk1\t2035.0\t138355.0
                        k0\tk1\tk2\t2015.0\t137015.0
                        k0\tk2\tk0\t2040.0\t138690.0
                        k0\tk2\tk1\t2020.0\t137350.0
                        k0\tk2\tk2\t2030.0\t135960.0
                        k0\tk3\tk0\t2025.0\t137685.0
                        k0\tk3\tk1\t2020.1127819548872\t136345.0
                        k0\tk3\tk2\t2029.8872180451128\t134970.0
                        k1\tk0\tk0\t2030.8872180451128\t135036.0
                        k1\tk0\tk1\t2026.0\t137752.0
                        k1\tk0\tk2\t2021.1127819548872\t136412.0
                        k1\tk1\tk0\t2031.0\t138087.0
                        k1\tk1\tk1\t2011.0\t136747.0
                        k1\tk1\tk2\t2021.0\t135366.0
                        k1\tk2\tk0\t2016.0\t137082.0
                        k1\tk2\tk1\t2026.0\t135696.0
                        k1\tk2\tk2\t2036.0\t138422.0
                        k1\tk3\tk0\t2016.1127819548872\t136077.0
                        k1\tk3\tk1\t2025.8872180451128\t134706.0
                        k1\tk3\tk2\t2021.0\t137417.0
                        k2\tk0\tk0\t2022.0\t137484.0
                        k2\tk0\tk1\t2017.1127819548872\t136144.0
                        k2\tk0\tk2\t2026.8872180451128\t134772.0
                        k2\tk1\tk0\t2022.1127819548872\t136479.0
                        k2\tk1\tk1\t2031.8872180451128\t135102.0
                        k2\tk1\tk2\t2027.0\t137819.0
                        k2\tk2\tk0\t2022.0\t135432.0
                        k2\tk2\tk1\t2032.0\t138154.0
                        k2\tk2\tk2\t2012.0\t136814.0
                        k2\tk3\tk0\t2037.0\t138489.0
                        k2\tk3\tk1\t2017.0\t137149.0
                        k2\tk3\tk2\t2027.0\t135762.0
                        k3\tk0\tk0\t2028.0\t135828.0
                        k3\tk0\tk1\t2038.0\t138556.0
                        k3\tk0\tk2\t2018.0\t137216.0
                        k3\tk1\tk0\t2027.8872180451128\t134838.0
                        k3\tk1\tk1\t2023.0\t137551.0
                        k3\tk1\tk2\t2018.1127819548872\t136211.0
                        k3\tk2\tk0\t2028.0\t137886.0
                        k3\tk2\tk1\t2023.1127819548872\t136546.0
                        k3\tk2\tk2\t2032.8872180451128\t135168.0
                        k3\tk3\tk0\t2013.0\t136881.0
                        k3\tk3\tk1\t2023.0\t135498.0
                        k3\tk3\tk2\t2033.0\t138221.0
                        k4\tk0\tk0\t2034.0\t138288.0
                        k4\tk0\tk1\t2014.0\t136948.0
                        k4\tk0\tk2\t2024.0\t135564.0
                        k4\tk1\tk0\t2019.0\t137283.0
                        k4\tk1\tk1\t2029.0\t135894.0
                        k4\tk1\tk2\t2039.0\t138623.0
                        k4\tk2\tk0\t2019.1127819548872\t136278.0
                        k4\tk2\tk1\t2028.8872180451128\t134904.0
                        k4\tk2\tk2\t2024.0\t137618.0
                        k4\tk3\tk0\t2033.8872180451128\t135234.0
                        k4\tk3\tk1\t2029.0\t137953.0
                        k4\tk3\tk2\t2024.1127819548872\t136613.0
                        """
        );
    }

    @Test
    public void testParallelMultiKeyGroupBySubQuery() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelMultiSymbolKeyGroupBy(
                "SELECT key1, key2, avg + sum from (" +
                        "  SELECT key1, key2, avg(value), sum(colTop) FROM tab" +
                        ") ORDER BY key1, key2",
                """
                        key1\tkey2\tcolumn
                        k0\tk0\t412030.0
                        k0\tk1\t413025.0
                        k0\tk2\t414030.0
                        k0\tk3\t411025.0
                        k1\tk0\t411226.0
                        k1\tk1\t412221.0
                        k1\tk2\t413226.0
                        k1\tk3\t410221.0
                        k2\tk0\t410422.0
                        k2\tk1\t411427.0
                        k2\tk2\t412422.0
                        k2\tk3\t413427.0
                        k3\tk0\t413628.0
                        k3\tk1\t410623.0
                        k3\tk2\t411628.0
                        k3\tk3\t412623.0
                        k4\tk0\t412824.0
                        k4\tk1\t413829.0
                        k4\tk2\t410824.0
                        k4\tk3\t411829.0
                        """
        );
    }

    @Test
    public void testParallelMultiKeyGroupByWithFilter() throws Exception {
        testParallelMultiSymbolKeyGroupBy(
                "SELECT key1, key2, avg(value), sum(colTop) FROM tab WHERE value < 80 ORDER BY key1, key2",
                """
                        key1\tkey2\tavg\tsum
                        k0\tk0\t45.0\t60.0
                        k0\tk1\t41.0\t65.0
                        k0\tk2\t46.0\t70.0
                        k0\tk3\t51.666666666666664\t130.0
                        k1\tk0\t52.666666666666664\t132.0
                        k1\tk1\t37.0\t61.0
                        k1\tk2\t42.0\t66.0
                        k1\tk3\t47.666666666666664\t122.0
                        k2\tk0\t48.666666666666664\t124.0
                        k2\tk1\t53.666666666666664\t134.0
                        k2\tk2\t38.0\t62.0
                        k2\tk3\t43.0\t67.0
                        k3\tk0\t44.0\t68.0
                        k3\tk1\t49.666666666666664\t126.0
                        k3\tk2\t54.666666666666664\t136.0
                        k3\tk3\t39.0\t63.0
                        k4\tk0\t40.0\t64.0
                        k4\tk1\t45.0\t69.0
                        k4\tk2\t50.666666666666664\t128.0
                        k4\tk3\t55.666666666666664\t138.0
                        """
        );
    }

    @Test
    public void testParallelMultiKeyGroupByWithLimit() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelMultiSymbolKeyGroupBy(
                "SELECT key1, key2, avg(value), sum(colTop) FROM tab ORDER BY key1, key2 LIMIT 3",
                """
                        key1\tkey2\tavg\tsum
                        k0\tk0\t2030.0\t410000.0
                        k0\tk1\t2025.0\t411000.0
                        k0\tk2\t2030.0\t412000.0
                        """,
                "SELECT key1, key2, avg(value), sum(colTop) FROM tab ORDER BY key1, key2 LIMIT -3",
                """
                        key1\tkey2\tavg\tsum
                        k4\tk1\t2029.0\t411800.0
                        k4\tk2\t2024.0\t408800.0
                        k4\tk3\t2029.0\t409800.0
                        """
        );
    }

    @Test
    public void testParallelMultiKeyGroupByWithNestedFilter() throws Exception {
        testParallelMultiSymbolKeyGroupBy(
                "SELECT avg(v), sum(ct), k1, k2 " +
                        "FROM (SELECT value v, colTop ct, key2 k2, key1 k1 FROM tab WHERE value < 80) ORDER BY k1, k2",
                """
                        avg\tsum\tk1\tk2
                        45.0\t60.0\tk0\tk0
                        41.0\t65.0\tk0\tk1
                        46.0\t70.0\tk0\tk2
                        51.666666666666664\t130.0\tk0\tk3
                        52.666666666666664\t132.0\tk1\tk0
                        37.0\t61.0\tk1\tk1
                        42.0\t66.0\tk1\tk2
                        47.666666666666664\t122.0\tk1\tk3
                        48.666666666666664\t124.0\tk2\tk0
                        53.666666666666664\t134.0\tk2\tk1
                        38.0\t62.0\tk2\tk2
                        43.0\t67.0\tk2\tk3
                        44.0\t68.0\tk3\tk0
                        49.666666666666664\t126.0\tk3\tk1
                        54.666666666666664\t136.0\tk3\tk2
                        39.0\t63.0\tk3\tk3
                        40.0\t64.0\tk4\tk0
                        45.0\t69.0\tk4\tk1
                        50.666666666666664\t128.0\tk4\tk2
                        55.666666666666664\t138.0\tk4\tk3
                        """
        );
    }

    @Test
    public void testParallelMultiKeyGroupByWithNoFunctions() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelMultiSymbolKeyGroupBy(
                "SELECT key1, key2 FROM tab GROUP BY key1, key2 ORDER BY key1, key2",
                """
                        key1\tkey2
                        k0\tk0
                        k0\tk1
                        k0\tk2
                        k0\tk3
                        k1\tk0
                        k1\tk1
                        k1\tk2
                        k1\tk3
                        k2\tk0
                        k2\tk1
                        k2\tk2
                        k2\tk3
                        k3\tk0
                        k3\tk1
                        k3\tk2
                        k3\tk3
                        k4\tk0
                        k4\tk1
                        k4\tk2
                        k4\tk3
                        """
        );
    }

    @Test
    public void testParallelMultiKeyGroupByWithNoFunctionsAndFilter() throws Exception {
        testParallelMultiSymbolKeyGroupBy(
                "SELECT key1, key2 FROM tab WHERE key1 != 'k1' and key2 != 'k2' GROUP BY key1, key2 ORDER BY key1, key2",
                """
                        key1\tkey2
                        k0\tk0
                        k0\tk1
                        k0\tk3
                        k2\tk0
                        k2\tk1
                        k2\tk3
                        k3\tk0
                        k3\tk1
                        k3\tk3
                        k4\tk0
                        k4\tk1
                        k4\tk3
                        """
        );
    }

    @Test
    public void testParallelMultiKeyGroupByWithNoFunctionsAndTooStrictFilter() throws Exception {
        testParallelMultiSymbolKeyGroupBy(
                "SELECT key1, key2 FROM tab WHERE value < 0 GROUP BY key1, key2 ORDER BY key1, key2",
                "key1\tkey2\n"
        );
    }

    @Test
    public void testParallelMultiKeyGroupByWithTooStrictFilter() throws Exception {
        testParallelMultiSymbolKeyGroupBy(
                "SELECT key1, key2, avg(value), sum(colTop) FROM tab WHERE value < 0 ORDER BY key1, key2",
                "key1\tkey2\tavg\tsum\n"
        );
    }

    @Test
    public void testParallelNonKeyedGroupBy() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelNonKeyedGroupBy(
                "SELECT vwap(price, quantity), sum(colTop) FROM tab",
                """
                        vwap\tsum
                        2684.615238095238\t8202000.0
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByConcurrent() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);

        final int numOfThreads = 8;
        final int numOfIterations = 50;
        final String query = "SELECT avg(value), sum(colTop) FROM tab";
        final String expected = """
                avg\tsum
                2025.5\t8202000.0
                """;

        final ConcurrentHashMap<Integer, Throwable> errors = new ConcurrentHashMap<>();
        final WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            "CREATE TABLE tab (" +
                                    "  ts TIMESTAMP," +
                                    "  value DOUBLE) timestamp (ts) PARTITION BY DAY",
                            sqlExecutionContext
                    );
                    engine.execute(
                            "insert into tab select (x * 864000000)::timestamp, x from long_sequence(" + ROW_COUNT + ")",
                            sqlExecutionContext
                    );
                    engine.execute("ALTER TABLE tab ADD COLUMN colTop DOUBLE", sqlExecutionContext);
                    engine.execute(
                            "insert into tab " +
                                    "select ((50 + x) * 864000000)::timestamp, 50 + x, 50 + x " +
                                    "from long_sequence(" + ROW_COUNT + ")",
                            sqlExecutionContext
                    );
                    if (convertToParquet) {
                        execute(compiler, "alter table tab convert partition to parquet where ts >= 0", sqlExecutionContext);
                    }

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
                            } catch (Throwable th) {
                                th.printStackTrace();
                                errors.put(threadId, th);
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
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelNonKeyedGroupBy(
                "SELECT count(*) FROM tab GROUP BY 1+2",
                """
                        count
                        8000
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByDecimalFunctions() throws Exception {
        testParallelDecimalKeyGroupBy(
                "SELECT " +
                        "sum(d8) sum_d8,     avg(d8) avg_d8,     avg(d8,1) avg2_d8,     min(d8) min_d8,     max(d8) max_d8,     first(d8) first_d8,     last(d8) last_d8,     first_not_null(d8) firstn_d8,     last_not_null(d8) lastn_d8, " +
                        "sum(d16) sum_d16,   avg(d16) avg_d16,   avg(d16,2) avg2_d16,   min(d16) min_d16,   max(d16) max_d16,   first(d16) first_d16,   last(d16) last_d16,   first_not_null(d16) firstn_d16,   last_not_null(d16) lastn_d16, " +
                        "sum(d32) sum_d32,   avg(d32) avg_d32,   avg(d32,3) avg2_d32,   min(d32) min_d32,   max(d32) max_d32,   first(d32) first_d32,   last(d32) last_d32,   first_not_null(d32) firstn_d32,   last_not_null(d32) lastn_d32, " +
                        "sum(d64) sum_d64,   avg(d64) avg_d64,   avg(d64,5) avg2_d64,   min(d64) min_d64,   max(d64) max_d64,   first(d64) first_d64,   last(d64) last_d64,   first_not_null(d64) firstn_d64,   last_not_null(d64) lastn_d64, " +
                        "sum(d128) sum_d128, avg(d128) avg_d128, avg(d128,1) avg2_d128, min(d128) min_d128, max(d128) max_d128, first(d128) first_d128, last(d128) last_d128, first_not_null(d128) firstn_d128, last_not_null(d128) lastn_d128, " +
                        "sum(d256) sum_d256, avg(d256) avg_d256, avg(d256,1) avg2_d256, min(d256) min_d256, max(d256) max_d256, first(d256) first_d256, last(d256) last_d256, first_not_null(d256) firstn_d256, last_not_null(d256) lastn_d256 " +
                        "FROM tab",
                """
                        sum_d8\tavg_d8\tavg2_d8\tmin_d8\tmax_d8\tfirst_d8\tlast_d8\tfirstn_d8\tlastn_d8\tsum_d16\tavg_d16\tavg2_d16\tmin_d16\tmax_d16\tfirst_d16\tlast_d16\tfirstn_d16\tlastn_d16\tsum_d32\tavg_d32\tavg2_d32\tmin_d32\tmax_d32\tfirst_d32\tlast_d32\tfirstn_d32\tlastn_d32\tsum_d64\tavg_d64\tavg2_d64\tmin_d64\tmax_d64\tfirst_d64\tlast_d64\tfirstn_d64\tlastn_d64\tsum_d128\tavg_d128\tavg2_d128\tmin_d128\tmax_d128\tfirst_d128\tlast_d128\tfirstn_d128\tlastn_d128\tsum_d256\tavg_d256\tavg2_d256\tmin_d256\tmax_d256\tfirst_d256\tlast_d256\tfirstn_d256\tlastn_d256
                        2000\t0\t0.5\t0\t1\t1\t0\t1\t0\t18000.0\t4.5\t4.50\t0.0\t9.0\t1.0\t0.0\t1.0\t0.0\t38000.0\t9.5\t9.500\t0.0\t19.0\t1.0\t0.0\t1.0\t0.0\t57910.00\t14.48\t14.47750\t0.00\t29.00\t1.00\t10.00\t1.00\t10.00\t78000.000\t19.500\t19.5\t0.000\t39.000\t1.000\t0.000\t1.000\t0.000\t198000.000000\t49.500000\t49.5\t0.000000\t99.000000\t1.000000\t0.000000\t1.000000\t0.000000
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByFaultTolerance() throws Exception {
        testParallelGroupByFaultTolerance("select vwap(price, quantity) from tab where npe();");
    }

    @Test
    public void testParallelNonKeyedGroupBySubQueryWithReadThreadSafeTimestampFilter() throws Exception {
        // Parallel GROUP BY shouldn't kick in on this query, yet we want
        // to validate the result correctness.
        testParallelGroupByAllTypes(
                "SELECT count_distinct(anint) FROM " +
                        "(SELECT * FROM tab WHERE ts in '1970-01-13' and anint > 0 LIMIT 10)",
                """
                        count_distinct
                        10
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupBySubQueryWithReadThreadUnsafeTimestampFilter() throws Exception {
        // Parallel GROUP BY shouldn't kick in on this query, yet we want
        // to validate the result correctness.
        testParallelGroupByAllTypes(
                "SELECT count_distinct(anint) FROM " +
                        "(SELECT * FROM tab WHERE ts in '1970-01-13' and anint > 0 and asymbol in (select asymbol from tab where length(asymbol) = 4) LIMIT 10)",
                """
                        count_distinct
                        10
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByThrowsOnTimeout() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(AbstractCairoTest.LOG);
        // We want the timeout to happen in reduce.
        // Page frame count is 40.
        final long tripWhenTicks = Math.max(10, rnd.nextLong(39));
        testParallelGroupByThrowsOnTimeout("select vwap(price, quantity) from tab", tripWhenTicks);
    }

    @Test
    public void testParallelNonKeyedGroupByWithAllBindVariableTypesInFilter() throws Exception {
        testParallelGroupByAllTypes(
                (sqlExecutionContext) -> {
                    BindVariableService bindVariableService = sqlExecutionContext.getBindVariableService();
                    bindVariableService.clear();
                    bindVariableService.setBoolean("aboolean", false);
                    bindVariableService.setByte("abyte", (byte) 28);
                    bindVariableService.setGeoHash("ageobyte", 0, ColumnType.getGeoHashTypeWithBits(4));
                    bindVariableService.setShort("ashort", (short) 243);
                    bindVariableService.setGeoHash("ageoshort", 0b011011000010L, ColumnType.getGeoHashTypeWithBits(12));
                    bindVariableService.setChar("achar", 'O');
                    bindVariableService.setInt("anint", 2085282008);
                    bindVariableService.setGeoHash("ageoint", 0b0101011010111101L, ColumnType.getGeoHashTypeWithBits(16));
                    bindVariableService.setStr("asymbol", "HYRX");
                    bindVariableService.setFloat("afloat", 0.48820507526397705f);
                    bindVariableService.setLong("along", -4986232506486815364L);
                    bindVariableService.setDouble("adouble", 0.42281342727402726);
                    bindVariableService.setDate("adate", 1443479385706L);
                    bindVariableService.setGeoHash("ageolong", 0b11010000001110101000110100011010L, ColumnType.getGeoHashTypeWithBits(32));
                    bindVariableService.setTimestamp("atimestamp", 400500000000L);
                },
                "SELECT max(along), min(along) FROM tab " +
                        "WHERE aboolean != :aboolean " +
                        " and abyte != :abyte " +
                        " and ageobyte != :ageobyte " +
                        " and ashort != :ashort " +
                        " and ageoshort != :ageoshort " +
                        " and achar != :achar " +
                        " and anint != :anint " +
                        " and ageoint != :ageoint " +
                        " and asymbol != :asymbol " +
                        " and afloat != :afloat " +
                        " and along != :along " +
                        " and adouble != :adouble " +
                        " and adate != :adate " +
                        " and ageolong != :ageolong " +
                        " and ts != :atimestamp",
                """
                        max\tmin
                        9222440717001210457\t-9216152523287705363
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithApproxCountDistinctIPv4Function() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT count_distinct(anint::ipv4), approx_count_distinct(anint::ipv4) FROM tab",
                """
                        count_distinct\tapprox_count_distinct
                        4000\t4000
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithApproxCountDistinctIntFunction() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT count_distinct(anint), approx_count_distinct(anint) FROM tab",
                """
                        count_distinct\tapprox_count_distinct
                        4000\t4000
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithApproxCountDistinctLongFunction() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT count_distinct(along), approx_count_distinct(along) FROM tab",
                """
                        count_distinct\tapprox_count_distinct
                        4000\t4000
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithBasicDoubleFunctions() throws Exception {
        Assume.assumeFalse(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT min(adouble), max(adouble), round(avg(adouble)*count(adouble)), round(sum(adouble)), first(adouble), last(adouble) FROM tab",
                """
                        min\tmax\tround\tround1\tfirst\tlast
                        2.0456303844185175E-4\t0.9999182937007105\t1679.0\t1679.0\t0.8799634725391621\t0.15322992873721464
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithBasicFloatFunctions() throws Exception {
        Assume.assumeFalse(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT min(afloat), max(afloat), round(avg(afloat)*count(afloat)), round(sum(afloat)), first(afloat), last(afloat) FROM tab",
                """
                        min\tmax\tround\tround1\tfirst\tlast
                        1.6343594E-4\t0.9997715\t1665.0\t1665.0\t0.87567717\t0.030083895
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithBasicIntFunctions() throws Exception {
        Assume.assumeFalse(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT min(anint), max(anint), round(avg(anint)*count(anint)), sum(anint), first(anint), last(anint) FROM tab",
                """
                        min\tmax\tround\tsum\tfirst\tlast
                        -2147365666\t2146394077\t-4.9631313424E10\t-49631313424\t-85170055\t1033747429
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithBasicLongFunctions() throws Exception {
        Assume.assumeFalse(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT min(along), max(along), round(avg(along)*count(along)), sum(along), first(along), last(along) FROM tab",
                """
                        min\tmax\tround\tsum\tfirst\tlast
                        -9220264229979566148\t9222440717001210457\t-9.223372036854776E18\t-8085484953408325183\t8416773233910814357\t6812734169481155056
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithBasicShortFunctions() throws Exception {
        Assume.assumeFalse(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT min(ashort), max(ashort), round(avg(ashort)), sum(ashort), first(ashort), last(ashort) FROM tab",
                """
                        min\tmax\tround\tsum\tfirst\tlast
                        10\t1024\t513.0\t2050140\t788\t859
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithBindVariablesInFilter() throws Exception {
        testParallelGroupByAllTypes(
                (sqlExecutionContext) -> {
                    sqlExecutionContext.getBindVariableService().clear();
                    sqlExecutionContext.getBindVariableService().setStr(0, "CPSW");
                },
                "SELECT min(anint), max(anint) FROM tab WHERE asymbol = $1",
                """
                        min\tmax
                        -2138876769\t2140835033
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithCaseExpression1() throws Exception {
        testParallelStringAndVarcharKeyGroupBy(
                "SELECT avg(length(CASE WHEN (key = 'k0') THEN 'foobar' ELSE 'foo' END)), avg(value) FROM tab",
                """
                        avg\tavg1
                        3.6\t1025.5
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithCaseExpression2() throws Exception {
        // Parallel GROUP BY shouldn't kick in on this query due to ::symbol cast,
        // yet we want to validate the result correctness.
        testParallelStringAndVarcharKeyGroupBy(
                "SELECT sum(length(CASE WHEN (key::symbol = 'k0') THEN 'foobar' ELSE 'foo' END)) FROM tab",
                """
                        sum
                        14400
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithCountDistinctIntFunction() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT count_distinct(along) FROM tab",
                """
                        count_distinct
                        4000
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithCountDistinctLongFunction() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT count_distinct(adate) FROM tab",
                """
                        count_distinct
                        3360
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithCountDistinctSymbolFunction1() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT count_distinct(asymbol) FROM tab",
                """
                        count_distinct
                        4
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithCountDistinctSymbolFunction2() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT count_distinct(asymbol), first(asymbol) FROM tab",
                """
                        count_distinct\tfirst
                        4\tCPSW
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithCountDistinctTimestampFunction() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT count_distinct(ts) FROM tab",
                """
                        count_distinct
                        4000
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithFilter() throws Exception {
        testParallelNonKeyedGroupBy(
                "SELECT vwap(price, quantity), sum(colTop) FROM tab WHERE quantity < 100",
                """
                        vwap\tsum
                        1981.006198090988\t3675.0
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithMinMaxIntExpressionFunction() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT max(length(asymbol)), max(length(astring)) FROM tab",
                """
                        max\tmax1
                        4\t16
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithMinMaxStrFunction() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelStringAndVarcharKeyGroupBy(
                "SELECT min(key), max(key) FROM tab",
                """
                        min\tmax
                        k0\tk4
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithMinMaxSymbolFunction() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelSymbolKeyGroupBy(
                "SELECT min(key), max(key) FROM tab",
                """
                        min\tmax
                        k0\tk4
                        """,
                null
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithMultipleCountDistinctFunctions() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT count_distinct(ashort), count_distinct(anint), count_distinct(along) FROM tab",
                """
                        count_distinct\tcount_distinct1\tcount_distinct2
                        993\t4000\t4000
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithNestedCaseFunction() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelStringAndVarcharKeyGroupBy(
                "SELECT sum(CASE WHEN (key = 'k0') THEN 1 ELSE 0 END) FROM tab",
                """
                        sum
                        800
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithNestedFilter() throws Exception {
        testParallelNonKeyedGroupBy(
                "SELECT vwap(p, q), sum(ct) " +
                        "FROM (SELECT colTop ct, quantity q, price p FROM tab WHERE quantity < 80)",
                """
                        vwap\tsum
                        1974.5391511088592\t1885.0
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithReadThreadSafeFilter() throws Exception {
        testParallelNonKeyedGroupBy(
                "SELECT vwap(price, quantity), sum(colTop) FROM tab WHERE quantity < 80",
                """
                        vwap\tsum
                        1974.5391511088592\t1885.0
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithReadThreadSafeTimestampFilter1() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT count_distinct(adate) FROM tab WHERE ts in '1970-01-13' and anint > 0",
                """
                        count_distinct
                        71
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithReadThreadSafeTimestampFilter2() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT min(key), max(key) FROM tab " +
                        "WHERE ts in '1970-01-13' and anint > 0",
                """
                        min\tmax
                        k0\tk4
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithReadThreadUnsafeFilter() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT vwap(price, quantity), sum(colTop) FROM tab WHERE key = 'k1'",
                """
                        vwap\tsum
                        2682.7321472695826\t1638800.0
                        """,
                null
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithReadThreadUnsafeTimestampFilter1() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT count_distinct(anint) FROM tab " +
                        "WHERE ts in '1970-01-13' and anint > 0 and asymbol in (select asymbol from tab where length(asymbol) = 4)",
                """
                        count_distinct
                        52
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithReadThreadUnsafeTimestampFilter2() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT min(asymbol), max(asymbol) FROM tab " +
                        "WHERE ts in '1970-01-13' and anint > 0 and asymbol in (select asymbol from tab where length(asymbol) >= 4)",
                """
                        min\tmax
                        CPSW\tVTJW
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithTooStrictFilter() throws Exception {
        testParallelNonKeyedGroupBy(
                "SELECT vwap(price, quantity), sum(colTop) FROM tab WHERE quantity < 0",
                """
                        vwap\tsum
                        null\tnull
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithTwoApproxCountDistinctIPv4Functions() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT " +
                        "count_distinct(anint::ipv4), " +
                        "approx_count_distinct(anint::ipv4), " +
                        "count_distinct((abs(anint) % 10)::ipv4), " +
                        "approx_count_distinct((abs(anint) % 10)::ipv4) " +
                        "FROM tab",
                """
                        count_distinct\tapprox_count_distinct\tcount_distinct1\tapprox_count_distinct1
                        4000\t4000\t9\t9
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithTwoApproxCountDistinctIntFunctions() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT " +
                        "count_distinct(anint), " +
                        "approx_count_distinct(anint), " +
                        "count_distinct(abs(anint) % 10), " +
                        "approx_count_distinct(abs(anint) % 10) " +
                        "FROM tab",
                """
                        count_distinct\tapprox_count_distinct\tcount_distinct1\tapprox_count_distinct1
                        4000\t4000\t10\t10
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithTwoApproxCountDistinctLongFunctions() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT " +
                        "count_distinct(along), " +
                        "approx_count_distinct(along), " +
                        "count_distinct(abs(along) % 10), " +
                        "approx_count_distinct(abs(along) % 10) " +
                        "FROM tab",
                """
                        count_distinct\tapprox_count_distinct\tcount_distinct1\tapprox_count_distinct1
                        4000\t4000\t10\t10
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithTwoCountDistinctLongFunctions() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT count_distinct(along), count_distinct(along % 2) FROM tab",
                """
                        count_distinct\tcount_distinct1
                        4000\t3
                        """
        );
    }

    @Test
    public void testParallelNonKeyedGroupByWithUnionAll() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT min(achar), max(achar) FROM tab WHERE astring = 'ZYJDGSUYYESCKPGP' " +
                        "UNION ALL " +
                        "SELECT min(achar), max(achar) FROM tab WHERE astring = 'BBOYHGPKTIIFPB';",
                """
                        min\tmax
                        S\tS
                        Y\tY
                        """
        );
    }

    @Test
    public void testParallelOperationKeyGroupBy() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelSymbolKeyGroupBy(
                "SELECT ((key is not null) and (colTop is not null)) key, sum(colTop) FROM tab ORDER BY key",
                """
                        key\tsum
                        false\tnull
                        true\t8202000.0
                        """,
                null
        );
    }

    @Test
    public void testParallelRegressionSlope() throws Exception {
        Assume.assumeTrue(enableJitCompiler);
        Assume.assumeFalse(convertToParquet);

        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                        execute(
                                compiler,
                                "create table tbl1 as (select rnd_double() x, rnd_double() y, rnd_symbol('a', 'b', 'c') sym from long_sequence(100000))",
                                sqlExecutionContext
                        );
                        TestUtils.assertSql(
                                engine,
                                sqlExecutionContext,
                                "select round(regr_slope(x, y), 5), sym from tbl1 WHERE x > 0.5 ORDER BY sym",
                                sink,
                                """
                                        round\tsym
                                        -0.00317\ta
                                        -0.00402\tb
                                        0.00476\tc
                                        """
                        );
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testParallelRostiAvg() throws Exception {
        testParallelRostiGroupBy(
                "SELECT key, avg(s) avg_s, avg(i) avg_i, avg(l) avg_l, round(avg(d)) avg_d " +
                        "FROM tab " +
                        "ORDER BY key",
                """
                        key\tavg_s\tavg_i\tavg_l\tavg_d
                        \t11.0\t12.0\t13.0\t15.0
                        k0\t-859.98\t128.27631578947367\t469.4625\t1.0
                        k1\t-2783.67\t113.6375\t548.2073170731708\t0.0
                        k2\t1722.65\t133.21686746987953\t557.6623376623377\t0.0
                        k3\t-376.12\t134.25301204819277\t512.6117647058824\t0.0
                        k4\t844.42\t124.53333333333333\t478.9146341463415\t1.0
                        """
        );
    }

    @Test
    public void testParallelRostiCount() throws Exception {
        testParallelRostiGroupBy(
                "SELECT key, count(i) count_i, count(l) count_l, count(d) count_d " +
                        "FROM tab " +
                        "ORDER BY key",
                """
                        key\tcount_i\tcount_l\tcount_d
                        \t3\t3\t3
                        k0\t76\t80\t88
                        k1\t80\t82\t86
                        k2\t83\t77\t87
                        k3\t83\t85\t75
                        k4\t75\t82\t84
                        """
        );
    }

    @Test
    public void testParallelRostiKSumNSum() throws Exception {
        testParallelRostiGroupBy(
                "SELECT key, round(ksum(d)) ksum_d, round(nsum(d)) nsum_d " +
                        "FROM tab " +
                        "ORDER BY key",
                """
                        key\tksum_d\tnsum_d
                        \t45.0\t45.0
                        k0\t48.0\t48.0
                        k1\t43.0\t43.0
                        k2\t39.0\t39.0
                        k3\t33.0\t33.0
                        k4\t46.0\t46.0
                        """
        );
    }

    @Test
    public void testParallelRostiMinMax() throws Exception {
        testParallelRostiGroupBy(
                "SELECT key, min(s) min_s, max(s) max_s, min(i) min_i, max(i) max_i, min(l) min_l, max(l) max_l, " +
                        "  min(d) min_d, max(d) max_d, min(dd) min_dd, max(dd) max_dd, min(t) min_t, max(t) max_t " +
                        "FROM tab " +
                        "ORDER BY key",
                """
                        key\tmin_s\tmax_s\tmin_i\tmax_i\tmin_l\tmax_l\tmin_d\tmax_d\tmin_dd\tmax_dd\tmin_t\tmax_t
                        \t1\t21\t2\t22\t3\t23\t5.0\t25.0\t1992-01-01T00:00:00.000Z\t2102-01-01T00:00:00.000Z\t1991-01-01T00:00:00.000000Z\t2101-01-01T00:00:00.000000Z
                        k0\t-32314\t32650\t5\t255\t1\t982\t0.023600615130049185\t0.9924997596095891\t1980-01-13T19:56:55.619Z\t1989-12-11T15:05:57.581Z\t1980-05-16T15:35:09.991442Z\t1989-12-01T00:45:38.931160Z
                        k1\t-31947\t32139\t0\t251\t12\t1022\t0.030997441190531494\t0.9869813021229126\t1980-01-27T20:04:53.149Z\t1989-11-09T23:54:33.595Z\t1980-01-20T15:13:30.780056Z\t1989-12-25T11:06:35.080985Z
                        k2\t-32474\t32378\t3\t256\t18\t1020\t0.0031075670450616544\t0.9887681426881507\t1980-02-06T21:12:31.508Z\t1989-10-14T12:01:28.825Z\t1980-01-09T11:42:16.059075Z\t1989-12-02T07:02:03.165501Z
                        k3\t-32129\t32752\t0\t254\t4\t1024\t0.017595931321539804\t0.9958686315610356\t1980-02-04T19:47:05.743Z\t1989-12-28T03:38:43.787Z\t1980-01-09T21:18:50.462647Z\t1989-12-13T15:24:44.892613Z
                        k4\t-32677\t32259\t5\t255\t4\t1017\t0.06790969300705241\t0.9923530546137099\t1980-01-25T07:20:26.116Z\t1989-11-11T09:29:53.477Z\t1980-01-25T11:45:16.361674Z\t1989-11-07T17:55:20.285921Z
                        """
        );
    }

    @Test
    public void testParallelRostiSum() throws Exception {
        testParallelRostiGroupBy(
                "SELECT key, sum(s) sum_s, sum(i) sum_i, sum(l) sum_l, sum(l256) sum_l256, round(sum(d)) sum_d " +
                        "FROM tab " +
                        "ORDER BY key",
                """
                        key\tsum_s\tsum_i\tsum_l\tsum_l256\tsum_d
                        \t33\t36\t39\t0x2a\t45.0
                        k0\t-85998\t9749\t37557\t0x248af96495cafa7d5c4dbe79c86d46054af590066639cabfb780bce1c77ea11c\t48.0
                        k1\t-278367\t9091\t44953\t0x50b8e23533380471b205e4a7adeb9498426e85e7cf92558e9ca39604592ccea6\t43.0
                        k2\t172265\t11057\t42940\t0xa914b3d66e12185a5d76310378e831be316071aaa2436b2c66e948497c8929ba\t39.0
                        k3\t-37612\t11143\t43572\t0x92fdbf6e1f5b9360329a1dec86290a74b5a3f6b9ed9725c4f457dbb833b212f5\t33.0
                        k4\t84442\t9340\t39271\t0x01708577a8ec2c4308e67d5f43e4cee420525d6d74f480ca312efa8e9fe584ce\t46.0
                        """
        );
    }

    @Test
    public void testParallelShortKeyGroupBy() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT ashort, min(along), max(along), min(anint), max(anint) FROM tab ORDER BY ashort LIMIT 10",
                """
                        ashort\tmin\tmax\tmin1\tmax1
                        10\t-7014037229734002476\t7183543099461850887\t-1923591798\t1614272726
                        11\t-8094563283586603797\t8266945525792915855\t-2019994796\t210531539
                        12\t5618009227733669273\t8843912384659881242\t-652636844\t1190519150
                        13\t-7540841016814556599\t6605106079379923850\t-1732278194\t732327460
                        14\t-5460379094988165507\t8508185526576303912\t-1006265366\t1983534078
                        15\t-8981189571240767552\t8925512637731362403\t-1578813385\t2074645817
                        16\t-4066776978860718462\t6817629763366381128\t-1183238579\t706490660
                        17\t-7818508517164276162\t8840276378973040058\t-1908962198\t1965693358
                        19\t-9081142346003583492\t5148856316963763479\t-1242130097\t810630533
                        20\t-8639548466303198922\t4998555152747068047\t-1594623294\t2041653124
                        """
        );
    }

    @Test
    public void testParallelShortKeyGroupBy2() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT ashort, max(along) - min(along) delta FROM tab ORDER BY ashort LIMIT 10",
                """
                        ashort\tdelta
                        10\t-4249163744513698253
                        11\t-2085235264330031964
                        12\t3225903156926211969
                        13\t-4300796977515071167
                        14\t-4478179452145082197
                        15\t-540041864737421661
                        16\t-7562337331482452026
                        17\t-1787959177572235396
                        19\t-4216745410742204645
                        20\t-4808640454659284647
                        """
        );
    }

    @Test
    public void testParallelShortKeyGroupByWithReadThreadSafeTimestampFilter() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT ashort, count_distinct(along) FROM tab " +
                        "WHERE ts in '1970-01-13' and ashort < 100 ORDER BY ashort DESC",
                """
                        ashort\tcount_distinct
                        96\t1
                        94\t1
                        89\t1
                        88\t1
                        82\t1
                        72\t2
                        70\t1
                        65\t1
                        55\t1
                        52\t1
                        49\t1
                        48\t1
                        45\t1
                        44\t1
                        43\t1
                        42\t1
                        40\t2
                        33\t1
                        19\t1
                        """
        );
    }

    @Test
    public void testParallelShortKeyGroupByWithReadThreadUnsafeTimestampFilter() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT ashort, count_distinct(anint) FROM tab " +
                        "WHERE ts in '1970-01-13' and ashort < 100 and key in ('k1', 'k2') ORDER BY ashort DESC",
                """
                        ashort\tcount_distinct
                        89\t1
                        88\t1
                        70\t1
                        49\t1
                        43\t1
                        40\t1
                        33\t1
                        """
        );
    }

    @Test
    public void testParallelShortKeyGroupByWithTooStrictFilter() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT ashort, min(along), max(along) FROM tab WHERE ashort < -990",
                "ashort\tmin\tmax\n"
        );
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
    public void testParallelSingleKeyGroupByThrowsOnTimeout() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(AbstractCairoTest.LOG);
        // We want the timeout to happen in either reduce or merge.
        // Page frame count is 40 and shard count is 8.
        final long tripWhenTicks = Math.max(10, rnd.nextLong(48));
        testParallelGroupByThrowsOnTimeout("select quantity % 100, vwap(price, quantity) from tab", tripWhenTicks);
    }

    @Test
    public void testParallelSingleKeyGroupByWithApproxCountDistinctIPv4Function() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT key, count_distinct(anint::ipv4), approx_count_distinct(anint::ipv4) FROM tab ORDER BY key",
                """
                        key\tcount_distinct\tapprox_count_distinct
                        k0\t800\t800
                        k1\t800\t800
                        k2\t800\t800
                        k3\t800\t800
                        k4\t800\t800
                        """
        );
    }

    @Test
    public void testParallelSingleKeyGroupByWithApproxCountDistinctIntFunction() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT key, count_distinct(anint), approx_count_distinct(anint) FROM tab ORDER BY key",
                """
                        key\tcount_distinct\tapprox_count_distinct
                        k0\t800\t800
                        k1\t800\t800
                        k2\t800\t800
                        k3\t800\t800
                        k4\t800\t800
                        """
        );
    }

    @Test
    public void testParallelSingleKeyGroupByWithApproxCountDistinctLongFunction() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT key, count_distinct(along), approx_count_distinct(along) FROM tab ORDER BY key",
                """
                        key\tcount_distinct\tapprox_count_distinct
                        k0\t800\t800
                        k1\t800\t800
                        k2\t800\t800
                        k3\t800\t800
                        k4\t800\t800
                        """
        );
    }

    @Test
    public void testParallelSingleKeyGroupByWithApproxCountDistinctSymbolFunction() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT key, count_distinct(asymbol) FROM tab ORDER BY key",
                """
                        key\tcount_distinct
                        k0\t4
                        k1\t4
                        k2\t4
                        k3\t4
                        k4\t4
                        """
        );
    }

    @Test
    public void testParallelSingleKeyGroupByWithTwoApproxCountDistinctIPv4Functions() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT " +
                        "key, " +
                        "count_distinct(anint::ipv4), " +
                        "approx_count_distinct(anint::ipv4), " +
                        "count_distinct((abs(anint) % 10)::ipv4), " +
                        "approx_count_distinct((abs(anint) % 10)::ipv4) " +
                        "FROM tab ORDER BY key",
                """
                        key\tcount_distinct\tapprox_count_distinct\tcount_distinct1\tapprox_count_distinct1
                        k0\t800\t800\t9\t9
                        k1\t800\t800\t9\t9
                        k2\t800\t800\t9\t9
                        k3\t800\t800\t9\t9
                        k4\t800\t800\t9\t9
                        """
        );
    }

    @Test
    public void testParallelSingleKeyGroupByWithTwoApproxCountDistinctIntFunctions() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT " +
                        "key, " +
                        "count_distinct(anint), " +
                        "approx_count_distinct(anint), " +
                        "count_distinct(abs(anint) % 10), " +
                        "approx_count_distinct(abs(anint) % 10) " +
                        "FROM tab ORDER BY key",
                """
                        key\tcount_distinct\tapprox_count_distinct\tcount_distinct1\tapprox_count_distinct1
                        k0\t800\t800\t10\t10
                        k1\t800\t800\t10\t10
                        k2\t800\t800\t10\t10
                        k3\t800\t800\t10\t10
                        k4\t800\t800\t10\t10
                        """
        );
    }

    @Test
    public void testParallelSingleKeyGroupByWithTwoApproxCountDistinctLongFunctions() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT " +
                        "key, " +
                        "count_distinct(along), " +
                        "approx_count_distinct(along), " +
                        "count_distinct(abs(along) % 10), " +
                        "approx_count_distinct(abs(along) % 10) " +
                        "FROM tab ORDER BY key",
                """
                        key\tcount_distinct\tapprox_count_distinct\tcount_distinct1\tapprox_count_distinct1
                        k0\t800\t800\t10\t10
                        k1\t800\t800\t10\t10
                        k2\t800\t800\t10\t10
                        k3\t800\t800\t10\t10
                        k4\t800\t800\t10\t10
                        """
        );
    }

    @Test
    public void testParallelStringAndVarcharKeyGroupBy() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelStringAndVarcharKeyGroupBy(
                "SELECT key, avg(value), sum(colTop), count() FROM tab ORDER BY key",
                """
                        key\tavg\tsum\tcount
                        k0\t1027.5\t421000.0\t800
                        k1\t1023.5\t419400.0\t800
                        k2\t1024.5\t419800.0\t800
                        k3\t1025.5\t420200.0\t800
                        k4\t1026.5\t420600.0\t800
                        """
        );
    }

    @Test
    public void testParallelStringAndVarcharKeyGroupByWithLimit() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelStringAndVarcharKeyGroupBy(
                "SELECT key, avg(value), sum(colTop), first(ts)::long c FROM tab ORDER BY c DESC LIMIT 2",
                """
                        key\tavg\tsum\tc
                        k0\t1027.5\t421000.0\t4320000000
                        k4\t1026.5\t420600.0\t3456000000
                        """
        );
    }

    @Test
    public void testParallelStringKeyGroupByConcurrent() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);

        final int numOfThreads = 8;
        final int numOfIterations = 50;
        final String query = "SELECT key, avg + sum from (" +
                "  SELECT key, avg(value), sum(colTop) FROM tab" +
                ") ORDER BY key";
        final String expected = """
                key\tcolumn
                k0\t1644027.5
                k1\t1640823.5
                k2\t1641624.5
                k3\t1642425.5
                k4\t1643226.5
                """;

        final ConcurrentHashMap<Integer, Throwable> errors = new ConcurrentHashMap<>();
        final WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            "CREATE TABLE tab (" +
                                    "  ts TIMESTAMP," +
                                    "  key STRING," +
                                    "  value DOUBLE) timestamp (ts) PARTITION BY DAY",
                            sqlExecutionContext
                    );
                    engine.execute(
                            "insert into tab select (x * 864000000)::timestamp, 'k' || (x % 5), x from long_sequence(" + ROW_COUNT + ")",
                            sqlExecutionContext
                    );
                    engine.execute("ALTER TABLE tab ADD COLUMN colTop DOUBLE", sqlExecutionContext);
                    engine.execute(
                            "insert into tab " +
                                    "select ((50 + x) * 864000000)::timestamp, 'k' || ((50 + x) % 5), 50 + x, 50 + x " +
                                    "from long_sequence(" + ROW_COUNT + ")",
                            sqlExecutionContext
                    );
                    if (convertToParquet) {
                        execute(compiler, "alter table tab convert partition to parquet where ts >= 0", sqlExecutionContext);
                    }

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
                            } catch (Throwable th) {
                                th.printStackTrace();
                                errors.put(threadId, th);
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
    public void testParallelStringKeyGroupByConcurrentNpeInReduce() throws Exception {
        // This query validates parallel processing and doesn't use JIT.
        Assume.assumeTrue(enableParallelGroupBy);
        Assume.assumeFalse(enableJitCompiler);
        // We'll need npe() function.
        node1.setProperty(PropertyKey.DEV_MODE_ENABLED, true);

        final int numOfThreads = 8;
        final int numOfIterations = 50;
        final ConcurrentHashMap<Integer, Throwable> errors = new ConcurrentHashMap<>();
        final WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            "CREATE TABLE tab (" +
                                    "  ts TIMESTAMP," +
                                    "  key STRING," +
                                    "  value DOUBLE) timestamp (ts) PARTITION BY DAY",
                            sqlExecutionContext
                    );
                    engine.execute(
                            "insert into tab select (x * 864000000)::timestamp, 'k' || (x % 5), x from long_sequence(" + ROW_COUNT + ")",
                            sqlExecutionContext
                    );
                    if (convertToParquet) {
                        execute(compiler, "alter table tab convert partition to parquet where ts >= 0", sqlExecutionContext);
                    }

                    final CyclicBarrier barrier = new CyclicBarrier(numOfThreads);
                    final SOCountDownLatch haltLatch = new SOCountDownLatch(numOfThreads);

                    for (int i = 0; i < numOfThreads; i++) {
                        final int threadId = i;
                        new Thread(() -> {
                            TestUtils.await(barrier);
                            // We expect an NPE (work stealing) or a CairoException (NPE caught by a worker)
                            try {
                                for (int j = 0; j < numOfIterations; j++) {
                                    assertCairoException(engine, sqlExecutionContext);
                                }
                            } catch (NullPointerException npe) {
                                // NPE is expected
                            } catch (Throwable th) {
                                th.printStackTrace();
                                errors.put(threadId, th);
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
    public void testParallelStringKeyGroupBySubQuery() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelStringAndVarcharKeyGroupBy(
                "SELECT key, avg + sum from (" +
                        "SELECT key, avg(value), sum(colTop) FROM tab" +
                        ") ORDER BY key",
                """
                        key\tcolumn
                        k0\t422027.5
                        k1\t420423.5
                        k2\t420824.5
                        k3\t421225.5
                        k4\t421626.5
                        """
        );
    }

    @Test
    public void testParallelStringKeyGroupByWithAllBindVariableTypesInFilter() throws Exception {
        testParallelGroupByAllTypes(
                (sqlExecutionContext) -> {
                    BindVariableService bindVariableService = sqlExecutionContext.getBindVariableService();
                    bindVariableService.clear();
                    bindVariableService.setBoolean("aboolean", true);
                    bindVariableService.setByte("abyte", (byte) 25);
                    bindVariableService.setGeoHash("ageobyte", 0b1100, ColumnType.getGeoHashTypeWithBits(4));
                    bindVariableService.setShort("ashort", (short) 1013);
                    bindVariableService.setGeoHash("ageoshort", 0b001111001001, ColumnType.getGeoHashTypeWithBits(12));
                    bindVariableService.setChar("achar", 'C');
                    bindVariableService.setInt("anint", -1269042121);
                    bindVariableService.setGeoHash("ageoint", 0b0101011010000100, ColumnType.getGeoHashTypeWithBits(16));
                    bindVariableService.setStr("asymbol", "PEHN");
                    bindVariableService.setLong("along", -3214230645884399728L);
                    bindVariableService.setDate("adate", 1447246617854L);
                    bindVariableService.setGeoHash("ageolong", 0b11111100110100011011101011101011L, ColumnType.getGeoHashTypeWithBits(32));
                    bindVariableService.setStr("astring", "LYXWCKYLSU");
                    bindVariableService.setTimestamp("atimestamp", 401000000000L);
                    bindVariableService.setStr("auuid", "78c594c4-9699-4885-aa18-96d0ad3419d2");
                },
                "SELECT key, count(anint), count(along) FROM tab " +
                        "WHERE aboolean = :aboolean " +
                        " and abyte = :abyte " +
                        " and ageobyte = :ageobyte " +
                        " and ashort = :ashort " +
                        " and ageoshort = :ageoshort " +
                        " and achar = :achar " +
                        " and anint = :anint " +
                        " and ageoint = :ageoint " +
                        " and asymbol = :asymbol " +
                        " and along = :along " +
                        " and adate = :adate " +
                        " and ageolong = :ageolong " +
                        " and astring = :astring " +
                        " and auuid = :auuid " +
                        " and ts = :atimestamp " +
                        "ORDER BY key",
                """
                        key\tcount\tcount1
                        k3\t1\t1
                        """
        );
    }

    @Test
    public void testParallelStringKeyGroupByWithBindVariablesInFilter() throws Exception {
        testParallelGroupByAllTypes(
                (sqlExecutionContext) -> {
                    sqlExecutionContext.getBindVariableService().clear();
                    sqlExecutionContext.getBindVariableService().setInt(0, 10);
                    sqlExecutionContext.getBindVariableService().setLong(1, 100);
                },
                "SELECT key, min(along), max(along) FROM tab WHERE anint > $1 and along < $2 ORDER BY key",
                """
                        key\tmin\tmax
                        k0\t-9219668933902983867\t-608075365086093881
                        k1\t-9183391647798320265\t-537033525381181954
                        k2\t-9185973508459496019\t-742318963566750163
                        k3\t-9188002349803268631\t-218100546579477944
                        k4\t-9211367153776244683\t-618781741402001353
                        """
        );
    }

    @Test
    public void testParallelStringKeyGroupByWithCountDistinctIntFunction() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT key, count_distinct(anint), count_distinct(anint + 42) FROM tab ORDER BY key",
                """
                        key\tcount_distinct\tcount_distinct1
                        k0\t800\t800
                        k1\t800\t800
                        k2\t800\t800
                        k3\t800\t800
                        k4\t800\t800
                        """
        );
    }

    @Test
    public void testParallelStringKeyGroupByWithCountDistinctLongFunction() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT key, count_distinct(along) FROM tab ORDER BY key",
                """
                        key\tcount_distinct
                        k0\t800
                        k1\t800
                        k2\t800
                        k3\t800
                        k4\t800
                        """
        );
    }

    @Test
    public void testParallelStringKeyGroupByWithFilter() throws Exception {
        testParallelStringAndVarcharKeyGroupBy(
                "SELECT key, avg(value), sum(colTop), count() FROM tab WHERE value < 80 ORDER BY key",
                """
                        key\tavg\tsum\tcount
                        k0\t46.25\t325.0\t20
                        k1\t45.31818181818182\t381.0\t22
                        k2\t46.31818181818182\t387.0\t22
                        k3\t47.31818181818182\t393.0\t22
                        k4\t48.31818181818182\t399.0\t22
                        """
        );
    }

    @Test
    public void testParallelStringKeyGroupByWithFilter2() throws Exception {
        testParallelStringAndVarcharKeyGroupBy(
                "SELECT key, avg(value), sum(colTop), count() FROM tab WHERE upper(key) = 'K3' ORDER BY key",
                """
                        key\tavg\tsum\tcount
                        k3\t1025.5\t420200.0\t800
                        """
        );
    }

    @Test
    public void testParallelStringKeyGroupByWithFilter3() throws Exception {
        testParallelStringAndVarcharKeyGroupBy(
                "SELECT key, avg(value), sum(colTop), count() FROM tab WHERE substring(key,2,1) = '3' ORDER BY key",
                """
                        key\tavg\tsum\tcount
                        k3\t1025.5\t420200.0\t800
                        """
        );
    }

    @Test
    public void testParallelStringKeyGroupByWithLimit() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelStringAndVarcharKeyGroupBy(
                "SELECT key, avg(value), sum(colTop) FROM tab ORDER BY key LIMIT 3",
                """
                        key\tavg\tsum
                        k0\t1027.5\t421000.0
                        k1\t1023.5\t419400.0
                        k2\t1024.5\t419800.0
                        """,
                "SELECT key, avg(value), sum(colTop) FROM tab ORDER BY key LIMIT -3",
                """
                        key\tavg\tsum
                        k2\t1024.5\t419800.0
                        k3\t1025.5\t420200.0
                        k4\t1026.5\t420600.0
                        """
        );
    }

    @Test
    public void testParallelStringKeyGroupByWithMinMaxStrFunction() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT key, min(astring), max(astring) FROM tab ORDER BY key",
                """
                        key\tmin\tmax
                        k0\tBBCNG\tZVBNJWFNBZC
                        k1\tBBSHZZIC\tZZUMQFJLG
                        k2\tBDHTRTU\tZYJDGSUYYESCKPGP
                        k3\tBBIMTJZLB\tZZCLVWGJMOXN
                        k4\tBBOYHGPKTIIFPB\tZYQPYIWSMSTJ
                        """
        );
    }

    @Test
    public void testParallelStringKeyGroupByWithMinMaxSymbolFunction() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT key, min(asymbol), max(asymbol) FROM tab ORDER BY key",
                """
                        key\tmin\tmax
                        k0\tCPSW\tVTJW
                        k1\tCPSW\tVTJW
                        k2\tCPSW\tVTJW
                        k3\tCPSW\tVTJW
                        k4\tCPSW\tVTJW
                        """
        );
    }

    @Test
    public void testParallelStringKeyGroupByWithNestedFilter() throws Exception {
        testParallelStringAndVarcharKeyGroupBy(
                "SELECT avg(v), k, sum(ct) " +
                        "FROM (SELECT colTop ct, value v, key k FROM tab WHERE value < 80) ORDER BY k",
                """
                        avg\tk\tsum
                        46.25\tk0\t325.0
                        45.31818181818182\tk1\t381.0
                        46.31818181818182\tk2\t387.0
                        47.31818181818182\tk3\t393.0
                        48.31818181818182\tk4\t399.0
                        """
        );
    }

    @Test
    public void testParallelStringKeyGroupByWithNotNullCheckInFilter() throws Exception {
        testParallelStringAndVarcharKeyGroupBy(
                "SELECT key, min(ts), max(ts) FROM tab WHERE key IS NOT NULL ORDER BY key",
                """
                        key\tmin\tmax
                        k0\t1970-01-01T01:12:00.000000Z\t1970-07-25T00:00:00.000000Z
                        k1\t1970-01-01T00:14:24.000000Z\t1970-07-24T14:24:00.000000Z
                        k2\t1970-01-01T00:28:48.000000Z\t1970-07-24T16:48:00.000000Z
                        k3\t1970-01-01T00:43:12.000000Z\t1970-07-24T19:12:00.000000Z
                        k4\t1970-01-01T00:57:36.000000Z\t1970-07-24T21:36:00.000000Z
                        """
        );
    }

    @Test
    public void testParallelStringKeyGroupByWithNullCheckInFilter() throws Exception {
        testParallelStringAndVarcharKeyGroupBy(
                "SELECT key, min(ts), max(ts) FROM tab WHERE key IS NULL ORDER BY key",
                "key\tmin\tmax\n"
        );
    }

    @Test
    public void testParallelStringKeyGroupByWithReadThreadSafeTimestampFilter() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT key, count_distinct(anint) FROM tab " +
                        "WHERE ts in '1970-01-13' and adouble < 1000 ORDER BY key DESC",
                """
                        key\tcount_distinct
                        k4\t29
                        k3\t27
                        k2\t31
                        k1\t29
                        k0\t28
                        """
        );
    }

    @Test
    public void testParallelStringKeyGroupByWithReadThreadUnsafeTimestampFilter() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT key, count_distinct(anint) FROM tab " +
                        "WHERE ts in '1970-01-13' and adouble < 1000 and key in ('k1', 'k2') ORDER BY key DESC",
                """
                        key\tcount_distinct
                        k2\t31
                        k1\t29
                        """
        );
    }

    @Test
    public void testParallelStringKeyGroupByWithTooStrictFilter() throws Exception {
        testParallelStringAndVarcharKeyGroupBy(
                "SELECT key, avg(value), sum(colTop), count() FROM tab WHERE value < 0 ORDER BY key",
                "key\tavg\tsum\tcount\n"
        );
    }

    @Test
    public void testParallelStringKeyGroupByWithTwoCountDistinctLongFunctions() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT key, count_distinct(along), count_distinct(abs(along) % 10) FROM tab ORDER BY key",
                """
                        key\tcount_distinct\tcount_distinct1
                        k0\t800\t10
                        k1\t800\t10
                        k2\t800\t10
                        k3\t800\t10
                        k4\t800\t10
                        """
        );
    }

    @Test
    public void testParallelStringKeyLikeFilter() throws Exception {
        final String fullResult = """
                key\tcount
                k0\t800
                k1\t800
                k2\t800
                k3\t800
                k4\t800
                """;
        testParallelStringAndVarcharKeyGroupBy(
                "SELECT key, count(*) FROM tab WHERE key like '%0' ORDER BY key",
                """
                        key\tcount
                        k0\t800
                        """,
                "SELECT key, count(*) FROM tab WHERE key like 'k%' ORDER BY key",
                fullResult,
                "SELECT key, count(*) FROM tab WHERE key like 'k_' ORDER BY key",
                fullResult,
                "SELECT key, count(*) FROM tab WHERE key like '%k%' ORDER BY key",
                fullResult,
                "SELECT key, count(*) FROM tab WHERE key like '%foobarbaz%' ORDER BY key",
                "key\tcount\n"
        );
    }

    @Test
    public void testParallelStringKeyNotLikeFilter() throws Exception {
        testParallelStringAndVarcharKeyGroupBy(
                "SELECT key, count(*) FROM tab WHERE key not like 'k0%' ORDER BY key",
                """
                        key\tcount
                        k1\t800
                        k2\t800
                        k3\t800
                        k4\t800
                        """
        );
    }

    @Test
    public void testParallelStringKeyedFirstFunction() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT key, " +
                        " first(aboolean) aboolean, first(abyte) abyte, first(ageobyte) ageobyte, " +
                        " first(ashort) ashort, first(ageoshort) ageoshort, first(achar) achar, " +
                        " first(anint) anint, first(anipv4) anipv4, first(ageoint) ageoint, first(afloat) afloat, " +
                        " first(along) along, first(adouble) adouble, first(adate) adate, first(ts) ts, first(ageolong) ageolong, " +
                        " first(asymbol) asymbol, first(astring) astring, " +
                        " first(auuid) auuid " +
                        "FROM tab ORDER BY key DESC",
                """
                        key\taboolean\tabyte\tageobyte\tashort\tageoshort\tachar\tanint\tanipv4\tageoint\tafloat\talong\tadouble\tadate\tts\tageolong\tasymbol\tastring\tauuid
                        k4\tfalse\t29\t1100\t664\t000101011000\tI\t1506802640\t66.9.11.179\t1011000011101011\t0.625966\t-5024542231726589509\tnull\t2015-08-03T15:58:03.335Z\t1970-01-05T15:31:40.000000Z\t01011101101001101000100100101110\t\tTKVVSJ\t8e4a7f66-1df6-432b-af17-1b3f06f6387d
                        k3\ttrue\t25\t1100\t1013\t001111001001\tC\t-1269042121\t184.92.27.200\t0101011010000100\t0.9566236\t-3214230645884399728\t0.5406709846540508\t2015-11-11T12:56:57.854Z\t1970-01-05T15:23:20.000000Z\t11111100110100011011101011101011\tPEHN\tLYXWCKYLSU\t78c594c4-9699-4885-aa18-96d0ad3419d2
                        k2\tfalse\t9\t0101\t279\t011101100011\tL\t1978144263\t171.117.213.66\t0111100011010111\tnull\t-7439145921574737517\t0.7763904674818695\t2015-09-18T13:48:49.642Z\t1970-01-05T15:15:00.000000Z\t01010100000001000011010111010101\tCPSW\tOOZZV\t9b27eba5-e9cf-41e2-9660-300cea7db540
                        k1\ttrue\t5\t1100\t788\t001111011001\tT\t-85170055\t149.34.19.60\t0010110111110001\t0.87567717\t8416773233910814357\t0.8799634725391621\t2015-08-17T21:12:06.116Z\t1970-01-05T15:06:40.000000Z\t10110001001100000010111011111011\tCPSW\tDXYSBEO\t4c009450-0fbf-4dfe-b6fb-2001fe5dfb09
                        k0\tfalse\t13\t0000\t165\t000000110100\tO\t-640305320\t22.51.83.99\t1011000000001111\t0.9918093\t-5315599072928175674\t0.32424562653969957\t2015-02-10T08:56:03.707Z\t1970-01-05T15:40:00.000000Z\t11011011111111001010110010100110\tCPSW\t\ta1d06d6e-b3a5-4079-8972-5663d8da9768
                        """
        );
    }

    @Test
    public void testParallelStringKeyedFirstFunctionFuzz() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testFirstLastFunctionFuzz(
                "SELECT key, " +
                        " first(aboolean) aboolean, first(abyte) abyte, first(ageobyte) ageobyte, " +
                        " first(ashort) ashort, first(ageoshort) ageoshort, first(achar) achar, " +
                        " first(anint) anint, first(anipv4) anipv4, first(ageoint) ageoint, first(afloat) afloat, " +
                        " first(along) along, first(adouble) adouble, first(adate) adate, first(ts) ts, first(ageolong) ageolong, " +
                        " first(asymbol) asymbol, first(astring) astring, " +
                        " first(auuid) auuid " +
                        "FROM tab ORDER BY key DESC"
        );
    }

    @Test
    public void testParallelStringKeyedFirstNotNullFunction() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT key, " +
                        " first_not_null(ageobyte) ageobyte, " +
                        " first_not_null(ageoshort) ageoshort, first_not_null(achar) achar, " +
                        " first_not_null(anint) anint, first_not_null(anipv4) anipv4, first_not_null(ageoint) ageoint, first_not_null(afloat) afloat, " +
                        " first_not_null(along) along, first_not_null(adouble) adouble, first_not_null(adate) adate, first_not_null(ts) ts, first_not_null(ageolong) ageolong, " +
                        " first_not_null(asymbol) asymbol, first_not_null(astring) astring, " +
                        " first_not_null(auuid) auuid " +
                        "FROM tab ORDER BY key DESC",
                """
                        key\tageobyte\tageoshort\tachar\tanint\tanipv4\tageoint\tafloat\talong\tadouble\tadate\tts\tageolong\tasymbol\tastring\tauuid
                        k4\t1100\t000101011000\tI\t1506802640\t66.9.11.179\t1011000011101011\t0.625966\t-5024542231726589509\t0.6213434403332111\t2015-08-03T15:58:03.335Z\t1970-01-05T15:31:40.000000Z\t01011101101001101000100100101110\tPEHN\tTKVVSJ\t8e4a7f66-1df6-432b-af17-1b3f06f6387d
                        k3\t1100\t001111001001\tC\t-1269042121\t184.92.27.200\t0101011010000100\t0.9566236\t-3214230645884399728\t0.5406709846540508\t2015-11-11T12:56:57.854Z\t1970-01-05T15:23:20.000000Z\t11111100110100011011101011101011\tPEHN\tLYXWCKYLSU\t78c594c4-9699-4885-aa18-96d0ad3419d2
                        k2\t0101\t011101100011\tL\t1978144263\t171.117.213.66\t0111100011010111\t0.5708643\t-7439145921574737517\t0.7763904674818695\t2015-09-18T13:48:49.642Z\t1970-01-05T15:15:00.000000Z\t01010100000001000011010111010101\tCPSW\tOOZZV\t9b27eba5-e9cf-41e2-9660-300cea7db540
                        k1\t1100\t001111011001\tT\t-85170055\t149.34.19.60\t0010110111110001\t0.87567717\t8416773233910814357\t0.8799634725391621\t2015-08-17T21:12:06.116Z\t1970-01-05T15:06:40.000000Z\t10110001001100000010111011111011\tCPSW\tDXYSBEO\t4c009450-0fbf-4dfe-b6fb-2001fe5dfb09
                        k0\t0000\t000000110100\tO\t-640305320\t22.51.83.99\t1011000000001111\t0.9918093\t-5315599072928175674\t0.32424562653969957\t2015-02-10T08:56:03.707Z\t1970-01-05T15:40:00.000000Z\t11011011111111001010110010100110\tCPSW\tQZSLQVFGPPRGSXB\ta1d06d6e-b3a5-4079-8972-5663d8da9768
                        """
        );
    }

    @Test
    public void testParallelStringKeyedFirstNotNullFunctionFuzz() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testFirstLastFunctionFuzz(
                "SELECT key, " +
                        " first_not_null(ageobyte) ageobyte, " +
                        " first_not_null(ageoshort) ageoshort, first_not_null(achar) achar, " +
                        " first_not_null(anint) anint, first_not_null(anipv4) anipv4, first_not_null(ageoint) ageoint, first_not_null(afloat) afloat, " +
                        " first_not_null(along) along, first_not_null(adouble) adouble, first_not_null(adate) adate, first_not_null(ts) ts, first_not_null(ageolong) ageolong, " +
                        " first_not_null(asymbol) asymbol, first_not_null(astring) astring, " +
                        " first_not_null(auuid) auuid " +
                        "FROM tab ORDER BY key DESC"
        );
    }

    @Test
    public void testParallelStringKeyedGroupByWithShortFunctions() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT key, sum(ashort), avg(ashort), min(ashort), max(ashort) FROM tab ORDER BY key",
                """
                        key\tsum\tavg\tmin\tmax
                        k0\t417840\t522.3\t11\t1024
                        k1\t415661\t519.57625\t10\t1022
                        k2\t393469\t491.83625\t10\t1022
                        k3\t412591\t515.73875\t10\t1023
                        k4\t410579\t513.22375\t14\t1024
                        """
        );
    }

    @Test
    public void testParallelStringKeyedGroupByWithUnionAll() throws Exception {
        testParallelGroupByAllTypes(
                "SELECT * " +
                        "FROM ( " +
                        "  SELECT key, min(achar), max(achar) FROM tab WHERE astring = 'ZZCLVWGJMOXN' " +
                        "  UNION ALL " +
                        "  SELECT key, min(achar), max(achar) FROM tab WHERE astring = 'BBCNG'" +
                        ") " +
                        "ORDER BY key DESC;",
                """
                        key\tmin\tmax
                        k3\tB\tB
                        k0\tD\tD
                        """
        );
    }

    @Test
    public void testParallelStringKeyedLastFunction() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT key, " +
                        " last(aboolean) aboolean, last(abyte) abyte, last(ageobyte) ageobyte, " +
                        " last(ashort) ashort, last(ageoshort) ageoshort, last(achar) achar, " +
                        " last(anint) anint, last(anipv4) anipv4, last(ageoint) ageoint, last(afloat) afloat, " +
                        " last(along) along, last(adouble) adouble, last(adate) adate, last(ts) ts, last(ageolong) ageolong, " +
                        " last(asymbol) asymbol, last(astring) astring, " +
                        " last(auuid) auuid " +
                        "FROM tab ORDER BY key DESC",
                """
                        key\taboolean\tabyte\tageobyte\tashort\tageoshort\tachar\tanint\tanipv4\tageoint\tafloat\talong\tadouble\tadate\tts\tageolong\tasymbol\tastring\tauuid
                        k4\tfalse\t31\t0101\t330\t110110100011\tF\t-848336394\t235.231.19.15\t0110111101100110\t0.25648606\t-9157587264521797613\t0.21377964990604514\t2015-02-01T20:25:30.629Z\t1970-01-28T18:23:20.000000Z\t01011011001110110000010000101101\tCPSW\tMPVGXH\t18362dcf-ef83-4aab-ac47-04e5093bf747
                        k3\tfalse\t38\t1010\t87\t110111011001\tJ\t1901541154\t37.251.146.2\t1001000010010001\tnull\t-5509931004723445033\t0.023379956696789717\t2015-02-12T10:52:41.010Z\t1970-01-28T18:15:00.000000Z\t11010110001110011010110000001111\tPEHN\tCSXKOBEGGNBZMI\t6e80006a-871f-417a-b33a-82ae2a7b83e8
                        k2\ttrue\t8\t0110\t556\t101001101101\tS\t1284672871\t123.157.83.21\t0000001101111100\t6.6161156E-4\t9154573717374787696\t0.151734552716993\t2015-02-06T11:08:08.607Z\t1970-01-28T18:06:40.000000Z\t00011101011001001010001110011010\t\tPWKZMYWJ\tcd1c6b4b-1b2d-4324-9477-dc8aeb3e13f3
                        k1\ttrue\t17\t1100\t147\t011101001110\tI\t1516951853\t88.98.63.55\t1010001110110001\t0.58338606\t-6618178923628468143\t0.1996073004071821\t2015-05-23T20:25:36.412Z\t1970-01-28T17:58:20.000000Z\t11001011111011110001101111100000\tPEHN\tFBGWS\t232fceaa-4da1-4f63-8f6d-0b7977b184bf
                        k0\tfalse\t28\t0100\t859\t111101110010\tY\t1033747429\t210.8.117.61\t0100111000110011\t0.030083895\t6812734169481155056\t0.15322992873721464\t2015-06-04T13:11:05.363Z\t1970-01-28T18:31:40.000000Z\t01001000100000110011110011111100\tHYRX\t\t8055fd98-3b39-4806-9dbf-5a050468a62a
                        """
        );
    }

    @Test
    public void testParallelStringKeyedLastFunctionFuzz() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testFirstLastFunctionFuzz(
                "SELECT key, " +
                        " last(aboolean) aboolean, last(abyte) abyte, last(ageobyte) ageobyte, " +
                        " last(ashort) ashort, last(ageoshort) ageoshort, last(achar) achar, " +
                        " last(anint) anint, last(anipv4) anipv4, last(ageoint) ageoint, last(afloat) afloat, " +
                        " last(along) along, last(adouble) adouble, last(adate) adate, last(ts) ts, last(ageolong) ageolong, " +
                        " last(asymbol) asymbol, last(astring) astring, " +
                        " last(auuid) auuid " +
                        "FROM tab ORDER BY key DESC"
        );
    }

    @Test
    public void testParallelStringKeyedLastNotNullFunction() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelGroupByAllTypes(
                "SELECT key, " +
                        " last_not_null(ageobyte) ageobyte, " +
                        " last_not_null(ageoshort) ageoshort, last_not_null(achar) achar, " +
                        " last_not_null(anint) anint, last_not_null(anipv4) anipv4, last_not_null(ageoint) ageoint, last_not_null(afloat) afloat, " +
                        " last_not_null(along) along, last_not_null(adouble) adouble, last_not_null(adate) adate, last_not_null(ts) ts, last_not_null(ageolong) ageolong, " +
                        " last_not_null(asymbol) asymbol, last_not_null(astring) astring, " +
                        " last_not_null(auuid) auuid " +
                        "FROM tab ORDER BY key DESC",
                """
                        key\tageobyte\tageoshort\tachar\tanint\tanipv4\tageoint\tafloat\talong\tadouble\tadate\tts\tageolong\tasymbol\tastring\tauuid
                        k4\t0101\t110110100011\tF\t-848336394\t235.231.19.15\t0110111101100110\t0.25648606\t-9157587264521797613\t0.21377964990604514\t2015-02-01T20:25:30.629Z\t1970-01-28T18:23:20.000000Z\t01011011001110110000010000101101\tCPSW\tMPVGXH\t18362dcf-ef83-4aab-ac47-04e5093bf747
                        k3\t1010\t110111011001\tJ\t1901541154\t37.251.146.2\t1001000010010001\t0.14877898\t-5509931004723445033\t0.023379956696789717\t2015-02-12T10:52:41.010Z\t1970-01-28T18:15:00.000000Z\t11010110001110011010110000001111\tPEHN\tCSXKOBEGGNBZMI\t6e80006a-871f-417a-b33a-82ae2a7b83e8
                        k2\t0110\t101001101101\tS\t1284672871\t123.157.83.21\t0000001101111100\t6.6161156E-4\t9154573717374787696\t0.151734552716993\t2015-02-06T11:08:08.607Z\t1970-01-28T18:06:40.000000Z\t00011101011001001010001110011010\tCPSW\tPWKZMYWJ\tcd1c6b4b-1b2d-4324-9477-dc8aeb3e13f3
                        k1\t1100\t011101001110\tI\t1516951853\t88.98.63.55\t1010001110110001\t0.58338606\t-6618178923628468143\t0.1996073004071821\t2015-05-23T20:25:36.412Z\t1970-01-28T17:58:20.000000Z\t11001011111011110001101111100000\tPEHN\tFBGWS\t232fceaa-4da1-4f63-8f6d-0b7977b184bf
                        k0\t0100\t111101110010\tY\t1033747429\t210.8.117.61\t0100111000110011\t0.030083895\t6812734169481155056\t0.15322992873721464\t2015-06-04T13:11:05.363Z\t1970-01-28T18:31:40.000000Z\t01001000100000110011110011111100\tHYRX\tFYJXOSBUGGYTSKTY\t8055fd98-3b39-4806-9dbf-5a050468a62a
                        """
        );
    }

    @Test
    public void testParallelStringKeyedLastNotNullFunctionFuzz() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testFirstLastFunctionFuzz(
                "SELECT key, " +
                        " last_not_null(ageobyte) ageobyte, " +
                        " last_not_null(ageoshort) ageoshort, last_not_null(achar) achar, " +
                        " last_not_null(anint) anint, last_not_null(anipv4) anipv4, last_not_null(ageoint) ageoint, last_not_null(afloat) afloat, " +
                        " last_not_null(along) along, last_not_null(adouble) adouble, last_not_null(adate) adate, last_not_null(ts) ts, last_not_null(ageolong) ageolong, " +
                        " last_not_null(asymbol) asymbol, last_not_null(astring) astring, " +
                        " last_not_null(auuid) auuid " +
                        "FROM tab ORDER BY key DESC"
        );
    }

    @Test
    public void testParallelSymbolKeyGroupBy() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelSymbolKeyGroupBy(
                "SELECT key, vwap(price, quantity), sum(colTop) FROM tab ORDER BY key",
                """
                        key\tvwap\tsum
                        k0\t2685.431565967941\t1642000.0
                        k1\t2682.7321472695826\t1638800.0
                        k2\t2683.4065201284266\t1639600.0
                        k3\t2684.081214514935\t1640400.0
                        k4\t2684.756229953121\t1641200.0
                        """,
                null
        );
    }

    @Test
    public void testParallelSymbolKeyGroupByFilterWithSubQuery() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key, vwap(price, quantity), sum(colTop) FROM tab " +
                        "where key in (select key from tab where key in ('k1','k3')) ORDER BY key",
                """
                        key\tvwap\tsum
                        k1\t2682.7321472695826\t1638800.0
                        k3\t2684.081214514935\t1640400.0
                        """,
                null
        );
    }

    @Test
    public void testParallelSymbolKeyGroupBySubQuery() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelSymbolKeyGroupBy(
                "SELECT key, vwap + sum FROM (" +
                        "  SELECT key, vwap(price, quantity), sum(colTop) FROM tab" +
                        ") ORDER BY key",
                """
                        key\tcolumn
                        k0\t1644685.4315659679
                        k1\t1641482.7321472696
                        k2\t1642283.4065201285
                        k3\t1643084.081214515
                        k4\t1643884.7562299531
                        """,
                null
        );
    }

    @Test
    public void testParallelSymbolKeyGroupByWithLimit() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelSymbolKeyGroupBy(
                "SELECT key, vwap(price, quantity), sum(colTop) FROM tab ORDER BY key LIMIT 3",
                """
                        key\tvwap\tsum
                        k0\t2685.431565967941\t1642000.0
                        k1\t2682.7321472695826\t1638800.0
                        k2\t2683.4065201284266\t1639600.0
                        """,
                null,
                "SELECT key, vwap(price, quantity), sum(colTop) FROM tab ORDER BY key LIMIT -3",
                """
                        key\tvwap\tsum
                        k2\t2683.4065201284266\t1639600.0
                        k3\t2684.081214514935\t1640400.0
                        k4\t2684.756229953121\t1641200.0
                        """,
                null
        );
    }

    @Test
    public void testParallelSymbolKeyGroupByWithMinMaxStrFunction() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelMultiSymbolKeyGroupBy(
                "SELECT key1, min(key2), max(key2) FROM tab ORDER BY key1",
                """
                        key1\tmin\tmax
                        k0\tk0\tk3
                        k1\tk0\tk3
                        k2\tk0\tk3
                        k3\tk0\tk3
                        k4\tk0\tk3
                        """
        );
    }

    @Test
    public void testParallelSymbolKeyGroupByWithNestedFilter() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT vwap(p, q), k, sum(ct) " +
                        "FROM (SELECT colTop ct, price p, quantity q, key k FROM tab WHERE quantity < 80) ORDER BY k",
                """
                        vwap\tk\tsum
                        56.62162162162162\tk0\t325.0
                        57.01805416248746\tk1\t381.0
                        57.76545632973504\tk2\t387.0
                        58.52353506243996\tk3\t393.0
                        59.29162746942615\tk4\t399.0
                        """,
                null
        );
    }

    @Test
    public void testParallelSymbolKeyGroupByWithNoFunctions() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelSymbolKeyGroupBy(
                "SELECT key FROM tab GROUP BY key ORDER BY key",
                """
                        key
                        k0
                        k1
                        k2
                        k3
                        k4
                        """,
                null
        );
    }

    @Test
    public void testParallelSymbolKeyGroupByWithNoFunctionsAndFilter() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key FROM tab WHERE key != 'k1' GROUP BY key ORDER BY key",
                """
                        key
                        k0
                        k2
                        k3
                        k4
                        """,
                null
        );
    }

    @Test
    public void testParallelSymbolKeyGroupByWithNoFunctionsAndTooStrictFilter() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key FROM tab WHERE quantity < 0 GROUP BY key ORDER BY key",
                "key\n",
                null
        );
    }

    @Test
    public void testParallelSymbolKeyGroupByWithWithReadThreadSafeFilter() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key, vwap(price, quantity), sum(colTop) FROM tab WHERE quantity < 80 ORDER BY key",
                """
                        key\tvwap\tsum
                        k0\t56.62162162162162\t325.0
                        k1\t57.01805416248746\t381.0
                        k2\t57.76545632973504\t387.0
                        k3\t58.52353506243996\t393.0
                        k4\t59.29162746942615\t399.0
                        """,
                null
        );
    }

    @Test
    public void testParallelSymbolKeyGroupByWithWithReadThreadUnsafeFilter() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key, vwap(price, quantity), sum(colTop) FROM tab WHERE key in ('k1','k2') ORDER BY key",
                """
                        key\tvwap\tsum
                        k1\t2682.7321472695826\t1638800.0
                        k2\t2683.4065201284266\t1639600.0
                        """,
                null
        );
    }

    @Test
    public void testParallelSymbolKeyLikeFilter() throws Exception {
        final String fullResult = """
                key\tcount
                k0\t1600
                k1\t1600
                k2\t1600
                k3\t1600
                k4\t1600
                """;
        testParallelSymbolKeyGroupBy(
                "SELECT key, count(*) FROM tab WHERE key like '%0' ORDER BY key",
                """
                        key\tcount
                        k0\t1600
                        """,
                null,
                "SELECT key, count(*) FROM tab WHERE key like 'k%' ORDER BY key",
                fullResult,
                null,
                "SELECT key, count(*) FROM tab WHERE key like 'k_' ORDER BY key",
                fullResult,
                null,
                "SELECT key, count(*) FROM tab WHERE key like '%k%' ORDER BY key",
                fullResult,
                null,
                "SELECT key, count(*) FROM tab WHERE key like '%foobarbaz%' ORDER BY key",
                "key\tcount\n",
                null
        );
    }

    @Test
    public void testParallelSymbolKeyNotLikeFilter() throws Exception {
        testParallelSymbolKeyGroupBy(
                "SELECT key, count(*) FROM tab WHERE key not like 'k0%' ORDER BY key",
                """
                        key\tcount
                        k1\t1600
                        k2\t1600
                        k3\t1600
                        k4\t1600
                        """,
                null
        );
    }

    @Test
    public void testParallelToStrFunctionKeyGroupBy() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        testParallelSymbolKeyGroupBy(
                "SELECT to_str(ts, 'yyyy-MM-dd') ts, max(price) FROM tab ORDER BY ts LIMIT 5",
                """
                        ts\tmax
                        1970-01-01\t99.0
                        1970-01-02\t199.0
                        1970-01-03\t299.0
                        1970-01-04\t399.0
                        1970-01-05\t499.0
                        """,
                null
        );
    }

    @Test
    public void testStringKeyGroupByEmptyTable() throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        // The table is empty, so there is nothing to convert.
        Assume.assumeFalse(convertToParquet);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        execute(
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

    private static void assertCairoException(CairoEngine engine, SqlExecutionContext sqlExecutionContext) throws SqlException {
        try {
            try (
                    RecordCursorFactory factory = engine.select("SELECT key, avg(value) FROM tab WHERE npe();", sqlExecutionContext);
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                cursor.hasNext();
            }
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "unexpected filter error");
        }
    }

    private void testFirstLastFunctionFuzz(String query) throws Exception {
        // With this test, we aim to verify correctness of merge() method
        // implementation in first/last functions.

        // This test controls sets enable parallel GROUP BY flag on its own.
        Assume.assumeTrue(enableParallelGroupBy);
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
                        sqlExecutionContext.setRandom(rnd);

                        execute(
                                compiler,
                                "create table tab as (select" +
                                        " 'k' || ((50 + x) % 5) key," +
                                        " rnd_boolean() aboolean," +
                                        " rnd_byte(2,50) abyte," +
                                        " rnd_geohash(4) ageobyte," +
                                        " rnd_short(10,1024) ashort," +
                                        " rnd_geohash(12) ageoshort," +
                                        " rnd_char() achar," +
                                        " rnd_int(0,1000,3) anint," +
                                        " rnd_ipv4() anipv4," +
                                        " rnd_geohash(16) ageoint," +
                                        " rnd_symbol(4,4,4,2) asymbol," +
                                        " rnd_float(3) afloat," +
                                        " rnd_long(0,1000,3) along," +
                                        " rnd_double(3) adouble," +
                                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 3) adate," +
                                        " rnd_geohash(32) ageolong," +
                                        " rnd_str(5,16,3) astring," +
                                        " rnd_uuid4() auuid," +
                                        " timestamp_sequence(400000000000, 500000000) ts" +
                                        " from long_sequence(10000)) timestamp(ts) partition by day",
                                sqlExecutionContext
                        );
                        if (convertToParquet) {
                            execute(compiler, "alter table tab convert partition to parquet where ts >= 0", sqlExecutionContext);
                        }

                        // Run with single-threaded GROUP BY.
                        sqlExecutionContext.setParallelGroupByEnabled(false);
                        try {
                            TestUtils.printSql(
                                    engine,
                                    sqlExecutionContext,
                                    query,
                                    sink
                            );
                        } finally {
                            sqlExecutionContext.setParallelGroupByEnabled(engine.getConfiguration().isSqlParallelGroupByEnabled());
                        }

                        // Run with parallel GROUP BY.
                        sqlExecutionContext.setParallelGroupByEnabled(true);
                        final StringSink sinkB = new StringSink();
                        try {
                            TestUtils.printSql(
                                    engine,
                                    sqlExecutionContext,
                                    query,
                                    sinkB
                            );
                        } finally {
                            sqlExecutionContext.setParallelGroupByEnabled(engine.getConfiguration().isSqlParallelGroupByEnabled());
                        }

                        // Compare the results.
                        TestUtils.assertEquals(sink, sinkB);
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void testParallelDecimalKeyGroupBy(String... queriesAndExpectedResults) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);

                        engine.execute(
                                "CREATE TABLE tab (" +
                                        "  ts TIMESTAMP," +
                                        "  key DECIMAL(2,0)," +
                                        "  d8 DECIMAL(2,0)," +
                                        "  d16 DECIMAL(4,1)," +
                                        "  d32 DECIMAL(7,1)," +
                                        "  d64 DECIMAL(15,2)," +
                                        "  d128 DECIMAL(32,3)," +
                                        "  d256 DECIMAL(76,6)" +
                                        ") timestamp (ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "insert into tab select (x * 864000000)::timestamp ts," +
                                        " cast((x % 5) as decimal(2,0)) key, " +
                                        " cast((x % 2) as decimal(2,0)) d8, " +
                                        " cast((x % 10) as decimal(4,1)) d16, " +
                                        " cast((x % 20) as decimal(7,1)) d32, " +
                                        " cast((x % 30) as decimal(15,2)) d64, " +
                                        " cast((x % 40) as decimal(32,3)) d128, " +
                                        " cast((x % 100) as decimal(76,6)) d256 " +
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

    private void testParallelGroupByAllTypes(BindVariablesInitializer initializer, String... queriesAndExpectedResults) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
                        if (initializer != null) {
                            initializer.init(sqlExecutionContext);
                        }

                        execute(
                                compiler,
                                "create table tab as (select" +
                                        " 'k' || ((50 + x) % 5) key," +
                                        " rnd_boolean() aboolean," +
                                        " rnd_byte(2,50) abyte," +
                                        " rnd_geohash(4) ageobyte," +
                                        " rnd_short(10,1024) ashort," +
                                        " rnd_geohash(12) ageoshort," +
                                        " rnd_char() achar," +
                                        " rnd_int() anint," +
                                        " rnd_ipv4() anipv4," +
                                        " rnd_geohash(16) ageoint," +
                                        " rnd_symbol(4,4,4,2) asymbol," +
                                        " rnd_float(2) afloat," +
                                        " rnd_long() along," +
                                        " rnd_double(2) adouble," +
                                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) adate," +
                                        " rnd_geohash(32) ageolong," +
                                        " rnd_str(5,16,2) astring," +
                                        " rnd_uuid4() auuid," +
                                        " timestamp_sequence(400000000000, 500000000) ts" +
                                        " from long_sequence(" + ROW_COUNT + ")) timestamp(ts) partition by day",
                                sqlExecutionContext
                        );
                        if (convertToParquet) {
                            execute(compiler, "alter table tab convert partition to parquet where ts >= 0", sqlExecutionContext);
                        }
                        assertQueries(engine, sqlExecutionContext, queriesAndExpectedResults);
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void testParallelGroupByAllTypes(String... queriesAndExpectedResults) throws Exception {
        testParallelGroupByAllTypes(null, queriesAndExpectedResults);
    }

    private void testParallelGroupByArray(String... queriesAndExpectedResults) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
                        execute(
                                compiler,
                                "create table tab as (select" +
                                        " 'k' || ((50 + x) % 5) key," +
                                        " rnd_double_array(2, 2, 3) darr," +
                                        " timestamp_sequence(400000000000, 500000000) ts" +
                                        " from long_sequence(" + ROW_COUNT + ")) timestamp(ts) partition by day",
                                sqlExecutionContext
                        );
                        if (convertToParquet) {
                            execute(compiler, "alter table tab convert partition to parquet where ts >= 0", sqlExecutionContext);
                        }
                        assertQueries(engine, sqlExecutionContext, queriesAndExpectedResults);
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void testParallelGroupByFaultTolerance(String query) throws Exception {
        Assume.assumeTrue(enableParallelGroupBy);
        node1.setProperty(PropertyKey.DEV_MODE_ENABLED, true);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);

                        engine.execute(
                                "CREATE TABLE tab (" +
                                        "  ts TIMESTAMP," +
                                        "  price DOUBLE," +
                                        "  quantity DOUBLE) timestamp (ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "insert into tab select (x * 864000000)::timestamp, x, x % 100 from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        if (convertToParquet) {
                            execute(compiler, "alter table tab convert partition to parquet where ts >= 0", sqlExecutionContext);
                        }

                        try {
                            try (final RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                                try (final RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                    //noinspection StatementWithEmptyBody
                                    while (cursor.hasNext()) {
                                    } // drain cursor until exception
                                    Assert.fail();
                                }
                            }
                        } catch (Throwable th) {
                            TestUtils.assertContains(th.getMessage(), "unexpected filter error");
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void testParallelGroupByThrowsOnTimeout(String query, long tripWhenTicks) throws Exception {
        // This query doesn't use filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        // Validate parallel GROUP BY factories.
        Assume.assumeTrue(enableParallelGroupBy);
        // The test is very sensitive to sharding threshold and page frame sizes.
        Assert.assertEquals(2, configuration.getGroupByShardingThreshold());
        final int rowCount = ROW_COUNT;
        Assert.assertEquals(40, rowCount / configuration.getSqlPageFrameMaxRows());
        assertMemoryLeak(() -> {
            circuitBreakerConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
                private final AtomicLong ticks = new AtomicLong();

                @Override
                @NotNull
                public MillisecondClock getClock() {
                    return () -> {
                        if (ticks.incrementAndGet() < tripWhenTicks) {
                            return 0;
                        }
                        return Long.MAX_VALUE;
                    };
                }

                @Override
                public long getQueryTimeout() {
                    return 1;
                }
            };

            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        final SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
                        final NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(engine, circuitBreakerConfiguration, MemoryTag.NATIVE_DEFAULT);
                        try {
                            engine.execute(
                                    "CREATE TABLE tab ( " +
                                            "  ts TIMESTAMP, " +
                                            "  price DOUBLE, " +
                                            "  quantity DOUBLE " +
                                            ") TIMESTAMP(ts) PARTITION BY DAY;",
                                    sqlExecutionContext
                            );
                            engine.execute(
                                    "insert into tab select (x * 864000000)::timestamp, x, x % 100 from long_sequence(" + rowCount + ")",
                                    sqlExecutionContext
                            );
                            if (convertToParquet) {
                                engine.execute("alter table tab convert partition to parquet where ts >= 0", sqlExecutionContext);
                            }

                            context.with(
                                    context.getSecurityContext(),
                                    context.getBindVariableService(),
                                    context.getRandom(),
                                    context.getRequestFd(),
                                    circuitBreaker
                            );
                            context.setJitMode(SqlJitMode.JIT_MODE_ENABLED);

                            TestUtils.assertSql(compiler, context, query, sink, "");
                            Assert.fail();
                        } catch (CairoException ex) {
                            TestUtils.assertContains(ex.getFlyweightMessage(), "timeout, query aborted");
                        } finally {
                            Misc.free(circuitBreaker);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void testParallelIPv4KeyGroupBy(String... queriesAndExpectedResults) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);

                        engine.execute(
                                "CREATE TABLE tab (" +
                                        "  ts TIMESTAMP," +
                                        "  key IPv4," +
                                        "  value DOUBLE" +
                                        ") TIMESTAMP (ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "insert into tab select (x * 864000000)::timestamp, ((x % 200)::int)::ipv4, x from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        if (convertToParquet) {
                            execute(compiler, "alter table tab convert partition to parquet where ts >= 0", sqlExecutionContext);
                        }
                        assertQueries(engine, sqlExecutionContext, queriesAndExpectedResults);
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void testParallelJsonKeyGroupBy(String... queriesAndExpectedResults) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);

                        engine.execute(
                                "CREATE TABLE tab (" +
                                        "  ts TIMESTAMP," +
                                        "  key VARCHAR," +
                                        "  price DOUBLE," +
                                        "  quantity LONG) timestamp (ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "insert into tab select (x * 864000000)::timestamp, '{\"key\": \"k' || (x % 5) || '\", \"foo\": \"bar\"}', x, x from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        if (convertToParquet) {
                            execute(compiler, "alter table tab convert partition to parquet where ts >= 0", sqlExecutionContext);
                        }
                        assertQueries(engine, sqlExecutionContext, queriesAndExpectedResults);
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void testParallelMultiSymbolKeyGroupBy(String... queriesAndExpectedResults) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);

                        engine.execute(
                                "CREATE TABLE tab (\n" +
                                        "  ts TIMESTAMP," +
                                        "  key1 SYMBOL," +
                                        "  key2 SYMBOL," +
                                        "  key3 SYMBOL," +
                                        "  value DOUBLE) timestamp (ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "insert into tab select (x * 864000000)::timestamp, 'k' || (x % 5), 'k' || (x % 4), 'k' || (x % 3), x from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        execute(compiler, "ALTER TABLE tab ADD COLUMN colTop DOUBLE", sqlExecutionContext);
                        engine.execute(
                                "insert into tab " +
                                        "select ((50 + x) * 864000000)::timestamp, 'k' || ((50 + x) % 5), 'k' || ((50 + x) % 4), 'k' || ((50 + x) % 3), 50 + x, 50 + x " +
                                        "from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        if (convertToParquet) {
                            execute(compiler, "alter table tab convert partition to parquet where ts >= 0", sqlExecutionContext);
                        }
                        assertQueries(engine, sqlExecutionContext, queriesAndExpectedResults);
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void testParallelNonKeyedGroupBy(String... queriesAndExpectedResults) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);

                        engine.execute(
                                "CREATE TABLE tab (" +
                                        "  ts TIMESTAMP," +
                                        "  price DOUBLE," +
                                        "  quantity DOUBLE) timestamp (ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "insert into tab select (x * 864000000)::timestamp, x, x % 100 from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        engine.execute("ALTER TABLE tab ADD COLUMN colTop DOUBLE", sqlExecutionContext);
                        engine.execute(
                                "insert into tab " +
                                        "select ((50 + x) * 864000000)::timestamp, 50 + x, 50 + x, 50 + x " +
                                        "from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        if (convertToParquet) {
                            execute(compiler, "alter table tab convert partition to parquet where ts >= 0", sqlExecutionContext);
                        }
                        assertQueries(engine, sqlExecutionContext, queriesAndExpectedResults);
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void testParallelRostiGroupBy(String query, String expected) throws Exception {
        // Rosti doesn't support filter, so we don't care about JIT.
        Assume.assumeTrue(enableJitCompiler);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        // We want each row to be in its own partition
                        execute(
                                compiler,
                                "CREATE TABLE tab AS (SELECT " +
                                        "cast('k' || (x%5) as symbol) key, " +
                                        "rnd_short() s, " +
                                        "rnd_int(0, 256, 2) i, " +
                                        "rnd_long(0, 1024, 2) l, " +
                                        "rnd_long256(2) l256, " +
                                        "rnd_double(2) d, " +
                                        "rnd_timestamp(to_date('1980', 'yyyy'), to_date('1990', 'yyyy'), 2) t, " +
                                        "rnd_date(to_date('1980', 'yyyy'), to_date('1990', 'yyyy'), 2) dd, " +
                                        "(x * 864000000)::timestamp ts " +
                                        "from long_sequence(500)) timestamp (ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        execute(
                                compiler,
                                "insert into tab values (null, 1, 2, 3, 4::long256, 5.0, '1991-01-01', '1992-01-01', 0::timestamp)," +
                                        "(null, 11, 12, 13, 14::long256, 15.0, '2001-01-01', '2002-01-01', (250L*864000000)::timestamp)," +
                                        "(null, 21, 22, 23, 24::long256, 25.0, '2101-01-01', '2102-01-01', (500L*864000000)::timestamp)",
                                sqlExecutionContext
                        );
                        if (convertToParquet) {
                            execute(compiler, "alter table tab convert partition to parquet where ts >= 0", sqlExecutionContext);
                        }

                        TestUtils.assertSql(
                                engine,
                                sqlExecutionContext,
                                query,
                                sink,
                                expected
                        );

                        if (enableParallelGroupBy) {
                            // Make sure that we're testing Rosti here.
                            try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                                RecordCursorFactory nestedFactory = factory.getBaseFactory();
                                while (nestedFactory != null) {
                                    if (nestedFactory.getClass() == GroupByRecordCursorFactory.class) {
                                        break;
                                    }
                                    nestedFactory = nestedFactory.getBaseFactory();
                                }
                                Assert.assertNotNull("parallel GROUP BY doesn't use vect.GroupByRecordCursorFactory", nestedFactory);
                            }
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void testParallelStringAndVarcharKeyGroupBy(String... queriesAndExpectedResults) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);

                        // Use 2x fewer rows in this test as otherwise it's slow.
                        final int rowCount = ROW_COUNT / 2;

                        // try with a String table first
                        engine.execute(
                                "CREATE TABLE tab (" +
                                        "  ts TIMESTAMP," +
                                        "  key STRING," +
                                        "  value DOUBLE) timestamp (ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "insert into tab select (x * 864000000)::timestamp, 'k' || (x % 5), x from long_sequence(" + rowCount + ")",
                                sqlExecutionContext
                        );
                        engine.execute("ALTER TABLE tab ADD COLUMN colTop DOUBLE", sqlExecutionContext);
                        engine.execute(
                                "insert into tab " +
                                        "select ((50 + x) * 8640000000)::timestamp, 'k' || ((50 + x) % 5), 50 + x, 50 + x " +
                                        "from long_sequence(" + rowCount + ")",
                                sqlExecutionContext
                        );
                        if (convertToParquet) {
                            execute(compiler, "alter table tab convert partition to parquet where ts >= 0", sqlExecutionContext);
                        }
                        assertQueries(engine, sqlExecutionContext, queriesAndExpectedResults);

                        // now drop the String table and recreate it with a Varchar key
                        engine.execute("DROP TABLE tab", sqlExecutionContext);
                        engine.execute(
                                "CREATE TABLE tab (" +
                                        "  ts TIMESTAMP," +
                                        "  key VARCHAR," +
                                        "  value DOUBLE) timestamp (ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "insert into tab select (x * 864000000)::timestamp, 'k' || (x % 5), x from long_sequence(" + rowCount + ")",
                                sqlExecutionContext
                        );
                        engine.execute("ALTER TABLE tab ADD COLUMN colTop DOUBLE", sqlExecutionContext);
                        engine.execute(
                                "insert into tab " +
                                        "select ((50 + x) * 8640000000)::timestamp, 'k' || ((50 + x) % 5), 50 + x, 50 + x " +
                                        "from long_sequence(" + rowCount + ")",
                                sqlExecutionContext
                        );
                        if (convertToParquet) {
                            execute(compiler, "alter table tab convert partition to parquet where ts >= 0", sqlExecutionContext);
                        }
                        assertQueries(engine, sqlExecutionContext, queriesAndExpectedResults);
                    },
                    configuration,
                    LOG
            );
        });
    }

    // unlike other test methods, this one expects triples, not couples of strings;
    // the third value may contain a fragment of query plan to assert
    private void testParallelSymbolKeyGroupBy(String... queriesExpectedResultsAndPlans) throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);

                        engine.execute(
                                "CREATE TABLE tab (" +
                                        "  ts TIMESTAMP," +
                                        "  key SYMBOL," +
                                        "  price DOUBLE," +
                                        "  quantity LONG) timestamp (ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "insert into tab select (x * 864000000)::timestamp, 'k' || (x % 5), x, x from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        engine.execute("ALTER TABLE tab ADD COLUMN colTop DOUBLE", sqlExecutionContext);
                        engine.execute(
                                "insert into tab " +
                                        "select ((50 + x) * 864000000)::timestamp, " +
                                        "  'k' || ((50 + x) % 5), 50 + x, 50 + x, 50 + x " +
                                        "from long_sequence(" + ROW_COUNT + ")",
                                sqlExecutionContext
                        );
                        if (convertToParquet) {
                            execute(compiler, "alter table tab convert partition to parquet where ts >= 0", sqlExecutionContext);
                        }
                        assertQueriesAndPlans(engine, sqlExecutionContext, queriesExpectedResultsAndPlans);
                    },
                    configuration,
                    LOG
            );
        });
    }

    static void assertQueries(CairoEngine engine, SqlExecutionContext sqlExecutionContext, String... queriesAndExpectedResults) throws SqlException {
        assertQueries(engine, sqlExecutionContext, sink, queriesAndExpectedResults);
    }

    static void assertQueries(CairoEngine engine, SqlExecutionContext sqlExecutionContext, StringSink sink, String... queriesAndExpectedResults) throws SqlException {
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

    static void assertQueriesAndPlans(CairoEngine engine, SqlExecutionContext sqlExecutionContext, String... queriesExpectedResultsAndPlans) throws SqlException {
        for (int i = 0, n = queriesExpectedResultsAndPlans.length; i < n; i += 3) {
            final String query = queriesExpectedResultsAndPlans[i];
            final String expected = queriesExpectedResultsAndPlans[i + 1];
            sink.clear();
            TestUtils.assertSql(
                    engine,
                    sqlExecutionContext,
                    query,
                    sink,
                    expected
            );

            // verify the plan, optionally
            final String expectedPlanFragment = queriesExpectedResultsAndPlans[i + 2];
            if (expectedPlanFragment != null) {
                TestUtils.printSql(
                        engine,
                        sqlExecutionContext,
                        "EXPLAIN " + query,
                        sink
                );
                TestUtils.assertContains(sink, expectedPlanFragment);
            }
        }
    }

    private interface BindVariablesInitializer {
        void init(SqlExecutionContext sqlExecutionContext) throws SqlException;
    }
}
