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

package io.questdb.test.griffin.engine.groupby.vect;

import io.questdb.Metrics;
import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.Numbers;
import io.questdb.std.Rosti;
import io.questdb.std.RostiAllocFacadeImpl;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The vectorized (Rosti) keyed GROUP BY over page frames whose key column holds no data: a column
 * top, which ALTER TABLE ADD COLUMN leaves in the partitions written before it, or a Parquet row
 * group whose key column is all NULL. Every row of such a frame has the NULL key, and the NULL key
 * must aggregate as it does in the non-vectorized GROUP BY, whatever the aggregate, its value
 * column, the key kind and the other aggregates of the query.
 */
@RunWith(Parameterized.class)
public class GroupByVectorizedNullKeyTest extends AbstractCairoTest {
    // Every aggregate that the vectorized GROUP BY implements, over each column type it takes; ts is
    // the designated timestamp, whose min() and max() take a shortcut of their own.
    private static final String[] AGGREGATES = {
            "avg(d)", "avg(i)", "avg(l)", "avg(s)",
            "count()", "count(d)", "count(i)", "count(l)", "count(dt)", "count(tsv)", "count(tsvn)",
            "ksum(d)", "nsum(d)",
            "max(d)", "max(dt)", "max(i)", "max(l)", "max(s)", "max(tsv)", "max(tsvn)", "max(ts)",
            "min(d)", "min(dt)", "min(i)", "min(l)", "min(s)", "min(tsv)", "min(tsvn)", "min(ts)",
            "sum(d)", "sum(i)", "sum(l)", "sum(l256)", "sum(s)"
    };
    // An INT key, a SYMBOL key, and hour() of a TIMESTAMP and of a TIMESTAMP_NS column.
    private static final String[] KEYS = {"k", "sym", "hour(kts) h", "hour(ktsn) h"};
    private static final String VALUES = """
            CASE WHEN x % 7 = 0 THEN NULL ELSE x / 4.0 - 300 END,
            CASE WHEN x % 7 = 1 THEN NULL ELSE (x - 1_500)::INT END,
            CASE WHEN x % 7 = 2 THEN NULL ELSE x * 1_000_003 - 1_500_000_000 END,
            (x % 300 - 150)::SHORT,
            CASE WHEN x % 7 = 3 THEN NULL ELSE (x * 86_400_000)::DATE END,
            CASE WHEN x % 7 = 4 THEN NULL ELSE (x * 7)::LONG256 END,
            CASE WHEN x % 7 = 5 THEN NULL ELSE (x * 1_000_000)::TIMESTAMP END,
            CASE WHEN x % 7 = 6 THEN NULL ELSE (x * 1_000_000_000)::TIMESTAMP_NS END""";
    private final boolean isWal;

    public GroupByVectorizedNullKeyTest(WalMode walMode) {
        this.isWal = walMode == WalMode.WITH_WAL;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{WalMode.WITH_WAL}, {WalMode.NO_WAL}});
    }

    @Test
    public void testNullKeyMatchesNonVectorizedGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            createColumnTopTable("t", false, 0);
            for (String key : KEYS) {
                for (String aggregate : AGGREGATES) {
                    // The aggregate alone, and after count(), which puts the NULL key in the map
                    // before the aggregate wraps up.
                    assertMatchesNonVectorized(sqlExecutionContext, "SELECT " + key + ", " + aggregate + " v FROM t ORDER BY 1");
                    assertMatchesNonVectorized(sqlExecutionContext, "SELECT " + key + ", count() c, " + aggregate + " v FROM t ORDER BY 1");
                }
                assertMatchesNonVectorized(sqlExecutionContext, allAggregates(key, "t"));
            }

            // The partition with the column tops, converted to Parquet, decodes the key column as
            // no data.
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalIfNeeded();
            assertQuery("SELECT name, isParquet FROM table_partitions('t')")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            name\tisParquet
                            2024-01-01\ttrue
                            2024-01-02\tfalse
                            """);
            for (String key : KEYS) {
                assertMatchesNonVectorized(sqlExecutionContext, allAggregates(key, "t"));
                assertMatchesNonVectorized(sqlExecutionContext, "SELECT " + key + ", avg(l) v FROM t ORDER BY 1");
                assertMatchesNonVectorized(sqlExecutionContext, "SELECT " + key + ", count() c, sum(l) v FROM t ORDER BY 1");
            }
        });
    }

    @Test
    public void testNullKeyMatchesNonVectorizedGroupByOverValueColumnTops() throws Exception {
        // The value columns are column tops in the first partition too: its rows add the NULL key
        // without a value.
        assertMemoryLeak(() -> {
            createColumnTopTable("tv", true, 0);
            for (String key : KEYS) {
                for (String aggregate : AGGREGATES) {
                    if (!isShortColumnTopSensitive(key, aggregate)) {
                        assertMatchesNonVectorized(sqlExecutionContext, "SELECT " + key + ", " + aggregate + " v FROM tv ORDER BY 1");
                    }
                }
                assertMatchesNonVectorized(sqlExecutionContext, allAggregates(key, "tv", true));
            }
        });
    }

    @Test
    public void testNullKeyMatchesNonVectorizedGroupByOnWorkers() throws Exception {
        // Both worker pool modes, each of which publishes and merges the per-worker maps its own way.
        assertMemoryLeak(() -> {
            final int workerCount = 4;
            for (WorkerPoolMode mode : WorkerPoolMode.values()) {
                final WorkerPool pool = new TestWorkerPool("nullKey", workerCount, Metrics.DISABLED, mode);
                WorkerPoolUtils.setupQueryJobs(pool, engine);
                pool.start(null);
                try (SqlExecutionContext parallelContext = new SqlExecutionContextImpl(engine, workerCount)
                        .with(securityContext, bindVariableService, null, -1, circuitBreaker)) {
                    parallelContext.initNow();
                    // Several partitions of column tops, so that the workers aggregate the NULL key in
                    // more than one map and the maps merge.
                    final String table = "t_" + mode.name().toLowerCase();
                    createColumnTopTable(table, false, 6);
                    for (String key : KEYS) {
                        assertMatchesNonVectorized(parallelContext, allAggregates(key, table));
                        assertMatchesNonVectorized(parallelContext, "SELECT " + key + ", avg(l) v FROM " + table + " ORDER BY 1");
                        assertMatchesNonVectorized(parallelContext, "SELECT " + key + ", count() c, sum(l) v FROM " + table + " ORDER BY 1");
                    }
                } finally {
                    pool.halt();
                }
            }
        });
    }

    @Test
    public void testNullKeyMemoryLimitInParquetDecode() throws Exception {
        // The per-query memory limit bounds the page frame pools that decode Parquet row groups, here
        // the row groups whose key column is a column top, which aggregate under the NULL key. The
        // Rosti maps themselves stay on the global RSS limit, see GroupByRecordCursorFactory. Each
        // breach fails the query with the limit's error and releases everything, and the query
        // returns its rows once the limit allows the decode.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 1_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v LONG, d DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" + walSuffix());
            execute("INSERT INTO t SELECT x, x / 2.0, timestamp_sequence('2024-01-01', 10_000_000) FROM long_sequence(5_000)");
            drainWalIfNeeded();
            execute("ALTER TABLE t ADD COLUMN k INT");
            execute("INSERT INTO t (v, d, ts, k) VALUES (7, 3.5, '2024-01-03T00:00:00Z', 1)");
            drainWalIfNeeded();
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalIfNeeded();
            final String sql = "SELECT k, count() c, sum(v) s, max(d) mx FROM t ORDER BY k";
            // The NULL key: the 5_000 column top rows, v 1 to 5_000 and d up to 2_500.0.
            final String expected = """
                    k\tc\ts\tmx
                    null\t5000\t12502500\t2500.0
                    1\t1\t7\t3.5
                    """;
            int failures = 0;
            try (RecordCursorFactory factory = select(sql)) {
                for (long limit = 1_024; ; limit *= 2) {
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        sink.clear();
                        CursorPrinter.println(cursor, factory.getMetadata(), sink);
                        TestUtils.assertEquals(expected, sink);
                        break;
                    } catch (CairoException e) {
                        Assert.assertTrue(e.getMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        failures++;
                    }
                    Assert.assertTrue("the limit never let the decode fit: " + limit, limit < Numbers.SIZE_1GB);
                }
                Assert.assertTrue(failures > 0);
                // The factory runs again without a limit.
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    sink.clear();
                    CursorPrinter.println(cursor, factory.getMetadata(), sink);
                    TestUtils.assertEquals(expected, sink);
                }
            }
            assertVectorized(sqlExecutionContext, sql, expected);
            assertMatchesNonVectorized(sqlExecutionContext, sql);
        });
    }

    @Test
    public void testNullKeyOverColumnTopFramesAroundChunkSize() throws Exception {
        // Frames of column tops of 1_024 rows, one row less and one more, and of two such chunks and
        // one row more: the GROUP BY aggregates each frame whole and merges it into the NULL key.
        assertMemoryLeak(() -> {
            for (int rows : new int[]{1, 1_023, 1_024, 1_025, 2_048, 2_049}) {
                execute("CREATE TABLE t (v LONG, d DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" + walSuffix());
                // v 1 to rows, d -1.0 to -rows.
                execute("INSERT INTO t SELECT x, -x::DOUBLE, timestamp_sequence('2024-01-01', 1_000_000) FROM long_sequence(" + rows + ")");
                drainWalIfNeeded();
                execute("ALTER TABLE t ADD COLUMN k INT");
                execute("INSERT INTO t (v, d, ts, k) VALUES (1_000_000, 0.5, '2024-01-02T00:00:00Z', 1)");
                drainWalIfNeeded();
                final long sum = (long) rows * (rows + 1) / 2;
                final String expected = "k\tc\tcv\ts\tmn\tmx\ta\tsd\n"
                        + "null\t" + rows + '\t' + rows + '\t' + sum + "\t1\t" + rows + '\t' + ((rows + 1) / 2.0) + '\t' + (-(double) sum) + '\n'
                        + "1\t1\t1\t1000000\t1000000\t1000000\t1000000.0\t0.5\n";
                final String sql = "SELECT k, count() c, count(v) cv, sum(v) s, min(v) mn, max(v) mx, avg(v) a, sum(d) sd FROM t ORDER BY k";
                assertVectorized(sqlExecutionContext, sql, expected);
                assertMatchesNonVectorized(sqlExecutionContext, sql);
                // Each aggregate alone, so that no other one puts the NULL key in the map first.
                assertVectorized(sqlExecutionContext, "SELECT k, sum(v) s FROM t ORDER BY k", "k\ts\nnull\t" + sum + "\n1\t1000000\n");
                assertVectorized(sqlExecutionContext, "SELECT k, count() c FROM t ORDER BY k", "k\tc\nnull\t" + rows + "\n1\t1\n");
                execute("DROP TABLE t");
            }
        });
    }

    @Test
    public void testNullKeyOverMidPartitionKeyColumnTop() throws Exception {
        // The key columns appear in the middle of the first partition: its first five rows are column
        // tops, the rows after them hold keys, NULL among them. Native frames split at the column
        // top; Parquet row groups of four rows hold column tops only, then both kinds of rows.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (v LONG, d DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" + walSuffix());
            execute("INSERT INTO t SELECT x, x / 2.0, timestamp_sequence('2024-01-01', 3_600_000_000) FROM long_sequence(5)");
            drainWalIfNeeded();
            execute("ALTER TABLE t ADD COLUMN k INT");
            execute("ALTER TABLE t ADD COLUMN sym SYMBOL");
            execute("""
                    INSERT INTO t (v, d, ts, k, sym) VALUES
                        (10, 5.0, '2024-01-01T05:00:00Z', 1, 'a'),
                        (20, 10.0, '2024-01-01T06:00:00Z', 2, 'b'),
                        (30, 15.0, '2024-01-01T07:00:00Z', NULL, NULL),
                        (40, 20.0, '2024-01-01T08:00:00Z', 1, 'a'),
                        (100, 50.0, '2024-01-02T00:00:00Z', 2, 'b')
                    """);
            drainWalIfNeeded();
            // The NULL key: v 1 to 5, the column tops, and 30; key 1 (a): 10 and 40; key 2 (b): 20 and 100.
            final String expected = """
                    %s\tc\ts\ta\tmn\tmx
                    %s\t6\t45\t7.5\t0.5\t15.0
                    %s\t2\t50\t25.0\t5.0\t20.0
                    %s\t2\t120\t60.0\t10.0\t50.0
                    """;
            final String columns = ", count() c, sum(v) s, avg(v) a, min(d) mn, max(d) mx FROM t ORDER BY 1";
            for (boolean isParquet : new boolean[]{false, true}) {
                if (isParquet) {
                    execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                    drainWalIfNeeded();
                    assertQuery("SELECT name, isParquet FROM table_partitions('t')")
                            .noRandomAccess()
                            .expectSize()
                            .noLeakCheck()
                            .returns("""
                                    name\tisParquet
                                    2024-01-01\ttrue
                                    2024-01-02\tfalse
                                    """);
                }
                // Whole partitions in a frame, then frames of three rows, which cut the column tops and
                // the rows with keys apart and together.
                for (int frameRows : new int[]{1_000_000, 3}) {
                    sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                    try {
                        assertVectorized(sqlExecutionContext, "SELECT k" + columns, expected.formatted("k", "null", "1", "2"));
                        assertVectorized(sqlExecutionContext, "SELECT sym" + columns, expected.formatted("sym", "", "a", "b"));
                        assertVectorized(sqlExecutionContext, "SELECT k, sum(v) s FROM t ORDER BY k", """
                                k\ts
                                null\t45
                                1\t50
                                2\t120
                                """);
                        assertMatchesNonVectorized(sqlExecutionContext, "SELECT k" + columns);
                        assertMatchesNonVectorized(sqlExecutionContext, "SELECT sym" + columns);
                    } finally {
                        sqlExecutionContext.restoreToDefaultPageFrameSizes();
                    }
                }
            }
        });
    }

    @Test
    public void testNullKeyBufferFreedWhenMapAllocationFails() throws Exception {
        // When the first per-worker map's or a later one's allocation fails, the failed factory frees
        // the maps it has allocated, which assertMemoryLeak checks, and the query compiles once the maps
        // allocate again.
        final AtomicInteger allocations = new AtomicInteger();
        final AtomicInteger failingAllocation = new AtomicInteger(-1);
        configOverrideRostiAllocFacade(new RostiAllocFacadeImpl() {
            @Override
            public long alloc(ColumnTypes types, long capacity) {
                if (allocations.getAndIncrement() == failingAllocation.get()) {
                    return 0;
                }
                return super.alloc(types, capacity);
            }
        });
        assertMemoryLeak(() -> {
            createColumnTopTable("t", false, 0);
            final String sql = "SELECT k, count() c, sum(l) s FROM t ORDER BY k";
            try (SqlExecutionContext context = new SqlExecutionContextImpl(engine, 4)
                    .with(securityContext, bindVariableService, null, -1, circuitBreaker)) {
                for (int failAt : new int[]{0, 2}) {
                    allocations.set(0);
                    failingAllocation.set(failAt);
                    try {
                        engine.select(sql, context).close();
                        Assert.fail("expected the map allocation to fail");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.getMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "could not allocate rosti hash table");
                    }
                    Assert.assertEquals(failAt + 1, allocations.get());
                }
                failingAllocation.set(-1);
            }
            assertMatchesNonVectorized(sqlExecutionContext, sql);
        });
    }

    @Test
    public void testNullKeyOutOfMemory() throws Exception {
        // Every map allocation fails while a query runs. The live keys of the first partition fill
        // the map up to its growth threshold, about 896 keys at the default capacity of 1_024, so
        // that the NULL key of the second partition's column tops, which comes last, needs the map
        // to grow. The sweep crosses the threshold: below it the query returns every key, above it
        // the query fails with an out-of-memory error, whichever key needs the map to grow.
        final AtomicBoolean isOomArmed = new AtomicBoolean(true);
        configOverrideRostiAllocFacade(new RostiAllocFacadeImpl() {
            @Override
            public void clear(long pRosti) {
                if (isOomArmed.get()) {
                    Rosti.enableOOMOnMalloc();
                }
                super.clear(pRosti);
            }

            @Override
            public boolean reset(long pRosti, int toSize) {
                Rosti.disableOOMOnMalloc();
                return super.reset(pRosti, toSize);
            }
        });
        assertMemoryLeak(() -> {
            boolean hasFailed = false;
            boolean hasSucceeded = false;
            for (int liveKeys = 880; liveKeys <= 904; liveKeys++) {
                execute("CREATE TABLE t (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" + walSuffix());
                execute("INSERT INTO t SELECT x, timestamp_sequence('2024-01-02', 1_000_000) FROM long_sequence(8)");
                execute("ALTER TABLE t ADD COLUMN k INT");
                // An earlier partition, written after the key column exists, so that the frames of
                // the live keys come before the frame of column tops.
                execute("INSERT INTO t (v, ts, k) SELECT x, timestamp_sequence('2024-01-01', 1_000_000), x::INT FROM long_sequence(" + liveKeys + ")");
                drainWalIfNeeded();
                final String sql = "SELECT k, count() c, sum(v) s FROM t ORDER BY k";
                printSql("EXPLAIN " + sql);
                TestUtils.assertContains(sink, "GroupBy vectorized: true");
                // The NULL key has the 8 column top rows, whose values are 1 to 8.
                final StringSink expected = new StringSink();
                expected.put("k\tc\ts\nnull\t8\t36\n");
                for (int key = 1; key <= liveKeys; key++) {
                    expected.put(key).put("\t1\t").put(key).put('\n');
                }
                // A new factory, whose map has the default capacity, runs the query once: a closed
                // cursor shrinks the map.
                try (RecordCursorFactory factory = select(sql)) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        CursorPrinter.println(cursor, factory.getMetadata(), sink);
                        TestUtils.assertEquals(expected, sink);
                        hasSucceeded = true;
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "could not resize rosti hash table");
                        hasFailed = true;
                    }
                }
                // The table still works once the map can grow again.
                if (liveKeys == 904) {
                    isOomArmed.set(false);
                    assertMatchesNonVectorized(sqlExecutionContext, sql);
                    isOomArmed.set(true);
                }
                execute("DROP TABLE t");
            }
            Assert.assertTrue(hasSucceeded);
            Assert.assertTrue(hasFailed);
        });
    }

    @Test
    public void testNullKeyOverAllNullParquetRowGroup() throws Exception {
        // Parquet row groups of four rows: the first and the last row group of the converted
        // partition have no non-NULL key.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k INT, l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" + walSuffix());
            execute("""
                    INSERT INTO t VALUES
                        (NULL, -10, '2024-01-01T00:00:00Z'),
                        (NULL, -20, '2024-01-01T01:00:00Z'),
                        (NULL, NULL, '2024-01-01T02:00:00Z'),
                        (NULL, -30, '2024-01-01T03:00:00Z'),
                        (1, 100, '2024-01-01T04:00:00Z'),
                        (NULL, 5, '2024-01-01T05:00:00Z'),
                        (2, 200, '2024-01-01T06:00:00Z'),
                        (1, 300, '2024-01-01T07:00:00Z'),
                        (NULL, 7, '2024-01-01T08:00:00Z'),
                        (2, 1, '2024-01-02T00:00:00Z')
                    """);
            drainWalIfNeeded();
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalIfNeeded();
            assertQuery("SELECT name, isParquet FROM table_partitions('t')")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            name\tisParquet
                            2024-01-01\ttrue
                            2024-01-02\tfalse
                            """);
            // NULL key: -10, -20, NULL, -30, 5 and 7; key 1: 100 and 300; key 2: 200 and 1.
            final String expected = """
                    k\tc\tcl\ts\ta\tmn\tmx
                    null\t6\t5\t-48\t-9.6\t-30\t7
                    1\t2\t2\t400\t200.0\t100\t300
                    2\t2\t2\t201\t100.5\t1\t200
                    """;
            final String sql = "SELECT k, count() c, count(l) cl, sum(l) s, avg(l) a, min(l) mn, max(l) mx FROM t ORDER BY k";
            assertVectorized(sqlExecutionContext, sql, expected);
            assertVectorized(sqlExecutionContext, "SELECT k, avg(l) a FROM t ORDER BY k", """
                    k\ta
                    null\t-9.6
                    1\t200.0
                    2\t100.5
                    """);
            assertMatchesNonVectorized(sqlExecutionContext, sql);
        });
    }

    @Test
    public void testNullKeyOverKeyAndValueColumnTops() throws Exception {
        // The key and the value column are both column tops in the first partition: the NULL key
        // has rows, all of whose values are NULL, so its aggregates are NULL and its count is 2.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" + walSuffix());
            execute("INSERT INTO t VALUES (1, '2024-01-01T00:00:00Z'), (2, '2024-01-01T01:00:00Z')");
            execute("ALTER TABLE t ADD COLUMN k INT");
            execute("ALTER TABLE t ADD COLUMN kts TIMESTAMP");
            execute("ALTER TABLE t ADD COLUMN v LONG");
            execute("ALTER TABLE t ADD COLUMN d DOUBLE");
            execute("""
                    INSERT INTO t (l, ts, k, kts, v, d) VALUES
                        (3, '2024-01-02T00:00:00Z', 1, '2024-01-02T05:00:00Z', 5, 0.5),
                        (4, '2024-01-02T01:00:00Z', 1, '2024-01-02T05:30:00Z', 7, 1.25)
                    """);
            drainWalIfNeeded();
            assertVectorized(sqlExecutionContext, "SELECT k, sum(v) s FROM t ORDER BY k", """
                    k\ts
                    null\tnull
                    1\t12
                    """);
            assertVectorized(sqlExecutionContext, "SELECT k, avg(v) a, max(v) mx, min(d) mn, sum(d) sd, count(v) cv FROM t ORDER BY k", """
                    k\ta\tmx\tmn\tsd\tcv
                    null\tnull\tnull\tnull\tnull\t0
                    1\t6.0\t7\t0.5\t1.75\t2
                    """);
            assertVectorized(sqlExecutionContext, "SELECT k, count() c, sum(v) s FROM t ORDER BY k", """
                    k\tc\ts
                    null\t2\tnull
                    1\t2\t12
                    """);
            assertVectorized(sqlExecutionContext, "SELECT hour(kts) h, sum(v) s, avg(d) a FROM t ORDER BY h", """
                    h\ts\ta
                    null\tnull\tnull
                    5\t12\t0.875
                    """);
            for (String key : new String[]{"k", "hour(kts) h"}) {
                for (String aggregate : new String[]{"sum(v)", "avg(v)", "max(v)", "min(d)", "nsum(d)", "ksum(d)", "count(d)"}) {
                    assertMatchesNonVectorized(sqlExecutionContext, "SELECT " + key + ", " + aggregate + " v FROM t ORDER BY 1");
                }
            }
        });
    }

    @Test
    public void testNullKeyOverKeyAndShortValueColumnTops() throws Exception {
        // The key and a SHORT value column are both column tops in the first partition. The
        // vectorized GROUP BY reads a SHORT column top as no value under every key (the
        // non-vectorized one reads it as 0, so it is no oracle here): the NULL key has two rows and
        // no value, so its avg(s) is NULL, as it is in the twin table t2, whose first partition has
        // an explicit NULL key over the same SHORT column top.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" + walSuffix());
            execute("INSERT INTO t VALUES (1, '2024-01-01T00:00:00Z'), (2, '2024-01-01T01:00:00Z')");
            execute("ALTER TABLE t ADD COLUMN k INT");
            execute("ALTER TABLE t ADD COLUMN kts TIMESTAMP");
            execute("ALTER TABLE t ADD COLUMN s SHORT");
            execute("CREATE TABLE t2 (l LONG, k INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" + walSuffix());
            execute("INSERT INTO t2 VALUES (1, NULL, '2024-01-01T00:00:00Z'), (2, NULL, '2024-01-01T01:00:00Z')");
            execute("ALTER TABLE t2 ADD COLUMN s SHORT");
            execute("""
                    INSERT INTO t (l, ts, k, kts, s) VALUES
                        (3, '2024-01-02T00:00:00Z', 1, '2024-01-02T05:00:00Z', 10::SHORT),
                        (4, '2024-01-02T01:00:00Z', 1, '2024-01-02T05:30:00Z', 30::SHORT)
                    """);
            execute("""
                    INSERT INTO t2 (l, ts, k, s) VALUES
                        (3, '2024-01-02T00:00:00Z', 1, 10::SHORT),
                        (4, '2024-01-02T01:00:00Z', 1, 30::SHORT)
                    """);
            drainWalIfNeeded();
            // Key 1: s 10 and 30.
            final String expected = """
                    k\ta
                    null\tnull
                    1\t20.0
                    """;
            assertVectorized(sqlExecutionContext, "SELECT k, avg(s) a FROM t ORDER BY k", expected);
            assertVectorized(sqlExecutionContext, "SELECT k, avg(s) a FROM t2 ORDER BY k", expected);
            final String expectedWithCount = """
                    k\tc\ta
                    null\t2\tnull
                    1\t2\t20.0
                    """;
            assertVectorized(sqlExecutionContext, "SELECT k, count() c, avg(s) a FROM t ORDER BY k", expectedWithCount);
            assertVectorized(sqlExecutionContext, "SELECT k, count() c, avg(s) a FROM t2 ORDER BY k", expectedWithCount);
            // hour() of a TIMESTAMP column top: kts 05:00 and 05:30 make hour 5.
            assertVectorized(sqlExecutionContext, "SELECT hour(kts) h, avg(s) a FROM t ORDER BY h", """
                    h\ta
                    null\tnull
                    5\t20.0
                    """);
        });
    }

    @Test
    public void testNullKeyOverKeyColumnTop() throws Exception {
        // The first partition precedes the key column: its rows have the NULL key, as does the last
        // row of the second partition.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (d DOUBLE, i INT, l LONG, s SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" + walSuffix());
            execute("""
                    INSERT INTO t VALUES
                        (1.5, 10, 10, 10::SHORT, '2024-01-01T00:00:00Z'),
                        (2.5, 20, -40, 20::SHORT, '2024-01-01T01:00:00Z'),
                        (NULL, NULL, NULL, 30::SHORT, '2024-01-01T02:00:00Z')
                    """);
            execute("ALTER TABLE t ADD COLUMN k INT");
            execute("ALTER TABLE t ADD COLUMN sym SYMBOL");
            execute("""
                    INSERT INTO t (d, i, l, s, ts, k, sym) VALUES
                        (100.5, 100, 100, 100::SHORT, '2024-01-02T00:00:00Z', 1, 'a'),
                        (300.5, 300, 300, 300::SHORT, '2024-01-02T01:00:00Z', 1, 'a'),
                        (60.5, 60, 60, 60::SHORT, '2024-01-02T02:00:00Z', NULL, NULL)
                    """);
            drainWalIfNeeded();
            // NULL key: d 1.5, 2.5, NULL, 60.5; i 10, 20, NULL, 60; l 10, -40, NULL, 60; s 10, 20, 30, 60.
            // Key 1 (or 'a'): d 100.5, 300.5; i, l and s 100, 300.
            final String columns = "count() c, avg(l) al, sum(l) sl, avg(i) ai, sum(i) si, avg(s) a_s, sum(s) ss,"
                    + " avg(d) ad, sum(d) sd, min(l) mnl, max(l) mxl, count(l) cl";
            final String expected = """
                    %s\tc\tal\tsl\tai\tsi\ta_s\tss\tad\tsd\tmnl\tmxl\tcl
                    %s\t4\t10.0\t30\t30.0\t90\t30.0\t120\t21.5\t64.5\t-40\t60\t3
                    %s\t2\t200.0\t400\t200.0\t400\t200.0\t400\t200.5\t401.0\t100\t300\t2
                    """;
            assertVectorized(sqlExecutionContext, "SELECT k, " + columns + " FROM t ORDER BY k", expected.formatted("k", "null", "1"));
            assertVectorized(sqlExecutionContext, "SELECT sym, " + columns + " FROM t ORDER BY sym", expected.formatted("sym", "", "a"));
            // Each aggregate alone, so that none of the others puts the NULL key in the map first.
            assertVectorized(sqlExecutionContext, "SELECT k, avg(l) v FROM t ORDER BY k", """
                    k\tv
                    null\t10.0
                    1\t200.0
                    """);
            assertVectorized(sqlExecutionContext, "SELECT k, avg(i) v FROM t ORDER BY k", """
                    k\tv
                    null\t30.0
                    1\t200.0
                    """);
            assertVectorized(sqlExecutionContext, "SELECT k, avg(s) v FROM t ORDER BY k", """
                    k\tv
                    null\t30.0
                    1\t200.0
                    """);
            assertVectorized(sqlExecutionContext, "SELECT k, sum(d) v FROM t ORDER BY k", """
                    k\tv
                    null\t64.5
                    1\t401.0
                    """);
        });
    }

    private static String allAggregates(String key, String table) {
        return allAggregates(key, table, false);
    }

    private static String allAggregates(String key, String table, boolean isValueColumnTop) {
        final StringBuilder sql = new StringBuilder("SELECT ").append(key);
        for (int i = 0; i < AGGREGATES.length; i++) {
            if (!isValueColumnTop || !isShortColumnTopSensitive(key, AGGREGATES[i])) {
                sql.append(", ").append(AGGREGATES[i]).append(" a").append(i);
            }
        }
        return sql.append(" FROM ").append(table).append(" ORDER BY 1").toString();
    }

    // SHORT has no NULL: the non-vectorized GROUP BY reads a SHORT column top as 0, where the
    // vectorized one reads it as no value, under every key. So their avg(s) differs over SHORT
    // column tops, and so do sum(s), min(s) and max(s) of a key all of whose rows are column tops,
    // as the NULL hour() key's are.
    private static boolean isShortColumnTopSensitive(String key, String aggregate) {
        return aggregate.equals("avg(s)") || (key.startsWith("hour(") && aggregate.endsWith("(s)"));
    }

    private void assertVectorized(SqlExecutionContext context, String sql, String expected) throws Exception {
        context.setParallelGroupByEnabled(true);
        assertQuery(sql)
                .withContext(context)
                .noLeakCheck()
                .expectSize()
                .withPlanContaining("GroupBy vectorized: true")
                .returns(expected);
    }

    private void assertMatchesNonVectorized(SqlExecutionContext context, String sql) throws Exception {
        // The non-vectorized GROUP BY gives the expected result.
        final StringSink expected = new StringSink();
        sqlExecutionContext.setParallelGroupByEnabled(false);
        try {
            final StringSink plan = new StringSink();
            engine.print("EXPLAIN " + sql, plan, sqlExecutionContext);
            Assert.assertFalse(plan.toString(), plan.toString().contains("vectorized: true"));
            engine.print(sql, expected, sqlExecutionContext);
        } finally {
            sqlExecutionContext.setParallelGroupByEnabled(true);
        }
        assertVectorized(context, sql, expected.toString());
    }

    // Creates a table whose first partitions precede the key columns k, sym, kts and ktsn, and,
    // when isValueColumnTop, the value columns as well; the last partition has them all.
    private void createColumnTopTable(String table, boolean isValueColumnTop, int extraColumnTopPartitions) throws Exception {
        final String valueColumns = "d DOUBLE, i INT, l LONG, s SHORT, dt DATE, l256 LONG256, tsv TIMESTAMP, tsvn TIMESTAMP_NS";
        if (isValueColumnTop) {
            execute("CREATE TABLE " + table + " (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" + walSuffix());
        } else {
            execute("CREATE TABLE " + table + " (" + valueColumns + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" + walSuffix());
        }
        final String columnTopInsert = isValueColumnTop
                ? "INSERT INTO " + table + " (ts) SELECT "
                : "INSERT INTO " + table + " (d, i, l, s, dt, l256, tsv, tsvn, ts) SELECT " + VALUES + ", ";
        // 3_000 rows before the key columns exist: a frame of column tops longer than the NULL keys
        // that the GROUP BY aggregates it with at a time.
        execute(columnTopInsert + "timestamp_sequence('2024-01-01', 10_000_000) FROM long_sequence(3_000)");
        for (int day = 1; day <= extraColumnTopPartitions; day++) {
            execute(columnTopInsert + "timestamp_sequence('2023-12-0" + day + "', 10_000_000) FROM long_sequence(1_500)");
        }
        drainWalIfNeeded();
        if (isValueColumnTop) {
            for (String column : valueColumns.split(", ")) {
                execute("ALTER TABLE " + table + " ADD COLUMN " + column);
            }
        }
        execute("ALTER TABLE " + table + " ADD COLUMN k INT");
        execute("ALTER TABLE " + table + " ADD COLUMN sym SYMBOL");
        execute("ALTER TABLE " + table + " ADD COLUMN kts TIMESTAMP");
        execute("ALTER TABLE " + table + " ADD COLUMN ktsn TIMESTAMP_NS");
        // Keys 0, 1, 2 and NULL; the hour() keys are never NULL.
        execute("INSERT INTO " + table + " (d, i, l, s, dt, l256, tsv, tsvn, ts, k, sym, kts, ktsn) SELECT " + VALUES + """
                ,
                timestamp_sequence('2024-01-02', 10_000_000),
                CASE WHEN x % 5 = 0 THEN NULL ELSE (x % 3)::INT END,
                CASE WHEN x % 5 = 0 THEN NULL ELSE 's' || (x % 3) END,
                timestamp_sequence('2024-01-03', 1_800_000_000),
                timestamp_sequence('2024-01-03', 1_800_000_000)::TIMESTAMP_NS
                FROM long_sequence(2_000)""");
        drainWalIfNeeded();
    }

    private void drainWalIfNeeded() {
        if (isWal) {
            drainWalQueue();
        }
    }

    private String walSuffix() {
        return isWal ? " WAL" : " BYPASS WAL";
    }
}
