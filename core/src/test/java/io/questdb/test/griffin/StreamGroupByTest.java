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
package io.questdb.test.griffin;

import com.sun.management.ThreadMXBean;
import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.PageFrameReduceDispatcher;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.griffin.engine.groupby.GroupByRecordCursorFactory;
import io.questdb.griffin.engine.groupby.StreamGroupByMap;
import io.questdb.griffin.engine.groupby.StreamGroupByRecordCursorFactory;
import io.questdb.griffin.engine.join.AsyncWindowJoinFastRecordCursorFactory;
import io.questdb.griffin.engine.join.AsyncWindowJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHorizonJoinProjectionRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncStreamGroupByAtom;
import io.questdb.griffin.engine.table.AsyncStreamGroupByRecordCursorFactory;
import io.questdb.jit.JitUtil;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.continuation.Fiber;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.DirectLongList;
import io.questdb.std.FilesFacade;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.IntHashSet;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.datetime.millitime.Dates;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.CountingSqlExecutionCircuitBreaker;
import io.questdb.test.tools.LimitedMemoryTracker;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@RunWith(Parameterized.class)
public class StreamGroupByTest extends AbstractCairoTest {
    // A keyed query, as only keys take span maps and lanes, see FrameHolder.
    private static final String HELD_FRAME_SQL = "SELECT timestamp_floor('1s', ts) bucket, x % 3 k, count() c, sum(x) s FROM t WHERE test_latched_counter()";
    private static final String SKEWED_KEYS_SQL = "SELECT timestamp_floor('1m', ts) bucket, k, count(*), sum(v) FROM trades";
    private final int frameRows;
    private final boolean parallel;
    private final int workers;

    public StreamGroupByTest(boolean parallel, int workers, int frameRows) {
        this.parallel = parallel;
        this.workers = workers;
        this.frameRows = frameRows;
    }

    @Parameterized.Parameters(name = "parallel={0}, workers={1}, frameRows={2}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {false, 1, 17},
                {true, 0, 17},
                {true, 1, 1},
                {true, 4, 17},
                {true, 4, 256}
        });
    }

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, Boolean.toString(parallel));
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, frameRows);
        super.setUp();
    }

    @Test
    public void testAggregatesAcrossFrames() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            assertMatchesHash(compiler, context,
                    "select timestamp_floor('1m', ts) bucket, count(*), sum(v), sum(d), avg(v), avg(d), "
                            + "min(v), max(v), min(d), max(d), first(v), last(v), first_not_null(v), last_not_null(v), "
                            + "first(d), last(d), first_not_null(d), last_not_null(d) "
                            + "from $table order by bucket");
        });
    }

    @Test
    public void testAllNullGroups() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("create table trades as (select "
                    + "timestamp_sequence('2024-01-01'::timestamp, 1_000_000L) ts, "
                    + "case when x > 60 and x <= 120 then x end v "
                    + "from long_sequence(181)) timestamp(ts) partition by day", context);
            engine.execute("create table hash_input (ts timestamp, v long)", context);
            engine.execute("insert into hash_input select * from trades", context);
            assertMatchesHash(compiler, context,
                    "select timestamp_floor('1m', ts) bucket, count(v), sum(v), avg(v), min(v), max(v), "
                            + "first(v), last(v), first_not_null(v), last_not_null(v) from $table order by bucket");
        });
    }

    @Test
    public void testAlternatingQueriesDoNotAllocate() throws Exception {
        Assume.assumeTrue(parallel && workers == 0);
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            try (
                    RecordCursorFactory sums = compiler.compile("SELECT timestamp_floor('1m', ts) bucket, sym, sum(v) FROM trades ORDER BY bucket, sym", context).getRecordCursorFactory();
                    RecordCursorFactory firsts = compiler.compile("SELECT timestamp_floor('1m', ts) bucket, sym, count(*), first(d) FROM trades ORDER BY bucket, sym", context).getRecordCursorFactory();
                    TestUtils.ThreadMetricsScope<ThreadMXBean> scope = TestUtils.threadAllocationScope()
            ) {
                Assert.assertTrue(getGroupBy(sums) instanceof AsyncStreamGroupByRecordCursorFactory);
                Assert.assertTrue(getGroupBy(firsts) instanceof AsyncStreamGroupByRecordCursorFactory);
                drain(sums, context);
                drain(firsts, context);
                final ThreadMXBean bean = scope.getBean();
                final long threadId = Thread.currentThread().threadId();
                final long before = bean.getThreadAllocatedBytes(threadId);
                for (int i = 0; i < 100; i++) {
                    drain(sums, context);
                    drain(firsts, context);
                }
                final long allocated = bean.getThreadAllocatedBytes(threadId) - before;
                Assert.assertTrue("200 runs allocated " + allocated + " bytes", allocated < 200 * 4 * 1024);
            }
        });
    }

    @Test
    public void testHighCardinalityBucketsMergeShards() throws Exception {
        // Every frame of a bucket brings more keys than the threshold, so a span map shards in any schedule.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 100);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 1_000);
        run((engine, compiler, context) -> {
            createHighCardinalityTables(engine, context);
            for (String filter : new String[]{"", " WHERE v % 7 <> 0"}) {
                for (String keys : new String[]{"k", "s", "k, sym"}) {
                    for (String order : new String[]{"bucket", "bucket DESC"}) {
                        final String sql = "SELECT timestamp_floor('10s', ts) bucket, " + keys + ", count(*), sum(v), max(v) FROM $table"
                                + filter + " ORDER BY " + order + ", " + keys;
                        assertMatchesHash(compiler, context, sql);
                        assertShardMerge(compiler, context, sql);
                    }
                }
            }
            // A selective filter on a parquet partition reads the rows through late materialization.
            for (String table : new String[]{"trades", "hash_input"}) {
                engine.execute("INSERT INTO " + table + " VALUES ('2024-01-02', 1, '1', 1, 'A')", context);
            }
            TestUtils.drainWalQueue(engine);
            convertToParquet(engine, context, "2024-01-01");
            for (String keys : new String[]{"k", "s"}) {
                final String sql = "SELECT timestamp_floor('10s', ts) bucket, " + keys + ", count(*), sum(v) FROM $table WHERE v % 3 = 0 ORDER BY bucket, " + keys;
                assertMatchesHash(compiler, context, sql);
                assertShardMerge(compiler, context, sql);
            }
        });
    }

    @Test
    public void testWorkersShareSinkClasses() throws Exception {
        Assume.assumeTrue(parallel && workers > 1);
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            try (RecordCursorFactory factory = compiler.compile("SELECT ts, sym::VARCHAR k, count(*) c, sum(d) s FROM trades SAMPLE BY 1m", context).getRecordCursorFactory()) {
                final AsyncStreamGroupByAtom atom = ((AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory)).getAtom();
                for (int i = 0; i < workers; i++) {
                    Assert.assertTrue(atom.getMap(i).hasSinkClassesOf(atom.getMap(-1)));
                }
                Assert.assertTrue(atom.getCollectorMap().hasSinkClassesOf(atom.getMap(-1)));
            }
        });
    }

    @Test
    public void testVarcharKeysRepeatAcrossFrameEnds() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 100);
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE trades AS (SELECT timestamp_sequence('2024-01-01', 1_000L) ts, (x % 150)::VARCHAR s, x v, x::DECIMAL(38, 2) d "
                    + "FROM long_sequence(6_000)) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("CREATE TABLE hash_input (ts TIMESTAMP, s VARCHAR, v LONG, d DECIMAL(38, 2))", context);
            engine.execute("INSERT INTO hash_input SELECT * FROM trades", context);
            for (String bucket : new String[]{"300T", "450T", "1s"}) {
                for (String filter : new String[]{"", " WHERE v % 7 <> 0"}) {
                    assertMatchesHash(compiler, context, "SELECT timestamp_floor('" + bucket + "', ts) bucket, s, count(*), sum(v), "
                            + "first(v), last(v), sum(d), max(d) FROM $table" + filter + " ORDER BY bucket, s");
                }
            }
        });
    }

    @Test
    public void testDecimalAggregatesAcrossWorkers() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE trades AS (SELECT timestamp_sequence('2024-01-01', 1_000L) ts, (x % 50)::SYMBOL sym, "
                    + "x::DECIMAL(38, 2) d128, x::DECIMAL(76, 2) d256 FROM long_sequence(20_000)) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("CREATE TABLE hash_input (ts TIMESTAMP, sym SYMBOL, d128 DECIMAL(38, 2), d256 DECIMAL(76, 2))", context);
            engine.execute("INSERT INTO hash_input SELECT * FROM trades", context);
            for (String bucket : new String[]{"7T", "128T", "1s"}) {
                assertMatchesHash(compiler, context, "SELECT timestamp_floor('" + bucket + "', ts) bucket, sym, count(*), "
                        + "sum(d128), max(d128), first(d128), sum(d256), min(d256), last(d256) FROM $table ORDER BY bucket, sym");
            }
        });
    }

    @Test
    public void testGappedBucketsAggregateInBatches() throws Exception {
        // Without workers, the owner's breaker sees every reduce.
        Assume.assumeTrue(parallel && workers == 0);
        run((engine, compiler, context) -> {
            final int rows = 20_000;
            engine.execute("CREATE TABLE trades AS (SELECT timestamp_sequence('2024-01-01', 1_000L) ts, x v, x::DOUBLE d, "
                    + "(x % 3)::SYMBOL sym FROM long_sequence(" + rows + ")) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("CREATE TABLE hash_input (ts TIMESTAMP, v LONG, d DOUBLE, sym SYMBOL)", context);
            engine.execute("INSERT INTO hash_input SELECT * FROM trades", context);
            final String sql = "SELECT timestamp_floor('$bucket', ts) bucket, count(*), count(d), sum(v), sum(d), avg(d), min(d), max(d), first(v), last(v), "
                    + "first_not_null(d), last_not_null(d), avg(d * 2) FROM $table WHERE v % 2 = 0 ORDER BY bucket";
            for (String bucket : new String[]{"1d", "1s", "7T"}) {
                assertMatchesHash(compiler, context, sql.replace("$bucket", bucket));
            }
            final CountingSqlExecutionCircuitBreaker breaker = new CountingSqlExecutionCircuitBreaker(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
            try (SqlExecutionContextImpl counted = new SqlExecutionContextImpl(engine, 1).with(context.getSecurityContext(), null, null, -1, breaker)) {
                final String daySql = sql.replace("$bucket", "1d").replace("$table", "trades").replace(" ORDER BY bucket", "");
                try (RecordCursorFactory factory = compiler.compile(daySql, counted).getRecordCursorFactory()) {
                    assertStream(factory, true);
                    final long before = breaker.getCheckCount();
                    Assert.assertFalse(read(factory, counted).isEmpty());
                    final long checks = breaker.getCheckCount() - before;
                    Assert.assertTrue(checks + " checks", checks < rows);
                }
            }
        });
    }

    @Test
    public void testGroupingSetsDoNotStream() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE t AS (SELECT timestamp_sequence('2024-01-01', 600_000_000L) ts, (x % 3)::SYMBOL sym, x v FROM long_sequence(100)) "
                    + "TIMESTAMP(ts) PARTITION BY DAY", context);
            for (String fill : new String[]{"", " FILL(NULL)"}) {
                final String rollup = "SELECT ts, sym, sum(v) s FROM t SAMPLE BY 1h ROLLUP(sym)" + fill + " ORDER BY ts, sym";
                final String reference = "SELECT * FROM (SELECT ts, sym, sum(v) s FROM t SAMPLE BY 1h" + fill
                        + " UNION ALL SELECT ts, NULL::SYMBOL sym, sum(v) s FROM t SAMPLE BY 1h" + fill + ") ORDER BY ts, sym";
                try (RecordCursorFactory actual = compiler.compile(rollup, context).getRecordCursorFactory();
                     RecordCursorFactory expected = compiler.compile(reference, context).getRecordCursorFactory()) {
                    assertStream(actual, false);
                    TestUtils.assertEquals(read(expected, context), read(actual, context));
                }
            }
        });
    }

    @Test
    public void testHighCardinalityBucketsEndInsideFrames() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 100);
        run((engine, compiler, context) -> {
            createHighCardinalityTables(engine, context);
            for (String bucket : new String[]{"7T", "128T", "200T", "300T"}) {
                for (String filter : new String[]{"", " WHERE v % 7 <> 0"}) {
                    for (String keys : new String[]{"k", "s"}) {
                        for (String order : new String[]{"bucket", "bucket DESC"}) {
                            assertMatchesHash(compiler, context, "SELECT timestamp_floor('" + bucket + "', ts) bucket, " + keys
                                    + ", count(*), sum(v), first(v), last(v) FROM $table" + filter + " ORDER BY " + order + ", " + keys);
                        }
                    }
                }
            }
            if (frameRows > 100) {
                // No 200-row bucket holds a whole frame.
                assertShardMerge(compiler, context, "SELECT timestamp_floor('200T', ts) bucket, k, count(*), sum(v) FROM $table ORDER BY bucket, k");
            }
        });
    }

    @Test
    public void testHighCardinalityBucketsPresizeShards() throws Exception {
        // Small maps start below the keys of a shard, so the merge and the next execution presize the shards.
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_KEY_CAPACITY, 4);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 100);
        run((engine, compiler, context) -> {
            createHighCardinalityTables(engine, context);
            final String sql = "SELECT timestamp_floor('10s', ts) bucket, k, count(*), sum(v) FROM $table ORDER BY bucket, k";
            assertMatchesHash(compiler, context, sql);
            assertShardMerge(compiler, context, sql);
        });
    }

    @Test
    public void testHugeBucketDoesNotBurdenLaterBuckets() throws Exception {
        Assume.assumeTrue(parallel);
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_KEY_CAPACITY, 4);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 50);
        run((engine, compiler, context) -> {
            createSkewedKeysTable(engine, context);
            try (RecordCursorFactory factory = compiler.compile(SKEWED_KEYS_SQL, context).getRecordCursorFactory()) {
                final AsyncStreamGroupByAtom atom = ((AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory)).getAtom();
                try (RecordCursor cursor = factory.getCursor(context)) {
                    Assert.assertTrue(cursor.hasNext());
                    final int hugeBucketMergeCapacity = atom.getMergeShardKeyCapacity();
                    final int hugeBucketShardCapacity = atom.getShardKeyCapacity();
                    // The first row of the second bucket follows its merge.
                    for (int row = 0; row < 60_000; row++) {
                        Assert.assertTrue(cursor.hasNext());
                    }
                    Assert.assertEquals(2, atom.getShardMergeCount());
                    final int mergeCapacity = atom.getMergeShardKeyCapacity();
                    Assert.assertTrue(mergeCapacity + " of " + hugeBucketMergeCapacity, mergeCapacity < hugeBucketMergeCapacity);
                    // Workers pool span maps, and a map that sat out the last merge remembers an earlier one.
                    if (workers == 0) {
                        final int shardCapacity = atom.getShardKeyCapacity();
                        Assert.assertTrue(shardCapacity + " of " + hugeBucketShardCapacity, shardCapacity < hugeBucketShardCapacity);
                    }
                }
            }
        });
    }

    @Test
    public void testShardsHonorDisabledPresize() throws Exception {
        Assume.assumeTrue(parallel);
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_KEY_CAPACITY, 4);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 50);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_PRESIZE_ENABLED, "false");
        run((engine, compiler, context) -> {
            createSkewedKeysTable(engine, context);
            try (RecordCursorFactory factory = compiler.compile(SKEWED_KEYS_SQL, context).getRecordCursorFactory()) {
                final AsyncStreamGroupByAtom atom = ((AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory)).getAtom();
                drain(factory, context);
                Assert.assertEquals(3, atom.getShardMergeCount());
                Assert.assertEquals(0, atom.getShardKeyCapacity());
            }
        });
    }

    @Test
    public void testRowCopiesIgnoreSortValueLimit() throws Exception {
        // The copies stand in for the map of a hash GROUP BY, which this limit never bounded.
        setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_MAX_BYTES, 1_024);
        run((engine, compiler, context) -> {
            createHighCardinalityTables(engine, context);
            final String sql = "SELECT timestamp_floor('10s', ts) bucket, k, sum(v) s FROM $table ORDER BY s DESC, bucket, k";
            assertMatchesHash(compiler, context, sql);
            assertMatchesHash(compiler, context, sql + " LIMIT 10_000");
        });
    }

    @Test
    public void testRowCopiesIgnoreSortValueLimitWithoutEncodedSort() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_MAX_BYTES, 1_024);
        setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, "false");
        run((engine, compiler, context) -> {
            createHighCardinalityTables(engine, context);
            assertMatchesHash(compiler, context,
                    "SELECT timestamp_floor('10s', ts) bucket, k, sum(v) s FROM $table ORDER BY s DESC, bucket, k");
        });
    }

    @Test
    public void testHighCardinalityBucketFailuresAndReuse() throws Exception {
        // Small shards grow while they merge, so a memory limit can fail a merge task on a worker.
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_KEY_CAPACITY, 4);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 100);
        run((engine, compiler, context) -> {
            createHighCardinalityTables(engine, context);
            final String sql = "SELECT timestamp_floor('10s', ts) bucket, s, count(*), sum(v) FROM $table ORDER BY bucket, s";
            final String expected;
            try (RecordCursorFactory factory = compiler.compile(sql.replace("$table", "hash_input"), context).getRecordCursorFactory()) {
                expected = read(factory, context);
            }
            try (RecordCursorFactory factory = compiler.compile(sql.replace("$table", "trades"), context).getRecordCursorFactory();
                 MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(context.getSecurityContext(), 42, MemoryTrackerWorkload.QUERY)) {
                assertStream(getGroupBy(factory), true);
                context.setMemoryTracker(tracker);
                int breaches = 0;
                try {
                    for (long limit = 1L << 26; limit >= 1L << 12; limit -= limit >> 3) {
                        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit);
                        try {
                            Assert.assertEquals(expected, read(factory, context));
                        } catch (CairoException e) {
                            Assert.assertTrue(e.getFlyweightMessage().toString(), e.isOutOfMemory());
                            breaches++;
                        } finally {
                            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                        }
                        Assert.assertEquals(0, tracker.getUsed());
                        Assert.assertEquals(expected, read(factory, context));
                        Assert.assertEquals(0, tracker.getUsed());
                    }
                } finally {
                    context.setMemoryTracker(null);
                }
                Assert.assertTrue(breaches > 0);
            }
            // Cancels the query while the owner waits for the shard merges, the only caller of checkIfTripped(). The
            // workers can finish every merge before the owner waits, and then the query completes uncancelled.
            int cancellations = 0;
            for (int iteration = 0; iteration < 3 || (parallel && cancellations == 0 && iteration < 64); iteration++) {
                final AtomicBoolean isCancelRequested = new AtomicBoolean();
                try (NetworkSqlExecutionCircuitBreaker network = new NetworkSqlExecutionCircuitBreaker(engine,
                        new DefaultSqlExecutionCircuitBreakerConfiguration() {
                            @Override
                            public int getCircuitBreakerThrottle() {
                                return 0;
                            }
                        })) {
                    final CountingSqlExecutionCircuitBreaker breaker = new CountingSqlExecutionCircuitBreaker(network) {
                        @Override
                        public boolean checkIfTripped() {
                            isCancelRequested.set(true);
                            network.cancel();
                            return super.checkIfTripped();
                        }
                    };
                    try (SqlExecutionContextImpl cancellable = new SqlExecutionContextImpl(engine, Math.max(1, workers))
                            .with(context.getSecurityContext(), null, null, -1, breaker);
                         RecordCursorFactory factory = compiler.compile(sql.replace("$table", "trades"), cancellable).getRecordCursorFactory()) {
                        try {
                            Assert.assertEquals(expected, read(factory, cancellable));
                            Assert.assertFalse(isCancelRequested.get());
                        } catch (CairoException e) {
                            Assert.assertTrue(e.getFlyweightMessage().toString(), e.isInterruption());
                            Assert.assertTrue(isCancelRequested.get());
                            cancellations++;
                        }
                        Assert.assertEquals(expected, read(factory, context));
                    }
                }
            }
            Assert.assertEquals(parallel, cancellations > 0);
            for (int trip = 1; trip < 4_096; trip <<= 1) {
                final int checks = trip;
                final AtomicLong count = new AtomicLong();
                try (NetworkSqlExecutionCircuitBreaker network = new NetworkSqlExecutionCircuitBreaker(engine,
                        new DefaultSqlExecutionCircuitBreakerConfiguration() {
                            @Override
                            public int getCircuitBreakerThrottle() {
                                return 0;
                            }
                        })) {
                    final CountingSqlExecutionCircuitBreaker breaker = new CountingSqlExecutionCircuitBreaker(network) {
                        @Override
                        public boolean checkIfTripped() {
                            trip();
                            return super.checkIfTripped();
                        }

                        @Override
                        public void statefulThrowExceptionIfTripped() {
                            trip();
                            super.statefulThrowExceptionIfTripped();
                        }

                        private void trip() {
                            if (count.incrementAndGet() == checks) {
                                network.cancel();
                            }
                        }
                    };
                    try (SqlExecutionContextImpl cancellable = new SqlExecutionContextImpl(engine, Math.max(1, workers))
                            .with(context.getSecurityContext(), null, null, -1, breaker);
                         RecordCursorFactory factory = compiler.compile(sql.replace("$table", "trades"), cancellable).getRecordCursorFactory()) {
                        try {
                            Assert.assertEquals(expected, read(factory, cancellable));
                        } catch (CairoException e) {
                            Assert.assertTrue(e.getFlyweightMessage().toString(), e.isInterruption());
                        }
                        Assert.assertEquals(expected, read(factory, context));
                    }
                }
            }
        });
    }

    @Test
    public void testHighCardinalityBucketMergeErrorReachesOwner() throws Exception {
        Assume.assumeTrue(parallel && workers == 0);
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_KEY_CAPACITY, 4);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 100);
        run((engine, compiler, context) -> {
            createHighCardinalityTables(engine, context);
            final String sql = "SELECT timestamp_floor('10s', ts) bucket, s, count(*), sum(v) FROM $table ORDER BY bucket, s";
            final String expected;
            try (RecordCursorFactory factory = compiler.compile(sql.replace("$table", "hash_input"), context).getRecordCursorFactory()) {
                expected = read(factory, context);
            }
            try (
                    LimitedMemoryTracker tracker = new LimitedMemoryTracker(0);
                    NetworkSqlExecutionCircuitBreaker network = new NetworkSqlExecutionCircuitBreaker(engine, new DefaultSqlExecutionCircuitBreakerConfiguration())
            ) {
                final AtomicBoolean isArmed = new AtomicBoolean();
                final CountingSqlExecutionCircuitBreaker breaker = new CountingSqlExecutionCircuitBreaker(network) {
                    @Override
                    public boolean checkIfTripped() {
                        if (isArmed.compareAndSet(false, true)) {
                            tracker.setLimit(tracker.getUsed() + 1);
                        }
                        return super.checkIfTripped();
                    }
                };
                try (
                        SqlExecutionContextImpl bounded = new SqlExecutionContextImpl(engine, 1).with(context.getSecurityContext(), null, null, -1, breaker);
                        RecordCursorFactory factory = compiler.compile(sql.replace("$table", "trades"), bounded).getRecordCursorFactory()
                ) {
                    assertStream(getGroupBy(factory), true);
                    // The query registry gives a query its own tracker, so this opens the plan below it.
                    Assert.assertTrue(factory instanceof QueryProgress);
                    final RecordCursorFactory plan = factory.getBaseFactory();
                    bounded.setMemoryTracker(tracker);
                    try {
                        try {
                            read(plan, bounded);
                            Assert.fail("expected a shard merge to run out of memory");
                        } catch (CairoException e) {
                            Assert.assertTrue(isArmed.get());
                            Assert.assertTrue(e.getFlyweightMessage().toString(), e.isOutOfMemory());
                            TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        } finally {
                            tracker.setLimit(0);
                        }
                        Assert.assertEquals(0, tracker.getUsed());
                        Assert.assertEquals(0, ((AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory)).getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                        Assert.assertEquals(expected, read(plan, bounded));
                        Assert.assertEquals(0, tracker.getUsed());
                    } finally {
                        bounded.setMemoryTracker(null);
                    }
                }
            }
        });
    }

    @Test
    public void testIntStreamKeySpansFrameSegments() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE trades AS (SELECT timestamp_sequence('2015-01-01', 8_640_000_000_000L) ts, x v, "
                    + "('s' || (x % 3))::symbol sym FROM long_sequence(40)) TIMESTAMP(ts)", context);
            engine.execute("CREATE TABLE hash_input (ts timestamp, v long, sym symbol)", context);
            engine.execute("INSERT INTO hash_input SELECT * FROM trades", context);
            assertMatchesHash(compiler, context, "SELECT year(ts) bucket, count(*), sum(v), first(v), last(v) FROM $table ORDER BY bucket");
            assertMatchesHash(compiler, context, "SELECT year(ts) bucket, sym, count(*), sum(v) FROM $table ORDER BY bucket, sym");
        });
    }

    @Test
    public void testLargeBucketEmitsWithoutPerRowBreakerChecks() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE big AS (SELECT (x - 1)::timestamp ts, (x - 1)::int k FROM long_sequence(100_000)) TIMESTAMP(ts) PARTITION BY DAY", context);
            final SqlExecutionContextImpl impl = (SqlExecutionContextImpl) context;
            final SqlExecutionCircuitBreaker breaker = impl.getCircuitBreaker();
            final CountingSqlExecutionCircuitBreaker counting = new CountingSqlExecutionCircuitBreaker(breaker);
            impl.with(counting);
            try (
                    RecordCursorFactory factory = compiler.compile("SELECT timestamp_floor('1h', ts) ts, k, count() c FROM big", context).getRecordCursorFactory();
                    RecordCursor cursor = factory.getCursor(context)
            ) {
                Assert.assertTrue(cursor.hasNext());
                final long checks = counting.getCheckCount();
                long rows = 1;
                while (cursor.hasNext()) {
                    rows++;
                }
                Assert.assertEquals(100_000, rows);
                Assert.assertTrue("breaker checks while emitting: " + (counting.getCheckCount() - checks), counting.getCheckCount() - checks < 1_000);
            } finally {
                impl.with(breaker);
            }
        });
    }

    @Test
    public void testSpanMapsMergeInParallel() throws Exception {
        Assume.assumeTrue(parallel && workers > 3);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, workers);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 16);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1_000);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 1_000);
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE trades (ts TIMESTAMP, s VARCHAR, v LONG) TIMESTAMP(ts) PARTITION BY HOUR", context);
            engine.execute("""
                    INSERT INTO trades
                    SELECT '2024-01-01T00'::TIMESTAMP + x * 1_000_000, 'k' || x, x FROM long_sequence(10)
                    UNION ALL SELECT '2024-01-01T01'::TIMESTAMP + x * 1_000_000, 'k' || (x + 10), x + 100 FROM long_sequence(9)
                    UNION ALL SELECT '2024-01-01T02'::TIMESTAMP + x * 1_000_000, 'k' || x, x + 200 FROM long_sequence(8)
                    UNION ALL SELECT '2024-01-01T03'::TIMESTAMP + x * 1_000_000, 'k' || (x + 19), x + 300 FROM long_sequence(7)
                    UNION ALL SELECT '2024-01-02T00'::TIMESTAMP + x * 1_000_000, 'k' || x, x + 400 FROM long_sequence(2)
                    UNION ALL SELECT '2024-01-02T01'::TIMESTAMP + x * 1_000_000, 'k' || (x + 2), x + 500 FROM long_sequence(2)
                    UNION ALL SELECT '2024-01-02T02'::TIMESTAMP + x * 1_000_000, 'k' || (x + 4), x + 600 FROM long_sequence(2)
                    """, context);
            engine.execute("CREATE TABLE hash_input AS (SELECT * FROM trades)", context);
            final String sql = "SELECT timestamp_floor('1d', ts) bucket, s, count(*), sum(v), first(v), last(v) FROM $table WHERE test_latched_counter() ORDER BY bucket, s";
            final String expected = compileAndRead(compiler, context, sql.replace("$table", "hash_input"));
            final AtomicReference<CountDownLatch> threads = new AtomicReference<>();
            final Set<Long> arrived = ConcurrentHashMap.newKeySet();
            TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                @Override
                public boolean onGet(Record rec, int count) {
                    if (rec.getLong(2) < 400 && arrived.add(Thread.currentThread().threadId())) {
                        threads.get().countDown();
                        try {
                            Assert.assertTrue("four threads did not read a partition each", threads.get().await(30, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return true;
                }
            });
            try (RecordCursorFactory factory = compiler.compile(sql.replace("$table", "trades"), context).getRecordCursorFactory()) {
                final AsyncStreamGroupByAtom atom = ((AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory)).getAtom();
                for (int run = 1; run < 3; run++) {
                    threads.set(new CountDownLatch(4));
                    arrived.clear();
                    Assert.assertEquals(expected, read(factory, context));
                    Assert.assertEquals(2 * run, atom.getSpanMapMergeCount());
                    Assert.assertEquals(0, atom.getShardMergeCount());
                }
            } finally {
                TestLatchedCounterFunctionFactory.reset(null);
            }
        });
    }

    @Test
    public void testSpanShardsStartWithSmallPages() throws Exception {
        Assume.assumeTrue(parallel);
        final int rows = 2_000;
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 100);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, rows);
        run((engine, compiler, context) -> {
            // A key of two columns makes the shards ordered maps, which allocate a page up front. One bucket covers
            // every frame, so at the default page size each frame's shards alone take 8 MiB, as do the merged ones.
            engine.execute("CREATE TABLE t AS (SELECT (x * 1_000L)::TIMESTAMP ts, x % 1_000 k, (x % 7)::SYMBOL y, x v FROM long_sequence(" + rows + ")) "
                    + "TIMESTAMP(ts) PARTITION BY YEAR", context);
            try (RecordCursorFactory factory = compiler.compile("SELECT count() count, sum(s) sum FROM (SELECT ts, k, y, sum(v) s FROM t SAMPLE BY 1h)", context).getRecordCursorFactory();
                 MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(context.getSecurityContext(), 42, MemoryTrackerWorkload.QUERY)) {
                context.setMemoryTracker(tracker);
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 8L << 20);
                try {
                    // 2,000 rows, 1,000 values of k and 7 of y that always differ on a shared k: every row is its own key.
                    Assert.assertEquals("count\tsum\n" + rows + "\t" + (rows * (rows + 1L) / 2) + "\n", read(factory, context));
                } finally {
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                    context.setMemoryTracker(null);
                }
                Assert.assertEquals(0, tracker.getUsed());
            }
            Assert.assertTrue(shardMergeCountOf(compiler, context, "SELECT ts, k, y, sum(v) s FROM t SAMPLE BY 1h") > 0);
        });
    }

    @Test
    public void testSpanShardsFitEntriesWiderThanTheirPage() throws Exception {
        Assume.assumeTrue(parallel);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 10);
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE t AS (SELECT (x * 1_000L)::TIMESTAMP ts, x % 100 k, (x % 7)::SYMBOL y, x v FROM long_sequence(500)) "
                    + "TIMESTAMP(ts) PARTITION BY YEAR", context);
            // 600 aggregates take 4,800 bytes per key. The optimizer folds sum(v + constant) into sum(v), but not max().
            final StringBuilder sums = new StringBuilder();
            for (int i = 0; i < 600; i++) {
                sums.append(", max(v + ").append(i).append(") s").append(i);
            }
            try (RecordCursorFactory factory = compiler.compile("SELECT ts, k, y" + sums + " FROM t SAMPLE BY 1h", context).getRecordCursorFactory();
                 RecordCursor cursor = factory.getCursor(context)) {
                final AsyncStreamGroupByRecordCursorFactory stream = (AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory);
                final int s0 = factory.getMetadata().getColumnIndex("s0");
                final int s599 = factory.getMetadata().getColumnIndex("s599");
                final Record record = cursor.getRecord();
                long rows = 0;
                long sum0 = 0;
                long sum599 = 0;
                while (cursor.hasNext()) {
                    rows++;
                    sum0 += record.getLong(s0);
                    sum599 += record.getLong(s599);
                }
                // Every row is its own key, and v adds up to 125,250 over the 500 rows.
                Assert.assertEquals(500, rows);
                Assert.assertEquals(125_250, sum0);
                Assert.assertEquals(125_250 + 500 * 599, sum599);
                Assert.assertTrue(stream.getAtom().getShardMergeCount() > 0 || frameRows < 11);
            }
        });
    }

    @Test
    public void testLargeBucketFitsMemoryLimit() throws Exception {
        // One thread aggregates the bucket only when frames do not spread over workers.
        Assume.assumeTrue(!parallel || workers == 0);
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE big AS (SELECT (x - 1)::timestamp ts, (x - 1)::int k, x v FROM long_sequence(100_000)) TIMESTAMP(ts) PARTITION BY DAY", context);
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, (parallel ? 7L : 9L) << 20);
            try {
                assertQuery("SELECT count() n, sum(c) c, sum(s) s, min(ts) lo, max(ts) hi FROM (SELECT timestamp_floor('1h', ts) ts, k, count() c, sum(v) s FROM big)")
                        .withEngine(engine).withContext(context)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                n\tc\ts\tlo\thi
                                100000\t100000\t5000050000\t1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z
                                """);
            } finally {
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
            }
        });
    }

    @Test
    public void testKeyedFillReadsStreamInputOnce() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE tr AS (SELECT timestamp_sequence('2024-01-01', 60_000_000L) ts, "
                    + "(CASE WHEN x <= 300 THEN 's1' ELSE 's2' END)::SYMBOL sym, x::DOUBLE px FROM long_sequence(600)) "
                    + "TIMESTAMP(ts) PARTITION BY DAY", context);
            final String inner = "SELECT ts, sym, sum(px) s FROM tr WHERE test_latched_counter() SAMPLE BY 1m";
            for (String fill : new String[]{" FILL(PREV)", " FILL(NULL)", " FILL(0)"}) {
                final String sql = "SELECT ts, sym, sum(s) s FROM (" + inner + ") SAMPLE BY 1h" + fill + " ALIGN TO FIRST OBSERVATION ORDER BY ts, sym";
                final String actual;
                try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                    Assert.assertTrue(sql, plan(factory, context).contains("Stream Group By"));
                    TestLatchedCounterFunctionFactory.reset(null);
                    actual = read(factory, context);
                    Assert.assertEquals(sql, 600, TestLatchedCounterFunctionFactory.getCount());
                }
                context.setStreamGroupByEnabled(false);
                try {
                    Assert.assertEquals(sql, compileAndRead(compiler, context, sql), actual);
                } finally {
                    context.setStreamGroupByEnabled(true);
                }
            }
        });
    }

    @Test
    public void testKSumCompensatesAcrossFrames() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE tk (ts TIMESTAMP, d DOUBLE, k SYMBOL) TIMESTAMP(ts) PARTITION BY DAY", context);
            // 8,760 values of 0.7 add up to exactly 6,132. Next to 1e16 only a Kahan sum's compensation keeps them.
            engine.execute("INSERT INTO tk VALUES ('2024-01-01', 1e16, 'a')", context);
            engine.execute("INSERT INTO tk SELECT timestamp_sequence('2024-01-01T00:00:01', 3_600_000_000L), 0.7, 'a' FROM long_sequence(8_760)", context);
            engine.execute("INSERT INTO tk VALUES ('2024-12-31T23:00', -1e16, 'a')", context);
            for (String sql : new String[]{"SELECT ts, ksum(d) s FROM tk SAMPLE BY 1y", "SELECT ts, k, ksum(d) s FROM tk SAMPLE BY 1y"}) {
                try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                    assertStream(factory, true);
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        Assert.assertTrue(cursor.hasNext());
                        final double sum = cursor.getRecord().getDouble(factory.getMetadata().getColumnIndex("s"));
                        // 2.0 is the distance between doubles near 1e16. Merging a worker's partial sum can lose that much.
                        Assert.assertEquals(sql, 6_132.0, sum, 2.0 * (workers + 1));
                        Assert.assertFalse(cursor.hasNext());
                    }
                }
            }
        });
    }

    @Test
    public void testStreamGroupByDisabledByConfiguration() throws Exception {
        final CairoConfiguration streamOff = new CairoConfigurationWrapper(configuration) {
            @Override
            public boolean isSqlStreamGroupByEnabled() {
                return false;
            }
        };
        assertMemoryLeak(() -> TestUtils.execute(workers == 0 ? null : new TestWorkerPool(workers), (engine, compiler, context) -> {
            createTable(compiler, context);
            for (String sql : new String[]{"SELECT ts, sym, count(*) FROM trades SAMPLE BY 1m", "SELECT timestamp_floor('1m', ts) bucket, count(*) FROM trades"}) {
                try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                    final String plan = plan(factory, context);
                    Assert.assertFalse(plan, plan.contains("Stream Group By"));
                    Assert.assertTrue(plan, plan.contains(parallel ? "Async Group By" : "GroupBy"));
                }
            }
        }, streamOff, LOG));
    }

    @Test
    public void testSumOfOppositeInfinitiesIsNullInEveryLayout() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE s (ts TIMESTAMP, d DOUBLE, f FLOAT, k SYMBOL) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("""
                    INSERT INTO s VALUES
                        ('2024-01-01T00', 1, 1, 'a'), ('2024-01-01T01', 'Infinity', 'Infinity', 'a'), ('2024-01-01T02', 2, 2, 'a'),
                        ('2024-01-02T00', 1, 1, 'b'), ('2024-01-02T01', '-Infinity', '-Infinity', 'b'), ('2024-01-02T02', 'Infinity', 'Infinity', 'b'),
                        ('2024-01-02T03', 1, 1, 'b'), ('2024-01-02T04', 1, 1, 'b'),
                        ('2024-01-03T00', null, null, 'c'), ('2024-01-03T01', 5, 5, 'c'),
                        ('2024-01-04T00', null, null, 'd'),
                        ('2024-01-05T00', '-Infinity', '-Infinity', 'e'), ('2024-01-05T01', 3, 3, 'e')""", context);
            // Text output prints Infinity as null, so these compare the values themselves.
            final double[] perGroup = {Double.POSITIVE_INFINITY, Double.NaN, 5, Double.NaN, Double.NEGATIVE_INFINITY};
            for (String arg : new String[]{"d", "f", "d + 0", "f + 0"}) {
                for (String filter : new String[]{"", " WHERE ts > 0"}) {
                    final String sum = "sum(" + arg + ") sum FROM s" + filter;
                    assertSums(compiler, context, "SELECT ts, k, " + sum + " SAMPLE BY 1d ORDER BY ts, k", perGroup);
                    assertSums(compiler, context, "SELECT ts, " + sum + " SAMPLE BY 1d ORDER BY ts", perGroup);
                    assertSums(compiler, context, "SELECT k, " + sum + " ORDER BY k", perGroup);
                    assertSums(compiler, context, "SELECT " + sum, new double[]{Double.NaN});
                }
            }
            // A sum that became NaN and one that never had a value are the same NULL to whatever reads them.
            try (RecordCursorFactory factory = compiler.compile("SELECT sum, count() count FROM (SELECT k, sum(d) sum FROM s WHERE k IN ('b', 'd'))", context).getRecordCursorFactory()) {
                Assert.assertEquals("sum\tcount\nnull\t2\n", read(factory, context));
            }
            // A key column added after the data reads as a column top, so vectorized GROUP BY sums each frame alone.
            engine.execute("ALTER TABLE s ADD COLUMN i INT", context);
            final String[] intervals = {"", " WHERE ts IN '2024-01-01'", " WHERE ts IN '2024-01-02'", " WHERE ts >= '2024-01-03'"};
            final double[] perInterval = {Double.NaN, Double.POSITIVE_INFINITY, Double.NaN, Double.NEGATIVE_INFINITY};
            for (int i = 0; i < intervals.length; i++) {
                final String sql = "SELECT i, sum(d) sum FROM s" + intervals[i];
                if (parallel) {
                    try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                        Assert.assertTrue(sql, plan(factory, context).contains("GroupBy vectorized: true"));
                    }
                }
                assertSums(compiler, context, sql, new double[]{perInterval[i]});
            }
        });
    }

    @Test
    public void testMaxMinSkipInfinityInEveryLayout() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE s (ts TIMESTAMP, d DOUBLE, f FLOAT, k SYMBOL, i INT) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("""
                    INSERT INTO s VALUES
                        ('2024-01-01T00', 1, 1, 'a', 1), ('2024-01-01T01', 'Infinity', 'Infinity', 'a', 1), ('2024-01-01T02', 2, 2, 'a', 1),
                        ('2024-01-02T00', 1, 1, 'b', 2), ('2024-01-02T01', '-Infinity', '-Infinity', 'b', 2), ('2024-01-02T02', 'Infinity', 'Infinity', 'b', 2),
                        ('2024-01-02T03', 1, 1, 'b', 2),
                        ('2024-01-03T00', null, null, 'c', 3), ('2024-01-03T01', 5, 5, 'c', 3),
                        ('2024-01-04T00', null, null, 'd', 4),
                        ('2024-01-05T00', '-Infinity', '-Infinity', 'e', 5), ('2024-01-05T01', 3, 3, 'e', 5),
                        ('2024-01-06T00', 'Infinity', 'Infinity', 'f', 6), ('2024-01-06T01', '-Infinity', '-Infinity', 'f', 6),
                        ('2024-01-07T00', 'Infinity', 'Infinity', 'g', 7), ('2024-01-07T01', 7, 7, 'g', 7), ('2024-01-07T02', 4, 4, 'g', 7)""", context);
            final double[][] expected = {
                    {2, 1, 5, Double.NaN, 3, Double.NaN, 7},
                    {1, 1, 5, Double.NaN, 3, Double.NaN, 4}
            };
            final double[][] notKeyed = {{7}, {1}};
            final String[] functions = {"max", "min"};
            if (parallel) {
                try (RecordCursorFactory factory = compiler.compile("SELECT i, max(d) v FROM s", context).getRecordCursorFactory()) {
                    Assert.assertTrue(plan(factory, context).contains("GroupBy vectorized: true"));
                }
            }
            for (boolean isStream : new boolean[]{true, false}) {
                context.setStreamGroupByEnabled(isStream);
                for (String arg : new String[]{"d", "f", "d + 0", "f + 0"}) {
                    for (String filter : new String[]{"", " WHERE ts > 0"}) {
                        for (int i = 0; i < functions.length; i++) {
                            final String value = functions[i] + "(" + arg + ") v FROM s" + filter;
                            assertValues(compiler, context, "SELECT ts, k, " + value + " SAMPLE BY 1d ORDER BY ts, k", "v", expected[i]);
                            assertValues(compiler, context, "SELECT ts, " + value + " SAMPLE BY 1d ORDER BY ts", "v", expected[i]);
                            assertValues(compiler, context, "SELECT k, " + value + " ORDER BY k", "v", expected[i]);
                            assertValues(compiler, context, "SELECT i, " + value + " ORDER BY i", "v", expected[i]);
                            assertValues(compiler, context, "SELECT " + value, "v", notKeyed[i]);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testLatestOnCopiesOnlyStreamInputs() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE trades AS (SELECT timestamp_sequence('2024-01-01', 7_000_000) ts, ('S' || (x % 5))::SYMBOL sym, x % 97 price "
                    + "FROM long_sequence(600)) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("CREATE TABLE notes AS (SELECT ('S' || (x % 5))::SYMBOL sym, x note FROM long_sequence(20))", context);
            final String bars = "SELECT ts, sym, sum(price) total FROM trades SAMPLE BY 1m";
            for (boolean isFullFat : new boolean[]{false, true}) {
                compiler.setFullFatJoins(isFullFat);
                for (String input : new String[]{
                        "SELECT t.ts, t.sym, t.price, s.total FROM trades t ASOF JOIN (" + bars + ") s ON (sym)",
                        "SELECT t.ts, t.sym, t.price, s.total FROM trades t LT JOIN (" + bars + ") s ON (sym)",
                        "SELECT s.ts, s.sym, s.total, t.price FROM (" + bars + ") s ASOF JOIN trades t ON (sym)",
                        "SELECT s.ts, s.sym, s.total, n.note FROM (" + bars + ") s JOIN notes n ON s.sym = n.sym",
                        "SELECT s.ts, s.sym, s.total, n.note FROM (" + bars + ") s LEFT JOIN notes n ON s.sym = n.sym",
                        "SELECT s.ts, s.sym, s.total, n.note FROM (" + bars + ") s CROSS JOIN notes n",
                        "SELECT ts, sym, price FROM trades UNION ALL " + bars,
                        bars + " UNION ALL SELECT ts, sym, price FROM trades"
                }) {
                    final String sql = "SELECT * FROM ((" + input + ") LATEST ON ts PARTITION BY sym) ORDER BY sym";
                    context.setStreamGroupByEnabled(false);
                    final String expected = compileAndRead(compiler, context, sql);
                    context.setStreamGroupByEnabled(true);
                    final String inputPlan = compileAndRead(compiler, context, "EXPLAIN " + input);
                    Assert.assertFalse(inputPlan, inputPlan.contains("Materialize"));
                    final String plan = compileAndRead(compiler, context, "EXPLAIN " + sql);
                    Assert.assertTrue(plan, plan.matches("(?s).*\n( *)Materialize\n\\1  +(Async )?Stream Group By.*") && !plan.matches("(?s).*LatestBy\n *Materialize.*"));
                    try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                        for (int pass = 0; pass < 2; pass++) {
                            Assert.assertEquals(sql, expected, read(factory, context));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testLatestOnOverSampleByFeedsTimeSeriesJoins() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE t (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("INSERT INTO t VALUES ('2024-01-01T00:10', 'a', 1), ('2024-01-01T01:20', 'b', 2), ('2024-01-01T02:10', 'a', 3)", context);
            engine.execute("CREATE TABLE u (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("INSERT INTO u VALUES ('2024-01-01T00:30', 1), ('2024-01-01T01:30', 2), ('2024-01-01T02:30', 4)", context);
            engine.execute("CREATE TABLE latest (ts TIMESTAMP, k SYMBOL, s DOUBLE) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("INSERT INTO latest VALUES ('2024-01-01T01:00', 'b', 2), ('2024-01-01T02:00', 'a', 3)", context);
            final String subquery = "(SELECT ts, k, sum(v) s FROM t SAMPLE BY 1h) LATEST ON ts PARTITION BY k";
            assertQuery(subquery).withEngine(engine).withContext(context)
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .withPlanContaining("LatestBy\n    Materialize")
                    .returns("""
                            ts\tk\ts
                            2024-01-01T01:00:00.000000Z\tb\t2.0
                            2024-01-01T02:00:00.000000Z\ta\t3.0
                            """);
            final String[] wrappedSubqueries = {"((" + subquery + ") TIMESTAMP(ts))", "(" + subquery + ")"};
            for (String join : new String[]{
                    "SELECT * FROM %s a SPLICE JOIN u b",
                    "SELECT * FROM u a SPLICE JOIN %s b",
                    "SELECT a.ts, a.k, h.offset, b.x FROM %s a HORIZON JOIN u b RANGE FROM 0s TO 1s STEP 1s AS h",
                    "SELECT h.offset, sum(b.x) sx, count() c FROM %s a HORIZON JOIN u b RANGE FROM 0s TO 1s STEP 1s AS h ORDER BY 1"
            }) {
                final String expected = compileAndRead(compiler, context, join.formatted("latest"));
                for (String wrapped : wrappedSubqueries) {
                    final String sql = join.formatted(wrapped);
                    try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                        Assert.assertTrue(sql, plan(factory, context).contains("LatestBy"));
                        for (int pass = 0; pass < 2; pass++) {
                            Assert.assertEquals(sql, expected, read(factory, context));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testRowIdJoinsCopyStreamsInsteadOfSorting() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE t (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("INSERT INTO t SELECT (x * 3_600_000_000L)::TIMESTAMP, x::INT FROM long_sequence(48)", context);
            engine.execute("CREATE TABLE u (ts TIMESTAMP, y INT) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("INSERT INTO u SELECT (x * 3_600_000_000L + 1_800_000_000L)::TIMESTAMP, x::INT FROM long_sequence(48)", context);
            // t has a row on each hour x = 1..48, and u a row half an hour later with y = x, so the latest u row at
            // hour h and one second later has y = h - 1. LIMIT -5 keeps hours 44 to 48, and INTERSECT the even ones.
            assertRows(compiler, context, "SELECT a.ts, a.c, h.offset, b.y FROM ((SELECT ts, count() c FROM t SAMPLE BY 1h ORDER BY ts LIMIT -5) "
                    + "INTERSECT (SELECT ts, count() c FROM t WHERE x % 2 = 0 SAMPLE BY 1h)) a HORIZON JOIN u b RANGE FROM 0s TO 1s STEP 1s AS h", """
                    ts\tc\toffset\ty
                    1970-01-02T20:00:00.000000Z\t1\t0\t43
                    1970-01-02T20:00:00.000000Z\t1\t1000000\t43
                    1970-01-02T22:00:00.000000Z\t1\t0\t45
                    1970-01-02T22:00:00.000000Z\t1\t1000000\t45
                    1970-01-03T00:00:00.000000Z\t1\t0\t47
                    1970-01-03T00:00:00.000000Z\t1\t1000000\t47
                    """);

            assertRowIdJoinsCopy(compiler, context, "(SELECT ts, count() c FROM t SAMPLE BY 1h)", "(Async )?Stream Group By");
            // INTERSECT and EXCEPT pass on the random access of their first input, which a stream GROUP BY under a
            // negative or bind-variable LIMIT lacks.
            for (String setOperation : new String[]{"INTERSECT", "EXCEPT", "INTERSECT ALL", "EXCEPT ALL"}) {
                for (String limit : new String[]{"ORDER BY ts LIMIT -5", "LIMIT -5", "ORDER BY ts LIMIT -10, -5", "LIMIT $1"}) {
                    assertRowIdJoinsCopy(
                            compiler,
                            context,
                            "((SELECT ts, count() c FROM t SAMPLE BY 1h " + limit + ") " + setOperation
                                    + " (SELECT ts, count() c FROM t WHERE x % 2 = 0 SAMPLE BY 1h))",
                            "(Intersect|Except)( All)?"
                    );
                }
            }

            // A UNION has no random access to pass on, so the join copies neither it nor an INTERSECT over it.
            final String union = "SELECT * FROM u a SPLICE JOIN (((SELECT ts, x c FROM t UNION ALL SELECT ts, x c FROM t) "
                    + "INTERSECT SELECT ts, x c FROM t) TIMESTAMP(ts)) b";
            try {
                compiler.compile(union, context);
                Assert.fail(union);
            } catch (SqlException e) {
                Assert.assertEquals(union.indexOf("SPLICE"), e.getPosition());
                TestUtils.assertEquals("right side of splice join doesn't support random access", e.getFlyweightMessage());
            }
        });
    }

    @Test
    public void testPartlyReadCursorLeavesShardFree() throws Exception {
        assertPartlyReadLeavesShardFree("SELECT ts, count() c, sum(x) s FROM t SAMPLE BY 1s", AsyncStreamGroupByRecordCursorFactory.class);
    }

    @Test
    public void testReducerErrorKeepsItsType() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE t AS (SELECT (x * 1_000_000)::TIMESTAMP ts, x FROM long_sequence(" + 50 * frameRows
                    + ")) TIMESTAMP(ts)", context);
            assertFilterFailureMatchesHash(compiler, context, 40L * frameRows);
        });
    }

    @Test
    public void testPartlyReadFilterLeavesShardFree() throws Exception {
        assertPartlyReadLeavesShardFree("SELECT * FROM t WHERE x > 0", null);
    }

    @Test
    public void testPartlyReadHorizonJoinLeavesShardFree() throws Exception {
        assertPartlyReadLeavesShardFree(
                "SELECT t.ts, h.offset, p.price FROM t HORIZON JOIN p ON (t.sym = p.sym) LIST (0s) AS h",
                AsyncHorizonJoinProjectionRecordCursorFactory.class
        );
    }

    @Test
    public void testPartlyReadKeyedWindowJoinLeavesShardFree() throws Exception {
        assertPartlyReadLeavesShardFree(
                "SELECT t.ts, sum(p.price) s FROM t WINDOW JOIN p ON (t.sym = p.sym) RANGE BETWEEN 1 second PRECEDING AND 1 second FOLLOWING",
                AsyncWindowJoinFastRecordCursorFactory.class
        );
    }

    @Test
    public void testPartlyReadWindowJoinLeavesShardFree() throws Exception {
        assertPartlyReadLeavesShardFree(
                "SELECT t.ts, sum(p.price) s FROM t WINDOW JOIN p RANGE BETWEEN 1 second PRECEDING AND 1 second FOLLOWING",
                AsyncWindowJoinRecordCursorFactory.class
        );
    }

    @Test
    public void testPartlyReadCursorAwaitsOnlyFramesInFlight() throws Exception {
        // A PostgreSQL wire client that fetches in pages makes its connection await the frames in flight before it binds
        // the variables of the next fetch. Here the worker ends frame h without taking frame h + L, as a worker whose frame
        // spends the lane budget, or an owner that steals the frame, does. Only the owner can publish frame h + L then, and
        // it is the thread that waits, so it must not wait for that frame.
        // Frame 0 returns the first row on its own only when it holds more than one bucket.
        Assume.assumeTrue(parallel && workers > 0 && frameRows > 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, Integer.MAX_VALUE);
        PageFrameSequence.setLaneRowBudgetForTesting(0);
        try {
            assertMemoryLeak(() -> TestUtils.execute(new TestWorkerPool(workers, WorkerPoolMode.LEGACY), (engine, compiler, context) -> {
                final String expected = createHeldFrameTable(engine, compiler, context, "");
                final FrameHolder holder = new FrameHolder(frameRows);
                TestLatchedCounterFunctionFactory.reset(holder);
                try (RecordCursorFactory factory = compiler.compile(HELD_FRAME_SQL, context).getRecordCursorFactory()) {
                    final AsyncStreamGroupByRecordCursorFactory stream = (AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory);
                    final RecordMetadata metadata = factory.getMetadata();
                    final StringSink sink = new StringSink();
                    CursorPrinter.println(metadata, sink);
                    try (RecordCursor cursor = openWithHeldFrame(factory, context, holder)) {
                        try {
                            CursorPrinter.println(cursor.getRecord(), metadata, sink);
                        } finally {
                            holder.release();
                        }
                        awaitPageFrameSequences(context);
                        // Every frame ended but the staged ones, among them frame h + L, which the owner still holds.
                        Assert.assertEquals(stream.getDispatchedFrameCount(), stream.getReducedFrameCount());
                        Assert.assertEquals(holder.getHeldFrame() + holder.getLaneCount(), stream.getStagedFrame(holder.getHeldLane()));
                        while (cursor.hasNext()) {
                            CursorPrinter.println(cursor.getRecord(), metadata, sink);
                        }
                    }
                    Assert.assertEquals(expected, sortedLines(sink));
                } finally {
                    TestLatchedCounterFunctionFactory.reset(null);
                }
            }, configuration, LOG));
        } finally {
            PageFrameSequence.setLaneRowBudgetForTesting(-1);
        }
    }

    @Test
    public void testClosedCursorForgetsFramesWorkersTook() throws Exception {
        // Closing a partly read cursor, the owner ends the staged frames that no worker took, then waits for the frames in
        // flight. Frame h is in flight when the owner looks at its lane. Its worker then ends it, takes frame h + L and ends
        // that one unreduced, all before the owner checks whether every frame ended. The owner must not keep frame h + L as
        // staged: the next run of the query would find it there.
        // Frame 0 returns the first row on its own only when it holds more than one bucket.
        Assume.assumeTrue(parallel && workers > 0 && frameRows > 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, Integer.MAX_VALUE);
        // Workers take the next frame of their lanes.
        PageFrameSequence.setLaneRowBudgetForTesting(Long.MAX_VALUE);
        try {
            assertMemoryLeak(() -> TestUtils.execute(new TestWorkerPool(workers, WorkerPoolMode.LEGACY), (engine, compiler, context) -> {
                final String expected = createHeldFrameTable(engine, compiler, context, "");
                final FrameHolder holder = new FrameHolder(frameRows);
                TestLatchedCounterFunctionFactory.reset(holder);
                final AtomicBoolean isArmed = new AtomicBoolean();
                try (RecordCursorFactory factory = compiler.compile(HELD_FRAME_SQL, context).getRecordCursorFactory()) {
                    final AsyncStreamGroupByRecordCursorFactory stream = (AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory);
                    PageFrameSequence.setStagedTasksSkippedHookForTesting(() -> {
                        if (isArmed.compareAndSet(true, false)) {
                            holder.release();
                            // The workers end every frame, the staged ones included, while the owner waits here.
                            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                            while (stream.getReducedFrameCount() < stream.getDispatchedFrameCount() || getStagedFrameCount(stream) > 0) {
                                if (System.nanoTime() > deadline) {
                                    throw new AssertionError("the frames in flight did not end");
                                }
                                Os.pause();
                            }
                        }
                    });
                    final RecordCursor cursor = openWithHeldFrame(factory, context, holder);
                    isArmed.set(true);
                    try {
                        cursor.close();
                    } finally {
                        holder.release();
                    }
                    Assert.assertFalse(isArmed.get());
                    Assert.assertEquals(0, getStagedFrameCount(stream));
                    holder.disarm();
                    Assert.assertEquals(expected, sortedLines(read(factory, context)));
                } finally {
                    PageFrameSequence.setStagedTasksSkippedHookForTesting(null);
                    TestLatchedCounterFunctionFactory.reset(null);
                }
            }, configuration, LOG));
        } finally {
            PageFrameSequence.setLaneRowBudgetForTesting(-1);
        }
    }

    @Test
    public void testReducerErrorLeavesLaneToOwner() throws Exception {
        // A fiber fails to reduce frame h with an Error, which ends the fiber, while frame h + L is staged behind frame h.
        // The fiber must leave frame h + L to the owner: a fiber that took it would drop it, and close() would wait for it
        // for good. The hook ends a frame taken that way, so that the test fails rather than hang.
        // Frame 0 returns the first row on its own only when it holds more than one bucket.
        Assume.assumeTrue(parallel && workers > 0 && frameRows > 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, Integer.MAX_VALUE);
        final AtomicLong successor = new AtomicLong(-1);
        final AtomicInteger takenSuccessorCount = new AtomicInteger();
        PageFrameSequence.setLaneClaimHookForTesting(taskIndex -> {
            if (taskIndex == successor.get()) {
                takenSuccessorCount.incrementAndGet();
                throw CairoException.nonCritical().put("frame taken after an error");
            }
        });
        try {
            assertMemoryLeak(() -> TestUtils.execute(new TestWorkerPool(workers, WorkerPoolMode.FIBER_HOST), (engine, compiler, context) -> {
                final String expected = createHeldFrameTable(engine, compiler, context, "");
                final FrameHolder holder = new FrameHolder(frameRows);
                TestLatchedCounterFunctionFactory.reset(holder);
                try (RecordCursorFactory factory = compiler.compile(HELD_FRAME_SQL, context).getRecordCursorFactory()) {
                    try (RecordCursor cursor = openWithHeldFrame(factory, context, holder)) {
                        successor.set(holder.getHeldFrame() + holder.getLaneCount());
                        holder.failWith(new StackOverflowError("frame failed"));
                        holder.release();
                        try {
                            //noinspection StatementWithEmptyBody
                            while (cursor.hasNext()) {
                            }
                            Assert.fail("the query ignored the error of frame " + holder.getHeldFrame());
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "frame failed");
                        }
                    }
                    Assert.assertEquals(0, takenSuccessorCount.get());
                    successor.set(-1);
                    holder.disarm();
                    Assert.assertEquals(expected, sortedLines(read(factory, context)));
                } finally {
                    TestLatchedCounterFunctionFactory.reset(null);
                }
            }, configuration, LOG));
        } finally {
            PageFrameSequence.setLaneClaimHookForTesting(null);
        }
    }

    @Test
    public void testFrameFailingAsOwnerChecksItFailsQuery() throws Exception {
        // Waiting for frame 1, the owner checks whether the frame is reduced and whether the query still runs. A worker
        // fails frame 1 between the two checks. The owner must throw the worker's error rather than return frame 1: the
        // frame's slot still holds the segments that the previous run left, whose first key closes the last bucket of
        // frame 0, and LIMIT ends the query on that bucket.
        Assume.assumeTrue(parallel && workers > 0 && frameRows > 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, Integer.MAX_VALUE);
        // The worker that fails frame 1 takes frame 1 + L next, right after it marks frame 1 ended.
        PageFrameSequence.setLaneRowBudgetForTesting(Long.MAX_VALUE);
        final AtomicBoolean isArmed = new AtomicBoolean();
        final AtomicBoolean isChecking = new AtomicBoolean();
        final AtomicBoolean isFailing = new AtomicBoolean();
        final AtomicBoolean isFrameEnded = new AtomicBoolean();
        final AtomicBoolean isStolen = new AtomicBoolean();
        final AtomicReference<Thread> failingWorker = new AtomicReference<>();
        final AtomicInteger laneCount = new AtomicInteger(-1);
        PageFrameSequence.setReducedCheckHookForTesting(taskIndex -> {
            if (taskIndex == 1 && isArmed.compareAndSet(true, false)) {
                // The owner waits here, so only a worker can still start frame 1.
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                while (failingWorker.get() == null && !isStolen.get()) {
                    if (System.nanoTime() > deadline) {
                        throw new AssertionError("no thread started frame 1");
                    }
                    Os.pause();
                }
                if (failingWorker.get() != null) {
                    isChecking.set(true);
                    while (!isFrameEnded.get()) {
                        if (System.nanoTime() > deadline) {
                            throw new AssertionError("frame 1 did not end");
                        }
                        Os.pause();
                    }
                }
            }
        });
        PageFrameSequence.setLaneClaimHookForTesting(taskIndex -> {
            if (taskIndex == 1 + laneCount.get() && Thread.currentThread() == failingWorker.get()) {
                isFrameEnded.set(true);
            }
        });
        try {
            assertMemoryLeak(() -> TestUtils.execute(new TestWorkerPool(workers, WorkerPoolMode.LEGACY), (engine, compiler, context) -> {
                // Each row is a bucket of its own, so the first rows are those of frame 0.
                final String expected = createHeldFrameTable(engine, compiler, context, " AND x <= " + frameRows);
                final Thread owner = Thread.currentThread();
                TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                    @Override
                    public boolean onGet(Record rec, int count) {
                        if (rec.getLong(1) == frameRows + 1 && isFailing.get()) {
                            // The owner may steal frame 1 while it waits for frame 0; the run then starts over.
                            if (Thread.currentThread() == owner) {
                                isStolen.set(true);
                                return true;
                            }
                            failingWorker.set(Thread.currentThread());
                            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                            while (!isChecking.get()) {
                                if (System.nanoTime() > deadline) {
                                    throw new AssertionError("the owner did not check frame 1");
                                }
                                Os.pause();
                            }
                            throw CairoException.nonCritical().put("frame 1 failed");
                        }
                        return true;
                    }
                });
                try (RecordCursorFactory factory = compiler.compile(HELD_FRAME_SQL + " LIMIT " + frameRows, context).getRecordCursorFactory()) {
                    laneCount.set(((AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory)).getAtom().getLaneCount());
                    // The first run leaves its segments of frame 1 in the frame's slot.
                    Assert.assertEquals(expected, sortedLines(read(factory, context)));
                    for (int run = 0; run < 100 && failingWorker.get() == null; run++) {
                        isStolen.set(false);
                        isFailing.set(true);
                        isArmed.set(true);
                        try {
                            final String rows = sortedLines(read(factory, context));
                            Assert.assertNull("the query returned rows past frame 1, which failed", failingWorker.get());
                            Assert.assertEquals(expected, rows);
                        } catch (CairoException e) {
                            Assert.assertNotNull(failingWorker.get());
                            TestUtils.assertContains(e.getFlyweightMessage(), "frame 1 failed");
                        } finally {
                            isFailing.set(false);
                            isArmed.set(false);
                        }
                    }
                    Assert.assertNotNull("no worker failed frame 1", failingWorker.get());
                    Assert.assertTrue(isFrameEnded.get());
                    Assert.assertEquals(expected, sortedLines(read(factory, context)));
                } finally {
                    TestLatchedCounterFunctionFactory.reset(null);
                }
            }, configuration, LOG));
        } finally {
            PageFrameSequence.setReducedCheckHookForTesting(null);
            PageFrameSequence.setLaneClaimHookForTesting(null);
            PageFrameSequence.setLaneRowBudgetForTesting(-1);
        }
    }

    @Test
    public void testLimitAndFillRewindLaxCopyUnderThem() throws Exception {
        // A negative LIMIT or a keyed FILL rewinds its input, so it copies a stream GROUP BY under it, which a rewind would
        // run again. Over a copy that a LIMIT or FILL under it made, it rewinds that copy and makes none of its own.
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE t AS (SELECT ((x - 1) * 60_000_000)::TIMESTAMP ts, x % 5 k, x v FROM long_sequence(4_320)) TIMESTAMP(ts)", context);
            assertCopyCount(compiler, context, "SELECT * FROM (SELECT ts, k, count() c FROM t SAMPLE BY 1h LIMIT -10) LIMIT -5", 1);
            assertCopyCount(compiler, context, "WITH s AS (SELECT ts, k, count() c FROM t SAMPLE BY 1h LIMIT -10) SELECT * FROM s LIMIT -5", 1);
            assertCopyCount(compiler, context, "SELECT * FROM (SELECT ts, k, max(v) m FROM t SAMPLE BY 1h LIMIT -10) LIMIT -5", 1);
            for (String fill : new String[]{"PREV", "NULL", "0"}) {
                assertCopyCount(compiler, context, "SELECT ts, k, sum(c) s FROM (SELECT ts, k, count() c FROM t SAMPLE BY 1m) SAMPLE BY 1h FILL(" + fill + ") LIMIT -5", 1);
                assertCopyCount(compiler, context, "SELECT ts, k, sum(c) s FROM (SELECT ts, k, count() c FROM t SAMPLE BY 1m LIMIT -100) SAMPLE BY 1h FILL(" + fill + ")", 1);
            }
            // A SAMPLE BY over the copy computes its rows from the copy again, as it did before the copy.
            assertCopyCount(compiler, context, "SELECT ts, k, sum(c) s FROM (SELECT ts, k, count() c FROM t SAMPLE BY 1m LIMIT -100) SAMPLE BY 1h LIMIT -5", 1);
            // A UNION ALL shows no copy under it, so the LIMIT over it makes one. Its last rows are those of its second
            // query, which a hash GROUP BY returns in order only outside a UNION.
            assertCopyCount(
                    compiler,
                    context,
                    "SELECT * FROM (SELECT ts, k, count() c FROM t SAMPLE BY 1h UNION ALL SELECT ts, k, count() c FROM t SAMPLE BY 1h) LIMIT -5",
                    "SELECT ts, k, count() c FROM t SAMPLE BY 1h LIMIT -5",
                    1
            );
        });
    }

    @Test
    public void testFramesReducedOutOfOrderEmitInOrder() throws Exception {
        // Whichever thread reduces frame 0 waits until a later frame is reduced. The first dispatch holds two frames,
        // so the owner or a worker is free to reduce frame 1 meanwhile.
        Assume.assumeTrue(parallel && workers > 0);
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE t AS (SELECT (x * 1_000_000)::TIMESTAMP ts, x FROM long_sequence("
                    + 8 * frameRows + ")) TIMESTAMP(ts)", context);
            final String sql = "SELECT ts, count() c, sum(x) s FROM t WHERE test_latched_counter() SAMPLE BY 3s";
            TestLatchedCounterFunctionFactory.reset(null);
            context.setStreamGroupByEnabled(false);
            final String expected = compileAndRead(compiler, context, sql);
            context.setStreamGroupByEnabled(true);
            // The filter takes the callback when it compiles, before the factory it waits on exists.
            final AtomicReference<AsyncStreamGroupByRecordCursorFactory> groupBy = new AtomicReference<>();
            final AtomicBoolean isFrameHeld = new AtomicBoolean();
            TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                @Override
                public boolean onGet(Record rec, int count) {
                    if (rec.getLong(1) == 1 && isFrameHeld.compareAndSet(false, true)) {
                        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                        while (groupBy.get().getReducedFrameCount() == 0) {
                            if (System.nanoTime() > deadline) {
                                throw new AssertionError("no later frame was reduced");
                            }
                            Os.pause();
                        }
                    }
                    return true;
                }
            });
            try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                groupBy.set((AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory));
                Assert.assertEquals(expected, read(factory, context));
                Assert.assertTrue(isFrameHeld.get());
            } finally {
                TestLatchedCounterFunctionFactory.reset(null);
            }
        });
    }

    @Test
    public void testHeldFrameHoldsOnlyItsLane() throws Exception {
        assertHeldFrameHoldsOnlyItsLane(WorkerPoolMode.LEGACY, false, null, null);
    }

    @Test
    public void testHeldFrameHoldsOnlyItsLaneOnFibers() throws Exception {
        assertHeldFrameHoldsOnlyItsLane(WorkerPoolMode.FIBER_HOST, false, null, null);
    }

    @Test
    public void testLaneTaskFailsWithItsWorker() throws Exception {
        assertHeldFrameHoldsOnlyItsLane(WorkerPoolMode.LEGACY, false, "test lane task failure", null);
    }

    @Test
    public void testLaneTaskFailsWithItsFiber() throws Exception {
        assertHeldFrameHoldsOnlyItsLane(WorkerPoolMode.FIBER_HOST, false, "test lane task failure", null);
    }

    @Test
    public void testSpentFiberBatchLaunchesLaneTask() throws Exception {
        assertHeldFrameHoldsOnlyItsLane(WorkerPoolMode.FIBER_HOST, true, null, null);
    }

    @Test
    public void testSpentWorkerBudgetLeavesLaneToOwner() throws Exception {
        assertHeldFrameHoldsOnlyItsLane(WorkerPoolMode.LEGACY, true, null, null);
    }

    @Test
    public void testLaneTaskStaysWithItsFiberWhenNoFiberIsFree() throws Exception {
        final LongList epochs = new LongList();
        final ObjList<Fiber> fibers = new ObjList<>();
        assertHeldFrameHoldsOnlyItsLane(WorkerPoolMode.FIBER_HOST, true, null, new LaneLaunchBlocker() {
            @Override
            public void block(WorkerPool pool, PageFrameReduceDispatcher dispatcher) {
                Fiber fiber;
                while ((fiber = pool.getFiberRuntime().tryReserveFiber()) != null) {
                    fibers.add(fiber);
                    epochs.add(fiber.getReservationEpoch());
                }
            }

            @Override
            public void unblock(WorkerPool pool, PageFrameReduceDispatcher dispatcher) {
                for (int i = 0, n = fibers.size(); i < n; i++) {
                    pool.getFiberRuntime().releaseReservedFiber(fibers.getQuick(i), epochs.getQuick(i));
                }
                fibers.clear();
            }
        });
    }

    @Test
    public void testLaneTaskStaysWithItsFiberWhenNoTaskIsFree() throws Exception {
        final AtomicInteger leaseCount = new AtomicInteger();
        assertHeldFrameHoldsOnlyItsLane(WorkerPoolMode.FIBER_HOST, true, null, new LaneLaunchBlocker() {
            @Override
            public void block(WorkerPool pool, PageFrameReduceDispatcher dispatcher) {
                while (dispatcher.tryLeaseTaskForTesting()) {
                    leaseCount.incrementAndGet();
                }
            }

            @Override
            public void unblock(WorkerPool pool, PageFrameReduceDispatcher dispatcher) {
                for (int i = leaseCount.getAndSet(0); i > 0; i--) {
                    dispatcher.releaseTaskLeaseForTesting();
                }
            }
        });
    }

    @Test
    public void testLaneTaskStaysWithItsFiberWhenTaskCannotBeMade() throws Exception {
        final ObjList<PageFrameReduceDispatcher.TaskLeaseForTesting> leases = new ObjList<>();
        final AtomicInteger failureCount = new AtomicInteger();
        assertHeldFrameHoldsOnlyItsLane(WorkerPoolMode.FIBER_HOST, true, null, new LaneLaunchBlocker() {
            @Override
            public void block(WorkerPool pool, PageFrameReduceDispatcher dispatcher) {
                // Leases the free tasks until the pool makes one, so that the next task the launch asks for is new.
                final int createdCount = dispatcher.getCreatedTaskCount();
                do {
                    leases.add(dispatcher.acquireTaskLeaseForTesting());
                } while (dispatcher.getCreatedTaskCount() == createdCount);
                dispatcher.setBeforeNewTaskForTesting(() -> {
                    failureCount.incrementAndGet();
                    throw CairoException.nonCritical().put("test task failure");
                });
            }

            @Override
            public boolean isBlockedForGood() {
                // The pool fails only the next task it makes.
                return false;
            }

            @Override
            public void unblock(WorkerPool pool, PageFrameReduceDispatcher dispatcher) {
                dispatcher.setBeforeNewTaskForTesting(null);
                for (int i = 0, n = leases.size(); i < n; i++) {
                    leases.getQuick(i).release();
                }
                leases.clear();
            }
        });
        Assert.assertEquals(parallel && workers == 1 ? 1 : 0, failureCount.get());
    }

    @Test
    public void testQuiescingDispatcherLeavesLaneTaskWithItsFiber() throws Exception {
        assertHeldFrameHoldsOnlyItsLane(WorkerPoolMode.FIBER_HOST, true, null, new LaneLaunchBlocker() {
            @Override
            public void block(WorkerPool pool, PageFrameReduceDispatcher dispatcher) {
                dispatcher.beginQuiesce();
            }

            @Override
            public String getQueryFailure() {
                // The owner may not publish once the dispatcher quiesces, so it stops the query.
                return "cancelled by user";
            }

            @Override
            public void unblock(WorkerPool pool, PageFrameReduceDispatcher dispatcher) {
            }
        });
    }

    @Test
    public void testUnkeyedStreamOnFibers() throws Exception {
        // Without keys there are no lanes: a fiber reduces each frame on its own.
        Assume.assumeTrue(parallel && workers > 0);
        assertMemoryLeak(() -> TestUtils.execute(new TestWorkerPool(workers, WorkerPoolMode.FIBER_HOST), (engine, compiler, context) -> {
            engine.execute("CREATE TABLE t AS (SELECT (x * 1_000_000)::TIMESTAMP ts, x FROM long_sequence(" + 64 * frameRows + ")) TIMESTAMP(ts)", context);
            final String sql = "SELECT timestamp_floor('3s', ts) bucket, count() c, sum(x) s FROM t";
            context.setStreamGroupByEnabled(false);
            final String expected = sortedLines(compileAndRead(compiler, context, sql));
            context.setStreamGroupByEnabled(true);
            try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                Assert.assertEquals(0, ((AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory)).getAtom().getLaneCount());
                Assert.assertEquals(expected, sortedLines(read(factory, context)));
            }
        }, configuration, LOG));
    }

    @Test
    public void testStoppedOwnerEndsStagedFrames() throws Exception {
        // Without workers the owner reduces every frame. It dispatches the first frame of each lane and stages the next
        // one. Once it reduces the first frame, it publishes the staged frame of that lane, which no worker took, and
        // stages the one after it; then LIMIT 1 stops it. Closing, it ends the frames in flight without reducing them,
        // and then each staged frame, once the frame before it in the lane has ended: a frame that stayed staged would
        // keep close() waiting.
        Assume.assumeTrue(parallel && workers == 0);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, Integer.MAX_VALUE);
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE t AS (SELECT (x * 1_000_000)::TIMESTAMP ts, x FROM long_sequence("
                    + 16 * frameRows + ")) TIMESTAMP(ts)", context);
            // A keyed GROUP BY, since only keys take span maps and lanes.
            final String sql = "SELECT timestamp_floor('1s', ts) bucket, x % 3 k, count() c, sum(x) s FROM t WHERE test_latched_counter() LIMIT 1";
            final IntHashSet reducedFrames = new IntHashSet();
            TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                @Override
                public boolean onGet(Record rec, int count) {
                    final long x = rec.getLong(1);
                    if ((x - 1) % frameRows == 0) {
                        synchronized (reducedFrames) {
                            reducedFrames.add((int) ((x - 1) / frameRows));
                        }
                    }
                    return true;
                }
            });
            // The context counts two workers that never run, so the query has lanes and the owner reduces every frame.
            try (
                    SqlExecutionContextImpl ownerContext = new SqlExecutionContextImpl(engine, 2).with(context.getSecurityContext());
                    RecordCursorFactory factory = compiler.compile(sql, ownerContext).getRecordCursorFactory()
            ) {
                final AsyncStreamGroupByRecordCursorFactory stream = (AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory);
                final int laneCount = stream.getAtom().getLaneCount();
                Assert.assertEquals(3, laneCount);
                Assert.assertEquals("bucket\tk\tc\ts\n1970-01-01T00:00:01.000000Z\t1\t1\t1\n", read(factory, ownerContext));
                Assert.assertEquals(1, reducedFrames.size());
                Assert.assertTrue(reducedFrames.contains(0));
                // The first frame of each lane, the staged frame behind each, and the frame staged last all ended.
                Assert.assertEquals(2 * laneCount + 1, stream.getReducedFrameCount());
            } finally {
                TestLatchedCounterFunctionFactory.reset(null);
            }
        });
    }

    @Test
    public void testRewindAndRerunKeepDoubleSums() throws Exception {
        // Floating-point sums depend on the order of their additions. The frames wholly inside a bucket add up in the
        // span maps of their lanes, so a rewind or another run adds up the same rows in the same order, however the
        // workers share the frames, and returns the same sums.
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE t AS (SELECT rnd_double() d, rnd_int(1, 2, 0) k, timestamp_sequence('2024-01-01', 7_000_000) ts"
                    + " FROM long_sequence(" + 200 * frameRows + ")) TIMESTAMP(ts) PARTITION BY DAY", context);
            for (String sql : new String[]{
                    "SELECT timestamp_floor('1h', ts) bucket, k, sum(d), avg(d) FROM t",
                    "SELECT ts, k, sum(d), avg(d) FROM t SAMPLE BY 1h",
                    "SELECT ts, k, sum(d), avg(d) FROM t WHERE d > 0.01 SAMPLE BY 1h",
                    "SELECT ts, k, sum(d), avg(d) FROM t SAMPLE BY 1h ORDER BY ts DESC"
            }) {
                try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                    assertStream(factory, true);
                    final String expected = sortedLines(read(factory, context));
                    final StringSink sink = new StringSink();
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        for (int i = 0; i < 20; i++) {
                            sink.clear();
                            CursorPrinter.println(cursor, factory.getMetadata(), sink, true, false);
                            Assert.assertEquals(sql, expected, sortedLines(sink));
                            cursor.toTop();
                        }
                    }
                    for (int i = 0; i < 5; i++) {
                        Assert.assertEquals(sql, expected, sortedLines(read(factory, context)));
                    }
                }
            }
        });
    }

    @Test
    public void testUnboundedDispatchLimitDispatchesFirstWindow() throws Exception {
        // The first window adds a frame for the owner to the parallel filter limit, which must not overflow: an
        // overflowed window dispatched nothing, and the owner reduced the first frame itself.
        Assume.assumeTrue(parallel);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, Integer.MAX_VALUE);
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE t AS (SELECT (x * 1_000_000)::TIMESTAMP ts, x FROM long_sequence("
                    + 8 * frameRows + ")) TIMESTAMP(ts)", context);
            final String sql = "SELECT ts, count() c, sum(x) s FROM t SAMPLE BY 3s";
            context.setStreamGroupByEnabled(false);
            final String expected = compileAndRead(compiler, context, sql);
            context.setStreamGroupByEnabled(true);
            try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                final AsyncStreamGroupByRecordCursorFactory stream = (AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory);
                Assert.assertEquals(expected, read(factory, context));
                Assert.assertEquals(0, stream.getLocallyReducedFrameCount());
            }
        });
    }

    @Test
    public void testStreamGroupByDisabled() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            for (String sql : new String[]{
                    "SELECT timestamp_floor('1m', ts) bucket, sym, count(*), sum(v) FROM trades ORDER BY bucket, sym",
                    "SELECT ts, sym, count(*), sum(v) FROM trades SAMPLE BY 1m ORDER BY ts, sym",
                    "SELECT ts, sym, sum(v) FROM trades SAMPLE BY 1m FILL(PREV) ORDER BY ts, sym",
                    "SELECT ts, sym, avg(d) FROM trades SAMPLE BY 1m FILL(LINEAR) ORDER BY ts, sym"
            }) {
                final String expected;
                try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                    Assert.assertTrue(sql, plan(factory, context).contains("Stream Group By"));
                    expected = read(factory, context);
                }
                context.setStreamGroupByEnabled(false);
                try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                    Assert.assertFalse(sql, plan(factory, context).contains("Stream Group By"));
                    Assert.assertEquals(sql, expected, read(factory, context));
                } finally {
                    context.setStreamGroupByEnabled(true);
                }
            }
        });
    }

    @Test
    public void testFilteredInput() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            for (String keys : new String[]{"", ", sym"}) {
                final String sql = "select timestamp_floor('1m', ts) bucket" + keys + ", count(*), sum(v), first(v), last(v) "
                        + "from $table where v % 7 = 0 order by bucket" + keys;
                assertMatchesHash(compiler, context, sql);
            }
        });
    }

    @Test
    public void testFilteredBindVariables() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_DISABLED, SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_FORCE_SCALAR}) {
                context.setJitMode(jitMode);
                context.getBindVariableService().setLong(0, 0);
                final String sql = "SELECT timestamp_floor('1m', ts) bucket, sym, count(*), sum(v), first(v), last(v) "
                        + "FROM $table WHERE v > $1 ORDER BY bucket, sym";
                try (RecordCursorFactory expected = compiler.compile(sql.replace("$table", "hash_input"), context).getRecordCursorFactory();
                     RecordCursorFactory actual = compiler.compile(sql.replace("$table", "trades"), context).getRecordCursorFactory()) {
                    assertStream(actual, true);
                    for (RecordCursorFactory f = actual; f != null; f = f.getBaseFactory()) {
                        if (f instanceof AsyncStreamGroupByRecordCursorFactory stream) {
                            Assert.assertEquals(JitUtil.isJitSupported() && jitMode != SqlJitMode.JIT_MODE_DISABLED,
                                    stream.usesCompiledFilter());
                        }
                    }
                    for (long limit : new long[]{1002, 70, 900, 0}) {
                        context.getBindVariableService().setLong(0, limit);
                        final String expectedText = read(expected, context);
                        Assert.assertEquals(expectedText, read(actual, context));
                        Assert.assertEquals(expectedText, read(actual, context));
                    }
                }
            }
        });
    }

    @Test
    public void testFilteredParquetAndEmptyFrames() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            for (int format = 0; format < 3; format++) {
                if (format == 1) {
                    convertFirstDayToParquet(engine, context);
                } else if (format == 2) {
                    engine.execute("ALTER TABLE trades ALTER COLUMN v TYPE STRING", context);
                }
                for (String filter : new String[]{"v::long < 0", "v::long % 101 = 0",
                        "v::long > 70 AND v::long < 310", "v IS NULL OR v::long >= 0"}) {
                    for (String keys : new String[]{"", ", sym"}) {
                        assertMatchesHash(compiler, context, "SELECT timestamp_floor('1m', ts) bucket" + keys
                                + ", count(*), sum(v::long), avg(d), first(v::long), last(v::long) "
                                + "FROM $table WHERE " + filter + " ORDER BY bucket" + keys);
                    }
                }
            }
        });
    }

    @Test
    public void testFilteredParquetFramesMatchHash() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        run((engine, compiler, context) -> {
            for (String table : new String[]{"trades", "hash_input"}) {
                final String timestamp = table.equals("trades") ? " TIMESTAMP(ts) PARTITION BY DAY" : "";
                engine.execute("CREATE TABLE " + table + " AS (SELECT timestamp_sequence('2024-01-01', 60_000_000L) ts, x v, "
                        + "(x % 7)::SYMBOL sym, rpad(x::VARCHAR, (x % 40)::INT + 1, 'abc') s FROM long_sequence(3_000))" + timestamp, context);
            }
            convertToParquet(engine, context, "2024-01-01");
            for (String table : new String[]{"trades", "hash_input"}) {
                engine.execute("ALTER TABLE " + table + " ADD COLUMN late LONG", context);
                engine.execute("INSERT INTO " + table + " (ts, v, sym, s, late) VALUES ('2024-01-03T12:00', 1, 'A', 'tail', 5)", context);
            }
            TestUtils.drainWalQueue(engine);
            for (String filter : new String[]{"", " WHERE v % 97 = 0", " WHERE v % 2 = 0", " WHERE length(s) > 30", " WHERE late = 5"}) {
                for (String bucket : new String[]{"1h", "1d"}) {
                    assertMatchesHash(compiler, context, "SELECT timestamp_floor('" + bucket + "', ts) bucket, count(*), sum(v), "
                            + "sum(length(s)), first(v), last(v), count(late) FROM $table" + filter + " ORDER BY bucket");
                    assertMatchesHash(compiler, context, "SELECT timestamp_floor('" + bucket + "', ts) bucket, sym, count(*), sum(v), "
                            + "sum(length(s)), first(v), last(v), count(late) FROM $table" + filter + " ORDER BY bucket, sym");
                }
            }
        });
    }

    @Test
    public void testRepeatedGroupBySubquery() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            final String sql = "WITH g AS (SELECT timestamp_floor('1m', ts) bucket, sum(v) s FROM $table) "
                    + "SELECT a.bucket, a.s, b.s FROM g a JOIN g b ON a.bucket = b.bucket ORDER BY a.bucket";
            try (RecordCursorFactory expected = compiler.compile(sql.replace("$table", "hash_input"), context).getRecordCursorFactory();
                 RecordCursorFactory actual = compiler.compile(sql.replace("$table", "trades"), context).getRecordCursorFactory()) {
                final String expectedText = read(expected, context);
                for (int iteration = 0; iteration < 3; iteration++) {
                    Assert.assertEquals(expectedText, read(actual, context));
                }
                final String plan = TestUtils.printSqlToString(engine, context,
                        "EXPLAIN " + sql.replace("$table", "trades"), new StringSink());
                Assert.assertTrue(plan, plan.contains("Stream Group By"));
            }
        });
    }

    @Test
    public void testCalendarBuckets() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            for (String key : new String[]{"date_trunc('month', ts)", "timestamp_floor('1y', ts)",
                    "timestamp_floor('15m', ts)", "timestamp_floor_utc('1d', ts, null, '00:00', null)",
                    "timestamp_floor('3M', ts)", "timestamp_floor('5y', ts)", "timestamp_floor('3w', ts)",
                    "timestamp_ceil('h', ts)", "year(ts)", "ts::long"}) {
                assertMatchesHash(compiler, context, "select " + key + " bucket, count(*), sum(v), avg(d) from $table order by bucket");
            }
        });
    }

    @Test
    public void testByteAndBooleanDistinct() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE trades AS (SELECT timestamp_sequence(86_398_976_000, 1000L) ts, "
                    + "(x - 129)::byte b, x % 2 = 0 flag, (x % 3)::symbol sym "
                    + "FROM long_sequence(2048)) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("CREATE TABLE hash_input (ts timestamp, b byte, flag boolean, sym symbol)", context);
            engine.execute("INSERT INTO hash_input SELECT * FROM trades", context);
            final String values = "count(*) n, count_distinct(b) bytes, count(DISTINCT flag) flags, "
                    + "approx_count_distinct(b) approx, approx_count_distinct(b, 4) low, "
                    + "approx_count_distinct(b, 18) high, sum(b::int) total";
            final String sql = "SELECT timestamp_floor('512T', ts) bucket, " + values + " FROM $table ORDER BY bucket";
            for (int iteration = 0; iteration < 2; iteration++) {
                if (iteration == 1) {
                    convertToParquet(engine, context, "1970-01-01");
                }
                assertMatchesHash(compiler, context, sql);
                assertMatchesHash(compiler, context, "SELECT timestamp_floor('512T', ts) bucket, sym, "
                        + values + " FROM $table ORDER BY bucket, sym");
                assertMatchesHash(compiler, context, "SELECT timestamp_floor('512T', ts) bucket, sym, "
                        + values + " FROM $table WHERE b < 0 OR flag ORDER BY bucket, sym");
                assertQuery(sql.replace("$table", "trades")).withEngine(engine).withContext(context)
                        .noLeakCheck().timestamp("bucket").noRandomAccess().returns("""
                                bucket\tn\tbytes\tflags\tapprox\tlow\thigh\ttotal
                                1970-01-01T23:59:58.976000Z\t512\t256\t2\t256\t256\t256\t-256
                                1970-01-01T23:59:59.488000Z\t512\t256\t2\t256\t256\t256\t-256
                                1970-01-02T00:00:00.000000Z\t512\t256\t2\t256\t256\t256\t-256
                                1970-01-02T00:00:00.512000Z\t512\t256\t2\t256\t256\t256\t-256
                                """);
            }
        });
    }

    @Test
    public void testByteAndBooleanDistinctColumnTop() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE trades AS (SELECT timestamp_sequence(86_397_000_000, 1_000_000L) ts "
                    + "FROM long_sequence(2)) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("ALTER TABLE trades ADD COLUMN b BYTE, flag BOOLEAN", context);
            engine.execute("INSERT INTO trades VALUES (86_400_000_000, -128, false), (86_400_000_000, 0, true), "
                    + "(86_400_000_000, 127, false), (86_401_000_000, null, null)", context);
            engine.execute("CREATE TABLE hash_input (ts timestamp, b byte, flag boolean)", context);
            engine.execute("INSERT INTO hash_input SELECT * FROM trades", context);
            final String values = "count(*) n, count_distinct(b) bytes, count_distinct(flag) flags, approx_count_distinct(b) approx";
            for (int iteration = 0; iteration < 2; iteration++) {
                if (iteration == 1) {
                    convertToParquet(engine, context, "1970-01-01");
                }
                assertMatchesHash(compiler, context, "SELECT timestamp_floor('1s', ts) bucket, "
                        + values + " FROM $table ORDER BY bucket");
                assertMatchesHash(compiler, context, "SELECT timestamp_floor('1s', ts) bucket, "
                        + values + " FROM $table WHERE b = 64 AND NOT flag ORDER BY bucket");
                final String sql = "SELECT ts, " + values + " FROM trades SAMPLE BY 1s FILL(PREV) ALIGN TO CALENDAR";
                try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                    assertStream(factory, true);
                }
                assertQuery(sql).withEngine(engine).withContext(context)
                        .noLeakCheck().timestamp("ts").noRandomAccess().returns("""
                                ts\tn\tbytes\tflags\tapprox
                                1970-01-01T23:59:57.000000Z\t1\t1\t1\t1
                                1970-01-01T23:59:58.000000Z\t1\t1\t1\t1
                                1970-01-01T23:59:59.000000Z\t1\t1\t1\t1
                                1970-01-02T00:00:00.000000Z\t3\t3\t2\t3
                                1970-01-02T00:00:01.000000Z\t1\t1\t1\t1
                                """);
            }
        });
    }

    @Test
    public void testConstantTimestampShifts() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            for (String key : new String[]{
                    "dateadd('h', 1, ts)",
                    "dateadd('H', 1, ts)",
                    "dateadd('u', 1, ts)",
                    "dateadd('M', 0, ts)",
                    "dateadd('w', -1, ts)",
                    "dateadd('h', 1, ts, '+05:30')",
                    "dateadd('u', 1, ts, '+05:30')",
                    "dateadd('y', 0, ts, '+05:30')",
                    "ts + 1000L",
                    "ts - 1000L",
                    "to_timezone(ts, '+05:30')",
                    "to_utc(ts, '+05:30')",
                    "timestamp_floor_utc('1h', ts, null, '00:00', '+05:30')",
                    "timestamp_floor_utc('1d', ts, null, '00:00', '-04:00')",
                    "timestamp_floor('1h', ts, null, '00:00', '+05:30')"
            }) {
                assertMatchesHash(compiler, context,
                        "select " + key + " bucket, count(*), sum(v), first(v), last(v) from $table order by bucket");
            }
        });
    }

    @Test
    public void testDstSubDayBuckets() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("create table trades (ts timestamp, v long) timestamp(ts) partition by day", context);
            for (String start : new String[]{"2024-03-10T05:00:00", "2024-11-03T04:00:00"}) {
                engine.execute("insert into trades select timestamp_sequence('" + start + "'::timestamp, 60_000_000L), x from long_sequence(600)", context);
            }
            engine.execute("create table hash_input (ts timestamp, v long)", context);
            engine.execute("insert into hash_input select * from trades", context);
            for (String bucket : new String[]{"17m", "1h", "3h"}) {
                assertMatchesHash(compiler, context,
                        "select timestamp_floor_utc('" + bucket + "', ts, null, '00:00', 'America/New_York') bucket, "
                                + "count(*), sum(v), first(v), last(v) from $table order by bucket");
            }
        });
    }

    @Test
    public void testTemporalJoinKeepsMixedKeyTypes() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE q AS (SELECT timestamp_sequence('2024-01-01', 7_000_000) ts, ('S' || (x % 5))::SYMBOL sym, "
                    + "timestamp_sequence('2024-01-01', 60_000_000) kts, x::DOUBLE bid FROM long_sequence(600)) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("CREATE TABLE t AS (SELECT timestamp_sequence('2024-01-01', 11_000_000) ts, ('S' || (x % 7))::VARCHAR v, ('S' || (x % 7))::STRING s, "
                    + "timestamp_sequence('2024-01-01', 60_000_000)::TIMESTAMP_NS kns FROM long_sequence(400)) TIMESTAMP(ts) PARTITION BY DAY", context);
            for (boolean isFullFat : new boolean[]{false, true}) {
                compiler.setFullFatJoins(isFullFat);
                for (String join : new String[]{"ASOF", "LT"}) {
                    for (String[] key : new String[][]{{"sym", "v", "SYMBOL"}, {"sym", "s", "SYMBOL"}, {"kts", "kns", "TIMESTAMP"}}) {
                        final String bars = "SELECT ts, " + key[0] + ", last(bid) bid FROM q SAMPLE BY 1m";
                        final String sql = "SELECT t.ts, b." + key[0] + ", b.bid FROM t " + join + " JOIN (" + bars + ") b ON b." + key[0] + " = t." + key[1];
                        context.setStreamGroupByEnabled(false);
                        final String expected;
                        final String expectedType;
                        try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                            expected = read(factory, context);
                            expectedType = ColumnType.nameOf(factory.getMetadata().getColumnType(1));
                        }
                        context.setStreamGroupByEnabled(true);
                        // Full-fat joins expose the key in its partner's type either way, so they copy nothing.
                        final String plan = compileAndRead(compiler, context, "EXPLAIN " + sql);
                        Assert.assertEquals(plan, !isFullFat, plan.contains("Materialize"));
                        try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                            Assert.assertEquals(sql, isFullFat ? expectedType : key[2], ColumnType.nameOf(factory.getMetadata().getColumnType(1)));
                            for (int pass = 0; pass < 2; pass++) {
                                Assert.assertEquals(sql, expected, read(factory, context));
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testTimestampDomainBounds() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE trades (ts timestamp, v long) TIMESTAMP(ts) PARTITION BY YEAR", context);
            engine.execute("INSERT INTO trades VALUES (0::timestamp, 1), (1::timestamp, 2), "
                    + "('1970-01-01T23:59:59.999999', 3), ('1999-12-31T23:59:59.999999', 4), "
                    + "('2000-02-29T23:59:59.999999', 5), ('9999-12-31T23:59:59.999999', 6)", context);
            engine.execute("CREATE TABLE hash_input (ts timestamp, v long)", context);
            engine.execute("INSERT INTO hash_input SELECT * FROM trades", context);
            for (String key : new String[]{"year(ts)", "timestamp_floor('3M', ts)", "timestamp_floor('5y', ts)",
                    "timestamp_floor('2147483647M', ts)", "timestamp_floor('3w', ts)", "timestamp_ceil('y', ts)",
                    "date_trunc('millennium', ts)", "dateadd('h', 24, ts)", "ts - 1000L",
                    "timestamp_floor_utc('3M', ts, null, '00:00', '+05:30')"}) {
                assertMatchesHash(compiler, context, "SELECT " + key + " bucket, count(*), sum(v), first(v), last(v) "
                        + "FROM $table ORDER BY bucket");
            }
        });
    }

    @Test
    public void testTimestampOverrideUsesHash() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("create table trades (ts timestamp, other timestamp, v long) timestamp(ts) partition by day", context);
            engine.execute("insert into trades values "
                    + "('2024-01-01', '2024-01-02', 1), "
                    + "('2024-01-02', null, 2), "
                    + "('2024-01-03', '2024-01-01', 3), "
                    + "('2024-01-04', '2024-01-02', 4), "
                    + "('2024-01-05', null, 5)", context);
            for (String source : new String[]{"trades", "(select v, other, ts from trades)"}) {
                try (RecordCursorFactory factory = compiler.compile(
                        "select other, count(*), sum(v) from " + source + " timestamp(other) sample by 1d", context).getRecordCursorFactory()) {
                    assertStream(factory, false);
                    Assert.assertEquals("other\tcount\tsum\n"
                            + "\t2\t7\n"
                            + "2024-01-01T00:00:00.000000Z\t1\t3\n"
                            + "2024-01-02T00:00:00.000000Z\t2\t5\n", read(factory, context));
                }
            }
        });
    }

    @Test
    public void testColumnTopsAndExpressions() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            engine.execute("alter table trades add column late double", context);
            engine.execute("insert into trades values ('2024-01-02T00:00:00', 5, 4.0, 'A', 9.0)", context);
            engine.execute("drop table hash_input", context);
            engine.execute("create table hash_input (ts timestamp, v long, d double, sym symbol, late double)", context);
            engine.execute("insert into hash_input select * from trades", context);
            assertMatchesHash(compiler, context,
                    "select timestamp_floor('1d', ts) bucket, sum(late), sum(v + 1), avg(d * 2), first(v), last(v) from $table order by bucket");
            assertMatchesHash(compiler, context,
                    "select timestamp_floor('1d', ts) bucket, count(*), sum(late), avg(late), first(late), last(late) "
                            + "from $table where late is null or late > 0 order by bucket");
        });
    }

    @Test
    public void testEmptyAndSingleRow() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("create table trades (ts timestamp, v long) timestamp(ts)", context);
            engine.execute("create table hash_input (ts timestamp, v long)", context);
            final String sql = "select timestamp_floor('1m', ts) bucket, count(*), sum(v) from $table order by bucket";
            assertMatchesHash(compiler, context, sql);
            engine.execute("insert into trades values ('2024-01-01', null)", context);
            engine.execute("insert into hash_input select * from trades", context);
            assertMatchesHash(compiler, context, sql);
        });
    }

    @Test
    public void testEveryRowIsAGroup() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            assertMatchesHash(compiler, context,
                    "select timestamp_floor('1U', ts) bucket, count(*), sum(v), avg(d) from $table order by bucket");
        });
    }

    @Test
    public void testLatestOnOverKeyedSampleBy() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("""
                    CREATE TABLE t AS (
                        SELECT (CASE WHEN x <= 8 THEN '2024-01-01T00:00' ELSE '2024-01-01T01:00' END)::TIMESTAMP + x ts,
                               ('k' || CASE WHEN x <= 8 THEN x ELSE x - 4 END)::SYMBOL k
                        FROM long_sequence(112)
                    ) TIMESTAMP(ts) PARTITION BY DAY""", context);
            engine.execute("CREATE TABLE hash_t (ts TIMESTAMP, k SYMBOL)", context);
            engine.execute("INSERT INTO hash_t SELECT * FROM t", context);
            final String latest = """
                    SELECT a.ts, a.k, a.c FROM (SELECT timestamp_floor('1h', ts) ts, k, count() c FROM hash_t) a
                    JOIN (SELECT k, max(timestamp_floor('1h', ts)) m FROM hash_t) b ON a.k = b.k AND a.ts = b.m""";
            final String byKey = compileAndRead(compiler, context, latest + " ORDER BY a.k");
            final String byTs = compileAndRead(compiler, context, latest + " ORDER BY a.ts, a.k LIMIT 6");
            for (String[] sql : new String[][]{
                    {"SELECT * FROM ((SELECT ts, k, count() c FROM t SAMPLE BY 1h) LATEST ON ts PARTITION BY k) ORDER BY k", byKey},
                    {"SELECT * FROM ((SELECT timestamp_floor('1h', ts) ts, k, count() c FROM t ORDER BY ts) LATEST ON ts PARTITION BY k) ORDER BY k", byKey},
                    {"SELECT * FROM ((SELECT ts, k, count() c FROM t SAMPLE BY 1h ORDER BY ts DESC) LATEST ON ts PARTITION BY k) ORDER BY ts, k LIMIT 6", byTs}
            }) {
                try (RecordCursorFactory factory = compiler.compile(sql[0], context).getRecordCursorFactory()) {
                    for (int i = 0; i < 3; i++) {
                        Assert.assertEquals(sql[0], sql[1], read(factory, context));
                    }
                }
            }
        });
    }

    @Test
    public void testLatestOnKeepsDesignatedTimestamp() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("INSERT INTO t VALUES ('2024-01-01T00:00', 'a', 1), ('2024-01-01T00:40', 'b', 2), ('2024-01-01T01:10', 'a', 3)", context);
            engine.execute("CREATE TABLE q (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("INSERT INTO q VALUES ('2024-01-01T00:50', 7)", context);
            final String prev = "(SELECT * FROM (SELECT ts, sym, sum(v) s FROM t SAMPLE BY 30m FILL(PREV)) LATEST ON ts PARTITION BY sym)";
            for (String sql : new String[]{
                    prev,
                    "SELECT * FROM (SELECT ts, sym, sum(v) s FROM t SAMPLE BY 30m FILL(NULL)) LATEST ON ts PARTITION BY sym",
                    "SELECT * FROM (SELECT ts, sym, sum(v) s FROM t SAMPLE BY 30m) LATEST ON ts PARTITION BY sym",
                    "SELECT * FROM (SELECT * FROM (SELECT ts, sym, sum(v) s FROM t SAMPLE BY 30m)) LATEST ON ts PARTITION BY sym"
            }) {
                try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                    assertStream(factory, true);
                    Assert.assertEquals(sql, 0, factory.getMetadata().getTimestampIndex());
                }
            }
            assertQuery("SELECT ts, sum(s) s FROM " + prev + " SAMPLE BY 1h")
                    .withEngine(engine).withContext(context).noLeakCheck().timestamp("ts").noRandomAccess()
                    .returns("ts\ts\n2024-01-01T01:00:00.000000Z\t5\n");
            assertQuery("SELECT l.sym, l.s, r.x FROM " + prev + " l ASOF JOIN q r ORDER BY l.sym")
                    .withEngine(engine).withContext(context).noLeakCheck().expectSize()
                    .returns("sym\ts\tx\na\t3\t7\nb\t2\t7\n");
            engine.execute("CREATE TABLE c AS " + prev + " PARTITION BY DAY", context);
            assertQuery("SELECT designatedTimestamp, partitionBy FROM tables() WHERE table_name = 'c'")
                    .withEngine(engine).withContext(context).noLeakCheck().noRandomAccess().sizeMayVary()
                    .returns("designatedTimestamp\tpartitionBy\nts\tDAY\n");
            assertQuery("SELECT sym, s FROM c ORDER BY sym")
                    .withEngine(engine).withContext(context).noLeakCheck().expectSize()
                    .returns("sym\ts\na\t3\nb\t2\n");
        });
    }

    @Test
    public void testLimitAndAggregateFilter() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            assertMatchesHash(compiler, context,
                    "select timestamp_floor('1m', ts) bucket, count(*), sum(v) from $table order by bucket limit 3");
            assertMatchesHash(compiler, context,
                    "select * from (select timestamp_floor('1m', ts) bucket, count(*) c, sum(v) from $table) where c > 10 order by bucket limit 2, 5");
            assertMatchesHash(compiler, context,
                    "select timestamp_floor('1m', ts) bucket, count(*), sum(v) from $table order by bucket desc limit 3");
        });
    }

    @Test
    public void testDescendingOrderScansBackward() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            for (String bucket : new String[]{"1d", "1m", "7s", "1U"}) {
                for (String keys : new String[]{"", ", sym"}) {
                    for (String filter : new String[]{"", " WHERE v % 7 <> 0"}) {
                        assertMatchesHash(compiler, context, "SELECT timestamp_floor('" + bucket + "', ts) bucket" + keys
                                + ", count(*), sum(v), min(d), max(d), first(v), last(v), first_not_null(v), last_not_null(v) "
                                + "FROM $table" + filter + " ORDER BY bucket DESC" + keys);
                    }
                }
            }
            assertMatchesHash(compiler, context,
                    "SELECT timestamp_floor('1m', ts) bucket, sym, count(*), last(v) FROM $table ORDER BY bucket DESC, sym LIMIT 3, 7");
            assertMatchesHash(compiler, context,
                    "SELECT * FROM (SELECT timestamp_floor('1m', ts) bucket, count(*), last(v) FROM $table) ORDER BY bucket DESC LIMIT 4");

            final String expected;
            try (RecordCursorFactory factory = compiler.compile(
                    "SELECT timestamp_floor('1m', ts) ts, sum(v), first(v) FROM hash_input ORDER BY ts DESC", context).getRecordCursorFactory()) {
                expected = read(factory, context);
            }
            final QueryAssertion sampleBy = assertQuery("SELECT ts, sum(v), first(v) FROM trades SAMPLE BY 1m ORDER BY ts DESC")
                    .withEngine(engine).withContext(context).noLeakCheck().timestampDesc("ts");
            if (parallel) {
                sampleBy.noRandomAccess().withPlan("""
                        Async Stream Group By workers: %d
                          keys: [ts]
                          keyFunctions: [timestamp_floor_utc('1m',ts)]
                          values: [sum(v),first(v)]
                            PageFrame
                                Row backward scan
                                Frame backward scan on: trades
                        """.formatted(Math.max(1, workers)));
            } else {
                sampleBy.withPlan("""
                        Encode sort
                          keys: [ts desc]
                            Stream Group By
                              keys: [ts]
                              keyFunctions: [timestamp_floor_utc('1m',ts)]
                              values: [sum(v),first(v)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: trades
                        """);
            }
            sampleBy.returns(expected);

            final String fillPlan = TestUtils.printSqlToString(engine, context,
                    "EXPLAIN SELECT ts, sum(v) FROM trades SAMPLE BY 1m FILL(NULL) ORDER BY ts DESC", new StringSink());
            Assert.assertTrue(fillPlan, fillPlan.contains("Frame forward scan"));
        });
    }

    @Test
    public void testDescendingOrderMatchesHashGroupByAroundJoinsAndUnions() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            final String sampleBy = "(SELECT ts, sum(v) s, last(v) l FROM trades SAMPLE BY 1m)";
            for (String sql : new String[]{
                    "SELECT * FROM " + sampleBy + " a ASOF JOIN trades b ORDER BY a.ts DESC",
                    "SELECT * FROM " + sampleBy + " a LT JOIN trades b ORDER BY a.ts DESC LIMIT 4",
                    "SELECT * FROM " + sampleBy + " a SPLICE JOIN " + sampleBy + " b ORDER BY a.ts DESC LIMIT 4",
                    "SELECT * FROM (" + sampleBy + " UNION ALL (SELECT ts, sum(v) s, first(v) l FROM trades SAMPLE BY 7m)) ORDER BY ts DESC, s",
                    "SELECT ts, s, sum(s) OVER (ORDER BY ts) FROM " + sampleBy + " ORDER BY ts DESC",
                    "SELECT * FROM (SELECT ts, sum(v) s FROM trades SAMPLE BY 1m ORDER BY ts DESC) LIMIT 3",
                    "SELECT * FROM (SELECT ts, sum(v) s FROM trades SAMPLE BY 1m ORDER BY ts DESC) ORDER BY ts",
                    "SELECT first(s), last(s) FROM (SELECT ts, sum(v) s FROM trades SAMPLE BY 1m ORDER BY ts DESC)",
            }) {
                context.setStreamGroupByEnabled(false);
                final String expected = compileAndRead(compiler, context, sql);
                context.setStreamGroupByEnabled(true);
                Assert.assertEquals(sql, expected, compileAndRead(compiler, context, sql));
            }
            final String overDescending = "SELECT ts, sum(s) FROM (SELECT ts, sum(v) s FROM trades SAMPLE BY 1m ORDER BY ts DESC) SAMPLE BY 1h";
            try {
                compileAndRead(compiler, context, overDescending);
                Assert.fail(overDescending);
            } catch (SqlException e) {
                TestUtils.assertEquals("base query does not provide ASC order over designated TIMESTAMP column", e.getFlyweightMessage());
            }
        });
    }

    @Test
    public void testLongTopKKeepsHashGroupBy() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            final String minutes = "SELECT ts, sym, sum(v) s, max(ts) m FROM trades SAMPLE BY 1m";
            for (String sql : new String[]{
                    minutes + " ORDER BY s DESC LIMIT 5",
                    minutes + " ORDER BY s LIMIT 1",
                    minutes + " ORDER BY m DESC LIMIT 3",
                    "SELECT * FROM (" + minutes + ") ORDER BY s DESC LIMIT 5",
                    "SELECT ts, s FROM (" + minutes + ") ORDER BY s DESC LIMIT 5",
                    "SELECT ts, s, s + 1 x FROM (" + minutes + ") ORDER BY s DESC LIMIT 5",
                    "SELECT ts FROM (" + minutes + ") ORDER BY s DESC LIMIT 5",
                    "SELECT * FROM (SELECT * FROM (" + minutes + ")) ORDER BY s DESC LIMIT 5",
                    "SELECT x.ts, x.s total FROM (" + minutes + ") x ORDER BY total DESC LIMIT 5",
                    "SELECT timestamp_floor('1m', ts) b, sym, sum(v) s FROM trades ORDER BY s DESC LIMIT 5",
                    "SELECT ts, sum(v) s FROM trades SAMPLE BY 1m ORDER BY s DESC LIMIT 3",
            }) {
                final String plan = compileAndRead(compiler, context, "EXPLAIN " + sql);
                TestUtils.assertContains(plan, "Long Top K");
                Assert.assertFalse(sql + '\n' + plan, plan.contains("Stream Group By"));
            }
        });
    }

    @Test
    public void testLongTopKKeepsHashGroupByForTimestampKeyOtherThanBucket() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE fills AS (SELECT "
                    + "timestamp_sequence('2024-01-01T00:00:00'::TIMESTAMP, 1_000_000L) ts, "
                    + "dateadd('s', ((x * 7_919) % 1_001)::INT, '2023-12-31T00:00:00'::TIMESTAMP) order_ts, "
                    + "x qty "
                    + "FROM long_sequence(1_001)) TIMESTAMP(ts) PARTITION BY DAY", context);
            final String keys = "SELECT timestamp_floor('1m', ts) m, order_ts, count(), sum(qty) s FROM fills";
            for (String sql : new String[]{
                    keys + " ORDER BY order_ts DESC LIMIT 2",
                    keys + " ORDER BY order_ts LIMIT 2",
                    "SELECT * FROM (" + keys + ") ORDER BY order_ts DESC LIMIT 2",
                    "SELECT m, order_ts o, s FROM (" + keys + ") ORDER BY o DESC LIMIT 2",
            }) {
                final String plan = compileAndRead(compiler, context, "EXPLAIN " + sql);
                TestUtils.assertContains(plan, "Long Top K");
                Assert.assertFalse(sql + '\n' + plan, plan.contains("Stream Group By"));
            }
            TestUtils.assertEquals(
                    """
                            m\torder_ts\tcount\ts
                            2024-01-01T00:00:00.000000Z\t2023-12-31T00:16:40.000000Z\t1\t45
                            2024-01-01T00:01:00.000000Z\t2023-12-31T00:16:39.000000Z\t1\t90
                            """,
                    compileAndRead(compiler, context, keys + " ORDER BY order_ts DESC LIMIT 2")
            );
            TestUtils.assertEquals(
                    """
                            m\torder_ts\tcount\ts
                            2024-01-01T00:16:00.000000Z\t2023-12-31T00:00:00.000000Z\t1\t1001
                            """,
                    compileAndRead(compiler, context, keys + " ORDER BY order_ts LIMIT 1")
            );
            final String buckets = "SELECT timestamp_floor('1m', ts) m, count(), sum(qty) s FROM fills";
            for (String sql : new String[]{
                    buckets + " ORDER BY m DESC LIMIT 3",
                    buckets + " ORDER BY m LIMIT 3",
                    "SELECT m, order_ts, s, dateadd('s', 1, order_ts) t FROM (" + keys + ") ORDER BY t DESC LIMIT 3",
            }) {
                context.setStreamGroupByEnabled(false);
                final String expected = compileAndRead(compiler, context, sql);
                context.setStreamGroupByEnabled(true);
                TestUtils.assertContains(compileAndRead(compiler, context, "EXPLAIN " + sql), "Stream Group By");
                Assert.assertEquals(sql, expected, compileAndRead(compiler, context, sql));
            }
        });
    }

    @Test
    public void testLongTopKResolvesDottedAliases() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED, "true");
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            context.getBindVariableService().clear();
            context.getBindVariableService().setLong(0, 3);
            TestUtils.assertEquals(
                    """
                            ts\ts.x
                            2024-01-01T00:15:00.000000Z\t50253
                            2024-01-01T00:14:00.000000Z\t47885
                            2024-01-01T00:13:00.000000Z\t43779
                            """,
                    compileAndRead(compiler, context, "SELECT ts, sum(v) AS \"s.x\" FROM trades SAMPLE BY 1m ORDER BY 2 DESC LIMIT 3")
            );
            for (String sql : new String[]{
                    "SELECT ts, sum(v) AS \"s.x\" FROM trades SAMPLE BY 1m ORDER BY 2 DESC LIMIT 3",
                    "SELECT ts, sum(t.v) FROM trades t SAMPLE BY 1m ORDER BY 2 DESC LIMIT 3",
                    "SELECT ts, sym, sum(v) AS \"s.x\" FROM trades WHERE v > 0 SAMPLE BY 1m ORDER BY 3 LIMIT 3",
                    "SELECT * FROM (SELECT ts, sym, sum(v) AS \"s.x\" FROM trades SAMPLE BY 1m) ORDER BY 3 DESC LIMIT 3",
                    "SELECT timestamp_floor('1m', ts) b, sum(v) AS \"s.x\" FROM trades ORDER BY 2 LIMIT 3",
                    "SELECT ts, max(t.ts) FROM trades t SAMPLE BY 1m ORDER BY 2 DESC LIMIT 3",
            }) {
                final String plan = compileAndRead(compiler, context, "EXPLAIN " + sql);
                TestUtils.assertContains(plan, "Long Top K");
                Assert.assertFalse(sql + '\n' + plan, plan.contains("Stream Group By"));
                context.setStreamGroupByEnabled(false);
                final String expected = compileAndRead(compiler, context, sql);
                context.setStreamGroupByEnabled(true);
                Assert.assertEquals(sql, expected, compileAndRead(compiler, context, sql));
            }
            for (String sql : new String[]{
                    "SELECT ts, sum(v * 1.5) FROM trades SAMPLE BY 1m ORDER BY 2 DESC LIMIT 3",
                    "SELECT ts, avg(d * 1.5), sum(v) FROM trades SAMPLE BY 1m ORDER BY 2 DESC, 3 LIMIT 3",
                    "SELECT ts, sum(v) AS \"s.x\" FROM trades SAMPLE BY 1m ORDER BY 2 DESC LIMIT -3",
                    "SELECT ts, sum(v) AS \"s.x\" FROM trades SAMPLE BY 1m ORDER BY 2 DESC LIMIT $1",
                    "SELECT * FROM (SELECT ts, sum(v * 1.5) FROM trades SAMPLE BY 1m) ORDER BY 2 DESC LIMIT 3",
            }) {
                TestUtils.assertContains(compileAndRead(compiler, context, "EXPLAIN " + sql), "Stream Group By");
                context.setStreamGroupByEnabled(false);
                final String expected = compileAndRead(compiler, context, sql);
                context.setStreamGroupByEnabled(true);
                Assert.assertEquals(sql, expected, compileAndRead(compiler, context, sql));
            }
            final String sql = "SELECT sym, sum(v * 1.5) FROM trades ORDER BY 2 DESC LIMIT 1";
            TestUtils.assertEquals("sym\tsum(v * 1.5)\nA\t341595.0\n", compileAndRead(compiler, context, sql));
        });
    }

    @Test
    public void testOtherOrderByLimitShapesKeepStreamGroupBy() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            context.getBindVariableService().clear();
            context.getBindVariableService().setLong(0, 3);
            final String minutes = "SELECT ts, sym, sum(v) s, avg(v) a, max(ts) m FROM trades SAMPLE BY 1m";
            for (String sql : new String[]{
                    "SELECT ts, sum(v) s FROM trades SAMPLE BY 1m ORDER BY ts DESC LIMIT 3",
                    minutes + " ORDER BY m DESC",
                    minutes + " ORDER BY s DESC LIMIT $1",
                    minutes + " ORDER BY s DESC LIMIT 1, 3",
                    minutes + " ORDER BY s DESC LIMIT -3",
                    minutes + " ORDER BY s DESC LIMIT 0",
                    minutes + " ORDER BY m DESC LIMIT 3_000_000_000",
                    minutes + " ORDER BY s DESC, m LIMIT 3",
                    minutes + " ORDER BY a DESC LIMIT 3",
                    "SELECT ts, s + 1 x FROM (" + minutes + ") ORDER BY x DESC LIMIT 3",
                    "SELECT * FROM (" + minutes + ") WHERE s > 0 ORDER BY s DESC LIMIT 3",
                    "SELECT * FROM ((" + minutes + ") UNION ALL (" + minutes + ")) ORDER BY s DESC LIMIT 3",
                    minutes + " UNION ALL " + minutes + " ORDER BY s DESC LIMIT 3",
                    "SELECT * FROM (" + minutes + ") ORDER BY s + 0 DESC LIMIT 3",
                    "SELECT ts, max(s) mx FROM (" + minutes + ") SAMPLE BY 1h ORDER BY mx DESC LIMIT 3",
                    "SELECT a.ts, a.s FROM (" + minutes + ") a JOIN trades b ON (ts) ORDER BY a.s DESC LIMIT 3",
                    "SELECT * FROM (SELECT a.ts, a.s FROM (" + minutes + ") a JOIN trades b ON (ts)) ORDER BY s DESC LIMIT 3",
                    "SELECT b.ts, b.s FROM trades a JOIN (" + minutes + ") b ON (ts) ORDER BY b.s DESC LIMIT 3",
                    "SELECT * FROM (SELECT * FROM (" + minutes + ") LIMIT 100) ORDER BY s DESC LIMIT 3",
                    "SELECT * FROM (SELECT timestamp_floor('1m', ts) b, sym, sum(v) s FROM trades LIMIT 100) ORDER BY s DESC LIMIT 3",
                    "SELECT * FROM (SELECT * FROM (" + minutes + ") UNION ALL SELECT * FROM (" + minutes + ")) ORDER BY s DESC LIMIT 3",
                    "SELECT * FROM (SELECT ts, s, sum(s) OVER () total FROM (" + minutes + ")) ORDER BY s DESC LIMIT 3",
                    "SELECT * FROM (SELECT ts, s + 1 x FROM (SELECT ts, s, sym FROM (" + minutes + "))) ORDER BY x DESC LIMIT 3",
                    "SELECT * FROM (" + minutes + ") LATEST ON ts PARTITION BY sym ORDER BY s DESC LIMIT 1",
                    "SELECT ts, sym, sum(v) s FROM trades SAMPLE BY 1m FILL(NULL) ORDER BY s DESC LIMIT 3",
            }) {
                context.setStreamGroupByEnabled(false);
                final String expected = compileAndRead(compiler, context, sql);
                context.setStreamGroupByEnabled(true);
                TestUtils.assertContains(compileAndRead(compiler, context, "EXPLAIN " + sql), "Stream Group By");
                Assert.assertEquals(sql, expected, compileAndRead(compiler, context, sql));
            }
            TestUtils.assertContains(compileAndRead(compiler, context, "EXPLAIN SELECT timestamp_floor('1m', ts) b, sym, sum(v) s FROM trades LIMIT 3"), "Stream Group By");
            final String scalar = "SELECT ts, v FROM trades WHERE ts >= (SELECT max(ts) FROM (SELECT ts, sum(v) s FROM trades SAMPLE BY 1s)) ORDER BY v DESC LIMIT 3";
            context.setStreamGroupByEnabled(false);
            final String expected = compileAndRead(compiler, context, scalar);
            context.setStreamGroupByEnabled(true);
            Assert.assertEquals(expected, compileAndRead(compiler, context, scalar));
        });
    }

    @Test
    public void testWrappedCopiesIgnoreSortAndJoinBudgets() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 1024);
        setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_MAX_BYTES, 1024);
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 1024);
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_MAX_PAGES, 1);
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            context.getBindVariableService().clear();
            context.getBindVariableService().setLong(0, 600);
            final String sampleBy = "SELECT ts, sym, sum(v) v FROM trades SAMPLE BY 1s";
            for (String wrapped : new String[]{
                    "SELECT * FROM (" + sampleBy + ") WHERE now() > '2000-01-01'",
                    sampleBy + " EXCEPT (SELECT ts, sym, v FROM trades WHERE v = 2)",
                    sampleBy + " EXCEPT ALL (SELECT ts, sym, v FROM trades WHERE v = 2)",
                    sampleBy + " INTERSECT (" + sampleBy + ")",
                    sampleBy + " INTERSECT ALL (" + sampleBy + ")",
                    sampleBy + " LIMIT -600",
                    sampleBy + " LIMIT $1",
            }) {
                for (String sql : new String[]{
                        "SELECT * FROM (" + wrapped + ") ORDER BY v, ts, sym",
                        "SELECT count(), sum(s.v) FROM (SELECT DISTINCT sym FROM trades) m JOIN (" + wrapped + ") s ON (sym)",
                }) {
                    context.setStreamGroupByEnabled(false);
                    final String expected = compileAndRead(compiler, context, sql);
                    context.setStreamGroupByEnabled(true);
                    Assert.assertEquals(sql, expected, compileAndRead(compiler, context, sql));
                }
            }
        });
    }

    @Test
    public void testColumnTopKeyAggregatesMatchPerRowSemantics() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE s (ts TIMESTAMP, d DOUBLE, e DOUBLE) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("""
                    INSERT INTO s (ts, d) VALUES
                        ('2024-01-01T00', 1), ('2024-01-01T01', 2), ('2024-01-01T02', 'Infinity'), ('2024-01-01T03', 3),
                        ('2024-01-02T00', 10), ('2024-01-02T01', 20),
                        ('2024-01-03T00', 'Infinity'), ('2024-01-04T00', 'Infinity'),
                        ('2024-01-05T00', null), ('2024-01-06T00', null)""", context);
            engine.execute("ALTER TABLE s ADD COLUMN i INT", context);
            engine.execute("INSERT INTO s (ts, d, i) VALUES ('2024-01-07T00', null, null), ('2024-01-07T01', 4, 1), ('2024-01-07T02', 'Infinity', 1), ('2024-01-07T03', null, 1)", context);
            final double inf = Double.POSITIVE_INFINITY;
            final double nan = Double.NaN;
            final String[] functions = {"avg", "sum", "ksum", "nsum", "min", "max"};
            final double[] finiteAndInfinite = {inf, inf, 36, 36, 1, 20};
            final double[] infiniteOnly = {inf, inf, nan, nan, nan, nan};
            final double[] keyed = {inf, inf, 4, 4, 4, 4};
            final String[] filters = {
                    " WHERE ts < '2024-01-03'",
                    " WHERE ts >= '2024-01-03' AND ts < '2024-01-05'",
                    " WHERE ts >= '2024-01-05' AND ts < '2024-01-07'",
                    " WHERE ts >= '2024-01-05'",
                    ""
            };
            for (int f = 0; f < functions.length; f++) {
                final double[][] expected = {
                        {finiteAndInfinite[f]},
                        {infiniteOnly[f]},
                        {nan},
                        {nan, keyed[f]},
                        {finiteAndInfinite[f], keyed[f]}
                };
                for (int j = 0; j < filters.length; j++) {
                    final String sql = "SELECT i, sum(e) e, " + functions[f] + "(d) v FROM s" + filters[j] + " ORDER BY i";
                    if (parallel) {
                        try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                            Assert.assertTrue(sql, plan(factory, context).contains("GroupBy vectorized: true"));
                        }
                    }
                    assertValues(compiler, context, sql, "v", expected[j]);
                    final double[] nulls = new double[expected[j].length];
                    Arrays.fill(nulls, nan);
                    assertValues(compiler, context, sql, "e", nulls);
                }
            }
        });
    }

    @Test
    public void testColumnTopKeySumsKeepOverflow() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE o (ts TIMESTAMP, d DOUBLE) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("INSERT INTO o VALUES ('2024-01-01T00', 1.5e308), ('2024-01-01T01', 1.5e308), ('2024-01-01T02', 1.0), ('2024-01-02T00', 5.0)", context);
            engine.execute("ALTER TABLE o ADD COLUMN i INT", context);
            engine.execute("INSERT INTO o VALUES ('2024-01-03T00', 1.5e308, 1), ('2024-01-03T01', 1.5e308, 1), ('2024-01-03T02', 5.0, 1)", context);
            for (String function : new String[]{"ksum", "nsum", "sum", "avg"}) {
                for (String sql : new String[]{"SELECT i, " + function + "(d) v FROM o ORDER BY i", "SELECT " + function + "(d) v FROM o WHERE ts < '2024-01-03'"}) {
                    try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                        if (parallel) {
                            Assert.assertTrue(sql, plan(factory, context).contains("vectorized: true"));
                        }
                        final int column = factory.getMetadata().getColumnIndex("v");
                        final int rows = column == 0 ? 1 : 2;
                        for (int pass = 0; pass < 2; pass++) {
                            try (RecordCursor cursor = factory.getCursor(context)) {
                                final Record record = cursor.getRecord();
                                for (int row = 0; row < rows; row++) {
                                    Assert.assertTrue(sql, cursor.hasNext());
                                    Assert.assertFalse(sql + ": " + record.getDouble(column), Numbers.isFinite(record.getDouble(column)));
                                }
                                Assert.assertFalse(sql, cursor.hasNext());
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testBatchBuffersStayOffQueryMemoryLimit() throws Exception {
        // A batch of 131_072 rows takes a 1 MiB buffer, as much as the query may use.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_BATCH_SIZE, 131_072);
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE t AS (SELECT timestamp_sequence('2024-01-01', 1_000_000L) ts, ('k' || (x % 10))::SYMBOL k, x v "
                    + "FROM long_sequence(200_000)) TIMESTAMP(ts) PARTITION BY DAY", context);
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 1L << 20);
            try {
                // 200_000 seconds span 56 hours, each with all 10 keys, and v adds up to 200_000 * 200_001 / 2.
                assertQuery("SELECT count() n, sum(s) s FROM (SELECT k, timestamp_floor('1h', ts) b, sum(v) s FROM t)")
                        .withEngine(engine).withContext(context)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                n\ts
                                560\t20000100000
                                """);
            } finally {
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
            }
        });
    }

    @Test
    public void testBatchNSumKeepsCancellingOverflow() throws Exception {
        // One frame holds every row, so the x86 kernel's eight lanes overflow: rows 0 and 8 to +Infinity, rows 1 and 9 to -Infinity.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1_000);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 1_000);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE t (ts TIMESTAMP, d DOUBLE) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("""
                    INSERT INTO t SELECT '2024-01-01'::TIMESTAMP + x, CASE x - 1
                        WHEN 0 THEN 1.7976931348623157E308
                        WHEN 8 THEN 1.7976931348623157E308
                        WHEN 1 THEN -1.7976931348623157E308
                        WHEN 9 THEN -1.7976931348623157E308
                        WHEN 2 THEN 5.0
                        ELSE 0.0 END
                    FROM long_sequence(16)""", context);
            engine.execute("CREATE TABLE n AS (SELECT ts, NULL::DOUBLE d FROM t) TIMESTAMP(ts) PARTITION BY DAY", context);
            for (String table : new String[]{"t", "n"}) {
                final double expected = "t".equals(table) ? 5.0 : Double.NaN;
                for (String[] query : new String[][]{
                        {"SELECT nsum(d) v FROM " + table, "vectorized: true"},
                        {"SELECT ts, nsum(d) v FROM " + table + " SAMPLE BY 1d", "Async Stream Group By"}
                }) {
                    if (parallel) {
                        try (RecordCursorFactory factory = compiler.compile(query[0], context).getRecordCursorFactory()) {
                            Assert.assertTrue(query[0], plan(factory, context).contains(query[1]));
                        }
                    }
                    assertValues(compiler, context, query[0], "v", new double[]{expected});
                }
            }
        });
    }

    @Test
    public void testColumnTopKeyIntegralAggregates() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE s (ts TIMESTAMP, i INT, l LONG, h SHORT, w LONG256) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("""
                    INSERT INTO s VALUES
                        ('2024-01-01T00', 1, 1, 1, to_long256(1, 0, 0, 0)),
                        ('2024-01-01T01', 2, 2, 2, to_long256(2, 0, 0, 0)),
                        ('2024-01-01T02', 3, 3, 3, to_long256(3, 0, 0, 0)),
                        ('2024-01-02T00', 0, 0, 0, to_long256(0, 0, 0, 0))""", context);
            engine.execute("ALTER TABLE s ADD COLUMN k INT", context);
            engine.execute("INSERT INTO s VALUES ('2024-01-03T00', 4, 4, 4, to_long256(4, 0, 0, 0), null), ('2024-01-03T01', 5, 5, 5, to_long256(5, 0, 0, 0), 1)", context);
            final String aggregates = "SELECT k, sum(i) si, sum(l) sl, sum(h) sh, sum(w) sw, avg(i) ai, avg(l) al, avg(h) ah FROM s";
            if (parallel) {
                try (RecordCursorFactory factory = compiler.compile(aggregates, context).getRecordCursorFactory()) {
                    final String plan = plan(factory, context);
                    Assert.assertTrue(plan, plan.contains("GroupBy vectorized: true"));
                }
            }
            assertRows(compiler, context, aggregates + " WHERE ts IN '2024-01-01' ORDER BY k", """
                    k\tsi\tsl\tsh\tsw\tai\tal\tah
                    null\t6\t6\t6\t0x06\t2.0\t2.0\t2.0
                    """);
            assertRows(compiler, context, "SELECT k, sum(h) sh FROM s WHERE ts IN '2024-01-02' ORDER BY k", """
                    k\tsh
                    null\t0
                    """);
            assertRows(compiler, context, aggregates + " ORDER BY k", """
                    k\tsi\tsl\tsh\tsw\tai\tal\tah
                    null\t10\t10\t10\t0x0a\t2.0\t2.0\t2.0
                    1\t5\t5\t5\t0x05\t5.0\t5.0\t5.0
                    """);
        });
    }

    @Test
    public void testColumnTopShortAggregates() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE kt (ts TIMESTAMP, s SHORT) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("INSERT INTO kt VALUES ('2024-01-01T00', -7), ('2024-01-01T01', -2)", context);
            engine.execute("ALTER TABLE kt ADD COLUMN k INT, y SYMBOL, z SHORT, w SHORT", context);
            engine.execute("""
                    INSERT INTO kt VALUES
                        ('2024-01-02T00', -3, null, null, -4, 4),
                        ('2024-01-02T01', -9, null, null, -6, 6),
                        ('2024-01-02T02', 5, 1, 'a', 7, 7)""", context);
            engine.execute("CREATE TABLE vt (ts TIMESTAMP, k INT, y SYMBOL) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("INSERT INTO vt VALUES ('2024-01-01T00', 1, 'a'), ('2024-01-01T01', 2, 'b'), ('2024-01-01T02', 3, 'c')", context);
            engine.execute("ALTER TABLE vt ADD COLUMN s SHORT", context);
            engine.execute("""
                    INSERT INTO vt VALUES
                        ('2024-01-02T00', 1, 'a', -5), ('2024-01-02T00:30', 1, 'a', -9),
                        ('2024-01-02T01', 2, 'b', 5), ('2024-01-02T01:30', 2, 'b', 9)""", context);

            // SHORT has no NULL: a column top reads as 0.
            final String aggregates = ", max(%1$s) %1$sx, min(%1$s) %1$sn, sum(%1$s) %1$ss, avg(%1$s) %1$sa";
            final String columns = "\t%1$sx\t%1$sn\t%1$ss\t%1$sa";
            final String shorts = ", count() c" + aggregates.formatted("s");
            final String header = "g\tc" + columns.formatted("s");
            for (String[] key : new String[][]{{"k", "null", "1"}, {"y", "", "a"}}) {
                final String select = "SELECT " + key[0] + " g" + shorts + aggregates.formatted("z") + aggregates.formatted("w") + " FROM kt";
                final String kHeader = header + columns.formatted("z") + columns.formatted("w") + "\n";
                assertVectorizedRows(compiler, context, select + " WHERE ts IN '2024-01-01' ORDER BY g", kHeader
                        + key[1] + "\t2\t-2\t-7\t-9\t-4.5\t0\t0\t0\t0.0\t0\t0\t0\t0.0\n");
                assertVectorizedRows(compiler, context, select + " ORDER BY g", kHeader
                        + key[1] + "\t4\t-2\t-9\t-21\t-5.25\t0\t-6\t-10\t-2.5\t6\t0\t10\t2.5\n"
                        + key[2] + "\t1\t5\t5\t5\t5.0\t7\t7\t7\t7.0\t7\t7\t7\t7.0\n");
            }
            for (String[] key : new String[][]{{"k", "1", "2", "3"}, {"y", "a", "b", "c"}, {"hour(ts)", "0", "1", "2"}}) {
                final String select = "SELECT " + key[0] + " g" + shorts + " FROM vt";
                assertVectorizedRows(compiler, context, select + " WHERE ts IN '2024-01-01' ORDER BY g", header + "\n"
                        + key[1] + "\t1\t0\t0\t0\t0.0\n"
                        + key[2] + "\t1\t0\t0\t0\t0.0\n"
                        + key[3] + "\t1\t0\t0\t0\t0.0\n");
                assertVectorizedRows(compiler, context, select + " ORDER BY g", header + "\n"
                        + key[1] + "\t3\t0\t-9\t-14\t-4.666666666666667\n"
                        + key[2] + "\t3\t9\t0\t14\t4.666666666666667\n"
                        + key[3] + "\t1\t0\t0\t0\t0.0\n");
            }
        });
    }

    @Test
    public void testCompensatedSumAcrossFrames() throws Exception {
        run((engine, compiler, context) -> {
            for (String[] table : new String[][]{{"a", "1e16", "1", "-1e16"}, {"b", "1e8", "1e-8", "-1e8"}}) {
                engine.execute("CREATE TABLE " + table[0] + " (ts TIMESTAMP, k SYMBOL, d DOUBLE) TIMESTAMP(ts) PARTITION BY DAY", context);
                engine.execute("INSERT INTO " + table[0] + " VALUES ('2024-01-01', '" + table[0] + "', " + table[1] + "), ('2024-01-01T01:00', '"
                        + table[0] + "', " + table[2] + "), ('2024-01-02', '" + table[0] + "', " + table[3] + ")", context);
            }
            engine.execute("CREATE TABLE ab AS (SELECT * FROM a UNION ALL SELECT * FROM b ORDER BY ts) TIMESTAMP(ts) PARTITION BY DAY", context);
            for (String[] query : new String[][]{
                    {"SELECT ts, nsum(d) s FROM a SAMPLE BY 1y", "ts\ts\n2024-01-01T00:00:00.000000Z\t1.0\n"},
                    {"SELECT ts, nsum(d) s FROM b SAMPLE BY 1y", "ts\ts\n2024-01-01T00:00:00.000000Z\t1.0E-8\n"},
                    {"SELECT timestamp_floor('1y', ts) ts, nsum(d) s FROM a ORDER BY ts DESC", "ts\ts\n2024-01-01T00:00:00.000000Z\t1.0\n"},
                    {"SELECT ts, k, nsum(d) s FROM ab SAMPLE BY 1y ORDER BY k", "ts\tk\ts\n2024-01-01T00:00:00.000000Z\ta\t1.0\n2024-01-01T00:00:00.000000Z\tb\t1.0E-8\n"},
                    {"SELECT nsum(d) s FROM a", "s\n1.0\n"},
                    {"SELECT nsum(d) s FROM b", "s\n1.0E-8\n"},
            }) {
                try (RecordCursorFactory factory = compiler.compile(query[0], context).getRecordCursorFactory()) {
                    for (int i = 0; i < 2; i++) {
                        Assert.assertEquals(query[0], query[1], read(factory, context));
                    }
                }
            }
        });
    }

    @Test
    public void testRecordValuesOverStreamGroupBy() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("INSERT INTO t VALUES ('2024-01-01T00:10', 1), ('2024-01-01T01:10', 2), ('2024-01-01T01:20', 3)", context);
            // pg_database() joins its RECORD in as a field, which no copy can hold.
            final String buckets = "(SELECT ts, c, pg_catalog.pg_database() r FROM (SELECT ts, count() c FROM t SAMPLE BY 1h))";
            for (boolean isStream : new boolean[]{true, false}) {
                context.setStreamGroupByEnabled(isStream);
                assertRows(compiler, context, "SELECT ts, c, (r).oid o FROM (" + buckets + " LIMIT -1)", """
                        ts\tc\to
                        2024-01-01T01:00:00.000000Z\t2\t1
                        """);
                context.getBindVariableService().setLong(0, 1);
                assertRows(compiler, context, "SELECT ts, c, (r).oid o FROM (" + buckets + " LIMIT $1)", """
                        ts\tc\to
                        2024-01-01T00:00:00.000000Z\t1\t1
                        """);
                context.getBindVariableService().clear();
                assertRows(compiler, context, "SELECT a.x, b.ts, (b.r).oid o FROM long_sequence(2) a CROSS JOIN " + buckets + " b", """
                        x\tts\to
                        1\t2024-01-01T00:00:00.000000Z\t1
                        1\t2024-01-01T01:00:00.000000Z\t1
                        2\t2024-01-01T00:00:00.000000Z\t1
                        2\t2024-01-01T01:00:00.000000Z\t1
                        """);
                final String matched = """
                        x\tts\to
                        2\t2024-01-01T00:00:00.000000Z\t1
                        2\t2024-01-01T01:00:00.000000Z\t1
                        3\t2024-01-01T00:00:00.000000Z\t1
                        3\t2024-01-01T01:00:00.000000Z\t1
                        """;
                assertRows(compiler, context, "SELECT a.x, b.ts, (b.r).oid o FROM (SELECT x + 1 x FROM long_sequence(2)) a LEFT JOIN " + buckets + " b ON a.x >= b.c", matched);
                assertRows(compiler, context, "SELECT a.x, b.ts, (b.r).oid o FROM " + buckets + " b RIGHT JOIN (SELECT x + 1 x FROM long_sequence(2)) a ON a.x >= b.c", matched);
                assertRows(compiler, context, "SELECT ts, c, (r).oid o FROM (" + buckets + " LATEST ON ts PARTITION BY c)", """
                        ts\tc\to
                        2024-01-01T00:00:00.000000Z\t1\t1
                        2024-01-01T01:00:00.000000Z\t2\t1
                        """);
                assertRows(compiler, context, "SELECT x, c, (r).oid o FROM ((SELECT x, c, pg_catalog.pg_database() r FROM (SELECT x, count() c FROM long_sequence(1) GROUP BY x)) LIMIT -1)", """
                        x\tc\to
                        1\t1\t1
                        """);
                final String counts = "(SELECT x, count() c FROM long_sequence(2) GROUP BY x UNION ALL SELECT x, count() c FROM long_sequence(2) GROUP BY x)";
                final String records = "(SELECT x, pg_catalog.pg_database() r FROM long_sequence(2))";
                assertRows(compiler, context, "SELECT c, (r).oid o FROM (SELECT b.c, a.r FROM " + records + " a LEFT JOIN " + counts + " b ON a.x = b.x LIMIT -1)", """
                        c\to
                        1\t1
                        """);
                context.getBindVariableService().setLong(0, 1);
                assertRows(compiler, context, "SELECT c, (r).oid o FROM (SELECT b.c, a.r FROM " + records + " a LEFT JOIN " + counts + " b ON a.x = b.x LIMIT $1)", """
                        c\to
                        1\t1
                        """);
                context.getBindVariableService().clear();
                for (String join : new String[]{"JOIN " + counts + " b ON a.x = b.x", "LEFT JOIN " + counts + " b ON a.x = b.x", "LEFT JOIN " + counts + " b ON a.x = b.x AND a.x + b.c > 1"}) {
                    final String sql = "SELECT a.x, b.c FROM long_sequence(2) a " + join + " LIMIT -1";
                    assertRows(compiler, context, sql, """
                            x\tc
                            2\t1
                            """);
                    if (isStream) {
                        try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                            final String plan = plan(factory, context);
                            Assert.assertTrue(plan, plan.contains("Hash") && plan.contains("Stream Group By") && !plan.contains("Materialize"));
                        }
                    }
                }
                assertRows(compiler, context, "SELECT a.ts, b.c FROM t a ASOF JOIN (SELECT ts, count() c FROM t SAMPLE BY 1h) b LIMIT -1", """
                        ts\tc
                        2024-01-01T01:20:00.000000Z\t2
                        """);
                // Neither a copy nor the full-fat join can hold the RECORD of a slave without random access.
                try (RecordCursorFactory ignored = compiler.compile("SELECT a.ts, (b.r).oid o FROM t a ASOF JOIN ((SELECT ts, v, pg_catalog.pg_database() r FROM t) TIMESTAMP(ts)) b ON (v)", context).getRecordCursorFactory()) {
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "right side column 'r' is of unsupported type");
                }
                final String streamMaster = "SELECT a.c FROM (SELECT x, count() c FROM long_sequence(2) GROUP BY x) a JOIN long_sequence(2) b ON a.x = b.x LIMIT -1";
                assertRows(compiler, context, streamMaster, """
                        c
                        1
                        """);
                if (isStream) {
                    try (RecordCursorFactory factory = compiler.compile(streamMaster, context).getRecordCursorFactory()) {
                        final String plan = plan(factory, context);
                        Assert.assertTrue(plan, plan.contains("Hash Join") && plan.contains("Stream Group By") && plan.contains("Materialize"));
                    }
                    try (RecordCursorFactory factory = compiler.compile("SELECT ts, c, (r).oid o FROM (" + buckets + " LATEST ON ts PARTITION BY c)", context).getRecordCursorFactory()) {
                        final String plan = plan(factory, context);
                        Assert.assertTrue(plan, plan.contains("Cross Join") && plan.contains("Materialize") && plan.contains("Stream Group By"));
                    }
                    // A slave the copy can hold leaves the stream uncopied.
                    try (RecordCursorFactory factory = compiler.compile("SELECT a.ts, a.c, b.x FROM (SELECT ts, count() c FROM t SAMPLE BY 1h) a CROSS JOIN long_sequence(1) b", context).getRecordCursorFactory()) {
                        final String plan = plan(factory, context);
                        Assert.assertTrue(plan, plan.contains("Cross Join") && plan.contains("Stream Group By") && !plan.contains("Materialize"));
                    }
                }
            }
            context.setStreamGroupByEnabled(true);
        });
    }

    @Test
    public void testCopyingSortsFitSmallMemoryLimit() throws Exception {
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 4 * 1024 * 1024);
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 3), ('2024-01-01T00:00:01', 1), ('2024-01-01T00:00:02', 2)", context);
            final String ascending = """
                    ts\ts
                    2024-01-01T00:00:01.000000Z\t1
                    2024-01-01T00:00:02.000000Z\t2
                    2024-01-01T00:00:00.000000Z\t3
                    """;
            assertQuery("SELECT ts, sum(v) s FROM t SAMPLE BY 1s ORDER BY s").withEngine(engine).withContext(context)
                    .noLeakCheck()
                    .withPlanContaining("Encode sort\n  keys: [s]")
                    .returns(ascending);
            assertQuery("SELECT ts, sum(v::DOUBLE) s FROM t SAMPLE BY 1s ORDER BY s DESC LIMIT 2").withEngine(engine).withContext(context)
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Encode sort lo: 2")
                    .returns("""
                            ts\ts
                            2024-01-01T00:00:00.000000Z\t3.0
                            2024-01-01T00:00:02.000000Z\t2.0
                            """);
            setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, "false");
            assertQuery("SELECT ts, sum(v) s FROM t SAMPLE BY 1s ORDER BY s").withEngine(engine).withContext(context)
                    .noLeakCheck()
                    .withPlanContaining("Sort\n  keys: [s]")
                    .returns(ascending);
        });
    }

    @Test
    public void testDescendingKeyKeepsFirstAndLastInStorageOrder() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE dup (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", context);
            engine.execute("""
                    INSERT INTO dup VALUES
                        ('2024-01-01T00:00', NULL), ('2024-01-01T00:00', 1), ('2024-01-01T00:00', 2), ('2024-01-01T00:00', NULL),
                        ('2024-01-01T01:00', 3), ('2024-01-01T01:00', 4)""", context);
            final String expected = """
                    k\tfirst\tlast\tfirst_not_null\tlast_not_null
                    2024-01-01T01:00:00.000000Z\t3\t4\t3\t4
                    2024-01-01T00:00:00.000000Z\tnull\tnull\t1\t2
                    """;
            for (String sql : new String[]{
                    "SELECT ts k, first(v), last(v), first_not_null(v), last_not_null(v) FROM dup ORDER BY k DESC",
                    "SELECT ts k, first(v), last(v), first_not_null(v), last_not_null(v) FROM dup GROUP BY ts ORDER BY k DESC"
            }) {
                Assert.assertEquals(sql, expected, compileAndRead(compiler, context, sql));
            }
        });
    }

    @Test
    public void testDesignatedTimestampOnlyWhenOrderedByKey() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("""
                    CREATE TABLE src AS (
                        SELECT timestamp_sequence('2024-01-01', 1_200_000_000L) ts, (CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END)::SYMBOL sym, x v
                        FROM long_sequence(6)
                    ) TIMESTAMP(ts) PARTITION BY DAY""", context);
            final String streamGroupBy = parallel ? "Async Stream Group By workers: " + Math.max(1, workers) : "Stream Group By";

            // a plain GROUP BY reports no designated timestamp, like the hash group-by
            final String unordered = "SELECT max(ts), timestamp_floor('1h', ts) b, count() FROM src GROUP BY b";
            assertQuery(unordered).withEngine(engine).withContext(context).noLeakCheck().noRandomAccess()
                    .withPlan("""
                            VirtualRecord
                              functions: [max,b,count]
                                %s
                                  keys: [b]
                                  keyFunctions: [timestamp_floor('hour',ts)]
                                  values: [max(ts),count(*)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: src
                            """.formatted(streamGroupBy))
                    .returns("""
                            max\tb\tcount
                            2024-01-01T00:40:00.000000Z\t2024-01-01T00:00:00.000000Z\t3
                            2024-01-01T01:40:00.000000Z\t2024-01-01T01:00:00.000000Z\t3
                            """);

            // so INSERT ... SELECT without a column list maps max(ts) onto the designated timestamp
            engine.execute("CREATE TABLE dst (ts TIMESTAMP, b TIMESTAMP, c LONG) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("INSERT INTO dst " + unordered, context);
            assertQuery("dst").withEngine(engine).withContext(context).noLeakCheck().timestamp("ts").expectSize()
                    .returns("""
                            ts\tb\tc
                            2024-01-01T00:40:00.000000Z\t2024-01-01T00:00:00.000000Z\t3
                            2024-01-01T01:40:00.000000Z\t2024-01-01T01:00:00.000000Z\t3
                            """);

            // SAMPLE BY and an ORDER BY led by the key report the key as the designated timestamp,
            // so no sort runs above the stream group-by
            assertQuery("SELECT ts, sum(v) s FROM src SAMPLE BY 1h").withEngine(engine).withContext(context)
                    .noLeakCheck().timestamp("ts").noRandomAccess()
                    .withPlan("""
                            %s
                              keys: [ts]
                              keyFunctions: [timestamp_floor_utc('1h',ts)]
                              values: [sum(v)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: src
                            """.formatted(streamGroupBy))
                    .returns("""
                            ts\ts
                            2024-01-01T00:00:00.000000Z\t6
                            2024-01-01T01:00:00.000000Z\t15
                            """);
            assertQuery("SELECT timestamp_floor('1h', ts) b, count() c FROM src GROUP BY b ORDER BY b")
                    .withEngine(engine).withContext(context).noLeakCheck().timestamp("b").noRandomAccess()
                    .withPlan("""
                            VirtualRecord
                              functions: [b,c]
                                %s
                                  keys: [b]
                                  keyFunctions: [timestamp_floor('hour',ts)]
                                  values: [count(*)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: src
                            """.formatted(streamGroupBy))
                    .returns("""
                            b\tc
                            2024-01-01T00:00:00.000000Z\t3
                            2024-01-01T01:00:00.000000Z\t3
                            """);
            assertQuery("SELECT ts, count() c FROM src GROUP BY ts ORDER BY ts")
                    .withEngine(engine).withContext(context).noLeakCheck().timestamp("ts").noRandomAccess()
                    .withPlan("""
                            %s
                              keys: [ts]
                              values: [count(*)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: src
                            """.formatted(streamGroupBy))
                    .returns("""
                            ts\tc
                            2024-01-01T00:00:00.000000Z\t1
                            2024-01-01T00:20:00.000000Z\t1
                            2024-01-01T00:40:00.000000Z\t1
                            2024-01-01T01:00:00.000000Z\t1
                            2024-01-01T01:20:00.000000Z\t1
                            2024-01-01T01:40:00.000000Z\t1
                            """);

            // CREATE TABLE AS adopts a designated timestamp only from the ordered queries
            engine.execute("CREATE TABLE rollup AS (SELECT timestamp_floor('1h', ts) b, sym, sum(v) s FROM src GROUP BY b, sym)", context);
            engine.execute("CREATE TABLE sampled AS (SELECT ts, sym, sum(v) s FROM src SAMPLE BY 1h)", context);
            engine.execute("CREATE TABLE ordered AS (SELECT ts, count() c FROM src GROUP BY ts ORDER BY ts)", context);
            assertQuery("SELECT table_name, designatedTimestamp FROM tables() WHERE table_name IN ('rollup', 'sampled', 'ordered') ORDER BY table_name")
                    .withEngine(engine).withContext(context).noLeakCheck()
                    .returns("""
                            table_name\tdesignatedTimestamp
                            ordered\tts
                            rollup\t
                            sampled\tts
                            """);
        });
    }

    @Test
    public void testSampleBy() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            try (RecordCursorFactory expected = compiler.compile(
                    "select timestamp_floor('1m', ts) ts, sum(v), avg(d) from hash_input order by ts", context).getRecordCursorFactory();
                 RecordCursorFactory actual = compiler.compile(
                         "select ts, sum(v), avg(d) from trades sample by 1m align to calendar", context).getRecordCursorFactory()) {
                assertStream(actual, true);
                Assert.assertEquals(read(expected, context), read(actual, context));
                Assert.assertEquals(0, actual.getMetadata().getTimestampIndex());
            }
        });
    }

    @Test
    public void testSegmentedKeysAcrossFrames() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            for (String keys : new String[]{
                    "timestamp_floor('1m', ts) bucket, sym",
                    "sym, timestamp_floor('1m', ts) bucket",
                    "sym::varchar name, timestamp_floor('1m', ts) bucket",
                    "sym::string name, timestamp_floor('1m', ts) bucket",
                    "v % 7 id, sym, timestamp_floor('1m', ts) bucket",
                    "CASE WHEN v % 3 = 0 THEN null ELSE sym::varchar END name, timestamp_floor('1m', ts) bucket",
                    "timestamp_floor('1m', ts) bucket, (v % 3)::decimal(18,2) price",
                    "year(ts) bucket, sym"
            }) {
                final String order = keys.contains(" id,") ? "bucket, sym, id"
                        : keys.contains(" name,") ? "bucket, name"
                          : keys.contains(" price") ? "bucket, price" : "bucket, sym";
                assertMatchesHash(compiler, context, "SELECT " + keys
                        + ", count(*), sum(v), avg(d), min(v), max(v), first(v), last(v), first(sym), last(sym) "
                        + "FROM $table ORDER BY " + order);
            }
        });
    }

    @Test
    public void testSegmentedDistinct() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            assertMatchesHash(compiler, context, "SELECT DISTINCT timestamp_floor('1m', ts) bucket, sym "
                    + "FROM $table ORDER BY bucket, sym");
        });
    }

    @Test
    public void testSegmentedDynamicSymbol() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            assertMatchesHash(compiler, context, "SELECT timestamp_floor('1m', ts) bucket, "
                    + "(v % 7)::symbol name, count(*), sum(v) FROM $table ORDER BY bucket, name", false);
        });
    }

    @Test
    public void testSegmentedFrameResultGrowth() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_PAGE_SIZE, 128);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 2048);
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE trades AS (SELECT "
                    + "timestamp_sequence('2024-01-01'::timestamp, 1_000_000L) ts, "
                    + "x v, (x % 31)::double d, (x % 17)::symbol sym "
                    + "FROM long_sequence(2001)) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("CREATE TABLE hash_input (ts timestamp, v long, d double, sym symbol)", context);
            engine.execute("INSERT INTO hash_input SELECT * FROM trades", context);
            final String sql = "SELECT timestamp_floor('1U', ts) bucket, sym, "
                    + "count(*), sum(v), avg(d), first(v), last(v) FROM $table ORDER BY bucket";
            assertMatchesHash(compiler, context, sql);
            if (parallel) {
                try (RecordCursorFactory factory = compiler.compile(sql.replace("$table", "trades"), context).getRecordCursorFactory()) {
                    final String expected = read(factory, context);
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 16 * 1024);
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        try {
                            while (cursor.hasNext()) {
                            }
                            Assert.fail("expected frame result memory limit breach");
                        } catch (CairoException e) {
                            Assert.assertTrue(e.getFlyweightMessage().toString(), e.isOutOfMemory());
                            TestUtils.assertContains(e.getFlyweightMessage(), "memoryTag=" + MemoryTag.NATIVE_RECORD_CHAIN + ']');
                        }
                    } finally {
                        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                    }
                    Assert.assertEquals(expected, read(factory, context));
                }
            }
        });
    }

    @Test
    public void testSegmentedMapGrowth() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE trades AS (SELECT "
                    + "timestamp_sequence('2024-01-01'::timestamp, 1_000_000L) ts, "
                    + "CASE WHEN x % 17 = 0 THEN null ELSE (x % 257)::symbol END sym, x v "
                    + "FROM long_sequence(2001)) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("CREATE TABLE hash_input (ts timestamp, sym symbol, v long)", context);
            engine.execute("INSERT INTO hash_input SELECT * FROM trades", context);
            for (String key : new String[]{"sym", "sym::varchar"}) {
                assertMatchesHash(compiler, context, "SELECT timestamp_floor('10m', ts) bucket, " + key
                        + " k, count(*), sum(v), first(v), last(v) FROM $table ORDER BY bucket, k");
            }
        });
    }

    @Test
    public void testSegmentedMapPresizesFromLastRun() throws Exception {
        run((engine, compiler, context) -> {
            // One key in the first second, 5_000 keys in the second one, then 999 seconds of one key each.
            engine.execute("""
                    CREATE TABLE trades AS (
                        SELECT '2024-01-01'::timestamp + CASE WHEN x = 1 THEN 0 WHEN x <= 5_001 THEN 1 ELSE x - 5_000 END * 1_000_000 ts,
                            CASE WHEN x <= 5_001 THEN x ELSE 0 END k
                        FROM long_sequence(6_000)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """, context);
            try (RecordCursorFactory factory = compiler.compile("SELECT timestamp_floor('1s', ts) bucket, k, count(*) FROM trades", context).getRecordCursorFactory()) {
                assertStream(factory, true);
                int initialCapacity = 0;
                for (int run = 0; run < 2; run++) {
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        Assert.assertTrue(cursor.hasNext());
                        if (run == 0) {
                            initialCapacity = getSegmentMapKeyCapacity(factory);
                        } else if (!parallel) {
                            // The second run sizes the map for the 5_000 keys before it reads the first segment.
                            Assert.assertTrue(getSegmentMapKeyCapacity(factory) > initialCapacity);
                        }
                        long count = 1;
                        while (cursor.hasNext()) {
                            count++;
                        }
                        Assert.assertEquals(6_000, count);
                        if (!parallel) {
                            Assert.assertEquals(initialCapacity, getSegmentMapKeyCapacity(factory));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSegmentedMapShrinksAfterHighCardinalitySegment() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 10_000);
        run((engine, compiler, context) -> {
            for (int[] segment : new int[][]{{5_000, 5_000}, {600, 100}}) {
                // The first second holds segment[0] rows of segment[1] keys, each of the 1_000 seconds after it one key.
                engine.execute("DROP TABLE IF EXISTS trades", context);
                engine.execute("""
                        CREATE TABLE trades AS (
                            SELECT '2024-01-01'::timestamp + CASE WHEN x <= %1$d THEN 0 ELSE (x - %1$d) * 1_000_000 END ts,
                                CASE WHEN x <= %1$d THEN x %% %2$d ELSE 0 END k
                            FROM long_sequence(%1$d + 1_000)
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """.formatted(segment[0], segment[1]), context);
                for (String keys : new String[]{"k", "k::varchar k", "k, k % 7 m"}) {
                    final String sql = "SELECT timestamp_floor('1s', ts) bucket, " + keys + ", count(*) FROM trades";
                    final int initialCapacity;
                    try (
                            RecordCursorFactory factory = compiler.compile(sql + " WHERE ts > '2024-01-01'", context).getRecordCursorFactory();
                            RecordCursor cursor = factory.getCursor(context)
                    ) {
                        Assert.assertTrue(cursor.hasNext());
                        initialCapacity = getSegmentMapKeyCapacity(factory);
                    }
                    try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                        assertStream(factory, true);
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertTrue(keys, getSegmentMapKeyCapacity(factory) > initialCapacity);
                            long count = 1;
                            while (cursor.hasNext()) {
                                count++;
                            }
                            Assert.assertEquals(segment[1] + 1_000, count);
                            // The single-key segments must not keep clearing the capacity the first one grew.
                            if (!parallel) {
                                Assert.assertEquals(keys, initialCapacity, getSegmentMapKeyCapacity(factory));
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSegmentedNativeKeyTypes() throws Exception {
        run((engine, compiler, context) -> {
            final String columns = "(ts TIMESTAMP, a DOUBLE[], b BINARY, id UUID, d DECIMAL(38,2), w DECIMAL(76,2), v LONG)";
            engine.execute("CREATE TABLE trades " + columns + " TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("INSERT INTO trades SELECT timestamp_sequence('2024-01-01'::timestamp, 1_000_000L), "
                    + "CASE WHEN x % 5 = 0 THEN null ELSE ARRAY[(x % 3)::double, 1.0] END, "
                    + "rnd_bin(8, 16, 3), CASE WHEN x % 7 = 0 THEN null ELSE to_uuid(x % 3, x % 7) END, "
                    + "CASE WHEN x % 11 = 0 THEN null ELSE (x % 13)::decimal(38,2) END, (x % 17)::decimal(76,2), x "
                    + "FROM long_sequence(257)", context);
            engine.execute("CREATE TABLE hash_input " + columns, context);
            engine.execute("INSERT INTO hash_input SELECT * FROM trades", context);
            for (String keys : new String[]{"a", "b", "id", "d", "w", "a, b, id, d, w"}) {
                assertMatchesHash(compiler, context, "SELECT timestamp_floor('2m', ts) bucket, " + keys
                        + ", count(*), min(v) first_v, first(id), last(id), first(d), last(d), sum(d), first(w), last(w), sum(w) "
                        + "FROM $table ORDER BY bucket, first_v");
            }
        });
    }

    @Test
    public void testSegmentedSampleByAndParquet() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            for (int iteration = 0; iteration < 2; iteration++) {
                if (iteration == 1) {
                    convertFirstDayToParquet(engine, context);
                }
                final String expectedSql = "SELECT timestamp_floor('1m', ts) ts, sym, count(*), sum(v), avg(d) "
                        + "FROM hash_input ORDER BY ts, sym";
                final String sql = "SELECT ts, sym, count(*), sum(v), avg(d) FROM trades "
                        + "SAMPLE BY 1m ALIGN TO CALENDAR ORDER BY ts, sym";
                try (RecordCursorFactory expected = compiler.compile(expectedSql, context).getRecordCursorFactory();
                     RecordCursorFactory actual = compiler.compile(sql, context).getRecordCursorFactory()) {
                    assertStream(actual, true);
                    Assert.assertEquals(read(expected, context), read(actual, context));
                    Assert.assertEquals(0, actual.getMetadata().getTimestampIndex());
                    final String plan = TestUtils.printSqlToString(engine, context, "EXPLAIN " + sql, new StringSink());
                    Assert.assertTrue(plan, plan.contains("segmentKey:"));
                }
            }
        });
    }

    @Test
    public void testSegmentedEmptyAndSingleRow() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE trades (ts timestamp, sym symbol, v long) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("CREATE TABLE hash_input (ts timestamp, sym symbol, v long)", context);
            final String sql = "SELECT timestamp_floor('1m', ts) bucket, sym, count(*), sum(v) FROM $table ORDER BY bucket, sym";
            assertMatchesHash(compiler, context, sql);
            engine.execute("INSERT INTO trades VALUES ('2024-01-01', null, null)", context);
            engine.execute("INSERT INTO hash_input SELECT * FROM trades", context);
            assertMatchesHash(compiler, context, sql);
        });
    }

    @Test
    public void testSegmentedColumnTopsAndPartitions() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE trades AS (SELECT timestamp_sequence('2024-01-01T23:59:30'::timestamp, 1_000_000L) ts, "
                    + "x v FROM long_sequence(101)) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("ALTER TABLE trades ADD COLUMN name varchar", context);
            engine.execute("INSERT INTO trades SELECT timestamp_sequence('2024-01-02T00:02:00'::timestamp, 1_000_000L), x, "
                    + "CASE WHEN x % 3 = 0 THEN null WHEN x % 2 = 0 THEN '東京' ELSE 'é' END::varchar FROM long_sequence(301)", context);
            engine.execute("CREATE TABLE hash_input (ts timestamp, v long, name varchar)", context);
            engine.execute("INSERT INTO hash_input SELECT * FROM trades", context);
            assertMatchesHash(compiler, context, "SELECT name, timestamp_floor('1h', ts) bucket, "
                    + "count(*), sum(v), first(v), last(v) FROM $table ORDER BY bucket, name");
            assertMatchesHash(compiler, context, "SELECT name, timestamp_floor('1h', ts) bucket, "
                    + "count(*), sum(v), first(v), last(v) FROM $table WHERE v % 7 = 0 ORDER BY bucket, name");
        });
    }

    @Test
    public void testSegmentedLongSequence() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE hash_input AS (SELECT x FROM long_sequence(101))", context);
            final String sql = "SELECT x % 3 k, x, count(*), sum(x) FROM ";
            try (RecordCursorFactory expected = compiler.compile(sql + "hash_input ORDER BY x", context).getRecordCursorFactory();
                 RecordCursorFactory actual = compiler.compile(sql + "long_sequence(101) ORDER BY x", context).getRecordCursorFactory()) {
                assertStream(actual, true, false);
                Assert.assertEquals(read(expected, context), read(actual, context));
            }
        });
    }

    @Test
    public void testUnsafeKeysAndAggregatesUseHash() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            for (String sql : new String[]{
                    "select timestamp_floor('1m', ts), rnd_long(), sum(v) from trades",
                    "select timestamp_floor('1m', ts), sym, count_distinct(v) from trades",
                    "select timestamp_floor('1m', ts), count_distinct(v) from trades",
                    "select timestamp_floor_utc('1d', ts, null, '01:30', 'America/New_York'), sum(v) from trades",
                    "select timestamp_floor_utc('1d', ts, null, '01:30', 'America/New_York'), sym, sum(v) from trades",
                    "select timestamp_floor('1m', ts), sum(rnd_long()) from trades",
                    "select timestamp_floor('1m', ts), sum(v) from trades where rnd_boolean()",
                    "select timestamp_floor('1m', ts), sym, sum(v) from trades where rnd_boolean()",
                    "select timestamp_floor('2147483647d', ts), sum(v) from trades",
                    "select timestamp_floor('3n', ts), sum(v) from trades",
                    "select dateadd('M', 1, ts), sum(v) from trades",
                    "select dateadd('h', 1, ts, 'Europe/Berlin'), sum(v) from trades",
                    "select timestamp_floor('1h', ts, null, '00:00', 'America/New_York'), sum(v) from trades",
                    "select ts, sym, count_distinct(v) from trades sample by 1m fill(prev) align to calendar"
            }) {
                try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                    assertStream(factory, false);
                    read(factory, context);
                }
            }
        });
    }

    @Test
    public void testDeterminismMatchesHash() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            for (String keys : new String[]{"", ", sym"}) {
                final String sql = "SELECT timestamp_floor('1m', ts) bucket" + keys + ", count(*), sum(v) FROM ";
                try (RecordCursorFactory expected = compiler.compile(sql + "hash_input", context).getRecordCursorFactory();
                     RecordCursorFactory actual = compiler.compile(sql + "trades", context).getRecordCursorFactory()) {
                    assertStream(actual, true);
                    final RecordCursorFactory hash = getGroupBy(expected);
                    final RecordCursorFactory stream = getGroupBy(actual);
                    Assert.assertEquals(sql, hash.isNonDeterministic(), stream.isNonDeterministic());
                    Assert.assertEquals(sql, hash.isStableWithinExecution(), stream.isStableWithinExecution());
                }
            }
        });
    }

    @Test
    public void testExplain() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            final String plan = TestUtils.printSqlToString(engine, context,
                    "explain select ts, sum(v) from trades sample by 1m align to calendar", new StringSink());
            Assert.assertEquals("QUERY PLAN\n"
                    + (parallel ? "Async Stream Group By workers: " + Math.max(1, workers) : "Stream Group By") + "\n"
                    + "  keys: [ts]\n"
                    + "  keyFunctions: [timestamp_floor_utc('1m',ts)]\n"
                    + "  values: [sum(v)]\n"
                    + "    PageFrame\n"
                    + "        Row forward scan\n"
                    + "        Frame forward scan on: trades\n", plan);
        });
    }

    @Test
    public void testNanosecondTimestamps() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("create table trades as (select "
                    + "timestamp_sequence_ns('2024-01-01'::timestamp_ns, 123_456_789L) ts, x v "
                    + "from long_sequence(501)) timestamp(ts) partition by day", context);
            engine.execute("create table hash_input (ts timestamp_ns, v long)", context);
            engine.execute("insert into hash_input select * from trades", context);
            for (String key : new String[]{"timestamp_floor('7s', ts)", "timestamp_floor('1n', ts)",
                    "timestamp_floor('3M', ts)", "timestamp_floor('5y', ts)", "date_trunc('month', ts)",
                    "dateadd('h', -1, ts, '+05:30')", "timestamp_ceil('n', ts)", "year(ts)"}) {
                assertMatchesHash(compiler, context, "select " + key + " bucket, count(*), sum(v), first(v), last(v) from $table order by bucket");
            }
            try (RecordCursorFactory factory = compiler.compile(
                    "select date_trunc('millennium', ts), sum(v) from trades", context).getRecordCursorFactory()) {
                assertStream(factory, false);
            }
            assertMatchesHash(compiler, context,
                    "select dateadd('h', -1, ts) bucket, count(*), sum(v) from $table order by bucket");
            assertMatchesHash(compiler, context,
                    "select ts::timestamp bucket, count(*), sum(v) from $table order by bucket");
            for (String key : new String[]{"dateadd('h', 1, ts)", "timestamp_floor_utc('1h', ts, null, '00:00', '+01:00')",
                    "timestamp_floor_utc('1w', ts, null, '00:00', null)", "timestamp_floor('1w', ts)",
                    "dateadd('h', 1, ts, '+05:30')", "timestamp_ceil('h', ts)"}) {
                try (RecordCursorFactory factory = compiler.compile(
                        "select " + key + ", sum(v) from trades", context).getRecordCursorFactory()) {
                    assertStream(factory, false);
                }
            }
        });
    }

    @Test
    public void testParquet() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            convertFirstDayToParquet(engine, context);
            assertMatchesHash(compiler, context,
                    "select timestamp_floor('1m', ts) bucket, count(*), sum(v), avg(d), first(v), last(v) from $table order by bucket");
        });
    }

    @Test
    public void testFailedCloseReleasesFrameResults() throws Exception {
        Assume.assumeTrue(parallel && workers > 0);
        setProperty(PropertyKey.CAIRO_INACTIVE_READER_MAX_OPEN_PARTITIONS, 1);
        final AtomicBoolean isArmed = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public void munmap(long address, long size, int memoryTag) {
                if (isArmed.compareAndSet(true, false)) {
                    throw CairoException.critical(0).put("injected munmap failure");
                }
                super.munmap(address, size, memoryTag);
            }
        };
        assertMemoryLeak(ff, () -> TestUtils.execute(new TestWorkerPool(workers), (engine, compiler, context) -> {
            engine.execute("CREATE TABLE t AS (SELECT timestamp_sequence('2024-01-01', 600_000_000L) ts, ('s' || (x % 5))::SYMBOL s, x v "
                    + "FROM long_sequence(288)) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("CREATE TABLE hash_input AS (SELECT * FROM t)", context);
            final String sql = "SELECT timestamp_floor('1h', ts) b, s, count(), sum(v) FROM $table ORDER BY b, s";
            final String expected = compileAndRead(compiler, context, sql.replace("$table", "hash_input"));
            try (
                    LimitedMemoryTracker tracker = new LimitedMemoryTracker(0);
                    RecordCursorFactory factory = compiler.compile(sql.replace("$table", "t"), context).getRecordCursorFactory()
            ) {
                final AsyncStreamGroupByRecordCursorFactory stream = (AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory);
                context.setMemoryTracker(tracker);
                try {
                    final RecordCursor cursor = stream.getCursor(context);
                    Assert.assertTrue(cursor.hasNext());
                    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                    while (stream.getReducedFrameCount() < stream.getDispatchedFrameCount()) {
                        Assert.assertTrue("workers did not reduce the dispatched frames", System.nanoTime() < deadline);
                        Os.pause();
                    }
                    isArmed.set(true);
                    try {
                        cursor.close();
                        Assert.fail("expected close() to fail");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "injected munmap failure");
                    }
                    Assert.assertEquals(0, tracker.getUsed());
                    Assert.assertEquals(expected, read(factory, context));
                } finally {
                    context.setMemoryTracker(null);
                }
            }
        }, configuration, LOG));
    }

    @Test
    public void testFixedStateTypes() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("create table trades as (select "
                    + "timestamp_sequence('2024-01-01'::timestamp, 1_000_000L) ts, "
                    + "case when x % 5 = 0 then null else (x % 37)::int end i, "
                    + "case when x % 7 = 0 then null else (x % 29)::float end f, "
                    + "(x % 11)::byte b, (x % 17)::short s, (65 + x % 5)::char ch, "
                    + "x % 2 = 0 flag, ('2024-01-01'::timestamp + x * 1000)::date dt, "
                    + "case when x % 3 = 0 then null else 'str' end str, "
                    + "case when x % 3 = 0 then null else 'str'::varchar end vc "
                    + "from long_sequence(701)) timestamp(ts) partition by day", context);
            engine.execute("create table hash_input (ts timestamp, i int, f float, b byte, s short, ch char, flag boolean, dt date, str string, vc varchar)", context);
            engine.execute("insert into hash_input select * from trades", context);
            assertMatchesHash(compiler, context, "select timestamp_floor('1m', ts) bucket, "
                    + "sum(i), avg(i), min(i), max(i), count(i), first(i), last(i), first_not_null(i), last_not_null(i), "
                    + "sum(f), avg(f), min(f), max(f), count(f), first(f), last(f), first_not_null(f), last_not_null(f), "
                    + "sum(b), min(b), max(b), count(b), first(b), last(b), "
                    + "sum(s), min(s), max(s), count(s), first(s), last(s), "
                    + "min(ch), max(ch), count(ch), first(ch), last(ch), first(flag), last(flag), "
                    + "min(dt), max(dt), count(dt), first(dt), last(dt), "
                    + "min(ts), max(ts), count(ts), first(ts), last(ts), count(str), count(vc) "
                    + "from $table order by bucket");
        });
    }

    @Test
    public void testStatisticalAggregates() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            final StringBuilder functions = new StringBuilder("count(*)");
            for (String function : new String[]{"ksum", "nsum", "stddev", "stddev_samp", "stddev_pop", "variance", "var_samp", "var_pop",
                    "skewness", "skewness_samp", "skewness_pop", "kurtosis", "kurtosis_samp", "kurtosis_pop"}) {
                functions.append(", round(").append(function).append("(d), 8)");
            }
            functions.append(", round(geomean(d + 1), 8)");
            for (String function : new String[]{"covar_samp", "covar_pop", "corr", "regr_slope", "regr_r2", "regr_intercept",
                    "weighted_stddev_rel", "weighted_stddev_freq", "weighted_avg", "vwap"}) {
                functions.append(", round(").append(function).append("(d, v), 8)");
            }
            assertMatchesHash(compiler, context, "select timestamp_floor('1m', ts) bucket, " + functions + " from $table order by bucket");
        });
    }

    @Test
    public void testDecimalAggregates() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            for (String type : new String[]{"decimal(2,0)", "decimal(4,1)", "decimal(9,2)", "decimal(18,2)", "decimal(38,2)", "decimal(60,2)"}) {
                final String arg = "d::" + type;
                assertMatchesHash(compiler, context, "select timestamp_floor('1m', ts) bucket, count(" + arg + "), "
                        + "sum(" + arg + "), avg(" + arg + "), avg(" + arg + ", 3), min(" + arg + "), max(" + arg + "), "
                        + "first(" + arg + "), last(" + arg + "), first_not_null(" + arg + "), last_not_null(" + arg + ") "
                        + "from $table order by bucket");
            }
        });
    }

    @Test
    public void testBitwiseAndBooleanAggregates() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            final StringBuilder functions = new StringBuilder("avg(v::short), bool_and(v > 10), bool_or(v > 10), mode(v > 10)");
            for (String type : new String[]{"byte", "short", "int", "long"}) {
                for (String function : new String[]{"bit_and", "bit_or", "bit_xor"}) {
                    functions.append(", ").append(function).append("(v::").append(type).append(')');
                }
            }
            assertMatchesHash(compiler, context, "select timestamp_floor('1m', ts) bucket, " + functions + " from $table order by bucket");
        });
    }

    @Test
    public void testOtherFixedWidthTypes() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            for (String arg : new String[]{"v::int::ipv4", "to_uuid(v, v)", "sym", "v::geohash(5b)", "v::geohash(10b)",
                    "v::geohash(20b)", "v::geohash(40b)"}) {
                assertMatchesHash(compiler, context, "select timestamp_floor('1m', ts) bucket, count(" + arg + "), "
                        + "first(" + arg + "), last(" + arg + "), first_not_null(" + arg + "), last_not_null(" + arg + ") "
                        + "from $table order by bucket");
            }
            assertMatchesHash(compiler, context, "select timestamp_floor('1m', ts) bucket, "
                    + "sum(v::long256), min(v::int::ipv4), max(v::int::ipv4) from $table order by bucket");
        });
    }

    @Test
    public void testArgMinMax() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            for (String value : new String[]{"'x'::char", "d", "v", "ts"}) {
                final StringBuilder functions = new StringBuilder("count(*)");
                for (String key : new String[]{"v::double", "v", "ts"}) {
                    functions.append(", arg_min(").append(value).append(',').append(key).append(')');
                    functions.append(", arg_max(").append(value).append(',').append(key).append(')');
                }
                assertMatchesHash(compiler, context, "select timestamp_floor('1m', ts) bucket, " + functions + " from $table order by bucket");
            }
            assertMatchesHash(compiler, context, "select timestamp_floor('1m', ts) bucket, "
                    + "arg_min(to_uuid(v, v), ts), arg_max(to_uuid(v, v), ts), "
                    + "arg_min(ts, to_uuid(v, v)), arg_max(ts, to_uuid(v, v)) from $table order by bucket");
        });
    }

    @Test
    public void testBoundedDispatch() throws Exception {
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 2);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 1);
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            for (String bucket : new String[]{"1d", "7s", "1U"}) {
                assertMatchesHash(compiler, context, "SELECT timestamp_floor('" + bucket + "', ts) bucket, "
                        + "count(*), sum(v), first(v), last(v) FROM $table ORDER BY bucket");
            }
        });
    }

    @Test
    public void testFramePredictionStopsAtRowsAcrossTimeGap() throws Exception {
        Assume.assumeTrue(parallel);
        // 300 days lie inside the first frame, so the share of time alone would predict millions of groups. A frame holds
        // all the rows of the single partition, or a quarter of them with four workers.
        final int rows = 131_572;
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, rows);
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE gap AS (SELECT (CASE WHEN x < 10_000 THEN x * 1_000_000L ELSE 25_920_000_000_000L + x * 1_000_000L END)::TIMESTAMP ts, "
                    + "x % 3 k, x v FROM long_sequence(" + rows + ")) TIMESTAMP(ts) PARTITION BY YEAR", context);
            try (RecordCursorFactory factory = compiler.compile("SELECT ts, sum(v) s FROM gap SAMPLE BY 1s", context).getRecordCursorFactory();
                 RecordCursor cursor = factory.getCursor(context)) {
                Assert.assertTrue(cursor.hasNext());
                final DirectLongList results = ((AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory)).getAtom().getResultValues(0);
                Assert.assertTrue("capacity " + results.getCapacity() + " for " + results.size() + " longs", results.getCapacity() <= results.size() * 5 / 4);
            }
            try (RecordCursorFactory factory = compiler.compile("SELECT ts, k, sum(v) s FROM gap SAMPLE BY 1s", context).getRecordCursorFactory();
                 RecordCursor cursor = factory.getCursor(context)) {
                Assert.assertTrue(cursor.hasNext());
                final RecordChain results = ((AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory)).getAtom().getResultRecords(0);
                final long used = results.getAppendOffset();
                Assert.assertTrue("used " + used, used > 128 * 1024);
                Assert.assertTrue("reserved " + results.getReservedBytes() + " for " + used, results.getReservedBytes() <= used * 5 / 4);
            }
        });
    }

    @Test
    public void testFrameResultsReserveAfterSample() throws Exception {
        Assume.assumeTrue(parallel);
        // A frame holds all the rows, or a quarter of them with four workers. Either way its two longs per group
        // land just above a doubling step of the initial 64 longs, so doubling would reserve twice what it needs.
        final int rows = 131_572;
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, rows);
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE even AS (SELECT (x * 1_000_000L)::TIMESTAMP ts, x v FROM long_sequence(" + rows + ")) "
                    + "TIMESTAMP(ts) PARTITION BY YEAR", context);
            // A burst of small buckets opens the frame, so the sample overestimates the groups that follow.
            engine.execute("CREATE TABLE burst AS (SELECT (CASE WHEN x <= 5_000 THEN x * 1_000_000L "
                    + "ELSE 10_000_000_000L + x * 1_000L END)::TIMESTAMP ts, x v FROM long_sequence(" + rows + ")) "
                    + "TIMESTAMP(ts) PARTITION BY YEAR", context);
            assertFirstFrameCapacityMatchesGroups(compiler, context, "SELECT ts, sum(v) s FROM even SAMPLE BY 1s");
            assertFirstFrameCapacityMatchesGroups(compiler, context, "SELECT ts, sum(v) s FROM even SAMPLE BY 1s ORDER BY ts DESC");
            // The burst covers less than half of the frame's time range, whatever share of the rows it holds.
            try (RecordCursorFactory factory = compiler.compile("SELECT ts, sum(v) s FROM burst SAMPLE BY 1s", context).getRecordCursorFactory();
                 RecordCursor cursor = factory.getCursor(context)) {
                Assert.assertTrue(cursor.hasNext());
                final DirectLongList results = ((AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory)).getAtom().getResultValues(0);
                Assert.assertTrue("capacity " + results.getCapacity() + " for " + results.size() + " longs", results.getCapacity() <= 3 * results.size());
            }
            final String expected = "count\tsum\n" + "5127\t" + (rows * (rows + 1L) / 2) + "\n";
            try (RecordCursorFactory factory = compiler.compile(
                    "SELECT count() count, sum(s)::long sum FROM (SELECT ts, sum(v) s FROM burst SAMPLE BY 1s)", context).getRecordCursorFactory()) {
                Assert.assertEquals(expected, read(factory, context));
            }
        });
    }

    @Test
    public void testShardMergeReservesForLargestSource() throws Exception {
        Assume.assumeTrue(parallel && workers > 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 100);
        final int keys = 40_000;
        final int frames = 16;
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, keys);
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE t AS (SELECT (x - 1)::TIMESTAMP ts, x % " + keys + " k, x v FROM long_sequence(" + keys * frames + ")) "
                    + "TIMESTAMP(ts) PARTITION BY YEAR", context);
            try (RecordCursorFactory factory = compiler.compile("SELECT timestamp_floor('1d', ts) ts, k, count() c FROM t", context).getRecordCursorFactory();
                 RecordCursor cursor = factory.getCursor(context)) {
                assertStream(factory, true);
                long count = 0;
                while (cursor.hasNext()) {
                    count++;
                }
                Assert.assertEquals(keys, count);
                final AsyncStreamGroupByAtom atom = ((AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory)).getAtom();
                Assert.assertTrue(atom.getShardMergeCount() > 0);
                // Every frame holds all the keys, so a shard needs room for its keys however many sources merge.
                final long capacity = atom.getShardMergeKeyCapacity();
                Assert.assertTrue(capacity + " slots", capacity <= 4L * keys);
            }
        });
    }

    @Test
    public void testShardedBucketChecksBreakerEvery4kRows() throws Exception {
        // Only parallel execution shards a bucket.
        Assume.assumeTrue(parallel);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 100);
        final int rows = 100_000;
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, rows);
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE big AS (SELECT (x - 1)::timestamp ts, x k FROM long_sequence(" + rows + ")) TIMESTAMP(ts) PARTITION BY YEAR", context);
            final SqlExecutionContextImpl impl = (SqlExecutionContextImpl) context;
            final SqlExecutionCircuitBreaker breaker = impl.getCircuitBreaker();
            final CountingSqlExecutionCircuitBreaker counting = new CountingSqlExecutionCircuitBreaker(breaker);
            impl.with(counting);
            try (RecordCursorFactory factory = compiler.compile("SELECT timestamp_floor('1h', ts) ts, k, count() c FROM big", context).getRecordCursorFactory();
                 RecordCursor cursor = factory.getCursor(context)) {
                assertStream(factory, true);
                long count = 0;
                while (cursor.hasNext()) {
                    count++;
                }
                Assert.assertEquals(rows, count);
                // With workers, the owner also polls the breaker for as long as it waits for them.
                if (workers == 0) {
                    // Every key is new, so a check per row would add 100k checks.
                    Assert.assertTrue("checks " + counting.getCheckCount(), counting.getCheckCount() < rows / 2);
                }
            } finally {
                impl.with(breaker);
            }
        });
    }

    @Test
    public void testFramesDoNotEachProbeConnection() throws Exception {
        Assume.assumeTrue(parallel);
        final int rows = 10_000;
        final int frames = 100;
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, rows / frames);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, rows / frames);
        final AtomicInteger probes = new AtomicInteger();
        circuitBreakerConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public long getCircuitBreakerConnectionCheckThrottle() {
                return Dates.HOUR_MILLIS;
            }

            @Override
            public int getCircuitBreakerThrottle() {
                return 2_000_000;
            }

            @Override
            public @NotNull NetworkFacade getNetworkFacade() {
                return new NetworkFacadeImpl() {
                    @Override
                    public boolean testConnection(long fd, long buffer, int bufferSize) {
                        probes.incrementAndGet();
                        return false;
                    }
                };
            }
        };
        try {
            run((engine, compiler, context) -> {
                engine.execute("CREATE TABLE t AS (SELECT (x * 10_000L)::TIMESTAMP ts, x % 3 k, x v, x::DOUBLE d FROM long_sequence(" + rows + ")) "
                        + "TIMESTAMP(ts) PARTITION BY YEAR", context);
                engine.execute("CREATE TABLE trades AS (SELECT timestamp_sequence('2024-01-01', 8_000_000L) ts, x % 3 k, x v FROM long_sequence(" + rows + ")) "
                        + "TIMESTAMP(ts) PARTITION BY DAY", context);
                engine.execute("INSERT INTO trades VALUES ('2024-01-02T12:00', 1, 1)", context);
                convertToParquet(engine, context, "2024-01-01");
                final SqlExecutionContextImpl impl = (SqlExecutionContextImpl) context;
                final SqlExecutionCircuitBreaker previous = impl.getCircuitBreaker();
                try (NetworkSqlExecutionCircuitBreaker breaker = new NetworkSqlExecutionCircuitBreaker(engine, circuitBreakerConfiguration)) {
                    breaker.of(1);
                    breaker.setTimeout(Long.MAX_VALUE);
                    breaker.resetTimer();
                    impl.with(breaker);
                    for (String sql : new String[]{
                            "SELECT ts, count() c, sum(v) s FROM t SAMPLE BY 1s",
                            "SELECT ts, count() c, sum(v) s FROM t WHERE v % 2 = 0 SAMPLE BY 1s",
                            "SELECT ts, avg(d * 2) a FROM t SAMPLE BY 1s",
                            "SELECT ts, k, count() c, sum(v) s FROM t SAMPLE BY 1s",
                            "SELECT ts, k, count() c, sum(v) s FROM t WHERE v % 2 = 0 SAMPLE BY 1s",
                            "SELECT ts, count() c, sum(v) s FROM trades WHERE v % 97 = 0 SAMPLE BY 1m",
                            "SELECT ts, k, count() c, sum(v) s FROM trades WHERE v % 97 = 0 SAMPLE BY 1m"
                    }) {
                        try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory();
                             RecordCursor cursor = factory.getCursor(context)) {
                            assertStream(factory, true);
                            probes.set(0);
                            long count = 0;
                            while (cursor.hasNext()) {
                                count++;
                            }
                            Assert.assertTrue(sql, count > 0);
                            Assert.assertTrue(sql + ": " + probes.get() + " probes", probes.get() < frames / 2);
                        }
                    }
                } finally {
                    impl.with(previous);
                }
            });
        } finally {
            circuitBreakerConfiguration = null;
        }
    }

    @Test
    public void testRowLoopChecksBreakerEvery4kRows() throws Exception {
        Assume.assumeTrue(parallel && workers == 0);
        final int rows = 100_000;
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, rows);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, rows);
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE t AS (SELECT (x - 1)::TIMESTAMP ts, x::DOUBLE d, x u FROM long_sequence(" + rows + ")) "
                    + "TIMESTAMP(ts) PARTITION BY YEAR", context);
            engine.execute("CREATE TABLE trades AS (SELECT ('2024-01-01'::TIMESTAMP + x - 1)::TIMESTAMP ts, x % 7 k, x v FROM long_sequence(" + rows + ")) "
                    + "TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("INSERT INTO trades VALUES ('2024-01-02T12:00', 1, 1)", context);
            convertToParquet(engine, context, "2024-01-01");
            final CountingSqlExecutionCircuitBreaker breaker = new CountingSqlExecutionCircuitBreaker(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
            try (SqlExecutionContextImpl counted = new SqlExecutionContextImpl(engine, 1).with(context.getSecurityContext(), null, null, -1, breaker)) {
                for (Object[] query : new Object[][]{
                        {"SELECT timestamp_floor('1h', ts) ts, avg(d * 2) a FROM t", rows, rows / 2},
                        {"SELECT timestamp_floor('20T', ts) ts, u, count() c FROM t", rows, rows},
                        {"SELECT timestamp_floor('1d', ts) ts, k, count() c, sum(v) s FROM trades WHERE v % 2 = 0", rows / 2, rows / 4}
                }) {
                    try (RecordCursorFactory factory = compiler.compile((String) query[0], counted).getRecordCursorFactory()) {
                        assertStream(factory, true);
                        final long before = breaker.getCheckCount();
                        Assert.assertFalse(read(factory, counted).isEmpty());
                        final long checks = breaker.getCheckCount() - before;
                        final int minChecks = (int) query[1] / (StreamGroupByMap.BREAKER_CHECK_MASK + 1);
                        Assert.assertTrue(query[0] + ": " + checks + " checks", checks >= minChecks && checks < (int) query[2]);
                    }
                }
            }
        });
    }

    @Test
    public void testFrameChainReservesAfterSample() throws Exception {
        Assume.assumeTrue(parallel);
        // A frame holds all the rows, or a quarter of them with four workers. Either way its chain lands just above
        // a power of two, so doubling would reserve almost twice what it needs.
        final int rows = 448_000;
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, rows);
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE keyed AS (SELECT (x * 250L)::TIMESTAMP ts, x % 1_000 k, x v FROM long_sequence(" + rows + ")) "
                    + "TIMESTAMP(ts) PARTITION BY YEAR", context);
            // A burst of small buckets opens the frame, so the sample overestimates the chain that follows.
            engine.execute("CREATE TABLE burst AS (SELECT (CASE WHEN x <= 5_000 THEN x * 1_000_000L "
                    + "ELSE 10_000_000_000L + x * 1_000L END)::TIMESTAMP ts, x % 3 k, x v FROM long_sequence(" + rows + ")) "
                    + "TIMESTAMP(ts) PARTITION BY YEAR", context);
            for (String order : new String[]{"", " ORDER BY ts DESC"}) {
                try (RecordCursorFactory factory = compiler.compile("SELECT ts, k, count() c, sum(v) s FROM keyed SAMPLE BY 1s" + order, context).getRecordCursorFactory();
                     RecordCursor cursor = factory.getCursor(context)) {
                    Assert.assertTrue(cursor.hasNext());
                    final RecordChain results = ((AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory)).getAtom().getResultRecords(0);
                    final long used = results.getAppendOffset();
                    Assert.assertTrue(order + " used " + used, used > 128 * 1024);
                    Assert.assertTrue(order + " reserved " + results.getReservedBytes() + " for " + used, results.getReservedBytes() <= used * 5 / 4);
                }
            }
            try (RecordCursorFactory factory = compiler.compile("SELECT ts, k, count() c, sum(v) s FROM burst SAMPLE BY 1s", context).getRecordCursorFactory();
                 RecordCursor cursor = factory.getCursor(context)) {
                Assert.assertTrue(cursor.hasNext());
                final RecordChain results = ((AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory)).getAtom().getResultRecords(0);
                final long used = results.getAppendOffset();
                Assert.assertTrue("reserved " + results.getReservedBytes() + " for " + used, results.getReservedBytes() <= 3 * used);
            }
        });
    }

    @Test
    public void testFrameChainPredictionFitsMemoryLimit() throws Exception {
        Assume.assumeTrue(parallel);
        final int rows = 131_572;
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, rows);
        run((engine, compiler, context) -> {
            // Every row of the burst adds a chain row, while the rest of the frame adds about one per second.
            engine.execute("CREATE TABLE burst AS (SELECT (CASE WHEN x <= 5_000 THEN x * 1_000_000L "
                    + "ELSE 10_000_000_000L + x * 1_000L END)::TIMESTAMP ts, (CASE WHEN x <= 5_000 THEN x ELSE 0 END) k, x v "
                    + "FROM long_sequence(" + rows + ")) TIMESTAMP(ts) PARTITION BY YEAR", context);
            final String expected = "count\tsum\n" + "5127\t" + (rows * (rows + 1L) / 2) + "\n";
            // The smallest first frame, with four workers, predicts its 40-byte chain row for each of its rows.
            final long predictedBytes = rows / 4 * 40L;
            long smallestLimit = Long.MAX_VALUE;
            try (RecordCursorFactory factory = compiler.compile(
                    "SELECT count() count, sum(s)::long sum FROM (SELECT ts, k, count() c, sum(v) s FROM burst SAMPLE BY 1s)", context).getRecordCursorFactory();
                 MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(context.getSecurityContext(), 42, MemoryTrackerWorkload.QUERY)) {
                context.setMemoryTracker(tracker);
                try {
                    for (long limit = 1L << 24; limit >= 1L << 15; limit -= limit >> 2) {
                        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit);
                        try {
                            Assert.assertEquals(expected, read(factory, context));
                            smallestLimit = limit;
                        } catch (CairoException e) {
                            Assert.assertTrue(e.getFlyweightMessage().toString(), e.isOutOfMemory());
                        } finally {
                            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                        }
                        Assert.assertEquals(0, tracker.getUsed());
                        Assert.assertEquals(expected, read(factory, context));
                        Assert.assertEquals(0, tracker.getUsed());
                    }
                } finally {
                    context.setMemoryTracker(null);
                }
            }
            Assert.assertTrue("smallest limit " + smallestLimit + ", prediction " + predictedBytes, smallestLimit < predictedBytes);
        });
    }

    @Test
    public void testFrameResultsPredictionFitsMemoryLimit() throws Exception {
        Assume.assumeTrue(parallel);
        final int rows = 131_572;
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, rows);
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE burst AS (SELECT (CASE WHEN x <= 5_000 THEN x * 1_000_000L "
                    + "ELSE 10_000_000_000L + x * 1_000L END)::TIMESTAMP ts, x v FROM long_sequence(" + rows + ")) "
                    + "TIMESTAMP(ts) PARTITION BY YEAR", context);
            final String expected = "count\tsum\n" + "5127\t" + (rows * (rows + 1L) / 2) + "\n";
            // The smallest first frame, with four workers, predicts this many bytes for its remaining rows.
            final long predictedBytes = (rows / 4 - 4_096L) * 2 * Long.BYTES;
            long smallestLimit = Long.MAX_VALUE;
            try (RecordCursorFactory factory = compiler.compile(
                    "SELECT count() count, sum(s)::long sum FROM (SELECT ts, sum(v) s FROM burst SAMPLE BY 1s)", context).getRecordCursorFactory();
                 MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(context.getSecurityContext(), 42, MemoryTrackerWorkload.QUERY)) {
                context.setMemoryTracker(tracker);
                try {
                    for (long limit = 1L << 23; limit >= 1L << 14; limit -= limit >> 2) {
                        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit);
                        try {
                            Assert.assertEquals(expected, read(factory, context));
                            smallestLimit = limit;
                        } catch (CairoException e) {
                            Assert.assertTrue(e.getFlyweightMessage().toString(), e.isOutOfMemory());
                        } finally {
                            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                        }
                        Assert.assertEquals(0, tracker.getUsed());
                        Assert.assertEquals(expected, read(factory, context));
                        Assert.assertEquals(0, tracker.getUsed());
                    }
                } finally {
                    context.setMemoryTracker(null);
                }
            }
            Assert.assertTrue("smallest limit " + smallestLimit + ", prediction " + predictedBytes, smallestLimit < predictedBytes);
        });
    }

    @Test
    public void testDispatchWindowRefillsWhileFramesAreCollectable() throws Exception {
        Assume.assumeTrue(parallel && workers > 0);
        final int queueCapacity = 8;
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, queueCapacity);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 2);
        run((engine, compiler, context) -> {
            final int frames = 32;
            engine.execute("CREATE TABLE w AS (SELECT (x * 1_000_000L)::TIMESTAMP ts, x v FROM long_sequence(" + frames * frameRows + ")) "
                    + "TIMESTAMP(ts) PARTITION BY YEAR", context);
            try (RecordCursorFactory factory = compiler.compile("SELECT ts, sum(v) s FROM w SAMPLE BY 1s", context).getRecordCursorFactory();
                 RecordCursor cursor = factory.getCursor(context)) {
                final AsyncStreamGroupByRecordCursorFactory stream = (AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory);
                Assert.assertTrue(cursor.hasNext());
                final int firstWindow = stream.getDispatchedFrameCount();
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (stream.getReducedFrameCount() < firstWindow) {
                    Assert.assertTrue("workers did not reduce the first window", System.nanoTime() < deadline);
                    Os.pause();
                }
                // Reading on collects frames that are ready, which must not leave the workers without new ones.
                for (int row = 0; row <= frameRows; row++) {
                    Assert.assertTrue(cursor.hasNext());
                }
                Assert.assertTrue("dispatched " + stream.getDispatchedFrameCount(), stream.getDispatchedFrameCount() >= 1 + queueCapacity);
            }
        });
    }

    @Test
    public void testDispatchWindowOpensAfterFirstFrame() throws Exception {
        Assume.assumeTrue(parallel);
        final int workerLimit = 2;
        final int initialLimit = workerLimit + 1;
        final int queueCapacity = 8;
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, queueCapacity);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, workerLimit);
        run((engine, compiler, context) -> {
            final int frames = 32;
            engine.execute("CREATE TABLE w AS (SELECT (x * 1_000_000L)::TIMESTAMP ts, x v FROM long_sequence(" + frames * frameRows + ")) "
                    + "TIMESTAMP(ts) PARTITION BY YEAR", context);
            try (RecordCursorFactory factory = compiler.compile("SELECT ts, sum(v) s FROM w SAMPLE BY 1s", context).getRecordCursorFactory()) {
                final AsyncStreamGroupByRecordCursorFactory stream = (AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory);
                Assert.assertEquals(initialLimit, stream.getAtom().getDispatchLimit(0));
                Assert.assertEquals(queueCapacity, stream.getAtom().getDispatchLimit(1));
                try (RecordCursor cursor = factory.getCursor(context)) {
                    long row = 0;
                    int maxInFlight = 0;
                    int maxFirstWindow = 0;
                    while (cursor.hasNext()) {
                        // A bucket ends at the first row of the next one.
                        final int collected = (int) (++row / frameRows);
                        final int inFlight = stream.getDispatchedFrameCount() - collected;
                        Assert.assertTrue(
                                "collected " + collected + " in flight " + inFlight,
                                inFlight <= (collected == 0 ? initialLimit : queueCapacity)
                        );
                        maxInFlight = Math.max(maxInFlight, inFlight);
                        if (collected == 0) {
                            maxFirstWindow = Math.max(maxFirstWindow, inFlight);
                        }
                    }
                    Assert.assertEquals(frames * frameRows, row);
                    if (workers == 0) {
                        Assert.assertEquals(queueCapacity, maxInFlight);
                        Assert.assertEquals(initialLimit, maxFirstWindow);
                    }
                }
            }
        });
    }

    @Test
    public void testBindVariableReuse() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            context.getBindVariableService().setLong(0, 1);
            final String sql = "SELECT timestamp_floor('1m', ts) bucket, sum(v + $1), $1 tag FROM ";
            try (RecordCursorFactory expected = compiler.compile(sql + "hash_input ORDER BY bucket", context).getRecordCursorFactory();
                 RecordCursorFactory actual = compiler.compile(sql + "trades ORDER BY bucket", context).getRecordCursorFactory()) {
                assertStream(actual, true);
                for (int i = 0; i < 3; i++) {
                    context.getBindVariableService().setLong(0, i * 17L);
                    Assert.assertEquals(read(expected, context), read(actual, context));
                }
            }
        });
    }

    @Test
    public void testLongSequence() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE hash_input AS (SELECT x FROM long_sequence(101))", context);
            for (String source : new String[]{"long_sequence(101)", "long_sequence(101, 1, 2)"}) {
                final String sql = "SELECT x, count(*), sum(x), first(x), last(x) FROM ";
                try (RecordCursorFactory expected = compiler.compile(sql + "hash_input ORDER BY x", context).getRecordCursorFactory();
                     RecordCursorFactory actual = compiler.compile(sql + source + " ORDER BY x", context).getRecordCursorFactory()) {
                    assertStream(expected, false);
                    assertStream(actual, true, false);
                    final String expectedText = read(expected, context);
                    for (int i = 0; i < 3; i++) {
                        try (RecordCursor cursor = actual.getCursor(context)) {
                            Assert.assertTrue(cursor.hasNext());
                            cursor.toTop();
                            final StringSink sink = new StringSink();
                            CursorPrinter.println(cursor, actual.getMetadata(), sink, true, false);
                            Assert.assertEquals(expectedText, sink.toString());
                        }
                    }
                }
                final String yearSql = "SELECT year(x) bucket, count(*) c, sum(x) s FROM ";
                try (RecordCursorFactory expected = compiler.compile(yearSql + "hash_input", context).getRecordCursorFactory();
                     RecordCursorFactory actual = compiler.compile(yearSql + source, context).getRecordCursorFactory()) {
                    assertStream(expected, false);
                    assertStream(actual, true, false);
                    final String expectedText = "bucket\tc\ts\n1970\t101\t5151\n";
                    Assert.assertEquals(expectedText, read(expected, context));
                    Assert.assertEquals(expectedText, read(actual, context));
                }
            }
        });
    }

    @Test
    public void testSerialOnlyAggregates() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            assertMatchesHash(compiler, context, "SELECT timestamp_floor('1m', ts) bucket, "
                    + "isOrdered(v), isOrdered(v::int::ipv4), haversine_dist_deg(d, d, ts), "
                    + "first(v::symbol), last(v::symbol) FROM $table ORDER BY bucket", false);
        });
    }

    @Test
    public void testStatisticalNullGroups() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE trades AS (SELECT "
                    + "timestamp_sequence('2024-01-01'::timestamp, 1_000_000L) ts, "
                    + "CASE WHEN x = 61 OR x > 120 THEN (x % 19)::double END d "
                    + "FROM long_sequence(181)) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("CREATE TABLE hash_input (ts timestamp, d double)", context);
            engine.execute("INSERT INTO hash_input SELECT * FROM trades", context);
            assertMatchesHash(compiler, context, "SELECT timestamp_floor('1m', ts) bucket, "
                    + "count(d), round(stddev_pop(d), 8), round(stddev_samp(d), 8), "
                    + "round(skewness_pop(d), 8), round(kurtosis_pop(d), 8), "
                    + "round(corr(d, d), 8), round(covar_pop(d, d), 8), round(regr_slope(d, d), 8) "
                    + "FROM $table ORDER BY bucket");
        });
    }

    @Test
    public void testStoredFixedWidthTypes() throws Exception {
        run((engine, compiler, context) -> {
            final String columns = "(ts timestamp, ip ipv4, id uuid, sym symbol, dec decimal(18,2), "
                    + "g1 geohash(5b), g2 geohash(10b), g4 geohash(20b), g8 geohash(40b))";
            engine.execute("CREATE TABLE trades " + columns + " TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("INSERT INTO trades SELECT "
                    + "timestamp_sequence('2024-01-01'::timestamp, 1_000_000L) ts, "
                    + "CASE WHEN x <= 120 OR x % 7 = 0 THEN null ELSE x::int::ipv4 END ip, "
                    + "CASE WHEN x <= 120 OR x % 7 = 0 THEN null ELSE to_uuid(x, x) END id, "
                    + "CASE WHEN x <= 120 OR x % 7 = 0 THEN null ELSE (x % 19)::symbol END sym, "
                    + "CASE WHEN x <= 120 OR x % 7 = 0 THEN null ELSE (x % 31)::decimal(18,2) END dec, "
                    + "CASE WHEN x <= 120 THEN null ELSE CAST(x AS geohash(5b)) END AS g1, CASE WHEN x <= 120 THEN null ELSE CAST(x AS geohash(10b)) END AS g2, "
                    + "CASE WHEN x <= 120 THEN null ELSE CAST(x AS geohash(20b)) END AS g4, CASE WHEN x <= 120 THEN null ELSE CAST(x AS geohash(40b)) END AS g8 "
                    + "FROM long_sequence(701)", context);
            engine.execute("CREATE TABLE hash_input " + columns, context);
            engine.execute("INSERT INTO hash_input SELECT * FROM trades", context);
            final StringBuilder functions = new StringBuilder("count(*)");
            for (String column : new String[]{"ip", "id", "sym", "dec", "g1", "g2", "g4", "g8"}) {
                final int start = functions.length();
                for (String function : new String[]{"count", "first", "last", "first_not_null", "last_not_null"}) {
                    functions.append(',').append(function).append('(').append(column).append(')');
                }
                assertMatchesHash(compiler, context, "SELECT timestamp_floor('1m', ts) bucket"
                        + functions.substring(start) + " FROM $table ORDER BY bucket");
            }
            functions.append(", sum(dec), avg(dec), min(dec), max(dec), min(ip), max(ip)");
            assertMatchesHash(compiler, context, "SELECT timestamp_floor('1m', ts) bucket, "
                    + functions + " FROM $table ORDER BY bucket");
        });
    }

    @Test
    public void testMemoryIsReleasedOnClose() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            for (String filter : new String[]{"", " where v % 7 = 0"}) {
                for (String keys : new String[]{"", ", sym::varchar name, v % 7 k"}) {
                    final String period = keys.isEmpty() ? "1U" : "1m";
                    try (RecordCursorFactory factory = compiler.compile(
                            "select timestamp_floor('" + period + "', ts) bucket" + keys + ", count(*), sum(v) from trades" + filter, context).getRecordCursorFactory();
                         MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(context.getSecurityContext(), 42, MemoryTrackerWorkload.MAT_VIEW_REFRESH)) {
                        assertStream(factory, true);
                        context.setMemoryTracker(tracker);
                        try {
                            for (int iteration = 0; iteration < 5; iteration++) {
                                try (RecordCursor cursor = factory.getCursor(context)) {
                                    Assert.assertTrue(cursor.hasNext());
                                    Assert.assertEquals(parallel || !keys.isEmpty(), tracker.getUsed() > 0);
                                    cursor.toTop();
                                    Assert.assertTrue(cursor.hasNext());
                                }
                                Assert.assertEquals(0, tracker.getUsed());
                            }
                        } finally {
                            context.setMemoryTracker(null);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testMemoryLimitAndReuse() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            for (String filter : new String[]{"", " where v is null or v >= 0"}) {
                for (String keys : new String[]{"", ", sym"}) {
                    final boolean hasQueryMemory = parallel || !keys.isEmpty();
                    try (RecordCursorFactory factory = compiler.compile(
                            "select timestamp_floor('1U', ts) bucket" + keys + ", count(*), sum(v), avg(d) from trades" + filter, context).getRecordCursorFactory()) {
                        assertStream(factory, true);
                        for (int iteration = 0; iteration < 3; iteration++) {
                            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, parallel && frameRows > 1 ? 128 : 1);
                            try {
                                read(factory, context);
                                Assert.assertFalse("expected memory limit breach", hasQueryMemory);
                            } catch (CairoException e) {
                                Assert.assertTrue(e.getFlyweightMessage().toString(), hasQueryMemory && e.isOutOfMemory());
                            } finally {
                                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                            }
                            try (RecordCursor cursor = factory.getCursor(context)) {
                                long count = 0;
                                while (cursor.hasNext()) {
                                    count++;
                                }
                                Assert.assertEquals(1001, count);
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testCancellationAndReuse() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            for (String filter : new String[]{"", " where v % 7 = 0"}) {
                for (String keys : new String[]{"", ", sym"}) {
                    final String period = keys.isEmpty() ? "1U" : "1h";
                    try (NetworkSqlExecutionCircuitBreaker breaker = new NetworkSqlExecutionCircuitBreaker(engine,
                            new DefaultSqlExecutionCircuitBreakerConfiguration() {
                                @Override
                                public int getCircuitBreakerThrottle() {
                                    return 0;
                                }
                            });
                         SqlExecutionContextImpl cancellable = new SqlExecutionContextImpl(engine, Math.max(1, workers))
                                 .with(context.getSecurityContext(), null, null, -1, breaker);
                         RecordCursorFactory factory = compiler.compile(
                                 "select timestamp_floor('" + period + "', ts) bucket" + keys + ", count(*), sum(v) from trades" + filter, cancellable).getRecordCursorFactory()) {
                        assertStream(factory, true);
                        for (int iteration = 0; iteration < 3; iteration++) {
                            try (RecordCursor cursor = factory.getCursor(cancellable)) {
                                Assert.assertTrue(cursor.hasNext());
                                breaker.cancel();
                                try {
                                    while (cursor.hasNext()) {
                                    }
                                    Assert.fail("expected cancellation");
                                } catch (CairoException e) {
                                    Assert.assertTrue(e.getFlyweightMessage().toString(), e.isInterruption());
                                }
                            } finally {
                                breaker.clear();
                            }
                            Assert.assertFalse(read(factory, cancellable).isEmpty());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testTimeThrottledBreakerChecksArePerFrame() throws Exception {
        run((engine, compiler, context) -> {
            final int rows = 4_000;
            engine.execute("CREATE TABLE trades AS (SELECT timestamp_sequence('2024-01-01', 100_000L) ts, x v, "
                    + "(x % 3)::symbol sym FROM long_sequence(" + rows + ")) TIMESTAMP(ts) PARTITION BY DAY", context);
            // Each time-throttled check reads the clock under a network breaker, so only coarse sites may use
            // it. An owner with workers also checks while it waits for them, so it does not count.
            final Thread owner = Thread.currentThread();
            final boolean isOwnerWaiting = parallel && workers > 0;
            final AtomicLong checks = new AtomicLong();
            final CountingSqlExecutionCircuitBreaker breaker = new CountingSqlExecutionCircuitBreaker(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER) {
                @Override
                public void statefulThrowExceptionIfTrippedTimeThrottled() {
                    super.statefulThrowExceptionIfTrippedTimeThrottled();
                    if (!isOwnerWaiting || Thread.currentThread() != owner) {
                        checks.incrementAndGet();
                    }
                }
            };
            final long frames = (rows + frameRows - 1) / frameRows;
            try (SqlExecutionContextImpl counted = new SqlExecutionContextImpl(engine, Math.max(1, workers))
                    .with(context.getSecurityContext(), null, null, -1, breaker)) {
                for (String filter : new String[]{"", " WHERE v % 3 <> 0"}) {
                    for (String keys : new String[]{"", ", sym"}) {
                        final String sql = "SELECT timestamp_floor('1s', ts) bucket" + keys + ", count(*), sum(v) FROM trades" + filter;
                        try (RecordCursorFactory factory = compiler.compile(sql, counted).getRecordCursorFactory()) {
                            assertStream(factory, true);
                            checks.set(0);
                            Assert.assertFalse(read(factory, counted).isEmpty());
                            Assert.assertTrue(sql + ": " + checks.get() + " checks over " + frames + " frames",
                                    checks.get() <= 2 * frames + 2);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testDstRepeatedBucketUsesHash() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("create table trades as (select "
                    + "timestamp_sequence('2024-11-03T04:50:00'::timestamp, 1_200_000_000L) ts, x v "
                    + "from long_sequence(7)) timestamp(ts) partition by day", context);
            try (RecordCursorFactory factory = compiler.compile(
                    "select timestamp_floor_utc('1d', ts, null, '01:30', 'America/New_York') bucket, count(*), sum(v) "
                            + "from trades order by bucket", context).getRecordCursorFactory()) {
                assertStream(factory, false);
                Assert.assertEquals("bucket\tcount\tsum\n"
                        + "2024-11-02T05:30:00.000000Z\t3\t8\n"
                        + "2024-11-03T05:30:00.000000Z\t2\t7\n"
                        + "2024-11-03T06:30:00.000000Z\t2\t13\n", read(factory, context));
            }
        });
    }

    // The first worker to reduce a frame h > 0 holds it. Frame h + L, where L is the lane count, shares its lane, so the
    // owner stages it and cannot stage frame h + 2L behind it: the owner hands the reducers the frames before h + 2L but
    // h + L, and all of them but h reduce meanwhile. Once the worker ends frame h, it takes frame h + L and reduces it
    // itself. With isBatchSpent, a fiber launches a fiber that reduces frame h + L instead, and a worker thread leaves
    // frame h + L to the owner, which publishes it. With failure, the worker fails right after it takes frame h + L,
    // which fails the query rather than leave it waiting for the frame. With blocker, which needs a single worker so
    // that no other fiber touches the dispatcher meanwhile, the first lane claim keeps the fiber from launching a fiber
    // for frame h + L: it reduces the frame itself.
    private void assertHeldFrameHoldsOnlyItsLane(
            WorkerPoolMode workerPoolMode,
            boolean isBatchSpent,
            @Nullable String failure,
            @Nullable LaneLaunchBlocker blocker
    ) throws Exception {
        Assume.assumeTrue(parallel && workers > 0);
        Assume.assumeTrue(blocker == null || workers == 1);
        final TestWorkerPool pool = new TestWorkerPool(workers, workerPoolMode);
        final AtomicReference<PageFrameReduceDispatcher> dispatcherRef = new AtomicReference<>();
        final AtomicInteger claimCount = new AtomicInteger();
        final AtomicBoolean isFailureArmed = new AtomicBoolean();
        final AtomicBoolean isBlocked = new AtomicBoolean();
        final String queryFailure = blocker != null ? blocker.getQueryFailure() : failure;
        PageFrameSequence.setLaneClaimHookForTesting(taskIndex -> {
            claimCount.incrementAndGet();
            if (isFailureArmed.compareAndSet(true, false)) {
                throw CairoException.nonCritical().put(failure);
            }
            if (blocker != null && isBlocked.compareAndSet(false, true)) {
                blocker.block(pool, dispatcherRef.get());
            }
        });
        try {
            assertMemoryLeak(() -> TestUtils.execute(pool, (engine, compiler, context) -> {
                dispatcherRef.set(engine.getMessageBus().getPageFrameReduceDispatcher());
                // A worker reduces the next frame of its lane only within its row budget; the test frames are as large
                // as the default budget, which a single frame spends.
                if (workerPoolMode == WorkerPoolMode.FIBER_HOST) {
                    engine.getMessageBus().getPageFrameReduceDispatcher().setBatchRowBudgetForTesting(isBatchSpent ? 0 : Long.MAX_VALUE);
                } else {
                    PageFrameSequence.setLaneRowBudgetForTesting(isBatchSpent ? 0 : Long.MAX_VALUE);
                }
                engine.execute("CREATE TABLE t AS (SELECT (x * 1_000_000)::TIMESTAMP ts, x FROM long_sequence("
                        + 64 * frameRows + ")) TIMESTAMP(ts)", context);
                final String sql = "SELECT timestamp_floor('3s', ts) bucket, x % 3 k, count() c, sum(x) s FROM t WHERE test_latched_counter()";
                TestLatchedCounterFunctionFactory.reset(null);
                context.setStreamGroupByEnabled(false);
                final String expected = sortedLines(compileAndRead(compiler, context, sql));
                context.setStreamGroupByEnabled(true);
                final Thread owner = Thread.currentThread();
                final AtomicReference<AsyncStreamGroupByRecordCursorFactory> groupBy = new AtomicReference<>();
                final AtomicLong heldFrame = new AtomicLong(-1);
                final AtomicReference<String> heldState = new AtomicReference<>();
                final AtomicReference<Thread> holder = new AtomicReference<>();
                final AtomicReference<Thread> nextFrameReducer = new AtomicReference<>();
                // The filter takes the callback when it compiles, before the factory it waits on exists.
                TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                    @Override
                    public boolean onGet(Record rec, int count) {
                        final long x = rec.getLong(1);
                        if ((x - 1) % frameRows != 0) {
                            return true;
                        }
                        final long frame = (x - 1) / frameRows;
                        final AsyncStreamGroupByRecordCursorFactory factory = groupBy.get();
                        final int laneCount = factory.getAtom().getLaneCount();
                        final long held = heldFrame.get();
                        if (held > -1 && frame == held + laneCount) {
                            nextFrameReducer.compareAndSet(null, Thread.currentThread());
                        }
                        // The owner publishes the frames, which it may not do while it holds one, so only a worker holds
                        // a frame, and one that leaves room for frame h + 2L in the table.
                        if (frame > 0 && frame + 2L * laneCount <= 64 && Thread.currentThread() != owner && heldFrame.compareAndSet(-1, frame)) {
                            holder.set(Thread.currentThread());
                            final long dispatchable = frame + 2L * laneCount;
                            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                            while (factory.getReducedFrameCount() < dispatchable - 2 || factory.getDispatchedFrameCount() < dispatchable - 1) {
                                if (System.nanoTime() > deadline) {
                                    throw new AssertionError("the frames of the other lanes were not reduced");
                                }
                                Os.pause();
                            }
                            heldState.set("reduced: " + factory.getReducedFrameCount() + ", dispatched: " + factory.getDispatchedFrameCount());
                            // The next lane task to be taken is frame h + L, by this worker.
                            isFailureArmed.set(failure != null);
                        }
                        return true;
                    }
                });
                try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                    final AsyncStreamGroupByRecordCursorFactory stream = (AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory);
                    groupBy.set(stream);
                    final int laneCount = stream.getAtom().getLaneCount();
                    Assert.assertEquals(workers + 1, laneCount);
                    // The owner may reduce every frame of a run.
                    for (int run = 0; run < 100 && heldState.get() == null; run++) {
                        try {
                            Assert.assertEquals(expected, sortedLines(read(factory, context)));
                        } catch (CairoException e) {
                            Assert.assertNotNull(e.getFlyweightMessage().toString(), queryFailure);
                            Assert.assertNotNull(heldState.get());
                            TestUtils.assertContains(e.getFlyweightMessage(), queryFailure);
                        }
                    }
                    final long dispatchable = heldFrame.get() + 2L * laneCount;
                    Assert.assertEquals("reduced: " + (dispatchable - 2) + ", dispatched: " + (dispatchable - 1), heldState.get());
                    if (failure != null) {
                        // The worker took frame h + L and failed before it reduced it.
                        Assert.assertNull(nextFrameReducer.get());
                        Assert.assertFalse(isFailureArmed.get());
                    } else if (blocker != null) {
                        Assert.assertTrue(isBlocked.get());
                        if (blocker.isBlockedForGood()) {
                            Assert.assertEquals(0, engine.getMessageBus().getPageFrameReduceDispatcher().getLaneLaunchCount());
                        }
                        if (blocker.getQueryFailure() == null) {
                            // The single worker reduced frame h + L, on its fiber or on one it launched.
                            Assert.assertSame(holder.get(), nextFrameReducer.get());
                        }
                    } else if (isBatchSpent) {
                        Assert.assertNotNull(nextFrameReducer.get());
                        if (workerPoolMode == WorkerPoolMode.FIBER_HOST) {
                            // A fiber that the holder launched reduced frame h + L.
                            Assert.assertTrue(engine.getMessageBus().getPageFrameReduceDispatcher().getLaneLaunchCount() > 0);
                        } else {
                            // No worker took a lane task: the owner published them all.
                            Assert.assertEquals(0, claimCount.get());
                        }
                    } else {
                        Assert.assertSame(holder.get(), nextFrameReducer.get());
                    }
                    Assert.assertTrue(claimCount.get() > 0 || (isBatchSpent && workerPoolMode == WorkerPoolMode.LEGACY));
                    if (blocker == null || blocker.getQueryFailure() == null) {
                        Assert.assertEquals(expected, sortedLines(read(factory, context)));
                    }
                } finally {
                    TestLatchedCounterFunctionFactory.reset(null);
                    PageFrameSequence.setLaneClaimHookForTesting(null);
                    if (blocker != null && isBlocked.get()) {
                        blocker.unblock(pool, dispatcherRef.get());
                    }
                }
            }, configuration, LOG));
        } finally {
            PageFrameSequence.setLaneClaimHookForTesting(null);
            PageFrameSequence.setLaneRowBudgetForTesting(-1);
        }
    }

    // Runs context.awaitPageFrameSequences() on another thread while this one, the owner, idles, and fails rather than
    // hang when it does not end.
    private static void awaitPageFrameSequences(SqlExecutionContext context) throws InterruptedException {
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread waiter = new Thread(() -> {
            try {
                context.awaitPageFrameSequences();
            } catch (Throwable th) {
                failure.set(th);
            }
        }, "page-frame-sequence-waiter");
        waiter.start();
        waiter.join(TimeUnit.SECONDS.toMillis(60));
        Assert.assertFalse("the wait for the frames in flight did not end", waiter.isAlive());
        Assert.assertNull(failure.get());
    }

    // Asserts the count of the copies in the plan of the query, and the rows it returns, against a hash GROUP BY.
    private static void assertCopyCount(SqlCompiler compiler, SqlExecutionContext context, String sql, int expectedCount) throws Exception {
        assertCopyCount(compiler, context, sql, sql, expectedCount);
    }

    private static void assertCopyCount(
            SqlCompiler compiler,
            SqlExecutionContext context,
            String sql,
            String expectedSql,
            int expectedCount
    ) throws Exception {
        context.setStreamGroupByEnabled(false);
        final String expected;
        try {
            expected = sortedLines(compileAndRead(compiler, context, expectedSql));
        } finally {
            context.setStreamGroupByEnabled(true);
        }
        try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
            final String plan = plan(factory, context);
            int count = 0;
            for (int i = plan.indexOf("Materialize"); i > -1; i = plan.indexOf("Materialize", i + 1)) {
                count++;
            }
            Assert.assertEquals(sql + '\n' + plan, expectedCount, count);
            Assert.assertEquals(sql, expected, sortedLines(read(factory, context)));
            Assert.assertEquals(sql, expected, sortedLines(read(factory, context)));
        }
    }

    // A cursor that its consumer stops reading, as a client that fetches in batches does, keeps no queue slot of its
    // shard. With no workers the owners reduce every frame, so the count of frames that a query reduces itself because its
    // shard has no room for them is exact.
    private void assertPartlyReadLeavesShardFree(
            String holderSql,
            @Nullable Class<? extends RecordCursorFactory> holderFactoryClass
    ) throws Exception {
        Assume.assumeTrue(parallel && workers == 0);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 1);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 16);
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE t AS (SELECT (x * 1_000_000)::TIMESTAMP ts, x, "
                    + "CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END::SYMBOL sym FROM long_sequence("
                    + 200 * frameRows + ")) TIMESTAMP(ts)", context);
            engine.execute("CREATE TABLE p AS (SELECT (x * 1_000_000)::TIMESTAMP ts, x price, "
                    + "CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END::SYMBOL sym FROM long_sequence(100)) TIMESTAMP(ts)", context);
            final String sql = "SELECT ts, count() c, sum(x) s FROM t SAMPLE BY 1s";
            context.setStreamGroupByEnabled(false);
            final String expected = compileAndRead(compiler, context, sql);
            context.setStreamGroupByEnabled(true);
            context.setParallelHorizonJoinEnabled(true);
            context.setParallelWindowJoinEnabled(true);
            try (
                    RecordCursorFactory holderFactory = compiler.compile(holderSql, context).getRecordCursorFactory();
                    RecordCursor holder = holderFactory.getCursor(context);
                    RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()
            ) {
                if (holderFactoryClass != null) {
                    TestUtils.assertFactoryInTree(holderFactory, holderFactoryClass);
                }
                // The holder reads ahead, returns its first row and idles.
                Assert.assertTrue(holder.hasNext());
                Assert.assertEquals(expected, read(factory, context));
                Assert.assertEquals(0, ((AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory)).getLocallyReducedFrameCount());
            }
        });
    }

    private void assertMatchesHash(SqlCompiler compiler, SqlExecutionContext context, String sql) throws Exception {
        assertMatchesHash(compiler, context, sql, parallel);
    }

    private void assertMatchesHash(SqlCompiler compiler, SqlExecutionContext context, String sql, boolean expectedParallel) throws Exception {
        try (RecordCursorFactory expected = compiler.compile(sql.replace("$table", "hash_input"), context).getRecordCursorFactory();
             RecordCursorFactory actual = compiler.compile(sql.replace("$table", "trades"), context).getRecordCursorFactory()) {
            assertStream(expected, false);
            try {
                assertStream(actual, true, expectedParallel);
            } catch (AssertionError error) {
                throw new AssertionError(sql + '\n' + TestUtils.printSqlToString(context.getCairoEngine(), context,
                        "explain " + sql.replace("$table", "trades"), new StringSink()), error);
            }
            final String expectedText = read(expected, context);
            for (int iteration = 0; iteration < 3; iteration++) {
                try (RecordCursor cursor = actual.getCursor(context)) {
                    cursor.hasNext();
                    cursor.toTop();
                    final StringSink sink = new StringSink();
                    CursorPrinter.println(cursor, actual.getMetadata(), sink, true, false);
                    Assert.assertEquals(sql, expectedText, sink.toString());
                    cursor.toTop();
                    sink.clear();
                    CursorPrinter.println(cursor, actual.getMetadata(), sink, true, false);
                    Assert.assertEquals(sql, expectedText, sink.toString());
                }
                try (RecordCursor cursor = actual.getCursor(context)) {
                    cursor.hasNext();
                }
                for (RecordCursorFactory f = actual; f != null; f = f.getBaseFactory()) {
                    if (f instanceof AsyncStreamGroupByRecordCursorFactory stream) {
                        Assert.assertEquals(0, stream.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    }
                }
            }
        }
    }

    private void assertStream(RecordCursorFactory factory, boolean expected) {
        assertStream(factory, expected, parallel);
    }

    private void assertStream(RecordCursorFactory factory, boolean expected, boolean expectedParallel) {
        boolean found = false;
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof AsyncStreamGroupByRecordCursorFactory) {
                Assert.assertTrue(expectedParallel);
                found = true;
            } else if (f instanceof StreamGroupByRecordCursorFactory) {
                Assert.assertFalse(expectedParallel);
                found = true;
            }
        }
        Assert.assertEquals(expected, found);
    }

    // Creates the table of HELD_FRAME_SQL and returns the sorted rows of the query, with the suffix, that a hash GROUP BY
    // reads.
    private String createHeldFrameTable(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext context, String suffix) throws Exception {
        engine.execute("CREATE TABLE t AS (SELECT (x * 1_000_000)::TIMESTAMP ts, x FROM long_sequence(" + 16 * frameRows + ")) TIMESTAMP(ts)", context);
        TestLatchedCounterFunctionFactory.reset(null);
        context.setStreamGroupByEnabled(false);
        try {
            return sortedLines(compileAndRead(compiler, context, HELD_FRAME_SQL + suffix));
        } finally {
            context.setStreamGroupByEnabled(true);
        }
    }

    private static int getStagedFrameCount(AsyncStreamGroupByRecordCursorFactory stream) {
        int count = 0;
        for (int lane = 0, n = stream.getAtom().getLaneCount(); lane < n; lane++) {
            if (stream.getStagedFrame(lane) > -1) {
                count++;
            }
        }
        return count;
    }

    // Opens a cursor of HELD_FRAME_SQL and reads its first row while a worker holds frame h and the owner stages frame h + L
    // behind it. The owner may reduce the first frames of the lanes itself, and then no worker holds one: the run starts
    // over. The caller releases frame h.
    private static RecordCursor openWithHeldFrame(RecordCursorFactory factory, SqlExecutionContext context, FrameHolder holder) throws SqlException {
        final AsyncStreamGroupByRecordCursorFactory stream = (AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory);
        for (int run = 0; run < 100; run++) {
            holder.arm(stream.getAtom().getLaneCount());
            final RecordCursor cursor = factory.getCursor(context);
            try {
                Assert.assertTrue(cursor.hasNext());
                final long heldFrame = holder.getHeldFrame();
                if (heldFrame > 0 && stream.getStagedFrame(holder.getHeldLane()) == heldFrame + holder.getLaneCount()) {
                    return cursor;
                }
            } catch (Throwable th) {
                holder.release();
                cursor.close();
                throw th;
            }
            holder.release();
            cursor.close();
        }
        throw new AssertionError("no worker held a frame");
    }

    private static void convertFirstDayToParquet(CairoEngine engine, SqlExecutionContext context) throws Exception {
        for (String table : new String[]{"trades", "hash_input"}) {
            engine.execute("INSERT INTO " + table + " VALUES ('2024-01-02', 1, 1.0, 'A')", context);
        }
        TestUtils.drainWalQueue(engine);
        convertToParquet(engine, context, "2024-01-01");
    }

    private static void convertToParquet(CairoEngine engine, SqlExecutionContext context, String partition) throws Exception {
        engine.execute("ALTER TABLE trades CONVERT PARTITION TO PARQUET LIST '" + partition + "'", context);
        TestUtils.drainWalQueue(engine);
        Assert.assertEquals(
                "isParquet\ntrue\n",
                TestUtils.printSqlToString(engine, context, "SELECT isParquet FROM table_partitions('trades') WHERE name = '" + partition + "'", new StringSink())
        );
    }

    private static void createTable(SqlCompiler compiler, SqlExecutionContext context) throws Exception {
        context.getCairoEngine().execute("CREATE TABLE trades AS (SELECT "
                + "timestamp_sequence('2024-01-01T00:00:00'::TIMESTAMP, 1_000_000L) ts, "
                + "CASE WHEN x % 11 = 0 THEN NULL ELSE x END v, "
                + "CASE WHEN x % 13 = 0 THEN NULL ELSE (x % 29)::DOUBLE END d, "
                + "CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END::SYMBOL sym "
                + "FROM long_sequence(1001)) TIMESTAMP(ts) PARTITION BY DAY", context);
        context.getCairoEngine().execute("CREATE TABLE hash_input (ts TIMESTAMP, v LONG, d DOUBLE, sym SYMBOL)", context);
        context.getCairoEngine().execute("INSERT INTO hash_input SELECT * FROM trades", context);
    }

    private static void drain(RecordCursorFactory factory, SqlExecutionContext context) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(context)) {
            while (cursor.hasNext()) {
            }
        }
    }

    private static String plan(RecordCursorFactory factory, SqlExecutionContext context) {
        final TextPlanSink sink = new TextPlanSink();
        sink.of(factory, context);
        return sink.getSink().toString();
    }

    private void assertShardMerge(SqlCompiler compiler, SqlExecutionContext context, String sql) throws Exception {
        try (RecordCursorFactory factory = compiler.compile(sql.replace("$table", "trades"), context).getRecordCursorFactory()) {
            read(factory, context);
            if (parallel) {
                Assert.assertTrue(sql, ((AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory)).getAtom().getShardMergeCount() > 0);
            }
        }
    }

    // Three 1m buckets of 60_000 rows: the first holds a key per row, the others 200 keys.
    // The first minute holds 60_000 keys and the next two 201 keys each. 201 shares no factor with the lane counts, so
    // even with a row per frame the span map of each lane holds every key, past the test sharding threshold.
    private static void createSkewedKeysTable(CairoEngine engine, SqlExecutionContext context) throws Exception {
        engine.execute("CREATE TABLE trades AS (SELECT timestamp_sequence('2024-01-01', 1_000L) ts, "
                + "CASE WHEN x <= 60_000 THEN x ELSE x % 201 END k, x v FROM long_sequence(180_000)) TIMESTAMP(ts) PARTITION BY DAY", context);
    }

    // Each 10s bucket holds 10_000 rows of 5_000 keys, past the test sharding threshold, and spans frames.
    private static void createHighCardinalityTables(CairoEngine engine, SqlExecutionContext context) throws Exception {
        engine.execute("CREATE TABLE trades AS (SELECT timestamp_sequence('2024-01-01', 1_000L) ts, x % 5_000 k, "
                + "(x % 5_000)::VARCHAR s, x v, (x % 3)::SYMBOL sym FROM long_sequence(30_000)) TIMESTAMP(ts) PARTITION BY DAY", context);
        engine.execute("CREATE TABLE hash_input (ts TIMESTAMP, k LONG, s VARCHAR, v LONG, sym SYMBOL)", context);
        engine.execute("INSERT INTO hash_input SELECT * FROM trades", context);
    }

    // SPLICE JOIN, HORIZON JOIN and the markout_horizon hint read their operand by row id. Over a stream GROUP BY they
    // read a copy of the node that copiedNodeRegex matches rather than sort rows that arrive in order, and return the
    // rows of the hash GROUP BY. Both factories run with the bind variable at -5 and at 5.
    private static void assertRowIdJoinsCopy(
            SqlCompiler compiler,
            SqlExecutionContext context,
            String operand,
            String copiedNodeRegex
    ) throws Exception {
        final String horizon = "SELECT a.ts, a.c, h.offset, b.y FROM %s a HORIZON JOIN u b RANGE FROM 0s TO 1s STEP 1s AS h";
        final String markout = "SELECT /*+ markout_horizon(a o) */ a.ts + o.usec_offs ts, a.c FROM %s a "
                + "CROSS JOIN (SELECT 1_000_000 * (x - 1) usec_offs FROM long_sequence(2)) o ORDER BY a.ts + o.usec_offs";
        for (String join : new String[]{
                "SELECT * FROM u a SPLICE JOIN %s b",
                "SELECT * FROM %s a SPLICE JOIN u b",
                horizon,
                horizon.formatted("(%s TIMESTAMP(ts))"),
                markout
        }) {
            final String sql = join.formatted(operand);
            final boolean isMarkout = join.equals(markout);
            for (boolean isParallelHorizon : join.contains("HORIZON JOIN") ? new boolean[]{false, true} : new boolean[]{false}) {
                context.setParallelHorizonJoinEnabled(isParallelHorizon);
                context.getBindVariableService().setLong(0, -5);
                context.setStreamGroupByEnabled(false);
                try (RecordCursorFactory expected = compiler.compile(sql, context).getRecordCursorFactory()) {
                    context.setStreamGroupByEnabled(true);
                    try (RecordCursorFactory actual = compiler.compile(sql, context).getRecordCursorFactory()) {
                        final String plan = compileAndRead(compiler, context, "EXPLAIN " + sql);
                        Assert.assertTrue(sql + '\n' + plan, plan.matches("(?s).*\n( *)Materialize\n\\1  +" + copiedNodeRegex + "( workers: \\d+)?\n.*"));
                        if (isMarkout) {
                            Assert.assertTrue(sql + '\n' + plan, plan.matches("(?s).*\n *Markout Horizon Join .*"));
                        } else {
                            Assert.assertFalse(sql + '\n' + plan, plan.matches("(?is).*\\bsort\\b.*"));
                        }
                        for (long bindLimit : new long[]{-5, 5}) {
                            context.getBindVariableService().setLong(0, bindLimit);
                            final String expectedRows = read(expected, context);
                            Assert.assertEquals(sql, expectedRows, read(actual, context));
                            Assert.assertEquals(sql, expectedRows, read(actual, context));
                        }
                    }
                }
            }
        }
    }

    // A filter that fails on row x = failingRow with a typed error makes the stream GROUP BY fail as the hash GROUP BY
    // does: with the same type, position and message. The errors are flyweights, so each is read before the other is
    // built.
    private static void assertFilterFailureMatchesHash(SqlCompiler compiler, SqlExecutionContext context, long failingRow) throws Exception {
        final String sql = "SELECT ts, sum(x) s FROM t WHERE test_latched_counter() SAMPLE BY 1s";
        // The filter takes the callback when it compiles.
        TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
            @Override
            public boolean onGet(Record rec, int count) {
                if (rec.getLong(1) == failingRow) {
                    throw ImplicitCastException.instance().position(42).put("test failure at x=").put(failingRow);
                }
                return true;
            }
        });
        try {
            context.setStreamGroupByEnabled(false);
            final String expected = readError(compiler, context, sql);
            Assert.assertEquals("ImplicitCastException@42: test failure at x=" + failingRow, expected);
            context.setStreamGroupByEnabled(true);
            Assert.assertEquals(sql, expected, readError(compiler, context, sql));
        } finally {
            TestLatchedCounterFunctionFactory.reset(null);
        }
    }

    private static void assertRows(SqlCompiler compiler, SqlExecutionContext context, String sql, String expected) throws Exception {
        try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
            TestUtils.assertEquals(sql, expected, read(factory, context));
            TestUtils.assertEquals(sql, expected, read(factory, context));
        }
    }

    private static void assertSums(SqlCompiler compiler, SqlExecutionContext context, String sql, double[] expected) throws Exception {
        assertValues(compiler, context, sql, "sum", expected);
    }

    private static void assertValues(SqlCompiler compiler, SqlExecutionContext context, String sql, String columnName, double[] expected) throws Exception {
        try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
            final int column = factory.getMetadata().getColumnIndex(columnName);
            final boolean isFloat = factory.getMetadata().getColumnType(column) == ColumnType.FLOAT;
            for (int pass = 0; pass < 2; pass++) {
                try (RecordCursor cursor = factory.getCursor(context)) {
                    final Record record = cursor.getRecord();
                    for (double value : expected) {
                        Assert.assertTrue(sql, cursor.hasNext());
                        Assert.assertEquals(sql, value, isFloat ? record.getFloat(column) : record.getDouble(column), 0);
                    }
                    Assert.assertFalse(sql, cursor.hasNext());
                }
            }
        }
    }

    private static int shardMergeCountOf(SqlCompiler compiler, SqlExecutionContext context, String sql) throws Exception {
        try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
            read(factory, context);
            return ((AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory)).getAtom().getShardMergeCount();
        }
    }

    private static RecordCursorFactory getGroupBy(RecordCursorFactory factory) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof GroupByRecordCursorFactory || f instanceof AsyncGroupByRecordCursorFactory
                    || f instanceof StreamGroupByRecordCursorFactory || f instanceof AsyncStreamGroupByRecordCursorFactory) {
                return f;
            }
        }
        throw new AssertionError("expected group by");
    }

    private static int getSegmentMapKeyCapacity(RecordCursorFactory factory) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof AsyncStreamGroupByRecordCursorFactory stream) {
                return stream.getAtom().getSpanMapKeyCapacity();
            }
            if (f instanceof StreamGroupByRecordCursorFactory stream) {
                return stream.getSegmentMap().getKeyCapacity();
            }
        }
        throw new AssertionError("expected stream group by");
    }

    private static String compileAndRead(SqlCompiler compiler, SqlExecutionContext context, String sql) throws Exception {
        try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
            return read(factory, context);
        }
    }

    private static String read(RecordCursorFactory factory, SqlExecutionContext context) throws Exception {
        final StringSink sink = new StringSink();
        try (RecordCursor cursor = factory.getCursor(context)) {
            CursorPrinter.println(cursor, factory.getMetadata(), sink, true, false);
        }
        return sink.toString();
    }

    private static String readError(SqlCompiler compiler, SqlExecutionContext context, String sql) throws Exception {
        try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
            read(factory, context);
        } catch (ImplicitCastException | NumericException | CairoException e) {
            final FlyweightMessageContainer error = (FlyweightMessageContainer) e;
            return e.getClass().getSimpleName() + '@' + error.getPosition() + ": " + error.getFlyweightMessage();
        }
        throw new AssertionError("expected an error: " + sql);
    }

    // The rows of a bucket come in no particular order.
    private static String sortedLines(CharSequence text) {
        final String[] lines = text.toString().split("\n");
        Arrays.sort(lines);
        return String.join("\n", lines);
    }

    private void assertFirstFrameCapacityMatchesGroups(SqlCompiler compiler, SqlExecutionContext context, String sql) throws Exception {
        try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory();
             RecordCursor cursor = factory.getCursor(context)) {
            Assert.assertTrue(cursor.hasNext());
            final DirectLongList results = ((AsyncStreamGroupByRecordCursorFactory) getGroupBy(factory)).getAtom().getResultValues(0);
            Assert.assertTrue(sql + ": size " + results.size(), results.size() > 2 * 4_096);
            Assert.assertTrue(sql + ": capacity " + results.getCapacity() + " for " + results.size() + " longs",
                    results.getCapacity() <= results.size() * 5 / 4);
        }
    }

    private void assertVectorizedRows(SqlCompiler compiler, SqlExecutionContext context, String sql, String expected) throws Exception {
        try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
            if (parallel) {
                final String plan = plan(factory, context);
                Assert.assertTrue(plan, plan.contains("GroupBy vectorized: true"));
            }
            TestUtils.assertEquals(sql, expected, read(factory, context));
            TestUtils.assertEquals(sql, expected, read(factory, context));
        }
    }

    private void run(CustomisableRunnable runnable) throws Exception {
        assertMemoryLeak(() -> TestUtils.execute(workers == 0 ? null : new TestWorkerPool(workers), runnable, configuration, LOG));
    }

    // Makes the first worker that starts one of the frames 1 to L - 1, the first frames of their lanes, hold it, as frame h,
    // until release(). Frame h keeps its lane busy, so the owner stages frame h + L once it dispatches that far, see
    // PageFrameSequence.stagedTasks, and meanwhile returns the first row of the query from frame 0.
    private static final class FrameHolder implements TestLatchedCounterFunctionFactory.Callback {
        private final int frameRows;
        private final AtomicLong heldFrame = new AtomicLong(-1);
        private final AtomicBoolean isReleased = new AtomicBoolean();
        private final Thread owner = Thread.currentThread();
        private volatile Error error;
        private volatile int laneCount;

        private FrameHolder(int frameRows) {
            this.frameRows = frameRows;
        }

        @Override
        public boolean onGet(Record rec, int count) {
            final long x = rec.getLong(1);
            final long frame = (x - 1) / frameRows;
            if ((x - 1) % frameRows == 0 && frame > 0 && frame < laneCount && Thread.currentThread() != owner && heldFrame.compareAndSet(-1, frame)) {
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                while (!isReleased.get()) {
                    if (System.nanoTime() > deadline) {
                        throw new AssertionError("frame " + frame + " was not released");
                    }
                    Os.pause();
                }
                final Error error = this.error;
                if (error != null) {
                    throw error;
                }
            }
            return true;
        }

        // Makes the next run hold a frame.
        void arm(int laneCount) {
            this.error = null;
            this.laneCount = laneCount;
            isReleased.set(false);
            heldFrame.set(-1);
        }

        // No frame is held from now on.
        void disarm() {
            laneCount = 0;
            error = null;
        }

        // Frame h fails with the error once released.
        void failWith(Error error) {
            this.error = error;
        }

        long getHeldFrame() {
            return heldFrame.get();
        }

        int getHeldLane() {
            return (int) (heldFrame.get() % laneCount);
        }

        int getLaneCount() {
            return laneCount;
        }

        void release() {
            isReleased.set(true);
        }
    }

    private interface LaneLaunchBlocker {
        // Runs on the fiber that took the first lane task, before the fiber tries to launch a fiber for it.
        void block(WorkerPool pool, PageFrameReduceDispatcher dispatcher);

        @Nullable
        default String getQueryFailure() {
            return null;
        }

        // No launch succeeds once block() ran.
        default boolean isBlockedForGood() {
            return true;
        }

        void unblock(WorkerPool pool, PageFrameReduceDispatcher dispatcher);
    }
}
