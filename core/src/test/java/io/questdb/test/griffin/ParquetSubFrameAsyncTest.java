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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.mp.WorkerPool;
import io.questdb.std.DirectIntList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Async-path coverage for parquet row groups larger than page.frame.max.rows. Each row group is
 * 1000 rows (CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE) while page.frame.max.rows is 100, yet every
 * row group stays one frame and one task. A small reduce queue makes a task
 * land on the reduce queue, be work-stolen, or be reduced locally across GROUP BY (unordered), WINDOW JOIN
 * (ordered, keyed and non-keyed), async filters, a negative LIMIT over a join that reads ranges of older
 * frames newest-to-oldest, and a mid-scan abandon. Results are compared row-for-row
 * against the native (pre-conversion) oracle; decode counts prove that each row group decodes once.
 */
public class ParquetSubFrameAsyncTest extends AbstractCairoTest {
    private static final int ROW_GROUP_COUNT = 20;
    private final AtomicLong decodeCalls = new AtomicLong();
    // Row windows passed to decodeRowGroup, keyed "rowGroup:rowLo:rowHi".
    private final Set<String> decodedWindows = ConcurrentHashMap.newKeySet();

    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100);
        // WINDOW JOIN reads the master with the small page-frame limits.
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 100);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 1000);
        // A small shard count and reduce queue force the row-group tasks through the reduce queue,
        // work-stealing, and the local-task fallback rather than always dispatching to a free worker.
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 1);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        super.setUp();
    }

    @Test
    public void testEarlyCloseOverLargeParquetRowGroupsNoLeak() throws Exception {
        // Abandoning a scan after the first row must free the held collect-queue slot and leak no native
        // memory: assertMemoryLeak wraps the loop and the async filter runs (reducing locally) with the
        // held-task bookkeeping (beginCollectTask / finalizeHeldTask force-collect via close -> await).
        assertMemoryLeak(() -> {
            buildLargeParquet("x");
            try (RecordCursorFactory factory = select("SELECT v FROM x WHERE ts in '2024-01-01' AND v % 3 = 0")) {
                for (int i = 0; i < 16; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        // read one row, then close mid-scan leaving a held task
                        Assert.assertTrue(cursor.hasNext());
                    }
                }
            }
        });
    }

    @Test
    public void testFilterDecodesOnce() throws Exception {
        // The collector must read the reduce output, not decode each row group again.
        // Covers a filter with late materialization and a count-only filter.
        assertMemoryLeak(() -> TestUtils.execute(
                new WorkerPool(() -> 4),
                (engine, compiler, sqlExecutionContext) -> {
                    sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                    createLargeParquet(engine, sqlExecutionContext);
                    assertDecodedOnce(compiler, sqlExecutionContext, "SELECT * FROM x WHERE v % 97 = 0");
                    assertDecodedOnce(compiler, sqlExecutionContext, "SELECT count() FROM x WHERE v % 97 = 0");
                },
                decodeCountingConfiguration(),
                LOG
        ));
    }

    @Test
    public void testGroupByLargeParquetRowGroupsMatchesNative() throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                    engine.execute("CREATE TABLE x (k SYMBOL, v LONG, ts TIMESTAMP) timestamp(ts) PARTITION BY DAY", sqlExecutionContext);
                    // 20_000 rows in 2024-01-01 => 20 parquet row groups of 1000, each above page.frame.max.rows
                    engine.execute(
                            "INSERT INTO x SELECT rnd_symbol('a','b','c','d'), x, timestamp_sequence('2024-01-01', 1000) FROM long_sequence(20_000)",
                            sqlExecutionContext
                    );
                    // a sentinel in a second partition keeps 2024-01-01 non-active so it can be converted
                    engine.execute("INSERT INTO x VALUES ('z', -1, '2024-01-02T00:00:00.000000Z')", sqlExecutionContext);

                    // vectorized parallel group by (reads the frames directly) and the Map-batching
                    // Async Group By (count_distinct is not vectorized) - the latter is the path whose 24-bit
                    // batch row index issue #2 is about. Both must match the native oracle over parquet frames.
                    final String vectorized = "SELECT k, count(), sum(v), min(v), max(v) FROM x WHERE ts in '2024-01-01' ORDER BY k";
                    final String batched = "SELECT k, count_distinct(v) cd, sum(v) sv FROM x WHERE ts in '2024-01-01' ORDER BY k";
                    assertPlanContains(compiler, sqlExecutionContext, vectorized, "GroupBy vectorized: true");
                    assertPlanContains(compiler, sqlExecutionContext, batched, "Async Group By");

                    final StringSink vectorizedOracle = new StringSink();
                    final StringSink batchedOracle = new StringSink();
                    TestUtils.printSql(compiler, sqlExecutionContext, vectorized, vectorizedOracle);
                    TestUtils.printSql(compiler, sqlExecutionContext, batched, batchedOracle);

                    engine.execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'", sqlExecutionContext);

                    final StringSink vectorizedActual = new StringSink();
                    final StringSink batchedActual = new StringSink();
                    TestUtils.printSql(compiler, sqlExecutionContext, vectorized, vectorizedActual);
                    TestUtils.printSql(compiler, sqlExecutionContext, batched, batchedActual);

                    Assert.assertTrue("expected non-empty result", vectorizedOracle.length() > 0);
                    TestUtils.assertEquals(vectorizedOracle, vectorizedActual);
                    TestUtils.assertEquals(batchedOracle, batchedActual);
                },
                configuration,
                LOG
        );
    }

    @Test
    public void testNegativeLimitDecodesOnce() throws Exception {
        // The negative-limit collector reads row ids only, so it must not decode a row group.
        // The single allowed repeat is recordAt() over the row group that holds the result row.
        assertMemoryLeak(() -> TestUtils.execute(
                new WorkerPool(() -> 4),
                (engine, compiler, sqlExecutionContext) -> {
                    sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                    createLargeParquet(engine, sqlExecutionContext);
                    final String sql = "SELECT * FROM x WHERE v > 19_990 LIMIT -1";
                    Assert.assertEquals(sql, 1, drain(compiler, sqlExecutionContext, sql));
                    Assert.assertTrue(sql, decodedWindows.size() > 0);
                    Assert.assertTrue(
                            sql + " [calls=" + decodeCalls.get() + ", windows=" + decodedWindows.size() + ']',
                            decodeCalls.get() - decodedWindows.size() <= 1
                    );
                },
                decodeCountingConfiguration(),
                LOG
        ));
    }

    @Test
    public void testNegativeLimitOverJoinReadsOlderFramesOfParquetAndNativePartitions() throws Exception {
        // A negative LIMIT over a join reads the last rows of the master's async filter, then ranges of older
        // frames newest-to-oldest until the join emits the rows it selects. The master alternates parquet and
        // native partitions, so the ranges cross parquet row groups, native page frames and partition edges.
        setProperty(PropertyKey.CAIRO_SQL_MAX_NEGATIVE_LIMIT, 4);
        // the owner thread reduces every frame
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                assertNegativeLimitOverJoinReadsOlderFrames(engine, compiler, sqlExecutionContext);
            }
        });
        // workers reduce the frames
        assertMemoryLeak(() -> TestUtils.execute(
                new WorkerPool(() -> 4),
                this::assertNegativeLimitOverJoinReadsOlderFrames,
                configuration,
                LOG
        ));
    }

    @Test
    public void testWindowJoinDecodesOnce() throws Exception {
        // The ordered WINDOW JOIN collector must read the master's reduce output, not decode it again.
        assertMemoryLeak(() -> TestUtils.execute(
                new WorkerPool(() -> 4),
                (engine, compiler, sqlExecutionContext) -> {
                    sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                    createLargeParquet(engine, sqlExecutionContext);
                    // The native slave adds no parquet decodes.
                    engine.execute("CREATE TABLE p (price DOUBLE, ts TIMESTAMP) timestamp(ts) PARTITION BY DAY", sqlExecutionContext);
                    engine.execute(
                            "INSERT INTO p SELECT rnd_double(), timestamp_sequence('2024-01-01', 500_000) FROM long_sequence(60)",
                            sqlExecutionContext
                    );
                    final String sql = "SELECT x.v, sum(p.price) FROM x " +
                            "WINDOW JOIN p RANGE BETWEEN 1 second preceding AND 1 second following";
                    assertPlanContains(compiler, sqlExecutionContext, sql, "Async Window Join");
                    assertDecodedOnce(compiler, sqlExecutionContext, sql);
                },
                decodeCountingConfiguration(),
                LOG
        ));
    }

    @Test
    public void testWindowJoinLargeParquetRowGroupsMatchesNative() throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                    // master (trades) is the table read through the ordered TYPE_WINDOW_JOIN page-frame
                    // sequence, with parquet row groups above page.frame.max.rows
                    engine.execute("CREATE TABLE trades (sym SYMBOL, price DOUBLE, ts TIMESTAMP) timestamp(ts) PARTITION BY DAY", sqlExecutionContext);
                    engine.execute(
                            "INSERT INTO trades SELECT rnd_symbol('a','b','c'), rnd_double(), timestamp_sequence('2024-01-01', 1000) FROM long_sequence(20_000)",
                            sqlExecutionContext
                    );
                    engine.execute("INSERT INTO trades VALUES ('z', -1.0, '2024-01-02T00:00:00.000000Z')", sqlExecutionContext);
                    engine.execute("CREATE TABLE prices (sym SYMBOL, price DOUBLE, ts TIMESTAMP) timestamp(ts) PARTITION BY DAY", sqlExecutionContext);
                    engine.execute(
                            "INSERT INTO prices SELECT rnd_symbol('a','b','c'), rnd_double(), timestamp_sequence('2024-01-01', 500_000) FROM long_sequence(60)",
                            sqlExecutionContext
                    );

                    final String nonKeyed = "SELECT t.sym, t.ts, sum(p.price) wp, count(p.price) cp FROM trades t " +
                            "WINDOW JOIN prices p RANGE BETWEEN 1 second preceding AND 1 second following " +
                            "ORDER BY t.ts, t.sym";
                    final String keyed = "SELECT t.sym, t.ts, sum(p.price) wp, count(p.price) cp FROM trades t " +
                            "WINDOW JOIN prices p ON (t.sym = p.sym) RANGE BETWEEN 1 second preceding AND 1 second following " +
                            "ORDER BY t.ts, t.sym";
                    assertPlanContains(compiler, sqlExecutionContext, nonKeyed, "Async Window Join");
                    assertPlanContains(compiler, sqlExecutionContext, keyed, "Async Window Fast Join");

                    final StringSink nonKeyedOracle = new StringSink();
                    final StringSink keyedOracle = new StringSink();
                    TestUtils.printSql(compiler, sqlExecutionContext, nonKeyed, nonKeyedOracle);
                    TestUtils.printSql(compiler, sqlExecutionContext, keyed, keyedOracle);

                    engine.execute("ALTER TABLE trades CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'", sqlExecutionContext);

                    final StringSink nonKeyedActual = new StringSink();
                    final StringSink keyedActual = new StringSink();
                    TestUtils.printSql(compiler, sqlExecutionContext, nonKeyed, nonKeyedActual);
                    TestUtils.printSql(compiler, sqlExecutionContext, keyed, keyedActual);

                    Assert.assertTrue("expected non-empty result", nonKeyedOracle.length() > 0);
                    TestUtils.assertEquals(nonKeyedOracle, nonKeyedActual);
                    TestUtils.assertEquals(keyedOracle, keyedActual);
                },
                configuration,
                LOG
        );
    }

    private static void assertPlanContains(SqlCompiler compiler, SqlExecutionContext ctx, String sql, String fragment) throws Exception {
        final StringSink sink = new StringSink();
        try (RecordCursorFactory factory = compiler.compile("EXPLAIN " + sql, ctx).getRecordCursorFactory();
             RecordCursor cursor = factory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                sink.put(record.getStrA(0)).put('\n');
            }
        }
        TestUtils.assertContains(sink, fragment);
    }

    private static void createLargeParquet(CairoEngine engine, SqlExecutionContext ctx) throws SqlException {
        engine.execute("CREATE TABLE x (v LONG, d DOUBLE, ts TIMESTAMP) timestamp(ts) PARTITION BY DAY", ctx);
        // 20_000 rows in 2024-01-01 => ROW_GROUP_COUNT parquet row groups of 1000, each above page.frame.max.rows
        engine.execute("INSERT INTO x SELECT x, x * 0.5, timestamp_sequence('2024-01-01', 1000) FROM long_sequence(20_000)", ctx);
        // a sentinel in a second partition keeps 2024-01-01 non-active so it can be converted
        engine.execute("INSERT INTO x VALUES (-1, -1.0, '2024-01-02T00:00:00.000000Z')", ctx);
        engine.execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'", ctx);
    }

    private static String lastRows(String rows, int count) {
        final int headerEnd = rows.indexOf('\n');
        int start = rows.length() - 1;
        for (int i = 0; i < count && start > headerEnd; i++) {
            start = rows.lastIndexOf('\n', start - 1);
        }
        return rows.substring(0, headerEnd + 1) + rows.substring(start + 1);
    }

    private void assertDecodedOnce(SqlCompiler compiler, SqlExecutionContext ctx, String sql) throws SqlException {
        Assert.assertTrue(sql, drain(compiler, ctx, sql) > 0);
        final String message = sql + " [calls=" + decodeCalls.get() + ", windows=" + decodedWindows.size() + ']';
        // One window per row group proves that no row group splits into sub-frames.
        Assert.assertEquals(message, ROW_GROUP_COUNT, decodedWindows.size());
        Assert.assertEquals(message, ROW_GROUP_COUNT, decodeCalls.get());
    }

    private void assertNegativeLimitOverJoinReadsOlderFrames(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext ctx
    ) throws Exception {
        // 2_880 rows a day: 2024-01-01 and 2024-01-03 become parquet partitions of three row groups each,
        // 2024-01-02 and 2024-01-04 stay native and split into page frames of 100 rows
        engine.execute(
                "CREATE TABLE m AS (SELECT x::INT i, x % 7 l, timestamp_sequence('2024-01-01', 30_000_000) ts FROM long_sequence(10_000))"
                        + " TIMESTAMP(ts) PARTITION BY DAY",
                ctx
        );
        // master rows 5 (three slave rows), 2_000, 2_001 sit in 2024-01-01, 4_000 in 2024-01-02, 6_500 in
        // 2024-01-03 and 9_999 in 2024-01-04; master row 7 fails the filter
        engine.execute("CREATE TABLE s (k INT, v LONG)", ctx);
        engine.execute("INSERT INTO s VALUES (5, 1), (5, 2), (5, 3), (7, 4), (2_000, 5), (2_001, 6), (4_000, 7), (6_500, 8), (9_999, 9)", ctx);
        // fewer joined rows than the LIMIT selects
        engine.execute("CREATE TABLE f (k INT, v LONG)", ctx);
        engine.execute("INSERT INTO f VALUES (10, 1), (3_000, 2)", ctx);
        // no joined row
        engine.execute("CREATE TABLE z (k INT, v LONG)", ctx);
        engine.execute("INSERT INTO z VALUES (100_001, 1), (100_002, 2)", ctx);
        final String[] joins = {
                "SELECT m.i, s.v FROM m JOIN s ON m.i = s.k WHERE m.l > 0",
                "SELECT m.i, f.v FROM m JOIN f ON m.i = f.k WHERE m.l > 0",
                "SELECT m.i, z.v FROM m JOIN z ON m.i = z.k WHERE m.l > 0",
        };
        // the oracle: every joined row of the native master, without a LIMIT
        final String[] all = new String[joins.length];
        final StringSink sink = new StringSink();
        for (int i = 0; i < joins.length; i++) {
            sink.clear();
            TestUtils.printSql(compiler, ctx, joins[i], sink);
            all[i] = sink.toString();
        }
        Assert.assertEquals(
                """
                        i\tv
                        5\t3
                        5\t2
                        5\t1
                        2000\t5
                        2001\t6
                        4000\t7
                        6500\t8
                        9999\t9
                        """,
                all[0]
        );
        Assert.assertEquals("i\tv\n10\t1\n3000\t2\n", all[1]);
        Assert.assertEquals("i\tv\n", all[2]);

        engine.execute("ALTER TABLE m CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-03'", ctx);
        for (int jitMode : new int[]{SqlJitMode.JIT_MODE_DISABLED, SqlJitMode.JIT_MODE_ENABLED}) {
            ctx.setJitMode(jitMode);
            for (int i = 0; i < joins.length; i++) {
                for (int n = 1; n < 5; n++) {
                    final String query = joins[i] + " LIMIT -" + n;
                    final String expected = lastRows(all[i], n);
                    assertQuery(query)
                            .noLeakCheck()
                            .withEngine(engine)
                            .withCompiler(compiler)
                            .withContext(ctx)
                            .assertsPlanContaining(jitMode == SqlJitMode.JIT_MODE_ENABLED ? "Async JIT Filter" : "Async Filter", "limit: " + n);
                    assertQuery(query)
                            .noLeakCheck()
                            .withEngine(engine)
                            .withCompiler(compiler)
                            .withContext(ctx)
                            .noRandomAccess()
                            .expectSize()
                            .returns(expected);
                    // without the parallel filter, the LIMIT runs the join once over every master row
                    ctx.setParallelFilterEnabled(false);
                    try {
                        sink.clear();
                        TestUtils.printSql(compiler, ctx, query, sink);
                        TestUtils.assertEquals(query, expected, sink);
                    } finally {
                        ctx.setParallelFilterEnabled(true);
                    }
                }
            }
        }
        // the next engine of the test opens the same database
        engine.execute("DROP TABLE m", ctx);
        engine.execute("DROP TABLE s", ctx);
        engine.execute("DROP TABLE f", ctx);
        engine.execute("DROP TABLE z", ctx);
    }

    private void buildLargeParquet(String table) throws SqlException {
        execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) timestamp(ts) PARTITION BY DAY");
        execute("INSERT INTO " + table + " SELECT x, timestamp_sequence('2024-01-01', 1000) FROM long_sequence(20_000)");
        execute("INSERT INTO " + table + " VALUES (-1, '2024-01-02T00:00:00.000000Z')");
        execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
    }

    // Counts the decodeRowGroup() calls and the distinct row windows that they decode.
    private CairoConfiguration decodeCountingConfiguration() {
        return new CairoConfigurationWrapper(configuration) {
            @Override
            public ParquetPartitionDecoder newParquetPartitionDecoder() {
                return new ParquetPartitionDecoder() {
                    @Override
                    public int decodeRowGroup(RowGroupBuffers rowGroupBuffers, DirectIntList columns, int rowGroupIndex, int rowLo, int rowHi) {
                        decodeCalls.incrementAndGet();
                        decodedWindows.add(rowGroupIndex + ":" + rowLo + ":" + rowHi);
                        return super.decodeRowGroup(rowGroupBuffers, columns, rowGroupIndex, rowLo, rowHi);
                    }
                };
            }
        };
    }

    // Resets the decode counters, runs the query to the end and returns the row count.
    private long drain(SqlCompiler compiler, SqlExecutionContext ctx, String sql) throws SqlException {
        decodeCalls.set(0);
        decodedWindows.clear();
        long rows = 0;
        try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory();
             RecordCursor cursor = factory.getCursor(ctx)) {
            while (cursor.hasNext()) {
                rows++;
            }
        }
        return rows;
    }
}
