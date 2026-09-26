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
 * Async-path coverage for parquet row groups split into several bounded sub-frames. Each row group is
 * 1000 rows (CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE) while page.frame.max.rows is 100, so every
 * row group splits into 10 sub-frames. Each sub-frame is its own task. A small reduce queue makes a task
 * land on the reduce queue, be work-stolen, or be reduced locally across GROUP BY (unordered), WINDOW JOIN
 * (ordered, keyed and non-keyed), async filters, and a mid-scan abandon. Results are compared row-for-row
 * against the native (pre-conversion) oracle; decode counts prove that no collector decodes a sub-frame again.
 */
public class ParquetSubFrameAsyncTest extends AbstractCairoTest {
    private static final int ROW_GROUP_COUNT = 20;
    private final AtomicLong decodeCalls = new AtomicLong();
    // Sub-frame windows passed to decodeRowGroup, keyed "rowGroup:rowLo:rowHi".
    private final Set<String> decodedWindows = ConcurrentHashMap.newKeySet();

    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100);
        // WINDOW JOIN reads the master with the small page-frame limits.
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 100);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 1000);
        // A small shard count and reduce queue force the sub-frame tasks through the reduce queue,
        // work-stealing, and the local-task fallback rather than always dispatching to a free worker.
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 1);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        super.setUp();
    }

    @Test
    public void testEarlyCloseOverSplitParquetRowGroupsNoLeak() throws Exception {
        // Abandoning a scan after the first row must free the held collect-queue slot and leak no native
        // memory: assertMemoryLeak wraps the loop and the async filter runs (reducing locally) with the
        // held-task bookkeeping (beginCollectTask / finalizeHeldTask force-collect via close -> await).
        assertMemoryLeak(() -> {
            buildSplitParquet("x");
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
        // The collector must read the reduce output, not decode each sub-frame again.
        // Covers a filter with late materialization and a count-only filter.
        assertMemoryLeak(() -> TestUtils.execute(
                new WorkerPool(() -> 4),
                (engine, compiler, sqlExecutionContext) -> {
                    sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                    createSplitParquet(engine, sqlExecutionContext);
                    assertDecodedOnce(compiler, sqlExecutionContext, "SELECT * FROM x WHERE v % 97 = 0");
                    assertDecodedOnce(compiler, sqlExecutionContext, "SELECT count() FROM x WHERE v % 97 = 0");
                },
                decodeCountingConfiguration(),
                LOG
        ));
    }

    @Test
    public void testGroupBySplitParquetRowGroupsMatchesNative() throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                    engine.execute("CREATE TABLE x (k SYMBOL, v LONG, ts TIMESTAMP) timestamp(ts) PARTITION BY DAY", sqlExecutionContext);
                    // 20_000 rows in 2024-01-01 => 20 parquet row groups of 1000, each split into 10 sub-frames
                    engine.execute(
                            "INSERT INTO x SELECT rnd_symbol('a','b','c','d'), x, timestamp_sequence('2024-01-01', 1000) FROM long_sequence(20_000)",
                            sqlExecutionContext
                    );
                    // a sentinel in a second partition keeps 2024-01-01 non-active so it can be converted
                    engine.execute("INSERT INTO x VALUES ('z', -1, '2024-01-02T00:00:00.000000Z')", sqlExecutionContext);

                    // vectorized parallel group by (reads the split frames directly) and the Map-batching
                    // Async Group By (count_distinct is not vectorized) - the latter is the path whose 24-bit
                    // batch row index issue #2 is about. Both must match the native oracle over split frames.
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
        // The negative-limit collector reads row ids only, so it must not decode a sub-frame.
        // The single allowed repeat is recordAt() over the sub-frame that holds the result row.
        assertMemoryLeak(() -> TestUtils.execute(
                new WorkerPool(() -> 4),
                (engine, compiler, sqlExecutionContext) -> {
                    sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                    createSplitParquet(engine, sqlExecutionContext);
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
    public void testWindowJoinDecodesOnce() throws Exception {
        // The ordered WINDOW JOIN collector must read the master's reduce output, not decode it again.
        assertMemoryLeak(() -> TestUtils.execute(
                new WorkerPool(() -> 4),
                (engine, compiler, sqlExecutionContext) -> {
                    sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                    createSplitParquet(engine, sqlExecutionContext);
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
    public void testWindowJoinSplitParquetRowGroupsMatchesNative() throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                    // master (trades) is the table read through the ordered TYPE_WINDOW_JOIN page-frame
                    // sequence, so its parquet row groups split into sub-frames
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

    private static void createSplitParquet(CairoEngine engine, SqlExecutionContext ctx) throws SqlException {
        engine.execute("CREATE TABLE x (v LONG, d DOUBLE, ts TIMESTAMP) timestamp(ts) PARTITION BY DAY", ctx);
        // 20_000 rows in 2024-01-01 => ROW_GROUP_COUNT parquet row groups of 1000, each split into 10 sub-frames
        engine.execute("INSERT INTO x SELECT x, x * 0.5, timestamp_sequence('2024-01-01', 1000) FROM long_sequence(20_000)", ctx);
        // a sentinel in a second partition keeps 2024-01-01 non-active so it can be converted
        engine.execute("INSERT INTO x VALUES (-1, -1.0, '2024-01-02T00:00:00.000000Z')", ctx);
        engine.execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'", ctx);
    }

    private void assertDecodedOnce(SqlCompiler compiler, SqlExecutionContext ctx, String sql) throws SqlException {
        Assert.assertTrue(sql, drain(compiler, ctx, sql) > 0);
        final String message = sql + " [calls=" + decodeCalls.get() + ", windows=" + decodedWindows.size() + ']';
        // More windows than row groups proves that the row groups split into sub-frames.
        Assert.assertTrue(message, decodedWindows.size() > ROW_GROUP_COUNT);
        Assert.assertEquals(message, decodedWindows.size(), decodeCalls.get());
    }

    private void buildSplitParquet(String table) throws SqlException {
        execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) timestamp(ts) PARTITION BY DAY");
        execute("INSERT INTO " + table + " SELECT x, timestamp_sequence('2024-01-01', 1000) FROM long_sequence(20_000)");
        execute("INSERT INTO " + table + " VALUES (-1, '2024-01-02T00:00:00.000000Z')");
        execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
    }

    // Counts the decodeRowGroup() calls and the distinct sub-frame windows that they decode.
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
