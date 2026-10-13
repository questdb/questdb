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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.table.AsyncGroupByNotKeyedRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.IndexGatherPageFrameCursor;
import io.questdb.griffin.engine.table.IndexGatherRecordCursorFactory;
import io.questdb.griffin.engine.table.IndexScanChoiceRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Misc;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The index-driven parallel GROUP BY ({@code cairo.sql.parallel.index.groupby.enabled}, the plan
 * {@code Async Index Group By}) and the choice between an index plan and a scan
 * ({@code cairo.sql.index.scan.choice.enabled}, the plan {@code Index Or Scan}), against the same
 * queries compiled with both off: a serial GROUP BY over the index scan.
 * <p>
 * <b>Tolerance.</b> Without a worker pool the query's thread runs every task, and a group that
 * draws from one key sees its rows in table order, as the serial plan does: such results must be
 * identical. A group that draws from several keys (grouped by another column, or not keyed) adds
 * its keys' rows key by key rather than in table order, and with a worker pool partial results
 * merge in an order that changes from run to run, as for any parallel GROUP BY: floating sums,
 * averages and the Welford/Chan states are then compared within 1e-9 relative, everything else
 * exactly.
 */
public class IndexGatherGroupByTest extends AbstractCairoTest {
    // symbols of every frequency: S0 is in about a third of the rows, S1..S9 are mid, R0..R49 are
    // rare, P0..P9 are in some partitions only, and every 23rd row is NULL
    private static final String AGGREGATES = "count() n, sum(x) sx, avg(x) ax, min(y) mny, max(y) mxy, sum(z) sz, " +
            "weighted_avg(x, y) wa, stddev_samp(x) sd, corr(x, f) cr, min(f) mnf, max(ts) mxt, count(y) cy";
    private static final int ROWS = 30_000;
    // order-sensitive aggregates over keys split by row range, see testOrderSensitiveOnWorkerPool()
    private static final String[] ORDER_SENSITIVE_QUERIES = {
            "select sym, array_agg(x) a from t where sym in ('A', 'B') order by sym",
            "select array_agg(x) a from t where sym = 'A'",
            "select sym, timestamp_floor('10m', ts) m, array_agg(x) a from t where sym in ('A', 'B') order by sym, m",
            "select sym, mode(y) m from t where sym in ('A', 'B') order by sym",
            "select sym, arg_max(x, y) am, arg_min(x, y) an from t where sym in ('A', 'B') order by sym",
            "select arg_max(x, y) am, arg_min(x, y) an, mode(y) m from t where sym = 'A'",
            "select sym, first(x) f, last(x) l, first_not_null(x) fn, last_not_null(x) ln from t where sym in ('A', 'B') order by sym",
    };

    @Override
    public void setUp() {
        super.setUp();
        // frames, tasks and batches smaller than the data, so a key's rows cross frames and tasks
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_INDEX_GROUPBY_FRAME_ROWS, 1024);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_INDEX_GROUPBY_TASK_ROWS, 300);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_BATCH_SIZE, 61);
        // maps past a few hundred groups shard, unless the grouping includes the key column
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 300);
    }

    @Test
    public void testAbsentAndDuplicateKeys() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "t", ROWS, 3);
            assertSameAsSerial("select sym, " + AGGREGATES + " from t where sym in ('S1', 'NOPE', 'S1', 'R3', 'R3') order by sym", true);
            assertSameAsSerial("select sym, " + AGGREGATES + " from t where sym in ('NOPE', 'NADA') order by sym", true);
            assertSameAsSerial("select " + AGGREGATES + " from t where sym in ('NOPE', 'NADA')", true);
            assertSameAsSerial("select " + AGGREGATES + " from t where sym = 'NOPE'", true);
            // keys of some partitions only
            assertSameAsSerial("select sym, " + AGGREGATES + " from t where sym in ('P0', 'P3', 'P7', 'R1') order by sym", true);
        });
    }

    @Test
    public void testBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "t", ROWS, 5);
            final String sql = "select sym, " + AGGREGATES + " from t where sym in ($1, $2, $3) order by sym";
            final String[][] binds = {{"S1", "R2", "P1"}, {"S0", null, "S2"}, {"NOPE", "R9", "R9"}, {null, null, null}};
            for (String[] values : binds) {
                bindVariableService.clear();
                for (int i = 0; i < values.length; i++) {
                    bindVariableService.setStr(i, values[i]);
                }
                assertSameAsSerial(sql, true);
            }
            final String single = "select sym, " + AGGREGATES + " from t where sym = $1 order by sym";
            for (String value : new String[]{"S3", null, "R7", "NOPE"}) {
                bindVariableService.clear();
                bindVariableService.setStr(0, value);
                assertSameAsSerial(single, true);
            }
        });
    }

    @Test
    public void testCachedFactoryReruns() throws Exception {
        // one factory, three executions, with rows committed between them: the readers it froze
        // are unfrozen, so the next execution sees the new rows
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "t", ROWS, 7);
            final String sql = "select sym, " + AGGREGATES + " from t where sym in ('S1', 'R4', 'P2') order by sym";
            setFeatures(true, false);
            try (RecordCursorFactory factory = select(sql)) {
                for (int round = 0; round < 3; round++) {
                    final String actual = print(factory);
                    setFeatures(false, false);
                    final String expected = runToString(engine, sqlExecutionContext, sql);
                    setFeatures(true, false);
                    TestUtils.assertEquals(sql, expected, actual);
                    final IndexGatherPageFrameCursor cursor = findGather(factory).getPageFrameCursorForTesting();
                    Assert.assertEquals(0, cursor.getFrozenReaderCount());
                    Assert.assertTrue(cursor.getTaskCounts()[0] > 0);
                    execute("insert into t select rnd_symbol('S1', 'R4', 'P2'), rnd_double(), rnd_int(0, 1000, 5), rnd_long(), rnd_float(), rnd_symbol('a', 'b', ''), " +
                            "'1970-01-0" + (3 + round) + "'::timestamp + x * 1_000_000L from long_sequence(500)");
                }
            }
        });
    }

    @Test
    public void testCancellation() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicLong ticks = new AtomicLong();
            final long[] tripAfter = {Long.MAX_VALUE};
            final DefaultSqlExecutionCircuitBreakerConfiguration cbConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
                @Override
                @NotNull
                public MillisecondClock getClock() {
                    return () -> ticks.incrementAndGet() < tripAfter[0] ? 0 : Long.MAX_VALUE;
                }

                @Override
                public long getQueryTimeout() {
                    return 1;
                }
            };
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, context) -> {
                final io.questdb.griffin.SqlExecutionContextImpl ctx = (io.questdb.griffin.SqlExecutionContextImpl) context;
                createTable(engine, ctx, "t", 100_000, 9);
                final String[] queries = {
                        "select sym, " + AGGREGATES + " from t where sym in ('S0', 'S1', 'S2') order by sym",
                        "select " + AGGREGATES + " from t where sym in ('S0', 'S1', 'S2')",
                };
                setFeatures(true, false);
                for (String sql : queries) {
                    final NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(engine, cbConfiguration);
                    try {
                        ctx.with(ctx.getSecurityContext(), ctx.getBindVariableService(), ctx.getRandom(), ctx.getRequestFd(), circuitBreaker);
                        tripAfter[0] = Long.MAX_VALUE;
                        try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
                            Assert.assertNotNull(sql, findGather(factory));
                            ticks.set(0);
                            tripAfter[0] = 5;
                            circuitBreaker.resetTimer();
                            try (RecordCursor cursor = factory.getCursor(ctx)) {
                                //noinspection StatementWithEmptyBody
                                while (cursor.hasNext()) {
                                    // drain
                                }
                                Assert.fail("expected a timeout: " + sql);
                            } catch (Throwable e) {
                                TestUtils.assertContains(e.getMessage(), "timeout, query aborted");
                            }
                            // the readers are unfrozen
                            Assert.assertEquals(0, findGather(factory).getPageFrameCursorForTesting().getFrozenReaderCount());
                        }
                    } finally {
                        Misc.free(circuitBreaker);
                    }
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testChoiceFallbackRebindsSymbolTables() throws Exception {
        // A consumer takes the GROUP BY's symbol tables when its cursor opens, before the first
        // row: a filter or a projection over it. When the scan plan then hits the memory limit and
        // the index plan opens in its place, the scan cursor and its table reader are closed.
        // What the consumer took must answer from the index plan's cursor from then on, not from
        // that closed reader, which the pool may close for good.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                engine.execute("create table m (sym symbol index type posting, k varchar, v long, ts timestamp) timestamp(ts) partition by day bypass wal", ctx);
                engine.execute("insert into m select rnd_symbol('A', 'B', 'C', 'D'), (x % 40_000)::varchar, x, timestamp_sequence(0, 1_000_000L) from long_sequence(200_000)", ctx);
                final String sql = "select sym, k, count() n, sum(v) s from m where sym in ('A', 'B', 'C')";
                setFeatures(false, false);
                final String expected = runToString(engine, ctx, sql + " order by sym, k");
                // the scan plan whatever the selectivity, so that its fallback is what runs
                setProperty(PropertyKey.CAIRO_SQL_INDEX_SCAN_CHOICE_MAX_SELECTIVITY, "0.0");
                int fallbacks = 0;
                for (long limit = 2L << 20; limit <= 48L << 20; limit += 1L << 20) {
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit);
                    // limits at which the index plan, which the fallback opens, completes
                    setFeatures(true, false);
                    try {
                        runToString(engine, ctx, sql);
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isOutOfMemory());
                        continue;
                    }
                    setFeatures(true, true);
                    try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
                        final IndexScanChoiceRecordCursorFactory chooser = findChoice(factory);
                        Assert.assertNotNull(chooser);
                        final long fallbacksBefore = chooser.getMemoryLimitFallbacks();
                        final StringSink sink = new StringSink();
                        try (RecordCursor cursor = factory.getCursor(ctx)) {
                            // taken before the first row, as a consumer's init() takes them
                            final SymbolTable shared = cursor.getSymbolTable(0);
                            final SymbolTable own = cursor.newSymbolTable(0);
                            final StaticSymbolTable staticTable = shared instanceof SymbolFunction f ? f.getStaticSymbolTable() : (StaticSymbolTable) shared;
                            Assert.assertNotNull(staticTable);
                            // size() before the first row must not lose the fallback either
                            final long size = cursor.size();
                            final Record record = cursor.getRecord();
                            boolean first = true;
                            while (cursor.hasNext()) {
                                if (first) {
                                    first = false;
                                    if (chooser.getMemoryLimitFallbacks() > fallbacksBefore) {
                                        // close the readers nobody holds, the scan plan's among
                                        // them: idle for a day, as far as the pool can tell
                                        setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks() + 86_400_000_000L);
                                        try {
                                            engine.releaseInactive();
                                        } finally {
                                            setCurrentMicros(-1);
                                        }
                                    }
                                }
                                final int key = record.getInt(0);
                                final CharSequence value = record.getSymA(0);
                                TestUtils.assertEquals(value, shared.valueOf(key));
                                TestUtils.assertEquals(value, own.valueOf(key));
                                TestUtils.assertEquals(value, staticTable.valueOf(key));
                                Assert.assertEquals(key, staticTable.keyOf(value));
                                sink.put(value).put('\t').put(record.getVarcharA(1)).put('\t').put(record.getLong(2)).put('\t').put(record.getLong(3)).put('\n');
                            }
                            Assert.assertTrue(size == -1 || size > 0);
                            Misc.freeIfCloseable(own);
                        }
                        if (chooser.getMemoryLimitFallbacks() > fallbacksBefore) {
                            fallbacks++;
                            // the rows are the serial plan's, in any order
                            final String[] e = expected.substring(expected.indexOf('\n') + 1).split("\n");
                            final String[] a = sink.toString().split("\n");
                            java.util.Arrays.sort(e);
                            java.util.Arrays.sort(a);
                            Assert.assertArrayEquals("limit " + limit, e, a);
                        }
                    }
                }
                Assert.assertTrue("no limit made the scan plan fall back", fallbacks > 0);

                // The pool hands the index plan the reader the scan plan just returned, so above
                // the tables taken early stay readable even when they are the scan's. Here the
                // scan plan's tables fail once its cursor has closed, as a closed reader's would.
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 48L << 20);
                setFeatures(true, true);
                try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
                    final IndexGatherRecordCursorFactory gather = findGather(factory);
                    Assert.assertNotNull(gather);
                    final boolean[] scanClosed = {false};
                    // the query registry gives a compiled query its memory tracker; this factory is
                    // not registered, so it gets one by hand
                    final io.questdb.std.MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(
                            ctx.getSecurityContext(),
                            Long.MAX_VALUE,
                            io.questdb.std.MemoryTrackerWorkload.QUERY
                    );
                    final io.questdb.std.MemoryTracker prevTracker = ctx.getMemoryTracker();
                    ctx.setMemoryTracker(tracker);
                    Assert.assertTrue(tracker.getLimit() > 0);
                    // a negative selectivity takes the scan plan whatever the estimate
                    try (
                            RecordCursorFactory chooser = new IndexScanChoiceRecordCursorFactory(
                                    new FakePlan(factory.getMetadata(), null, false),
                                    new FakePlan(factory.getMetadata(), scanClosed, true),
                                    gather,
                                    -1.0
                            );
                            RecordCursor cursor = chooser.getCursor(ctx)
                    ) {
                        final SymbolTable shared = cursor.getSymbolTable(0);
                        final SymbolTable own = cursor.newSymbolTable(0);
                        Assert.assertTrue(shared instanceof SymbolFunction);
                        final StaticSymbolTable staticTable = ((SymbolFunction) shared).getStaticSymbolTable();
                        Assert.assertNotNull(staticTable);
                        // the scan plan hits the limit in size(), before any row
                        Assert.assertEquals(-1, cursor.size());
                        Assert.assertTrue("the scan plan's cursor did not close", scanClosed[0]);
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(1, ((IndexScanChoiceRecordCursorFactory) chooser).getMemoryLimitFallbacks());
                        TestUtils.assertEquals("v7", shared.valueOf(7));
                        TestUtils.assertEquals("v7", shared.valueBOf(7));
                        TestUtils.assertEquals("v7", own.valueOf(7));
                        TestUtils.assertEquals("v7", staticTable.valueOf(7));
                        Assert.assertEquals(7, staticTable.keyOf("v7"));
                        Assert.assertEquals(FakeSymbolTable.COUNT, staticTable.getSymbolCount());
                        Assert.assertFalse(cursor.hasNext());
                        Misc.freeIfCloseable(own);
                    } finally {
                        ctx.setMemoryTracker(prevTracker);
                        tracker.close();
                    }
                }
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
            }, configuration, LOG);
        });
    }

    @Test
    public void testColumnTopInsideIndexedPartition() throws Exception {
        // the symbol column added to a partition that has rows, then rows appended in order to that
        // same partition: its index starts at the column top, and the rows above it are NULL
        assertMemoryLeak(() -> {
            execute("create table t (x double, ts timestamp) timestamp(ts) partition by day bypass wal");
            execute("insert into t select x::double, (x * 1_000_000L)::timestamp from long_sequence(5000)");
            execute("alter table t add column sym symbol index type posting");
            execute("insert into t (x, ts, sym) select 5000 + x, (5000_000_000L + x * 1_000_000L)::timestamp, rnd_symbol('A', 'B', null) from long_sequence(5000)");
            execute("insert into t (x, ts, sym) select 10000 + x, ('1970-01-02'::timestamp + x * 1_000_000L), rnd_symbol('A', 'B', null) from long_sequence(5000)");
            assertSameAsSerial("select sym, count() n, sum(x) s, min(x) mn, max(x) mx from t where sym in ('A', null) order by sym", true);
            assertSameAsSerial("select count() n, sum(x) s, min(x) mn, max(x) mx from t where sym in (null)", true);
            assertSameAsSerial("select count() n, sum(x) s, min(x) mn, max(x) mx from t where sym = 'B'", true);
            assertSameAsSerial("select sym, count() n, sum(x) s from t where sym = null order by sym", true);
            assertSameAsSerial("select sym, first(x) f, last(x) l, array_agg(x) a from t where sym in ('A', null, 'B') order by sym", true);
        });
    }

    @Test
    public void testColumnTops() throws Exception {
        // the symbol and an aggregated column added after the first partitions: rows before the
        // tops are NULL, and partitions without the symbol column have no posting reader
        assertMemoryLeak(() -> {
            execute("create table t (x double, ts timestamp) timestamp(ts) partition by day bypass wal");
            execute("insert into t select rnd_double(), timestamp_sequence(0, 20_000_000L) from long_sequence(8000)");
            execute("alter table t add column sym symbol index type posting");
            execute("alter table t add column y int");
            execute("insert into t (x, ts, sym, y) select rnd_double(), '1970-01-01T12'::timestamp + x * 10_000_000L, " +
                    "rnd_symbol('A', 'B', 'C', null), rnd_int(0, 100, 3) from long_sequence(12_000)");
            final String[] lists = {"('A', 'B')", "('A', null)", "(null)", "('C')"};
            for (String list : lists) {
                assertSameAsSerial("select sym, count() n, sum(x) sx, min(y) mn, max(y) mx, count(y) cy from t where sym in " + list + " order by sym", true);
                assertSameAsSerial("select count() n, sum(x) sx, min(y) mn, count(y) cy from t where sym in " + list, false);
            }
            assertSameAsSerial("select sym, count() n, sum(x) sx from t where sym = null order by sym", true);
            assertSameAsSerial("select count() n, sum(x) sx from t where sym is null", true);
        });
    }

    @Test
    public void testExplain() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "t", 1000, 11);
            setFeatures(true, false);
            assertPlan(
                    "select sym, sum(x) from t where sym in ('S1', 'S2') order by sym",
                    "Encode sort light\n" +
                            "  keys: [sym]\n" +
                            "    Async Index Group By workers: 1\n" +
                            "      keys: [sym]\n" +
                            "      values: [sum(x)]\n" +
                            "      filter: null\n" +
                            "        FilterOnValues symbolOrder: desc\n" +
                            "            Cursor-order scan\n" +
                            "                Index forward scan on: sym deferred: true\n" +
                            "                  filter: sym='S1'\n" +
                            "                Index forward scan on: sym deferred: true\n" +
                            "                  filter: sym='S2'\n" +
                            "            Frame forward scan on: t\n"
            );
            setFeatures(true, true);
            assertPlan(
                    "select sum(x) from t where sym = 'S1' and y > 10",
                    "Index Or Scan\n" +
                            "  maxSelectivity: 0.05\n" +
                            "  selected: 0\n" +
                            "  scanned: 1000\n" +
                            "  opened: index\n" +
                            "    Async Index Group By workers: 1\n" +
                            "      vectorized: false\n" +
                            "      values: [sum(x)]\n" +
                            "      filter: 10<y\n" +
                            "        FilterOnValues symbolOrder: desc\n" +
                            "            Cursor-order scan\n" +
                            "                Index forward scan on: sym deferred: true\n" +
                            "                  symbolFilter: sym='S1'\n" +
                            "                  filter: 10<y\n" +
                            "            Frame forward scan on: t\n" +
                            "    Async JIT Group By workers: 1\n" +
                            "      vectorized: false\n" +
                            "      values: [sum(x)]\n" +
                            "      filter: (sym='S1' and 10<y)\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n"
            );
            // the hints take one plan
            assertPlan(
                    "select /*+ no_index */ sum(x) from t where sym = 'S1'",
                    "Async JIT Group By workers: 1\n" +
                            "  vectorized: false\n" +
                            "  values: [sum(x)]\n" +
                            "  filter: sym='S1'\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n"
            );
            // a third of the rows: the open takes the scan, and EXPLAIN says so
            final StringSink sink = new StringSink();
            printSql("explain select sum(x) from t where sym = 'S0'", sink);
            TestUtils.assertContains(sink, "  opened: scan\n");
            sink.clear();
            printSql("explain select /*+ use_index */ sum(x) from t where sym = 'S1'", sink);
            TestUtils.assertContains(sink, "Async Index Group By");
            Assert.assertFalse(sink.toString().contains("Index Or Scan"));
        });
    }

    @Test
    public void testFilterExpressionReleasedUnderSort() throws Exception {
        // twap() puts a sort between the GROUP BY and the index scan, so the GROUP BY does not
        // gather; the gather still drops the filter expression, whose nodes the compiler reuses
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "t", 1000, 53);
            for (boolean choice : new boolean[]{false, true}) {
                setFeatures(true, choice);
                for (String sql : new String[]{
                        "select /*+ use_index */ twap(x, ts) tw from t where sym = 'S0' and y > 10",
                        "select sym, sum(x) s from t where sym in ('S0', 'S1') and y > 10 order by sym",
                }) {
                    try (RecordCursorFactory factory = select(sql)) {
                        final IndexGatherRecordCursorFactory gather = findGather(factory);
                        Assert.assertNotNull(sql, gather);
                        Assert.assertNull(sql, gather.getStealFilterExpr());
                    }
                }
            }
        });
    }

    @Test
    public void testGroupedByKeyAndMoreShards() throws Exception {
        // A grouping of the key column alone never shards: a group per key of the list. A grouping
        // of the key column and more shards past ten times the usual threshold, 3,000 groups here:
        // (sym, x) has a group per row; (sym, 5 minutes), 2,304 groups, would shard at the usual
        // threshold but not at ten times it.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                createTable(engine, ctx, "t", 200_000, 59);
                final String[] queries = {
                        "select sym, x, count() n, sum(y) s from t where sym in ('S0', 'S1') order by sym, x",
                        "select sym, timestamp_floor('5m', ts) m, count() n, sum(y) s from t where sym in ('S0', 'S1') order by sym, m",
                        "select sym, count() n, sum(y) s from t where sym in ('S0', 'S1', 'S2', 'R1') order by sym",
                };
                final boolean[] sharded = {true, false, false};
                for (int q = 0; q < queries.length; q++) {
                    final String sql = queries[q];
                    setFeatures(false, false);
                    final String expected = runToString(engine, ctx, sql);
                    setFeatures(true, false);
                    try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
                        final AsyncGroupByRecordCursorFactory async = findAsyncGather(factory);
                        Assert.assertNotNull(sql, async);
                        final StringSink sink = new StringSink();
                        try (RecordCursor cursor = factory.getCursor(ctx)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(sql, sharded[q], async.getAtom().isSharded());
                            cursor.toTop();
                            io.questdb.cairo.CursorPrinter.println(cursor, factory.getMetadata(), sink);
                        }
                        assertWithinTolerance(sql, expected, sink.toString());
                    }
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testGroupedByKeyOnlyNeverShards() throws Exception {
        // a threshold of one group: any other grouping would shard, the key column alone does not
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                createTable(engine, ctx, "t", 200_000, 61);
                // 30 keys: past ten times the threshold, which a grouping with the key column takes
                final StringSink list = new StringSink();
                for (int i = 0; i < 30; i++) {
                    list.put(i == 0 ? "" : ", ").put('\'').put(i < 10 ? "S" : "R").put(i < 10 ? i : i - 10).put('\'');
                }
                final String[] queries = {
                        "select sym, count() n, sum(y) s from t where sym in (" + list + ") order by sym",
                        "select sym, timestamp_floor('1d', ts) d, count() n, sum(y) s from t where sym in (" + list + ") order by sym, d",
                };
                final boolean[] sharded = {false, true};
                for (int q = 0; q < queries.length; q++) {
                    final String sql = queries[q];
                    setFeatures(false, false);
                    final String expected = runToString(engine, ctx, sql);
                    setFeatures(true, false);
                    try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
                        final AsyncGroupByRecordCursorFactory async = findAsyncGather(factory);
                        Assert.assertNotNull(sql, async);
                        final StringSink sink = new StringSink();
                        try (RecordCursor cursor = factory.getCursor(ctx)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(sql, sharded[q], async.getAtom().isSharded());
                            cursor.toTop();
                            io.questdb.cairo.CursorPrinter.println(cursor, factory.getMetadata(), sink);
                        }
                        assertWithinTolerance(sql, expected, sink.toString());
                    }
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testIntervalFilters() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "t", ROWS, 13);
            final String[] filters = {
                    "ts in '1970-01-02'",
                    "ts between '1970-01-01T13:00' and '1970-01-03T07:30'",
                    "ts > '1970-01-02T11:11:11' and ts < '1970-01-02T11:12:00'",
                    "ts < '1970-01-01'",
            };
            for (String interval : filters) {
                assertSameAsSerial("select sym, " + AGGREGATES + " from t where sym in ('S0', 'S4', 'R5', 'P4') and " + interval + " order by sym", true);
                assertSameAsSerial("select " + AGGREGATES + " from t where sym = 'S2' and " + interval, true);
            }
        });
    }

    @Test
    public void testKeyedByOtherColumns() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "t", ROWS, 17);
            // groups within one key: the same order of rows as the serial plan
            assertSameAsSerial("select sym, timestamp_floor('1h', ts) h, " + AGGREGATES + " from t where sym in ('S0', 'S5', 'R8') order by sym, h", true);
            assertSameAsSerial("select sym, s2, " + AGGREGATES + " from t where sym in ('S0', 'S5', 'R8', null) order by sym, s2", true);
            // groups across keys
            assertSameAsSerial("select s2, " + AGGREGATES + " from t where sym in ('S0', 'S5', 'R8') order by s2", false);
            assertSameAsSerial("select y % 7 k, count() n, sum(z) sz, min(x) mn, max(x) mx from t where sym in ('S1', 'S2', 'S3') order by k", true);
            // a single key, grouped by another column, is in table order
            assertSameAsSerial("select timestamp_floor('10m', ts) m, s2, avg(x) a, sum(y) s from t where sym = 'S0' order by m, s2", true);
        });
    }

    @Test
    public void testKeyedBySymbol() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "t", ROWS, 19);
            assertSameAsSerial("select sym, " + AGGREGATES + " from t where sym in ('S0', 'S1', 'S2', 'R0', 'R1', 'R2', 'P1', null) order by sym", true);
            assertSameAsSerial("select sym, " + AGGREGATES + " from t where sym = 'S0' order by sym", true);
            assertSameAsSerial("select sym, " + AGGREGATES + " from t where sym in ('R10', 'R11', 'R12', 'R13', 'R14', 'R15', 'R16', 'R17', 'R18', 'R19', 'R20', 'R21') order by sym", true);
            // order-sensitive aggregates, every group within one key
            assertSameAsSerial("select sym, first(x) fx, last(x) lx, first(ts) ft, last(y) ly from t where sym in ('S0', 'S6', 'R3', null) order by sym", true);
            assertSameAsSerial("select sym, timestamp_floor('5m', ts) m, first(x) o, max(x) h, min(x) l, last(x) c from t where sym in ('S1', 'R4') order by sym, m", true);
        });
    }

    @Test
    public void testLimit() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "t", ROWS, 23);
            assertSameAsSerial("select sym, " + AGGREGATES + " from t where sym in ('S0', 'S1', 'S2', 'S3', 'R1') order by sym limit 2", true);
            assertSameAsSerial("select sym, " + AGGREGATES + " from t where sym in ('S0', 'S1', 'S2', 'S3', 'R1') order by sym desc limit 3", true);
            assertSameAsSerial("select sym, sum(x) s from t where sym in ('S0', 'S1', 'S2', 'S3', 'R1') order by s limit -2", true);
        });
    }

    @Test
    public void testMemoryLimitFallsBackToOneMap() throws Exception {
        // a query whose per-worker maps break the limit while one map fits completes with the
        // same rows on the query's thread
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                engine.execute("create table m (sym symbol index type posting, k varchar, v long, ts timestamp) timestamp(ts) partition by day bypass wal", ctx);
                engine.execute("insert into m select rnd_symbol('A', 'B', 'C', 'D'), (x % 40_000)::varchar, x, timestamp_sequence(0, 1_000_000L) from long_sequence(200_000)", ctx);
                final String sql = "select k, count() n, sum(v) s from m where sym in ('A', 'B', 'C') order by k";
                setFeatures(false, false);
                final String expected = runToString(engine, ctx, sql);
                boolean gatherRetried = false;
                boolean choiceFellBack = false;
                for (long limit = 2L << 20; limit <= 48L << 20; limit += 1L << 20) {
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit);
                    setFeatures(false, false);
                    final String serial;
                    try {
                        serial = runToString(engine, ctx, sql);
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isOutOfMemory());
                        continue;
                    }
                    TestUtils.assertEquals(expected, serial);
                    for (boolean choice : new boolean[]{false, true}) {
                        setFeatures(true, choice);
                        try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
                            TestUtils.assertEquals("limit " + limit, expected, print(factory, ctx));
                            if (choice) {
                                final IndexScanChoiceRecordCursorFactory chooser = findChoice(factory);
                                Assert.assertNotNull(chooser);
                                choiceFellBack |= chooser.getMemoryLimitFallbacks() > 0;
                            } else {
                                final AsyncGroupByRecordCursorFactory async = findAsyncGather(factory);
                                Assert.assertNotNull(async);
                                gatherRetried |= async.getSerialRetries() > 0;
                            }
                        }
                    }
                }
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                // the limits must have reached both fallbacks: the scan plan's, which opens the
                // index plan, and the index gather's own, which runs its tasks on one map
                Assert.assertTrue("no limit made the index gather retry serially", gatherRetried);
                Assert.assertTrue("no limit made the scan plan fall back", choiceFellBack);
            }, configuration, LOG);
        });
    }

    @Test
    public void testNotKeyed() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "t", ROWS, 29);
            assertSameAsSerial("select " + AGGREGATES + " from t where sym in ('S0', 'S1', 'R2', 'P5', null)", false);
            assertSameAsSerial("select " + AGGREGATES + " from t where sym = 'S3'", true);
            assertSameAsSerial("select " + AGGREGATES + " from t where sym = 'R3'", true);
            // a single key is in table order, also for order-sensitive aggregates
            assertSameAsSerial("select first(x) fx, last(x) lx, first(ts) ft from t where sym = 'S2'", true);
        });
    }

    @Test
    public void testOrderSensitiveAcrossKeysStaysSerial() throws Exception {
        // first() of a group that spans keys depends on the table order across keys, which only
        // the serial index scan returns
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "t", ROWS, 31);
            final String[] queries = {
                    "select first(x) fx, last(y) ly from t where sym in ('S1', 'S2')",
                    "select s2, first(x) fx, last(ts) lt from t where sym in ('S1', 'R2') order by s2",
            };
            for (String sql : queries) {
                setFeatures(true, false);
                final StringSink plan = new StringSink();
                printSql("explain " + sql, plan);
                Assert.assertFalse(sql + "\n" + plan, plan.toString().contains("Async Index Group By"));
                assertSameAsSerial(sql, true, false);
            }
        });
    }

    @Test
    public void testOrderSensitiveConcurrentQueries() throws Exception {
        // queries of several threads share one pool: a thread that steals another query's task
        // takes any free slot, so slots do not see their tasks in order
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_INDEX_GROUPBY_FRAME_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                createSplitKeyTable(engine, ctx);
                final String[] queries = ORDER_SENSITIVE_QUERIES;
                final String[] expected = new String[queries.length];
                setFeatures(false, false);
                for (int q = 0; q < queries.length; q++) {
                    expected[q] = runToString(engine, ctx, queries[q]);
                }
                setFeatures(true, false);
                final int threads = 4;
                final int iterations = 8;
                final AtomicInteger runs = new AtomicInteger();
                final AtomicReference<Throwable> error = new AtomicReference<>();
                final CyclicBarrier barrier = new CyclicBarrier(threads);
                final Thread[] ts = new Thread[threads];
                for (int t = 0; t < threads; t++) {
                    final int offset = t;
                    ts[t] = new Thread(() -> {
                        try (SqlExecutionContextImpl c = TestUtils.createSqlExecutionCtx(engine, 4)) {
                            barrier.await();
                            for (int i = 0; i < iterations; i++) {
                                for (int q = 0; q < queries.length; q++) {
                                    final int query = (q + offset) % queries.length;
                                    TestUtils.assertEquals(queries[query], expected[query], runToString(engine, c, queries[query]));
                                    runs.incrementAndGet();
                                }
                            }
                        } catch (Throwable th) {
                            error.compareAndSet(null, th);
                        }
                    });
                    ts[t].start();
                }
                for (Thread t : ts) {
                    t.join();
                }
                if (error.get() != null) {
                    throw new AssertionError(error.get());
                }
                Assert.assertEquals(threads * iterations * queries.length, runs.get());
            }, configuration, LOG);
        });
    }

    @Test
    public void testOrderSensitiveOnWorkerPool() throws Exception {
        // One partition in one frame, and tasks of 300 rows: a frequent key holds about 100 tasks'
        // worth of rows of the same frame. Aggregates that depend on the order rows arrive in
        // (array_agg) or on which of equal rows comes first (mode, arg_max, arg_min) must see each
        // key's rows of a frame on one slot, in order, so the key is not split by row range.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_INDEX_GROUPBY_FRAME_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                createSplitKeyTable(engine, ctx);
                for (String sql : ORDER_SENSITIVE_QUERIES) {
                    setFeatures(false, false);
                    final String expected = runToString(engine, ctx, sql);
                    setFeatures(true, false);
                    for (int run = 0; run < 10; run++) {
                        try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
                            final IndexGatherRecordCursorFactory gather = findGather(factory);
                            Assert.assertNotNull(sql, gather);
                            TestUtils.assertEquals(sql + ", run " + run, expected, print(factory, ctx));
                            // one task per key of the partition
                            final int keys = sql.contains("sym = 'A'") ? 1 : 2;
                            Assert.assertEquals(sql, keys, gather.getPageFrameCursorForTesting().getTaskCounts()[0]);
                        }
                    }
                    // The choice: these lists select most rows, so it opens the scan, which splits
                    // the partition into page frames on several slots. Its ties (mode, arg_max,
                    // arg_min) then depend on the merge order, as they always have.
                    if (!sql.contains("mode(") && !sql.contains("arg_")) {
                        setFeatures(true, true);
                        TestUtils.assertEquals(sql, expected, runToString(engine, ctx, sql));
                    }
                }
                // aggregates whose index plan does not gather: equal to the serial plan too
                for (String sql : new String[]{
                        "select /*+ use_index */ twap(x, ts) tw from t where sym = 'A'",
                        "select /*+ use_index */ sym, twap(x, ts) tw from t where sym in ('A', 'B') order by sym",
                        "select /*+ use_index */ sym, sparkline(y) sp from t where sym in ('A', 'B') and x < 3000 order by sym",
                }) {
                    setFeatures(false, false);
                    final String expected = runToString(engine, ctx, sql);
                    setFeatures(true, true);
                    for (int run = 0; run < 3; run++) {
                        TestUtils.assertEquals(sql, expected, runToString(engine, ctx, sql));
                    }
                }
                // an order-insensitive aggregate still splits the frequent key by row range
                final String sql = "select sym, count() n, sum(x) s, min(y) mn from t where sym in ('A', 'B') order by sym";
                setFeatures(false, false);
                final String expected = runToString(engine, ctx, sql);
                setFeatures(true, false);
                try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
                    TestUtils.assertEquals(sql, expected, print(factory, ctx));
                    Assert.assertTrue(findGather(factory).getPageFrameCursorForTesting().getTaskCounts()[0] > 50);
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testParquetPartitions() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "t", ROWS, 37);
            execute("alter table t convert partition to parquet where ts < '1970-01-02'");
            assertSameAsSerial("select sym, " + AGGREGATES + " from t where sym in ('S0', 'S7', 'R6', 'P0', null) order by sym", true);
            assertSameAsSerial("select sym, " + AGGREGATES + " from t where sym in ('S0', 'S7') and y > 300 order by sym", true);
            assertSameAsSerial("select " + AGGREGATES + " from t where sym = 'S7'", true);
            final String sql = "select sym, count() n from t where sym in ('S0', 'S7') order by sym";
            setFeatures(true, false);
            try (RecordCursorFactory factory = select(sql)) {
                print(factory);
                final long[] counts = findGather(factory).getPageFrameCursorForTesting().getTaskCounts();
                Assert.assertTrue(counts[0] > 0);
                Assert.assertTrue(counts[1] > 0);
            }
        });
    }

    @Test
    public void testPlanChoiceFollowsSelectivity() throws Exception {
        // one cached factory takes the index for a list that selects few rows and the scan for one
        // that selects most of them, as the bind variables change
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "t", ROWS, 41);
            setFeatures(true, true);
            final String sql = "select sym, count() n, sum(x) s from t where sym in ($1, $2) order by sym";
            bindVariableService.clear();
            bindVariableService.setStr(0, "R1");
            bindVariableService.setStr(1, "R2");
            try (RecordCursorFactory factory = select(sql)) {
                final IndexScanChoiceRecordCursorFactory chooser = findChoice(factory);
                Assert.assertNotNull(chooser);
                final String[][] lists = {{"R1", "R2"}, {"S0", "S1"}, {"R3", null}, {"S0", "S2"}, {"NOPE", "NADA"}};
                final boolean[] index = {true, false, true, false, true};
                for (int i = 0; i < lists.length; i++) {
                    bindVariableService.setStr(0, lists[i][0]);
                    bindVariableService.setStr(1, lists[i][1]);
                    final long indexOpens = chooser.getIndexPlanOpens();
                    final String actual = print(factory);
                    Assert.assertEquals(lists[i][0] + "," + lists[i][1] + " selected " + chooser.getLastSelected() + " of " + chooser.getLastScanned(),
                            index[i], chooser.getIndexPlanOpens() > indexOpens);
                    setFeatures(false, false);
                    TestUtils.assertEquals(sql, runToString(engine, sqlExecutionContext, sql), actual);
                    setFeatures(true, true);
                }
                Assert.assertTrue(chooser.getScanPlanOpens() > 0);
            }
        });
    }

    @Test
    public void testScanPlanKeepsSubQueryPredicates() throws Exception {
        // The choice generates the GROUP BY twice, the second time as a scan. A sub-query in the
        // residual filter is a model that the first generation consumed the where clause of: the
        // scan plan must still read the sub-query's key and interval, and so return the rows of
        // no_index for a frequent key, which the choice opens as the scan.
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "t", ROWS, 47);
            createTable(engine, sqlExecutionContext, "r", ROWS, 53);
            setFeatures(true, true);
            final String[] queries = {
                    "select count() n, sum(x) s from t where sym = 'S0' and x > (select avg(x) from r where sym = 'R1')",
                    "select count() n, sum(x) s from t where sym = 'S0' and y > (select max(y) - 100 from r where sym = 'S2' and ts in '1970-01-02')",
                    "select sym, count() n, sum(x) s from t where sym in ('S0', 'S1') and x > (select avg(x) from r where sym = 'R1' and ts in '1970-01-03') order by sym",
                    "select sym, count() n from t where sym in ('S0', 'S1') and y < (select min(y) + 300 from r where sym = 'S0' and ts in '1970-01-01' and x > (select avg(x) from t where sym = 'R2')) order by sym",
            };
            for (String sql : queries) {
                try (RecordCursorFactory factory = select(sql)) {
                    final IndexScanChoiceRecordCursorFactory chooser = findChoice(factory);
                    Assert.assertNotNull(sql, chooser);
                    final long scanOpens = chooser.getScanPlanOpens();
                    final String actual = print(factory, sqlExecutionContext);
                    Assert.assertTrue(sql, chooser.getScanPlanOpens() > scanOpens);
                    final String reference = runToString(engine, sqlExecutionContext, sql.replaceFirst("select ", "select /*+ no_index */ "));
                    TestUtils.assertEquals(sql, reference, actual);
                    // the gather alone compiles the rest of the filter a second time, for its tasks
                    setFeatures(true, false);
                    final StringSink plan = new StringSink();
                    printSql("explain " + sql, plan);
                    Assert.assertTrue(sql + '\n' + plan, plan.toString().contains("Async Index Group By"));
                    TestUtils.assertEquals(sql, reference, runToString(engine, sqlExecutionContext, sql));
                    setFeatures(true, true);
                }
            }
        });
    }

    @Test
    public void testScanPlanKeepsSubQueryLatestOn() throws Exception {
        // The choice generates the GROUP BY twice, the second time as a scan, and the gather alone
        // compiles the rest of its filter twice, the second time for its tasks. A LATEST ON
        // sub-query in that filter is a model whose LATEST BY the first generation or compile
        // consumed: the second must still read one row per partition of the sub-query. The
        // expected results are literal: no_index is no reference for plans generated twice.
        assertMemoryLeak(() -> {
            execute("create table g (ts timestamp, sym symbol index type posting, s2 symbol, v long) timestamp(ts) partition by day");
            // F is a fifth of the rows, G nearly as many, R one in 250; 100 symbols; the latest R
            // is row 29,751, the latest R of the first day row 8,501
            execute("insert into g select (x * 10_000_000L)::timestamp,"
                    + " case when x % 5 = 0 then 'F' when x % 250 = 1 then 'R' when x % 5 = 1 then 'G' else 'k' || (x % 97) end,"
                    + " 's' || (x % 3), x from long_sequence(30_000)");
            final String[][] cases = {
                    {
                            "select sym, count() n, sum(v) s from g where sym in ('F', 'G') and v < (select v from g where sym = 'R' latest on ts partition by sym) order by sym",
                            "sym\tn\ts\nF\t5950\t88521125\nG\t5831\t86741956\n"
                    },
                    {
                            "select sym, count() n, sum(v) s from g where sym in ('F', 'G') and v < (select count() from (select * from g latest on ts partition by sym)) order by sym",
                            "sym\tn\ts\nF\t19\t950\nG\t19\t969\n"
                    },
                    {
                            "select sym, count() n, sum(v) s from g where sym in ('F', 'G') and s2 in (select s2 from g where sym = 'R' latest on ts partition by sym) order by sym",
                            "sym\tn\ts\nF\t2000\t30015000\nG\t1960\t29391960\n"
                    },
                    {
                            "select sym, count() n, sum(v) s from g where sym in ('F', 'G') and v < (select v from g where sym = 'R' and ts in '1970-01-01' latest on ts partition by sym) order by sym",
                            "sym\tn\ts\nF\t1700\t7229250\nG\t1666\t7082166\n"
                    },
            };
            for (String[] c : cases) {
                final String sql = c[0];
                setFeatures(true, true);
                try (RecordCursorFactory factory = select(sql)) {
                    final IndexScanChoiceRecordCursorFactory chooser = findChoice(factory);
                    Assert.assertNotNull(sql, chooser);
                    final long scanOpens = chooser.getScanPlanOpens();
                    final String actual = print(factory, sqlExecutionContext);
                    Assert.assertTrue(sql, chooser.getScanPlanOpens() > scanOpens);
                    TestUtils.assertEquals(sql, c[1], actual);
                }
                // the gather alone
                setFeatures(true, false);
                final StringSink plan = new StringSink();
                printSql("explain " + sql, plan);
                Assert.assertTrue(sql + '\n' + plan, plan.toString().contains("Async Index Group By"));
                TestUtils.assertEquals(sql, c[1], runToString(engine, sqlExecutionContext, sql));
                // and the scan
                setFeatures(false, false);
                TestUtils.assertEquals(sql, c[1], runToString(engine, sqlExecutionContext, sql));
            }
        });
    }

    @Test
    public void testResidualFilters() throws Exception {
        assertMemoryLeak(() -> {
            createTable(engine, sqlExecutionContext, "t", ROWS, 43);
            assertSameAsSerial("select sym, " + AGGREGATES + " from t where sym in ('S0', 'S1', 'R0') and s2 is not null and s2 != '' order by sym", true);
            assertSameAsSerial("select sym, " + AGGREGATES + " from t where sym in ('S0', 'S1', 'R0') and y > 500 and x < 0.7 order by sym", true);
            assertSameAsSerial("select " + AGGREGATES + " from t where sym = 'S0' and (y % 3 = 0 or s2 = 'a')", true);
            assertSameAsSerial("select sym, timestamp_floor('5m', ts) m, first(x) o, last(x) c, sum(z) s from t where sym in ('S1', 'R2') and s2 is not null and s2 != '' order by sym, m", true);
        });
    }

    @Test
    public void testWorkerPool() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                createTable(engine, ctx, "t", 200_000, 47);
                final String[] queries = {
                        "select sym, " + AGGREGATES + " from t where sym in ('S0', 'S1', 'S2', 'R0', 'R1', 'P1', null) order by sym",
                        "select sym, timestamp_floor('1h', ts) h, " + AGGREGATES + " from t where sym in ('S0', 'S5', 'R8') order by sym, h",
                        "select " + AGGREGATES + " from t where sym in ('S0', 'S1', 'R2')",
                        "select sym, first(x) fx, last(x) lx from t where sym in ('S0', 'S3', 'R3') order by sym",
                        "select s2, " + AGGREGATES + " from t where sym in ('S0', 'S5', 'R8') and y > 100 order by s2",
                        // many groups: the maps shard
                        "select z % 20_000 k, count() n, sum(x) s from t where sym in ('S0', 'S1') order by k",
                };
                long workerTasks = 0;
                for (String sql : queries) {
                    setFeatures(false, false);
                    final String expected = runToString(engine, ctx, sql);
                    setFeatures(true, false);
                    for (int run = 0; run < 3; run++) {
                        try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
                            final IndexGatherRecordCursorFactory gather = findGather(factory);
                            Assert.assertNotNull(sql, gather);
                            assertWithinTolerance(sql, expected, print(factory, ctx));
                            workerTasks += gather.getPageFrameCursorForTesting().getTaskCounts()[2];
                        }
                    }
                    setFeatures(true, true);
                    assertWithinTolerance(sql, expected, runToString(engine, ctx, sql));
                }
                Assert.assertTrue("no worker took a task", workerTasks > 0);
            }, configuration, LOG);
        });
    }

    private static void assertWithinTolerance(String sql, String expected, String actual) {
        final String[] e = expected.split("\n");
        final String[] a = actual.split("\n");
        Assert.assertEquals(sql + "\n" + expected + "\n" + actual, e.length, a.length);
        for (int r = 0; r < e.length; r++) {
            final String[] ec = e[r].split("\t", -1);
            final String[] ac = a[r].split("\t", -1);
            Assert.assertEquals(sql, ec.length, ac.length);
            for (int c = 0; c < ec.length; c++) {
                if (ec[c].equals(ac[c])) {
                    continue;
                }
                final double x;
                final double y;
                try {
                    x = Double.parseDouble(ec[c]);
                    y = Double.parseDouble(ac[c]);
                } catch (NumberFormatException ex) {
                    Assert.fail(sql + ": row " + r + " col " + c + ": " + ec[c] + " vs " + ac[c]);
                    return;
                }
                final double diff = Math.abs(x - y);
                Assert.assertTrue(
                        sql + ": row " + r + " col " + c + ": " + ec[c] + " vs " + ac[c],
                        diff <= 1e-9 || diff <= 1e-9 * Math.max(Math.abs(x), Math.abs(y))
                );
            }
        }
    }

    // one partition: 30K rows, 9 in 10 of them A, x the row number and y = x % 3
    private static void createSplitKeyTable(CairoEngine engine, SqlExecutionContext ctx) throws SqlException {
        engine.execute("create table t (sym symbol index type posting, x double, y int, ts timestamp) timestamp(ts) partition by day bypass wal", ctx);
        engine.execute(
                "insert into t select case when x % 10 = 0 then 'B' else 'A' end, x::double, (x % 3)::int, (x * 100_000L)::timestamp from long_sequence(30_000)",
                ctx
        );
    }

    private static void createTable(CairoEngine engine, SqlExecutionContext ctx, String name, int rows, long seed) throws SqlException {
        engine.execute(
                "create table " + name + " (sym symbol index type posting, x double, y int, z long, f float, s2 symbol, ts timestamp)" +
                        " timestamp(ts) partition by day bypass wal",
                ctx
        );
        // 4 days; P<n> only on day n % 4; NULL every 23rd row
        engine.execute(
                "insert into " + name + " select " +
                        "case when x % 23 = 0 then null " +
                        "when x % 3 = 0 then 'S0' " +
                        "when x % 3 = 1 and x % 5 != 0 then 'S' || (1 + (x * " + seed + ") % 9) " +
                        "when x % 7 = 0 then 'P' || (((x * 4) / " + rows + ") * 3 + x % 3) " +
                        "else 'R' || ((x * " + (seed + 2) + ") % 50) end, " +
                        "case when x % 11 = 0 then null else rnd_double() end, " +
                        "rnd_int(0, 1000, 7), rnd_long(-1000, 1000, 9), rnd_float(), rnd_symbol('a', 'b', '', null), " +
                        "(x * (4 * 86_400_000_000L / " + rows + "))::timestamp " +
                        "from long_sequence(" + rows + ")",
                ctx
        );
    }

    private static class FakePlan extends io.questdb.cairo.AbstractRecordCursorFactory {
        private final boolean[] closed;
        private final boolean failsOnMemory;

        FakePlan(io.questdb.cairo.sql.RecordMetadata metadata, boolean[] closed, boolean failsOnMemory) {
            super(metadata);
            this.closed = closed;
            this.failsOnMemory = failsOnMemory;
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            final boolean[] closed = this.closed != null ? this.closed : new boolean[1];
            closed[0] = false;
            return new io.questdb.cairo.sql.NoRandomAccessRecordCursor() {
                private boolean done;

                @Override
                public void close() {
                    closed[0] = true;
                }

                @Override
                public Record getRecord() {
                    return null;
                }

                @Override
                public SymbolTable getSymbolTable(int columnIndex) {
                    return new FakeSymbolTable(closed);
                }

                @Override
                public boolean hasNext() {
                    if (failsOnMemory) {
                        throw CairoException.nonCritical().put("query memory limit exceeded").setOutOfMemory(true);
                    }
                    final boolean hasNext = !done;
                    done = true;
                    return hasNext;
                }

                @Override
                public SymbolTable newSymbolTable(int columnIndex) {
                    return new FakeSymbolTable(closed);
                }

                @Override
                public long preComputedStateSize() {
                    return 0;
                }

                @Override
                public long size() {
                    if (failsOnMemory) {
                        throw CairoException.nonCritical().put("query memory limit exceeded").setOutOfMemory(true);
                    }
                    return -1;
                }

                @Override
                public void toTop() {
                    done = false;
                }
            };
        }

        @Override
        public int getScanDirection() {
            return SCAN_DIRECTION_OTHER;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(io.questdb.griffin.PlanSink sink) {
            sink.type("fake");
        }
    }

    // a static symbol table v0..v9 that fails once its cursor has closed, as a closed reader's would
    private static class FakeSymbolTable implements StaticSymbolTable {
        static final int COUNT = 10;
        private final boolean[] closed;

        FakeSymbolTable(boolean[] closed) {
            this.closed = closed;
        }

        @Override
        public boolean containsNullValue() {
            check();
            return false;
        }

        @Override
        public int getSymbolCount() {
            check();
            return COUNT;
        }

        @Override
        public int keyOf(CharSequence value) {
            check();
            return Integer.parseInt(value.subSequence(1, value.length()).toString());
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            check();
            return "v" + key;
        }

        private void check() {
            if (closed[0]) {
                throw new AssertionError("symbol table read after its cursor closed");
            }
        }
    }

    private static AsyncGroupByRecordCursorFactory findAsyncGather(RecordCursorFactory factory) {
        RecordCursorFactory f = factory;
        for (int i = 0; f != null && i < 6; i++) {
            if (f instanceof AsyncGroupByRecordCursorFactory async && async.getBaseFactory() instanceof IndexGatherRecordCursorFactory) {
                return async;
            }
            f = f instanceof IndexScanChoiceRecordCursorFactory choice ? choice.getIndexPlan() : f.getBaseFactory();
        }
        return null;
    }

    private static IndexScanChoiceRecordCursorFactory findChoice(RecordCursorFactory factory) {
        RecordCursorFactory f = factory;
        for (int i = 0; f != null && i < 6; i++) {
            if (f instanceof IndexScanChoiceRecordCursorFactory choice) {
                return choice;
            }
            f = f.getBaseFactory();
        }
        return null;
    }

    private static IndexGatherRecordCursorFactory findGather(RecordCursorFactory factory) {
        RecordCursorFactory f = factory;
        for (int i = 0; f != null && i < 6; i++) {
            if (f instanceof IndexGatherRecordCursorFactory gather) {
                return gather;
            }
            if (f instanceof IndexScanChoiceRecordCursorFactory choice) {
                f = choice.getIndexPlan();
                continue;
            }
            if ((f instanceof AsyncGroupByRecordCursorFactory || f instanceof AsyncGroupByNotKeyedRecordCursorFactory)
                    && f.getBaseFactory() instanceof IndexGatherRecordCursorFactory gather) {
                return gather;
            }
            f = f.getBaseFactory();
        }
        return null;
    }

    private static String print(RecordCursorFactory factory, SqlExecutionContext ctx) throws SqlException {
        final StringSink sink = new StringSink();
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            io.questdb.cairo.sql.RecordMetadata metadata = factory.getMetadata();
            io.questdb.cairo.CursorPrinter.println(cursor, metadata, sink);
        }
        return sink.toString();
    }

    private static String runToString(CairoEngine engine, SqlExecutionContext ctx, String sql) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
            return print(factory, ctx);
        }
    }

    private static void setFeatures(boolean gather, boolean choice) {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_INDEX_GROUPBY_ENABLED, String.valueOf(gather));
        setProperty(PropertyKey.CAIRO_SQL_INDEX_SCAN_CHOICE_ENABLED, String.valueOf(choice));
    }

    private void assertSameAsSerial(String sql, boolean exact) throws Exception {
        assertSameAsSerial(sql, exact, true);
    }

    /**
     * Runs the query serially (both features off), with the index gather, and with the choice,
     * and compares; with {@code expectGather}, the gather plan must be the one compiled.
     */
    private void assertSameAsSerial(String sql, boolean exact, boolean expectGather) throws Exception {
        setFeatures(false, false);
        final String expected = runToString(engine, sqlExecutionContext, sql);
        final StringSink plan = new StringSink();
        printSql("explain " + sql, plan);
        Assert.assertFalse(sql, plan.toString().contains("Async Index Group By"));
        Assert.assertFalse(sql, plan.toString().contains("Index Or Scan"));

        setFeatures(true, false);
        plan.clear();
        printSql("explain " + sql, plan);
        Assert.assertEquals(sql + "\n" + plan, expectGather, plan.toString().contains("Async Index Group By"));
        final String gathered = runToString(engine, sqlExecutionContext, sql);
        if (exact) {
            TestUtils.assertEquals(sql, expected, gathered);
        } else {
            assertWithinTolerance(sql, expected, gathered);
        }

        // the choice: whichever plan it opens returns the same rows
        setFeatures(true, true);
        final String chosen = runToString(engine, sqlExecutionContext, sql);
        assertWithinTolerance(sql, expected, chosen);
        // the choice without the gather picks between the serial index plan and the scan
        setFeatures(false, true);
        assertWithinTolerance(sql, expected, runToString(engine, sqlExecutionContext, sql));
    }

    private void assertPlan(String sql, String expected) throws Exception {
        final StringSink sink = new StringSink();
        printSql("explain " + sql, sink);
        TestUtils.assertEquals(sql, "QUERY PLAN\n" + expected, sink);
    }

    private String print(RecordCursorFactory factory) throws SqlException {
        return print(factory, sqlExecutionContext);
    }
}
