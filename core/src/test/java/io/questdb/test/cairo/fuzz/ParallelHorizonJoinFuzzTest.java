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

package io.questdb.test.cairo.fuzz;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AsyncHorizonJoinProjectionRecordCursorFactory;
import io.questdb.griffin.engine.table.HorizonJoinProjectionRecordCursorFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

// This is not a fuzz test in traditional sense, but it's multithreaded, and we want to run it
// in CI frequently along with other fuzz tests.
public class ParallelHorizonJoinFuzzTest extends AbstractCairoTest {
    private static final int MIN_PAGE_FRAME_MAX_ROWS = 100;
    private static final int PAGE_FRAME_COUNT = 4; // also used to set queue size, so must be a power of 2
    private static final int ROW_COUNT = 10 * PAGE_FRAME_COUNT * MIN_PAGE_FRAME_MAX_ROWS;
    private final boolean convertToParquet;
    private final boolean enableParallelHorizonJoin;
    private final Rnd rnd;

    public ParallelHorizonJoinFuzzTest() {
        this.rnd = TestUtils.generateRandom(LOG);
        this.enableParallelHorizonJoin = rnd.nextBoolean();
        this.convertToParquet = rnd.nextBoolean();
    }

    @Override
    @Before
    public void setUp() {
        final int pageFrameMaxRows = MIN_PAGE_FRAME_MAX_ROWS + rnd.nextInt(10);
        // Async horizon join uses small page frames.
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, pageFrameMaxRows);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, pageFrameMaxRows);
        // We intentionally use small values for shard count and reduce
        // queue capacity to exhibit various edge cases.
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 1 + rnd.nextInt(4));
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, PAGE_FRAME_COUNT);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 1 + rnd.nextInt(PAGE_FRAME_COUNT));
        // Randomize the sharding threshold so that both sharded and non-sharded paths are exercised.
        // Keyed tests produce ~500 group keys (100 symbols × 5 offsets), so a range of 1-1000
        // gives roughly equal chances for both paths.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1 + rnd.nextInt(1000));
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1 + rnd.nextInt(16));
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HORIZON_JOIN_ENABLED, String.valueOf(enableParallelHorizonJoin));
        // Randomize adaptive backward-to-forward scan switch thresholds.
        setProperty(PropertyKey.CAIRO_SQL_HORIZON_JOIN_BWD_SCAN_ABSOLUTE_THRESHOLD, 1 + rnd.nextLong(262_144));
        setProperty(PropertyKey.CAIRO_SQL_HORIZON_JOIN_BWD_SCAN_MIN_GAP, 1 + rnd.nextLong(2_048));
        setProperty(PropertyKey.CAIRO_SQL_HORIZON_JOIN_BWD_SCAN_SWITCH_FACTOR, 1 + rnd.nextLong(16));
        if (rnd.nextBoolean()) {
            // A HORIZON JOIN without aggregation matches at most page.frame.max.rows / (offsets *
            // slaves) rows of a frame per task; the owner thread matches the rest. Keep that
            // budget below the frame size half of the time.
            setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 8 + rnd.nextInt(64));
        }
        super.setUp();
    }

    @Test
    public void testParallelHorizonJoinAdaptiveScan() throws Exception {
        testParallelHorizonJoinAdaptiveScan(
                "LIST (-1s, 0s) AS h",
                new long[]{-1_000_000, 0}
        );
    }

    @Test
    public void testParallelHorizonJoinAdaptiveScan2() throws Exception {
        testParallelHorizonJoinAdaptiveScan(
                "RANGE FROM 0 TO 1s STEP 1s AS h",
                new long[]{0, 1_000_000}
        );
    }

    @Test
    public void testParallelHorizonJoinConstArrayKeyDoesNotLeak() throws Exception {
        // A thread-safe constant ARRAY group key alongside a non-thread-safe aggregate
        // (count_distinct) over a HORIZON JOIN makes the async horizon join create per-worker
        // copies. Each worker's copy of the thread-safe ArrayConstant key must be extracted and
        // freed; otherwise it leaks NATIVE_ND_ARRAY native memory, scaling with worker count.
        Assume.assumeTrue(enableParallelHorizonJoin);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(rnd));
            TestUtils.execute(
                    pool,
                    (engine, _, sqlExecutionContext) -> {
                        engine.execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, qty LONG) TIMESTAMP(ts) PARTITION BY DAY", sqlExecutionContext);
                        engine.execute("CREATE TABLE prices (ts TIMESTAMP, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY", sqlExecutionContext);
                        engine.execute("INSERT INTO trades SELECT (x * 3_600_000_000)::timestamp, ('s' || (x % 5))::symbol, x % 7 FROM long_sequence(" + ROW_COUNT + ")", sqlExecutionContext);
                        engine.execute("INSERT INTO prices SELECT (x * 3_600_000_000)::timestamp, x * 1.5 FROM long_sequence(" + ROW_COUNT + ")", sqlExecutionContext);
                        TestUtils.printSql(engine, sqlExecutionContext, "SELECT ARRAY[0.5] k, count_distinct(t.qty) c FROM trades t HORIZON JOIN prices p RANGE FROM 0s TO 0s STEP 1s AS h", sink);
                        TestUtils.assertEquals("k\tc\n[0.5]\t7\n", sink);
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testParallelHorizonJoinFiltered() throws Exception {
        testParallelHorizonJoin(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                true,
                "t.side = 'sell'"
        );
    }

    @Test
    public void testParallelHorizonJoinFilteredThreadUnsafe() throws Exception {
        testParallelHorizonJoin(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                true,
                "concat(t.side, '_00') = 'sell_00'"
        );
    }

    @Test
    public void testParallelHorizonJoinFilteredWithBindVariables() throws Exception {
        testParallelHorizonJoin(
                (sqlExecutionContext) -> {
                    BindVariableService bindVariableService = sqlExecutionContext.getBindVariableService();
                    bindVariableService.clear();
                    bindVariableService.setStr("side", "sell");
                },
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                true,
                "t.side = :side"
        );
    }

    @Test
    public void testParallelHorizonJoinLateMaterialization() throws Exception {
        // price ranges from 10 to 30; price > 28 passes ~10% of rows,
        // well below the 20% selectivity threshold for late materialization.
        testParallelHorizonJoin(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                true,
                "t.price > 28.0"
        );
    }

    @Test
    public void testParallelHorizonJoinLateMaterializationNotKeyed() throws Exception {
        testParallelHorizonJoin(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                false,
                "t.price > 28.0"
        );
    }

    @Test
    public void testParallelHorizonJoinList() throws Exception {
        testParallelHorizonJoin(
                "LIST (-2s, 0s, 2s) AS h",
                new long[]{-2_000_000, 0, 2_000_000},
                true,
                null
        );
    }

    @Test
    public void testParallelHorizonJoinListManyOffsets() throws Exception {
        testParallelHorizonJoin(
                "LIST (-10s, -7s, -5s, -3s, -1s, 0s, 1s, 3s, 5s, 7s, 10s) AS h",
                new long[]{-10_000_000, -7_000_000, -5_000_000, -3_000_000, -1_000_000, 0, 1_000_000, 3_000_000, 5_000_000, 7_000_000, 10_000_000},
                true,
                null
        );
    }

    @Test
    public void testParallelHorizonJoinNotKeyed() throws Exception {
        testParallelHorizonJoin(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                false,
                null
        );
    }

    @Test
    public void testParallelHorizonJoinNotKeyedManyOffsets() throws Exception {
        testParallelHorizonJoin(
                "RANGE FROM -5s TO 5s STEP 1s AS h",
                rangeOffsets(-5, 5),
                false,
                null
        );
    }

    @Test
    public void testParallelHorizonJoinProjection() throws Exception {
        testParallelHorizonJoinProjection(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                true,
                null
        );
    }

    @Test
    public void testParallelHorizonJoinProjectionBindVariableReuse() throws Exception {
        // A PGWire prepared statement runs one factory again with new bind values. Each execution
        // has to initialize again the bind variables of the master filter, the bind memory of its
        // JIT form and the worker clones of the right-hand filters, and must not carry over the
        // key misses of an execution whose right-hand filter matched nothing. Each execution of
        // the reused factory must return the rows of a fresh serial compile with the values
        // inlined.
        final String[] queries = {
                // Keyed: a right-hand filter that isn't thread-safe and a JIT master filter.
                "SELECT t.ts, t.sym, h.offset, p.bid FROM trades t"
                        + " HORIZON JOIN (prices WHERE concat(sym, '') = $1) p ON (sym)"
                        + " LIST (-1s, 0s, 1s) AS h WHERE t.price > $2",
                // Unkeyed: a Java master filter that isn't thread-safe.
                "SELECT t.ts, h.offset, p.sym, p.bid FROM trades t"
                        + " HORIZON JOIN (prices WHERE concat(sym, '') = $1) p"
                        + " LIST (-1s, 0s, 1s) AS h WHERE concat(t.side, '') = $3",
                // Two right-hand tables, keyed: bind variables in a sub-query and in an ON clause.
                "SELECT t.ts, t.sym, h.offset, p.bid, r.ask FROM trades t"
                        + " HORIZON JOIN (prices WHERE concat(sym, '') = $1) p ON (sym)"
                        + " HORIZON JOIN prices r ON (t.sym = r.sym AND r.ask > $4)"
                        + " LIST (0s, 2s) AS h WHERE concat(t.side, '') = $3",
                // Two right-hand tables, unkeyed.
                "SELECT t.ts, h.offset, p.sym, p.bid, r.sym, r.ask FROM trades t"
                        + " HORIZON JOIN (prices WHERE concat(sym, '') = $1) p"
                        + " HORIZON JOIN (prices WHERE ask > $4) r"
                        + " LIST (-2s, 0s) AS h WHERE t.price > $2",
        };
        // $1 and $3 are strings, $2 and $4 doubles. The right-hand filters match, then match
        // nothing, then match again; then the master filters do the same.
        final String[][] values = {
                {"sym0", "20.0", "sell", "10.0"},
                {"ZZZ", "20.0", "sell", "100.0"},
                {"sym1", "20.0", "sell", "12.0"},
                {"sym1", "1000.0", "none", "12.0"},
                {"sym0", "15.0", "buy", "10.0"},
                {"sym0", "20.0", "sell", "10.0"},
        };
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(rnd));
            TestUtils.execute(
                    pool,
                    (engine, _, sqlExecutionContext) -> {
                        createTradesTable(engine, sqlExecutionContext);
                        createPricesTable(engine, sqlExecutionContext, "prices");
                        if (convertToParquet) {
                            engine.execute("ALTER TABLE trades CONVERT PARTITION TO PARQUET WHERE ts >= 0", sqlExecutionContext);
                            engine.execute("ALTER TABLE prices CONVERT PARTITION TO PARQUET WHERE ts >= 0", sqlExecutionContext);
                        }
                        final BindVariableService bindVariableService = sqlExecutionContext.getBindVariableService();
                        final StringSink expected = new StringSink();
                        final StringSink actual = new StringSink();
                        for (String query : queries) {
                            for (boolean isParallel : new boolean[]{false, true}) {
                                setBindVariables(bindVariableService, values[0]);
                                sqlExecutionContext.setParallelHorizonJoinEnabled(isParallel);
                                try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
                                    TestUtils.assertFactoryInTree(
                                            factory,
                                            isParallel ? AsyncHorizonJoinProjectionRecordCursorFactory.class : HorizonJoinProjectionRecordCursorFactory.class,
                                            query
                                    );
                                    for (String[] value : values) {
                                        sqlExecutionContext.setParallelHorizonJoinEnabled(false);
                                        TestUtils.printSql(engine, sqlExecutionContext, inlineBindVariables(query, value), expected);
                                        setBindVariables(bindVariableService, value);
                                        printRows(factory, sqlExecutionContext, actual);
                                        TestUtils.assertEquals(query + " " + String.join(", ", value), expected, actual);
                                    }
                                }
                            }
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testParallelHorizonJoinProjectionConcurrent() throws Exception {
        // Queries share one worker pool, so the reading thread of one query reduces the tasks of
        // another and takes a worker slot of that query's atom. Each thread runs one shape after
        // another, and every run must return the rows of the serial factory.
        final String[] queries = {
                // Keyed.
                "SELECT t.ts, t.sym, h.offset, p.bid FROM trades t HORIZON JOIN prices p ON (sym) LIST (-1s, 0s, 1s) AS h",
                // Unkeyed.
                "SELECT t.ts, h.offset, p.sym, p.ask FROM trades t HORIZON JOIN prices p RANGE FROM -2s TO 2s STEP 1s AS h",
                // JIT master filter.
                "SELECT t.ts, t.sym, h.offset, p.bid FROM trades t HORIZON JOIN prices p ON (sym) LIST (0s, 3s) AS h WHERE t.price > 25",
                // Master and right-hand filters that aren't thread-safe.
                "SELECT t.ts, h.offset, p.bid FROM trades t HORIZON JOIN (prices WHERE concat(sym, '') != 'sym0') p ON (sym)"
                        + " LIST (-1s, 1s) AS h WHERE concat(t.side, '') = 'buy'",
                // Two right-hand tables.
                "SELECT t.ts, h.offset, p.bid, r.ask FROM trades t HORIZON JOIN prices p ON (sym) HORIZON JOIN prices r LIST (-1s, 2s) AS h",
                // An outer GROUP BY.
                "SELECT h_offset, sym, count() n, count(bid) nb, max(ask) a FROM ("
                        + "SELECT h.offset h_offset, t.sym, p.bid, p.ask FROM trades t HORIZON JOIN prices p ON (sym) LIST (0s, 1s, 5s) AS h"
                        + ") ORDER BY h_offset, sym",
                // A LIMIT closes the cursor while tasks are still in flight.
                "SELECT t.ts, h.offset, p.bid FROM trades t HORIZON JOIN prices p ON (sym) LIST (0s, 1s) AS h LIMIT 7",
                // 200 offsets, above the slot cap: the serial factory matches, and the master
                // filter runs on the workers.
                "SELECT t.ts, h.offset, p.bid FROM trades t HORIZON JOIN prices p ON (sym) RANGE FROM 0s TO 199s STEP 1s AS h WHERE t.price > 29",
        };
        final int threadCount = 6;
        final int iterationCount = 20;
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(rnd));
            TestUtils.execute(
                    pool,
                    (engine, _, sqlExecutionContext) -> {
                        createTradesTable(engine, sqlExecutionContext);
                        createPricesTable(engine, sqlExecutionContext, "prices");
                        if (convertToParquet) {
                            engine.execute("ALTER TABLE trades CONVERT PARTITION TO PARQUET WHERE ts >= 0", sqlExecutionContext);
                            engine.execute("ALTER TABLE prices CONVERT PARTITION TO PARQUET WHERE ts >= 0", sqlExecutionContext);
                        }
                        final ObjList<String> expected = new ObjList<>();
                        final StringSink sink = new StringSink();
                        sqlExecutionContext.setParallelHorizonJoinEnabled(false);
                        for (String query : queries) {
                            TestUtils.printSql(engine, sqlExecutionContext, query, sink);
                            expected.add(sink.toString());
                        }

                        final ConcurrentHashMap<Integer, Throwable> errors = new ConcurrentHashMap<>();
                        final CyclicBarrier barrier = new CyclicBarrier(threadCount);
                        final SOCountDownLatch haltLatch = new SOCountDownLatch(threadCount);
                        final int workerCount = sqlExecutionContext.getSharedQueryWorkerCount();
                        for (int i = 0; i < threadCount; i++) {
                            final int threadId = i;
                            final Rnd threadRnd = new Rnd(rnd.nextLong(), rnd.nextLong());
                            new Thread(() -> {
                                final StringSink actual = new StringSink();
                                // Each thread compiles and runs against a context of its own.
                                try (SqlExecutionContextImpl threadContext = TestUtils.createSqlExecutionCtx(engine, workerCount)) {
                                    threadContext.setParallelHorizonJoinEnabled(true);
                                    TestUtils.await(barrier);
                                    for (int j = 0; j < iterationCount; j++) {
                                        final int q = threadRnd.nextInt(queries.length);
                                        TestUtils.printSql(engine, threadContext, queries[q], actual);
                                        TestUtils.assertEquals(queries[q], expected.getQuick(q), actual);
                                    }
                                } catch (Throwable th) {
                                    errors.put(threadId, th);
                                } finally {
                                    Path.clearThreadLocals();
                                    haltLatch.countDown();
                                }
                            }).start();
                        }
                        haltLatch.await();
                        assertNoThreadErrors(errors);
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testParallelHorizonJoinProjectionConcurrentWithWriter() throws Exception {
        // One thread appends trades and quotes while others run the projection, serially and in
        // parallel, with fresh and with reused factories. Every quote the writer appends lies a
        // day past the last horizon timestamp, so it changes no row, and the trades arrive in
        // timestamp order. Every result must therefore hold the first trades, each with one row
        // per offset and the bid of the per-offset ASOF JOIN. Once the writer stops, the result
        // must hold every trade.
        final int initialTradeCount = 1_000;
        final int tradeCount = 4_000;
        final int batchSize = 100;
        final long[] offsets = {-1_000_000, 0, 2_000_000};
        final String query = "SELECT t.id, h.offset, q.bid FROM trades t HORIZON JOIN quotes q ON (sym) LIST (-1s, 0s, 2s) AS h";
        final boolean isTradesWal = rnd.nextBoolean();
        final int readerCount = 3;
        assertMemoryLeak(() -> {
            // Only the test's own engine can lock the table name registry, which a WAL table
            // needs, so the pool runs on that engine.
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(rnd));
            try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, pool.getWorkerCount())) {
                TestUtils.setupWorkerPool(pool, engine);
                pool.start(LOG);
                try {
                    // Trades about a second apart; E has no quotes until the writer adds some.
                    engine.execute("""
                            CREATE TABLE trades_src AS (
                                SELECT x id, '2020-01-01'::TIMESTAMP + x * 1_000_000L + rnd_long(-400_000, 400_000, 0) ts,
                                       rnd_symbol('A', 'B', 'C', 'D', 'E') sym
                                FROM long_sequence(%d)
                            ) TIMESTAMP(ts) PARTITION BY HOUR
                            """.formatted(tradeCount), context);
                    engine.execute(
                            "CREATE TABLE trades (id LONG, ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts) PARTITION BY HOUR" + (isTradesWal ? " WAL" : " BYPASS WAL"),
                            context
                    );
                    engine.execute("CREATE TABLE quotes (ts TIMESTAMP, sym SYMBOL, bid DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR WAL", context);
                    // Quotes every 300 ms until 10 seconds past the last trade.
                    engine.execute("""
                            INSERT INTO quotes
                            SELECT '2020-01-01'::TIMESTAMP + x * 300_000L, rnd_symbol('A', 'B', 'C', 'D'), rnd_double()
                            FROM long_sequence(%d)
                            """.formatted((tradeCount + 10) * 10 / 3), context);
                    engine.execute("INSERT INTO trades SELECT * FROM trades_src WHERE id <= " + initialTradeCount, context);
                    drainWalQueue(engine);
                    final double[] bids = readAsOfJoinBids(engine, context, "trades_src", offsets, tradeCount);

                    final ConcurrentHashMap<Integer, Throwable> errors = new ConcurrentHashMap<>();
                    final AtomicBoolean isWriterDone = new AtomicBoolean();
                    final CyclicBarrier barrier = new CyclicBarrier(readerCount + 1);
                    final SOCountDownLatch haltLatch = new SOCountDownLatch(readerCount + 1);
                    final int workerCount = context.getSharedQueryWorkerCount();
                    new Thread(() -> {
                        try (SqlExecutionContextImpl writerContext = TestUtils.createSqlExecutionCtx(engine, workerCount)) {
                            TestUtils.await(barrier);
                            for (int lo = initialTradeCount; lo < tradeCount; lo += batchSize) {
                                engine.execute("INSERT INTO trades SELECT * FROM trades_src WHERE id > " + lo + " AND id <= " + (lo + batchSize), writerContext);
                                // A day past the last trade, with quotes for E among them.
                                engine.execute(
                                        "INSERT INTO quotes SELECT '2020-01-02'::TIMESTAMP + (" + lo + " + x) * 1_000_000L, rnd_symbol('A', 'B', 'C', 'D', 'E'), rnd_double()"
                                                + " FROM long_sequence(" + batchSize + ")",
                                        writerContext
                                );
                                drainWalQueue(engine);
                            }
                        } catch (Throwable th) {
                            errors.put(-1, th);
                        } finally {
                            isWriterDone.set(true);
                            Path.clearThreadLocals();
                            haltLatch.countDown();
                        }
                    }).start();
                    for (int i = 0; i < readerCount; i++) {
                        final int readerId = i;
                        final Rnd readerRnd = new Rnd(rnd.nextLong(), rnd.nextLong());
                        new Thread(() -> {
                            try (SqlExecutionContextImpl readerContext = TestUtils.createSqlExecutionCtx(engine, workerCount)) {
                                // The first reader runs the serial factory.
                                readerContext.setParallelHorizonJoinEnabled(readerId > 0);
                                TestUtils.await(barrier);
                                try (RecordCursorFactory reusedFactory = engine.select(query, readerContext)) {
                                    int lastTradeCount = initialTradeCount;
                                    for (int runs = 0; !isWriterDone.get() || runs < 10; runs++) {
                                        final int seenTradeCount;
                                        if (readerRnd.nextBoolean()) {
                                            try (RecordCursor cursor = reusedFactory.getCursor(readerContext)) {
                                                seenTradeCount = assertTradePrefix(cursor, offsets, bids);
                                            }
                                        } else {
                                            try (
                                                    RecordCursorFactory factory = engine.select(query, readerContext);
                                                    RecordCursor cursor = factory.getCursor(readerContext)
                                            ) {
                                                seenTradeCount = assertTradePrefix(cursor, offsets, bids);
                                            }
                                        }
                                        // A later run reads a later snapshot.
                                        Assert.assertTrue(seenTradeCount + " < " + lastTradeCount, seenTradeCount >= lastTradeCount);
                                        lastTradeCount = seenTradeCount;
                                    }
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
                    assertNoThreadErrors(errors);

                    drainWalQueue(engine);
                    // The appended quotes leave the per-offset ASOF JOIN over the final tables unchanged.
                    Assert.assertArrayEquals(bids, readAsOfJoinBids(engine, context, "trades", offsets, tradeCount), 0.0);
                    for (boolean isParallel : new boolean[]{false, true}) {
                        context.setParallelHorizonJoinEnabled(isParallel);
                        try (
                                RecordCursorFactory factory = engine.select(query, context);
                                RecordCursor cursor = factory.getCursor(context)
                        ) {
                            Assert.assertEquals(tradeCount, assertTradePrefix(cursor, offsets, bids));
                        }
                    }
                } finally {
                    pool.haltAndAssertCleanForTest(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
                }
            }
        });
    }

    @Test
    public void testParallelHorizonJoinProjectionFiltered() throws Exception {
        testParallelHorizonJoinProjection(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                true,
                "t.side = 'sell'"
        );
    }

    @Test
    public void testParallelHorizonJoinProjectionFilteredThreadUnsafe() throws Exception {
        testParallelHorizonJoinProjection(
                "LIST (-1s, 0s, 3s) AS h",
                new long[]{-1_000_000, 0, 3_000_000},
                true,
                "concat(t.side, '_00') = 'sell_00'"
        );
    }

    @Test
    public void testParallelHorizonJoinProjectionLateMaterialization() throws Exception {
        // price > 28 passes ~10% of the rows, below the late materialization threshold.
        testParallelHorizonJoinProjection(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                true,
                "t.price > 28.0"
        );
    }

    @Test
    public void testParallelHorizonJoinProjectionManyOffsets() throws Exception {
        testParallelHorizonJoinProjection(
                "LIST (-10s, -7s, -5s, -3s, -1s, 0s, 1s, 3s, 5s, 7s, 10s) AS h",
                new long[]{-10_000_000, -7_000_000, -5_000_000, -3_000_000, -1_000_000, 0, 1_000_000, 3_000_000, 5_000_000, 7_000_000, 10_000_000},
                true,
                null
        );
    }

    @Test
    public void testParallelHorizonJoinProjectionNotKeyed() throws Exception {
        testParallelHorizonJoinProjection(
                "RANGE FROM -1s TO 1s STEP 1s AS h",
                rangeOffsets(-1, 1),
                false,
                "t.price > 12.0"
        );
    }

    @Test
    public void testParallelHorizonJoinRange() throws Exception {
        testParallelHorizonJoin(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                true,
                null
        );
    }

    @Test
    public void testParallelHorizonJoinRangeManyOffsets() throws Exception {
        testParallelHorizonJoin(
                "RANGE FROM -10s TO 10s STEP 1s AS h",
                rangeOffsets(-10, 10),
                true,
                null
        );
    }

    @Test
    public void testParallelHorizonJoinWithoutOnClause() throws Exception {
        testParallelHorizonJoin(
                "RANGE FROM -1s TO 1s STEP 1s AS h",
                rangeOffsets(-1, 1),
                false,
                null
        );
    }

    @Test
    public void testParallelMultiHorizonJoinFiltered() throws Exception {
        testParallelMultiHorizonJoin(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                true,
                "t.side = 'sell'"
        );
    }

    @Test
    public void testParallelMultiHorizonJoinFilteredThreadUnsafe() throws Exception {
        testParallelMultiHorizonJoin(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                true,
                "concat(t.side, '_00') = 'sell_00'"
        );
    }

    @Test
    public void testParallelMultiHorizonJoinFilteredWithBindVariables() throws Exception {
        testParallelMultiHorizonJoin(
                (sqlExecutionContext) -> {
                    BindVariableService bindVariableService = sqlExecutionContext.getBindVariableService();
                    bindVariableService.clear();
                    bindVariableService.setStr("side", "sell");
                },
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                true,
                "t.side = :side"
        );
    }

    @Test
    public void testParallelMultiHorizonJoinKeyed() throws Exception {
        testParallelMultiHorizonJoin(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                true,
                null
        );
    }

    @Test
    public void testParallelMultiHorizonJoinManyOffsets() throws Exception {
        testParallelMultiHorizonJoin(
                "LIST (-10s, -5s, -1s, 0s, 1s, 5s, 10s) AS h",
                new long[]{-10_000_000, -5_000_000, -1_000_000, 0, 1_000_000, 5_000_000, 10_000_000},
                true,
                null
        );
    }

    @Test
    public void testParallelMultiHorizonJoinNotKeyed() throws Exception {
        testParallelMultiHorizonJoin(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                false,
                null
        );
    }

    @Test
    public void testParallelMultiHorizonJoinProjection() throws Exception {
        testParallelMultiHorizonJoinProjection(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                true,
                null
        );
    }

    @Test
    public void testParallelMultiHorizonJoinProjectionFiltered() throws Exception {
        testParallelMultiHorizonJoinProjection(
                "LIST (-3s, 0s, 1s) AS h",
                new long[]{-3_000_000, 0, 1_000_000},
                true,
                "t.side = 'buy'"
        );
    }

    @Test
    public void testParallelMultiHorizonJoinProjectionNotKeyed() throws Exception {
        testParallelMultiHorizonJoinProjection(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                false,
                null
        );
    }

    // Verifies that per-worker x per-slave flat list indexing is correct
    // in BaseAsyncMultiHorizonJoinAtom. Uses workerCount=3 and slaveCount=2
    // with mixed keyed/non-keyed slaves so that the bug (slave-major construction
    // vs worker-major access formula) produces null/non-null map mismatches.
    @Test
    public void testParallelMultiHorizonJoinWorkerSlaveIndexing() throws Exception {
        final long[] offsetsMicros = {-1_000_000, 0, 1_000_000};

        // Slave 0 is keyed (ON clause), slave 1 is non-keyed (no ON clause).
        // With wrong indexing, some workers get null ASOF maps for the keyed slave
        // and non-null maps for the non-keyed slave, producing wrong results.
        String horizonQuery = """
                SELECT h.offset AS h_offset, t.sym,
                       count(p0.bid) AS cnt_bid0, max(p0.bid) AS max_bid0,
                       count(p1.ask) AS cnt_ask1, max(p1.ask) AS max_ask1
                FROM trades t
                HORIZON JOIN prices0 AS p0 ON (t.sym = p0.sym)
                HORIZON JOIN prices1 AS p1
                    LIST (-1s, 0s, 1s) AS h
                ORDER BY h_offset, t.sym
                """;

        // Reference query: per-offset ASOF JOINs.
        StringBuilder ref = new StringBuilder();
        ref.append("SELECT h_offset, sym, count(bid0) AS cnt_bid0, max(bid0) AS max_bid0,");
        ref.append(" count(ask1) AS cnt_ask1, max(ask1) AS max_ask1 FROM (");
        for (int i = 0; i < offsetsMicros.length; i++) {
            if (i > 0) {
                ref.append(" UNION ALL ");
            }
            ref.append("SELECT CAST(").append(offsetsMicros[i]).append(" AS long) AS h_offset, t.sym");
            ref.append(", p0.bid AS bid0, p1.ask AS ask1");
            ref.append(" FROM (SELECT * FROM (SELECT dateadd('u', ").append(offsetsMicros[i]);
            ref.append(", ts) AS ts, sym FROM trades) TIMESTAMP(ts)) t");
            ref.append(" ASOF JOIN prices0 p0 ON (t.sym = p0.sym)");
            ref.append(" ASOF JOIN prices1 p1");
        }
        ref.append(") GROUP BY h_offset, sym ORDER BY h_offset, sym");
        String referenceQuery = ref.toString();

        assertMemoryLeak(() -> {
            // 3 workers vs 2 slaves to trigger the indexing mismatch.
            final WorkerPool pool = new TestWorkerPool(3, TestUtils.getWorkerPoolMode(rnd));
            TestUtils.execute(
                    pool,
                    (engine, _, sqlExecutionContext) -> {
                        // Force parallel execution regardless of randomized setUp() value.
                        sqlExecutionContext.setParallelHorizonJoinEnabled(true);

                        engine.execute(
                                """
                                        CREATE TABLE trades (
                                                ts TIMESTAMP,
                                                sym SYMBOL
                                        ) TIMESTAMP(ts) PARTITION BY HOUR;
                                        """,
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO trades"
                                        + "  SELECT '2020-01-01T00:05'::timestamp + (3_600_000 * x) AS ts,"
                                        + "      rnd_symbol('A', 'B', 'C', 'D', 'E') AS sym"
                                        + "  FROM long_sequence(" + ROW_COUNT + ");",
                                sqlExecutionContext
                        );

                        // Slave 0: keyed (ON clause), bid prices.
                        engine.execute(
                                """
                                        CREATE TABLE prices0 (
                                            ts TIMESTAMP,
                                            sym SYMBOL CAPACITY 128,
                                            bid DOUBLE
                                        ) TIMESTAMP(ts) PARTITION BY HOUR;
                                        """,
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO prices0"
                                        + "  SELECT '2020-01-01'::timestamp + (360_000 * x) AS ts,"
                                        + "      rnd_symbol('A', 'B', 'C', 'D', 'E') AS sym,"
                                        + "      rnd_double() * 50.0 AS bid"
                                        + "  FROM long_sequence(" + 10 * ROW_COUNT + ");",
                                sqlExecutionContext
                        );

                        // Slave 1: non-keyed (no ON clause), ask prices.
                        engine.execute(
                                """
                                        CREATE TABLE prices1 (
                                            ts TIMESTAMP,
                                            ask DOUBLE
                                        ) TIMESTAMP(ts) PARTITION BY HOUR;
                                        """,
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO prices1"
                                        + "  SELECT '2020-01-01'::timestamp + (720_000 * x) AS ts,"
                                        + "      rnd_double() * 200.0 + 100.0 AS ask"
                                        + "  FROM long_sequence(" + 5 * ROW_COUNT + ");",
                                sqlExecutionContext
                        );

                        final StringSink horizonSink = new StringSink();
                        TestUtils.printSql(engine, sqlExecutionContext, horizonQuery, horizonSink);

                        final StringSink referenceSink = new StringSink();
                        TestUtils.printSql(engine, sqlExecutionContext, referenceQuery, referenceSink);

                        try {
                            TestUtils.assertEquals(referenceSink, horizonSink);
                        } catch (AssertionError e) {
                            LOG.error().$("Multi HORIZON JOIN query: ").$(horizonQuery).$();
                            LOG.error().$("Reference query: ").$(referenceQuery).$();
                            throw e;
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    private static String asOfSlave(String table, String alias, boolean keyed, String slaveFilter) {
        return " ASOF JOIN " + slaveTable(table, slaveFilter) + " " + alias
                + (keyed ? " ON (t.sym = " + alias + ".sym)" : "");
    }

    private static void assertNoThreadErrors(ConcurrentHashMap<Integer, Throwable> errors) {
        if (!errors.isEmpty()) {
            for (Map.Entry<Integer, Throwable> entry : errors.entrySet()) {
                LOG.error().$("error in thread [id=").$(entry.getKey()).$("] ").$(entry.getValue()).$();
            }
            final Throwable first = errors.values().iterator().next();
            throw new AssertionError("errors in " + errors.size() + " thread(s), first: " + first, first);
        }
    }

    private static void assertProjectionFactory(CairoEngine engine, SqlExecutionContext sqlExecutionContext, String query) throws SqlException {
        try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
            TestUtils.assertFactoryInTree(
                    factory,
                    sqlExecutionContext.isParallelHorizonJoinEnabled()
                            ? AsyncHorizonJoinProjectionRecordCursorFactory.class
                            : HorizonJoinProjectionRecordCursorFactory.class,
                    query
            );
        }
    }

    private static void assertQueriesMatch(CairoEngine engine, SqlExecutionContext sqlExecutionContext, String query, String referenceQuery) throws SqlException {
        final StringSink actual = new StringSink();
        TestUtils.printSql(engine, sqlExecutionContext, query, actual);
        final StringSink expected = new StringSink();
        TestUtils.printSql(engine, sqlExecutionContext, referenceQuery, expected);
        try {
            TestUtils.assertEquals(expected, actual);
        } catch (AssertionError e) {
            LOG.error().$("HORIZON JOIN query: ").$(query).$();
            LOG.error().$("Reference query: ").$(referenceQuery).$();
            throw e;
        }
    }

    // Reads the rows of a projection over the first trades of the table: one row per trade and
    // offset, the trade ids from 1 and the bids of the per-offset ASOF JOIN. Returns the number
    // of trades.
    private static int assertTradePrefix(RecordCursor cursor, long[] offsets, double[] bids) {
        final Record record = cursor.getRecord();
        int row = 0;
        while (cursor.hasNext()) {
            Assert.assertTrue("more rows than trades", row < bids.length);
            final int offsetIndex = row % offsets.length;
            Assert.assertEquals("row " + row, row / offsets.length + 1, record.getLong(0));
            Assert.assertEquals("row " + row, offsets[offsetIndex], record.getLong(1));
            Assert.assertEquals("row " + row, bids[row], record.getDouble(2), 0.0);
            row++;
        }
        Assert.assertEquals(0, row % offsets.length);
        return row / offsets.length;
    }

    private static void createPricesTable(CairoEngine engine, SqlExecutionContext sqlExecutionContext, String name) throws SqlException {
        engine.execute(
                """
                        CREATE TABLE %s (
                            ts TIMESTAMP,
                            sym SYMBOL CAPACITY 1024,
                            bid DOUBLE,
                            ask DOUBLE
                        ) TIMESTAMP(ts) PARTITION BY HOUR;
                        """.formatted(name),
                sqlExecutionContext
        );
        // Price rows at 360_000 us = 360ms spacing, 10x denser than trades.
        engine.execute(
                """
                        INSERT INTO %s
                        SELECT
                            '2020-01-01'::TIMESTAMP + (360_000*x) + rnd_long(-200, 200, 0) AS ts,
                            rnd_symbol_zipf(100, 2.0) AS sym,
                            rnd_double() * 10.0 + 5.0 AS bid,
                            rnd_double() * 10.0 + 5.0 AS ask
                        FROM long_sequence(%d);
                        """.formatted(name, 10 * ROW_COUNT),
                sqlExecutionContext
        );
    }

    private static void createTradesTable(CairoEngine engine, SqlExecutionContext sqlExecutionContext) throws SqlException {
        engine.execute(
                """
                        CREATE TABLE trades (
                                ts TIMESTAMP,
                                sym SYMBOL,
                                side SYMBOL,
                                price DOUBLE,
                                amount DOUBLE
                        ) TIMESTAMP(ts) PARTITION BY HOUR;
                        """,
                sqlExecutionContext
        );
        // 3_600_000 us = 3.6s per row with at most 200us of jitter, so trade timestamps are unique
        // and ascending, and span several hourly partitions.
        engine.execute(
                """
                        INSERT INTO trades
                        SELECT
                            '2020-01-01T00:05'::TIMESTAMP + (3_600_000*x) + rnd_long(-200, 200, 0) AS ts,
                            rnd_symbol_zipf(100, 2.0) AS sym,
                            rnd_symbol('buy', 'sell') AS side,
                            rnd_double() * 20 + 10 AS price,
                            rnd_double() * 20 + 10 AS amount
                        FROM long_sequence(%d);
                        """.formatted(ROW_COUNT),
                sqlExecutionContext
        );
    }

    private static String horizonSlave(String table, String alias, boolean keyed, String slaveFilter, boolean isOnFilter) {
        final String onFilter = isOnFilter && slaveFilter != null ? slaveFilter.replace("$", alias + ".") : null;
        final String keyCondition = keyed ? "t.sym = " + alias + ".sym" : null;
        final String on = keyCondition != null && onFilter != null ? keyCondition + " AND " + onFilter
                : keyCondition != null ? keyCondition : onFilter;
        return " HORIZON JOIN " + slaveTable(table, isOnFilter ? null : slaveFilter) + " AS " + alias
                + (on != null ? " ON (" + on + ")" : "");
    }

    private static String inlineBindVariables(String query, String[] values) {
        return query.replace("$1", "'" + values[0] + "'")
                .replace("$2", values[1])
                .replace("$3", "'" + values[2] + "'")
                .replace("$4", values[3]);
    }

    private static void printRows(RecordCursorFactory factory, SqlExecutionContext sqlExecutionContext, StringSink sink) throws SqlException {
        sink.clear();
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            CursorPrinter.println(cursor, factory.getMetadata(), sink);
        }
    }

    private static String projectionReference(
            long[] offsetsMicros,
            int slaveCount,
            boolean isKeyed,
            String innerFilter,
            String projectedSlaveColumns,
            String[] slaveFilters
    ) {
        // Per offset, an ASOF JOIN of the trades shifted by the offset; one row per trade and
        // offset, ordered like the HORIZON JOIN output: by trade, then by offset.
        final StringBuilder ref = new StringBuilder();
        ref.append("SELECT trade_ts, h_offset, sym").append(projectedSlaveColumns).append(" FROM (");
        for (int i = 0; i < offsetsMicros.length; i++) {
            if (i > 0) {
                ref.append(" UNION ALL ");
            }
            ref.append("SELECT (").append(offsetsMicros[i]).append(")::LONG AS h_offset, t.trade_ts, t.sym");
            for (int s = 0; s < slaveCount; s++) {
                final String alias = slaveCount == 1 ? "p" : "p" + s;
                final String suffix = slaveCount == 1 ? "" : String.valueOf(s);
                ref.append(", ").append(alias).append(".bid AS bid").append(suffix);
                ref.append(", ").append(alias).append(".ask AS ask").append(suffix);
            }
            ref.append(" FROM (SELECT * FROM (SELECT dateadd('u', ").append(offsetsMicros[i])
                    .append(", ts) AS ts, ts AS trade_ts, sym, side, price, amount FROM trades");
            if (innerFilter != null) {
                ref.append(" WHERE ").append(innerFilter);
            }
            ref.append(") TIMESTAMP(ts)) t");
            for (int s = 0; s < slaveCount; s++) {
                final String alias = slaveCount == 1 ? "p" : "p" + s;
                final String table = slaveCount == 1 ? "prices" : "prices" + s;
                ref.append(asOfSlave(table, alias, isKeyed, slaveFilters[s]));
            }
        }
        ref.append(") ORDER BY trade_ts, h_offset");
        return ref.toString();
    }

    private static long[] rangeOffsets(int fromSec, int toSec) {
        int count = (toSec - fromSec) + 1;
        long[] offsets = new long[count];
        for (int i = 0; i < count; i++) {
            offsets[i] = (fromSec + (long) i) * 1_000_000L;
        }
        return offsets;
    }

    // The bid of the ASOF JOIN at each trade of the table and offset, in trade id and offset order.
    private static double[] readAsOfJoinBids(
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            String tradeTable,
            long[] offsets,
            int tradeCount
    ) throws SqlException {
        final StringSink sql = new StringSink();
        sql.put("SELECT id, k, bid FROM (");
        for (int k = 0; k < offsets.length; k++) {
            if (k > 0) {
                sql.put(" UNION ALL ");
            }
            sql.put("SELECT t.id, ").put(k).put(" k, q.bid FROM (SELECT * FROM (SELECT id, dateadd('u', ").put(offsets[k])
                    .put(", ts) ts, sym FROM ").put(tradeTable).put(") TIMESTAMP(ts)) t ASOF JOIN quotes q ON (sym)");
        }
        sql.put(") ORDER BY id, k");
        final double[] bids = new double[tradeCount * offsets.length];
        try (
                RecordCursorFactory factory = engine.select(sql, sqlExecutionContext);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final Record record = cursor.getRecord();
            int row = 0;
            while (cursor.hasNext()) {
                Assert.assertEquals(row / offsets.length + 1, record.getLong(0));
                bids[row++] = record.getDouble(2);
            }
            Assert.assertEquals(bids.length, row);
        }
        return bids;
    }

    private static void setBindVariables(BindVariableService bindVariableService, String[] values) throws SqlException {
        bindVariableService.setStr(0, values[0]);
        bindVariableService.setDouble(1, Double.parseDouble(values[1]));
        bindVariableService.setStr(2, values[2]);
        bindVariableService.setDouble(3, Double.parseDouble(values[3]));
    }

    private static String slaveTable(String table, String slaveFilter) {
        return slaveFilter != null ? "(" + table + " WHERE " + slaveFilter.replace("$", "") + ")" : table;
    }

    private String generateSlaveFilter() {
        return switch (rnd.nextInt(5)) {
            case 0, 1 -> null;
            case 2 -> "$bid > " + (5 + rnd.nextInt(1_000) / 100.0);
            case 3 -> "$ask < " + (5 + rnd.nextInt(1_000) / 100.0);
            default -> "$bid > $ask";
        };
    }

    private void testParallelHorizonJoin(
            String horizonClause,
            long[] offsetsMicros,
            boolean keyed,
            String filter
    ) throws Exception {
        testParallelHorizonJoin(null, horizonClause, offsetsMicros, keyed, filter);
    }

    private void testParallelHorizonJoin(
            BindVariablesInitializer initializer,
            String horizonClause,
            long[] offsetsMicros,
            boolean keyed,
            String filter
    ) throws Exception {
        final String slaveFilter = generateSlaveFilter();
        final boolean isOnFilter = keyed && rnd.nextBoolean();

        // Build HORIZON JOIN query.
        String horizonQuery = "SELECT h.offset AS h_offset"
                + (keyed ? ", t.sym" : "")
                + ", count(p.bid) AS cnt_bid, max(p.ask) AS max_ask"
                + " FROM trades t"
                + horizonSlave("prices", "p", keyed, slaveFilter, isOnFilter)
                + " " + horizonClause
                + (filter != null ? " WHERE " + filter : "")
                + " ORDER BY h_offset"
                + (keyed ? ", t.sym" : "");

        // Build reference query: UNION ALL of ASOF JOINs per offset, then GROUP BY.
        // HORIZON JOIN at offset O is equivalent to ASOF JOIN where master timestamps
        // are shifted forward by O microseconds.
        String innerFilter = filter != null ? filter.replace("t.", "") : null;
        StringBuilder ref = new StringBuilder();
        ref.append("SELECT h_offset");
        if (keyed) {
            ref.append(", sym");
        }
        ref.append(", count(bid) AS cnt_bid, max(ask) AS max_ask FROM (");

        for (int i = 0; i < offsetsMicros.length; i++) {
            if (i > 0) {
                ref.append(" UNION ALL ");
            }
            ref.append("SELECT CAST(").append(offsetsMicros[i]).append(" AS long) AS h_offset");
            if (keyed) {
                ref.append(", t.sym");
            }
            ref.append(", p.bid, p.ask");
            ref.append(" FROM (SELECT * FROM (SELECT dateadd('u', ")
                    .append(offsetsMicros[i])
                    .append(", ts) AS ts, sym, side, price, amount FROM trades");
            if (innerFilter != null) {
                ref.append(" WHERE ").append(innerFilter);
            }
            ref.append(") TIMESTAMP(ts)) t").append(asOfSlave("prices", "p", keyed, slaveFilter));
        }

        ref.append(") GROUP BY h_offset");
        if (keyed) {
            ref.append(", sym");
        }
        ref.append(" ORDER BY h_offset");
        if (keyed) {
            ref.append(", sym");
        }
        String referenceQuery = ref.toString();

        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(rnd));
            TestUtils.execute(
                    pool,
                    (engine, _, sqlExecutionContext) -> {
                        if (initializer != null) {
                            initializer.init(sqlExecutionContext);
                        }

                        engine.execute(
                                """
                                        CREATE TABLE IF NOT EXISTS trades (
                                                ts TIMESTAMP,
                                                sym SYMBOL,
                                                side SYMBOL,
                                                price DOUBLE,
                                                amount DOUBLE
                                        ) TIMESTAMP(ts) PARTITION BY HOUR;
                                        """,
                                sqlExecutionContext
                        );
                        // 3_600_000 us = 3.6s per row; with ROW_COUNT rows this spans
                        // multiple hourly partitions, allowing Parquet conversion
                        // (the most recent partition stays native).
                        engine.execute(
                                "INSERT INTO trades"
                                        + "  SELECT "
                                        + "      '2020-01-01T00:05'::timestamp + (3600000*x) + rnd_long(-200, 200, 0) as ts, "
                                        + "      rnd_symbol_zipf(100, 2.0) AS sym, "
                                        + "      rnd_symbol('buy', 'sell') as side, "
                                        + "      rnd_double() * 20 + 10 AS price, "
                                        + "      rnd_double() * 20 + 10 AS amount "
                                        + "  FROM long_sequence(" + ROW_COUNT + ");",
                                sqlExecutionContext
                        );
                        engine.execute(
                                """
                                        CREATE TABLE prices (
                                            ts TIMESTAMP,
                                            sym SYMBOL CAPACITY 1024,
                                            bid DOUBLE,
                                            ask DOUBLE
                                        ) TIMESTAMP(ts) PARTITION BY HOUR;
                                        """,
                                sqlExecutionContext
                        );
                        // Price rows at 360_000 us = 360ms spacing, 10x denser than trades.
                        engine.execute(
                                "INSERT INTO prices "
                                        + "  SELECT "
                                        + "      '2020-01-01'::timestamp + (360000*x) + rnd_long(-200, 200, 0) as ts, "
                                        + "      rnd_symbol_zipf(100, 2.0) as sym, "
                                        + "      rnd_double() * 10.0 + 5.0 as bid, "
                                        + "      rnd_double() * 10.0 + 5.0 as ask "
                                        + "  FROM long_sequence(" + 10 * ROW_COUNT + ");",
                                sqlExecutionContext
                        );

                        if (convertToParquet) {
                            engine.execute("ALTER TABLE trades CONVERT PARTITION TO PARQUET WHERE ts >= 0", sqlExecutionContext);
                            engine.execute("ALTER TABLE prices CONVERT PARTITION TO PARQUET WHERE ts >= 0", sqlExecutionContext);
                        }

                        final StringSink horizonSink = new StringSink();
                        TestUtils.printSql(engine, sqlExecutionContext, horizonQuery, horizonSink);

                        final StringSink referenceSink = new StringSink();
                        TestUtils.printSql(engine, sqlExecutionContext, referenceQuery, referenceSink);

                        try {
                            TestUtils.assertEquals(referenceSink, horizonSink);
                        } catch (AssertionError e) {
                            LOG.error().$("HORIZON JOIN query: ").$(horizonQuery).$();
                            LOG.error().$("Reference query: ").$(referenceQuery).$();
                            throw e;
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    // Tests keyed HORIZON JOIN with data designed to trigger the adaptive
    // backward-to-forward scan switch. Prices have a rare symbol only at the
    // beginning, causing deep backward scans that trigger the switch.
    private void testParallelHorizonJoinAdaptiveScan(
            String horizonClause,
            long[] offsetsMicros
    ) throws Exception {
        // Build HORIZON JOIN query (keyed, no filter).
        String horizonQuery = "SELECT h.offset AS h_offset, t.sym"
                + ", count(p.bid) AS cnt_bid, max(p.ask) AS max_ask"
                + " FROM trades t"
                + " HORIZON JOIN prices p ON (t.sym = p.sym)"
                + " " + horizonClause
                + " ORDER BY h_offset, t.sym";

        // Build reference query: UNION ALL of ASOF JOINs per offset.
        StringBuilder ref = new StringBuilder();
        ref.append("SELECT h_offset, sym, count(bid) AS cnt_bid, max(ask) AS max_ask FROM (");
        for (int i = 0; i < offsetsMicros.length; i++) {
            if (i > 0) {
                ref.append(" UNION ALL ");
            }
            ref.append("SELECT CAST(").append(offsetsMicros[i]).append(" AS long) AS h_offset");
            ref.append(", t.sym, p.bid, p.ask");
            ref.append(" FROM (SELECT * FROM (SELECT dateadd('u', ")
                    .append(offsetsMicros[i])
                    .append(", ts) AS ts, sym FROM trades) TIMESTAMP(ts)) t ASOF JOIN prices p ON (t.sym = p.sym)");
        }
        ref.append(") GROUP BY h_offset, sym ORDER BY h_offset, sym");
        String referenceQuery = ref.toString();

        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(rnd));
            TestUtils.execute(
                    pool,
                    (engine, _, sqlExecutionContext) -> {
                        // 50,000 prices at 1us spacing (all within one partition).
                        // RARE at row 0, COMMON everywhere else.
                        // The rare key causes deep backward scans that trigger the adaptive switch.
                        engine.execute(
                                """
                                        CREATE TABLE prices (
                                            ts TIMESTAMP,
                                            sym SYMBOL,
                                            bid DOUBLE,
                                            ask DOUBLE
                                        ) TIMESTAMP(ts) PARTITION BY HOUR;
                                        """,
                                sqlExecutionContext
                        );
                        engine.execute(
                                """
                                        INSERT INTO prices
                                        SELECT dateadd('u', x::int, '2020-01-01T00:00:00.000000Z'),
                                               CASE WHEN x = 1 THEN 'RARE' ELSE 'COMMON' END,
                                               x * 0.01 + 5.0,
                                               x * 0.01 + 5.5
                                        FROM long_sequence(50_000)
                                        """,
                                sqlExecutionContext
                        );

                        // 20 trades at 2,000us spacing, alternating COMMON/RARE.
                        // All in one partition so ASOF position gaps are exact row counts (~2,000).
                        engine.execute(
                                """
                                        CREATE TABLE trades (
                                            ts TIMESTAMP,
                                            sym SYMBOL
                                        ) TIMESTAMP(ts) PARTITION BY HOUR;
                                        """,
                                sqlExecutionContext
                        );
                        engine.execute(
                                """
                                        INSERT INTO trades
                                        SELECT dateadd('u', (2000 * x)::int, '2020-01-01T00:00:00.000000Z'),
                                               CASE WHEN x % 2 = 0 THEN 'RARE' ELSE 'COMMON' END
                                        FROM long_sequence(20)
                                        """,
                                sqlExecutionContext
                        );

                        if (convertToParquet) {
                            engine.execute("ALTER TABLE trades CONVERT PARTITION TO PARQUET WHERE ts >= 0", sqlExecutionContext);
                            engine.execute("ALTER TABLE prices CONVERT PARTITION TO PARQUET WHERE ts >= 0", sqlExecutionContext);
                        }

                        final StringSink horizonSink = new StringSink();
                        TestUtils.printSql(engine, sqlExecutionContext, horizonQuery, horizonSink);

                        final StringSink referenceSink = new StringSink();
                        TestUtils.printSql(engine, sqlExecutionContext, referenceQuery, referenceSink);

                        try {
                            TestUtils.assertEquals(referenceSink, horizonSink);
                        } catch (AssertionError e) {
                            LOG.error().$("HORIZON JOIN query: ").$(horizonQuery).$();
                            LOG.error().$("Reference query: ").$(referenceQuery).$();
                            throw e;
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void testParallelHorizonJoinProjection(
            String horizonClause,
            long[] offsetsMicros,
            boolean isKeyed,
            String filter
    ) throws Exception {
        // The slave filter goes into a subquery or, on a keyed join, into the ON clause.
        final String[] slaveFilters = {generateSlaveFilter()};
        final boolean isOnFilter = isKeyed && rnd.nextBoolean();
        // One row per trade and offset, in trade order and then offset order.
        final String rowsQuery = "SELECT t.ts AS trade_ts, h.offset AS h_offset, t.sym, p.bid, p.ask"
                + " FROM trades t"
                + horizonSlave("prices", "p", isKeyed, slaveFilters[0], isOnFilter)
                + " " + horizonClause
                + (filter != null ? " WHERE " + filter : "");
        final String innerFilter = filter != null ? filter.replace("t.", "") : null;
        final String rowsReference = projectionReference(offsetsMicros, 1, isKeyed, innerFilter, ", bid, ask", slaveFilters);

        // The aggregation of the report: an outer GROUP BY over the projection.
        final String aggregateQuery = "SELECT h_offset" + (isKeyed ? ", sym" : "")
                + ", count() AS n, count(bid) AS cnt_bid, max(ask) AS max_ask FROM (" + rowsQuery + ")"
                + " ORDER BY h_offset" + (isKeyed ? ", sym" : "");
        final String aggregateReference = "SELECT h_offset" + (isKeyed ? ", sym" : "")
                + ", count() AS n, count(bid) AS cnt_bid, max(ask) AS max_ask FROM (" + rowsReference + ")"
                + " ORDER BY h_offset" + (isKeyed ? ", sym" : "");

        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(rnd));
            TestUtils.execute(
                    pool,
                    (engine, _, sqlExecutionContext) -> {
                        createTradesTable(engine, sqlExecutionContext);
                        createPricesTable(engine, sqlExecutionContext, "prices");
                        if (convertToParquet) {
                            engine.execute("ALTER TABLE trades CONVERT PARTITION TO PARQUET WHERE ts >= 0", sqlExecutionContext);
                            engine.execute("ALTER TABLE prices CONVERT PARTITION TO PARQUET WHERE ts >= 0", sqlExecutionContext);
                        }
                        assertProjectionFactory(engine, sqlExecutionContext, rowsQuery);
                        assertQueriesMatch(engine, sqlExecutionContext, rowsQuery, rowsReference);
                        assertQueriesMatch(engine, sqlExecutionContext, aggregateQuery, aggregateReference);
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void testParallelMultiHorizonJoin(
            String horizonClause,
            long[] offsetsMicros,
            boolean keyed,
            String filter
    ) throws Exception {
        testParallelMultiHorizonJoin(null, horizonClause, offsetsMicros, keyed, filter);
    }

    private void testParallelMultiHorizonJoin(
            BindVariablesInitializer initializer,
            String horizonClause,
            long[] offsetsMicros,
            boolean keyed,
            String filter
    ) throws Exception {
        // Randomize 2-4 slave tables
        int slaveCount = 2 + rnd.nextInt(3);
        final String[] slaveFilters = new String[slaveCount];
        final boolean[] isOnFilters = new boolean[slaveCount];
        for (int s = 0; s < slaveCount; s++) {
            slaveFilters[s] = generateSlaveFilter();
            isOnFilters[s] = keyed && rnd.nextBoolean();
        }

        // Build multi-slave HORIZON JOIN query
        StringBuilder hq = new StringBuilder();
        hq.append("SELECT h.offset AS h_offset");
        if (keyed) {
            hq.append(", t.sym");
        }
        for (int s = 0; s < slaveCount; s++) {
            hq.append(", count(p").append(s).append(".bid) AS cnt_bid").append(s);
            hq.append(", max(p").append(s).append(".ask) AS max_ask").append(s);
        }
        hq.append(" FROM trades t");
        for (int s = 0; s < slaveCount; s++) {
            hq.append(horizonSlave("prices" + s, "p" + s, keyed, slaveFilters[s], isOnFilters[s]));
        }
        hq.append(' ').append(horizonClause);
        if (filter != null) {
            hq.append(" WHERE ").append(filter);
        }
        hq.append(" ORDER BY h_offset");
        if (keyed) {
            hq.append(", t.sym");
        }
        String horizonQuery = hq.toString();

        // Build reference query: per offset, chain ASOF JOINs for all slaves
        String innerFilter = filter != null ? filter.replace("t.", "") : null;
        StringBuilder ref = new StringBuilder();
        ref.append("SELECT h_offset");
        if (keyed) {
            ref.append(", sym");
        }
        for (int s = 0; s < slaveCount; s++) {
            ref.append(", count(bid").append(s).append(") AS cnt_bid").append(s);
            ref.append(", max(ask").append(s).append(") AS max_ask").append(s);
        }
        ref.append(" FROM (");

        for (int i = 0; i < offsetsMicros.length; i++) {
            if (i > 0) {
                ref.append(" UNION ALL ");
            }
            ref.append("SELECT CAST(").append(offsetsMicros[i]).append(" AS long) AS h_offset");
            if (keyed) {
                ref.append(", t.sym");
            }
            for (int s = 0; s < slaveCount; s++) {
                ref.append(", p").append(s).append(".bid AS bid").append(s);
                ref.append(", p").append(s).append(".ask AS ask").append(s);
            }
            ref.append(" FROM (SELECT * FROM (SELECT dateadd('u', ")
                    .append(offsetsMicros[i])
                    .append(", ts) AS ts, sym, side, price, amount FROM trades");
            if (innerFilter != null) {
                ref.append(" WHERE ").append(innerFilter);
            }
            ref.append(") TIMESTAMP(ts)) t");
            for (int s = 0; s < slaveCount; s++) {
                ref.append(asOfSlave("prices" + s, "p" + s, keyed, slaveFilters[s]));
            }
        }

        ref.append(") GROUP BY h_offset");
        if (keyed) {
            ref.append(", sym");
        }
        ref.append(" ORDER BY h_offset");
        if (keyed) {
            ref.append(", sym");
        }
        String referenceQuery = ref.toString();

        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(rnd));
            TestUtils.execute(
                    pool,
                    (engine, _, sqlExecutionContext) -> {
                        if (initializer != null) {
                            initializer.init(sqlExecutionContext);
                        }

                        engine.execute(
                                """
                                        CREATE TABLE IF NOT EXISTS trades (
                                                ts TIMESTAMP,
                                                sym SYMBOL,
                                                side SYMBOL,
                                                price DOUBLE,
                                                amount DOUBLE
                                        ) TIMESTAMP(ts) PARTITION BY HOUR;
                                        """,
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO trades"
                                        + "  SELECT "
                                        + "      '2020-01-01T00:05'::timestamp + (3600000*x) + rnd_long(-200, 200, 0) as ts, "
                                        + "      rnd_symbol_zipf(100, 2.0) AS sym, "
                                        + "      rnd_symbol('buy', 'sell') as side, "
                                        + "      rnd_double() * 20 + 10 AS price, "
                                        + "      rnd_double() * 20 + 10 AS amount "
                                        + "  FROM long_sequence(" + ROW_COUNT + ");",
                                sqlExecutionContext
                        );

                        for (int s = 0; s < slaveCount; s++) {
                            engine.execute(
                                    "CREATE TABLE prices" + s + " ("
                                            + "    ts TIMESTAMP,"
                                            + "    sym SYMBOL CAPACITY 1024,"
                                            + "    bid DOUBLE,"
                                            + "    ask DOUBLE"
                                            + ") TIMESTAMP(ts) PARTITION BY HOUR;",
                                    sqlExecutionContext
                            );
                            // Each slave gets independently generated data
                            engine.execute(
                                    "INSERT INTO prices" + s
                                            + "  SELECT "
                                            + "      '2020-01-01'::timestamp + (360000*x) + rnd_long(-200, 200, 0) as ts, "
                                            + "      rnd_symbol_zipf(100, 2.0) as sym, "
                                            + "      rnd_double() * 10.0 + 5.0 as bid, "
                                            + "      rnd_double() * 10.0 + 5.0 as ask "
                                            + "  FROM long_sequence(" + 10 * ROW_COUNT + ");",
                                    sqlExecutionContext
                            );
                        }

                        if (convertToParquet) {
                            engine.execute("ALTER TABLE trades CONVERT PARTITION TO PARQUET WHERE ts >= 0", sqlExecutionContext);
                            for (int s = 0; s < slaveCount; s++) {
                                engine.execute("ALTER TABLE prices" + s + " CONVERT PARTITION TO PARQUET WHERE ts >= 0", sqlExecutionContext);
                            }
                        }

                        final StringSink horizonSink = new StringSink();
                        TestUtils.printSql(engine, sqlExecutionContext, horizonQuery, horizonSink);

                        final StringSink referenceSink = new StringSink();
                        TestUtils.printSql(engine, sqlExecutionContext, referenceQuery, referenceSink);

                        try {
                            TestUtils.assertEquals(referenceSink, horizonSink);
                        } catch (AssertionError e) {
                            LOG.error().$("Multi HORIZON JOIN query: ").$(horizonQuery).$();
                            LOG.error().$("Reference query: ").$(referenceQuery).$();
                            throw e;
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void testParallelMultiHorizonJoinProjection(
            String horizonClause,
            long[] offsetsMicros,
            boolean isKeyed,
            String filter
    ) throws Exception {
        final int slaveCount = 2 + rnd.nextInt(3);
        final StringBuilder rows = new StringBuilder("SELECT t.ts AS trade_ts, h.offset AS h_offset, t.sym");
        final StringBuilder slaveColumns = new StringBuilder();
        for (int s = 0; s < slaveCount; s++) {
            rows.append(", p").append(s).append(".bid AS bid").append(s).append(", p").append(s).append(".ask AS ask").append(s);
            slaveColumns.append(", bid").append(s).append(", ask").append(s);
        }
        rows.append(" FROM trades t");
        // Each slave gets its own filter, in a subquery or, on a keyed join, in the ON clause.
        final String[] slaveFilters = new String[slaveCount];
        for (int s = 0; s < slaveCount; s++) {
            slaveFilters[s] = generateSlaveFilter();
            rows.append(horizonSlave("prices" + s, "p" + s, isKeyed, slaveFilters[s], isKeyed && rnd.nextBoolean()));
        }
        rows.append(' ').append(horizonClause);
        if (filter != null) {
            rows.append(" WHERE ").append(filter);
        }
        final String rowsQuery = rows.toString();
        final String innerFilter = filter != null ? filter.replace("t.", "") : null;
        final String rowsReference = projectionReference(offsetsMicros, slaveCount, isKeyed, innerFilter, slaveColumns.toString(), slaveFilters);

        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(rnd));
            TestUtils.execute(
                    pool,
                    (engine, _, sqlExecutionContext) -> {
                        createTradesTable(engine, sqlExecutionContext);
                        for (int s = 0; s < slaveCount; s++) {
                            createPricesTable(engine, sqlExecutionContext, "prices" + s);
                        }
                        if (convertToParquet) {
                            engine.execute("ALTER TABLE trades CONVERT PARTITION TO PARQUET WHERE ts >= 0", sqlExecutionContext);
                            for (int s = 0; s < slaveCount; s++) {
                                engine.execute("ALTER TABLE prices" + s + " CONVERT PARTITION TO PARQUET WHERE ts >= 0", sqlExecutionContext);
                            }
                        }
                        assertProjectionFactory(engine, sqlExecutionContext, rowsQuery);
                        assertQueriesMatch(engine, sqlExecutionContext, rowsQuery, rowsReference);
                    },
                    configuration,
                    LOG
            );
        });
    }

    private interface BindVariablesInitializer {
        void init(SqlExecutionContext sqlExecutionContext) throws SqlException;
    }
}
