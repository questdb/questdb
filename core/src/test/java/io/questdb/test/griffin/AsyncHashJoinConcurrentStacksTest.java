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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * Stacked and swapped stars of the parallel hash join under concurrent load, on thread workers and
 * on fiber workers: stacks, swapped stacks, LIMITs, negative LIMITs, counts and post-filtered
 * ORDER BY over stars, next to Async Filter, parallel GROUP BY and fused hash join GROUP BY queries,
 * from many threads over small shared queues, with some executions closed mid-stream while their
 * tasks are in flight; a consumer that dwells on a frame, a deferred chain or a pending
 * continuation while other queries run to their end on the same queues; and writers that insert out
 * of order, convert partitions to Parquet and drop partitions under open cursors of stacked joins,
 * on WAL and non-WAL tables, which keep the snapshot they opened. Latches force every interleaving, and every result compares
 * with the ordinary plan's, in order.
 */
public class AsyncHashJoinConcurrentStacksTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(AsyncHashJoinConcurrentStacksTest.class);
    // The queries of the mixed load: stars of the operator, then an Async Filter, a parallel GROUP BY
    // and a fused hash join GROUP BY.
    private static final String[] MIXED_QUERIES = {
            "SELECT t.k, t.v, d.w, e.ew, g.gw FROM t LEFT JOIN d ON t.k = d.k JOIN e ON t.l = e.l LEFT JOIN g ON t.s = g.s",
            "SELECT t.k, t.v, d.w, e.ew FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v % 50 LEFT JOIN e ON t.l = e.l WHERE e.ew > t.v % 7 OR e.ew IS NULL",
            "SELECT d.w, t.v, e.ew FROM d JOIN t ON d.k = t.k LEFT JOIN e ON d.l = e.l",
            "SELECT t.k, t.v, d.w, e.ew FROM t LEFT JOIN d ON t.k = d.k JOIN e ON t.l = e.l LIMIT 37",
            "SELECT t.k, t.v, d.w, e.ew FROM t LEFT JOIN d ON t.k = d.k JOIN e ON t.l = e.l LIMIT -41",
            "SELECT count(*) FROM (SELECT t.k, d.w, e.ew FROM t LEFT JOIN d ON t.k = d.k JOIN e ON t.l = e.l)",
            "SELECT t.k, t.v, d.w, g.gw FROM t LEFT JOIN d ON t.k = d.k LEFT JOIN g ON t.s = g.s ORDER BY t.ts DESC",
            "SELECT * FROM t WHERE v % 7 = 3",
            "SELECT k, count(*), sum(v) FROM t GROUP BY k ORDER BY k",
            "SELECT g.s, sum(t.v) FROM t JOIN g ON t.s = g.s GROUP BY g.s ORDER BY g.s",
    };
    private static final int WORKER_COUNT = 4;

    @After
    public void restorePageFrameSizes() {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
    }

    @Test
    public void testConsumerDwellsWhileOtherQueriesRunOnFiberWorkers() throws Exception {
        assertConsumerDwells(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testConsumerDwellsWhileOtherQueriesRunOnThreadWorkers() throws Exception {
        assertConsumerDwells(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testMixedLoadOnFiberWorkers() throws Exception {
        assertMixedLoad(WorkerPoolMode.FIBER_HOST);
    }

    @Test
    public void testMixedLoadOnThreadWorkers() throws Exception {
        assertMixedLoad(WorkerPoolMode.LEGACY);
    }

    @Test
    public void testOpenCursorsKeepTheirSnapshotNonWal() throws Exception {
        assertOpenCursorsKeepTheirSnapshot(false);
    }

    @Test
    public void testOpenCursorsKeepTheirSnapshotWal() throws Exception {
        assertOpenCursorsKeepTheirSnapshot(true);
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(60, TimeUnit.SECONDS)) {
                throw new AssertionError("latch timed out");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static void createTables(CairoEngine db, SqlExecutionContext context) throws Exception {
        db.execute("CREATE TABLE t AS (SELECT (x % 400)::INT k, x v, (x % 300)::LONG l, ('s' || (x % 250))::SYMBOL s, " +
                "CASE WHEN x <= 2 THEN 0 ELSE x::INT END h, timestamp_sequence('2024-01-01', 40_000_000) ts FROM long_sequence(6_000)) TIMESTAMP(ts) PARTITION BY DAY", context);
        db.execute("CREATE TABLE d AS (SELECT (x % 350)::INT k, x w, (x % 300)::LONG l FROM long_sequence(2_000))", context);
        db.execute("CREATE TABLE e AS (SELECT (x % 300)::LONG l, x ew FROM long_sequence(600))", context);
        db.execute("CREATE TABLE g AS (SELECT ('s' || (x % 200))::SYMBOL s, x gw FROM long_sequence(400))", context);
        // Every row of hot shares the key that t's first two rows hold.
        db.execute("CREATE TABLE hot AS (SELECT 0 h, x hw FROM long_sequence(300))", context);
        db.execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-02'", context);
    }

    private static SqlExecutionContextImpl newContext(CairoEngine db) {
        final SqlExecutionContextImpl context = new SqlExecutionContextImpl(db, WORKER_COUNT)
                .with(AllowAllSecurityContext.INSTANCE, null, null, -1, null);
        context.setParallelHashJoinEnabled(true);
        return context;
    }

    // Reads the rows that the cursor still returns into the sink, as ParallelHashJoinTestUtils prints
    // them without their extras.
    private static void readRest(RecordCursor cursor, RecordMetadata metadata, StringSink sink) {
        while (cursor.hasNext()) {
            CursorPrinter.println(cursor.getRecord(), metadata, sink, true);
        }
    }

    private static int readRows(RecordCursor cursor, RecordMetadata metadata, StringSink sink, int rows) {
        int read = 0;
        while (read < rows && cursor.hasNext()) {
            CursorPrinter.println(cursor.getRecord(), metadata, sink, true);
            read++;
        }
        return read;
    }

    private static String rows(CairoEngine db, String sql, SqlExecutionContextImpl context, boolean isParallel) throws Exception {
        final boolean wasEnabled = context.isParallelHashJoinEnabled();
        context.setParallelHashJoinEnabled(isParallel);
        try (RecordCursorFactory factory = db.select(sql, context)) {
            Assert.assertEquals(sql, isParallel, find(factory) != null);
            final StringSink sink = new StringSink();
            CursorPrinter.println(factory.getMetadata(), sink);
            try (RecordCursor cursor = factory.getCursor(context)) {
                readRest(cursor, factory.getMetadata(), sink);
            }
            return sink.toString();
        } finally {
            context.setParallelHashJoinEnabled(wasEnabled);
        }
    }

    /**
     * A join's consumer reads a few rows and then holds its cursor, while its dispatched frames sit in
     * the small queue, holding the frame it reads, the chain of a hot key that it expands, or a frame
     * whose rest waits for a continuation. Other threads run joins and Async Filter queries on the same
     * queues to their end meanwhile, stealing and reducing locally where the queue is full. The held
     * consumer then reads on, and every result equals the ordinary plan's.
     */
    private void assertConsumerDwells(WorkerPoolMode mode) throws Exception {
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
        setProperty(PropertyKey.CAIRO_UNORDERED_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 3);
        assertMemoryLeak(() -> TestUtils.execute(new TestWorkerPool(WORKER_COUNT, mode), (db, compiler, ignore) -> {
            // [SQL, rows the consumer reads before it dwells, cap of tuples per task]
            final String[][] heldQueries = {
                    // The consumer holds a frame of tuples, with later frames dispatched.
                    {"SELECT t.k, t.v, d.w, e.ew FROM t LEFT JOIN d ON t.k = d.k JOIN e ON t.l = e.l", "5", "0"},
                    // The consumer expands the chains that t's first rows meet in hot and d.
                    {"SELECT t.v, d.w, hot.hw FROM t JOIN hot ON t.h = hot.h LEFT JOIN d ON t.k = d.k", "50", "0"},
                    // The task of the first frame stopped at its cap of one tuple, so the frame's rest
                    // waits for a continuation.
                    {"SELECT t.k, t.v, d.w FROM t LEFT JOIN d ON t.k = d.k AND d.w > t.v % 50", "1", "1"},
            };
            final String[] otherQueries = {
                    MIXED_QUERIES[0], MIXED_QUERIES[2], MIXED_QUERIES[5], MIXED_QUERIES[7], MIXED_QUERIES[8],
            };
            final String[] heldExpected = new String[heldQueries.length];
            final String[] otherExpected = new String[otherQueries.length];
            try (SqlExecutionContextImpl context = newContext(db)) {
                createTables(db, context);
                context.changePageFrameSizes(16, 64);
                for (int i = 0; i < heldQueries.length; i++) {
                    heldExpected[i] = rows(db, heldQueries[i][0], context, false);
                }
                for (int i = 0; i < otherQueries.length; i++) {
                    otherExpected[i] = ordinary(otherQueries[i], context);
                }
            }
            for (int q = 0; q < heldQueries.length; q++) {
                final String heldSql = heldQueries[q][0];
                final int heldRows = Integer.parseInt(heldQueries[q][1]);
                final boolean isCapped = heldQueries[q][2].equals("1");
                if (isCapped) {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
                }
                final CountDownLatch dwelling = new CountDownLatch(1);
                final CountDownLatch release = new CountDownLatch(1);
                final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
                final StringSink heldSink = new StringSink();
                final AsyncHashJoinRecordCursorFactory[] heldJoin = new AsyncHashJoinRecordCursorFactory[1];
                final Thread holder = new Thread(() -> {
                    try (SqlExecutionContextImpl context = newContext(db)) {
                        context.changePageFrameSizes(16, 64);
                        try (RecordCursorFactory factory = db.select(heldSql, context)) {
                            heldJoin[0] = find(factory);
                            Assert.assertNotNull(heldSql, heldJoin[0]);
                            CursorPrinter.println(factory.getMetadata(), heldSink);
                            try (RecordCursor cursor = factory.getCursor(context)) {
                                Assert.assertEquals(heldRows, readRows(cursor, factory.getMetadata(), heldSink, heldRows));
                                dwelling.countDown();
                                await(release);
                                readRest(cursor, factory.getMetadata(), heldSink);
                            }
                            Assert.assertEquals(0, heldJoin[0].getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                        }
                    } catch (Throwable th) {
                        errors.add(th);
                    } finally {
                        dwelling.countDown();
                    }
                }, "held-consumer");
                holder.start();
                await(dwelling);
                Assert.assertTrue(errors.isEmpty() ? "" : errors.peek().toString(), errors.isEmpty());
                if (isCapped) {
                    // The first frame's rest waits for a continuation while the consumer dwells.
                    Assert.assertTrue(heldJoin[0].getAtom().getRing().getResumePosition(0) > -1);
                }
                // Other queries run to their end on the same queues while the consumer dwells.
                final int threadCount = 4;
                final ObjList<Thread> threads = new ObjList<>();
                final AtomicInteger runs = new AtomicInteger();
                for (int t = 0; t < threadCount; t++) {
                    final int threadIndex = t;
                    final Thread thread = new Thread(() -> {
                        try (SqlExecutionContextImpl context = newContext(db)) {
                            for (int i = 0; i < otherQueries.length; i++) {
                                final int o = (threadIndex + i) % otherQueries.length;
                                context.changePageFrameSizes(16, 64);
                                try (RecordCursorFactory factory = db.select(otherQueries[o], context)) {
                                    Assert.assertEquals(otherQueries[o], otherExpected[o], result(factory, context));
                                }
                                runs.incrementAndGet();
                            }
                        } catch (Throwable th) {
                            errors.add(th);
                        }
                    }, "other-" + t);
                    threads.add(thread);
                    thread.start();
                }
                for (int t = 0; t < threadCount; t++) {
                    threads.getQuick(t).join(TimeUnit.SECONDS.toMillis(120));
                    Assert.assertFalse("the other queries hang while a consumer dwells", threads.getQuick(t).isAlive());
                }
                release.countDown();
                holder.join(TimeUnit.SECONDS.toMillis(120));
                Assert.assertFalse(holder.isAlive());
                for (Throwable th : errors) {
                    throw new AssertionError(th);
                }
                Assert.assertEquals(threadCount * otherQueries.length, runs.get());
                TestUtils.assertEquals(heldSql, heldExpected[q], heldSink);
                if (isCapped) {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, (String) null);
                }
            }
        }, configuration, LOG));
    }

    private void assertMixedLoad(WorkerPoolMode mode) throws Exception {
        // Small queues that the concurrent queries fill, so that their owners steal and reduce
        // locally; builds of 500 rows or more in rounds of partitions of 64 rows, the smaller builds
        // of a stack at once; and a cap of three tuples per task.
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
        setProperty(PropertyKey.CAIRO_UNORDERED_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 4);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 500);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 64);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 3);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 3);
        assertMemoryLeak(() -> TestUtils.execute(new TestWorkerPool(WORKER_COUNT, mode), (db, compiler, ignore) -> {
            final ObjList<String> expected = new ObjList<>();
            try (SqlExecutionContextImpl context = newContext(db)) {
                createTables(db, context);
                for (String sql : MIXED_QUERIES) {
                    context.changePageFrameSizes(16, 64);
                    expected.add(ordinary(sql, context));
                }
            }
            final int threadCount = 8;
            final int iterations = 30;
            final CyclicBarrier start = new CyclicBarrier(threadCount);
            final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
            final AtomicInteger runs = new AtomicInteger();
            final AtomicInteger earlyCloses = new AtomicInteger();
            final ObjList<Thread> threads = new ObjList<>();
            for (int i = 0; i < threadCount; i++) {
                final int threadIndex = i;
                final Thread thread = new Thread(() -> {
                    final Rnd rnd = new Rnd(threadIndex * 31L + 7, threadIndex * 17L + 3);
                    try (SqlExecutionContextImpl context = newContext(db)) {
                        start.await(60, TimeUnit.SECONDS);
                        for (int n = 0; n < iterations; n++) {
                            final int q = rnd.nextInt(MIXED_QUERIES.length);
                            context.changePageFrameSizes(16, 64);
                            try (RecordCursorFactory factory = db.select(MIXED_QUERIES[q], context)) {
                                if (rnd.nextInt(4) == 0) {
                                    // Stops early: reads a few rows and closes the cursor while its
                                    // tasks are in flight.
                                    try (RecordCursor cursor = factory.getCursor(context)) {
                                        final int stop = rnd.nextInt(200);
                                        for (int r = 0; r < stop && cursor.hasNext(); r++) {
                                            cursor.getRecord();
                                        }
                                    }
                                    earlyCloses.incrementAndGet();
                                }
                                Assert.assertEquals(MIXED_QUERIES[q], expected.getQuick(q), result(factory, context));
                                final AsyncHashJoinRecordCursorFactory join = find(factory);
                                if (join != null) {
                                    Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                                    Assert.assertEquals(0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
                                    Assert.assertEquals(0, join.getAtom().getRing().getAllocatedBytes());
                                }
                                runs.incrementAndGet();
                            }
                        }
                    } catch (Throwable th) {
                        errors.add(th);
                        start.reset();
                    }
                }, "mixed-" + i);
                threads.add(thread);
                thread.start();
            }
            for (int i = 0; i < threadCount; i++) {
                threads.getQuick(i).join();
            }
            for (Throwable th : errors) {
                throw new AssertionError(th);
            }
            Assert.assertEquals(threadCount * iterations, runs.get());
            Assert.assertTrue(earlyCloses.get() > 0);
        }, configuration, LOG));
    }

    /**
     * A cursor of a stacked join, of a swapped stack and of a join with an ON residual reads part of
     * its rows; a writer thread then inserts rows out of order into the probe and build tables,
     * converts a probe partition to Parquet and drops a build partition, and the cursor reads on: it
     * returns the rows of the snapshot it opened, which the ordinary plan read before the writes. A
     * new execution of the same factory then reads the new data, as the ordinary plan does.
     */
    private void assertOpenCursorsKeepTheirSnapshot(boolean isWal) throws Exception {
        assertMemoryLeak(() -> {
            final String wal = isWal ? " WAL" : " BYPASS WAL";
            // The tables of each query take names of their own: t, d1 and d2 followed by the query's
            // index.
            final String[] sqls = {
                    "SELECT t.k, t.v, d1.w, d2.w2 FROM t%1$d t LEFT JOIN d1%1$d d1 ON t.k = d1.k JOIN d2%1$d d2 ON t.l = d2.l",
                    "SELECT d1.w, t.v, d2.w2 FROM d1%1$d d1 JOIN t%1$d t ON d1.k = t.k LEFT JOIN d2%1$d d2 ON d1.w = d2.l",
                    "SELECT t.k, t.v, d1.w FROM t%1$d t LEFT JOIN d1%1$d d1 ON t.k = d1.k AND d1.w > t.v %% 50",
            };
            sqlExecutionContext.setParallelHashJoinEnabled(true);
            for (int q = 0; q < sqls.length; q++) {
                execute("CREATE TABLE t" + q + " AS (SELECT (x % 13)::INT k, (x % 5)::LONG l, x v, timestamp_sequence('2024-01-01', 600_000_000) ts FROM long_sequence(600)) TIMESTAMP(ts) PARTITION BY DAY" + wal);
                execute("CREATE TABLE d1" + q + " AS (SELECT (x % 11)::INT k, x w, timestamp_sequence('2024-01-01', 900_000_000) dts FROM long_sequence(300)) TIMESTAMP(dts) PARTITION BY DAY" + wal);
                execute("CREATE TABLE d2" + q + " AS (SELECT (x % 5)::LONG l, x w2 FROM long_sequence(20))");
                drainWalQueue();
                final int index = q;
                final String sql = String.format(sqls[q], q);
                sqlExecutionContext.changePageFrameSizes(16, 16);
                final String expected = rows(engine, sql, (SqlExecutionContextImpl) sqlExecutionContext, false);
                final CountDownLatch paused = new CountDownLatch(1);
                final CountDownLatch written = new CountDownLatch(1);
                final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
                final Thread writer = new Thread(() -> {
                    try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1)
                            .with(AllowAllSecurityContext.INSTANCE, null, null, -1, null)) {
                        await(paused);
                        engine.execute("INSERT INTO t" + index + " SELECT (x % 13)::INT, (x % 5)::LONG, -x, timestamp_sequence('2024-01-01T00:00:01', 3_000_000_000) FROM long_sequence(40)", context);
                        engine.execute("INSERT INTO d1" + index + " SELECT (x % 11)::INT, -x, timestamp_sequence('2024-01-01T00:00:01', 3_000_000_000) FROM long_sequence(40)", context);
                        engine.execute("INSERT INTO d2" + index + " SELECT (x % 5)::LONG, -x FROM long_sequence(5)", context);
                        engine.execute("ALTER TABLE t" + index + " CONVERT PARTITION TO PARQUET LIST '2024-01-02'", context);
                        engine.execute("ALTER TABLE d1" + index + " DROP PARTITION LIST '2024-01-02'", context);
                    } catch (Throwable th) {
                        errors.add(th);
                    } finally {
                        // The writes took thread-local paths of the writer thread.
                        Path.clearThreadLocals();
                        written.countDown();
                    }
                }, "writer");
                writer.start();
                final StringSink actual = new StringSink();
                try (RecordCursorFactory factory = select(sql)) {
                    Assert.assertNotNull(sql, find(factory));
                    CursorPrinter.println(factory.getMetadata(), actual);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertEquals(25, readRows(cursor, factory.getMetadata(), actual, 25));
                        paused.countDown();
                        await(written);
                        // The WAL tables apply the writes while the cursor is open.
                        drainWalQueue();
                        readRest(cursor, factory.getMetadata(), actual);
                    }
                    writer.join();
                    for (Throwable th : errors) {
                        throw new AssertionError(th);
                    }
                    TestUtils.assertEquals(sql, expected, actual);
                    // A new execution of the same factory reads the new data, as the ordinary plan does.
                    final String after = rows(engine, sql, (SqlExecutionContextImpl) sqlExecutionContext, false);
                    Assert.assertNotEquals(sql, expected, after);
                    final StringSink again = new StringSink();
                    CursorPrinter.println(factory.getMetadata(), again);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        readRest(cursor, factory.getMetadata(), again);
                    }
                    TestUtils.assertEquals(sql, after, again);
                } finally {
                    writer.join();
                }
                Assert.assertEquals(0, engine.getBusyReaderCount());
            }
        });
    }
}
