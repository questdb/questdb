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

package io.questdb.test.griffin.engine.join;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.griffin.engine.functions.test.TestMatchFunctionFactory;
import io.questdb.griffin.engine.join.MaterializedRecordCursorFactory;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.CustomisableRunnable;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Parents that rewind their input, nested loop joins and negative LIMIT, read a stream GROUP BY through a copy,
 * so the aggregation runs once per cursor. Nested loop, splice and serial horizon joins read it through a strict
 * copy, which fails at the memory limit rather than rerun the aggregation, and gives splice and horizon joins random
 * access. The expected rows come from the same aggregation over a table without a designated
 * timestamp, which runs a hash GROUP BY. The test_match() filter under each stream GROUP BY counts the rewinds that
 * reach the aggregation.
 */
@RunWith(Parameterized.class)
public class MaterializedRecordCursorFactoryTest extends AbstractCairoTest {
    // A base whose rows the tests can count with test_latched_counter(), x = 1 to 1000 and y = 2x, and a limit that a
    // copy of it fits well within.
    private static final String LAX_COPY_BASE = "SELECT x, x * 2 y FROM long_sequence(1000) WHERE test_latched_counter()";
    private static final long LAX_COPY_LIMIT = 1_048_576;
    private static final String CROSS_JOIN = "SELECT m.x, s.* FROM m CROSS JOIN ($sub) s ORDER BY x, $keys";
    private static final String FULL_JOIN = "SELECT m.x, s.* FROM m FULL JOIN ($sub) s ON s.s < m.x * 30 ORDER BY x, $keys";
    private static final String HORIZON_JOIN = "SELECT h.offset, count() n, sum(s.s) total, sum(q.x) qx "
            + "FROM (($sub) TIMESTAMP(ts)) s HORIZON JOIN q RANGE FROM 0s TO 1h STEP 30m AS h ORDER BY 1";
    private static final String LEFT_JOIN = "SELECT m.x, s.* FROM m LEFT JOIN ($sub) s ON s.s < m.x * 30 ORDER BY x, $keys";
    private static final String MULTI_HORIZON_JOIN = "SELECT h.offset, count() n, sum(s.s) total, sum(q.x) qx, sum(r.x) rx "
            + "FROM (($sub) TIMESTAMP(ts)) s HORIZON JOIN q HORIZON JOIN q r RANGE FROM 0s TO 1h STEP 30m AS h ORDER BY 1";
    private static final String NEGATIVE_LIMIT = "SELECT * FROM (SELECT * FROM ($sub) LIMIT -10) ORDER BY $keys, c";
    private static final String RIGHT_JOIN = "SELECT m.x, s.* FROM ($sub) s RIGHT JOIN m ON s.s < m.x * 30 ORDER BY x, $keys";
    // The stream subquery, the same aggregation over the table without a designated timestamp, and the keys.
    private static final String[][] SUBQUERIES = {
            {
                    "SELECT ts, count() c, sum(v) s FROM t WHERE test_match() SAMPLE BY 1h",
                    "SELECT timestamp_floor('1h', ts) ts, count() c, sum(v) s FROM t_hash ORDER BY ts",
                    "ts"
            },
            {
                    "SELECT ts, sym, count() c, sum(v) s FROM t WHERE test_match() SAMPLE BY 1h",
                    "SELECT timestamp_floor('1h', ts) ts, sym, count() c, sum(v) s FROM t_hash ORDER BY ts, sym",
                    "ts, sym"
            },
            {
                    "SELECT ts, count() c, sum(v) s FROM t WHERE test_match() GROUP BY ts",
                    "SELECT ts, count() c, sum(v) s FROM t_hash GROUP BY ts ORDER BY ts",
                    "ts"
            },
            {
                    "SELECT ts, sym, count() c, sum(v) s FROM t WHERE test_match() GROUP BY ts, sym",
                    "SELECT ts, sym, count() c, sum(v) s FROM t_hash GROUP BY ts, sym ORDER BY ts, sym",
                    "ts, sym"
            }
    };
    private final int frameRows;
    private final boolean isParallel;
    private final int workerCount;

    public MaterializedRecordCursorFactoryTest(boolean isParallel, int workerCount, int frameRows) {
        this.isParallel = isParallel;
        this.workerCount = workerCount;
        this.frameRows = frameRows;
    }

    @Parameterized.Parameters(name = "parallel={0}, workers={1}, frameRows={2}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {false, 1, 17},
                {true, 0, 17},
                {true, 4, 17}
        });
    }

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, Boolean.toString(isParallel));
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, frameRows);
        super.setUp();
    }

    @Test
    public void testConstructorFailureClosesBase() throws Exception {
        run((engine, compiler, context) -> {
            final RuntimeException failure = new RuntimeException("injected metadata failure");
            final int[] closeCount = {0};
            try (RecordCursorFactory delegate = compiler.compile("SELECT x FROM long_sequence(1)", context).getRecordCursorFactory()) {
                final GenericRecordMetadata metadata = new GenericRecordMetadata() {
                    @Override
                    public int getColumnType(int columnIndex) {
                        throw failure;
                    }
                };
                GenericRecordMetadata.copyColumns(delegate.getMetadata(), metadata);
                final RecordCursorFactory base = new RecordCursorFactory() {
                    @Override
                    public void close() {
                        closeCount[0]++;
                        delegate.close();
                    }

                    @Override
                    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
                        return delegate.getCursor(executionContext);
                    }

                    @Override
                    public RecordMetadata getMetadata() {
                        return metadata;
                    }

                    @Override
                    public boolean recordCursorSupportsRandomAccess() {
                        return false;
                    }
                };
                // RecordArray reads column types during construction, after the wrapper adopts base.
                // No rows reach the sink: this tests consume-on-entry, including constructor failure.
                try (RecordCursorFactory ignored = new MaterializedRecordCursorFactory(configuration, base, null, true, true, false)) {
                    Assert.fail("expected metadata failure");
                } catch (RuntimeException e) {
                    Assert.assertSame(failure, e);
                }
                Assert.assertEquals(1, closeCount[0]);
            }
        });
    }

    @Test
    public void testCopiesInBatches() throws Exception {
        run((engine, compiler, context) -> {
            final LongList rowIds = new LongList();
            for (int rowCount : new int[]{0, 1, 255, 256, 257, 600}) {
                for (boolean isCopiedInBatches : new boolean[]{false, true}) {
                    final String label = rowCount + " rows, batches: " + isCopiedInBatches;
                    try (
                            RecordCursorFactory copy = copy(compiler, context, "SELECT x FROM long_sequence(" + rowCount + ")", true, isCopiedInBatches);
                            RecordCursor cursor = copy.getCursor(context)
                    ) {
                        final Record record = cursor.getRecord();
                        rowIds.clear();
                        for (int i = 1; i <= rowCount; i++) {
                            Assert.assertTrue(label, cursor.hasNext());
                            Assert.assertEquals(label, i, record.getLong(0));
                            Assert.assertEquals(label, isCopiedInBatches ? Math.min(rowCount, (i + 255) / 256 * 256) : i, cursor.preComputedStateSize());
                            rowIds.add(record.getRowId());
                        }
                        Assert.assertFalse(label, cursor.hasNext());
                        Assert.assertEquals(label, rowCount, cursor.preComputedStateSize());

                        cursor.toTop();
                        final Record recordB = cursor.getRecordB();
                        for (int i = 1; i <= rowCount; i++) {
                            Assert.assertTrue(label, cursor.hasNext());
                            Assert.assertEquals(label, i, record.getLong(0));
                            cursor.recordAt(recordB, rowIds.getQuick(rowCount - i));
                            Assert.assertEquals(label, rowCount - i + 1, recordB.getLong(0));
                        }
                        Assert.assertFalse(label, cursor.hasNext());
                        Assert.assertEquals(label, rowCount, cursor.preComputedStateSize());
                    }
                }
            }
        });
    }

    @Test
    public void testCopiesInBatchesUntilMemoryLimit() throws Exception {
        // The 24-byte rows reach the limit inside a batch rather than on a batch boundary.
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_LIGHT_VALUE_PAGE_SIZE, 256);
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_MEMORY_LIMIT_BYTES, 10_000);
        run((engine, compiler, context) -> {
            try (RecordCursorFactory copy = copy(compiler, context, "SELECT x, x * 2 y, x * 3 z FROM long_sequence(1_000)", true, true)) {
                long rowCount = 0;
                try (MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(context.getSecurityContext(), 42, MemoryTrackerWorkload.MAT_VIEW_REFRESH)) {
                    context.setMemoryTracker(tracker);
                    try (RecordCursor cursor = copy.getCursor(context)) {
                        final Record record = cursor.getRecord();
                        while (cursor.hasNext()) {
                            Assert.assertEquals(++rowCount, record.getLong(0));
                        }
                        Assert.fail("expected memory limit breach");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded [workload=MAT_VIEW_REFRESH");
                    } finally {
                        context.setMemoryTracker(null);
                    }
                    Assert.assertEquals(0, tracker.getUsed());
                }
                // The breach fails the batch that holds it, after the parent read every earlier batch.
                Assert.assertTrue(rowCount > 0);
                Assert.assertEquals(0, rowCount % 256);
                try (RecordCursor cursor = copy.getCursor(context)) {
                    final Record record = cursor.getRecord();
                    for (int i = 1; i <= 1_000; i++) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(i, record.getLong(0));
                    }
                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testCrossJoin() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            assertMaterialized(engine, context, CROSS_JOIN, null, true, false);
            // count() sizes the join, which sizes the copy, rewinds it and sizes it again.
            assertMaterialized(engine, context, "SELECT count() FROM m CROSS JOIN ($sub)", null, false, true);
        });
    }

    @Test
    public void testCrossJoinOverNegativeLimit() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            final String[] subquery = withNegativeLimit(SUBQUERIES[1]);
            Assert.assertEquals(0, assertMaterialized(engine, context, CROSS_JOIN, subquery, null, true, true));
            // The copy under the limit can give up under the memory limit and stream its base, so the join keeps its
            // own copy of the limit's rows.
            final String plan = TestUtils.printSqlToString(
                    engine,
                    context,
                    "EXPLAIN " + CROSS_JOIN.replace("$sub", subquery[0]).replace("$keys", subquery[2]),
                    new StringSink()
            );
            Assert.assertTrue(plan, plan.matches("(?s).*\n( *)Materialize\n\\1  +Limit .*\n( *)Materialize\n.*"));
        });
    }

    @Test
    public void testCrossJoinCopiesNegativeLimitThatStreams() throws Exception {
        // Under the query memory limit, the copy under LIMIT -100 gives up and streams its base, so each rewind runs
        // the stream GROUP BY again. The join keeps its own copy of the limit's rows, so the stream runs as often
        // under one outer row as under five, and every outer row joins the same 100 rows. Small pages let the
        // copy of 100 rows fit the limit that the copy of 10,000 rows breaches. Workers allocate frame results against
        // the same limit meanwhile, and one that reaches it frees the copy rather than fail the query.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 65_536);
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 4096);
        run((engine, compiler, context) -> {
            createTables(engine, context);
            engine.execute(
                    """
                            CREATE TABLE big AS (
                                SELECT timestamp_sequence('2024-01-01', 1_000_000L) ts, x v FROM long_sequence(10_000)
                            ) TIMESTAMP(ts) PARTITION BY DAY""",
                    context
            );
            final String sql = "SELECT count() n, sum(s.c) c, sum(s.s) s FROM %s o CROSS JOIN "
                    + "(SELECT ts, count() c, sum(v) s FROM big WHERE test_latched_counter() GROUP BY ts LIMIT -100) s";
            try (MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(context.getSecurityContext(), 42, MemoryTrackerWorkload.QUERY)) {
                context.setMemoryTracker(tracker);
                try {
                    // The last 100 buckets hold one row each, v = 9_901 to 10_000.
                    final int oneRowReads = countFilterCalls(engine, context, sql.formatted("(SELECT * FROM m WHERE x = 1)"), """
                            n\tc\ts
                            100\t100\t995050
                            """);
                    final int fiveRowReads = countFilterCalls(engine, context, sql.formatted("m"), """
                            n\tc\ts
                            500\t500\t4975250
                            """);
                    Assert.assertEquals(oneRowReads, fiveRowReads);
                    Assert.assertEquals(0, tracker.getUsed());
                } finally {
                    context.setMemoryTracker(null);
                    TestLatchedCounterFunctionFactory.reset(null);
                }
            }
        });
    }

    @Test
    public void testLaxCopyCountsRowsInEveryState() throws Exception {
        // calculateSize() and skipRows() copy the base rows they pass, replay the copied ones, and stream once the copy
        // is gone.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_MEMORY_LIMIT_BYTES, LAX_COPY_LIMIT);
        run((engine, compiler, context) -> withLaxCopy(engine, compiler, context, LAX_COPY_BASE, (tracker, cursor) -> {
            final RecordCursor.Counter counter = new RecordCursor.Counter();
            cursor.calculateSize(null, counter);
            Assert.assertEquals(1000, counter.get());
            Assert.assertEquals(1000, TestLatchedCounterFunctionFactory.getCount());
            // Replays the copy, which holds every row.
            cursor.toTop();
            counter.clear();
            cursor.calculateSize(null, counter);
            Assert.assertEquals(1000, counter.get());
            Assert.assertEquals(1000, TestLatchedCounterFunctionFactory.getCount());
            cursor.toTop();
            counter.set(10);
            cursor.skipRows(counter, Long.MAX_VALUE);
            Assert.assertEquals(0, counter.get());
            assertLaxRows(cursor, 990, 11);
            Assert.assertEquals(1000, TestLatchedCounterFunctionFactory.getCount());
        }));
        run((engine, compiler, context) -> withLaxCopy(engine, compiler, context, LAX_COPY_BASE, (tracker, cursor) -> {
            // A rewind in the middle of the first pass, then a count that replays 100 rows and copies the rest.
            assertLaxRows(cursor, 100, 1);
            cursor.toTop();
            final RecordCursor.Counter counter = new RecordCursor.Counter();
            cursor.calculateSize(null, counter);
            Assert.assertEquals(1000, counter.get());
            Assert.assertEquals(1000, TestLatchedCounterFunctionFactory.getCount());
            // A count that copies until an allocation frees the copy, then streams.
            cursor.toTop();
            counter.clear();
            counter.set(500);
            cursor.skipRows(counter, Long.MAX_VALUE);
            Assert.assertEquals(0, counter.get());
            Assert.assertEquals(1000, TestLatchedCounterFunctionFactory.getCount());
        }));
        run((engine, compiler, context) -> withLaxCopy(engine, compiler, context, LAX_COPY_BASE, (tracker, cursor) -> {
            assertLaxRows(cursor, 100, 1);
            final long size = LAX_COPY_LIMIT - tracker.getUsed() + 1;
            final long ptr = allocate(tracker, size, false);
            try {
                final RecordCursor.Counter counter = new RecordCursor.Counter();
                cursor.calculateSize(null, counter);
                Assert.assertEquals(900, counter.get());
                cursor.toTop();
                counter.set(990);
                cursor.skipRows(counter, Long.MAX_VALUE);
                Assert.assertEquals(0, counter.get());
                assertLaxRows(cursor, 10, 991);
                Assert.assertFalse(cursor.hasNext());
                Assert.assertEquals(size, tracker.getUsed());
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT, tracker);
            }
        }));
        run((engine, compiler, context) -> withLaxCopy(engine, compiler, context, LAX_COPY_BASE, (tracker, cursor) -> {
            // Skipping past the end, on the first pass and on a replay, leaves the rows it could not skip.
            final RecordCursor.Counter counter = new RecordCursor.Counter();
            counter.set(1500);
            cursor.skipRows(counter, Long.MAX_VALUE);
            Assert.assertEquals(500, counter.get());
            cursor.toTop();
            counter.set(1200);
            cursor.skipRows(counter, Long.MAX_VALUE);
            Assert.assertEquals(200, counter.get());
            Assert.assertEquals(1000, TestLatchedCounterFunctionFactory.getCount());
        }));
    }

    @Test
    public void testLaxCopyFreedByAllocationOnAnotherThread() throws Exception {
        assertLaxCopyFreedByAllocation(true);
    }

    @Test
    public void testLaxCopyFreedByAllocationOnOwnerThread() throws Exception {
        assertLaxCopyFreedByAllocation(false);
    }

    @Test
    public void testLaxCopyFreedOnceRowIsCopied() throws Exception {
        // A thread that frees the copy while the owner copies a row waits for the row, then frees the copy. The probe
        // reclaimer, which the tracker asks before the copy, tells the owner that the thread is about to wait.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_MEMORY_LIMIT_BYTES, LAX_COPY_LIMIT);
        run((engine, compiler, context) -> {
            final CountDownLatch isAsked = new CountDownLatch(1);
            final MemoryTracker.Reclaimer probe = isAsked::countDown;
            final AtomicLong size = new AtomicLong();
            final AtomicReference<Thread> allocator = new AtomicReference<>();
            final AtomicLong ptr = new AtomicLong();
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final AtomicReference<MemoryTracker> trackerRef = new AtomicReference<>();
            // The copy reads the flag column as it copies row 50, under its lock.
            TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
                @Override
                public boolean onGet(Record rec, int count) {
                    if (rec.getLong(0) == 50 && allocator.get() == null) {
                        final MemoryTracker tracker = trackerRef.get();
                        size.set(LAX_COPY_LIMIT - tracker.getUsed() + 1);
                        final Thread thread = new Thread(() -> {
                            try {
                                ptr.set(Unsafe.malloc(size.get(), MemoryTag.NATIVE_DEFAULT, tracker));
                            } catch (Throwable th) {
                                failure.set(th);
                            }
                        });
                        allocator.set(thread);
                        thread.start();
                        try {
                            Assert.assertTrue(isAsked.await(30, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                    }
                    return true;
                }
            });
            try (
                    MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(context.getSecurityContext(), 42, MemoryTrackerWorkload.MAT_VIEW_REFRESH);
                    RecordCursorFactory copy = copy(compiler, context, "SELECT x, test_latched_counter() flag FROM long_sequence(1000)", false, false)
            ) {
                trackerRef.set(tracker);
                context.setMemoryTracker(tracker);
                Assert.assertTrue(tracker.addReclaimer(probe));
                try (RecordCursor cursor = copy.getCursor(context)) {
                    final Record record = cursor.getRecord();
                    for (int x = 1; x <= 1000; x++) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(x, record.getLong(0));
                    }
                    Assert.assertFalse(cursor.hasNext());
                    allocator.get().join();
                    Assert.assertNull(failure.get());
                    // The thread freed the copy, so the cursor streamed the rest of the base.
                    Assert.assertEquals(size.get(), tracker.getUsed());
                    Assert.assertEquals(1, tracker.getReclaimerCount());
                } finally {
                    tracker.removeReclaimer(probe);
                    context.setMemoryTracker(null);
                    TestLatchedCounterFunctionFactory.reset(null);
                    if (ptr.get() != 0) {
                        Unsafe.free(ptr.get(), size.get(), MemoryTag.NATIVE_DEFAULT, tracker);
                    }
                }
                Assert.assertEquals(0, tracker.getReclaimerCount());
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testLaxCopyReplayedOnceComplete() throws Exception {
        // Once the copy holds every base row, a rewind replays it, and no allocation frees it.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_MEMORY_LIMIT_BYTES, LAX_COPY_LIMIT);
        run((engine, compiler, context) -> withLaxCopy(engine, compiler, context, LAX_COPY_BASE, (tracker, cursor) -> {
            assertLaxRows(cursor, 1000, 1);
            Assert.assertFalse(cursor.hasNext());
            Assert.assertEquals(0, tracker.getReclaimerCount());
            final long copyBytes = tracker.getUsed();
            try {
                allocate(tracker, LAX_COPY_LIMIT - copyBytes + 1, true);
                Assert.fail("expected the copy to keep its memory");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
            }
            cursor.toTop();
            assertLaxRows(cursor, 1000, 1);
            Assert.assertFalse(cursor.hasNext());
            Assert.assertEquals(1000, TestLatchedCounterFunctionFactory.getCount());
            Assert.assertEquals(copyBytes, tracker.getUsed());
        }));
    }

    @Test
    public void testLaxCopyHasNoRandomAccess() throws Exception {
        // A lax copy may stream its base, so it gives no random access. Its state size is the copy's while it copies,
        // and the base's once it streams.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_MEMORY_LIMIT_BYTES, LAX_COPY_LIMIT);
        run((engine, compiler, context) -> withLaxCopy(engine, compiler, context, LAX_COPY_BASE, (tracker, cursor) -> {
            Assert.assertThrows(UnsupportedOperationException.class, cursor::getRecordB);
            Assert.assertThrows(UnsupportedOperationException.class, () -> cursor.recordAt(cursor.getRecord(), 0));
            assertLaxRows(cursor, 100, 1);
            Assert.assertEquals(100, cursor.preComputedStateSize());
            final long size = LAX_COPY_LIMIT - tracker.getUsed() + 1;
            final long ptr = allocate(tracker, size, false);
            try {
                Assert.assertEquals(0, cursor.preComputedStateSize());
                // Another allocation past the limit finds the copy gone, and fails.
                try {
                    allocate(tracker, LAX_COPY_LIMIT - size + 1, true);
                    Assert.fail("expected the per-query limit to reject the allocation");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                }
                assertLaxRows(cursor, 900, 101);
                Assert.assertFalse(cursor.hasNext());
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT, tracker);
            }
        }));
    }

    @Test
    public void testLaxCopyKeepsCopyWhenTrackerHoldsNoMoreReclaimers() throws Exception {
        // A tracker that holds as many reclaimers as it can takes no more: the copy then counts as any memory, and an
        // allocation past the limit fails rather than free it.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_MEMORY_LIMIT_BYTES, LAX_COPY_LIMIT);
        run((engine, compiler, context) -> {
            TestLatchedCounterFunctionFactory.reset(null);
            final ObjList<MemoryTracker.Reclaimer> probes = new ObjList<>();
            try (
                    MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(context.getSecurityContext(), 42, MemoryTrackerWorkload.MAT_VIEW_REFRESH);
                    RecordCursorFactory copy = copy(compiler, context, LAX_COPY_BASE, false, false)
            ) {
                for (int i = 0; i < MemoryTracker.MAX_RECLAIMER_COUNT; i++) {
                    final MemoryTracker.Reclaimer probe = () -> {
                    };
                    probes.add(probe);
                    Assert.assertTrue(tracker.addReclaimer(probe));
                }
                context.setMemoryTracker(tracker);
                try (RecordCursor cursor = copy.getCursor(context)) {
                    assertLaxRows(cursor, 100, 1);
                    Assert.assertEquals(MemoryTracker.MAX_RECLAIMER_COUNT, tracker.getReclaimerCount());
                    try {
                        allocate(tracker, LAX_COPY_LIMIT - tracker.getUsed() + 1, true);
                        Assert.fail("expected the copy to keep its memory");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                    }
                    assertLaxRows(cursor, 900, 101);
                    cursor.toTop();
                    assertLaxRows(cursor, 1000, 1);
                    Assert.assertEquals(1000, TestLatchedCounterFunctionFactory.getCount());
                } finally {
                    context.setMemoryTracker(null);
                    TestLatchedCounterFunctionFactory.reset(null);
                    for (int i = 0, n = probes.size(); i < n; i++) {
                        tracker.removeReclaimer(probes.getQuick(i));
                    }
                }
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testLaxCopyRerunsRecomputingBase() throws Exception {
        // A lax copy streams its base once it gives up, so a rewind reruns a stream GROUP BY under it, but not a table.
        run((engine, compiler, context) -> {
            createTables(engine, context);
            for (boolean isStrict : new boolean[]{true, false}) {
                try (RecordCursorFactory copy = copy(compiler, context, "SELECT ts, sym, sum(v) s FROM t SAMPLE BY 1h", isStrict, false)) {
                    Assert.assertTrue(copy.getBaseFactory().isRewindRecomputing());
                    Assert.assertEquals(!isStrict, copy.isRewindRecomputing());
                }
                try (RecordCursorFactory copy = copy(compiler, context, "SELECT * FROM t", isStrict, false)) {
                    Assert.assertFalse(copy.getBaseFactory().isRewindRecomputing());
                    Assert.assertFalse(copy.isRewindRecomputing());
                }
            }
        });
    }

    @Test
    public void testLaxCopyRewoundMidwayReplaysThenCopiesOn() throws Exception {
        // A rewind in the middle of the first pass replays the rows copied so far, which no allocation frees meanwhile,
        // and then copies the base from where it stopped. An allocation then frees the copy, and the next rewind runs
        // the base again.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_MEMORY_LIMIT_BYTES, LAX_COPY_LIMIT);
        run((engine, compiler, context) -> withLaxCopy(engine, compiler, context, LAX_COPY_BASE, (tracker, cursor) -> {
            assertLaxRows(cursor, 100, 1);
            cursor.toTop();
            Assert.assertEquals(0, tracker.getReclaimerCount());
            assertLaxRows(cursor, 100, 1);
            Assert.assertEquals(100, TestLatchedCounterFunctionFactory.getCount());
            assertLaxRows(cursor, 100, 101);
            Assert.assertEquals(200, TestLatchedCounterFunctionFactory.getCount());
            Assert.assertEquals(1, tracker.getReclaimerCount());
            final long size = LAX_COPY_LIMIT - tracker.getUsed() + 1;
            final long ptr = allocate(tracker, size, true);
            try {
                assertLaxRows(cursor, 800, 201);
                Assert.assertFalse(cursor.hasNext());
                cursor.toTop();
                assertLaxRows(cursor, 1000, 1);
                Assert.assertEquals(2000, TestLatchedCounterFunctionFactory.getCount());
                Assert.assertEquals(size, tracker.getUsed());
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT, tracker);
            }
        }));
    }

    @Test
    public void testLaxCopyWithoutTrackerReplays() throws Exception {
        // In a context without a memory tracker, as SqlExecutionContext has by default, nothing can free the copy, and
        // a rewind replays it.
        run((engine, compiler, context) -> {
            TestLatchedCounterFunctionFactory.reset(null);
            try (
                    SqlExecutionContextImpl untracked = new SqlExecutionContextImpl(engine, 1) {
                        @Override
                        public MemoryTracker getMemoryTracker() {
                            return null;
                        }

                        @Override
                        public void setMemoryTracker(MemoryTracker tracker) {
                        }
                    }.with(context.getSecurityContext());
                    RecordCursorFactory copy = copy(compiler, untracked, LAX_COPY_BASE, false, false);
                    RecordCursor cursor = copy.getCursor(untracked)
            ) {
                assertLaxRows(cursor, 1000, 1);
                Assert.assertFalse(cursor.hasNext());
                cursor.toTop();
                assertLaxRows(cursor, 1000, 1);
                Assert.assertEquals(1000, TestLatchedCounterFunctionFactory.getCount());
            } finally {
                TestLatchedCounterFunctionFactory.reset(null);
            }
        });
    }

    @Test
    public void testFullJoin() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            assertMaterialized(engine, context, FULL_JOIN, null, true, false);
        });
    }

    @Test
    public void testHashJoinCopiesStreamInBatches() throws Exception {
        run((engine, compiler, context) -> {
            // The 1_000 rows of the aggregation fill three batches and part of a fourth.
            engine.execute(
                    """
                            CREATE TABLE b AS (
                                SELECT timestamp_sequence('2024-01-01', 1_000_000L) ts, x v FROM long_sequence(1_000)
                            ) TIMESTAMP(ts) PARTITION BY DAY""",
                    context
            );
            engine.execute("CREATE TABLE b_hash (ts TIMESTAMP, v LONG)", context);
            engine.execute("INSERT INTO b_hash SELECT * FROM b", context);
            engine.execute("CREATE TABLE h AS (SELECT timestamp_sequence('2024-01-01', 3_000_000L) ts, x FROM long_sequence(400))", context);
            final String[] subquery = {
                    "SELECT ts, count() c, sum(v) s FROM b WHERE test_match() GROUP BY ts",
                    "SELECT ts, count() c, sum(v) s FROM b_hash GROUP BY ts ORDER BY ts",
                    "ts"
            };
            for (String join : new String[]{"JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"}) {
                final String query = "SELECT h.x, s.* FROM h " + join + " ($sub) s ON h.ts = s.ts ORDER BY x, $keys";
                Assert.assertEquals(join, 0, assertMaterialized(engine, context, query, subquery, null, true, false));
            }
        });
    }

    @Test
    public void testHashJoinSizeDoesNotRewindStreamMaster() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            final String sql = "SELECT a.ts, a.sym, a.c, b.s FROM (" + SUBQUERIES[1][0] + ") a JOIN "
                    + "(SELECT ts, v s FROM t UNION ALL SELECT ts, v s FROM t) b ON (ts)";
            assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck().assertsPlanContaining("Hash Join\n");
            try (
                    RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory();
                    RecordCursor cursor = factory.getCursor(context)
            ) {
                while (cursor.hasNext()) {
                    // drain the cursor
                }
                final int topCount = TestMatchFunctionFactory.getTopCounter();
                Assert.assertEquals(-1, cursor.size());
                Assert.assertEquals(topCount, TestMatchFunctionFactory.getTopCounter());
            }
        });
    }

    @Test
    public void testHashJoinValuePageOfOneByte() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT timestamp_sequence('2024-01-01', 60_000_000L) ts, (x % 2)::VARCHAR s, x v FROM long_sequence(200)) TIMESTAMP(ts) PARTITION BY DAY");
            assertQuery("SELECT ts, s, sum(v) sv FROM t SAMPLE BY 1h ORDER BY ts, s LIMIT -2")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\ts\tsv
                            2024-01-01T03:00:00.000000Z\t0\t1910
                            2024-01-01T03:00:00.000000Z\t1\t1900
                            """);
            assertQuery("SELECT count(), sum(sv) FROM long_sequence(3) a CROSS JOIN (SELECT ts, s, sum(v) sv FROM t SAMPLE BY 1h) b")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            24\t60300
                            """);
        });
    }

    @Test
    public void testHashJoinReadsStreamThroughCopy() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            engine.execute("CREATE TABLE h AS (SELECT timestamp_sequence('2024-01-01', 3_600_000_000L) ts, x FROM long_sequence(5))", context);
            for (String[] join : new String[][]{
                    {"JOIN", "Hash Join Light"},
                    {"LEFT JOIN", "Hash Left Outer Join Light"},
                    {"RIGHT JOIN", "Hash Right Outer Join Light"},
                    {"FULL JOIN", "Hash Full Outer Join Light"}
            }) {
                final String query = "SELECT h.x, s.* FROM h " + join[0] + " ($sub) s ON h.ts = s.ts ORDER BY x, $keys";
                assertMaterialized(engine, context, query, null, true, false);
                assertQuery(query.replace("$sub", SUBQUERIES[0][0]).replace("$keys", SUBQUERIES[0][2]))
                        .withEngine(engine)
                        .withContext(context)
                        .noLeakCheck()
                        .assertsPlanContaining(join[1]);
            }
            for (String right : new String[]{
                    "SELECT ts, c + 1 c FROM (SELECT ts, count() c FROM t SAMPLE BY 1h)",
                    "SELECT * FROM (SELECT ts, count() c FROM t SAMPLE BY 1h) WHERE c > 1",
                    "SELECT ts, count() c FROM t SAMPLE BY 1h LIMIT 3"
            }) {
                assertQuery("SELECT h.x, s.* FROM h JOIN (" + right + ") s ON h.ts = s.ts")
                        .withEngine(engine)
                        .withContext(context)
                        .noLeakCheck()
                        .assertsPlanContaining("Hash Join Light", "Materialize", "Stream Group By");
            }
            for (String right : new String[]{
                    "SELECT ts, count() c FROM t SAMPLE BY 1h FILL(PREV)",
                    "SELECT ts, count() c FROM t SAMPLE BY 1h UNION ALL SELECT ts, count() c FROM t SAMPLE BY 1d",
                    "SELECT ts, c, row_number() OVER () n FROM (SELECT ts, count() c FROM t SAMPLE BY 1h)"
            }) {
                assertQuery("SELECT h.x, s.* FROM h JOIN (" + right + ") s ON h.ts = s.ts")
                        .withEngine(engine)
                        .withContext(context)
                        .noLeakCheck()
                        .assertsPlanContaining("Hash Join\n");
            }
        });
    }

    @Test
    public void testHorizonJoin() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            assertMaterialized(engine, context, HORIZON_JOIN, null, true, true);
            assertMaterialized(engine, context, MULTI_HORIZON_JOIN, null, true, true);
            for (String[] subquery : SUBQUERIES) {
                Assert.assertEquals(0, assertMaterialized(engine, context, HORIZON_JOIN, withNegativeLimit(subquery), null, true, true));
            }
        });
    }

    @Test
    public void testInputsHiddenFromBaseFactory() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            final String stream = SUBQUERIES[1][0];
            final String half = "SELECT * FROM (SELECT ts, sym, count() c, sum(v) s FROM t WHERE test_match() AND ts %s '2024-01-01T12' SAMPLE BY 1h)";
            final String halves = half.formatted("<") + " UNION ALL " + half.formatted(">=");
            final String window = "SELECT a.*, count() n FROM (" + stream + ") a WINDOW JOIN t b %s RANGE BETWEEN 1 hour PRECEDING AND CURRENT ROW";
            final String[][] inputs = {
                    {"Union All", halves},
                    {"Except", "SELECT * FROM (" + stream + ") EXCEPT SELECT * FROM (" + stream + ") WHERE c < 0"},
                    {"Hash Join", "SELECT a.* FROM (" + stream + ") a JOIN (SELECT DISTINCT sym FROM t) b ON (sym)"},
                    {"Window Fast Join", window.formatted("ON (sym)")},
                    {"Window Join", window.formatted("")},
                    {"Unnest", "SELECT a.ts, a.sym, a.c, a.s, u.e FROM (SELECT *, ARRAY[1.0] arr FROM (" + stream + ")) a, UNNEST(a.arr) u(e)"}
            };
            for (String[] input : inputs) {
                for (String query : new String[]{CROSS_JOIN, LEFT_JOIN, RIGHT_JOIN, FULL_JOIN, NEGATIVE_LIMIT}) {
                    assertStreamReadOnce(engine, compiler, context, query, input[1], input[0]);
                }
            }
            assertStreamReadOnce(engine, compiler, context, NEGATIVE_LIMIT, "SELECT * FROM (" + halves + ") ORDER BY ts", "Union All Merge");

            final String hashed = "SELECT * FROM (" + SUBQUERIES[1][1] + ") EXCEPT SELECT * FROM (" + stream + ")";
            final String sql = CROSS_JOIN.replace("$sub", hashed).replace("$keys", SUBQUERIES[1][2]);
            assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck().assertsPlanNotContaining("Materialize");
            Assert.assertEquals(countRewinds(compiler, context, hashed), countRewinds(compiler, context, sql));
        });
    }

    @Test
    public void testJoinBudgetBoundsOnlyJoinCopies() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 256);
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_MAX_PAGES, 1);
        run((engine, compiler, context) -> {
            createTables(engine, context);
            for (String query : new String[]{CROSS_JOIN, LEFT_JOIN, RIGHT_JOIN, FULL_JOIN}) {
                for (String[] subquery : SUBQUERIES) {
                    Assert.assertEquals(query, 0, assertMaterialized(engine, context, query, subquery, null, true, false));
                }
            }
            assertQuery("SELECT m.x, s.* FROM m FULL JOIN (SELECT * FROM t_hash UNION ALL SELECT * FROM t_hash) s ON s.v < m.x")
                    .withEngine(engine)
                    .withContext(context)
                    .noLeakCheck()
                    .failsWith("materialized rows exceed the memory limit [maxBytes=256] (raise cairo.sql.hash.join.value.max.pages)");
        });
    }

    @Test
    public void testLeftJoin() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            assertMaterialized(engine, context, LEFT_JOIN, null, true, false);
        });
    }

    @Test
    public void testMemoryLimit() throws Exception {
        // The aggregation fits in the limit and a copy of its 10_000 rows does not.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_MEMORY_LIMIT_BYTES, 65_536);
        run((engine, compiler, context) -> {
            createTables(engine, context);
            engine.execute(
                    """
                            CREATE TABLE big AS (
                                SELECT timestamp_sequence('2024-01-01', 1_000_000L) ts, x v FROM long_sequence(10_000)
                            ) TIMESTAMP(ts) PARTITION BY DAY""",
                    context
            );
            final String sub = "SELECT ts, count() c, sum(v) s FROM big WHERE test_match() GROUP BY ts";
            final String crossJoin = "SELECT count() FROM m CROSS JOIN (" + sub + ")";
            final int topCount = TestMatchFunctionFactory.getTopCounter();
            try (MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(context.getSecurityContext(), 42, MemoryTrackerWorkload.MAT_VIEW_REFRESH)) {
                context.setMemoryTracker(tracker);
                try {
                    for (String join : new String[]{
                            crossJoin,
                            "SELECT m.x, s.* FROM m LEFT JOIN (" + sub + ") s ON s.s < m.x",
                            "SELECT m.x, s.* FROM (" + sub + ") s RIGHT JOIN m ON s.s < m.x",
                            "SELECT m.x, s.* FROM m FULL JOIN (" + sub + ") s ON s.s < m.x"
                    }) {
                        assertQuery(join)
                                .withEngine(engine)
                                .withContext(context)
                                .noLeakCheck()
                                .failsWith("query memory limit exceeded [workload=MAT_VIEW_REFRESH");
                        Assert.assertEquals(join, 0, tracker.getUsed());
                    }
                } finally {
                    context.setMemoryTracker(null);
                }
            }
            Assert.assertEquals(topCount, TestMatchFunctionFactory.getTopCounter());
            assertQuery(crossJoin)
                    .withEngine(engine)
                    .withContext(context)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Materialize")
                    .returns("count\n50000\n");
            Assert.assertEquals(topCount, TestMatchFunctionFactory.getTopCounter());
        });
    }

    @Test
    public void testMemoryLimitStreamsExactRows() throws Exception {
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 65_536);
        run((engine, compiler, context) -> {
            createTables(engine, context);
            engine.execute(
                    """
                            CREATE TABLE big AS (
                                SELECT timestamp_sequence('2024-01-01', 1_000_000L) ts, x v FROM long_sequence(10_000)
                            ) TIMESTAMP(ts) PARTITION BY DAY""",
                    context
            );
            final String sub = "SELECT ts, count() c, sum(v) s FROM big GROUP BY ts";
            try (MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(context.getSecurityContext(), 42, MemoryTrackerWorkload.QUERY)) {
                context.setMemoryTracker(tracker);
                try {
                    assertQuery("SELECT count() n, sum(s.c) c, sum(s.s) s, min(s.ts) lo, max(s.ts) hi, "
                            + "sum(s.ts::LONG - 1_704_067_200_000_000) offsets FROM m CROSS JOIN (" + sub + ") s")
                            .withEngine(engine)
                            .withContext(context)
                            .noLeakCheck()
                            .failsWith("query memory limit exceeded [workload=QUERY");
                    Assert.assertEquals(0, tracker.getUsed());
                    assertQuery("SELECT count() n, sum(c) c, sum(s) s, min(ts) lo, max(ts) hi FROM (" + sub + " ORDER BY ts LIMIT -9000)")
                            .withEngine(engine)
                            .withContext(context)
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .withPlanContaining("Materialize")
                            .returns("""
                                    n\tc\ts\tlo\thi
                                    9000\t9000\t49504500\t2024-01-01T00:16:40.000000Z\t2024-01-01T02:46:39.000000Z
                                    """);
                    Assert.assertEquals(0, tracker.getUsed());
                } finally {
                    context.setMemoryTracker(null);
                }
            }
        });
    }

    @Test
    public void testMemoryTrackerBalanced() throws Exception {
        // Copies of stream GROUP BY rows ignore the 1024-byte join budget, and every join balances the tracker.
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 256);
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_MAX_PAGES, 4);
        run((engine, compiler, context) -> {
            createTables(engine, context);
            for (String query : new String[]{CROSS_JOIN, LEFT_JOIN, RIGHT_JOIN, FULL_JOIN}) {
                for (String[] subquery : SUBQUERIES) {
                    final String sql = query.replace("$sub", subquery[0]).replace("$keys", subquery[2]);
                    try (
                            RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory();
                            // QueryRegistry replaces a QUERY tracker with its own, but keeps a refresh tracker.
                            MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(context.getSecurityContext(), 42, MemoryTrackerWorkload.MAT_VIEW_REFRESH)
                    ) {
                        context.setMemoryTracker(tracker);
                        try {
                            for (int i = 0; i < 2; i++) {
                                try (RecordCursor cursor = factory.getCursor(context)) {
                                    long peak = 0;
                                    while (cursor.hasNext()) {
                                        peak = Math.max(peak, tracker.getUsed());
                                    }
                                    Assert.assertTrue(sql, peak > 0);
                                } catch (CairoException e) {
                                    Assert.assertSame(sql, FULL_JOIN, query);
                                    TestUtils.assertContains(e.getFlyweightMessage(), "materialized rows exceed the memory limit");
                                }
                                Assert.assertEquals(sql, 0, tracker.getUsed());
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
    public void testNegativeLimit() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            context.getBindVariableService().setLong(0, -6);
            for (String limit : new String[]{"-10", "-10, -4", "4, -6", "$1"}) {
                assertMaterialized(
                        engine,
                        context,
                        "SELECT * FROM (SELECT * FROM ($sub) LIMIT " + limit + ") ORDER BY $keys, c",
                        "ts",
                        true,
                        true
                );
            }
            // A constant non-negative limit never rewinds the stream after reading it.
            assertQuery("SELECT * FROM (SELECT ts, count() c FROM t SAMPLE BY 1h) LIMIT 4, 6")
                    .withEngine(engine)
                    .withContext(context)
                    .noLeakCheck()
                    .assertsPlanNotContaining("Materialize");
        });
    }

    @Test
    public void testSmallestLightValuePage() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_LIGHT_VALUE_PAGE_SIZE, 12);
        assertCopiesVarSizeRows();
    }

    @Test
    public void testSmallestValuePage() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 8);
        assertCopiesVarSizeRows();
    }

    @Test
    public void testRightJoin() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            assertMaterialized(engine, context, RIGHT_JOIN, null, true, false);
        });
    }

    @Test
    public void testRowsAndOffsetsShareTheBudget() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 4096);
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_MAX_PAGES, 16);
        run((engine, compiler, context) -> {
            createTables(engine, context);
            engine.execute(
                    "CREATE TABLE v AS (SELECT timestamp_sequence('2024-01-01', 1_000_000L) ts, ('k' || x)::VARCHAR k FROM long_sequence(3_000)) TIMESTAMP(ts) PARTITION BY DAY",
                    context
            );
            final String join = "SELECT count() FROM m FULL JOIN (SELECT * FROM v WHERE ts < '%s' UNION ALL SELECT * FROM v WHERE ts < '2024') s ON s.k < 'a'";
            assertQuery(join.formatted("2024-01-01T00:16:40"))
                    .withEngine(engine)
                    .withContext(context)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n1005\n");
            assertQuery(join.formatted("2024-01-01T01:00"))
                    .withEngine(engine)
                    .withContext(context)
                    .noLeakCheck()
                    .failsWith("materialized rows exceed the memory limit [maxBytes=65536] (raise cairo.sql.hash.join.value.max.pages)");
        });
    }

    @Test
    public void testStrictCopySkipsRows() throws Exception {
        // A strict copy copies the rows it skips, replays them after a rewind, and leaves the rows it could not skip.
        run((engine, compiler, context) -> {
            TestLatchedCounterFunctionFactory.reset(null);
            try (
                    RecordCursorFactory copy = copy(compiler, context, LAX_COPY_BASE, true, false);
                    RecordCursor cursor = copy.getCursor(context)
            ) {
                final RecordCursor.Counter counter = new RecordCursor.Counter();
                counter.set(10);
                cursor.skipRows(counter, Long.MAX_VALUE);
                Assert.assertEquals(0, counter.get());
                assertLaxRows(cursor, 990, 11);
                cursor.toTop();
                counter.set(1500);
                cursor.skipRows(counter, Long.MAX_VALUE);
                Assert.assertEquals(500, counter.get());
                Assert.assertEquals(1000, TestLatchedCounterFunctionFactory.getCount());
            } finally {
                TestLatchedCounterFunctionFactory.reset(null);
            }
        });
    }

    @Test
    public void testSpliceJoin() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            for (String query : new String[]{
                    "SELECT * FROM (($sub) TIMESTAMP(ts)) s SPLICE JOIN q",
                    "SELECT * FROM q SPLICE JOIN (($sub) TIMESTAMP(ts)) s"
            }) {
                // The multi-key rows of an hour share a timestamp, and their order, which differs between the
                // plans, decides which of them the splice pairs.
                for (int i = 0; i < SUBQUERIES.length; i += 2) {
                    Assert.assertEquals(query, 0, assertMaterialized(engine, context, query, SUBQUERIES[i], null, false, false));
                    Assert.assertEquals(query, 0, assertMaterialized(engine, context, query, withNegativeLimit(SUBQUERIES[i]), null, false, false));
                }
            }
        });
    }

    private void assertCopiesVarSizeRows() throws Exception {
        run((engine, compiler, context) -> {
            createTables(engine, context);
            final String[] subquery = {
                    "SELECT ts, sym::VARCHAR vc, sym::STRING str, count() c FROM t WHERE test_match() SAMPLE BY 1h",
                    "SELECT timestamp_floor('1h', ts) ts, sym::VARCHAR vc, sym::STRING str, count() c FROM t_hash ORDER BY ts, vc",
                    "ts, vc"
            };
            assertMaterialized(engine, context, CROSS_JOIN, subquery, null, true, false);
            assertMaterialized(engine, context, NEGATIVE_LIMIT, subquery, "ts", true, true);
        });
    }

    /**
     * Returns the number of rewinds that reached the stream GROUP BY.
     */
    // An allocation of the query that would pass the memory limit frees the copy first, from any thread, and the cursor
    // streams the base from then on, so a rewind runs the base again.
    private void assertLaxCopyFreedByAllocation(boolean isOtherThread) throws Exception {
        setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_MEMORY_LIMIT_BYTES, LAX_COPY_LIMIT);
        run((engine, compiler, context) -> withLaxCopy(engine, compiler, context, LAX_COPY_BASE, (tracker, cursor) -> {
            assertLaxRows(cursor, 100, 1);
            Assert.assertEquals(1, tracker.getReclaimerCount());
            final long copyBytes = tracker.getUsed();
            Assert.assertTrue(copyBytes > 0);
            // Fits only once the copy is gone.
            final long size = LAX_COPY_LIMIT - copyBytes + 1;
            final long ptr = allocate(tracker, size, isOtherThread);
            try {
                Assert.assertEquals(size, tracker.getUsed());
                assertLaxRows(cursor, 900, 101);
                Assert.assertFalse(cursor.hasNext());
                Assert.assertEquals(0, tracker.getReclaimerCount());
                Assert.assertEquals(1000, TestLatchedCounterFunctionFactory.getCount());
                cursor.toTop();
                assertLaxRows(cursor, 1000, 1);
                Assert.assertEquals(2000, TestLatchedCounterFunctionFactory.getCount());
                // Nothing is copied while the cursor streams.
                Assert.assertEquals(size, tracker.getUsed());
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT, tracker);
            }
        }));
    }

    private int assertMaterialized(
            CairoEngine engine,
            SqlExecutionContext context,
            String query,
            String[] subquery,
            CharSequence timestamp,
            boolean supportsRandomAccess,
            boolean isSizeKnown
    ) throws Exception {
        final String keys = subquery[2];
        final String expected = TestUtils.printSqlToString(
                engine,
                context,
                query.replace("$sub", subquery[1]).replace("$keys", keys),
                new StringSink()
        );
        final int topCount = TestMatchFunctionFactory.getTopCounter();
        assertQuery(query.replace("$sub", subquery[0]).replace("$keys", keys))
                .withEngine(engine)
                .withContext(context)
                .noLeakCheck()
                .timestamp(timestamp)
                .supportsRandomAccess(supportsRandomAccess)
                .expectSize(isSizeKnown)
                .withPlanContaining("Materialize", "Stream Group By")
                .returns(expected);
        return TestMatchFunctionFactory.getTopCounter() - topCount;
    }

    private void assertMaterialized(
            CairoEngine engine,
            SqlExecutionContext context,
            String query,
            CharSequence timestamp,
            boolean supportsRandomAccess,
            boolean isSizeKnown
    ) throws Exception {
        for (String[] subquery : SUBQUERIES) {
            // The parent rewinds the copy, so the stream GROUP BY under it runs once per cursor.
            Assert.assertEquals(query, 0, assertMaterialized(engine, context, query, subquery, timestamp, supportsRandomAccess, isSizeKnown));
        }
    }

    private void assertStreamReadOnce(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext context,
            String query,
            String input,
            String operator
    ) throws Exception {
        final String keys = SUBQUERIES[1][2];
        final boolean isLimit = NEGATIVE_LIMIT.equals(query);
        final String[] subquery = {input, "SELECT * FROM (" + input + ") ORDER BY " + keys, keys};
        assertMaterialized(engine, context, query, subquery, isLimit ? "ts" : null, true, isLimit);
        final String sql = query.replace("$sub", input).replace("$keys", keys);
        assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck().assertsPlanContaining(operator);
        Assert.assertEquals(sql, countRewinds(compiler, context, input), countRewinds(compiler, context, sql));
    }

    // Allocates against the tracker on the calling thread or on another one, and returns the address.
    private static long allocate(MemoryTracker tracker, long size, boolean isOtherThread) throws Exception {
        if (!isOtherThread) {
            return Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT, tracker);
        }
        final AtomicLong ptr = new AtomicLong();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread thread = new Thread(() -> {
            try {
                ptr.set(Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT, tracker));
            } catch (Throwable th) {
                failure.set(th);
            }
        });
        thread.start();
        thread.join();
        if (failure.get() != null) {
            if (failure.get() instanceof CairoException e) {
                throw e;
            }
            throw new AssertionError(failure.get());
        }
        return ptr.get();
    }

    // Reads rowCount rows of LAX_COPY_BASE, the first with x = firstX.
    private static void assertLaxRows(RecordCursor cursor, int rowCount, long firstX) {
        final Record record = cursor.getRecord();
        for (int i = 0; i < rowCount; i++) {
            Assert.assertTrue(cursor.hasNext());
            Assert.assertEquals(firstX + i, record.getLong(0));
            Assert.assertEquals(2 * (firstX + i), record.getLong(1));
        }
    }

    private static int countRewinds(SqlCompiler compiler, SqlExecutionContext context, String sql) throws SqlException {
        try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
            final int topCount = TestMatchFunctionFactory.getTopCounter();
            try (RecordCursor cursor = factory.getCursor(context)) {
                while (cursor.hasNext()) {
                    // drain the cursor
                }
            }
            return TestMatchFunctionFactory.getTopCounter() - topCount;
        }
    }

    // Runs the query once and returns how often its filter ran, after checking the result.
    private static int countFilterCalls(CairoEngine engine, SqlExecutionContext context, String sql, String expected) throws SqlException {
        TestLatchedCounterFunctionFactory.reset(null);
        final String actual = TestUtils.printSqlToString(engine, context, sql, new StringSink());
        final int calls = TestLatchedCounterFunctionFactory.getCount();
        TestUtils.assertEquals(sql, expected, actual);
        return calls;
    }

    private static void createTables(CairoEngine engine, SqlExecutionContext context) throws SqlException {
        // Every hour holds both symbols, so a limit with even bounds cuts the multi-key rows between hours.
        engine.execute(
                """
                        CREATE TABLE t AS (
                            SELECT timestamp_sequence('2024-01-01', 420_000_000L) ts,
                                   CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END::SYMBOL sym,
                                   x v
                            FROM long_sequence(200)
                        ) TIMESTAMP(ts) PARTITION BY DAY""",
                context
        );
        engine.execute("CREATE TABLE t_hash (ts TIMESTAMP, sym SYMBOL, v LONG)", context);
        engine.execute("INSERT INTO t_hash SELECT * FROM t", context);
        engine.execute("CREATE TABLE m AS (SELECT x FROM long_sequence(5))", context);
        engine.execute(
                "CREATE TABLE q AS (SELECT timestamp_sequence('2024-01-01T00:30', 3_600_000_000L) ts, x FROM long_sequence(10)) TIMESTAMP(ts) PARTITION BY DAY",
                context
        );
    }

    private static RecordCursorFactory copy(
            SqlCompiler compiler,
            SqlExecutionContext context,
            String sql,
            boolean isStrict,
            boolean isCopiedInBatches
    ) throws SqlException {
        final RecordCursorFactory base = compiler.compile(sql, context).getRecordCursorFactory();
        final RecordSink recordSink;
        try {
            final EntityColumnFilter columns = new EntityColumnFilter();
            columns.of(base.getMetadata().getColumnCount());
            recordSink = RecordSinkFactory.getInstance(configuration, new BytecodeAssembler(), base.getMetadata(), columns);
        } catch (Throwable th) {
            Misc.free(base);
            throw th;
        }
        return new MaterializedRecordCursorFactory(configuration, base, recordSink, isStrict, false, isCopiedInBatches);
    }

    // Opens a lax copy of base under a memory tracker; test_latched_counter() counts the base rows read. The tracker is
    // a materialized view refresh's, which the query registration of the base keeps, where it would replace one of a
    // query with its own.
    private static void withLaxCopy(CairoEngine engine, SqlCompiler compiler, SqlExecutionContext context, String base, LaxCopyTest test) throws Exception {
        TestLatchedCounterFunctionFactory.reset(null);
        try (
                MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(context.getSecurityContext(), 42, MemoryTrackerWorkload.MAT_VIEW_REFRESH);
                RecordCursorFactory copy = copy(compiler, context, base, false, false)
        ) {
            context.setMemoryTracker(tracker);
            try (RecordCursor cursor = copy.getCursor(context)) {
                test.run(tracker, cursor);
            } finally {
                context.setMemoryTracker(null);
                TestLatchedCounterFunctionFactory.reset(null);
            }
            Assert.assertEquals(0, tracker.getReclaimerCount());
            Assert.assertEquals(0, tracker.getUsed());
        }
    }

    private static String[] withNegativeLimit(String[] subquery) {
        return new String[]{"SELECT * FROM (" + subquery[0] + ") LIMIT -6", "SELECT * FROM (" + subquery[1] + ") LIMIT -6", subquery[2]};
    }

    private void run(CustomisableRunnable runnable) throws Exception {
        assertMemoryLeak(() -> TestUtils.execute(workerCount == 0 ? null : new TestWorkerPool(workerCount), runnable, configuration, LOG));
    }

    @FunctionalInterface
    private interface LaxCopyTest {
        void run(MemoryTracker tracker, RecordCursor cursor) throws Exception;
    }
}
