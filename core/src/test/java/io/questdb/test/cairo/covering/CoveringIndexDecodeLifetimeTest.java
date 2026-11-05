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

package io.questdb.test.cairo.covering;

import io.questdb.MessageBus;
import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.mp.RingQueue;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.PerQueryMemoryTrackerProvider;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * The lifetime of the buffers a covered (single-key covering-index) frame is decoded into. A
 * covered frame is a decoded frame, as a Parquet frame is: its buffers live while the frame memory
 * or a record of the pool is bound to them, or while the pool's cache budget has room for them, and
 * the per-frame release frees them. So a scan holds the frames it has in flight, not every frame it
 * has decoded; a shared reduce-queue slot keeps nothing of its frame once collected, whichever
 * queries share the queue; and a record over a covered frame is not stable, so a reader that keeps
 * a value past the frame copies it.
 */
public class CoveringIndexDecodeLifetimeTest extends AbstractCairoTest {
    private static final String COVERED_FILTER = "SELECT ts, sym, s, px FROM cov WHERE sym = 'S0' AND px > 0.5";
    // PageFrameMemoryPool keeps at most this many shells of covered buffers
    private static final int COVERING_SHELL_CAP = 512;
    private static final int FRAME_ROWS = 200;
    private static final int PARTITIONS = 64;
    private static final String REF_FILTER = "SELECT ts, sym, s, px FROM ref WHERE sym = 'S0' AND px > 0.5";
    private static final int ROWS_PER_PARTITION = 2000;
    // the scratch a pool decodes a compressed covered run into: 4,096 longs
    private static final long RUN_DECODE_SCRATCH_BYTES = 4096 * Long.BYTES;
    // queries whose readers may keep a covered VARCHAR past its frame, or read frames back: each
    // runs over cov and over its non-indexed twin ref, with the freed covered buffers poisoned
    private static final String[] VALUE_KEEPING_QUERIES = {
            // async keyed GROUP BY: first(), last(), min() and max() keep a VARCHAR per group
            "SELECT sym, first(s), last(s), min(s), max(s), count() FROM %s WHERE sym = 'S0'",
            // a VARCHAR key, kept by the map
            "SELECT s, count(), sum(px) FROM %s WHERE sym = 'S0' GROUP BY s ORDER BY s",
            // not keyed
            "SELECT first(s), last(s), count() FROM %s WHERE sym = 'S0'",
            // with a filter on the covered columns
            "SELECT s, first(px), last(ts), first(s) FROM %s WHERE sym = 'S0' AND px > 100 GROUP BY s ORDER BY s",
            // top K over the async filter: the sort keeps rows of earlier frames
            "SELECT ts, s, px FROM %s WHERE sym = 'S0' AND px > 0.5 ORDER BY s DESC, ts LIMIT 25",
            // a full sort, which reads the rows back through the cursor's own pool
            "SELECT ts, s, px FROM %s WHERE sym = 'S0' AND px > 0.5 ORDER BY s, px, ts",
            // negative limit
            "SELECT ts, s, px FROM %s WHERE sym = 'S0' AND px > 0.5 LIMIT -50",
            "SELECT ts, first(s), last(s), count() FROM %s WHERE sym = 'S0' SAMPLE BY 1d",
            "SELECT DISTINCT s FROM %s WHERE sym = 'S0' ORDER BY s",
            // a serial GROUP BY over the rows of a limited async filter
            "SELECT first(s), last(s), count() FROM (SELECT ts, s FROM %s WHERE sym = 'S0' AND px > 0.5 LIMIT 5000)",
            "SELECT ts, s, count(s) OVER (ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) FROM %s WHERE sym = 'S0' AND px > 990",
            "SELECT string_agg(s, ',') FROM (SELECT ts, s FROM %s WHERE sym = 'S0' AND px > 990)",
    };

    @Test
    public void testBudgetedPoolKeepsUnboundCoveredFramesWhileTheyFit() throws Exception {
        // A pool with a cache budget, as a cursor that reads rows back has, keeps the covered frames
        // no holder is bound to while the budget has room for them, and frees the oldest first.
        assertMemoryLeak(() -> {
            createPoolTable();
            final long frameBytes = coveredFrameBytes();
            final int budgetFrames = 3;
            try (
                    RecordCursorFactory factory = select("SELECT sym, payload, v FROM t WHERE sym = 'A'");
                    PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                    PageFrameAddressCache addressCache = new PageFrameAddressCache();
                    // room for three frames, not four
                    PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, budgetFrames * frameBytes + frameBytes / 2);
                    PageFrameMemoryRecord recordA = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)
            ) {
                final int frameCount = buildAddressCache(factory, cursor, addressCache);
                assertTrue(frameCount > 2 * budgetFrames);
                // the full budget, and room for more frames than the budget allows
                pool.of(addressCache, ParquetDecodeHint.SCATTERED);
                recordA.of(cursor);
                // the frame memory stays on frame 0 while the record scans every frame
                final PageFrameMemory frameMemory = pool.navigateTo(0);
                for (int f = 0; f < frameCount; f++) {
                    pool.navigateTo(f, recordA);
                    assertFrameValues(recordA, addressCache, f);
                    // frame 0 with the frame memory, f with the record, and the unbound frames
                    // decoded last, while the budget has room for them
                    final int expectedFrames = Math.min(f + 1, budgetFrames);
                    assertEquals("frames held at frame " + f, expectedFrames, pool.getCoveredFrameCount());
                    assertEquals(expectedFrames * frameBytes, pool.getCoveredCachedBytes());
                }
                // the frame memory's frame survived the scan
                final PageFrameMemoryRecord recordB = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_B_LETTER);
                try {
                    recordB.of(cursor);
                    recordB.init(frameMemory);
                    assertFrameValues(recordB, addressCache, 0);
                } finally {
                    recordB.close();
                }
                // a frame that was freed decodes again
                pool.navigateTo(1, recordA);
                assertFrameValues(recordA, addressCache, 1);
                pool.releaseParquetBuffers();
                assertEquals(0, pool.getCoveredFrameCount());
                assertEquals(0, pool.getCoveredCachedBytes());
            }
        });
    }

    @Test
    public void testCoveredDecodeReleasesItsTrackerCharge() throws Exception {
        // A covered frame's buffers charge the pool's tracker while they live, and their free
        // releases the charge, so the query's memory limit sees the frames the query holds. A free
        // after the tracker has been reconciled, at its query's end, is global-only: the
        // reconciliation released the charge already.
        assertMemoryLeak(() -> {
            createPoolTable();
            try (
                    PerQueryMemoryTrackerProvider provider = new PerQueryMemoryTrackerProvider(configuration);
                    RecordCursorFactory factory = select("SELECT sym, payload, v FROM t WHERE sym = 'A'");
                    PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                    PageFrameAddressCache addressCache = new PageFrameAddressCache();
                    PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, 0L);
                    PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)
            ) {
                final int frameCount = buildAddressCache(factory, cursor, addressCache);
                final MemoryTracker tracker = provider.acquire(AllowAllSecurityContext.INSTANCE, 1, MemoryTrackerWorkload.QUERY);
                try {
                    pool.setMemoryTracker(tracker);
                    pool.of(addressCache);
                    record.of(cursor);
                    for (int f = 0; f < frameCount; f++) {
                        pool.navigateTo(f, record);
                        assertFrameValues(record, addressCache, f);
                        // the frame the record is on, alone
                        assertTrue(pool.getCoveredCachedBytes() > 0);
                        assertEquals("charge at frame " + f, pool.getCoveredCachedBytes(), tracker.getUsed());
                    }
                    pool.releaseParquetBuffers();
                    assertEquals(0, pool.getCoveredCachedBytes());
                    assertEquals("the free released the charge", 0, tracker.getUsed());

                    // a frame that outlives its query's tracker
                    pool.navigateTo(0, record);
                    final long frameBytes = pool.getCoveredCachedBytes();
                    assertEquals(frameBytes, tracker.getUsed());
                    final long globalBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_INDEX_READER);
                    tracker.reconcileCovered();
                    assertEquals("the reconciliation released the charge", 0, tracker.getUsed());
                    pool.releaseParquetBuffers();
                    assertEquals("the late free released no charge again", 0, tracker.getUsed());
                    assertEquals(globalBefore - frameBytes, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_INDEX_READER));
                } finally {
                    pool.clear();
                    tracker.close();
                }
            }
        });
    }

    @Test
    public void testCoveredFramesDecodedAheadOutliveTheReleaseUntilFolded() throws Exception {
        // A frame decoded ahead of a fold stays through the release that the fold's reducers call
        // after each frame, until the fold releases it; a query boundary releases it too.
        assertMemoryLeak(() -> {
            createPoolTable();
            try (
                    RecordCursorFactory factory = select("SELECT sym, payload, v FROM t WHERE sym = 'A'");
                    PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                    PageFrameAddressCache addressCache = new PageFrameAddressCache();
                    PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, 0L);
                    PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)
            ) {
                buildAddressCache(factory, cursor, addressCache);
                pool.of(addressCache);
                record.of(cursor);
                final long frameBytes = coveredFrameBytes();
                pool.decodeAhead(0);
                pool.decodeAhead(1);
                pool.decodeAhead(2);
                // decoded ahead, they stay beyond the zero budget
                assertEquals(3, pool.getCoveredFrameCount());
                assertEquals(3 * frameBytes, pool.getCoveredCachedBytes());
                // as a reducer of the fold reads a frame and releases the decoded frames
                record.init(pool.navigateTo(1));
                assertFrameValues(record, addressCache, 1);
                pool.releaseParquetBuffers();
                assertEquals(3, pool.getCoveredFrameCount());
                pool.releaseDecodedAhead(1);
                assertEquals(2, pool.getCoveredFrameCount());
                // a frame that is not decoded ahead is released as any other
                record.init(pool.navigateTo(3));
                assertEquals(3, pool.getCoveredFrameCount());
                pool.releaseDecodedAhead(3);
                pool.releaseParquetBuffers();
                assertEquals(2, pool.getCoveredFrameCount());
                // the fold reads frame 0 through a record, which keeps it once released
                pool.navigateTo(0, record);
                assertFrameValues(record, addressCache, 0);
                pool.releaseDecodedAhead(0);
                assertEquals(2, pool.getCoveredFrameCount());
                pool.navigateTo(4, record);
                assertEquals("the record left frame 0, which nothing keeps", 2, pool.getCoveredFrameCount());
                // nothing to release for a frame the pool does not hold
                pool.releaseDecodedAhead(9);
                assertEquals(2, pool.getCoveredFrameCount());
                // the query boundary frees frame 2, still decoded ahead
                pool.of(addressCache);
                assertEquals(0, pool.getCoveredFrameCount());
                assertEquals(0, pool.getCoveredCachedBytes());
                // and so does clear()
                pool.decodeAhead(0);
                assertEquals(1, pool.getCoveredFrameCount());
                pool.clear();
                assertEquals(0, pool.getCoveredFrameCount());
                assertEquals(0, pool.getCoveredCachedBytes());
            }
        });
    }

    @Test
    public void testDecodeAheadFoldsOverCoveredFrames() throws Exception {
        // Rare keys over many partitions: fewer rows than the covering scan reduces on the query's
        // thread, in more frames than one thread decodes, so the workers decode the frames ahead of
        // a fold on the query's thread. A single key's frames are decoded ahead, each kept by its
        // pool until the fold has read it; merged frames of two keys, decoded as the scan builds
        // them, are not, and the fold reads them through its own pool. Both equal the non-indexed
        // twin.
        UnorderedPageFrameSequence.setLocalReduceOpenCountingForTesting(true);
        try {
            assertMemoryLeak(() -> TestUtils.execute(
                    new TestWorkerPool(4, WorkerPoolMode.LEGACY),
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute("CREATE TABLE m (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (v, s), v LONG, s VARCHAR)" +
                                " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", sqlExecutionContext);
                        engine.execute("CREATE TABLE m_ref (ts TIMESTAMP, sym SYMBOL, v LONG, s VARCHAR) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", sqlExecutionContext);
                        // 200 days of 100 rows: an R1 and an R2 row a day, the rest other keys
                        final String rows = "SELECT ('2024-01-01'::TIMESTAMP + (x - 1) * 864_000_000L)::timestamp," +
                                " CASE WHEN x % 100 = 1 THEN 'R1' WHEN x % 100 = 51 THEN 'R2' ELSE 'k' || (x % 7) END," +
                                " x, 'value-' || x FROM long_sequence(20_000)";
                        engine.execute("INSERT INTO m " + rows, sqlExecutionContext);
                        engine.execute("INSERT INTO m_ref " + rows, sqlExecutionContext);
                        engine.releaseAllWriters();
                        PageFrameMemoryPool.setPoisoningFreedCoveredBuffers(true);
                        try {
                            for (String keys : new String[]{"sym = 'R1'", "sym IN ('R1', 'R2')"}) {
                                final String query = "SELECT sym, count(), sum(v), first(s), last(s) FROM %s WHERE " + keys + " ORDER BY sym";
                                CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
                                final long decodedAheadBefore = UnorderedPageFrameSequence.getLocalReduceOpensForTesting()[2];
                                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, String.format(query, "m_ref"), String.format(query, "m"), LOG);
                                assertEquals(keys, 1, UnorderedPageFrameSequence.getLocalReduceOpensForTesting()[2] - decodedAheadBefore);
                                // the two keys' frames are merged ones
                                assertEquals(keys, keys.startsWith("sym IN") ? 1 : 0, CoveringIndexRecordCursorFactory.getMergedModeOpensForTesting());
                            }
                        } finally {
                            PageFrameMemoryPool.setPoisoningFreedCoveredBuffers(false);
                        }
                    },
                    configuration,
                    LOG
            ));
        } finally {
            UnorderedPageFrameSequence.setLocalReduceOpenCountingForTesting(false);
        }
    }

    @Test
    public void testFailedCoveredDecodeFreesItsFrame() throws Exception {
        // A covered decode that breaches the query's memory limit frees what it allocated: the
        // pool holds no buffers of the frame, the tracker no charge, and the record or frame
        // memory is cleared, so that it does not read through its binding. A record's decode, of
        // the frame or of the declared rows alone, and the frame memory's decode of the filter's
        // columns fail alike. The pool decodes the frame once the limit allows.
        assertMemoryLeak(() -> {
            createPoolTable();
            final long frameBytes = coveredFrameBytes();
            // the VARCHAR, whose decode alone needs more than the limit
            final IntHashSet filterColumns = new IntHashSet();
            filterColumns.add(1);
            try (
                    PerQueryMemoryTrackerProvider provider = new PerQueryMemoryTrackerProvider(new DefaultCairoConfiguration(configuration.getDbRoot()) {
                        @Override
                        public long getQueryMemoryLimitBytes() {
                            // less than a frame decodes into
                            return frameBytes / 2;
                        }
                    });
                    RecordCursorFactory factory = select("SELECT sym, payload, v FROM t WHERE sym = 'A'");
                    PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                    PageFrameAddressCache addressCache = new PageFrameAddressCache();
                    PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, 0L);
                    PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)
            ) {
                buildAddressCache(factory, cursor, addressCache);
                final MemoryTracker tracker = provider.acquire(AllowAllSecurityContext.INSTANCE, 1, MemoryTrackerWorkload.QUERY);
                try {
                    // the record's frame decode, its decode of declared rows, the frame memory's
                    // decode of the filter's columns
                    for (int path = 0; path < 3; path++) {
                        // the record is on frame 0, decoded with no limit, and the decode of frame
                        // 1 fails under the limit
                        pool.setMemoryTracker(null);
                        pool.of(addressCache);
                        record.of(cursor);
                        pool.navigateTo(0, record);
                        assertFrameValues(record, addressCache, 0);
                        pool.setMemoryTracker(tracker);
                        if (path == 1) {
                            // three of frame 1's 12 rows, one of them twice, few enough to decode
                            // alone
                            pool.setRecordAtRows((target, cache) -> {
                                target.add(Rows.toRowID(1, 3));
                                target.add(Rows.toRowID(1, 7));
                                target.add(Rows.toRowID(1, 7));
                                target.add(Rows.toRowID(1, 9));
                            });
                        }
                        final long decodedBefore = PageFrameMemoryPool.getCoveredRowDecodeCount();
                        try {
                            if (path == 2) {
                                pool.navigateTo(1, filterColumns);
                            } else {
                                pool.navigateTo(1, record);
                            }
                            fail("expected the query's memory limit to stop the decode");
                        } catch (CairoException e) {
                            assertTrue(e.isOutOfMemory());
                            TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        }
                        assertEquals(path == 1 ? 1 : 0, PageFrameMemoryPool.getCoveredRowDecodeCount() - decodedBefore);
                        assertEquals(0, tracker.getUsed());
                        if (path == 2) {
                            // the record keeps frame 0
                            assertEquals(1, pool.getCoveredFrameCount());
                            assertEquals(frameBytes, pool.getCoveredCachedBytes());
                            assertFrameValues(record, addressCache, 0);
                        } else {
                            assertEquals("the record is cleared", -1, record.getFrameIndex());
                            // frame 0, which the record left, made room for frame 1, whose
                            // buffers the failure freed
                            assertEquals(0, pool.getCoveredFrameCount());
                            assertEquals(0, pool.getCoveredCachedBytes());
                        }
                        // with no limit, the frame decodes
                        pool.setMemoryTracker(null);
                        if (path == 2) {
                            try (PageFrameMemoryRecord frameRecord = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_B_LETTER)) {
                                frameRecord.of(cursor);
                                frameRecord.init(pool.navigateTo(1, filterColumns));
                                final StringSink sink = new StringSink();
                                for (int r = 0; r < 12; r++) {
                                    frameRecord.setRowIndex(r);
                                    sink.clear();
                                    sink.put(frameRecord.getVarcharA(1));
                                    assertEquals(String.format("payload-%012d", 24 + 2 * r + 1), sink.toString());
                                }
                            }
                        } else {
                            pool.navigateTo(1, record);
                            if (path == 1) {
                                assertDeclaredRows(record, 3, 7, 9);
                                // the fast path of a record on frame 1's declared rows
                                pool.navigateTo(1, record);
                                assertDeclaredRows(record, 3, 7, 9);
                                // other rows declared: the rows decoded for the earlier ones give
                                // way to those
                                pool.setRecordAtRows((target, cache) -> target.add(Rows.toRowID(1, 5)));
                                pool.navigateTo(1, record);
                                assertDeclaredRows(record, 5);
                                assertEquals(3, PageFrameMemoryPool.getCoveredRowDecodeCount() - decodedBefore);
                            } else {
                                assertFrameValues(record, addressCache, 1);
                            }
                        }
                        pool.clear();
                    }
                } finally {
                    pool.clear();
                    tracker.close();
                }
            }
        });
    }

    @Test
    public void testOutOfMemoryAtEveryCoveredAllocationLeavesThePoolUsable() throws Exception {
        // An allocation that fails anywhere in the decode of a covered frame, in the reopen of a
        // parked shell's address lists, in the index reader's cursor or in a column buffer, leaves
        // no buffers of the frame behind, and the pool decodes the frame once memory allows, for
        // the frame memory and for a record alike.
        assertMemoryLeak(() -> {
            createPoolTable();
            try (
                    RecordCursorFactory factory = select("SELECT sym, payload, v FROM t WHERE sym = 'A'");
                    PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                    PageFrameAddressCache addressCache = new PageFrameAddressCache();
                    PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, 0L);
                    PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
                    PageFrameMemoryRecord frameRecord = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_B_LETTER)
            ) {
                buildAddressCache(factory, cursor, addressCache);
                pool.of(addressCache);
                record.of(cursor);
                frameRecord.of(cursor);
                for (boolean isRecord : new boolean[]{true, false}) {
                    // a parked shell, which the next decode reopens
                    pool.navigateTo(0, record);
                    pool.releaseParquetBuffers();
                    assertEquals(1, pool.getCoveringShellCount());
                    int failures = 0;
                    boolean hasDecodedUnderLimit = false;
                    for (int slack = 0; !hasDecodedUnderLimit; slack += 16) {
                        Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + slack);
                        try {
                            if (isRecord) {
                                pool.navigateTo(1, record);
                            } else {
                                frameRecord.init(pool.navigateTo(1));
                            }
                            hasDecodedUnderLimit = true;
                        } catch (CairoException e) {
                            assertTrue(e.getFlyweightMessage().toString(), e.isOutOfMemory());
                            failures++;
                            assertEquals("slack " + slack, 0, pool.getCoveredFrameCount());
                            assertEquals("slack " + slack, 0, pool.getCoveredCachedBytes());
                        } finally {
                            Unsafe.setRssMemLimit(0);
                        }
                        // decoded, under the limit or now
                        if (isRecord) {
                            pool.navigateTo(1, record);
                            assertFrameValues(record, addressCache, 1);
                        } else {
                            frameRecord.init(pool.navigateTo(1));
                            assertFrameValues(frameRecord, addressCache, 1);
                        }
                        pool.releaseParquetBuffers();
                        assertEquals(0, pool.getCoveredCachedBytes());
                    }
                    // the decode allocates in several places, each of which failed at some limit
                    assertTrue("failures: " + failures, failures > 3);
                }
            }
        });
    }

    @Test
    public void testPoolKeepsShellsUpToItsCap() throws Exception {
        // A pool that keeps shells keeps as many as its cap, and closes the buffers it releases
        // past it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE many (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (v), v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            // a covered frame a day
            execute("INSERT INTO many SELECT '2024-01-01'::TIMESTAMP + (x - 1) * 86_400_000_000L, 'A', x FROM long_sequence(600)");
            engine.releaseAllWriters();
            try (
                    RecordCursorFactory factory = select("SELECT sym, v FROM many WHERE sym = 'A'");
                    PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                    PageFrameAddressCache addressCache = new PageFrameAddressCache();
                    PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, 0L)
            ) {
                assertEquals(600, buildAddressCache(factory, cursor, addressCache));
                pool.setRetainsCoveringShells(true);
                pool.of(addressCache);
                // frames decoded ahead stay, whatever the budget
                for (int f = 0; f < 600; f++) {
                    pool.decodeAhead(f);
                }
                assertEquals(600, pool.getCoveredFrameCount());
                pool.clear();
                assertEquals(0, pool.getCoveredFrameCount());
                assertEquals(0, pool.getCoveredCachedBytes());
                assertEquals(COVERING_SHELL_CAP, pool.getCoveringShellCount());
            }
        });
    }

    @Test
    public void testPoolOfSharedQueueKeepsNoDecodeMemoryPastTheFrame() throws Exception {
        // The per-frame release of a pool that keeps no shells, as a shared queue task's does not,
        // frees all of a covered frame's native memory: its buffers, the address lists of its
        // shell and the scratch of its compressed runs. A pool that keeps shells, as an atom's
        // does, keeps the scratch for its next frames, until its query boundary.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t2 (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (v), v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            // two commits into each day, which the index seals into a generation of compressed
            // blocks of covered values
            execute("INSERT INTO t2 SELECT '2024-01-01'::TIMESTAMP + (x - 1) * 3_600_000_000L, CASE WHEN x % 2 = 1 THEN 'A' ELSE 'B' END, x FROM long_sequence(240)");
            engine.releaseAllWriters();
            execute("INSERT INTO t2 SELECT '2024-01-01'::TIMESTAMP + (x - 1) * 3_600_000_000L + 1, CASE WHEN x % 2 = 1 THEN 'A' ELSE 'B' END, 1_000 + x FROM long_sequence(240)");
            engine.releaseAllWriters();
            try (
                    RecordCursorFactory factory = select("SELECT sym, v FROM t2 WHERE sym = 'A'");
                    PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                    PageFrameAddressCache addressCache = new PageFrameAddressCache()
            ) {
                final int frameCount = buildAddressCache(factory, cursor, addressCache);
                assertEquals(10, frameCount);
                // a first pass, so that the index readers hold the caches they keep
                try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, 0L)) {
                    pool.of(addressCache);
                    for (int f = 0; f < frameCount; f++) {
                        pool.navigateTo(f);
                    }
                }
                final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_INDEX_READER);
                for (boolean isRetainingShells : new boolean[]{false, true}) {
                    try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, 0L)) {
                        pool.setRetainsCoveringShells(isRetainingShells);
                        pool.of(addressCache);
                        final long nativeDefaultBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
                        for (int f = 0; f < frameCount; f++) {
                            pool.navigateTo(f);
                            // the frame's buffers and the run scratch
                            assertEquals(
                                    "retaining shells: " + isRetainingShells,
                                    pool.getCoveredCachedBytes() + RUN_DECODE_SCRATCH_BYTES,
                                    Unsafe.getMemUsedByTag(MemoryTag.NATIVE_INDEX_READER) - before
                            );
                            pool.releaseParquetBuffers();
                            assertEquals(0, pool.getCoveredCachedBytes());
                            assertEquals(
                                    "retaining shells: " + isRetainingShells,
                                    isRetainingShells ? RUN_DECODE_SCRATCH_BYTES : 0,
                                    Unsafe.getMemUsedByTag(MemoryTag.NATIVE_INDEX_READER) - before
                            );
                            if (!isRetainingShells) {
                                // the shell is parked, its address lists freed
                                assertEquals(nativeDefaultBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT));
                            }
                        }
                        pool.clear();
                        assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_INDEX_READER));
                        if (isRetainingShells) {
                            // a pool that stops keeping shells parks the shells it keeps
                            assertEquals(1, pool.getCoveringShellCount());
                            final long nativeDefaultKept = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
                            pool.setRetainsCoveringShells(false);
                            assertEquals(1, pool.getCoveringShellCount());
                            assertTrue(Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT) < nativeDefaultKept);
                            // and reopens them for the next decodes
                            pool.of(addressCache);
                            pool.navigateTo(0);
                            assertEquals(0, pool.getCoveringShellCount());
                            assertTrue(pool.getCoveredCachedBytes() > 0);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testPoolOfUncoveredFramesDecodesNothingAhead() throws Exception {
        // decodeAhead() decodes covered frames alone: a native frame needs no decode, and the pool
        // holds nothing for it.
        assertMemoryLeak(() -> {
            createPoolTable();
            execute("CREATE TABLE plain (ts TIMESTAMP, sym SYMBOL, payload VARCHAR, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO plain SELECT * FROM t WHERE sym = 'A'");
            engine.releaseAllWriters();
            try (
                    RecordCursorFactory factory = select("SELECT sym, payload, v FROM plain");
                    PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                    PageFrameAddressCache addressCache = new PageFrameAddressCache();
                    PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, 0L);
                    PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)
            ) {
                // a frame a day
                assertEquals(10, buildAddressCache(factory, cursor, addressCache));
                pool.of(addressCache);
                record.of(cursor);
                pool.decodeAhead(0);
                assertEquals(0, pool.getCoveredFrameCount());
                pool.releaseDecodedAhead(0);
                record.init(pool.navigateTo(0));
                assertEquals(0, pool.getCoveredFrameCount());
                assertEquals(12, addressCache.getFrameSize(0));
                for (int r = 0; r < 12; r++) {
                    record.setRowIndex(r);
                    assertEquals(2L * r + 1, record.getLong(2));
                }
            }
        });
    }

    @Test
    public void testRecordsOverCoveredFramesAreNotStable() throws Exception {
        // A covered frame lives until its pool releases it, so neither its frame memory nor a record
        // bound to it is stable. A multi-key covered frame, decoded eagerly by the page frame cursor,
        // which frees it at close, is, and so is a native frame.
        assertMemoryLeak(() -> {
            createPoolTable();
            execute("CREATE TABLE plain (ts TIMESTAMP, sym SYMBOL, payload VARCHAR, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO plain SELECT * FROM t");
            engine.releaseAllWriters();
            assertEquals(10, assertStability("SELECT sym, payload, v FROM t WHERE sym = 'A'"));
            // per-key frames are single-key covered ones, merged frames are not
            assertStability("SELECT sym, payload, v FROM t WHERE sym IN ('A', 'B')");
            assertEquals(0, assertStability("SELECT sym, payload, v FROM plain"));
            // a decoded Parquet frame is not
            execute("CREATE TABLE pq (ts TIMESTAMP, sym SYMBOL, payload VARCHAR, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO pq SELECT * FROM t");
            // all days but the last, the active partition
            execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-10'");
            engine.releaseAllWriters();
            assertEquals(0, assertStability("SELECT sym, payload, v FROM pq"));
            // a frame memory that is not a pool's tells by its format
            try (PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)) {
                record.init(new FormatOnlyFrameMemory(PartitionFormat.NATIVE));
                assertTrue(record.isStable());
                record.init(new FormatOnlyFrameMemory(PartitionFormat.PARQUET));
                assertFalse(record.isStable());
            }
        });
    }

    @Test
    public void testReduceQueueHoldsCoveredFramesInFlightOnly() throws Exception {
        // A reduce-queue slot frees its covered frame once the cursor has collected it, so a covered
        // scan holds at most a frame per slot, however many frames it decodes. Were the frames kept
        // until the end of the query, the slots would hold most of the scan's decoded volume.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, FRAME_ROWS);
        assertMemoryLeak(() -> {
            for (WorkerPoolMode mode : WorkerPoolMode.values()) {
                TestUtils.execute(
                        new TestWorkerPool(4, mode),
                        (engine, compiler, sqlExecutionContext) -> {
                            createCoveredTables(engine, sqlExecutionContext);
                            // the bytes of one frame, decoded alone; every frame holds 200 rows
                            // of the same short values, which decode into as many bytes
                            final long frameBytes;
                            try (
                                    RecordCursorFactory factory = compiler.compile("SELECT ts, sym, s, px FROM cov WHERE sym = 'S0'", sqlExecutionContext).getRecordCursorFactory();
                                    PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                                    PageFrameAddressCache addressCache = new PageFrameAddressCache()
                            ) {
                                assertEquals(PARTITIONS * ROWS_PER_PARTITION / 2 / FRAME_ROWS, buildAddressCache(factory, cursor, addressCache));
                                frameBytes = decodedFrameBytes(addressCache, 0);
                                assertEquals(frameBytes, decodedFrameBytes(addressCache, 1));
                            }
                            assertTrue(frameBytes > 0);
                            final int slots = engine.getConfiguration().getPageFrameReduceQueueCapacity();
                            final int frameCount = PARTITIONS * ROWS_PER_PARTITION / 2 / FRAME_ROWS;
                            // the bound is well below what the scan decodes in all
                            assertTrue(4L * slots < frameCount);
                            long maxHeld = 0;
                            long rows = 0;
                            try (
                                    RecordCursorFactory factory = compiler.compile(COVERED_FILTER, sqlExecutionContext).getRecordCursorFactory();
                                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                            ) {
                                while (cursor.hasNext()) {
                                    if (++rows % 500 == 0) {
                                        maxHeld = Math.max(maxHeld, reduceQueueCoveredBytes(engine));
                                    }
                                }
                            }
                            assertEquals(expectedFilterRowCount(), rows);
                            assertTrue(mode + ": held " + maxHeld + " bytes, a frame is " + frameBytes, maxHeld <= slots * frameBytes);
                            assertEquals(mode.toString(), 0, reduceQueueCoveredBytes(engine));
                            assertEquals(mode.toString(), 0, reduceQueueCoveredFrames(engine));
                            dropCoveredTables(engine, sqlExecutionContext);
                        },
                        configuration,
                        LOG
                );
            }
        });
    }

    @Test
    public void testReduceQueueKeepsNoCoveredFrameAfterConcurrentQueries() throws Exception {
        // With one reduce queue, the two cursors of a comparison share its slots, and the covered
        // one, starved of slots while the other holds them, reduces most of its frames on the
        // query's thread. Its last frames in the queue are then not its last frames, which a slot
        // used to free only. No slot keeps a covered frame past its collection, so none is left once
        // both cursors close, and the covered decode memory is back where a lone query leaves it.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, FRAME_ROWS);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 1);
        assertMemoryLeak(() -> {
            for (WorkerPoolMode mode : WorkerPoolMode.values()) {
                TestUtils.execute(
                        new TestWorkerPool(4, mode),
                        (engine, compiler, sqlExecutionContext) -> {
                            createCoveredTables(engine, sqlExecutionContext);
                            final StringSink sink = new StringSink();
                            TestUtils.printSql(compiler, sqlExecutionContext, COVERED_FILTER, sink);
                            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_INDEX_READER);
                            assertEquals(mode.toString(), 0, reduceQueueCoveredBytes(engine));
                            for (int i = 0; i < 3; i++) {
                                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, REF_FILTER, COVERED_FILTER, LOG);
                                assertEquals(mode + ", run " + i, 0, reduceQueueCoveredBytes(engine));
                                assertEquals(mode + ", run " + i, 0, reduceQueueCoveredFrames(engine));
                                assertEquals(mode + ", run " + i, baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_INDEX_READER));
                                TestUtils.printSql(compiler, sqlExecutionContext, COVERED_FILTER, sink);
                                assertEquals(mode + ", run " + i, baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_INDEX_READER));
                            }
                            dropCoveredTables(engine, sqlExecutionContext);
                        },
                        configuration,
                        LOG
                );
            }
        });
    }

    @Test
    public void testReduceQueueKeepsNoCoveredFrameAfterEarlyClose() throws Exception {
        // A cursor closed before its last frame, and one read twice, leave no covered frame in the
        // reduce queue either.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, FRAME_ROWS);
        assertMemoryLeak(() -> TestUtils.execute(
                new TestWorkerPool(4, WorkerPoolMode.LEGACY),
                (engine, compiler, sqlExecutionContext) -> {
                    createCoveredTables(engine, sqlExecutionContext);
                    try (RecordCursorFactory factory = compiler.compile(COVERED_FILTER, sqlExecutionContext).getRecordCursorFactory()) {
                        for (int rowsToRead : new int[]{1, 1_000, 10_000}) {
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                for (int r = 0; r < rowsToRead && cursor.hasNext(); r++) {
                                    assertEquals(0, cursor.getRecord().getInt(1));
                                }
                            }
                            assertEquals("after " + rowsToRead + " rows", 0, reduceQueueCoveredBytes(engine));
                        }
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            for (int pass = 0; pass < 2; pass++) {
                                long rows = 0;
                                while (cursor.hasNext()) {
                                    rows++;
                                }
                                assertEquals(expectedFilterRowCount(), rows);
                                cursor.toTop();
                            }
                        }
                        assertEquals(0, reduceQueueCoveredBytes(engine));
                    }
                    // LIMIT stops the dispatch early
                    final StringSink sink = new StringSink();
                    TestUtils.printSql(compiler, sqlExecutionContext, COVERED_FILTER + " LIMIT 10", sink);
                    assertEquals(0, reduceQueueCoveredBytes(engine));
                },
                configuration,
                LOG
        ));
    }

    @Test
    public void testReduceQueueKeepsNoCoveredFrameAfterQueriesOnSeveralThreads() throws Exception {
        // Covered queries that run on several threads at once share the reduce queue's slots. Each
        // equals its non-indexed twin, with the freed covered buffers poisoned, and once all are
        // done no slot holds a covered frame.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, FRAME_ROWS);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 1);
        final int threadCount = 3;
        assertMemoryLeak(() -> TestUtils.execute(
                new TestWorkerPool(4, WorkerPoolMode.LEGACY),
                (engine, compiler, sqlExecutionContext) -> {
                    createCoveredTables(engine, sqlExecutionContext);
                    PageFrameMemoryPool.setPoisoningFreedCoveredBuffers(true);
                    try {
                        final CyclicBarrier barrier = new CyclicBarrier(threadCount);
                        final AtomicReference<Throwable> failure = new AtomicReference<>();
                        final Thread[] threads = new Thread[threadCount];
                        for (int t = 0; t < threadCount; t++) {
                            final int threadIndex = t;
                            threads[t] = new Thread(() -> {
                                try (
                                        SqlCompiler threadCompiler = engine.getSqlCompiler();
                                        SqlExecutionContext threadContext = TestUtils.createSqlExecutionCtx(engine, 4)
                                ) {
                                    barrier.await(30, TimeUnit.SECONDS);
                                    for (int i = 0; i < 3; i++) {
                                        final String query = VALUE_KEEPING_QUERIES[(threadIndex + i) % VALUE_KEEPING_QUERIES.length];
                                        TestUtils.assertSqlCursors(threadCompiler, threadContext, String.format(query, "ref"), String.format(query, "cov"), LOG);
                                        TestUtils.assertSqlCursors(threadCompiler, threadContext, REF_FILTER, COVERED_FILTER, LOG);
                                    }
                                } catch (Throwable th) {
                                    failure.compareAndSet(null, th);
                                }
                            });
                            threads[t].start();
                        }
                        for (Thread thread : threads) {
                            thread.join();
                        }
                        if (failure.get() != null) {
                            throw new AssertionError(failure.get());
                        }
                    } finally {
                        PageFrameMemoryPool.setPoisoningFreedCoveredBuffers(false);
                    }
                    assertEquals(0, reduceQueueCoveredBytes(engine));
                    assertEquals(0, reduceQueueCoveredFrames(engine));
                },
                configuration,
                LOG
        ));
    }

    @Test
    public void testValueKeepingReadersCopyCoveredValues() throws Exception {
        // Every covered frame is freed once its reader is done with it, and its buffers are poisoned
        // as they are freed: a reader that kept a pointer into a covered VARCHAR past its frame
        // would read the poison, and its query would not equal the non-indexed twin's.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, FRAME_ROWS);
        assertMemoryLeak(() -> {
            for (WorkerPoolMode mode : WorkerPoolMode.values()) {
                TestUtils.execute(
                        new TestWorkerPool(4, mode),
                        (engine, compiler, sqlExecutionContext) -> {
                            createCoveredTables(engine, sqlExecutionContext);
                            PageFrameMemoryPool.setPoisoningFreedCoveredBuffers(true);
                            try {
                                for (String query : VALUE_KEEPING_QUERIES) {
                                    TestUtils.assertSqlCursors(compiler, sqlExecutionContext, String.format(query, "ref"), String.format(query, "cov"), LOG);
                                }
                            } finally {
                                PageFrameMemoryPool.setPoisoningFreedCoveredBuffers(false);
                            }
                            assertEquals(mode.toString(), 0, reduceQueueCoveredBytes(engine));
                            dropCoveredTables(engine, sqlExecutionContext);
                        },
                        configuration,
                        LOG
                );
            }
        });
    }

    @Test
    public void testZeroBudgetPoolHoldsTheBoundCoveredFramesAlone() throws Exception {
        // A zero-budget pool, as a reduce slot's, keeps the covered frames its frame memory and its
        // records are bound to, and frees a frame once none of them is on it.
        assertMemoryLeak(() -> {
            createPoolTable();
            try (
                    RecordCursorFactory factory = select("SELECT sym, payload, v FROM t WHERE sym = 'A'");
                    PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                    PageFrameAddressCache addressCache = new PageFrameAddressCache();
                    PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, 0L);
                    PageFrameMemoryRecord recordA = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
                    PageFrameMemoryRecord recordB = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_B_LETTER);
                    PageFrameMemoryRecord frameRecord = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)
            ) {
                final int frameCount = buildAddressCache(factory, cursor, addressCache);
                assertTrue(frameCount > 5);
                pool.of(addressCache);
                recordA.of(cursor);
                recordB.of(cursor);
                frameRecord.of(cursor);
                final long frameBytes = coveredFrameBytes();

                frameRecord.init(pool.navigateTo(0));
                assertEquals(1, pool.getCoveredFrameCount());
                pool.navigateTo(1, recordA);
                pool.navigateTo(2, recordB);
                assertEquals(3, pool.getCoveredFrameCount());
                assertEquals(3 * frameBytes, pool.getCoveredCachedBytes());
                // record A moves on: frame 1 is freed
                pool.navigateTo(3, recordA);
                assertEquals(3, pool.getCoveredFrameCount());
                assertFrameValues(frameRecord, addressCache, 0);
                assertFrameValues(recordA, addressCache, 3);
                assertFrameValues(recordB, addressCache, 2);
                // so does the frame memory: frame 0 is freed
                frameRecord.init(pool.navigateTo(4));
                assertEquals(3, pool.getCoveredFrameCount());
                assertFrameValues(frameRecord, addressCache, 4);
                // both records on one frame
                pool.navigateTo(4, recordA);
                pool.navigateTo(4, recordB);
                assertEquals(1, pool.getCoveredFrameCount());
                assertEquals(frameBytes, pool.getCoveredCachedBytes());
                assertFrameValues(recordB, addressCache, 4);
                // record B's move freed frame 2, which fails record A's fast path, so A binds its
                // frame again
                pool.navigateTo(4, recordA);
                assertEquals(1, pool.getCoveredFrameCount());
                assertFrameValues(recordA, addressCache, 4);
                // record B bound through the frame memory, which does not hold it on frame 5,
                // while its letter holds frame 4: the navigation binds it to frame 5
                recordB.init(pool.navigateTo(5));
                pool.navigateTo(5, recordB);
                assertEquals(2, pool.getCoveredFrameCount());
                assertFrameValues(recordB, addressCache, 5);
                assertFrameValues(recordA, addressCache, 4);
                // the frame memory releases frame 5, which record B keeps
                pool.releaseFrameMemory();
                pool.navigateTo(6, recordA);
                assertEquals(2, pool.getCoveredFrameCount());
                pool.navigateTo(6, recordB);
                assertEquals("frame 5, released by the frame memory and left by record B, is freed", 1, pool.getCoveredFrameCount());
                assertFrameValues(recordB, addressCache, 6);
                // the release frees the bound frames too
                pool.releaseParquetBuffers();
                assertEquals(0, pool.getCoveredFrameCount());
                assertEquals(0, pool.getCoveredCachedBytes());
                // and a record's next navigation decodes its frame again
                pool.navigateTo(1, recordA);
                assertFrameValues(recordA, addressCache, 1);
                assertEquals(1, pool.getCoveredFrameCount());
                // a record bound through the frame memory alone, its letter bound to nothing
                pool.releaseParquetBuffers();
                recordB.init(pool.navigateTo(2));
                pool.navigateTo(2, recordB);
                assertEquals(1, pool.getCoveredFrameCount());
                pool.releaseFrameMemory();
                pool.navigateTo(3, recordA);
                assertEquals("record B keeps frame 2", 2, pool.getCoveredFrameCount());
                assertFrameValues(recordB, addressCache, 2);
            }
        });
    }

    // createPoolTable(): the rows of frame 1, x = 24 + 2 * r + 1
    private static void assertDeclaredRows(PageFrameMemoryRecord record, int... rows) {
        final StringSink sink = new StringSink();
        for (int r : rows) {
            record.setRowIndex(r);
            final long x = 24 + 2L * r + 1;
            sink.clear();
            sink.put(record.getVarcharA(1));
            assertEquals("row " + r, String.format("payload-%012d", x), sink.toString());
            assertEquals("row " + r, x, record.getLong(2));
        }
    }

    private static void assertFrameValues(PageFrameMemoryRecord record, PageFrameAddressCache addressCache, int frameIndex) {
        final StringSink sink = new StringSink();
        // createPoolTable(): frame f holds the 'A' rows of day f, x = 24 * f + 1 .. 24 * f + 24 with
        // the odd x in 'A', 12 of them
        final long frameSize = addressCache.getFrameSize(frameIndex);
        assertEquals(12, frameSize);
        for (int r = 0; r < frameSize; r++) {
            record.setRowIndex(r);
            final long x = 24L * frameIndex + 2L * r + 1;
            sink.clear();
            sink.put(record.getVarcharA(1));
            assertEquals("frame " + frameIndex + ", row " + r, String.format("payload-%012d", x), sink.toString());
            assertEquals("frame " + frameIndex + ", row " + r, x, record.getLong(2));
        }
    }

    // Asserts that every frame of the query is stable but the single-key covered and the Parquet
    // ones, and returns how many single-key covered frames there are.
    private static int assertStability(String query) throws Exception {
        try (
                RecordCursorFactory factory = select(query);
                PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                PageFrameAddressCache addressCache = new PageFrameAddressCache();
                PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, 0L);
                PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
                PageFrameMemoryRecord frameRecord = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_B_LETTER)
        ) {
            final int frameCount = buildAddressCache(factory, cursor, addressCache);
            assertTrue(query, frameCount > 0);
            pool.of(addressCache);
            record.of(cursor);
            frameRecord.of(cursor);
            final IntHashSet filterColumns = new IntHashSet();
            filterColumns.add(2);
            int coveredFrames = 0;
            for (int f = 0; f < frameCount; f++) {
                final boolean isCovered = addressCache.isFrameSingleKeyCovered(f);
                final boolean isStable = !isCovered && addressCache.getFrameFormat(f) == PartitionFormat.NATIVE;
                coveredFrames += isCovered ? 1 : 0;
                // as a filter's first pass reads the frame, then as its rows are read in full
                assertEquals(query, isStable, pool.navigateTo(f, filterColumns).isStable());
                final PageFrameMemory frameMemory = pool.navigateTo(f);
                assertEquals(query, isStable, frameMemory.isStable());
                frameRecord.init(frameMemory);
                assertEquals(query, isStable, frameRecord.isStable());
                pool.navigateTo(f, record);
                assertEquals(query, isStable, record.isStable());
                // the filter's first pass again, served by the frame read in full
                assertEquals(query, isStable, pool.navigateTo(f, filterColumns).isStable());
            }
            return coveredFrames;
        }
    }

    private static int buildAddressCache(RecordCursorFactory factory, PageFrameCursor cursor, PageFrameAddressCache addressCache) {
        addressCache.of(factory.getMetadata(), cursor.getColumnMapping(), cursor.isExternal());
        int frameCount = 0;
        PageFrame frame;
        while ((frame = cursor.next(0)) != null) {
            addressCache.add(frameCount++, frame);
        }
        return frameCount;
    }

    // The bytes of one frame of createPoolTable()'s 'A' rows, decoded alone.
    private static long coveredFrameBytes() throws Exception {
        try (
                RecordCursorFactory factory = select("SELECT sym, payload, v FROM t WHERE sym = 'A'");
                PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                PageFrameAddressCache addressCache = new PageFrameAddressCache()
        ) {
            buildAddressCache(factory, cursor, addressCache);
            final long bytes = decodedFrameBytes(addressCache, 0);
            // every frame holds 12 rows of 20-byte values, which decode into as many bytes
            assertEquals(bytes, decodedFrameBytes(addressCache, 1));
            return bytes;
        }
    }

    private static void createCoveredTables(CairoEngine engine, SqlExecutionContext sqlExecutionContext) throws SqlException {
        engine.execute(
                "CREATE TABLE cov (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (s, px), px DOUBLE, s VARCHAR)" +
                        " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                sqlExecutionContext
        );
        engine.execute(
                "CREATE TABLE ref (ts TIMESTAMP, sym SYMBOL, px DOUBLE, s VARCHAR) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                sqlExecutionContext
        );
        // 64 day partitions of 2,000 rows, half of them 'S0', which a covered scan cuts into
        // 200-row frames: 320 of them
        final String rows = "SELECT" +
                " ('2024-01-01'::TIMESTAMP + (x - 1) * " + (86_400_000_000L / ROWS_PER_PARTITION) + "L)::timestamp ts," +
                " ('S' || ((x - 1) % 2))::symbol sym," +
                " (x % 997)::double px," +
                " ('v' || (x % 50))::varchar s" +
                " FROM long_sequence(" + (long) PARTITIONS * ROWS_PER_PARTITION + ")";
        engine.execute("INSERT INTO cov " + rows, sqlExecutionContext);
        engine.execute("INSERT INTO ref " + rows, sqlExecutionContext);
        engine.releaseAllWriters();
    }

    // 24 rows a day over 10 days, 'A' and 'B' by turns, with 20-byte VARCHARs, stored out of the
    // aux vector, so that a covered scan of 'A' cuts a 12-row frame per day, each of which decodes
    // into as many bytes
    private static void createPoolTable() throws SqlException {
        execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (payload, v), payload VARCHAR, v LONG)" +
                " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO t SELECT" +
                " '2024-01-01'::TIMESTAMP + (x - 1) * 3_600_000_000L," +
                " CASE WHEN x % 2 = 1 THEN 'A' ELSE 'B' END," +
                " 'payload-' || lpad(x::varchar, 12, '0')," +
                " x" +
                " FROM long_sequence(240)");
        engine.releaseAllWriters();
    }

    // The bytes a fresh pool holds once it has decoded the frame.
    private static long decodedFrameBytes(PageFrameAddressCache addressCache, int frameIndex) {
        try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, 0L)) {
            pool.of(addressCache);
            pool.navigateTo(frameIndex);
            return pool.getCoveredCachedBytes();
        }
    }

    private static void dropCoveredTables(CairoEngine engine, SqlExecutionContext sqlExecutionContext) throws SqlException {
        engine.execute("DROP TABLE cov", sqlExecutionContext);
        engine.execute("DROP TABLE ref", sqlExecutionContext);
    }

    // createCoveredTables(): the 'S0' rows, x odd, but those with x % 997 == 0, whose px is 0
    private static long expectedFilterRowCount() {
        long count = 0;
        for (long x = 1, n = (long) PARTITIONS * ROWS_PER_PARTITION; x <= n; x += 2) {
            if (x % 997 != 0) {
                count++;
            }
        }
        return count;
    }

    private static long reduceQueueCoveredBytes(CairoEngine engine) {
        final MessageBus bus = engine.getMessageBus();
        long bytes = 0;
        for (int shard = 0, n = bus.getPageFrameReduceShardCount(); shard < n; shard++) {
            final RingQueue<PageFrameReduceTask> queue = bus.getPageFrameReduceQueue(shard);
            for (int slot = 0, m = queue.getCycle(); slot < m; slot++) {
                bytes += queue.get(slot).getFrameMemoryPool().getCoveredCachedBytes();
            }
        }
        return bytes;
    }

    private static int reduceQueueCoveredFrames(CairoEngine engine) {
        final MessageBus bus = engine.getMessageBus();
        int frames = 0;
        for (int shard = 0, n = bus.getPageFrameReduceShardCount(); shard < n; shard++) {
            final RingQueue<PageFrameReduceTask> queue = bus.getPageFrameReduceQueue(shard);
            for (int slot = 0, m = queue.getCycle(); slot < m; slot++) {
                frames += queue.get(slot).getFrameMemoryPool().getCoveredFrameCount();
            }
        }
        return frames;
    }

    // A frame memory with a format and nothing else, which leaves isStable() to its default.
    private static class FormatOnlyFrameMemory implements PageFrameMemory {
        private final byte format;

        FormatOnlyFrameMemory(byte format) {
            this.format = format;
        }

        @Override
        public long getAuxPageAddress(int columnIndex) {
            return 0;
        }

        @Override
        public DirectLongList getAuxPageAddresses() {
            return null;
        }

        @Override
        public DirectLongList getAuxPageSizes() {
            return null;
        }

        @Override
        public int getColumnCount() {
            return 0;
        }

        @Override
        public int getColumnOffset() {
            return 0;
        }

        @Override
        public byte getFrameFormat() {
            return format;
        }

        @Override
        public int getFrameIndex() {
            return 0;
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return 0;
        }

        @Override
        public DirectLongList getPageAddresses() {
            return null;
        }

        @Override
        public long getPageSize(int columnIndex) {
            return 0;
        }

        @Override
        public DirectLongList getPageSizes() {
            return null;
        }

        @Override
        public PageFrameMemoryPool getPool() {
            return null;
        }

        @Override
        public long getRowIdOffset() {
            return 0;
        }

        @Override
        public int getSourceColumnType(int columnIndex) {
            return -1;
        }

        @Override
        public boolean hasColumnTops() {
            return false;
        }

        @Override
        public boolean hasColumnTypeCasts() {
            return false;
        }

        @Override
        public boolean populateRemainingColumns(IntHashSet filterColumnIndexes, DirectLongList filteredRows, boolean fillWithNulls) {
            return false;
        }
    }
}
