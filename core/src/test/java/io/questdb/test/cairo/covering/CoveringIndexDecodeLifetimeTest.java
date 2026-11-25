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
import io.questdb.cairo.SqlJitMode;
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
 * The lifetime of the buffers a covered (single-key covering-index) frame is decoded into. A pool
 * keeps them until its query ends, so that a record over a covered frame is stable, as one over a
 * native frame is, and a cursor that reads rows back finds every frame it decoded. The pool of a
 * reduce queue task cannot keep them: the queue is shared by every query, and a task carries one
 * frame at a time. Its pool frees a covered frame once the cursor has collected the task, and a
 * record over it is not stable, so a reader that keeps a value past the frame, above an Async
 * Filter, copies it. So no slot keeps a covered frame past its collection, whichever queries share
 * the queue, and a covered Async Filter scan holds the frames it has in flight.
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
            // Serial readers over the rows of a limited async filter, whose frames the reduce
            // queue's tasks decode and free as the cursor collects them, while the readers go on:
            // first(), last() and arg_max() keep a value of an earlier frame, the map of a VARCHAR
            // key keeps its keys. The short VARCHAR is inlined into the aux vector, the long one
            // lives in the data vector.
            "SELECT first(s), last(s), count() FROM (SELECT ts, s FROM %s WHERE sym = 'S0' AND px > 0.5 LIMIT 5000)",
            "SELECT first(l), last(l), count() FROM (SELECT ts, l FROM %s WHERE sym = 'S0' AND px > 0.5 LIMIT 1_000_000)",
            "SELECT s, count() FROM (SELECT s FROM %s WHERE sym = 'S0' AND px > 0.5 LIMIT 1_000_000) ORDER BY s",
            "SELECT l, count() FROM (SELECT l FROM %s WHERE sym = 'S0' AND px > 0.5 LIMIT 1_000_000) ORDER BY l",
            "SELECT arg_max(l, px), count() FROM (SELECT l, px FROM %s WHERE sym = 'S0' AND px > 0.5 LIMIT 1_000_000)",
            "SELECT ts, first(l), last(l), count() FROM %s WHERE sym = 'S0' AND px > 0.5 SAMPLE BY 1d FILL(PREV)",
            "SELECT ts, s, count(s) OVER (ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) FROM %s WHERE sym = 'S0' AND px > 990",
            "SELECT string_agg(s, ',') FROM (SELECT ts, s FROM %s WHERE sym = 'S0' AND px > 990)",
    };

    @Test
    public void testCoveredDecodeReleasesItsTrackerCharge() throws Exception {
        // A covered frame's buffers charge the pool's tracker while they live. A pool that frees its
        // covered frames with their frame, as a reduce queue task's does, releases the charge with
        // them, so that the query's memory limit sees the frames the query holds; a pool that keeps
        // them for its query holds the charge of every frame it has decoded until its query boundary
        // releases it. A free after the tracker has been reconciled, at its query's end, is
        // global-only: the reconciliation released the charge already.
        assertMemoryLeak(() -> {
            createPoolTable();
            final long frameBytes = coveredFrameBytes();
            try (
                    PerQueryMemoryTrackerProvider provider = new PerQueryMemoryTrackerProvider(configuration);
                    RecordCursorFactory factory = select("SELECT sym, payload, v FROM t WHERE sym = 'A'");
                    PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                    PageFrameAddressCache addressCache = new PageFrameAddressCache();
                    PageFrameMemoryPool freeingPool = new PageFrameMemoryPool(configuration, 0L);
                    PageFrameMemoryPool keepingPool = new PageFrameMemoryPool(configuration, 0L);
                    PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)
            ) {
                final int frameCount = buildAddressCache(factory, cursor, addressCache);
                freeingPool.setKeepsCoveredFramesForQuery(false);
                record.of(cursor);
                MemoryTracker tracker = provider.acquire(AllowAllSecurityContext.INSTANCE, 1, MemoryTrackerWorkload.QUERY);
                try {
                    freeingPool.setMemoryTracker(tracker);
                    freeingPool.of(addressCache);
                    for (int f = 0; f < frameCount; f++) {
                        freeingPool.navigateTo(f, record);
                        assertFrameValues(record, addressCache, f);
                        assertEquals("charge at frame " + f, frameBytes, tracker.getUsed());
                        // as the reduce queue task releases its frame once collected
                        freeingPool.releaseParquetBuffers();
                        assertEquals(0, freeingPool.getCoveredCachedBytes());
                        assertEquals("the free released the charge", 0, tracker.getUsed());
                    }

                    // a frame that outlives its query's tracker
                    freeingPool.navigateTo(0, record);
                    assertEquals(frameBytes, tracker.getUsed());
                    final long globalBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_INDEX_READER);
                    tracker.reconcileCovered();
                    assertEquals("the reconciliation released the charge", 0, tracker.getUsed());
                    freeingPool.releaseParquetBuffers();
                    assertEquals("the late free released no charge again", 0, tracker.getUsed());
                    assertEquals(globalBefore - frameBytes, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_INDEX_READER));
                } finally {
                    freeingPool.clear();
                    tracker.close();
                }

                tracker = provider.acquire(AllowAllSecurityContext.INSTANCE, 2, MemoryTrackerWorkload.QUERY);
                try {
                    keepingPool.setMemoryTracker(tracker);
                    keepingPool.of(addressCache);
                    for (int f = 0; f < frameCount; f++) {
                        keepingPool.navigateTo(f, record);
                        // the release of a reducer keeps the covered frames
                        keepingPool.releaseParquetBuffers();
                        assertEquals("charge at frame " + f, (f + 1) * frameBytes, tracker.getUsed());
                    }
                    // the query boundary frees every frame, and its charge with it
                    keepingPool.clear();
                    assertEquals(0, keepingPool.getCoveredCachedBytes());
                    assertEquals(0, tracker.getUsed());
                } finally {
                    keepingPool.clear();
                    tracker.close();
                }
            }
        });
    }

    @Test
    public void testCoveredFrameStabilityFollowsItsPool() throws Exception {
        // A record over a single-key covered frame is stable when its pool keeps the frame until its
        // query ends, as every pool but a reduce queue task's does; over the frames of a pool that
        // frees them with their frame, neither the frame memory nor a record is stable. A multi-key
        // covered frame, decoded eagerly by the page frame cursor, which frees it at close, is stable
        // in either pool, and so is a native frame; a decoded Parquet frame never is.
        assertMemoryLeak(() -> {
            createPoolTable();
            execute("CREATE TABLE plain (ts TIMESTAMP, sym SYMBOL, payload VARCHAR, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO plain SELECT * FROM t");
            execute("CREATE TABLE pq (ts TIMESTAMP, sym SYMBOL, payload VARCHAR, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO pq SELECT * FROM t");
            // all days but the last, the active partition
            execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-10'");
            engine.releaseAllWriters();
            for (boolean isKeepingCoveredFrames : new boolean[]{true, false}) {
                assertEquals(10, assertStability("SELECT sym, payload, v FROM t WHERE sym = 'A'", isKeepingCoveredFrames));
                // per-key frames are single-key covered ones, merged frames are not
                assertStability("SELECT sym, payload, v FROM t WHERE sym IN ('A', 'B')", isKeepingCoveredFrames);
                assertEquals(0, assertStability("SELECT sym, payload, v FROM plain", isKeepingCoveredFrames));
                assertEquals(0, assertStability("SELECT sym, payload, v FROM pq", isKeepingCoveredFrames));
            }
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
    public void testDecodeAheadFoldsOverCoveredFrames() throws Exception {
        // Rare keys over many partitions: fewer rows than the covering scan reduces on the query's
        // thread, in more frames than one thread decodes, so the workers decode the frames ahead of
        // a fold on the query's thread. A single key's frames are decoded ahead, each kept by the
        // pool that decoded it until the query ends, and the fold reads it there; merged frames of
        // two keys, decoded as the scan builds them, are not, and the fold reads them through its
        // own pool. Both equal the non-indexed twin, with the freed covered buffers poisoned.
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
    public void testFailedCoveredDecodeLeavesNoChargeOnceReleased() throws Exception {
        // A covered decode that breaches the query's memory limit fails with the limit's error: a
        // record's decode of the frame, its decode of the declared rows alone, and the frame
        // memory's decode of the filter's columns alike. A pool that frees its covered frames with
        // their frame frees what the failed decode allocated, and the charge with it, once the frame
        // is released, as the reduce queue task releases it when the cursor collects the failed
        // frame. The pool decodes the frame once the limit allows.
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
                pool.setKeepsCoveredFramesForQuery(false);
                final MemoryTracker tracker = provider.acquire(AllowAllSecurityContext.INSTANCE, 1, MemoryTrackerWorkload.QUERY);
                try {
                    // the record's frame decode, its decode of declared rows, the frame memory's
                    // decode of the filter's columns
                    for (int path = 0; path < 3; path++) {
                        pool.setMemoryTracker(tracker);
                        pool.of(addressCache);
                        record.of(cursor);
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
                        // what the failed decode allocated stays charged until the frame is released
                        assertTrue(tracker.getUsed() <= frameBytes / 2);
                        assertEquals(pool.getCoveredCachedBytes(), tracker.getUsed());
                        pool.releaseParquetBuffers();
                        assertEquals(0, pool.getCoveredFrameCount());
                        assertEquals(0, pool.getCoveredCachedBytes());
                        assertEquals(0, tracker.getUsed());

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
    public void testKeepingPoolKeepsCoveredFramesUntilItsQueryEnds() throws Exception {
        // A pool that keeps covered frames for its query, as every pool but a reduce queue task's
        // does, keeps every covered frame it decodes until its query boundary, whatever its cache
        // budget, and through the release its reducers call after each frame: a reader that kept a
        // pointer into a frame reads it intact, and a record that comes back to a frame finds it
        // decoded, in the same buffers, so that a cursor that reads rows back in any order decodes
        // each frame once.
        assertMemoryLeak(() -> {
            createPoolTable();
            final long frameBytes = coveredFrameBytes();
            try (
                    RecordCursorFactory factory = select("SELECT sym, payload, v FROM t WHERE sym = 'A'");
                    PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                    PageFrameAddressCache addressCache = new PageFrameAddressCache();
                    // a cache budget of half a frame
                    PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, frameBytes / 2);
                    PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
                    PageFrameMemoryRecord frameRecord = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_B_LETTER)
            ) {
                final int frameCount = buildAddressCache(factory, cursor, addressCache);
                assertEquals(10, frameCount);
                // the full budget, as for a cursor whose rows are read back in any order
                pool.of(addressCache, ParquetDecodeHint.SCATTERED);
                record.of(cursor);
                frameRecord.of(cursor);
                // a reader bound to the frame memory, as a reducer's record is
                frameRecord.init(pool.navigateTo(0));
                assertTrue(frameRecord.isStable());
                final long[] valueAddresses = new long[frameCount];
                for (int f = 0; f < frameCount; f++) {
                    pool.navigateTo(f, record);
                    assertTrue(record.isStable());
                    assertFrameValues(record, addressCache, f);
                    valueAddresses[f] = record.getPageAddress(2);
                    // as a reducer releases the decoded frames after each frame
                    pool.releaseParquetBuffers();
                    assertEquals(f + 1, pool.getCoveredFrameCount());
                    assertEquals((f + 1) * frameBytes, pool.getCoveredCachedBytes());
                }
                // the frame the reader was bound to, before the frame memory was released
                assertFrameValues(frameRecord, addressCache, 0);
                // the record comes back to every frame, in reverse order, and finds it decoded
                for (int f = frameCount - 1; f >= 0; f--) {
                    pool.navigateTo(f, record);
                    assertEquals(valueAddresses[f], record.getPageAddress(2));
                    assertFrameValues(record, addressCache, f);
                }
                assertEquals(frameCount, pool.getCoveredFrameCount());
                assertEquals(frameCount * frameBytes, pool.getCoveredCachedBytes());
                // the query boundary frees them
                pool.of(addressCache);
                assertEquals(0, pool.getCoveredFrameCount());
                assertEquals(0, pool.getCoveredCachedBytes());
                // and so does clear()
                pool.navigateTo(3, record);
                assertFrameValues(record, addressCache, 3);
                assertEquals(1, pool.getCoveredFrameCount());
                pool.clear();
                assertEquals(0, pool.getCoveredFrameCount());
                assertEquals(0, pool.getCoveredCachedBytes());
            }
        });
    }

    @Test
    public void testOutOfMemoryAtEveryCoveredAllocationLeavesThePoolUsable() throws Exception {
        // An allocation that fails anywhere in the decode of a covered frame, in the reopen of a
        // parked shell's address lists, in the index reader's cursor or in a column buffer, leaves
        // the pool usable: the frame decodes in full once memory allows, for the frame memory and
        // for a record alike, and the release frees what the failed decode allocated, in a pool
        // that frees its covered frames with their frame and in one that keeps them for its query.
        assertMemoryLeak(() -> {
            createPoolTable();
            try (
                    RecordCursorFactory factory = select("SELECT sym, payload, v FROM t WHERE sym = 'A'");
                    PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                    PageFrameAddressCache addressCache = new PageFrameAddressCache();
                    PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
                    PageFrameMemoryRecord frameRecord = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_B_LETTER)
            ) {
                buildAddressCache(factory, cursor, addressCache);
                record.of(cursor);
                frameRecord.of(cursor);
                for (boolean isKeepingCoveredFrames : new boolean[]{true, false}) {
                    try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, 0L)) {
                        pool.setKeepsCoveredFramesForQuery(isKeepingCoveredFrames);
                        for (boolean isRecord : new boolean[]{true, false}) {
                            int failures = 0;
                            boolean hasDecodedUnderLimit = false;
                            for (int slack = 0; !hasDecodedUnderLimit; slack += 16) {
                                // frame 0 decoded, and, in a pool that frees its covered frames,
                                // released: its parked shell is what the next decode reopens
                                pool.of(addressCache);
                                pool.navigateTo(0, record);
                                if (!isKeepingCoveredFrames) {
                                    pool.releaseParquetBuffers();
                                    assertEquals(1, pool.getCoveringShellCount());
                                }
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
                                if (isKeepingCoveredFrames) {
                                    // the query boundary frees the frames
                                    pool.of(addressCache);
                                } else {
                                    pool.releaseParquetBuffers();
                                }
                                assertEquals(0, pool.getCoveredFrameCount());
                                assertEquals(0, pool.getCoveredCachedBytes());
                            }
                            // the decode allocates in several places, each of which failed at some limit
                            assertTrue("failures: " + failures, failures > 3);
                        }
                    }
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
                // every frame stays, whatever the budget
                for (int f = 0; f < 600; f++) {
                    pool.navigateTo(f);
                }
                assertEquals(600, pool.getCoveredFrameCount());
                pool.clear();
                assertEquals(0, pool.getCoveredFrameCount());
                assertEquals(0, pool.getCoveredCachedBytes());
                assertEquals(COVERING_SHELL_CAP, pool.getCoveringShellCount());
                // the next query's frames take the shells
                pool.of(addressCache);
                pool.navigateTo(0);
                assertEquals(COVERING_SHELL_CAP - 1, pool.getCoveringShellCount());
            }
        });
    }

    @Test
    public void testPoolThatFreesCoveredFramesKeepsNoDecodeMemoryPastTheFrame() throws Exception {
        // A pool that frees its covered frames with their frame, as a reduce queue task's, frees all
        // of a covered frame's native memory once the frame is released: its buffers, the address
        // lists of its shell, which it parks for the next frame, and the scratch of its compressed
        // runs. A pool that keeps them, with its shells, as an atom's, holds every frame and the
        // scratch until its query boundary.
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
                try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, 0L)) {
                    pool.setKeepsCoveredFramesForQuery(false);
                    pool.of(addressCache);
                    final long nativeDefaultBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
                    for (int f = 0; f < frameCount; f++) {
                        assertFalse(pool.navigateTo(f).isStable());
                        // the frame's buffers, in the shell the previous frame left, and the run scratch
                        assertEquals(1, pool.getCoveredFrameCount());
                        assertEquals(0, pool.getCoveringShellCount());
                        assertEquals(pool.getCoveredCachedBytes() + RUN_DECODE_SCRATCH_BYTES, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_INDEX_READER) - before);
                        pool.releaseParquetBuffers();
                        assertEquals(0, pool.getCoveredFrameCount());
                        assertEquals(0, pool.getCoveredCachedBytes());
                        assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_INDEX_READER));
                        // the shell is parked, its address lists freed
                        assertEquals(1, pool.getCoveringShellCount());
                        assertEquals(nativeDefaultBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT));
                    }
                    pool.clear();
                    assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_INDEX_READER));
                }
                try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, 0L)) {
                    pool.setRetainsCoveringShells(true);
                    pool.of(addressCache);
                    for (int f = 0; f < frameCount; f++) {
                        assertTrue(pool.navigateTo(f).isStable());
                        pool.releaseParquetBuffers();
                        assertEquals(f + 1, pool.getCoveredFrameCount());
                        assertEquals(pool.getCoveredCachedBytes() + RUN_DECODE_SCRATCH_BYTES, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_INDEX_READER) - before);
                    }
                    pool.clear();
                    assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_INDEX_READER));
                    // the shells keep their address lists for the next query
                    assertEquals(frameCount, pool.getCoveringShellCount());
                }
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
    public void testSortReadsEachCoveredFrameBackFromOneDecode() throws Exception {
        // A sort over a covered Async Filter reads its rows back through the filter cursor's own
        // pool, in the sort's order, which goes from frame to frame for every px value. The pool
        // keeps every covered frame it decodes until the query ends, so that each frame decodes
        // once, whatever the cache budget: here a budget far smaller than a frame. The sort declares
        // the rows it reads, few enough per frame for the pool to decode them alone, and to count
        // the decodes. A pool that freed covered frames under its budget would decode a frame again
        // at every return to it.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, FRAME_ROWS);
        setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 1024);
        assertMemoryLeak(() -> TestUtils.execute(
                new TestWorkerPool(4, WorkerPoolMode.LEGACY),
                (engine, compiler, sqlExecutionContext) -> {
                    createCoveredTables(engine, sqlExecutionContext);
                    final String query = "SELECT ts, s, px FROM %s WHERE sym = 'S0' AND px > 900 ORDER BY px, ts";
                    final StringSink sink = new StringSink();
                    TestUtils.printSql(compiler, sqlExecutionContext, "EXPLAIN " + String.format(query, "cov"), sink);
                    TestUtils.assertContains(sink, "Encode sort light");
                    TestUtils.assertContains(sink, "Async JIT Filter");
                    TestUtils.assertContains(sink, "CoveringIndex on: sym");
                    final StringSink expected = new StringSink();
                    TestUtils.printSql(compiler, sqlExecutionContext, String.format(query, "ref"), expected);
                    PageFrameMemoryPool.setPoisoningFreedCoveredBuffers(true);
                    try {
                        for (int pass = 0; pass < 2; pass++) {
                            final long decodesBefore = PageFrameMemoryPool.getCoveredRowDecodeCount();
                            sink.clear();
                            TestUtils.printSql(compiler, sqlExecutionContext, String.format(query, "cov"), sink);
                            TestUtils.assertEquals(expected, sink);
                            assertEquals("pass " + pass, framesWithPxAbove(900), PageFrameMemoryPool.getCoveredRowDecodeCount() - decodesBefore);
                        }
                    } finally {
                        PageFrameMemoryPool.setPoisoningFreedCoveredBuffers(false);
                    }
                    assertEquals(0, reduceQueueCoveredBytes(engine));
                },
                configuration,
                LOG
        ));
    }

    @Test
    public void testValueKeepingReadersCopyCoveredValues() throws Exception {
        // A covered frame of a reduce queue task is freed once the cursor has collected it, and its
        // buffers are poisoned as they are freed: a reader above the Async Filter that kept a pointer
        // into a covered VARCHAR past its frame would read the poison, and its query would not equal
        // the non-indexed twin's. The workers of a covered GROUP BY, and a cursor that reads rows
        // back, keep their covered frames until the query ends, and their readers keep pointers into
        // them, which stay valid. With JIT filters and without.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, FRAME_ROWS);
        assertMemoryLeak(() -> {
            for (WorkerPoolMode mode : WorkerPoolMode.values()) {
                TestUtils.execute(
                        new TestWorkerPool(4, mode),
                        (engine, compiler, sqlExecutionContext) -> {
                            createCoveredTables(engine, sqlExecutionContext);
                            PageFrameMemoryPool.setPoisoningFreedCoveredBuffers(true);
                            try {
                                for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                                    sqlExecutionContext.setJitMode(jitMode);
                                    for (String query : VALUE_KEEPING_QUERIES) {
                                        TestUtils.assertSqlCursors(compiler, sqlExecutionContext, String.format(query, "ref"), String.format(query, "cov"), LOG);
                                    }
                                }
                            } finally {
                                PageFrameMemoryPool.setPoisoningFreedCoveredBuffers(false);
                                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
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

    // Asserts that every frame of the query is stable but the Parquet ones and, in a pool that does
    // not keep covered frames for its query, the single-key covered ones, and returns how many
    // single-key covered frames there are.
    private static int assertStability(String query, boolean isKeepingCoveredFrames) throws Exception {
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
            pool.setKeepsCoveredFramesForQuery(isKeepingCoveredFrames);
            pool.of(addressCache);
            record.of(cursor);
            frameRecord.of(cursor);
            final IntHashSet filterColumns = new IntHashSet();
            filterColumns.add(2);
            int coveredFrames = 0;
            for (int f = 0; f < frameCount; f++) {
                final boolean isCovered = addressCache.isFrameSingleKeyCovered(f);
                final boolean isStable = (isKeepingCoveredFrames || !isCovered) && addressCache.getFrameFormat(f) == PartitionFormat.NATIVE;
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
                if (!isKeepingCoveredFrames) {
                    // as a reduce queue task releases its frame once collected
                    pool.releaseParquetBuffers();
                }
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
                "CREATE TABLE cov (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (s, px, l), px DOUBLE, s VARCHAR, l VARCHAR)" +
                        " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                sqlExecutionContext
        );
        engine.execute(
                "CREATE TABLE ref (ts TIMESTAMP, sym SYMBOL, px DOUBLE, s VARCHAR, l VARCHAR) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                sqlExecutionContext
        );
        // 64 day partitions of 2,000 rows, half of them 'S0', which a covered scan cuts into
        // 200-row frames: 320 of them. s is short enough to be inlined into the aux vector, l is
        // not, so its bytes live in the data vector.
        final String rows = "SELECT" +
                " ('2024-01-01'::TIMESTAMP + (x - 1) * " + (86_400_000_000L / ROWS_PER_PARTITION) + "L)::timestamp ts," +
                " ('S' || ((x - 1) % 2))::symbol sym," +
                " (x % 997)::double px," +
                " ('v' || (x % 50))::varchar s," +
                " ('a-longer-value-' || (x % 50) || '-past-the-inline-limit')::varchar l" +
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

    // createCoveredTables(): the covered frames of the 'S0' rows, x odd, that hold a row whose px,
    // x % 997, is above pxLo. A partition holds x = 2,000 p + 1 .. 2,000 p + 2,000, whose 1,000 odd
    // x the scan cuts into five 200-row frames.
    private static int framesWithPxAbove(int pxLo) {
        int frames = 0;
        for (long p = 0; p < PARTITIONS; p++) {
            for (long k = 0; k < ROWS_PER_PARTITION / 2 / FRAME_ROWS; k++) {
                final long xLo = ROWS_PER_PARTITION * p + 2L * FRAME_ROWS * k + 1;
                for (long x = xLo, xHi = xLo + 2L * FRAME_ROWS; x < xHi; x += 2) {
                    if (x % 997 > pxLo) {
                        frames++;
                        break;
                    }
                }
            }
        }
        return frames;
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
