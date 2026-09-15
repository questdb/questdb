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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.parquet.ParquetDecoder;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.LimitedMemoryTracker;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class PageFrameAddressCacheTest extends AbstractCairoTest {
    private static final String FILTERED_ROWS = """
            x\ts\tk\tts
            1\t1\tb\t1970-01-01T00:00:00.000000Z
            10\t10\tb\t1970-01-01T09:00:00.000000Z
            """;

    @Test
    public void testFailedOpenReleasesTrackedCacheFilterOnExcludedValues() throws Exception {
        assertFailedOpenReleasesTrackedCache(
                1,
                "SELECT * FROM t WHERE k != 'a' AND s ~ $1",
                FILTERED_ROWS,
                "FilterOnExcludedValues",
                "filter: s ~ "
        );
    }

    @Test
    public void testFailedOpenReleasesTrackedCacheFilterOnSubQuery() throws Exception {
        assertFailedOpenReleasesTrackedCache(
                1,
                "SELECT * FROM t WHERE k IN (SELECT k FROM t WHERE x = 1) AND s ~ $1",
                FILTERED_ROWS,
                "FilterOnSubQuery",
                "filter: s ~ "
        );
    }

    @Test
    public void testFailedOpenReleasesTrackedCacheFilterOnValues() throws Exception {
        assertFailedOpenReleasesTrackedCache(
                1,
                "SELECT * FROM t WHERE k IN ('a', 'b') AND s ~ $1",
                FILTERED_ROWS,
                "FilterOnValues",
                "and s ~ "
        );
    }

    @Test
    public void testFailedOpenReleasesTrackedCacheIndexedPageFrame() throws Exception {
        assertFailedOpenReleasesTrackedCache(
                1,
                "SELECT * FROM t WHERE k = 'b' AND s ~ $1",
                FILTERED_ROWS,
                "PageFrame",
                "Index forward scan on: k",
                "and s ~ "
        );
    }

    @Test
    public void testFailedOpenReleasesTrackedCacheOrderedSequence() throws Exception {
        assertFailedOpenReleasesTrackedCache(
                4,
                "SELECT * FROM t WHERE s ~ $1",
                FILTERED_ROWS,
                "Async Filter",
                "filter: s ~ "
        );
    }

    @Test
    public void testFailedOpenReleasesTrackedCacheUnorderedSequence() throws Exception {
        assertFailedOpenReleasesTrackedCache(
                4,
                "SELECT k, count() FROM t WHERE s ~ $1",
                """
                        k\tcount
                        b\t2
                        """,
                "Async Group By",
                "filter: s ~ "
        );
    }

    @Test
    public void testProjectedMixedFramesResolveBorrowedDecodersInBothDirections() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select x::int id, timestamp_sequence(0, 42_187_500) ts from long_sequence(6144)) timestamp(ts) partition by day");
            execute("insert into x values (0, '1970-01-04')");
            setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 32);
            execute("alter table x convert partition to parquet where ts != '1970-01-02'");
            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1);
                 PageFrameAddressCache cache = new PageFrameAddressCache()) {
                context.with(sqlExecutionContext.getSecurityContext(), null, null, -1, null);
                context.changePageFrameSizes(1, 1);
                try (RecordCursorFactory factory = select("select ts, id a, id b from x where ts < '1970-01-04'", context)) {
                    for (int order : new int[]{PartitionFrameCursorFactory.ORDER_ASC, PartitionFrameCursorFactory.ORDER_DESC}) {
                        try (PageFrameCursor frames = factory.getPageFrameCursor(context, order)) {
                            Assert.assertTrue(frames.supportsParquetDecoderLookup());
                            cache.of(factory.getMetadata(), frames);
                            ObjList<ParquetDecoder> decoders = new ObjList<>();
                            decoders.setAll(3, null);
                            PageFrame frame;
                            int count = 0;
                            while ((frame = frames.next()) != null) {
                                cache.add(count++, frame);
                                if (frame.getFormat() == PartitionFormat.PARQUET) {
                                    decoders.setQuick(frame.getPartitionIndex(), frame.getParquetDecoder());
                                }
                            }
                            Assert.assertTrue(count > 2048);
                            long totalRows = 0;
                            Assert.assertNotNull(decoders.getQuick(0));
                            Assert.assertNotNull(decoders.getQuick(2));
                            Assert.assertNotSame(decoders.getQuick(0), decoders.getQuick(2));
                            for (int i = 0; i < count; i++) {
                                int partition = Rows.toPartitionIndex(cache.getRowIdOffset(i));
                                totalRows += cache.getFrameSize(i);
                                Assert.assertFalse(cache.isFrameCovered(i));
                                if (partition == 1) {
                                    Assert.assertEquals(PartitionFormat.NATIVE, cache.getFrameFormat(i));
                                } else {
                                    Assert.assertEquals(PartitionFormat.PARQUET, cache.getFrameFormat(i));
                                    Assert.assertSame(decoders.getQuick(partition), cache.getParquetDecoder(i));
                                }
                            }
                            Assert.assertEquals(6144, totalRows);
                            cache.close();
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSparseParquetIndexTrackingAndBoundedDecoderShells() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select x::int id, timestamp_sequence(0, 1000) ts from long_sequence(32_768)) timestamp(ts) partition by day");
            execute("insert into x values (0, '1970-01-02')");
            setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 32);
            execute("alter table x convert partition to parquet where ts >= 0");
            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1);
                 LimitedMemoryTracker tracker = new LimitedMemoryTracker(0)) {
                context.with(sqlExecutionContext.getSecurityContext(), null, null, -1, null);
                context.changePageFrameSizes(32, 32);
                try (RecordCursorFactory factory = select("x where ts < '1970-01-02'", context);
                     PageFrameCursor frames = factory.getPageFrameCursor(context, PartitionFrameCursorFactory.ORDER_ASC);
                     PageFrameAddressCache cache = new PageFrameAddressCache();
                     PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
                     PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)) {
                    cache.setMemoryTracker(tracker);
                    cache.of(factory.getMetadata(), frames);
                    Assert.assertEquals(1024, fill(cache, frames));
                    Assert.assertTrue(cache.hasParquetFrames());
                    final long cacheBytes = tracker.getUsed();
                    RecordCursor.RowIdSource rows = (target, ignored) -> {
                        for (int i = 0; i < cache.getFrameCount(); i++) {
                            target.add(Rows.toRowID(i, 0));
                        }
                    };
                    // Row IDs fit; first reject counts, then reject the native slice index.
                    for (long extra : new long[]{8192, 12_288}) {
                        tracker.setLimit(cacheBytes + extra);
                        pool.setMemoryTracker(tracker);
                        pool.of(cache, ParquetDecodeHint.SCATTERED);
                        Assert.assertThrows(CairoException.class, () -> pool.setRecordAtRows(rows));
                        pool.releaseQueryResources();
                        Assert.assertEquals(cacheBytes, tracker.getUsed());
                    }
                    tracker.setLimit(0);
                    pool.setMemoryTracker(tracker);
                    pool.of(cache, ParquetDecodeHint.SCATTERED);
                    pool.setRecordAtRows(rows);
                    Assert.assertEquals(cacheBytes + 20_480, tracker.getUsed());
                    record.of(frames);
                    for (int pass = 0; pass < 2; pass++) {
                        for (int i = 0; i < cache.getFrameCount(); i++) {
                            int frame = pass == 0 ? i : cache.getFrameCount() - i - 1;
                            pool.navigateTo(frame, record);
                            record.setRowIndex(0);
                            Assert.assertEquals(frame * 32 + 1, record.getInt(0));
                            // Each declared frame keeps its row-filtered buffer beyond the
                            // SCATTERED 256-entry cap, so the backward pass decodes nothing.
                            Assert.assertEquals(pass == 0 ? i + 1 : cache.getFrameCount(), pool.getCachedFrameCount());
                        }
                    }
                    // A new query drops the declaration, so its frame count no longer raises
                    // the cap: full-frame decodes evict at 256 entries.
                    record.clear();
                    pool.of(cache, ParquetDecodeHint.SCATTERED);
                    for (int i = 0; i < cache.getFrameCount(); i++) {
                        pool.navigateTo(i, record);
                        record.setRowIndex(1);
                        Assert.assertEquals(i * 32 + 2, record.getInt(0));
                        Assert.assertEquals(Math.min(i + 1, 256), pool.getCachedFrameCount());
                    }
                    record.clear();
                    pool.releaseQueryResources();
                    Assert.assertEquals(cacheBytes, tracker.getUsed());
                    cache.close();
                    Assert.assertEquals(0, tracker.getUsed());
                }
            }
        });
    }

    @Test
    public void testTrackedFrameGrowthFailureAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select x::int id, timestamp_sequence(0, 1000) ts from long_sequence(4096)) timestamp(ts) partition by day");
            try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1);
                 LimitedMemoryTracker tracker = new LimitedMemoryTracker(1);
                 PageFrameAddressCache cache = new PageFrameAddressCache()) {
                context.with(sqlExecutionContext.getSecurityContext(), null, null, -1, null);
                context.changePageFrameSizes(1, 1);
                try (RecordCursorFactory factory = select("x", context)) {
                    long nativeBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
                    cache.setMemoryTracker(tracker);
                    for (int attempt = 0; attempt < 3; attempt++) {
                        try (PageFrameCursor frames = factory.getPageFrameCursor(context, PartitionFrameCursorFactory.ORDER_ASC)) {
                            if (attempt == 0) {
                                Assert.assertThrows(CairoException.class, () -> cache.of(factory.getMetadata(), frames));
                            } else {
                                cache.of(factory.getMetadata(), frames);
                                if (attempt == 1) {
                                    tracker.setLimit(tracker.getUsed());
                                    Assert.assertThrows(CairoException.class, () -> fill(cache, frames));
                                } else {
                                    Assert.assertEquals(4096, fill(cache, frames));
                                    for (int i = 0; i < cache.getFrameCount(); i++) {
                                        Assert.assertEquals(1, cache.getFrameSize(i));
                                        Assert.assertEquals(i, cache.getRowIdOffset(i));
                                        Assert.assertEquals(PartitionFormat.NATIVE, cache.getFrameFormat(i));
                                        Assert.assertFalse(cache.isFrameCovered(i));
                                        Assert.assertNull(cache.getCoveredIndexReader(i));
                                    }
                                    Assert.assertTrue(tracker.getUsed() > 4096 * Long.BYTES);
                                    Assert.assertEquals(tracker.getUsed(), Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT) - nativeBefore);
                                }
                            }
                            cache.close();
                            Assert.assertEquals(0, tracker.getUsed());
                            tracker.setLimit(0);
                        }
                    }
                }
            }
        });
    }

    private void assertFailedOpenReleasesTrackedCache(
            int workerCount,
            String query,
            String expected,
            String... planFragments
    ) throws Exception {
        // Every open of a cached factory acquires a pooled per-query tracker and charges the page
        // frame address cache to it. An invalid regex bind variable fails the open after the cache
        // owner has reopened the cache. If the failed open keeps that charge, the next open recycles
        // the same tracker with a non-zero used count and PerQueryMemoryTracker.init() asserts.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT x, x::VARCHAR s,
                            CASE WHEN x % 3 = 0 THEN 'a' WHEN x % 3 = 1 THEN 'b' ELSE 'c' END::SYMBOL k,
                            timestamp_sequence(0, 3_600_000_000) ts
                        FROM long_sequence(10)
                    ), INDEX(k) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            try (SqlExecutionContext context = TestUtils.createSqlExecutionCtx(engine, bindVariableService, workerCount)) {
                bindVariableService.clear();
                bindVariableService.setStr(0, "abc");
                assertQuery(query).noLeakCheck().withContext(context).assertsPlanContaining(planFragments);
                try (
                        SqlCompiler compiler = engine.getSqlCompiler();
                        RecordCursorFactory factory = compiler.compile(query, context).getRecordCursorFactory()
                ) {
                    bindVariableService.setStr(0, "[");
                    for (int i = 0; i < 2; i++) {
                        try (RecordCursor ignore = factory.getCursor(context)) {
                            Assert.fail("expected an invalid regex failure during cursor open at attempt " + i);
                        } catch (SqlException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "Unclosed character class");
                        }
                    }
                    bindVariableService.setStr(0, "1");
                    for (int i = 0; i < 2; i++) {
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            println(factory, cursor);
                            TestUtils.assertEquals(expected, sink);
                        }
                    }
                }
            }
        });
    }

    private static int fill(PageFrameAddressCache cache, PageFrameCursor frames) {
        PageFrame frame;
        int count = 0;
        while ((frame = frames.next()) != null) {
            cache.add(count++, frame);
        }
        return count;
    }
}
