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

package io.questdb.test.cairo.sql;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.PartitionFrameState;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.table.parquet.ParquetDecoder;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class PageFrameAddressCacheTest extends AbstractCairoTest {
    // Fixed ABI header of a Rust partition state handle, in longs.
    private static final int HEADER_LONGS = 13;

    @Test
    public void testHistoricalKeyPagesVaryByFrame() throws Exception {
        assertMemoryLeak(() -> {
            try (PageFrameAddressCache cache = new PageFrameAddressCache()) {
                cache.of(new GenericRecordMetadata(), new CustomFrameCursor());
                cache.add(0, keyFrame(0, 0));
                cache.add(1, keyFrame(2, 100));
                cache.add(2, keyFrame(1, 200));
                Assert.assertEquals(0, cache.getDedupKeyPageCount(0));
                Assert.assertEquals(2, cache.getDedupKeyPageCount(1));
                Assert.assertEquals(1, cache.getDedupKeyPageCount(2));
                Assert.assertEquals(111, cache.getDedupKeyPage(1, 1, 1));
                Assert.assertEquals(201, cache.getDedupKeyPage(2, 0, 1));

                // Lazy opening can replace a keyless skeleton with historical key pages.
                cache.updateAddresses(0, keyFrame(3, 300));
                cache.updateAddresses(1, keyFrame(1, 400));
                Assert.assertEquals(3, cache.getDedupKeyPageCount(0));
                Assert.assertEquals(321, cache.getDedupKeyPage(0, 2, 1));
                Assert.assertEquals(1, cache.getDedupKeyPageCount(1));
                Assert.assertEquals(401, cache.getDedupKeyPage(1, 0, 1));
                Assert.assertEquals(201, cache.getDedupKeyPage(2, 0, 1));
                cache.clear();
                cache.add(0, keyFrame(1, 500));
                Assert.assertEquals(501, cache.getDedupKeyPage(0, 0, 1));
            }
        });
    }

    @Test
    public void testInexactFrameRequiresCustomDecode() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Without a partition frame state there is no decoder able to report an actual
            // count, so a frame claiming an inexact span has no way to ever be resolved.
            try (PageFrameAddressCache cache = new PageFrameAddressCache()) {
                try {
                    cache.add(0, new BoundedFrame(0, 0, 10, 0));
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "without a custom decode cannot report an inexact row count");
                }
            }
        });
    }

    @Test
    public void testInexactFrameRowCountBounds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long inexactState = allocPartitionFrameState(10, PartitionFrameState.WINDOW_HAS_DELTA | PartitionFrameState.WINDOW_ROW_COUNT_INEXACT);
            final long exactState = allocPartitionFrameState(10, PartitionFrameState.WINDOW_HAS_DELTA);
            try (PageFrameAddressCache cache = new PageFrameAddressCache()) {
                Assert.assertFalse(PartitionFrameState.isExactRowCount(inexactState, 0));
                Assert.assertTrue(PartitionFrameState.isExactRowCount(exactState, 0));
                Assert.assertTrue(PartitionFrameState.requiresMaterialization(inexactState, 0));

                cache.of(new GenericRecordMetadata(), new CustomFrameCursor());
                Assert.assertFalse(cache.hasInexactFrames());

                // A custom-decode frame over an inexact window promises nothing beyond its span.
                cache.add(0, new BoundedFrame(inexactState, 0, 10, 0));
                Assert.assertEquals(0, cache.getFrameRowCountLo(0));
                Assert.assertEquals(10, cache.getFrameRowCountHi(0));
                Assert.assertEquals(10, cache.getFrameSize(0));
                Assert.assertFalse(cache.isFrameRowCountExact(0));
                Assert.assertTrue(cache.hasInexactFrames());
                Assert.assertTrue(cache.hasDecodedFrames());

                // A custom-decode frame over an exact window keeps its declared count.
                cache.add(1, new BoundedFrame(exactState, 0, 7, 7));
                Assert.assertEquals(7, cache.getFrameRowCountLo(1));
                Assert.assertEquals(7, cache.getFrameRowCountHi(1));
                Assert.assertTrue(cache.isFrameRowCountExact(1));

                // A partial lower bound is carried through as is.
                cache.add(2, new BoundedFrame(inexactState, 2, 10, 3));
                Assert.assertEquals(3, cache.getFrameRowCountLo(2));
                Assert.assertEquals(8, cache.getFrameRowCountHi(2));
                Assert.assertFalse(cache.isFrameRowCountExact(2));

                // Patching a skeleton entry re-validates and re-records the bounds.
                cache.updateAddresses(1, new BoundedFrame(inexactState, 0, 7, 0));
                Assert.assertEquals(0, cache.getFrameRowCountLo(1));
                Assert.assertFalse(cache.isFrameRowCountExact(1));

                // Re-adding the same frame is a no-op and keeps its bounds.
                cache.add(0, new BoundedFrame(inexactState, 0, 10, 0));
                Assert.assertEquals(0, cache.getFrameRowCountLo(0));

                // Another frame at a cached index takes the index with its own bounds.
                cache.add(0, new BoundedFrame(exactState, 0, 10, 10));
                Assert.assertEquals(10, cache.getFrameRowCountLo(0));
                Assert.assertTrue(cache.isFrameRowCountExact(0));

                // clear() resets the cache-level flag with everything else.
                cache.clear();
                Assert.assertFalse(cache.hasInexactFrames());
                Assert.assertEquals(0, cache.getFrameCount());
            } finally {
                Unsafe.free(inexactState, partitionFrameStateBytes(), MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(exactState, partitionFrameStateBytes(), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testInvalidFrameRowCountBoundsAreRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long state = allocPartitionFrameState(10, PartitionFrameState.WINDOW_HAS_DELTA | PartitionFrameState.WINDOW_ROW_COUNT_INEXACT);
            try (PageFrameAddressCache cache = new PageFrameAddressCache()) {
                cache.of(new GenericRecordMetadata(), new CustomFrameCursor());
                // lo above hi
                assertRejected(cache, new BoundedFrame(state, 0, 10, 11));
                // negative lo
                assertRejected(cache, new BoundedFrame(state, 0, 10, -1));
                // hi that is not the declared span
                assertRejected(cache, new BoundedFrame(state, 0, 10, 0) {
                    @Override
                    public long getRowCountHi() {
                        return 9;
                    }
                });
                Assert.assertEquals(0, cache.getFrameCount());
                Assert.assertFalse(cache.hasInexactFrames());
            } finally {
                Unsafe.free(state, partitionFrameStateBytes(), MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testNativeAndParquetSkipDelta() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT x, timestamp_sequence('2000-01-01', 86_400_000_000L) ts FROM long_sequence(3)) TIMESTAMP(ts) PARTITION BY DAY");
            try (PageFrameAddressCache cache = new PageFrameAddressCache()) {
                for (int pass = 0; pass < 2; pass++) {
                    if (pass == 1) {
                        execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2000-01-01', '2000-01-02'");
                    }
                    try (
                            RecordCursorFactory factory = select("SELECT * FROM t");
                            PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                            PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
                            PageFrameMemoryRecord record = new PageFrameMemoryRecord()
                    ) {
                        Assert.assertFalse(cursor.hasCustomFrames());
                        cache.of(factory.getMetadata(), cursor);
                        Assert.assertEquals(0, cache.getPageTops().getAddress());
                        pool.of(cache);
                        record.of(cursor);

                        int frameIndex = 0;
                        long rows = 0;
                        PageFrame frame;
                        while ((frame = cursor.next()) != null) {
                            cache.add(frameIndex, frame);
                            cache.updateAddresses(frameIndex, frame);
                            Assert.assertEquals(0, cache.getPartitionFrameState(frameIndex));
                            Assert.assertEquals(0, cache.getPageTops().getAddress());
                            // Every native and parquet frame declares an exact count, and the
                            // materialized memory reports the same number.
                            final long frameSize = frame.getPartitionHi() - frame.getPartitionLo();
                            Assert.assertTrue(frame.isExactRowCount());
                            Assert.assertEquals(frameSize, frame.getRowCountLo());
                            Assert.assertEquals(frameSize, frame.getRowCountHi());
                            Assert.assertTrue(cache.isFrameRowCountExact(frameIndex));
                            Assert.assertEquals(frameSize, cache.getFrameRowCountLo(frameIndex));
                            Assert.assertEquals(frameSize, cache.getFrameRowCountHi(frameIndex));
                            final PageFrameMemory frameMemory = pool.navigateTo(frameIndex);
                            final long decodedRows = frameMemory.getRowCount();
                            Assert.assertEquals(frameSize, decodedRows);
                            pool.navigateTo(frameIndex++, record);
                            for (long row = 0; row < decodedRows; row++) {
                                record.setRowIndex(row);
                                Assert.assertEquals(++rows, record.getLong(0));
                            }
                        }
                        Assert.assertFalse(cache.hasInexactFrames());
                        Assert.assertEquals(3, rows);
                    }
                }
            }
        });
    }

    @Test
    public void testNativeRepeatVisit() throws Exception {
        // A record already on a native frame needs no frame-state check, e.g. each row of a scan.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT x, timestamp_sequence('2000-01-01', 86_400_000_000L) ts FROM long_sequence(3)) TIMESTAMP(ts) PARTITION BY DAY");
            try (
                    CountingAddressCache cache = new CountingAddressCache();
                    RecordCursorFactory factory = select("SELECT * FROM t");
                    PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                    PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
                    PageFrameMemoryRecord record = new PageFrameMemoryRecord()
            ) {
                cache.of(factory.getMetadata(), cursor);
                pool.of(cache);
                record.of(cursor);
                final PageFrame frame = cursor.next();
                Assert.assertNotNull(frame);
                cache.add(0, frame);
                cache.updateAddresses(0, frame);
                pool.navigateTo(0, record);
                final int checks = cache.decodedFrameChecks;
                for (int i = 0; i < 10; i++) {
                    pool.navigateTo(0, record);
                }
                Assert.assertEquals(checks, cache.decodedFrameChecks);
                record.setRowIndex(0);
                Assert.assertEquals(1, record.getLong(0));
            }
        });
    }

    @Test
    public void testSkipOnlyParquetFrameIsCached() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final GuardDecoder decoder = new GuardDecoder();
            try (PageFrameAddressCache cache = new PageFrameAddressCache()) {
                cache.add(0, new SkipOnlyParquetFrame(decoder));

                Assert.assertSame(decoder, cache.getParquetDecoder(0));
                Assert.assertTrue(cache.hasDecodedFrames());
                Assert.assertEquals(-1, cache.getParquetRowGroup(0));
            } finally {
                decoder.close();
            }
        });
    }

    // Builds a minimal Rust-ABI partition frame state with one window of {@code rows}
    // logical rows and the given summary flags; see PartitionFrameState for the layout.
    private static long allocPartitionFrameState(long rows, long flags) {
        final long addr = Unsafe.malloc(partitionFrameStateBytes(), MemoryTag.NATIVE_DEFAULT);
        final long summaries = addr + HEADER_LONGS * Long.BYTES;
        Unsafe.getUnsafe().putLong(addr, summaries);                    // HEADER_SUMMARIES_ADDR
        Unsafe.getUnsafe().putLong(addr + Long.BYTES, 1);               // HEADER_WINDOW_COUNT
        Unsafe.getUnsafe().putLong(addr + 2 * Long.BYTES, rows);        // HEADER_LOGICAL_ROWS
        Unsafe.getUnsafe().putLong(addr + 3 * Long.BYTES, 1);           // HEADER_HAS_CUSTOM_FRAMES
        Unsafe.getUnsafe().putLong(addr + 4 * Long.BYTES, rows);        // HEADER_SUBFRAME_SIZE
        Unsafe.getUnsafe().putLong(addr + 5 * Long.BYTES, rows);        // HEADER_BASE_ROWS
        for (int cell = 6; cell < HEADER_LONGS; cell++) {
            Unsafe.getUnsafe().putLong(addr + cell * Long.BYTES, 0);    // base id, Parquet mappings, dedup keys
        }
        Unsafe.getUnsafe().putLong(summaries, rows);                    // SUMMARY_BASE_ROWS
        Unsafe.getUnsafe().putLong(summaries + Long.BYTES, 0);          // visible delta rows
        Unsafe.getUnsafe().putLong(summaries + 2 * Long.BYTES, rows);   // SUMMARY_LOGICAL_ROWS
        Unsafe.getUnsafe().putLong(summaries + 3 * Long.BYTES, flags);  // SUMMARY_FLAGS
        return addr;
    }

    private static void assertRejected(PageFrameAddressCache cache, PageFrame frame) {
        try {
            cache.add(cache.getFrameCount(), frame);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "invalid page frame row count bounds");
        }
    }

    private static PageFrame keyFrame(int count, long seed) {
        return new BoundedFrame(0, 0, 10, 10) {
            @Override
            public long getDedupKeyPage(int key, int cell) {
                return seed + 10L * key + cell;
            }

            @Override
            public int getDedupKeyPageCount() {
                return count;
            }
        };
    }

    private static long partitionFrameStateBytes() {
        return (HEADER_LONGS + PartitionFrameState.SUMMARY_LONGS) * Long.BYTES;
    }

    /**
     * A column-less custom-decode frame over window 0 of {@code state}, with an explicit
     * lower row-count bound. Mirrors what the table reader cursors emit for a cold-delta
     * window that needs materialization.
     */
    private static class BoundedFrame implements PageFrame {
        private final long partitionHi;
        private final long partitionLo;
        private final long rowCountLo;
        private final long state;

        private BoundedFrame(long state, long partitionLo, long partitionHi, long rowCountLo) {
            this.state = state;
            this.partitionLo = partitionLo;
            this.partitionHi = partitionHi;
            this.rowCountLo = rowCountLo;
        }

        @Override
        public long getAuxPageAddress(int columnIndex) {
            return 0;
        }

        @Override
        public long getAuxPageSize(int columnIndex) {
            return 0;
        }

        @Override
        public int getColumnCount() {
            return 0;
        }

        @Override
        public byte getFormat() {
            return PartitionFormat.NATIVE;
        }

        @Override
        public IndexReader getIndexReader(int columnIndex, int direction) {
            return null;
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return 0;
        }

        @Override
        public long getPageSize(int columnIndex) {
            return 0;
        }

        @Override
        public long getPartitionFrameState() {
            return state;
        }

        @Override
        public int getParquetRowGroup() {
            return 0;
        }

        @Override
        public int getParquetRowGroupHi() {
            return (int) partitionHi;
        }

        @Override
        public int getParquetRowGroupLo() {
            return (int) partitionLo;
        }

        @Override
        public long getPartitionHi() {
            return partitionHi;
        }

        @Override
        public int getPartitionIndex() {
            return 0;
        }

        @Override
        public long getPartitionLo() {
            return partitionLo;
        }

        @Override
        public long getRowCountLo() {
            return rowCountLo;
        }
    }

    private static class CountingAddressCache extends PageFrameAddressCache {
        private int decodedFrameChecks;

        @Override
        public boolean hasDecodedFrames() {
            decodedFrameChecks++;
            return super.hasDecodedFrames();
        }
    }

    /**
     * Column-less cursor stub whose only job is to flip the cache into custom-frame mode.
     */
    private static class CustomFrameCursor implements PageFrameCursor {
        private final ColumnMapping columnMapping = new ColumnMapping();

        @Override
        public void calculateSize(RecordCursor.Counter counter) {
        }

        @Override
        public void close() {
        }

        @Override
        public ColumnMapping getColumnMapping() {
            return columnMapping;
        }

        @Override
        public long getRemainingRowsInInterval() {
            return 0;
        }

        @Override
        public StaticSymbolTable getSymbolTable(int columnIndex) {
            return null;
        }

        @Override
        public boolean hasCustomFrames() {
            return true;
        }

        @Override
        public boolean isExternal() {
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return null;
        }

        @Override
        public PageFrame next(long skipTarget) {
            return null;
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public boolean supportsSizeCalculation() {
            return false;
        }

        @Override
        public void toTop() {
        }
    }

    private static class GuardDecoder extends ParquetPartitionDecoder {
        @Override
        public long getFileSize() {
            return 1;
        }
    }

    private static class SkipOnlyParquetFrame implements PageFrame {
        private final ParquetDecoder decoder;

        private SkipOnlyParquetFrame(ParquetDecoder decoder) {
            this.decoder = decoder;
        }

        @Override
        public long getAuxPageAddress(int columnIndex) {
            return 0;
        }

        @Override
        public long getAuxPageSize(int columnIndex) {
            return 0;
        }

        @Override
        public int getColumnCount() {
            return 0;
        }

        @Override
        public byte getFormat() {
            return PartitionFormat.PARQUET;
        }

        @Override
        public IndexReader getIndexReader(int columnIndex, int direction) {
            return null;
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return 0;
        }

        @Override
        public long getPageSize(int columnIndex) {
            return 0;
        }

        @Override
        public ParquetDecoder getParquetDecoder() {
            return decoder;
        }

        @Override
        public int getParquetRowGroup() {
            return -1;
        }

        @Override
        public int getParquetRowGroupHi() {
            return -1;
        }

        @Override
        public int getParquetRowGroupLo() {
            return -1;
        }

        @Override
        public long getPartitionHi() {
            return 4;
        }

        @Override
        public int getPartitionIndex() {
            return 0;
        }

        @Override
        public long getPartitionLo() {
            return 4;
        }
    }
}
