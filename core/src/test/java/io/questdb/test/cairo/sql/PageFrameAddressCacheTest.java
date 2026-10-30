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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFrameDecoder;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.table.parquet.ParquetDecoder;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.std.DirectIntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class PageFrameAddressCacheTest extends AbstractCairoTest {

    @Test
    public void testCustomFrameLogicalBoundsAreLong() throws Exception {
        assertMemoryLeak(() -> {
            final long logicalLo = (1L << 32) + 7;
            final long logicalHi = logicalLo + 17;
            final long stateBytes = 10L * Long.BYTES;
            final long state = Unsafe.calloc(stateBytes, MemoryTag.NATIVE_DEFAULT);
            try {
                final long summaries = state + 6L * Long.BYTES;
                Unsafe.putLong(state, summaries);
                Unsafe.putLong(state + Long.BYTES, 1);
                Unsafe.putLong(state + 2L * Long.BYTES, logicalHi);
                Unsafe.putLong(state + 3L * Long.BYTES, 1);
                Unsafe.putLong(state + 4L * Long.BYTES, 1_000_000);
                Unsafe.putLong(summaries + 2L * Long.BYTES, logicalHi);
                Unsafe.putLong(summaries + 3L * Long.BYTES, 1);

                final RecordingPartitionFrameDecoder decoder = new RecordingPartitionFrameDecoder(state);
                final CairoConfiguration testConfiguration = new DefaultTestCairoConfiguration(root) {
                    @Override
                    public PartitionFrameDecoder newPartitionFrameDecoder() {
                        return decoder;
                    }
                };
                final CustomFrameCursor cursor = new CustomFrameCursor();
                final PageFrame frame = new LongLogicalPageFrame(state, logicalLo, logicalHi);
                try (
                        PageFrameAddressCache cache = new PageFrameAddressCache();
                        PageFrameMemoryPool pool = new PageFrameMemoryPool(testConfiguration);
                        PageFrameMemoryRecord record = new PageFrameMemoryRecord()
                ) {
                    cache.of(new GenericRecordMetadata(), cursor);
                    cache.add(0, frame);
                    Assert.assertEquals(0, cache.getParquetRowGroupLo(0));
                    Assert.assertEquals(17, cache.getParquetRowGroupHi(0));
                    Assert.assertEquals(logicalLo, cache.getPartitionFrameRowLo(0));
                    Assert.assertEquals(logicalHi, cache.getPartitionFrameRowHi(0));

                    pool.of(cache);
                    record.of(cursor);
                    pool.navigateTo(0, record);
                    Assert.assertEquals(logicalLo, decoder.decodedLo);
                    Assert.assertEquals(logicalHi, decoder.decodedHi);
                }
            } finally {
                Unsafe.free(state, stateBytes, MemoryTag.NATIVE_DEFAULT);
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
                            pool.navigateTo(frameIndex++, record);
                            for (long row = 0, n = frame.getPartitionHi() - frame.getPartitionLo(); row < n; row++) {
                                record.setRowIndex(row);
                                Assert.assertEquals(++rows, record.getLong(0));
                            }
                        }
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

    private static class CountingAddressCache extends PageFrameAddressCache {
        private int decodedFrameChecks;

        @Override
        public boolean hasDecodedFrames() {
            decodedFrameChecks++;
            return super.hasDecodedFrames();
        }
    }

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
            return 17;
        }

        @Override
        public boolean supportsSizeCalculation() {
            return true;
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

    private static class LongLogicalPageFrame implements PageFrame {
        private final long logicalHi;
        private final long logicalLo;
        private final long state;

        private LongLogicalPageFrame(long state, long logicalLo, long logicalHi) {
            this.state = state;
            this.logicalLo = logicalLo;
            this.logicalHi = logicalHi;
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
        public int getParquetRowGroup() {
            return 0;
        }

        @Override
        public int getParquetRowGroupHi() {
            return Math.toIntExact(logicalHi - logicalLo);
        }

        @Override
        public int getParquetRowGroupLo() {
            return 0;
        }

        @Override
        public long getPartitionFrameRowHi() {
            return logicalHi;
        }

        @Override
        public long getPartitionFrameRowLo() {
            return logicalLo;
        }

        @Override
        public long getPartitionFrameState() {
            return state;
        }

        @Override
        public long getPartitionHi() {
            return logicalHi;
        }

        @Override
        public int getPartitionIndex() {
            return 0;
        }

        @Override
        public long getPartitionLo() {
            return logicalLo;
        }
    }

    private static class RecordingPartitionFrameDecoder implements PartitionFrameDecoder {
        private final long expectedState;
        private long decodedHi = -1;
        private long decodedLo = -1;

        private RecordingPartitionFrameDecoder(long expectedState) {
            this.expectedState = expectedState;
        }

        @Override
        public void bind(long state) {
            Assert.assertEquals(expectedState, state);
        }

        @Override
        public void close() {
        }

        @Override
        public long decodeFilteredSubframe(
                RowGroupBuffers buffers,
                int columnOffset,
                long rowLo,
                long rowHi,
                long survivorRowsAddress,
                long survivorRowCount,
                boolean fillNulls
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long decodeSubframe(RowGroupBuffers buffers, long rowLo, long rowHi) {
            decodedLo = rowLo;
            decodedHi = rowHi;
            return 0;
        }

        @Override
        public long materializeRemaining(
                RowGroupBuffers buffers,
                int columnOffset,
                DirectIntList columns,
                long rowLo,
                long rowHi,
                long survivorRowsAddress,
                long survivorRowCount,
                boolean fillNulls
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void prepareWindow(
                int windowIndex,
                DirectIntList projectionPairs,
                int primaryColumnCount,
                long baseViewsAddress,
                int baseViewCount,
                long memoryTrackerAddress
        ) {
            Assert.assertEquals(0, windowIndex);
            Assert.assertEquals(0, baseViewCount);
        }

        @Override
        public void releaseDecodeResource(long resource) {
        }

        @Override
        public void releaseWindow() {
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
