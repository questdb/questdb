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

import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.table.parquet.ParquetDecoder;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class PageFrameAddressCacheTest extends AbstractCairoTest {

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
