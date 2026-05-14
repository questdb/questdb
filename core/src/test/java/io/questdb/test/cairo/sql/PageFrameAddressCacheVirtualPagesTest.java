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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.griffin.engine.table.parquet.ParquetDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class PageFrameAddressCacheVirtualPagesTest extends AbstractCairoTest {

    private static final ParquetDecoder STUB_DECODER = new StubParquetDecoder();

    @Test
    public void testNativeFrameDefaultsAreZero() throws Exception {
        assertMemoryLeak(() -> {
            try (PageFrameAddressCache cache = new PageFrameAddressCache()) {
                cache.of(buildMetadata(), new ColumnMapping(), false);
                cache.add(0, new StubFrame(PartitionFormat.NATIVE, 0, 100, 0xAAAA, 0xBBBB, 0L, 0L, null));
                Assert.assertEquals(0L, cache.getVirtualPageAddresses().get(0));
                Assert.assertEquals(0L, cache.getVirtualPageSizes().get(0));
                Assert.assertEquals(0L, cache.getVirtualAuxPageAddresses().get(0));
                Assert.assertEquals(0L, cache.getVirtualAuxPageSizes().get(0));
                // Regular addresses still populated as before.
                Assert.assertEquals(0xAAAA, cache.getPageAddresses().get(0));
                Assert.assertEquals(0xBBBB, cache.getPageSizes().get(0));
            }
        });
    }

    @Test
    public void testParquetFrameVirtualAddressesPopulated() throws Exception {
        // Virtual addresses are captured for parquet frames so the memory pool overlay
        // can use them after parquetBuffers.decode wipes the regular slots.
        assertMemoryLeak(() -> {
            try (PageFrameAddressCache cache = new PageFrameAddressCache()) {
                cache.of(buildMetadata(), new ColumnMapping(), true);
                cache.add(0, new StubFrame(PartitionFormat.PARQUET, 0, 100,
                        0L, 0L, 0xCAFE, 800, STUB_DECODER));
                // Regular slots stay at zero for parquet frames.
                Assert.assertEquals(0L, cache.getPageAddresses().get(0));
                Assert.assertEquals(0L, cache.getPageSizes().get(0));
                // Virtual slots capture the frame's reported addresses.
                Assert.assertEquals(0xCAFE, cache.getVirtualPageAddresses().get(0));
                Assert.assertEquals(800, cache.getVirtualPageSizes().get(0));
            }
        });
    }

    @Test
    public void testVirtualAddressesPerFrame() throws Exception {
        // Two frames with different virtual addresses must not bleed into each other.
        assertMemoryLeak(() -> {
            try (PageFrameAddressCache cache = new PageFrameAddressCache()) {
                cache.of(buildMetadata(), new ColumnMapping(), true);
                cache.add(0, new StubFrame(PartitionFormat.PARQUET, 0, 100,
                        0L, 0L, 0x1111, 100, STUB_DECODER));
                cache.add(1, new StubFrame(PartitionFormat.PARQUET, 100, 200,
                        0L, 0L, 0x2222, 200, STUB_DECODER));

                int off0 = cache.toColumnOffset(0);
                int off1 = cache.toColumnOffset(1);

                Assert.assertEquals(0x1111, cache.getVirtualPageAddresses().get(off0));
                Assert.assertEquals(100, cache.getVirtualPageSizes().get(off0));
                Assert.assertEquals(0x2222, cache.getVirtualPageAddresses().get(off1));
                Assert.assertEquals(200, cache.getVirtualPageSizes().get(off1));
            }
        });
    }

    private static GenericRecordMetadata buildMetadata() {
        GenericRecordMetadata md = new GenericRecordMetadata();
        md.add(new TableColumnMetadata("c0", ColumnType.LONG));
        return md;
    }

    /**
     * Minimal {@link PageFrame} that lets the test seed each address slot directly.
     * Only fields used by {@link PageFrameAddressCache#add} are wired.
     */
    private static final class StubFrame implements PageFrame {
        private final ParquetDecoder decoder;
        private final byte format;
        private final long pageAddress;
        private final long pageSize;
        private final long partitionHi;
        private final long partitionLo;
        private final long virtualAddress;
        private final long virtualSize;

        StubFrame(
                byte format,
                long partitionLo,
                long partitionHi,
                long pageAddress,
                long pageSize,
                long virtualAddress,
                long virtualSize,
                ParquetDecoder decoder
        ) {
            this.format = format;
            this.partitionLo = partitionLo;
            this.partitionHi = partitionHi;
            this.pageAddress = pageAddress;
            this.pageSize = pageSize;
            this.virtualAddress = virtualAddress;
            this.virtualSize = virtualSize;
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
            return 1;
        }

        @Override
        public byte getFormat() {
            return format;
        }

        @Override
        public io.questdb.cairo.idx.IndexReader getIndexReader(int columnIndex, int direction) {
            return null;
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return pageAddress;
        }

        @Override
        public long getPageSize(int columnIndex) {
            return pageSize;
        }

        @Override
        public ParquetDecoder getParquetDecoder() {
            return decoder;
        }

        @Override
        public int getParquetRowGroup() {
            return 0;
        }

        @Override
        public int getParquetRowGroupHi() {
            return 0;
        }

        @Override
        public int getParquetRowGroupLo() {
            return 0;
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
        public long getVirtualPageAddress(int columnIndex) {
            return virtualAddress;
        }

        @Override
        public long getVirtualPageSize(int columnIndex) {
            return virtualSize;
        }
    }

    /**
     * Satisfies the {@code add()} assertion that PARQUET frames carry a non-null
     * decoder with {@code fileSize > 0}. The decode methods are unreachable from
     * this cache-only test.
     */
    private static final class StubParquetDecoder implements ParquetDecoder {
        @Override
        public int decodeRowGroup(RowGroupBuffers buffers, DirectIntList columns, int rowGroup, int rowLo, int rowHi) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void decodeRowGroupWithRowFilter(RowGroupBuffers buffers, int columnOffset, DirectIntList columns, int rowGroup, int rowLo, int rowHi, DirectLongList filteredRows) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void decodeRowGroupWithRowFilterFillNulls(RowGroupBuffers buffers, int columnOffset, DirectIntList columns, int rowGroup, int rowLo, int rowHi, DirectLongList filteredRows) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getColumnCount() {
            return 1;
        }

        @Override
        public int getColumnId(int columnIndex) {
            return -1;
        }

        @Override
        public long getFileAddr() {
            return 1;
        }

        @Override
        public long getFileSize() {
            return 1;
        }
    }
}
