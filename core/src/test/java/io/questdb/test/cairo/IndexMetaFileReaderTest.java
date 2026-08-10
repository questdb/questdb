/*******************************************************************************
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

import io.questdb.cairo.IndexMetaFileReader;
import io.questdb.cairo.IndexMetaFileWriter;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class IndexMetaFileReaderTest extends AbstractCairoTest {

    /**
     * Pins the absolute byte offsets of every section of the sample _im file.
     * The other tests compare the Java reader against the Rust writer, so a
     * layout change applied to both implementations at once would keep them
     * green; this test fails when any section moves.
     * <p>
     * Layout for 4 index row groups / 2 index columns / 3 data boundaries:
     * <pre>
     * 0   header (48 bytes)
     * 48  RG_FIRST_KEY, 4 entries plus the key count sentinel at 64
     * 68  4 padding bytes to the 8-byte alignment
     * 72  RG_ROW_ID_MIN, 4 x 8 bytes
     * 104 RG_ROW_ID_MAX, 4 x 8 bytes
     * 136 DATA_RG_BOUNDARY, 3 x 8 bytes
     * 160 RG_COL_RANGE, 4 x 2 x 16 bytes
     * 288 CRC32
     * 292 total
     * </pre>
     */
    @Test
    public void testFileLayoutByteOffsets() throws Exception {
        assertMemoryLeak(() -> withSample(reader -> {
            final long addr = reader.getAddr();
            Assert.assertEquals(292, reader.getFileSize());
            Assert.assertEquals(292, Unsafe.getUnsafe().getLong(addr));
            Assert.assertEquals(0, Unsafe.getUnsafe().getInt(addr + 48));
            Assert.assertEquals(11_403, Unsafe.getUnsafe().getInt(addr + 52));
            Assert.assertEquals(11_403, Unsafe.getUnsafe().getInt(addr + 56));
            Assert.assertEquals(11_404, Unsafe.getUnsafe().getInt(addr + 60));
            Assert.assertEquals(11_405, Unsafe.getUnsafe().getInt(addr + 64));
            Assert.assertEquals(0, Unsafe.getUnsafe().getInt(addr + 68));
            Assert.assertEquals(0, Unsafe.getUnsafe().getLong(addr + 72));
            Assert.assertEquals(100_000, Unsafe.getUnsafe().getLong(addr + 80));
            Assert.assertEquals(99_999, Unsafe.getUnsafe().getLong(addr + 104));
            Assert.assertEquals(157_999, Unsafe.getUnsafe().getLong(addr + 112));
            Assert.assertEquals(0, Unsafe.getUnsafe().getLong(addr + 136));
            Assert.assertEquals(500_000, Unsafe.getUnsafe().getLong(addr + 144));
            Assert.assertEquals(1_000_000, Unsafe.getUnsafe().getLong(addr + 152));
            Assert.assertEquals(4, Unsafe.getUnsafe().getLong(addr + 160));
            Assert.assertEquals(100, Unsafe.getUnsafe().getLong(addr + 168));
            Assert.assertEquals(484, Unsafe.getUnsafe().getLong(addr + 240));
            Assert.assertEquals(80, Unsafe.getUnsafe().getLong(addr + 248));
        }));
    }

    @Test
    public void testKeyOutOfRangeReturnsMinusOne() throws Exception {
        assertMemoryLeak(() -> withSample(reader ->
                Assert.assertEquals(-1, reader.getRowGroupLoForKey(11_405))));
    }

    @Test
    public void testKeyPackedIntoSharedRowGroup() throws Exception {
        assertMemoryLeak(() -> withSample(reader -> {
            Assert.assertEquals(0, reader.getRowGroupLoForKey(5));
            Assert.assertEquals(0, reader.getRowGroupHiForKey(5));
            Assert.assertEquals(3, reader.getRowGroupLoForKey(11_404));
            Assert.assertEquals(3, reader.getRowGroupHiForKey(11_404));
        }));
    }

    @Test
    public void testKeySpanningMultipleRowGroups() throws Exception {
        assertMemoryLeak(() -> withSample(reader -> {
            Assert.assertEquals(1, reader.getRowGroupLoForKey(11_403));
            Assert.assertEquals(2, reader.getRowGroupHiForKey(11_403));
        }));
    }

    @Test
    public void testRoundTripHeaderFields() throws Exception {
        assertMemoryLeak(() -> withSample(reader -> {
            Assert.assertEquals(0, reader.getPayloadKind());
            Assert.assertEquals(11_405, reader.getKeyCount());
            Assert.assertEquals(4, reader.getIndexRowGroupCount());
            Assert.assertEquals(2, reader.getDataRowGroupCount());
            Assert.assertEquals(2, reader.getIndexColumnCount());
        }));
    }

    @Test
    public void testZoneMapsAndByteRanges() throws Exception {
        assertMemoryLeak(() -> withSample(reader -> {
            Assert.assertEquals(100_000, reader.getRowIdMin(1));
            Assert.assertEquals(157_999, reader.getRowIdMax(1));
            Assert.assertEquals(484, reader.getColumnByteRangeOffset(2, 1));
            Assert.assertEquals(80, reader.getColumnByteRangeLength(2, 1));
            Assert.assertEquals(0, reader.getDataRowGroupBoundary(0));
            Assert.assertEquals(1_000_000, reader.getDataRowGroupBoundary(2));
        }));
    }

    private static void addRowGroup(long writerPtr, int firstKey, long lo, long hi, long o0, long l0, long o1, long l1) {
        long ranges = Unsafe.malloc(4 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putLong(ranges, o0);
            Unsafe.getUnsafe().putLong(ranges + 8, l0);
            Unsafe.getUnsafe().putLong(ranges + 16, o1);
            Unsafe.getUnsafe().putLong(ranges + 24, l1);
            IndexMetaFileWriter.addRowGroup(writerPtr, firstKey, lo, hi, ranges, 2);
        } finally {
            Unsafe.free(ranges, 4 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void withSample(SampleAssertion assertion) {
        long writerPtr = IndexMetaFileWriter.create();
        long resultPtr = 0;
        try {
            IndexMetaFileWriter.setPayload(writerPtr, 0, 11_405);
            addRowGroup(writerPtr, 0, 0, 99_999, 4, 100, 104, 200);
            addRowGroup(writerPtr, 11_403, 100_000, 157_999, 304, 50, 354, 60);
            addRowGroup(writerPtr, 11_403, 158_000, 240_000, 414, 70, 484, 80);
            addRowGroup(writerPtr, 11_404, 240_001, 999_999, 564, 90, 654, 10);
            long boundaries = Unsafe.malloc(3 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putLong(boundaries, 0L);
                Unsafe.getUnsafe().putLong(boundaries + 8, 500_000L);
                Unsafe.getUnsafe().putLong(boundaries + 16, 1_000_000L);
                IndexMetaFileWriter.setDataRowGroupBoundaries(writerPtr, boundaries, 3);
            } finally {
                Unsafe.free(boundaries, 3 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
            resultPtr = IndexMetaFileWriter.finish(writerPtr);
            try (IndexMetaFileReader reader = new IndexMetaFileReader()) {
                reader.ofAddress(
                        IndexMetaFileWriter.resultDataPtr(resultPtr),
                        IndexMetaFileWriter.resultDataLen(resultPtr)
                );
                assertion.run(reader);
            }
        } finally {
            if (resultPtr != 0) {
                IndexMetaFileWriter.destroyResult(resultPtr);
            }
            IndexMetaFileWriter.destroyWriter(writerPtr);
        }
    }

    @FunctionalInterface
    private interface SampleAssertion {
        void run(IndexMetaFileReader reader);
    }
}
