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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.IndexMetaFileReader;
import io.questdb.cairo.IndexMetaFileWriter;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
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

    /**
     * The 4 row group sample ends the key directory at 68 and pads to 72, so it
     * only ever exercises the padded branch of the alignment rule. 3 row groups
     * end it at 64, already aligned, so nothing is padded. Without this case an
     * alignment rule change would go undetected in Java and Rust at once.
     * <p>
     * Layout for 3 index row groups / 2 index columns / 3 data boundaries:
     * <pre>
     * 0   header (48 bytes)
     * 48  RG_FIRST_KEY, 3 entries plus the key count sentinel at 60
     * 64  RG_ROW_ID_MIN, 3 x 8 bytes, no padding before it
     * 88  RG_ROW_ID_MAX, 3 x 8 bytes
     * 112 DATA_RG_BOUNDARY, 3 x 8 bytes
     * 136 RG_COL_RANGE, 3 x 2 x 16 bytes
     * 232 CRC32
     * 236 total
     * </pre>
     */
    @Test
    public void testFileLayoutByteOffsetsOddRowGroupCount() throws Exception {
        assertMemoryLeak(() -> withOddRowGroupSample(reader -> {
            final long addr = reader.getAddr();
            Assert.assertEquals(236, reader.getFileSize());
            Assert.assertEquals(236, Unsafe.getUnsafe().getLong(addr));
            Assert.assertEquals(0, Unsafe.getUnsafe().getInt(addr + 48));
            Assert.assertEquals(300, Unsafe.getUnsafe().getInt(addr + 52));
            Assert.assertEquals(700, Unsafe.getUnsafe().getInt(addr + 56));
            Assert.assertEquals(900, Unsafe.getUnsafe().getInt(addr + 60));
            Assert.assertEquals(0, Unsafe.getUnsafe().getLong(addr + 64));
            Assert.assertEquals(100, Unsafe.getUnsafe().getLong(addr + 72));
            Assert.assertEquals(99, Unsafe.getUnsafe().getLong(addr + 88));
            Assert.assertEquals(299, Unsafe.getUnsafe().getLong(addr + 104));
            Assert.assertEquals(0, Unsafe.getUnsafe().getLong(addr + 112));
            Assert.assertEquals(150, Unsafe.getUnsafe().getLong(addr + 120));
            Assert.assertEquals(300, Unsafe.getUnsafe().getLong(addr + 128));
            Assert.assertEquals(4, Unsafe.getUnsafe().getLong(addr + 136));
            Assert.assertEquals(100, Unsafe.getUnsafe().getLong(addr + 144));
            Assert.assertEquals(484, Unsafe.getUnsafe().getLong(addr + 216));
            Assert.assertEquals(80, Unsafe.getUnsafe().getLong(addr + 224));
            Assert.assertEquals(3, reader.getIndexRowGroupCount());
            Assert.assertEquals(2, reader.getIndexColumnCount());
            Assert.assertEquals(2, reader.getDataRowGroupCount());
            Assert.assertEquals(900, reader.getKeyCount());
            Assert.assertEquals(2, reader.getRowGroupLoForKey(700));
            Assert.assertEquals(2, reader.getRowGroupHiForKey(700));
            Assert.assertEquals(484, reader.getColumnByteRangeOffset(2, 1));
            Assert.assertEquals(80, reader.getColumnByteRangeLength(2, 1));
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

    /**
     * A byte flipped inside the CRC coverage window must surface as a clean
     * CairoException, and the failing open must leave nothing mapped -- the
     * enclosing assertMemoryLeak checks the error path did not leak the
     * mapping it had already taken.
     */
    @Test
    public void testOpenAndMapROCorruptedCrc() throws Exception {
        assertMemoryLeak(() -> withSampleBytes((dataPtr, dataLen) -> {
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path()) {
                path.of(root).concat("corrupt-crc._im");
                writeFile(ff, path.$(), dataPtr, dataLen);
                // Offset 48 is RG_FIRST_KEY[0], inside the CRC area [8, size - 4).
                flipByte(ff, path.$(), 48);
                try (IndexMetaFileReader reader = new IndexMetaFileReader()) {
                    IndexMetaFileReader.openAndMapRO(ff, path.$(), reader);
                    Assert.fail("expected CairoException from the CRC check");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "_im CRC32 mismatch");
                }
            }
        }));
    }

    /**
     * The descriptor must not outlive the call: the mapping survives it, and
     * Phase 2 keeps one reader per indexed column per partition.
     */
    @Test
    public void testOpenAndMapROReleasesFdAndMapping() throws Exception {
        assertMemoryLeak(() -> withSampleBytes((dataPtr, dataLen) -> {
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path()) {
                path.of(root).concat("close._im");
                writeFile(ff, path.$(), dataPtr, dataLen);
                final long openFilesBefore = Files.getOpenFileCount();
                final long cachedFilesBefore = Files.getOpenCachedFileCount();
                final long mappedBefore = Unsafe.getMemUsedByTag(MemoryTag.MMAP_PARQUET_METADATA_READER);
                final IndexMetaFileReader reader = new IndexMetaFileReader();
                final long addr = IndexMetaFileReader.openAndMapRO(ff, path.$(), reader);
                Assert.assertNotEquals(0L, addr);
                Assert.assertTrue(reader.isOpen());
                Assert.assertTrue(Unsafe.getMemUsedByTag(MemoryTag.MMAP_PARQUET_METADATA_READER) > mappedBefore);
                // The fd is already gone while the mapping is still live.
                Assert.assertEquals(openFilesBefore, Files.getOpenFileCount());
                Assert.assertEquals(cachedFilesBefore, Files.getOpenCachedFileCount());
                reader.close();
                Assert.assertFalse(reader.isOpen());
                Assert.assertEquals(openFilesBefore, Files.getOpenFileCount());
                Assert.assertEquals(cachedFilesBefore, Files.getOpenCachedFileCount());
                Assert.assertEquals(mappedBefore, Unsafe.getMemUsedByTag(MemoryTag.MMAP_PARQUET_METADATA_READER));
            }
        }));
    }

    @Test
    public void testOpenAndMapRORoundTripFromFile() throws Exception {
        assertMemoryLeak(() -> withSampleBytes((dataPtr, dataLen) -> {
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path()) {
                path.of(root).concat("round-trip._im");
                writeFile(ff, path.$(), dataPtr, dataLen);
                try (IndexMetaFileReader reader = new IndexMetaFileReader()) {
                    Assert.assertNotEquals(0L, IndexMetaFileReader.openAndMapRO(ff, path.$(), reader));
                    Assert.assertTrue(reader.isOpen());
                    Assert.assertEquals(292, reader.getFileSize());
                    Assert.assertEquals(0, reader.getPayloadKind());
                    Assert.assertEquals(11_405, reader.getKeyCount());
                    Assert.assertEquals(4, reader.getIndexRowGroupCount());
                    Assert.assertEquals(2, reader.getDataRowGroupCount());
                    Assert.assertEquals(2, reader.getIndexColumnCount());
                    Assert.assertEquals(0, reader.getRowGroupLoForKey(5));
                    Assert.assertEquals(0, reader.getRowGroupHiForKey(5));
                    Assert.assertEquals(1, reader.getRowGroupLoForKey(11_403));
                    Assert.assertEquals(2, reader.getRowGroupHiForKey(11_403));
                    Assert.assertEquals(-1, reader.getRowGroupLoForKey(11_405));
                    Assert.assertEquals(100_000, reader.getRowIdMin(1));
                    Assert.assertEquals(157_999, reader.getRowIdMax(1));
                    Assert.assertEquals(484, reader.getColumnByteRangeOffset(2, 1));
                    Assert.assertEquals(80, reader.getColumnByteRangeLength(2, 1));
                    Assert.assertEquals(0, reader.getDataRowGroupBoundary(0));
                    Assert.assertEquals(1_000_000, reader.getDataRowGroupBoundary(2));
                }
            }
        }));
    }

    /**
     * A file whose committed IM_FILE_SIZE exceeds its length is corruption:
     * mapping IM_FILE_SIZE bytes and reading the trailer would run past EOF.
     */
    @Test
    public void testOpenAndMapROTruncatedBelowFileSize() throws Exception {
        assertMemoryLeak(() -> withSampleBytes((dataPtr, dataLen) -> {
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path()) {
                path.of(root).concat("truncated._im");
                writeFile(ff, path.$(), dataPtr, dataLen);
                truncateFile(ff, path.$(), 100);
                try (IndexMetaFileReader reader = new IndexMetaFileReader()) {
                    IndexMetaFileReader.openAndMapRO(ff, path.$(), reader);
                    Assert.fail("expected CairoException from the file length check");
                } catch (CairoException e) {
                    TestUtils.assertContains(
                            e.getFlyweightMessage(),
                            "invalid _im IM_FILE_SIZE exceeds file length"
                    );
                }
            }
        }));
    }

    /**
     * A crash between the writer's creat() and its first write leaves a
     * zero-length _im. IM_FILE_SIZE is patched last as the commit signal, so
     * this is "not committed yet", not corruption: the open reports absent.
     * Before the pread-first guard, openAndMapRO mapped a header off this file
     * and read a page beyond EOF, raising SIGBUS.
     */
    @Test
    public void testOpenAndMapROZeroLengthFile() throws Exception {
        assertMemoryLeak(() -> {
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path()) {
                path.of(root).concat("zero-length._im");
                final long fd = ff.openRW(path.$(), 0);
                Assert.assertTrue(fd >= 0);
                ff.close(fd);
                Assert.assertEquals(0, ff.length(path.$()));
                try (IndexMetaFileReader reader = new IndexMetaFileReader()) {
                    Assert.assertEquals(0L, IndexMetaFileReader.openAndMapRO(ff, path.$(), reader));
                    Assert.assertFalse(reader.isOpen());
                }
            }
        });
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

    private static void buildOddRowGroupSample(long writerPtr) {
        IndexMetaFileWriter.setPayload(writerPtr, 0, 900);
        addRowGroup(writerPtr, 0, 0, 99, 4, 100, 104, 200);
        addRowGroup(writerPtr, 300, 100, 199, 304, 50, 354, 60);
        addRowGroup(writerPtr, 700, 200, 299, 414, 70, 484, 80);
        setDataRowGroupBoundaries(writerPtr, 0L, 150L, 300L);
    }

    private static void buildSample(long writerPtr) {
        IndexMetaFileWriter.setPayload(writerPtr, 0, 11_405);
        addRowGroup(writerPtr, 0, 0, 99_999, 4, 100, 104, 200);
        addRowGroup(writerPtr, 11_403, 100_000, 157_999, 304, 50, 354, 60);
        addRowGroup(writerPtr, 11_403, 158_000, 240_000, 414, 70, 484, 80);
        addRowGroup(writerPtr, 11_404, 240_001, 999_999, 564, 90, 654, 10);
        setDataRowGroupBoundaries(writerPtr, 0L, 500_000L, 1_000_000L);
    }

    private static void flipByte(FilesFacade ff, LPSZ path, long offset) {
        final long fd = ff.openRW(path, 0);
        Assert.assertTrue(fd >= 0);
        try {
            final long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
            try {
                Assert.assertEquals(1, ff.read(fd, buf, 1, offset));
                Unsafe.putByte(buf, (byte) (Unsafe.getByte(buf) ^ 0xFF));
                Assert.assertEquals(1, ff.write(fd, buf, 1, offset));
            } finally {
                Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            ff.close(fd);
        }
    }

    private static void setDataRowGroupBoundaries(long writerPtr, long b0, long b1, long b2) {
        long boundaries = Unsafe.malloc(3 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putLong(boundaries, b0);
            Unsafe.getUnsafe().putLong(boundaries + 8, b1);
            Unsafe.getUnsafe().putLong(boundaries + 16, b2);
            IndexMetaFileWriter.setDataRowGroupBoundaries(writerPtr, boundaries, 3);
        } finally {
            Unsafe.free(boundaries, 3 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void truncateFile(FilesFacade ff, LPSZ path, long size) {
        final long fd = ff.openRW(path, 0);
        Assert.assertTrue(fd >= 0);
        try {
            Assert.assertTrue(ff.truncate(fd, size));
        } finally {
            ff.close(fd);
        }
    }

    private static void writeFile(FilesFacade ff, LPSZ path, long dataPtr, long dataLen) {
        final long fd = ff.openRW(path, 0);
        Assert.assertTrue(fd >= 0);
        try {
            Assert.assertEquals(dataLen, ff.write(fd, dataPtr, dataLen, 0));
        } finally {
            ff.close(fd);
        }
    }

    /**
     * Builds an _im file with the Rust writer and hands the finished bytes to
     * {@code assertion}. The buffer is owned by the native result and freed on
     * every path, including the exceptional one.
     */
    private void withBytes(SampleBuilder builder, BytesAssertion assertion) {
        long writerPtr = IndexMetaFileWriter.create();
        long resultPtr = 0;
        try {
            builder.build(writerPtr);
            resultPtr = IndexMetaFileWriter.finish(writerPtr);
            assertion.run(
                    IndexMetaFileWriter.resultDataPtr(resultPtr),
                    IndexMetaFileWriter.resultDataLen(resultPtr)
            );
        } finally {
            if (resultPtr != 0) {
                IndexMetaFileWriter.destroyResult(resultPtr);
            }
            IndexMetaFileWriter.destroyWriter(writerPtr);
        }
    }

    private void withOddRowGroupSample(SampleAssertion assertion) {
        withReader(IndexMetaFileReaderTest::buildOddRowGroupSample, assertion);
    }

    private void withReader(SampleBuilder builder, SampleAssertion assertion) {
        withBytes(builder, (dataPtr, dataLen) -> {
            try (IndexMetaFileReader reader = new IndexMetaFileReader()) {
                reader.ofAddress(dataPtr, dataLen);
                assertion.run(reader);
            }
        });
    }

    private void withSample(SampleAssertion assertion) {
        withReader(IndexMetaFileReaderTest::buildSample, assertion);
    }

    private void withSampleBytes(BytesAssertion assertion) {
        withBytes(IndexMetaFileReaderTest::buildSample, assertion);
    }

    @FunctionalInterface
    private interface BytesAssertion {
        void run(long dataPtr, long dataLen);
    }

    @FunctionalInterface
    private interface SampleAssertion {
        void run(IndexMetaFileReader reader);
    }

    @FunctionalInterface
    private interface SampleBuilder {
        void build(long writerPtr);
    }
}
