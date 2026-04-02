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
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.ParquetMetaFileWriter;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ParquetMetaFileReaderTest extends AbstractCairoTest {
    @BeforeClass
    public static void loadNativeLib() {
        Os.init();
    }

    // ── Helper to build a _pm file via the real Rust writer ────────────

    /**
     * Builds a _pm file with the given column count and row group sizes
     * using the real Rust writer via JNI. Returns a handle that must
     * be freed with {@link PmTestFile#close()}.
     */
    private static PmTestFile buildFile(int columnCount, long... rowGroupSizes) {
        return buildFile(columnCount, 0, 0, rowGroupSizes);
    }

    private static PmTestFile buildFile(int columnCount, long parquetFooterOff, int parquetFooterLen, long... rowGroupSizes) {
        long writerPtr = ParquetMetaFileWriter.create();
        try {
            ParquetMetaFileWriter.setDesignatedTimestamp(writerPtr, -1);
            for (int i = 0; i < columnCount; i++) {
                try (DirectUtf8Sink name = new DirectUtf8Sink(16)) {
                    name.put("col_").put(i);
                    ParquetMetaFileWriter.addColumn(writerPtr, 0, name.ptr(), (int) name.size(), i, 5, 0, 0, 0, 0, 0);
                }
            }
            for (long numRows : rowGroupSizes) {
                ParquetMetaFileWriter.addRowGroup(writerPtr, numRows);
            }
            ParquetMetaFileWriter.setParquetFooter(writerPtr, parquetFooterOff, parquetFooterLen);
            long resultPtr = ParquetMetaFileWriter.finish(writerPtr);
            return new PmTestFile(resultPtr);
        } finally {
            ParquetMetaFileWriter.destroyWriter(writerPtr);
        }
    }

    private static class PmTestFile implements AutoCloseable {
        final long resultPtr;
        final long dataPtr;
        final long dataLen;
        final long footerOffset;

        PmTestFile(long resultPtr) {
            this.resultPtr = resultPtr;
            this.dataPtr = ParquetMetaFileWriter.resultDataPtr(resultPtr);
            this.dataLen = ParquetMetaFileWriter.resultDataLen(resultPtr);
            this.footerOffset = ParquetMetaFileWriter.resultFooterOffset(resultPtr);
        }

        @Override
        public void close() {
            ParquetMetaFileWriter.destroyResult(resultPtr);
        }
    }

    // ── Happy path tests ────────────────────────────────────────────────

    @Test
    public void testSingleRowGroup() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(2, 1000)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.dataLen);

                Assert.assertTrue(reader.isOpen());
                Assert.assertEquals(2, reader.getColumnCount());
                Assert.assertEquals(1, reader.getRowGroupCount());
                Assert.assertEquals(1000, reader.getRowGroupSize(0));

                reader.clear();
                Assert.assertFalse(reader.isOpen());
            }
        });
    }

    @Test
    public void testMultipleRowGroups() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(3, 0, 0, 100, 200, 500, 1_000_000)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.dataLen);

                Assert.assertEquals(3, reader.getColumnCount());
                Assert.assertEquals(4, reader.getRowGroupCount());
                Assert.assertEquals(100, reader.getRowGroupSize(0));
                Assert.assertEquals(200, reader.getRowGroupSize(1));
                Assert.assertEquals(500, reader.getRowGroupSize(2));
                Assert.assertEquals(1_000_000, reader.getRowGroupSize(3));
            }
        });
    }

    @Test
    public void testLargeRowGroupCount() throws Exception {
        assertMemoryLeak(() -> {
            long[] sizes = new long[128];
            for (int i = 0; i < sizes.length; i++) {
                sizes[i] = (i + 1) * 10L;
            }
            try (PmTestFile file = buildFile(1, sizes)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.dataLen);

                Assert.assertEquals(128, reader.getRowGroupCount());
                for (int i = 0; i < 128; i++) {
                    Assert.assertEquals((i + 1) * 10L, reader.getRowGroupSize(i));
                }
            }
        });
    }

    @Test
    public void testReopenWithOf() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    PmTestFile file1 = buildFile(1, 0, 0, 42);
                    PmTestFile file2 = buildFile(2, 0, 0, 99, 101)
            ) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();

                reader.of(file1.dataPtr, file1.dataLen);
                Assert.assertEquals(1, reader.getRowGroupCount());
                Assert.assertEquals(42, reader.getRowGroupSize(0));

                reader.of(file2.dataPtr, file2.dataLen);
                Assert.assertEquals(2, reader.getRowGroupCount());
                Assert.assertEquals(99, reader.getRowGroupSize(0));
                Assert.assertEquals(101, reader.getRowGroupSize(1));
            }
        });
    }

    @Test
    public void testGetParquetFileSize() throws Exception {
        assertMemoryLeak(() -> {
            // parquet file size = parquetFooterOffset + parquetFooterLength + 8
            try (PmTestFile file = buildFile(1, 4096, 256, 100)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.dataLen);

                Assert.assertEquals(4096 + 256 + 8, reader.getParquetFileSize());
            }
        });
    }

    // ── Edge case tests ─────────────────────────────────────────────────

    @Test
    public void testZeroRowGroups() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.dataLen);

                Assert.assertEquals(0, reader.getRowGroupCount());
            }
        });
    }

    @Test
    public void testMaxRowGroupSize() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, Long.MAX_VALUE)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.dataLen);

                Assert.assertEquals(Long.MAX_VALUE, reader.getRowGroupSize(0));
            }
        });
    }

    @Test
    public void testSingleRowInRowGroup() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 1)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.dataLen);

                Assert.assertEquals(1, reader.getRowGroupSize(0));
            }
        });
    }

    @Test
    public void testManyColumnsAffectsBlockSize() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(50, 0, 0, 777, 888)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.dataLen);

                Assert.assertEquals(50, reader.getColumnCount());
                Assert.assertEquals(2, reader.getRowGroupCount());
                Assert.assertEquals(777, reader.getRowGroupSize(0));
                Assert.assertEquals(888, reader.getRowGroupSize(1));
            }
        });
    }

    // ── Error / invalid input tests ──────────────────────────────────────

    @Test
    public void testInvalidFormatVersion() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 100)) {
                // Corrupt format version to 99
                Unsafe.getUnsafe().putInt(file.dataPtr, 99);

                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                try {
                    reader.of(file.dataPtr, file.dataLen);
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage().contains("unsupported _pm format version"));
                }
            }
        });
    }

    @Test
    public void testFormatVersionZero() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 100)) {
                Unsafe.getUnsafe().putInt(file.dataPtr, 0);

                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                try {
                    reader.of(file.dataPtr, file.dataLen);
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage().contains("unsupported _pm format version"));
                }
            }
        });
    }

    @Test
    public void testCorruptedTrailer() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 100)) {
                // Corrupt the footer length trailer to point past the file
                Unsafe.getUnsafe().putInt(file.dataPtr + file.dataLen - 4, (int) file.dataLen);

                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                try {
                    reader.of(file.dataPtr, file.dataLen);
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage().contains("invalid _pm footer offset"));
                }
            }
        });
    }

    @Test
    public void testFileTooSmall() throws Exception {
        assertMemoryLeak(() -> {
            // Allocate a tiny buffer that's too small for any valid _pm file
            long addr = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
            try {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                try {
                    reader.of(addr, 8);
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage().contains("pm file too small"));
                }
            } finally {
                Unsafe.free(addr, 8, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testClearResetsState() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 42)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.dataLen);
                Assert.assertTrue(reader.isOpen());

                reader.clear();
                Assert.assertFalse(reader.isOpen());
                Assert.assertEquals(0, reader.getRowGroupCount());
                Assert.assertEquals(0, reader.getColumnCount());
            }
        });
    }

    @Test
    public void testOfAfterClear() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(2, 55)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.dataLen);
                reader.clear();
                Assert.assertFalse(reader.isOpen());

                reader.of(file.dataPtr, file.dataLen);
                Assert.assertTrue(reader.isOpen());
                Assert.assertEquals(2, reader.getColumnCount());
                Assert.assertEquals(1, reader.getRowGroupCount());
                Assert.assertEquals(55, reader.getRowGroupSize(0));
            }
        });
    }

    @Test
    public void testMultipleColumnCountsProduceDifferentLayouts() throws Exception {
        assertMemoryLeak(() -> {
            // Same row group sizes but different column counts produce different
            // file layouts (block sizes differ). Verify the reader handles both.
            try (
                    PmTestFile file1 = buildFile(1, 0, 0, 500, 600);
                    PmTestFile file2 = buildFile(10, 0, 0, 500, 600)
            ) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();

                reader.of(file1.dataPtr, file1.dataLen);
                Assert.assertEquals(1, reader.getColumnCount());
                Assert.assertEquals(500, reader.getRowGroupSize(0));
                Assert.assertEquals(600, reader.getRowGroupSize(1));

                reader.of(file2.dataPtr, file2.dataLen);
                Assert.assertEquals(10, reader.getColumnCount());
                Assert.assertEquals(500, reader.getRowGroupSize(0));
                Assert.assertEquals(600, reader.getRowGroupSize(1));

                // Files should have different sizes due to different column counts
                Assert.assertTrue(file2.dataLen > file1.dataLen);
            }
        });
    }

    @Test
    public void testDesignatedTimestampColumnIndex() throws Exception {
        assertMemoryLeak(() -> {
            // Build with designated timestamp at column 0.
            long writerPtr = ParquetMetaFileWriter.create();
            try {
                ParquetMetaFileWriter.setDesignatedTimestamp(writerPtr, 0);
                try (DirectUtf8Sink name = new DirectUtf8Sink(16)) {
                    name.put("ts");
                    ParquetMetaFileWriter.addColumn(writerPtr, 0, name.ptr(), (int) name.size(), 0, 8, 0, 0, 0, 0, 0);
                }
                try (DirectUtf8Sink name = new DirectUtf8Sink(16)) {
                    name.put("val");
                    ParquetMetaFileWriter.addColumn(writerPtr, 0, name.ptr(), (int) name.size(), 1, 5, 0, 0, 0, 0, 0);
                }
                long resultPtr = ParquetMetaFileWriter.finish(writerPtr);
                try {
                    long dataPtr = ParquetMetaFileWriter.resultDataPtr(resultPtr);
                    long dataLen = ParquetMetaFileWriter.resultDataLen(resultPtr);

                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    reader.of(dataPtr, dataLen);
                    Assert.assertEquals(0, reader.getDesignatedTimestampColumnIndex());
                } finally {
                    ParquetMetaFileWriter.destroyResult(resultPtr);
                }
            } finally {
                ParquetMetaFileWriter.destroyWriter(writerPtr);
            }
        });
    }

    @Test
    public void testPartitionRowCount() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(2, 0, 0, 100, 200, 300)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.dataLen);
                Assert.assertEquals(600, reader.getPartitionRowCount());
            }
        });
    }

    @Test
    public void testPartitionRowCountEmpty() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.dataLen);
                Assert.assertEquals(0, reader.getPartitionRowCount());
            }
        });
    }

    @Test
    public void testDesignatedTimestampColumnIndexNone() throws Exception {
        assertMemoryLeak(() -> {
            // Build without designated timestamp.
            try (PmTestFile file = buildFile(2, 100)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.dataLen);
                Assert.assertEquals(-1, reader.getDesignatedTimestampColumnIndex());
            }
        });
    }
}
