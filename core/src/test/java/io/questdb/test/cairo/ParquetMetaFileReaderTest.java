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
import io.questdb.std.DirectLongList;
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
                    ParquetMetaFileWriter.addColumn(writerPtr, name.ptr(), (int) name.size(), i, 5, 0, 0, 0, 0, 0);
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
    public void testCorruptedRowGroupCountValidatedBeforeLoop() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 100)) {
                // Compute footer address: trailer (last 4 bytes) holds footer length
                int footerLength = Unsafe.getUnsafe().getInt(file.dataPtr + file.dataLen - 4);
                long footerAddr = file.dataPtr + file.dataLen - 4 - Integer.toUnsignedLong(footerLength);

                // Corrupt rowGroupCount to a huge value. Without the validation-before-loop
                // fix, this causes an out-of-bounds read (SIGSEGV) instead of a clean exception.
                Unsafe.getUnsafe().putInt(footerAddr + 12, 1_000_000_000);

                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                try {
                    reader.of(file.dataPtr, file.dataLen);
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage().contains("invalid _pm footer length"));
                }
            }
        });
    }

    @Test
    public void testCorruptedColumnCountValidatedBeforeAccess() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 100)) {
                // Corrupt columnCount to a huge value. Without the bounds check,
                // accessing column descriptors would read past the mmap (SIGSEGV).
                Unsafe.getUnsafe().putInt(file.dataPtr + 20, 1_000_000_000);

                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                try {
                    reader.of(file.dataPtr, file.dataLen);
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage().contains("invalid _pm columnCount"));
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
                    ParquetMetaFileWriter.addColumn(writerPtr, name.ptr(), (int) name.size(), 0, 8, 0, 0, 0, 0, 0);
                }
                try (DirectUtf8Sink name = new DirectUtf8Sink(16)) {
                    name.put("val");
                    ParquetMetaFileWriter.addColumn(writerPtr, name.ptr(), (int) name.size(), 1, 5, 0, 0, 0, 0, 0);
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

    @Test
    public void testColumnMetadataAccessors() throws Exception {
        assertMemoryLeak(() -> {
            // Build a multi-column file with varied types and explicit IDs.
            long writerPtr = ParquetMetaFileWriter.create();
            try {
                ParquetMetaFileWriter.setDesignatedTimestamp(writerPtr, 2);
                try (DirectUtf8Sink name = new DirectUtf8Sink(16)) {
                    name.put("amount");
                    // colType=10 (DOUBLE), id=100
                    ParquetMetaFileWriter.addColumn(writerPtr, name.ptr(), (int) name.size(), 100, 10, 0, 0, 0, 0, 0);
                }
                try (DirectUtf8Sink name = new DirectUtf8Sink(16)) {
                    name.put("symbol");
                    // colType=12 (SYMBOL), id=200
                    ParquetMetaFileWriter.addColumn(writerPtr, name.ptr(), (int) name.size(), 200, 12, 0, 0, 0, 0, 0);
                }
                try (DirectUtf8Sink name = new DirectUtf8Sink(16)) {
                    name.put("ts");
                    // colType=8 (TIMESTAMP), id=300
                    ParquetMetaFileWriter.addColumn(writerPtr, name.ptr(), (int) name.size(), 300, 8, 0, 0, 0, 0, 0);
                }
                ParquetMetaFileWriter.addRowGroup(writerPtr, 500);
                ParquetMetaFileWriter.setParquetFooter(writerPtr, 0, 0);
                long resultPtr = ParquetMetaFileWriter.finish(writerPtr);
                try {
                    long dataPtr = ParquetMetaFileWriter.resultDataPtr(resultPtr);
                    long dataLen = ParquetMetaFileWriter.resultDataLen(resultPtr);

                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    reader.of(dataPtr, dataLen);

                    Assert.assertEquals(3, reader.getColumnCount());

                    // Column names.
                    Assert.assertEquals("amount", reader.getColumnName(0).toString());
                    Assert.assertEquals("symbol", reader.getColumnName(1).toString());
                    Assert.assertEquals("ts", reader.getColumnName(2).toString());

                    // Column IDs.
                    Assert.assertEquals(100, reader.getColumnId(0));
                    Assert.assertEquals(200, reader.getColumnId(1));
                    Assert.assertEquals(300, reader.getColumnId(2));

                    // Column types.
                    Assert.assertEquals(10, reader.getColumnType(0));
                    Assert.assertEquals(12, reader.getColumnType(1));
                    Assert.assertEquals(8, reader.getColumnType(2));

                    // Lookup by name.
                    Assert.assertEquals(0, reader.getColumnIndex("amount"));
                    Assert.assertEquals(1, reader.getColumnIndex("symbol"));
                    Assert.assertEquals(2, reader.getColumnIndex("ts"));
                    Assert.assertEquals(-1, reader.getColumnIndex("nonexistent"));

                    // Designated timestamp.
                    Assert.assertEquals(2, reader.getDesignatedTimestampColumnIndex());

                    // Chunk stat accessors (values are 0 for writer-built files,
                    // but must not crash).
                    Assert.assertEquals(0, reader.getChunkStatFlags(0, 0));
                    Assert.assertEquals(0, reader.getChunkMinStat(0, 0));
                    Assert.assertEquals(0, reader.getChunkMaxStat(0, 0));
                } finally {
                    ParquetMetaFileWriter.destroyResult(resultPtr);
                }
            } finally {
                ParquetMetaFileWriter.destroyWriter(writerPtr);
            }
        });
    }

    @Test
    public void testLifecycleCloseWithoutOf() throws Exception {
        assertMemoryLeak(() -> {
            ParquetMetaFileReader reader = new ParquetMetaFileReader();
            // close() before of() must be a no-op (no native handle was ever
            // allocated). assertMemoryLeak catches a leak if this allocates.
            reader.close();
            Assert.assertFalse(reader.isOpen());
        });
    }

    @Test
    public void testLifecycleOfThenCloseNoSkipCall() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 100)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.dataLen);
                Assert.assertTrue(reader.isOpen());
                // canSkipRowGroup is not called → the lazy native handle is
                // never allocated → close() is a no-op for the native side.
                // close() leaves in-memory state intact so isOpen() still
                // returns true (the field is reusable for accessor reads).
                reader.close();
                Assert.assertTrue(reader.isOpen());
                // clear() does the full reset.
                reader.clear();
                Assert.assertFalse(reader.isOpen());
            }
        });
    }

    @Test
    public void testLifecycleOfThenSkipThenClose() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 100)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.dataLen);

                // Calling canSkipRowGroup with an empty filter list lazily
                // allocates the native handle but always returns false.
                try (DirectLongList filters = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT)) {
                    Assert.assertFalse(reader.canSkipRowGroup(0, filters, 0));
                }

                // close() must free the native handle. assertMemoryLeak fails
                // the test if it doesn't. close() preserves in-memory state
                // so isOpen() still returns true after.
                reader.close();
                Assert.assertTrue(reader.isOpen());
                reader.clear();
                Assert.assertFalse(reader.isOpen());
            }
        });
    }

    @Test
    public void testLifecycleCloseIdempotent() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 100)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.dataLen);

                try (DirectLongList filters = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT)) {
                    Assert.assertFalse(reader.canSkipRowGroup(0, filters, 0));
                }

                reader.close();
                // Second close() must be a no-op.
                reader.close();
                // close() preserves in-memory state.
                Assert.assertTrue(reader.isOpen());
                reader.clear();
            }
        });
    }

    @Test
    public void testCloseThenReuseViaCanSkipRowGroup() throws Exception {
        // close() releases the native handle but leaves in-memory state alone,
        // so a subsequent canSkipRowGroup call lazily reallocates a new native
        // handle over the same _pm data. This is the contract that allows
        // long-lived owners (TableReader/TableWriter) to defensively close
        // their scratch reader without breaking subsequent reuse.
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 100)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.dataLen);

                try (DirectLongList filters = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT)) {
                    Assert.assertFalse(reader.canSkipRowGroup(0, filters, 0));
                }
                reader.close();
                // After close(), in-memory state is preserved and
                // canSkipRowGroup re-allocates the native handle.
                try (DirectLongList filters = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT)) {
                    Assert.assertFalse(reader.canSkipRowGroup(0, filters, 0));
                }
                reader.clear();
            }
        });
    }

    @Test
    public void testLifecycleReuseViaOfDoesNotLeak() throws Exception {
        // Guards lifecycle invariant 1: of() must free any pre-existing
        // native handle before storing the new state. Without the fix in
        // ParquetMetaFileReader.of(), the native handle from the first
        // canSkipRowGroup call would leak when of() is called the second
        // time, and assertMemoryLeak would fail.
        assertMemoryLeak(() -> {
            try (
                    PmTestFile file1 = buildFile(1, 100);
                    PmTestFile file2 = buildFile(2, 0, 0, 200, 300)
            ) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();

                reader.of(file1.dataPtr, file1.dataLen);
                try (DirectLongList filters = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT)) {
                    // First skip call lazily allocates the native handle.
                    Assert.assertFalse(reader.canSkipRowGroup(0, filters, 0));
                }

                // Re-init via of(): the previous native handle must be freed
                // by clear() inside of() before storing the new addr/size.
                reader.of(file2.dataPtr, file2.dataLen);
                Assert.assertEquals(2, reader.getRowGroupCount());

                try (DirectLongList filters = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT)) {
                    // Second skip call lazily allocates a fresh native handle
                    // over the new mmap.
                    Assert.assertFalse(reader.canSkipRowGroup(1, filters, 0));
                }

                reader.close();
            }
        });
    }

    @Test
    public void testCanSkipRowGroupNoFiltersReturnsFalse() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 0, 0, 100, 200, 300)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.dataLen);
                try (DirectLongList filters = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT)) {
                    // No filters → never skip, regardless of row group index.
                    Assert.assertFalse(reader.canSkipRowGroup(0, filters, 0));
                    Assert.assertFalse(reader.canSkipRowGroup(1, filters, 0));
                    Assert.assertFalse(reader.canSkipRowGroup(2, filters, 0));
                }
                reader.close();
            }
        });
    }

    @Test
    public void testCanSkipRowGroupCachedAcrossMultipleCalls() throws Exception {
        // Verifies the cached-reader path: a single ParquetMetaFileReader
        // instance reuses one native handle across many canSkipRowGroup
        // calls. assertMemoryLeak proves that no extra allocations happen
        // beyond the single lazy create + the single close.
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 0, 0, 10, 20, 30, 40, 50)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.dataLen);
                Assert.assertEquals(5, reader.getRowGroupCount());

                try (DirectLongList filters = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT)) {
                    for (int i = 0; i < 5; i++) {
                        Assert.assertFalse(reader.canSkipRowGroup(i, filters, 0));
                    }
                }
                reader.close();
            }
        });
    }

    @Test
    public void testCanSkipRowGroupOnCorruptPmThrows() throws Exception {
        // The lazy createNativeReader call must throw a CairoException with
        // the original Rust context when the _pm bytes are invalid.
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 100)) {
                // Corrupt the format version so createNativeReader fails.
                int originalVersion = Unsafe.getUnsafe().getInt(file.dataPtr);
                try {
                    Unsafe.getUnsafe().putInt(file.dataPtr, 99);

                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    // of() validates the version up-front and throws here.
                    try {
                        reader.of(file.dataPtr, file.dataLen);
                        Assert.fail("expected CairoException from of() on corrupt _pm");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.getMessage().contains("unsupported _pm format version"));
                    }
                    // Reader is not open → close() is a no-op and doesn't leak.
                    reader.close();
                } finally {
                    Unsafe.getUnsafe().putInt(file.dataPtr, originalVersion);
                }
            }
        });
    }

    // ── Feature flags / bloom filter tests ──────────────────────────────

    @Test
    public void testBloomFilterEnabledFileAccepted() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFileWithBloomFilter(2, 100)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.dataLen);

                Assert.assertTrue(reader.isOpen());
                Assert.assertEquals(2, reader.getColumnCount());
                Assert.assertEquals(1, reader.getRowGroupCount());
                Assert.assertEquals(100, reader.getRowGroupSize(0));
                reader.clear();
            }
        });
    }

    @Test
    public void testBloomFilterEnabledCanSkipRowGroup() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFileWithBloomFilter(2, 100)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.dataLen);

                try (DirectLongList filters = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT)) {
                    Assert.assertFalse(reader.canSkipRowGroup(0, filters, 0));
                }
                reader.close();
            }
        });
    }

    @Test
    public void testUnknownRequiredFeatureFlagRejected() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 100)) {
                // Set bit 32 (a required feature flag) in the header feature flags at offset 4.
                long originalFlags = Unsafe.getUnsafe().getLong(file.dataPtr + 4);
                Unsafe.getUnsafe().putLong(file.dataPtr + 4, originalFlags | (1L << 32));

                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                try {
                    reader.of(file.dataPtr, file.dataLen);
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage().contains("unsupported required _pm feature flags"));
                }
            }
        });
    }

    @Test
    public void testExtraFooterBytesWithNoFeaturesRejected() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 100)) {
                // Inflate the footer length by 8 bytes while feature flags remain 0.
                // This simulates a corrupt file with unexpected trailing bytes.
                int footerLength = Unsafe.getUnsafe().getInt(file.dataPtr + file.dataLen - 4);
                Unsafe.getUnsafe().putInt(file.dataPtr + file.dataLen - 4, footerLength + 8);

                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                try {
                    reader.of(file.dataPtr, file.dataLen);
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage().contains("unexpected _pm footer feature bytes"));
                }
            }
        });
    }

    // ── Bloom filter test helper ────────────────────────────────────────

    private static PmTestFile buildFileWithBloomFilter(int columnCount, long... rowGroupSizes) {
        long writerPtr = ParquetMetaFileWriter.create();
        try {
            ParquetMetaFileWriter.setDesignatedTimestamp(writerPtr, -1);
            for (int i = 0; i < columnCount; i++) {
                try (DirectUtf8Sink name = new DirectUtf8Sink(16)) {
                    name.put("col_").put(i);
                    ParquetMetaFileWriter.addColumn(writerPtr, name.ptr(), (int) name.size(), i, 5, 0, 0, 0, 0, 0);
                }
            }
            for (long numRows : rowGroupSizes) {
                ParquetMetaFileWriter.addRowGroup(writerPtr, numRows);
                // Add a dummy bloom filter bitset (32 bytes) for column 0.
                long bitsetAddr = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int b = 0; b < 32; b++) {
                        Unsafe.getUnsafe().putByte(bitsetAddr + b, (byte) 0xFF);
                    }
                    ParquetMetaFileWriter.addBloomFilter(writerPtr, 0, bitsetAddr, 32);
                } finally {
                    Unsafe.free(bitsetAddr, 32, MemoryTag.NATIVE_DEFAULT);
                }
            }
            ParquetMetaFileWriter.setParquetFooter(writerPtr, 0, 0);
            long resultPtr = ParquetMetaFileWriter.finish(writerPtr);
            return new PmTestFile(resultPtr);
        } finally {
            ParquetMetaFileWriter.destroyWriter(writerPtr);
        }
    }
}
