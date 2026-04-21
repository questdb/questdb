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
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ParquetMetaFileReaderTest extends AbstractCairoTest {
    @BeforeClass
    public static void loadNativeLib() {
        Os.init();
    }

    @Test
    public void testBloomFilterEnabledCanSkipRowGroup() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFileWithBloomFilter(2, 100)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);

                try (DirectLongList filters = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT)) {
                    Assert.assertFalse(reader.canSkipRowGroup(0, filters, 0));
                }
                reader.close();
            }
        });
    }

    @Test
    public void testBloomFilterEnabledFileAccepted() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFileWithBloomFilter(2, 100)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);

                Assert.assertTrue(reader.isOpen());
                Assert.assertEquals(2, reader.getColumnCount());
                Assert.assertEquals(1, reader.getRowGroupCount());
                Assert.assertEquals(100, reader.getRowGroupSize(0));
                reader.clear();
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
                reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);
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
    public void testCanSkipRowGroupNoFiltersReturnsFalse() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 0, 0, 100, 200, 300)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);
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
    public void testClearResetsState() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 42)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);
                Assert.assertTrue(reader.isOpen());

                reader.clear();
                Assert.assertFalse(reader.isOpen());
                Assert.assertEquals(0, reader.getRowGroupCount());
                Assert.assertEquals(0, reader.getColumnCount());
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
                reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);

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
                    long pmSize = ParquetMetaFileWriter.resultParquetMetaFileSize(resultPtr);

                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    reader.of(dataPtr, pmSize, Long.MAX_VALUE);

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
    public void testCorruptedColumnCountValidatedBeforeAccess() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 100)) {
                // Corrupt columnCount to a huge value. Without the bounds check,
                // accessing column descriptors would read past the mmap (SIGSEGV).
                Unsafe.getUnsafe().putInt(file.dataPtr + 24, 1_000_000_000);

                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                try {
                    reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage().contains("invalid _pm columnCount"));
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
                    reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage().contains("invalid _pm footer length"));
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
                    reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage().contains("invalid _pm footer offset"));
                }
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
                    long pmSize = ParquetMetaFileWriter.resultParquetMetaFileSize(resultPtr);

                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    reader.of(dataPtr, pmSize, Long.MAX_VALUE);
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
    public void testDesignatedTimestampColumnIndexNone() throws Exception {
        assertMemoryLeak(() -> {
            // Build without designated timestamp.
            try (PmTestFile file = buildFile(2, 100)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);
                Assert.assertEquals(-1, reader.getDesignatedTimestampColumnIndex());
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
                    reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage().contains("unexpected _pm footer feature bytes"));
                }
            }
        });
    }

    @Test
    public void testFileTooSmall() throws Exception {
        assertMemoryLeak(() -> {
            // Allocate a tiny buffer and plant a bogus parquet_meta_file_size in the
            // first 8 bytes. The reader must reject it as too small.
            long addr = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putLong(addr, 4L); // implausibly small
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                try {
                    reader.of(addr, 4L, Long.MAX_VALUE);
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage().contains("invalid _pm parquet_meta_file_size"));
                }
            } finally {
                Unsafe.free(addr, 8, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testFooterChainWalkResolvesCorrectFooter() throws Exception {
        assertMemoryLeak(() -> {
            // Build a single-footer _pm: 1 column, parquetFooterOff=100, parquetFooterLen=50,
            // 1 row group with 1000 rows. Derived parquet size = 100 + 50 + 8 = 158.
            try (PmTestFile file = buildFile(1, 100, 50, 1000)) {
                long origLen = file.dataLen;
                // The original file's footer offset is derived from the trailer.
                int origFooterLength = Unsafe.getUnsafe().getInt(file.dataPtr + origLen - 4);
                long origFooterOffset = origLen - 4 - Integer.toUnsignedLong(origFooterLength);

                // Read the row group entry from the original footer to reuse in footer2.
                int rowGroupEntry = Unsafe.getUnsafe().getInt(
                        file.dataPtr + origFooterOffset + 40 // FOOTER_FIXED_SIZE
                );

                // Append a second footer with a different parquet file size.
                // Footer2: parquetFooterOff=200, parquetFooterLen=80 → derived size = 288.
                // Layout: fixed(40) + 1 rg entry(4) + CRC(4) + trailer(4) = 52 bytes.
                int newFooterBytes = 52;
                long newTotalLen = origLen + newFooterBytes;
                long newBuf = Unsafe.malloc(newTotalLen, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().copyMemory(file.dataPtr, newBuf, origLen);

                    long newFooterOff = origLen;
                    long fa = newBuf + newFooterOff;

                    // Footer fixed portion (40 bytes). prev_parquet_meta_file_size
                    // is the old committed size — the original file's length —
                    // which lets the reader walk back via the old trailer at
                    // origLen - 4.
                    Unsafe.getUnsafe().putLong(fa, 200L);              // parquet_footer_offset
                    Unsafe.getUnsafe().putInt(fa + 8, 80);             // parquet_footer_length
                    Unsafe.getUnsafe().putInt(fa + 12, 1);             // row_group_count
                    Unsafe.getUnsafe().putLong(fa + 16, 0L);           // unused_bytes
                    Unsafe.getUnsafe().putLong(fa + 24, origLen);      // prev_parquet_meta_file_size
                    Unsafe.getUnsafe().putLong(fa + 32, 0L);           // footer_feature_flags

                    // Row group entry (reuse the same block offset)
                    Unsafe.getUnsafe().putInt(fa + 40, rowGroupEntry);

                    // CRC placeholder (of() does not verify the CRC hash value)
                    Unsafe.getUnsafe().putInt(fa + 44, 0);

                    // Trailer: footer_length = fixed(40) + rg(4) + CRC(4) = 48
                    Unsafe.getUnsafe().putInt(fa + 48, 48);

                    // Patch header parquet_meta_file_size to publish the new snapshot —
                    // this is the MVCC commit signal the reader observes.
                    Unsafe.getUnsafe().putLong(newBuf, newTotalLen);

                    ParquetMetaFileReader reader = new ParquetMetaFileReader();

                    // Latest footer (parquet size 288) resolves directly.
                    reader.of(newBuf, newTotalLen, 288L);
                    Assert.assertEquals(288L, reader.getParquetFileSize());
                    Assert.assertEquals(1, reader.getRowGroupCount());
                    Assert.assertEquals(1000L, reader.getRowGroupSize(0));

                    // Old footer (parquet size 158) resolves via chain walk.
                    reader.of(newBuf, newTotalLen, 158L);
                    Assert.assertEquals(158L, reader.getParquetFileSize());
                    Assert.assertEquals(1, reader.getRowGroupCount());
                    Assert.assertEquals(1000L, reader.getRowGroupSize(0));

                    // Non-matching parquet size throws STALE_PARQUET_METADATA.
                    try {
                        reader.of(newBuf, newTotalLen, 9999L);
                        Assert.fail("Expected CairoException for stale _pm");
                    } catch (CairoException e) {
                        Assert.assertEquals(CairoException.STALE_PARQUET_METADATA, e.getErrno());
                        TestUtils.assertContains(e.getFlyweightMessage(), "no _pm footer found for parquet size");
                    }
                } finally {
                    Unsafe.free(newBuf, newTotalLen, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testGetParquetFileSize() throws Exception {
        assertMemoryLeak(() -> {
            // parquet file size = parquetFooterOffset + parquetFooterLength + 8
            try (PmTestFile file = buildFile(1, 4096, 256, 100)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);

                Assert.assertEquals(4096 + 256 + 8, reader.getParquetFileSize());
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
                reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);

                Assert.assertEquals(128, reader.getRowGroupCount());
                for (int i = 0; i < 128; i++) {
                    Assert.assertEquals((i + 1) * 10L, reader.getRowGroupSize(i));
                }
            }
        });
    }

    @Test
    public void testLifecycleCloseIdempotent() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 100)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);

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
                reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);
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
                reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);

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

                reader.of(file1.dataPtr, file1.parquetMetaFileSize, Long.MAX_VALUE);
                try (DirectLongList filters = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT)) {
                    // First skip call lazily allocates the native handle.
                    Assert.assertFalse(reader.canSkipRowGroup(0, filters, 0));
                }

                // Re-init via of(): the previous native handle must be freed
                // by clear() inside of() before storing the new addr/size.
                reader.of(file2.dataPtr, file2.parquetMetaFileSize, Long.MAX_VALUE);
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
    public void testManyColumnsAffectsBlockSize() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(50, 0, 0, 777, 888)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);

                Assert.assertEquals(50, reader.getColumnCount());
                Assert.assertEquals(2, reader.getRowGroupCount());
                Assert.assertEquals(777, reader.getRowGroupSize(0));
                Assert.assertEquals(888, reader.getRowGroupSize(1));
            }
        });
    }

    @Test
    public void testMaxRowGroupSize() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, Long.MAX_VALUE)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);

                Assert.assertEquals(Long.MAX_VALUE, reader.getRowGroupSize(0));
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

                reader.of(file1.dataPtr, file1.parquetMetaFileSize, Long.MAX_VALUE);
                Assert.assertEquals(1, reader.getColumnCount());
                Assert.assertEquals(500, reader.getRowGroupSize(0));
                Assert.assertEquals(600, reader.getRowGroupSize(1));

                reader.of(file2.dataPtr, file2.parquetMetaFileSize, Long.MAX_VALUE);
                Assert.assertEquals(10, reader.getColumnCount());
                Assert.assertEquals(500, reader.getRowGroupSize(0));
                Assert.assertEquals(600, reader.getRowGroupSize(1));

                // Files should have different sizes due to different column counts
                Assert.assertTrue(file2.dataLen > file1.dataLen);
            }
        });
    }

    @Test
    public void testMultipleRowGroups() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(3, 0, 0, 100, 200, 500, 1_000_000)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);

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
    public void testOfAfterClear() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(2, 55)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);
                reader.clear();
                Assert.assertFalse(reader.isOpen());

                reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);
                Assert.assertTrue(reader.isOpen());
                Assert.assertEquals(2, reader.getColumnCount());
                Assert.assertEquals(1, reader.getRowGroupCount());
                Assert.assertEquals(55, reader.getRowGroupSize(0));
            }
        });
    }

    @Test
    public void testPartitionRowCount() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(2, 0, 0, 100, 200, 300)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);
                Assert.assertEquals(600, reader.getPartitionRowCount());
            }
        });
    }

    @Test
    public void testPartitionRowCountEmpty() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);
                Assert.assertEquals(0, reader.getPartitionRowCount());
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

                reader.of(file1.dataPtr, file1.parquetMetaFileSize, Long.MAX_VALUE);
                Assert.assertEquals(1, reader.getRowGroupCount());
                Assert.assertEquals(42, reader.getRowGroupSize(0));

                reader.of(file2.dataPtr, file2.parquetMetaFileSize, Long.MAX_VALUE);
                Assert.assertEquals(2, reader.getRowGroupCount());
                Assert.assertEquals(99, reader.getRowGroupSize(0));
                Assert.assertEquals(101, reader.getRowGroupSize(1));
            }
        });
    }

    @Test
    public void testSingleRowGroup() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(2, 1000)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);

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
    public void testSingleRowInRowGroup() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 1)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);

                Assert.assertEquals(1, reader.getRowGroupSize(0));
            }
        });
    }

    @Test
    public void testStalePmThrowsWithCorrectErrno() throws Exception {
        assertMemoryLeak(() -> {
            // Build a _pm with derived parquet size = 100 + 50 + 8 = 158.
            try (PmTestFile file = buildFile(2, 100, 50, 1000)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                // Request parquet size 9999 — no footer matches.
                try {
                    reader.of(file.dataPtr, file.parquetMetaFileSize, 9999);
                    Assert.fail("Expected CairoException for stale _pm");
                } catch (CairoException e) {
                    Assert.assertEquals(CairoException.STALE_PARQUET_METADATA, e.getErrno());
                    TestUtils.assertContains(e.getFlyweightMessage(), "no _pm footer found for parquet size");
                }
            }
        });
    }

    @Test
    public void testUnknownRequiredFeatureFlagRejected() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 100)) {
                // Set bit 32 (a required feature flag) in the header feature flags at offset 4.
                long originalFlags = Unsafe.getUnsafe().getLong(file.dataPtr + 8);
                Unsafe.getUnsafe().putLong(file.dataPtr + 8, originalFlags | (1L << 32));

                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                try {
                    reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage().contains("unsupported required _pm feature flags"));
                }
            }
        });
    }

    @Test
    public void testUnknownRequiredFooterFeatureFlagRejected() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1, 100)) {
                // Footer feature flags live at footerOffset + 32 (FOOTER_FEATURE_FLAGS_OFF).
                // Derive the footer offset from the trailer, then set bit 32
                // (a required footer feature flag) on the footer flags.
                int footerLength = Unsafe.getUnsafe().getInt(file.dataPtr + file.parquetMetaFileSize - 4);
                long footerOffset = file.parquetMetaFileSize - 4 - Integer.toUnsignedLong(footerLength);
                long footerFlagsAddr = file.dataPtr + footerOffset + 32;
                long originalFlags = Unsafe.getUnsafe().getLong(footerFlagsAddr);
                Unsafe.getUnsafe().putLong(footerFlagsAddr, originalFlags | (1L << 32));

                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                try {
                    reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage().contains("unsupported required _pm footer feature flags"));
                }
            }
        });
    }

    @Test
    public void testZeroRowGroups() throws Exception {
        assertMemoryLeak(() -> {
            try (PmTestFile file = buildFile(1)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize, Long.MAX_VALUE);

                Assert.assertEquals(0, reader.getRowGroupCount());
            }
        });
    }

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

    private static class PmTestFile implements AutoCloseable {
        final long dataLen;
        final long dataPtr;
        final long parquetMetaFileSize;
        final long resultPtr;

        PmTestFile(long resultPtr) {
            this.resultPtr = resultPtr;
            this.dataPtr = ParquetMetaFileWriter.resultDataPtr(resultPtr);
            this.dataLen = ParquetMetaFileWriter.resultDataLen(resultPtr);
            this.parquetMetaFileSize = ParquetMetaFileWriter.resultParquetMetaFileSize(resultPtr);
        }

        @Override
        public void close() {
            ParquetMetaFileWriter.destroyResult(resultPtr);
        }
    }
}
