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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.ParquetMetaFileWriter;
import io.questdb.std.DirectLongList;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.zip.CRC32;

public class ParquetMetaFileReaderTest extends AbstractCairoTest {
    @BeforeClass
    public static void loadNativeLib() {
        Os.init();
    }

    @Test
    public void testBloomFilterEnabledFileShortCircuitsWithoutFilter() throws Exception {
        // Empty filters list short-circuits canSkipRowGroup to false even when
        // the file carries a bloom-filter-enabled feature flag — the early
        // exit fires before any bloom-filter logic would run.
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFileWithBloomFilter(2, 100)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                reader.resolveLastFooter();

                try (DirectLongList filters = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT)) {
                    Assert.assertFalse(reader.canSkipRowGroup(0, filters, 0));
                }
                reader.clear();
            }
        });
    }

    @Test
    public void testBloomFilterEnabledFileAccepted() throws Exception {
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFileWithBloomFilter(2, 100)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                reader.resolveLastFooter();

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
            try (ParquetMetaTestFile file = buildFile(1, 0, 0, 10, 20, 30, 40, 50)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                reader.resolveLastFooter();
                Assert.assertEquals(5, reader.getRowGroupCount());

                try (DirectLongList filters = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT)) {
                    for (int i = 0; i < 5; i++) {
                        Assert.assertFalse(reader.canSkipRowGroup(i, filters, 0));
                    }
                }
                reader.clear();
            }
        });
    }

    @Test
    public void testCanSkipRowGroupNoFiltersReturnsFalse() throws Exception {
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFile(1, 0, 0, 100, 200, 300)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                reader.resolveLastFooter();
                try (DirectLongList filters = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT)) {
                    // No filters → never skip, regardless of row group index.
                    Assert.assertFalse(reader.canSkipRowGroup(0, filters, 0));
                    Assert.assertFalse(reader.canSkipRowGroup(1, filters, 0));
                    Assert.assertFalse(reader.canSkipRowGroup(2, filters, 0));
                }
                reader.clear();
            }
        });
    }

    @Test
    public void testClearResetsState() throws Exception {
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFile(1, 42)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                reader.resolveLastFooter();
                Assert.assertTrue(reader.isOpen());

                reader.clear();
                Assert.assertFalse(reader.isOpen());
                Assert.assertEquals(0, reader.getRowGroupCount());
                Assert.assertEquals(0, reader.getColumnCount());
            }
        });
    }

    @Test
    public void testColumnIdLookupBuildsIndexOncePerBinding() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    ParquetMetaTestFile wide = buildFile(64, 1);
                    ParquetMetaTestFile narrow = buildFile(3, 1)
            ) {
                final CountingColumnIdReader reader = new CountingColumnIdReader();
                reader.of(wide.dataPtr, wide.parquetMetaFileSize);
                Assert.assertTrue(reader.resolveLastFooter());

                Assert.assertEquals(63, reader.getColumnIndexById(63));
                Assert.assertEquals(0, reader.getColumnIndexById(0));
                Assert.assertEquals(-1, reader.getColumnIndexById(1_000));
                Assert.assertEquals(
                        "repeated lookups must scan the bound column descriptors only once",
                        64,
                        reader.getColumnIdReadCount()
                );

                reader.of(narrow.dataPtr, narrow.parquetMetaFileSize);
                Assert.assertTrue(reader.resolveLastFooter());
                Assert.assertEquals(2, reader.getColumnIndexById(2));
                Assert.assertEquals(
                        "rebinding must invalidate and rebuild the id index for the new footer",
                        67,
                        reader.getColumnIdReadCount()
                );
                reader.clear();
            }
        });
    }

    @Test
    public void testColumnIdLookupPreservesLegacyAndFirstMatchSemantics() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    ParquetMetaTestFile positional = buildFile(new int[]{-1, -1, -1}, 1);
                    ParquetMetaTestFile collisions = buildFile(new int[]{-1, 0, 5, 5}, 1)
            ) {
                final ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(positional.dataPtr, positional.parquetMetaFileSize);
                Assert.assertTrue(reader.resolveLastFooter());

                Assert.assertEquals(0, reader.getColumnIndexById(0));
                Assert.assertEquals(2, reader.getColumnIndexById(2));
                Assert.assertEquals(-1, reader.getColumnIndexById(3));

                reader.of(collisions.dataPtr, collisions.parquetMetaFileSize);
                Assert.assertTrue(reader.resolveLastFooter());
                Assert.assertEquals(
                        "the first descriptor must win an effective-id collision",
                        0,
                        reader.getColumnIndexById(0)
                );
                Assert.assertEquals(
                        "the first duplicate id must win",
                        2,
                        reader.getColumnIndexById(5)
                );
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
                    ParquetMetaFileWriter.addColumn(writerPtr, name.ptr(), name.size(), 100, ColumnType.DOUBLE, 0, 0, 0, 0, 0);
                }
                try (DirectUtf8Sink name = new DirectUtf8Sink(16)) {
                    name.put("symbol");
                    ParquetMetaFileWriter.addColumn(writerPtr, name.ptr(), name.size(), 200, ColumnType.SYMBOL, 0, 0, 0, 0, 0);
                }
                try (DirectUtf8Sink name = new DirectUtf8Sink(16)) {
                    name.put("ts");
                    ParquetMetaFileWriter.addColumn(writerPtr, name.ptr(), name.size(), 300, ColumnType.TIMESTAMP, 0, 0, 0, 0, 0);
                }
                ParquetMetaFileWriter.addRowGroup(writerPtr, 500);
                ParquetMetaFileWriter.setParquetFooter(writerPtr, 0, 0);
                long resultPtr = ParquetMetaFileWriter.finish(writerPtr);
                try {
                    long dataPtr = ParquetMetaFileWriter.resultDataPtr(resultPtr);
                    long parquetMetaSize = ParquetMetaFileWriter.resultParquetMetaFileSize(resultPtr);

                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    reader.of(dataPtr, parquetMetaSize);
                    Assert.assertTrue(reader.resolveLastFooter());

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
                    Assert.assertEquals(ColumnType.DOUBLE, reader.getColumnType(0));
                    Assert.assertEquals(ColumnType.SYMBOL, reader.getColumnType(1));
                    Assert.assertEquals(ColumnType.TIMESTAMP, reader.getColumnType(2));

                    // Lookup by name.
                    Assert.assertEquals(0, reader.getColumnIndex("amount"));
                    Assert.assertEquals(1, reader.getColumnIndex("symbol"));
                    Assert.assertEquals(2, reader.getColumnIndex("ts"));
                    Assert.assertEquals(-1, reader.getColumnIndex("nonexistent"));

                    // Designated timestamp.
                    Assert.assertEquals(2, reader.getDesignatedTimestampColumnIndex());

                    // Writer-built files have no stats: stat_flags is 0.
                    // getChunkMin/MaxStat are precondition-guarded on
                    // MIN_PRESENT / MAX_PRESENT (see
                    // testGetChunkMinStatAssertsWhenMinAbsent /
                    // testGetChunkMaxStatAssertsWhenMaxAbsent), so only
                    // the flags accessor is exercised here.
                    Assert.assertEquals(0, reader.getChunkStatFlags(0, 0));
                } finally {
                    ParquetMetaFileWriter.destroyResult(resultPtr);
                }
            } finally {
                ParquetMetaFileWriter.destroyWriter(writerPtr);
            }
        });
    }

    @Test
    public void testGetColumnMaxDefLevel() throws Exception {
        // O3PartitionJob.hasLegacyRequiredNoSentinelColumn keys off this accessor to detect a
        // legacy file whose BYTE/SHORT/CHAR/SYMBOL columns are Required (max def level 0) so it
        // can force a full re-encode instead of corrupting the file with a migrated Optional
        // footer over raw-copied Required pages. Verify the off-heap read of max_def_level.
        assertMemoryLeak(() -> {
            long writerPtr = ParquetMetaFileWriter.create();
            try {
                ParquetMetaFileWriter.setDesignatedTimestamp(writerPtr, 2);
                try (DirectUtf8Sink name = new DirectUtf8Sink(16)) {
                    name.put("legacy_byte"); // Required (legacy): max def level 0
                    ParquetMetaFileWriter.addColumn(writerPtr, name.ptr(), name.size(), 100, ColumnType.BYTE, 0, 0, 0, 0, 0);
                }
                try (DirectUtf8Sink name = new DirectUtf8Sink(16)) {
                    name.put("modern_short"); // Optional (modern): max def level 1
                    ParquetMetaFileWriter.addColumn(writerPtr, name.ptr(), name.size(), 200, ColumnType.SHORT, 0, 0, 0, 0, 1);
                }
                try (DirectUtf8Sink name = new DirectUtf8Sink(16)) {
                    name.put("ts"); // designated timestamp: always Required, max def level 0
                    ParquetMetaFileWriter.addColumn(writerPtr, name.ptr(), name.size(), 300, ColumnType.TIMESTAMP, 0, 0, 0, 0, 0);
                }
                ParquetMetaFileWriter.addRowGroup(writerPtr, 500);
                ParquetMetaFileWriter.setParquetFooter(writerPtr, 0, 0);
                long resultPtr = ParquetMetaFileWriter.finish(writerPtr);
                try {
                    long dataPtr = ParquetMetaFileWriter.resultDataPtr(resultPtr);
                    long parquetMetaSize = ParquetMetaFileWriter.resultParquetMetaFileSize(resultPtr);

                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    reader.of(dataPtr, parquetMetaSize);
                    // Freshly staged single-snapshot _pm with no committed parquet size to
                    // MVCC-match on: resolve the physically-last footer, same as the sibling
                    // testColumnMetadataAccessors. resolveFooter(parquetFileSize) would never
                    // match (derived size 0+0+PARQUET_TRAILER_SIZE != the requested size).
                    Assert.assertTrue(reader.resolveLastFooter());

                    // Required (legacy) columns report 0; Optional (modern) report 1. The
                    // designated timestamp is Required in both old and new files, so the
                    // detection predicate must exclude it by tag rather than by repetition.
                    Assert.assertEquals(0, reader.getColumnMaxDefLevel(0));
                    Assert.assertEquals(1, reader.getColumnMaxDefLevel(1));
                    Assert.assertEquals(0, reader.getColumnMaxDefLevel(2));
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
            try (ParquetMetaTestFile file = buildFile(1, 100)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                try {
                    reader.of(file.dataPtr, file.parquetMetaFileSize);
                    Assert.assertTrue(reader.resolveLastFooter());

                    // Prime native verification on valid bytes, then corrupt columnCount.
                    // The second resolve reuses that cached verification and must reach the
                    // Java bounds check before any descriptor access can run past the mmap.
                    Unsafe.putInt(file.dataPtr + 24, 1_000_000_000);
                    reader.resolveLastFooter();
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getMessage(), "invalid _pm columnCount [count=1000000000");
                } finally {
                    reader.clear();
                }
            }
        });
    }

    @Test
    public void testCorruptedRowGroupCountValidatedBeforeLoop() throws Exception {
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFile(1, 100)) {
                // Compute footer address: trailer (last 4 bytes) holds footer length
                int footerLength = Unsafe.getInt(file.dataPtr + file.dataLen - 4);
                long footerAddr = file.dataPtr + file.dataLen - 4 - Integer.toUnsignedLong(footerLength);

                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                try {
                    reader.of(file.dataPtr, file.parquetMetaFileSize);
                    Assert.assertTrue(reader.resolveLastFooter());

                    // Prime native verification on valid bytes, then corrupt the footer.
                    // Reusing the same reader bypasses native reparsing so the Java
                    // length check must reject the count before the row-group loop.
                    Unsafe.putInt(footerAddr + 12, 1_000_000_000);
                    reader.resolveLastFooter();
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getMessage(), "invalid _pm footer length [rowGroupCount=1000000000");
                } finally {
                    reader.clear();
                }
            }
        });
    }

    @Test
    public void testCorruptedSortingColumnCountValidatedBeforeArrayRead() throws Exception {
        assertMemoryLeak(() -> {
            // buildFile sets dts=-1 and no sorting columns, so SORTING_IS_DTS_ASC
            // is clear and resolveFooter enters the explicit sorting-array bound
            // check. The first resolveFooter caches the CRC; the second skips
            // the checksum parse (which would otherwise reject the corrupt count via
            // the Rust header parse) and reaches the Java-side bound check, which
            // must reject before any accessor reads past the sorting array.
            try (ParquetMetaTestFile file = buildFile(1, 100)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                reader.resolveLastFooter();

                Unsafe.putInt(file.dataPtr + 20, 1_000_000_000); // HEADER_SORTING_COL_CNT_OFF

                try {
                    reader.resolveLastFooter();
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("invalid _pm sorting column count"));
                }
            }
        });
    }

    @Test
    public void testCorruptedTrailer() throws Exception {
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFile(1, 100)) {
                // Corrupt the footer length trailer to point past the file.
                // The trailer sits outside the CRC region, so re-checksumming
                // would not help: the checksum parse's from_file_size step uses
                // the trailer to derive the footer offset and rejects the
                // file ("footer length ... exceeds file size") before
                // computing the CRC. The Java-side resolveFooter has the
                // same trailer bound check ("invalid _pm footer offset");
                // either message is acceptable since both prove the corrupt
                // trailer surfaces a clean exception instead of a SIGSEGV.
                Unsafe.putInt(file.dataPtr + file.dataLen - 4, (int) file.dataLen);

                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                try {
                    reader.of(file.dataPtr, file.parquetMetaFileSize);
                    reader.resolveLastFooter();
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    Assert.assertTrue(
                            e.getMessage(),
                            e.getMessage().contains("invalid _pm footer offset")
                                    || e.getMessage().contains("footer length")
                    );
                }
            }
        });
    }

    @Test
    public void testCyclicMvccChainRejected() throws Exception {
        // Forge an MVCC chain that doubles back on itself. resolveFooter walks
        // back via prev_parquet_meta_file_size; a cyclic chain like B -> A -> B
        // (or self-loop A -> A) must terminate with a clean CairoException
        // rather than an infinite loop or SIGBUS. The strict-monotone check
        // (prevSize < currentSize) at ParquetMetaFileReader.java:669 is what
        // breaks the cycle. This test forges the second snapshot's prevSize
        // so the chain walk is asked to step from currentSize=newTotalLen
        // back to prevSize=newTotalLen, which violates the monotonicity guard.
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFile(1, 100, 50, 1000)) {
                long origLen = file.dataLen;
                int origFooterLength = Unsafe.getInt(file.dataPtr + origLen - 4);
                long origFooterOffset = origLen - 4 - Integer.toUnsignedLong(origFooterLength);
                int rowGroupEntry = Unsafe.getInt(file.dataPtr + origFooterOffset + 40);

                // Append a second footer (same layout as
                // testFooterChainWalkResolvesCorrectFooter), then overwrite
                // its prev_parquet_meta_file_size with the published total
                // size — the chain step would point back at the same snapshot
                // it just resolved, i.e. prevSize == currentSize.
                int newFooterBytes = 52;
                long newTotalLen = origLen + newFooterBytes;
                long newBuf = Unsafe.malloc(newTotalLen, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.copyMemory(file.dataPtr, newBuf, origLen);
                    long fa = newBuf + origLen;
                    Unsafe.putLong(fa, 200L);             // parquet_footer_offset
                    Unsafe.putInt(fa + 8, 80);            // parquet_footer_length
                    Unsafe.putInt(fa + 12, 1);            // row_group_count
                    Unsafe.putLong(fa + 16, 0L);          // unused_bytes
                    Unsafe.putLong(fa + 24, newTotalLen); // prev_parquet_meta_file_size == currentSize (cycle)
                    Unsafe.putLong(fa + 32, 0L);          // footer_feature_flags
                    Unsafe.putInt(fa + 40, rowGroupEntry);
                    Unsafe.putInt(fa + 44, 0);            // CRC placeholder
                    Unsafe.putInt(fa + 48, 48);           // trailer
                    Unsafe.putLong(newBuf, newTotalLen);  // publish snapshot
                    patchCrc(newBuf, newTotalLen);

                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    reader.of(newBuf, newTotalLen);
                    // The latest footer's derived parquet size is 288. Request
                    // a non-matching size so the chain walk advances and trips
                    // the monotonicity guard. resolveFooter signals "no match
                    // and chain refuses to continue" by returning false.
                    Assert.assertFalse(reader.resolveFooter(9999L));
                } finally {
                    Unsafe.free(newBuf, newTotalLen, MemoryTag.NATIVE_DEFAULT);
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
                    long parquetMetaSize = ParquetMetaFileWriter.resultParquetMetaFileSize(resultPtr);

                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    reader.of(dataPtr, parquetMetaSize);
                    reader.resolveLastFooter();
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
    public void testSortingColumnIndexDtsAsc() throws Exception {
        assertMemoryLeak(() -> {
            // Sort column == ascending designated timestamp -> SORTING_IS_DTS_ASC,
            // no explicit array; getSortingColumnIndex reads the designated index.
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
                ParquetMetaFileWriter.addSortingColumn(writerPtr, 0);
                long resultPtr = ParquetMetaFileWriter.finish(writerPtr);
                try {
                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    reader.of(ParquetMetaFileWriter.resultDataPtr(resultPtr), ParquetMetaFileWriter.resultParquetMetaFileSize(resultPtr));
                    reader.resolveLastFooter();
                    Assert.assertEquals(1, reader.getSortingColumnCount());
                    Assert.assertEquals(0, reader.getSortingColumnIndex(0));
                } finally {
                    ParquetMetaFileWriter.destroyResult(resultPtr);
                }
            } finally {
                ParquetMetaFileWriter.destroyWriter(writerPtr);
            }
        });
    }

    @Test
    public void testSortingColumnIndexExplicitArray() throws Exception {
        assertMemoryLeak(() -> {
            // Sort column != designated timestamp -> the index comes from the
            // explicit on-disk array, not getDesignatedTimestampColumnIndex().
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
                ParquetMetaFileWriter.addSortingColumn(writerPtr, 1);
                long resultPtr = ParquetMetaFileWriter.finish(writerPtr);
                try {
                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    reader.of(ParquetMetaFileWriter.resultDataPtr(resultPtr), ParquetMetaFileWriter.resultParquetMetaFileSize(resultPtr));
                    reader.resolveLastFooter();
                    Assert.assertEquals(1, reader.getSortingColumnCount());
                    Assert.assertEquals(0, reader.getDesignatedTimestampColumnIndex());
                    Assert.assertEquals(1, reader.getSortingColumnIndex(0));
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
            try (ParquetMetaTestFile file = buildFile(2, 100)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                reader.resolveLastFooter();
                Assert.assertEquals(-1, reader.getDesignatedTimestampColumnIndex());
            }
        });
    }

    @Test
    public void testExtraFooterBytesWithNoFeaturesRejected() throws Exception {
        // Forge a footer with 8 unaccounted bytes between the row-group entry and
        // the CRC and no optional feature flags: it resolves by parquet size, but
        // currentFooterLength != the expected baseFooterLength trips the guard.
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFile(1, 100)) {
                long origLen = file.dataLen;
                int origFooterLength = Unsafe.getInt(file.dataPtr + origLen - 4);
                long origFooterOffset = origLen - 4 - Integer.toUnsignedLong(origFooterLength);
                int rowGroupEntry = Unsafe.getInt(file.dataPtr + origFooterOffset + 40);

                // Appended footer: fixed(40) + 1 rg entry(4) + 8 extra bytes + CRC(4) + trailer(4) = 60.
                long newTotalLen = origLen + 60;
                long newBuf = Unsafe.malloc(newTotalLen, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.copyMemory(file.dataPtr, newBuf, origLen);
                    long fa = newBuf + origLen;
                    Unsafe.putLong(fa, 400L);             // parquet_footer_offset
                    Unsafe.putInt(fa + 8, 80);            // parquet_footer_length -> derived size 488
                    Unsafe.putInt(fa + 12, 1);            // row_group_count
                    Unsafe.putLong(fa + 16, 0L);          // unused_bytes
                    Unsafe.putLong(fa + 24, origLen);     // prev_parquet_meta_file_size
                    Unsafe.putLong(fa + 32, 0L);          // footer_feature_flags (none)
                    Unsafe.putInt(fa + 40, rowGroupEntry);
                    Unsafe.putLong(fa + 44, 0L);          // 8 unaccounted bytes
                    Unsafe.putInt(fa + 52, 0);            // CRC placeholder (patched below)
                    Unsafe.putInt(fa + 56, 56);           // trailer: footer length incl. the extra bytes
                    Unsafe.putLong(newBuf, newTotalLen);  // publish snapshot
                    patchCrc(newBuf, newTotalLen);

                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    try {
                        reader.of(newBuf, newTotalLen);
                        reader.resolveFooter(488L);
                        Assert.fail("expected CairoException");
                    } catch (CairoException e) {
                        Assert.assertTrue(
                                e.getMessage(),
                                e.getMessage().contains("unexpected _pm footer feature bytes")
                        );
                    }
                } finally {
                    Unsafe.free(newBuf, newTotalLen, MemoryTag.NATIVE_DEFAULT);
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
                Unsafe.putLong(addr, 4L); // implausibly small
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                try {
                    reader.of(addr, 4L);
                    reader.resolveLastFooter();
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
    public void testFooterChainWalkRejectsUndersizedPrevSize() throws Exception {
        // HEADER_FIXED_SIZE(32) + FOOTER_TRAILER_SIZE(4) = 36 is the minimum
        // legal prev_parquet_meta_file_size. Values in [1, 35] would cause the
        // next iteration to dereference before the mapping start without the
        // guard at ParquetMetaFileReader.of(). The walk must reject them with
        // a CairoException instead of crashing.
        assertMemoryLeak(() -> {
            for (long badPrevSize : new long[]{1L, 3L, 35L}) {
                try (ParquetMetaTestFile file = buildFile(1, 100, 50, 1000)) {
                    long origLen = file.dataLen;

                    // Mirror the two-footer layout from
                    // testFooterChainWalkResolvesCorrectFooter, then overwrite
                    // prev_parquet_meta_file_size in the latest footer with
                    // the bad value.
                    int rowGroupEntry = Unsafe.getInt(
                            file.dataPtr + origLen - 4
                                    - Integer.toUnsignedLong(Unsafe.getInt(file.dataPtr + origLen - 4))
                                    + 40 // FOOTER_FIXED_SIZE
                    );
                    int newFooterBytes = 52;
                    long newTotalLen = origLen + newFooterBytes;
                    long newBuf = Unsafe.malloc(newTotalLen, MemoryTag.NATIVE_DEFAULT);
                    try {
                        Unsafe.copyMemory(file.dataPtr, newBuf, origLen);
                        long fa = newBuf + origLen;
                        Unsafe.putLong(fa, 200L);              // parquet_footer_offset
                        Unsafe.putInt(fa + 8, 80);             // parquet_footer_length
                        Unsafe.putInt(fa + 12, 1);             // row_group_count
                        Unsafe.putLong(fa + 16, 0L);           // unused_bytes
                        Unsafe.putLong(fa + 24, badPrevSize);  // prev_parquet_meta_file_size (bad)
                        Unsafe.putLong(fa + 32, 0L);           // footer_feature_flags
                        Unsafe.putInt(fa + 40, rowGroupEntry); // row group entry
                        Unsafe.putInt(fa + 44, 0);             // CRC placeholder
                        Unsafe.putInt(fa + 48, 48);            // trailer: footer_length
                        Unsafe.putLong(newBuf, newTotalLen);   // header parquet_meta_file_size

                        // Recompute CRC after the snapshot is fully published
                        // so resolveFooter's up-front checksum step
                        // accepts the file and the chain-walk validation
                        // (the test's actual subject) gets to fire.
                        patchCrc(newBuf, newTotalLen);

                        ParquetMetaFileReader reader = new ParquetMetaFileReader();
                        // Latest footer's derived parquet size is 288; request
                        // a different size to force the chain walk to step
                        // back to prev_parquet_meta_file_size.
                        reader.of(newBuf, newTotalLen);
                        Assert.assertFalse(reader.resolveFooter(9999L));
                    } finally {
                        Unsafe.free(newBuf, newTotalLen, MemoryTag.NATIVE_DEFAULT);
                    }
                }
            }
        });
    }

    @Test
    public void testFooterChainWalkResolvesAcrossThreeLevels() throws Exception {
        assertMemoryLeak(() -> {
            // Three-footer MVCC chain. Each appended footer points its
            // prev_parquet_meta_file_size back at the previous snapshot's
            // committed size, forcing the walk in ParquetMetaFileReader.of()
            // to iterate twice when resolving the oldest parquet size.
            try (ParquetMetaTestFile file = buildFile(1, 100, 50, 1000)) {
                long origLen = file.dataLen;
                int origFooterLength = Unsafe.getInt(file.dataPtr + origLen - 4);
                long origFooterOffset = origLen - 4 - Integer.toUnsignedLong(origFooterLength);
                int rowGroupEntry = Unsafe.getInt(file.dataPtr + origFooterOffset + 40);

                // Layout per appended footer: fixed(40) + 1 rg entry(4) + CRC(4) + trailer(4) = 52.
                int appendedFooterBytes = 52;
                long footer2Start = origLen;
                long footer3Start = origLen + appendedFooterBytes;
                long newTotalLen = origLen + 2L * appendedFooterBytes;

                long newBuf = Unsafe.malloc(newTotalLen, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.copyMemory(file.dataPtr, newBuf, origLen);

                    // Footer 2: parquetFooterOff=200, parquetFooterLen=80 -> derived size = 288.
                    // prev = origLen points back at the base snapshot's committed size.
                    long fa2 = newBuf + footer2Start;
                    Unsafe.putLong(fa2, 200L);
                    Unsafe.putInt(fa2 + 8, 80);
                    Unsafe.putInt(fa2 + 12, 1);
                    Unsafe.putLong(fa2 + 16, 0L);
                    Unsafe.putLong(fa2 + 24, origLen);
                    Unsafe.putLong(fa2 + 32, 0L);
                    Unsafe.putInt(fa2 + 40, rowGroupEntry);
                    Unsafe.putInt(fa2 + 44, 0);
                    Unsafe.putInt(fa2 + 48, 48);

                    // Footer 3: parquetFooterOff=400, parquetFooterLen=80 -> derived size = 488.
                    // prev = footer3Start points back at footer 2's committed size.
                    long fa3 = newBuf + footer3Start;
                    Unsafe.putLong(fa3, 400L);
                    Unsafe.putInt(fa3 + 8, 80);
                    Unsafe.putInt(fa3 + 12, 1);
                    Unsafe.putLong(fa3 + 16, 0L);
                    Unsafe.putLong(fa3 + 24, footer3Start);
                    Unsafe.putLong(fa3 + 32, 0L);
                    Unsafe.putInt(fa3 + 40, rowGroupEntry);
                    Unsafe.putInt(fa3 + 44, 0);
                    Unsafe.putInt(fa3 + 48, 48);

                    // Patch header parquet_meta_file_size to publish footer 3.
                    Unsafe.putLong(newBuf, newTotalLen);

                    // Recompute the cumulative CRC of every snapshot the test
                    // resolves, oldest-first (each footer's CRC covers the file
                    // through its own committed size). resolveFooter now verifies
                    // the *resolved* footer's CRC after the chain walk — not just
                    // the latest physical footer — so footer 2 needs a valid CRC
                    // too. The base footer's CRC was written by buildFile.
                    patchCrc(newBuf, footer3Start);
                    patchCrc(newBuf, newTotalLen);

                    ParquetMetaFileReader reader = new ParquetMetaFileReader();

                    // Latest footer resolves directly.
                    reader.of(newBuf, newTotalLen);
                    reader.resolveFooter(488L);
                    Assert.assertEquals(488L, reader.getParquetFileSize());
                    Assert.assertEquals(1, reader.getRowGroupCount());
                    Assert.assertEquals(1000L, reader.getRowGroupSize(0));

                    // Middle footer resolves via one chain step.
                    reader.of(newBuf, newTotalLen);
                    reader.resolveFooter(288L);
                    Assert.assertEquals(288L, reader.getParquetFileSize());
                    Assert.assertEquals(1, reader.getRowGroupCount());
                    Assert.assertEquals(1000L, reader.getRowGroupSize(0));

                    // Base footer resolves only after two chain steps.
                    reader.of(newBuf, newTotalLen);
                    reader.resolveFooter(158L);
                    Assert.assertEquals(158L, reader.getParquetFileSize());
                    Assert.assertEquals(1, reader.getRowGroupCount());
                    Assert.assertEquals(1000L, reader.getRowGroupSize(0));

                    // Unmatched size exhausts the chain and throws cleanly.
                    reader.of(newBuf, newTotalLen);
                    Assert.assertFalse(reader.resolveFooter(9999L));
                } finally {
                    Unsafe.free(newBuf, newTotalLen, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testResolveFooterSkipsOrphanedDeadFooter() throws Exception {
        // Models the append-past-dead-footer design. A committed footer C ends
        // at origLen (derived parquet size 158). A since-rolled-back in-place
        // update left an orphaned dead footer D in the tail. The next update
        // appended a new footer C' whose prev_parquet_meta_file_size points at C
        // (origLen), NOT at D — so the MVCC walk reaches C' then C and never
        // reads D, and D's version is unreachable. The published-snapshot CRC
        // still spans D's bytes, so a reader resolving C' (now CRC-validated
        // against the *resolved* footer, not the physically-last one) accepts it.
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFile(1, 100, 50, 1000)) {
                long origLen = file.dataLen;
                int origFooterLength = Unsafe.getInt(file.dataPtr + origLen - 4);
                long origFooterOffset = origLen - 4 - Integer.toUnsignedLong(origFooterLength);
                int rowGroupEntry = Unsafe.getInt(file.dataPtr + origFooterOffset + 40);

                // Layout per appended footer: fixed(40) + 1 rg entry(4) + CRC(4) + trailer(4) = 52.
                int appendedFooterBytes = 52;
                long deadStart = origLen;                       // orphaned dead footer D
                long newStart = origLen + appendedFooterBytes;  // new footer C'
                long newTotalLen = origLen + 2L * appendedFooterBytes;

                long newBuf = Unsafe.malloc(newTotalLen, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.copyMemory(file.dataPtr, newBuf, origLen);

                    // Dead footer D: derived parquet size 288, prev back at C. It
                    // was published by a since-rolled-back update; nothing in the
                    // live chain points at it.
                    long fd = newBuf + deadStart;
                    Unsafe.putLong(fd, 200L);
                    Unsafe.putInt(fd + 8, 80);
                    Unsafe.putInt(fd + 12, 1);
                    Unsafe.putLong(fd + 16, 0L);
                    Unsafe.putLong(fd + 24, origLen);
                    Unsafe.putLong(fd + 32, 0L);
                    Unsafe.putInt(fd + 40, rowGroupEntry);
                    Unsafe.putInt(fd + 44, 0);
                    Unsafe.putInt(fd + 48, 48);

                    // New footer C': derived parquet size 488. prev points at C
                    // (origLen), SKIPPING the dead footer D at deadStart.
                    long fc = newBuf + newStart;
                    Unsafe.putLong(fc, 400L);
                    Unsafe.putInt(fc + 8, 80);
                    Unsafe.putInt(fc + 12, 1);
                    Unsafe.putLong(fc + 16, 0L);
                    Unsafe.putLong(fc + 24, origLen); // prev = C, not D
                    Unsafe.putLong(fc + 32, 0L);
                    Unsafe.putInt(fc + 40, rowGroupEntry);
                    Unsafe.putInt(fc + 44, 0);
                    Unsafe.putInt(fc + 48, 48);

                    // Publish C' and recompute the CRC over the whole snapshot,
                    // which covers the dead footer's bytes too.
                    Unsafe.putLong(newBuf, newTotalLen);
                    patchCrc(newBuf, newTotalLen);

                    ParquetMetaFileReader reader = new ParquetMetaFileReader();

                    // C' resolves directly; the CRC validated is the resolved
                    // footer's, spanning the dead region.
                    reader.of(newBuf, newTotalLen);
                    Assert.assertTrue(reader.resolveFooter(488L));
                    Assert.assertEquals(488L, reader.getParquetFileSize());
                    Assert.assertEquals(1, reader.getRowGroupCount());
                    Assert.assertEquals(newTotalLen, reader.getResolvedFileSize());

                    // C resolves via one chain step that jumps from C' straight to
                    // C — never reading the dead footer D.
                    reader.of(newBuf, newTotalLen);
                    Assert.assertTrue(reader.resolveFooter(158L));
                    Assert.assertEquals(158L, reader.getParquetFileSize());
                    Assert.assertEquals(1, reader.getRowGroupCount());
                    Assert.assertEquals(origLen, reader.getResolvedFileSize());

                    // The dead footer's version is unreachable: nothing's prev
                    // points at it, so the walk exhausts the chain.
                    reader.of(newBuf, newTotalLen);
                    Assert.assertFalse(reader.resolveFooter(288L));
                } finally {
                    Unsafe.free(newBuf, newTotalLen, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testDirtyAheadHeaderResolvesToCommittedHead() throws Exception {
        // The crash window: a "dirty ahead" header (M) points at a speculative
        // footer C' past the committed footer C (an update published the header
        // then crashed before its _txn commit). resolveFooter walks back to C, so
        // getResolvedFileSize() -- the in-place-update parse anchor -- is C's head
        // N, not the mapped M.
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFile(1, 100, 50, 1000)) {
                final long committedHead = file.dataLen;     // N: footer C, derived parquet size 158
                final int origFooterLength = Unsafe.getInt(file.dataPtr + committedHead - 4);
                final long origFooterOffset = committedHead - 4 - Integer.toUnsignedLong(origFooterLength);
                final int rowGroupEntry = Unsafe.getInt(file.dataPtr + origFooterOffset + 40);

                // Layout per appended footer: fixed(40) + 1 rg entry(4) + CRC(4) + trailer(4) = 52.
                final int appendedFooterBytes = 52;
                final long dirtyLen = committedHead + appendedFooterBytes; // M: header points past C, at C'

                final long buf = Unsafe.malloc(dirtyLen, MemoryTag.NATIVE_DEFAULT);
                final long scratch = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                final FilesFacade ff = configuration.getFilesFacade();
                try (Path path = new Path()) {
                    Unsafe.copyMemory(file.dataPtr, buf, committedHead);

                    // Speculative footer C' (derived parquet size 488), prev = C.
                    final long fc = buf + committedHead;
                    Unsafe.putLong(fc, 400L);
                    Unsafe.putInt(fc + 8, 80);
                    Unsafe.putInt(fc + 12, 1);
                    Unsafe.putLong(fc + 16, 0L);
                    Unsafe.putLong(fc + 24, committedHead);
                    Unsafe.putLong(fc + 32, 0L);
                    Unsafe.putInt(fc + 40, rowGroupEntry);
                    Unsafe.putInt(fc + 44, 0);
                    Unsafe.putInt(fc + 48, 48);
                    // Dirty header points at C'; CRC spans the snapshot.
                    Unsafe.putLong(buf, dirtyLen);
                    patchCrc(buf, dirtyLen);

                    path.concat(root).concat("dirty_ahead_pm").$();
                    long fd = ff.openRW(path.$(), configuration.getWriterFileOpenOpts());
                    Assert.assertTrue(fd > -1);
                    try {
                        Assert.assertEquals(dirtyLen, ff.write(fd, buf, dirtyLen, 0));
                    } finally {
                        ff.close(fd);
                    }

                    // Open at the dirty header, resolve the committed footer (size 158).
                    final ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    long addr = ParquetMetaFileReader.openAndMapRO(ff, path.$(), reader);
                    Assert.assertTrue("openAndMapRO must map the dirty-ahead _pm", addr != 0);
                    long mappedSize = reader.getFileSize();
                    try {
                        Assert.assertTrue(reader.resolveFooter(158L));
                        Assert.assertEquals("mapped header is the dirty-ahead M", dirtyLen, reader.getFileSize());
                        Assert.assertEquals("resolved head is the committed N", committedHead, reader.getResolvedFileSize());
                        Assert.assertEquals("physical length is M (never truncated)", dirtyLen, ff.length(path.$()));
                        Assert.assertTrue("committed head precedes the dirty header", reader.getResolvedFileSize() < reader.getFileSize());
                        Assert.assertEquals(158L, reader.getParquetFileSize());
                        Assert.assertEquals(1, reader.getRowGroupCount());
                    } finally {
                        reader.clear();
                        ff.munmap(addr, mappedSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
                    }

                    // A plain failed update leaves the header at the committed head
                    // N (it is never patched) with the speculative footer as a dead
                    // tail past it -- no truncate, no restore. Model that by writing
                    // N to the header.
                    Unsafe.putLong(scratch, committedHead);
                    fd = ff.openRW(path.$(), configuration.getWriterFileOpenOpts());
                    Assert.assertTrue(fd > -1);
                    try {
                        Assert.assertEquals(Long.BYTES, ff.write(fd, scratch, Long.BYTES, 0));
                    } finally {
                        ff.close(fd);
                    }

                    // Re-open: header == N, C' kept as a dead tail past it.
                    addr = ParquetMetaFileReader.openAndMapRO(ff, path.$(), reader);
                    Assert.assertTrue(addr != 0);
                    mappedSize = reader.getFileSize();
                    try {
                        Assert.assertTrue(reader.resolveFooter(158L));
                        Assert.assertEquals("header at the committed head", committedHead, reader.getFileSize());
                        Assert.assertEquals(committedHead, reader.getResolvedFileSize());
                        final long physical = ff.length(path.$());
                        Assert.assertEquals("dead tail retained, not truncated", dirtyLen, physical);
                        Assert.assertTrue("physical exceeds the committed header", physical > reader.getFileSize());
                        Assert.assertEquals(158L, reader.getParquetFileSize());
                        Assert.assertEquals(1, reader.getRowGroupCount());
                    } finally {
                        reader.clear();
                        ff.munmap(addr, mappedSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
                    }
                } finally {
                    Unsafe.free(buf, dirtyLen, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(scratch, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testResolveLastFooterTakesPhysicallyLastFooter() throws Exception {
        // resolveLastFooter() takes the physically-last footer -- the complement
        // of resolveFooter(committedSize), which skips a dead tail. The header
        // points past the committed footer C (size 158) at a dead footer C' (size
        // 488): resolveFooter(158) resolves C, resolveLastFooter() resolves C'.
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFile(1, 100, 50, 1000)) {
                final long committedHead = file.dataLen;
                final int origFooterLength = Unsafe.getInt(file.dataPtr + committedHead - 4);
                final long origFooterOffset = committedHead - 4 - Integer.toUnsignedLong(origFooterLength);
                final int rowGroupEntry = Unsafe.getInt(file.dataPtr + origFooterOffset + 40);

                // Appended footer C': fixed(40) + 1 rg entry(4) + CRC(4) + trailer(4) = 52.
                final long dirtyLen = committedHead + 52;
                final long buf = Unsafe.malloc(dirtyLen, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.copyMemory(file.dataPtr, buf, committedHead);
                    final long fc = buf + committedHead;
                    Unsafe.putLong(fc, 400L);               // parquet_footer_offset
                    Unsafe.putInt(fc + 8, 80);              // parquet_footer_length -> derived size 488
                    Unsafe.putInt(fc + 12, 1);              // row_group_count
                    Unsafe.putLong(fc + 16, 0L);            // unused_bytes
                    Unsafe.putLong(fc + 24, committedHead); // prev_parquet_meta_file_size -> C
                    Unsafe.putLong(fc + 32, 0L);            // footer_feature_flags
                    Unsafe.putInt(fc + 40, rowGroupEntry);
                    Unsafe.putInt(fc + 44, 0);              // CRC placeholder (patched below)
                    Unsafe.putInt(fc + 48, 48);             // trailer: footer length
                    Unsafe.putLong(buf, dirtyLen);          // header points at C'
                    patchCrc(buf, dirtyLen);

                    final ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    // Matching the committed size walks back past C' to C.
                    reader.of(buf, dirtyLen);
                    Assert.assertTrue(reader.resolveFooter(158L));
                    Assert.assertEquals(committedHead, reader.getResolvedFileSize());
                    Assert.assertEquals(158L, reader.getParquetFileSize());

                    // resolveLastFooter() takes C', the physically-last footer.
                    reader.of(buf, dirtyLen);
                    Assert.assertTrue(reader.resolveLastFooter());
                    Assert.assertEquals(dirtyLen, reader.getResolvedFileSize());
                    Assert.assertEquals(488L, reader.getParquetFileSize());
                    reader.clear();
                } finally {
                    Unsafe.free(buf, dirtyLen, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testDirtyAheadCanSkipRowGroupUsesCommittedFooter() throws Exception {
        // canSkipRowGroup's native reader must key on the resolved
        // committed head, not the raw dirty-ahead header. The committed footer C
        // carries TWO row groups; the orphaned dead footer C' past it carries
        // ONE. Keyed on C', row group index 1 is out of range and the native side
        // throws; keyed on the committed footer both indices prune cleanly.
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFile(1, 100, 50, 1000, 2000)) {
                final long committedHead = file.dataLen; // N: committed footer C (2 row groups), derived parquet size 158
                final int origFooterLength = Unsafe.getInt(file.dataPtr + committedHead - 4);
                final long origFooterOffset = committedHead - 4 - Integer.toUnsignedLong(origFooterLength);
                final int rowGroupEntry0 = Unsafe.getInt(file.dataPtr + origFooterOffset + 40); // C's first row group entry

                // Dead footer C': fixed(40) + 1 rg entry(4) + CRC(4) + trailer(4) = 52.
                final int appendedFooterBytes = 52;
                final long dirtyLen = committedHead + appendedFooterBytes; // M: header points past C, at C'

                final long buf = Unsafe.malloc(dirtyLen, MemoryTag.NATIVE_DEFAULT);
                final FilesFacade ff = configuration.getFilesFacade();
                try (
                        Path path = new Path();
                        DirectLongList emptyFilters = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT)
                ) {
                    Unsafe.copyMemory(file.dataPtr, buf, committedHead);

                    // Speculative footer C' with a SINGLE row group (fewer than
                    // committed C's two), reusing C's first block, prev = C.
                    final long fc = buf + committedHead;
                    Unsafe.putLong(fc, 400L);
                    Unsafe.putInt(fc + 8, 80);
                    Unsafe.putInt(fc + 12, 1);
                    Unsafe.putLong(fc + 16, 0L);
                    Unsafe.putLong(fc + 24, committedHead);
                    Unsafe.putLong(fc + 32, 0L);
                    Unsafe.putInt(fc + 40, rowGroupEntry0);
                    Unsafe.putInt(fc + 44, 0);
                    Unsafe.putInt(fc + 48, 48);
                    // Dirty header points at C'; CRC spans the whole snapshot.
                    Unsafe.putLong(buf, dirtyLen);
                    patchCrc(buf, dirtyLen);

                    path.concat(root).concat("dirty_ahead_canskip_pm").$();
                    long fd = ff.openRW(path.$(), configuration.getWriterFileOpenOpts());
                    Assert.assertTrue(fd > -1);
                    try {
                        Assert.assertEquals(dirtyLen, ff.write(fd, buf, dirtyLen, 0));
                    } finally {
                        ff.close(fd);
                    }

                    final ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    long addr = ParquetMetaFileReader.openAndMapRO(ff, path.$(), reader);
                    Assert.assertTrue("openAndMapRO must map the dirty-ahead _pm", addr != 0);
                    final long mappedSize = reader.getFileSize();
                    try {
                        Assert.assertTrue(reader.resolveFooter(158L));
                        Assert.assertEquals("mapped header is the dirty-ahead M", dirtyLen, reader.getFileSize());
                        Assert.assertEquals("resolved head is the committed N", committedHead, reader.getResolvedFileSize());
                        Assert.assertEquals("committed footer has two row groups", 2, reader.getRowGroupCount());

                        // First skip lazily creates the native reader. It must
                        // parse the committed footer C, so both committed row
                        // groups are addressable; keyed on the dead footer C',
                        // index 1 would throw a CairoException -- the C1 bug.
                        Assert.assertFalse(reader.canSkipRowGroup(0, emptyFilters, 0));
                        Assert.assertFalse(reader.canSkipRowGroup(1, emptyFilters, 0));
                    } finally {
                        reader.clear();
                        ff.munmap(addr, mappedSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
                    }
                } finally {
                    Unsafe.free(buf, dirtyLen, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testFooterChainWalkResolvesCorrectFooter() throws Exception {
        assertMemoryLeak(() -> {
            // Build a single-footer _pm: 1 column, parquetFooterOff=100, parquetFooterLen=50,
            // 1 row group with 1000 rows. Derived parquet size = 100 + 50 + 8 = 158.
            try (ParquetMetaTestFile file = buildFile(1, 100, 50, 1000)) {
                long origLen = file.dataLen;
                // The original file's footer offset is derived from the trailer.
                int origFooterLength = Unsafe.getInt(file.dataPtr + origLen - 4);
                long origFooterOffset = origLen - 4 - Integer.toUnsignedLong(origFooterLength);

                // Read the row group entry from the original footer to reuse in footer2.
                int rowGroupEntry = Unsafe.getInt(
                        file.dataPtr + origFooterOffset + 40 // FOOTER_FIXED_SIZE
                );

                // Append a second footer with a different parquet file size.
                // Footer2: parquetFooterOff=200, parquetFooterLen=80 → derived size = 288.
                // Layout: fixed(40) + 1 rg entry(4) + CRC(4) + trailer(4) = 52 bytes.
                int newFooterBytes = 52;
                long newTotalLen = origLen + newFooterBytes;
                long newBuf = Unsafe.malloc(newTotalLen, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.copyMemory(file.dataPtr, newBuf, origLen);

                    long newFooterOff = origLen;
                    long fa = newBuf + newFooterOff;

                    // Footer fixed portion (40 bytes). prev_parquet_meta_file_size
                    // is the old committed size — the original file's length —
                    // which lets the reader walk back via the old trailer at
                    // origLen - 4.
                    Unsafe.putLong(fa, 200L);              // parquet_footer_offset
                    Unsafe.putInt(fa + 8, 80);             // parquet_footer_length
                    Unsafe.putInt(fa + 12, 1);             // row_group_count
                    Unsafe.putLong(fa + 16, 0L);           // unused_bytes
                    Unsafe.putLong(fa + 24, origLen);      // prev_parquet_meta_file_size
                    Unsafe.putLong(fa + 32, 0L);           // footer_feature_flags

                    // Row group entry (reuse the same block offset)
                    Unsafe.putInt(fa + 40, rowGroupEntry);

                    // CRC placeholder, patched after header parquet_meta_file_size.
                    Unsafe.putInt(fa + 44, 0);

                    // Trailer: footer_length = fixed(40) + rg(4) + CRC(4) = 48
                    Unsafe.putInt(fa + 48, 48);

                    // Patch header parquet_meta_file_size to publish the new snapshot —
                    // this is the MVCC commit signal the reader observes.
                    Unsafe.putLong(newBuf, newTotalLen);

                    // Recompute CRC over the new snapshot now that the header
                    // size and the new footer are in their final layout. The
                    // resolveFooter() path verifies CRC up front before any
                    // chain walk, so the published snapshot must carry a
                    // matching CRC.
                    patchCrc(newBuf, newTotalLen);

                    ParquetMetaFileReader reader = new ParquetMetaFileReader();

                    // Latest footer (parquet size 288) resolves directly.
                    reader.of(newBuf, newTotalLen);
                    reader.resolveFooter(288L);
                    Assert.assertEquals(288L, reader.getParquetFileSize());
                    Assert.assertEquals(1, reader.getRowGroupCount());
                    Assert.assertEquals(1000L, reader.getRowGroupSize(0));

                    // Old footer (parquet size 158) resolves via chain walk.
                    reader.of(newBuf, newTotalLen);
                    reader.resolveFooter(158L);
                    Assert.assertEquals(158L, reader.getParquetFileSize());
                    Assert.assertEquals(1, reader.getRowGroupCount());
                    Assert.assertEquals(1000L, reader.getRowGroupSize(0));

                    // Non-matching parquet size throws STALE_PARQUET_METADATA.
                    reader.of(newBuf, newTotalLen);
                    Assert.assertFalse(reader.resolveFooter(9999L));
                } finally {
                    Unsafe.free(newBuf, newTotalLen, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testGetChunkMaxStatAssertsWhenMaxAbsent() throws Exception {
        // buildFile() writes row groups with stat_flags == 0, so neither
        // MAX_PRESENT nor MAX_INLINED is set. The accessor must trip its
        // precondition assert under -ea (which AbstractCairoTest runs with).
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFile(1, 100)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                reader.resolveLastFooter();
                try {
                    reader.getChunkMaxStat(0, 0);
                    Assert.fail("expected AssertionError");
                } catch (AssertionError expected) {
                    TestUtils.assertContains(expected.getMessage(), "max_stat absent or not inlined");
                }
                reader.clear();
            }
        });
    }

    @Test
    public void testGetChunkMinStatAssertsWhenMinAbsent() throws Exception {
        // buildFile() writes row groups with stat_flags == 0, so neither
        // MIN_PRESENT nor MIN_INLINED is set. The accessor must trip its
        // precondition assert under -ea (which AbstractCairoTest runs with).
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFile(1, 100)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                reader.resolveLastFooter();
                try {
                    reader.getChunkMinStat(0, 0);
                    Assert.fail("expected AssertionError");
                } catch (AssertionError expected) {
                    TestUtils.assertContains(expected.getMessage(), "min_stat absent or not inlined");
                }
                reader.clear();
            }
        });
    }

    @Test
    public void testGetParquetFileSize() throws Exception {
        assertMemoryLeak(() -> {
            // parquet file size = parquetFooterOffset + parquetFooterLength + 8
            try (ParquetMetaTestFile file = buildFile(1, 4096, 256, 100)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                reader.resolveLastFooter();

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
            try (ParquetMetaTestFile file = buildFile(1, sizes)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                reader.resolveLastFooter();

                Assert.assertEquals(128, reader.getRowGroupCount());
                for (int i = 0; i < 128; i++) {
                    Assert.assertEquals((i + 1) * 10L, reader.getRowGroupSize(i));
                }
            }
        });
    }

    @Test
    public void testLifecycleCloseWithoutOf() throws Exception {
        assertMemoryLeak(() -> {
            ParquetMetaFileReader reader = new ParquetMetaFileReader();
            // close() before of() must be a no-op (no native handle was ever
            // allocated). assertMemoryLeak catches a leak if this allocates.
            reader.clear();
            Assert.assertFalse(reader.isOpen());
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
                    ParquetMetaTestFile file1 = buildFile(1, 100);
                    ParquetMetaTestFile file2 = buildFile(2, 0, 0, 200, 300)
            ) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();

                reader.of(file1.dataPtr, file1.parquetMetaFileSize);
                reader.resolveLastFooter();
                try (DirectLongList filters = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT)) {
                    // First skip call lazily allocates the native handle.
                    Assert.assertFalse(reader.canSkipRowGroup(0, filters, 0));
                }

                // Re-init via of(): the previous native handle must be freed
                // by clear() inside of() before storing the new addr/size.
                reader.of(file2.dataPtr, file2.parquetMetaFileSize);
                reader.resolveLastFooter();
                Assert.assertEquals(2, reader.getRowGroupCount());

                try (DirectLongList filters = new DirectLongList(0, MemoryTag.NATIVE_DEFAULT)) {
                    // Second skip call lazily allocates a fresh native handle
                    // over the new mmap.
                    Assert.assertFalse(reader.canSkipRowGroup(1, filters, 0));
                }

                reader.clear();
            }
        });
    }

    @Test
    public void testManyColumnsAffectsBlockSize() throws Exception {
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFile(50, 0, 0, 777, 888)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                reader.resolveLastFooter();

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
            try (ParquetMetaTestFile file = buildFile(1, Long.MAX_VALUE)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                reader.resolveLastFooter();

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
                    ParquetMetaTestFile file1 = buildFile(1, 0, 0, 500, 600);
                    ParquetMetaTestFile file2 = buildFile(10, 0, 0, 500, 600)
            ) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();

                reader.of(file1.dataPtr, file1.parquetMetaFileSize);
                reader.resolveLastFooter();
                Assert.assertEquals(1, reader.getColumnCount());
                Assert.assertEquals(500, reader.getRowGroupSize(0));
                Assert.assertEquals(600, reader.getRowGroupSize(1));

                reader.of(file2.dataPtr, file2.parquetMetaFileSize);
                reader.resolveLastFooter();
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
            try (ParquetMetaTestFile file = buildFile(3, 0, 0, 100, 200, 500, 1_000_000)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                reader.resolveLastFooter();

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
            try (ParquetMetaTestFile file = buildFile(2, 55)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                reader.resolveLastFooter();
                reader.clear();
                Assert.assertFalse(reader.isOpen());

                reader.of(file.dataPtr, file.parquetMetaFileSize);
                reader.resolveLastFooter();
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
            try (ParquetMetaTestFile file = buildFile(2, 0, 0, 100, 200, 300)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                reader.resolveLastFooter();
                Assert.assertEquals(600, reader.getPartitionRowCount());
            }
        });
    }

    @Test
    public void testPartitionRowCountEmpty() throws Exception {
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFile(1)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                reader.resolveLastFooter();
                Assert.assertEquals(0, reader.getPartitionRowCount());
            }
        });
    }

    @Test
    public void testReadPartitionMetaDtsAtFirstColumn() throws Exception {
        // Designated timestamp column index at the lower boundary (0). This
        // is the boundary case for the dtsIndex < -1 || dtsIndex >= columnCount
        // guard in resolveFooter; it must not trip when DTS == 0 and
        // columnCount > 0.
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFileWithDts(0, 3, 12L)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                Assert.assertTrue(reader.resolveLastFooter());
                Assert.assertEquals(0, reader.getDesignatedTimestampColumnIndex());

                long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
                try {
                    reader.readPartitionMeta(buf);
                    Assert.assertEquals(12L, Unsafe.getLong(buf));
                    Assert.assertEquals(-1L, Unsafe.getLong(buf + 8));
                } finally {
                    Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
                }
                reader.clear();
            }
        });
    }

    @Test
    public void testReadPartitionMetaDtsAtLastColumn() throws Exception {
        // Designated timestamp at columnCount - 1 — the upper boundary the
        // resolveFooter dtsIndex guard accepts. Asserts the boundary does not
        // trip and readPartitionMeta still surfaces the row count.
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFileWithDts(2, 3, 7L)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                Assert.assertTrue(reader.resolveLastFooter());
                Assert.assertEquals(2, reader.getDesignatedTimestampColumnIndex());

                long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
                try {
                    reader.readPartitionMeta(buf);
                    Assert.assertEquals(7L, Unsafe.getLong(buf));
                    Assert.assertEquals(-1L, Unsafe.getLong(buf + 8));
                } finally {
                    Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
                }
                reader.clear();
            }
        });
    }

    @Test
    public void testReadPartitionMetaMultiRowGroup() throws Exception {
        // Total row count is the sum across every row group; verifies the
        // checked_add accumulator in read_partition_meta_impl on a multi-rg
        // _pm.
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFile(2, 3L, 7L, 2L)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                Assert.assertTrue(reader.resolveLastFooter());

                long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
                try {
                    reader.readPartitionMeta(buf);
                    Assert.assertEquals(12L, Unsafe.getLong(buf));
                    Assert.assertEquals(-1L, Unsafe.getLong(buf + 8));
                } finally {
                    Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
                }
                reader.clear();
            }
        });
    }

    @Test
    public void testReadPartitionMetaTruncatedFooter() throws Exception {
        // Truncate the published snapshot 4 bytes short of its real footer
        // start so the trailer-derived footer offset lands inside the column
        // descriptor region. The Rust-side parser inside readPartitionMeta
        // must reject this with a CairoException — the StoragePolicyJob
        // sidecar contract treats any such failure as "stale, will reconvert"
        // (StoragePolicyJob.java:675-681). A SIGSEGV instead would skip that
        // contract.
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFile(1, 100)) {
                long origSize = file.parquetMetaFileSize;
                long shrunkSize = origSize - 4;
                Assert.assertTrue("shrunk size below precondition", shrunkSize >= 36);
                Unsafe.putLong(file.dataPtr, shrunkSize);
                patchCrc(file.dataPtr, shrunkSize);

                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, shrunkSize);
                try {
                    reader.resolveLastFooter();
                    Assert.fail("expected CairoException");
                } catch (CairoException expected) {
                    Assert.assertTrue(
                            expected.getMessage(),
                            expected.getMessage().contains("invalid _pm")
                                    || expected.getMessage().contains("footer")
                                    || expected.getMessage().contains("checksum")
                                    || expected.getMessage().contains("checkSum")
                    );
                }
                reader.clear();
            }
        });
    }

    @Test
    public void testReadPartitionMetaWellFormedFooter() throws Exception {
        // Verifies row_count is the sum of row group sizes and squash_tracker
        // is -1 when the SQUASH_TRACKER feature section is absent (the case
        // for every _pm produced by the standard writer path).
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFile(2, 5L)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                Assert.assertTrue(reader.resolveLastFooter());

                long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
                try {
                    reader.readPartitionMeta(buf);
                    Assert.assertEquals(5L, Unsafe.getLong(buf));
                    Assert.assertEquals(-1L, Unsafe.getLong(buf + 8));
                } finally {
                    Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
                }
                reader.clear();
            }
        });
    }

    @Test
    public void testReadPartitionMetaZeroColumns() throws Exception {
        // A _pm with columnCount == 0 still has to expose a usable row count
        // through readPartitionMeta. The reader's column descriptor loop and
        // headerEndOffset arithmetic both have to handle the zero case
        // without underflow. The writer requires row groups to declare at
        // least one column, so the only column-zero file the writer can
        // produce is also row-group-zero — readPartitionMeta must still
        // report row_count == 0 cleanly.
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFile(0)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                Assert.assertTrue(reader.resolveLastFooter());
                Assert.assertEquals(0, reader.getColumnCount());
                Assert.assertEquals(0, reader.getRowGroupCount());

                long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
                try {
                    reader.readPartitionMeta(buf);
                    Assert.assertEquals(0L, Unsafe.getLong(buf));
                    Assert.assertEquals(-1L, Unsafe.getLong(buf + 8));
                } finally {
                    Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
                }
                reader.clear();
            }
        });
    }

    @Test
    public void testReadPartitionMetaZeroRowGroups() throws Exception {
        // A _pm with no row groups must report row_count == 0; the JNI
        // accumulator loop should be a no-op rather than reading past the
        // (empty) row group table.
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFile(2)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                Assert.assertTrue(reader.resolveLastFooter());
                Assert.assertEquals(0, reader.getRowGroupCount());

                long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
                try {
                    reader.readPartitionMeta(buf);
                    Assert.assertEquals(0L, Unsafe.getLong(buf));
                    Assert.assertEquals(-1L, Unsafe.getLong(buf + 8));
                } finally {
                    Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
                }
                reader.clear();
            }
        });
    }

    @Test
    public void testReopenWithOf() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    ParquetMetaTestFile file1 = buildFile(1, 0, 0, 42);
                    ParquetMetaTestFile file2 = buildFile(2, 0, 0, 99, 101)
            ) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();

                reader.of(file1.dataPtr, file1.parquetMetaFileSize);
                reader.resolveLastFooter();
                Assert.assertEquals(1, reader.getRowGroupCount());
                Assert.assertEquals(42, reader.getRowGroupSize(0));

                reader.of(file2.dataPtr, file2.parquetMetaFileSize);
                reader.resolveLastFooter();
                Assert.assertEquals(2, reader.getRowGroupCount());
                Assert.assertEquals(99, reader.getRowGroupSize(0));
                Assert.assertEquals(101, reader.getRowGroupSize(1));
            }
        });
    }

    @Test
    public void testSelfReferentialPrevSize() throws Exception {
        // Single-snapshot _pm whose footer claims its own committed size as
        // prev_parquet_meta_file_size. The strict-monotone guard in
        // resolveFooter must reject the chain step instead of looping back to
        // the same footer (which would walk forever or read identical bytes).
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFile(1, 100, 50, 1000)) {
                long len = file.dataLen;
                int footerLength = Unsafe.getInt(file.dataPtr + len - 4);
                long footerOffset = len - 4 - Integer.toUnsignedLong(footerLength);

                long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.copyMemory(file.dataPtr, buf, len);
                    // FOOTER_PREV_PARQUET_META_FILE_SIZE_OFF == 24 from
                    // footer start.
                    Unsafe.putLong(buf + footerOffset + 24, len);
                    patchCrc(buf, len);

                    ParquetMetaFileReader reader = new ParquetMetaFileReader();
                    reader.of(buf, len);
                    // Latest footer's parquet size is 158; request a different
                    // size so the chain walk has to step. The self-referential
                    // prev forces resolveFooter to either reject (false) or
                    // throw — anything but loop or SIGBUS.
                    Assert.assertFalse(reader.resolveFooter(9999L));
                } finally {
                    Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testSingleRowGroup() throws Exception {
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFile(2, 1000)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                reader.resolveLastFooter();

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
            try (ParquetMetaTestFile file = buildFile(1, 1)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                reader.resolveLastFooter();

                Assert.assertEquals(1, reader.getRowGroupSize(0));
            }
        });
    }

    @Test
    public void testStaleParquetMetadataReturnsFalse() throws Exception {
        // resolveFooter signals MVCC miss by returning false (not throwing):
        // when no footer in the chain matches the requested parquet file
        // size, the walk reaches the chain root and returns false so the
        // caller can treat it as a stale-snapshot retry signal rather than
        // a corruption error.
        assertMemoryLeak(() -> {
            // Build a _pm with derived parquet size = 100 + 50 + 8 = 158.
            try (ParquetMetaTestFile file = buildFile(2, 100, 50, 1000)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                // Request parquet size 9999 — no footer matches.
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                Assert.assertFalse(reader.resolveFooter(9999));
            }
        });
    }

    @Test
    public void testUnknownRequiredFeatureFlagRejected() throws Exception {
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFile(1, 100)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                try {
                    reader.of(file.dataPtr, file.parquetMetaFileSize);
                    Assert.assertTrue(reader.resolveLastFooter());

                    // Prime native verification on valid bytes, then set required bit 32.
                    // The second resolve reuses the cached native reader and must reach
                    // the Java required-feature guard.
                    long originalFlags = Unsafe.getLong(file.dataPtr + 8);
                    Unsafe.putLong(file.dataPtr + 8, originalFlags | (1L << 32));
                    reader.resolveLastFooter();
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getMessage(), "unsupported required _pm feature flags [flags=0x100000000]");
                } finally {
                    reader.clear();
                }
            }
        });
    }

    @Test
    public void testUnknownRequiredFooterFeatureFlagRejected() throws Exception {
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFile(1, 100)) {
                int footerLength = Unsafe.getInt(file.dataPtr + file.parquetMetaFileSize - 4);
                long footerOffset = file.parquetMetaFileSize - 4 - Integer.toUnsignedLong(footerLength);
                long footerFlagsAddr = file.dataPtr + footerOffset + 32;
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                try {
                    reader.of(file.dataPtr, file.parquetMetaFileSize);
                    Assert.assertTrue(reader.resolveLastFooter());

                    // Prime native verification on valid bytes, then set required bit 32.
                    // The second resolve reuses the cached native reader and must reach
                    // the Java required-footer-feature guard.
                    long originalFlags = Unsafe.getLong(footerFlagsAddr);
                    Unsafe.putLong(footerFlagsAddr, originalFlags | (1L << 32));
                    reader.resolveLastFooter();
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getMessage(), "unsupported required _pm footer feature flags [flags=0x100000000]");
                } finally {
                    reader.clear();
                }
            }
        });
    }

    @Test
    public void testZeroRowGroups() throws Exception {
        assertMemoryLeak(() -> {
            try (ParquetMetaTestFile file = buildFile(1)) {
                ParquetMetaFileReader reader = new ParquetMetaFileReader();
                reader.of(file.dataPtr, file.parquetMetaFileSize);
                reader.resolveLastFooter();

                Assert.assertEquals(0, reader.getRowGroupCount());
            }
        });
    }

    /**
     * Builds a _pm file with the given column count and row group sizes
     * using the real Rust writer via JNI. Returns a handle that must
     * be freed with {@link ParquetMetaTestFile#close()}.
     */
    private static ParquetMetaTestFile buildFile(int columnCount, long... rowGroupSizes) {
        return buildFile(columnCount, 0, 0, rowGroupSizes);
    }

    private static ParquetMetaTestFile buildFile(int columnCount, long parquetFooterOff, int parquetFooterLen, long... rowGroupSizes) {
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
            return new ParquetMetaTestFile(resultPtr);
        } finally {
            ParquetMetaFileWriter.destroyWriter(writerPtr);
        }
    }

    private static ParquetMetaTestFile buildFile(int[] columnIds, long... rowGroupSizes) {
        long writerPtr = ParquetMetaFileWriter.create();
        try {
            ParquetMetaFileWriter.setDesignatedTimestamp(writerPtr, -1);
            for (int i = 0; i < columnIds.length; i++) {
                try (DirectUtf8Sink name = new DirectUtf8Sink(16)) {
                    name.put("col_").put(i);
                    ParquetMetaFileWriter.addColumn(
                            writerPtr,
                            name.ptr(),
                            (int) name.size(),
                            columnIds[i],
                            5,
                            0,
                            0,
                            0,
                            0,
                            0
                    );
                }
            }
            for (long numRows : rowGroupSizes) {
                ParquetMetaFileWriter.addRowGroup(writerPtr, numRows);
            }
            ParquetMetaFileWriter.setParquetFooter(writerPtr, 0, 0);
            long resultPtr = ParquetMetaFileWriter.finish(writerPtr);
            return new ParquetMetaTestFile(resultPtr);
        } finally {
            ParquetMetaFileWriter.destroyWriter(writerPtr);
        }
    }

    private static ParquetMetaTestFile buildFileWithDts(int dtsIndex, int columnCount, long... rowGroupSizes) {
        long writerPtr = ParquetMetaFileWriter.create();
        try {
            ParquetMetaFileWriter.setDesignatedTimestamp(writerPtr, dtsIndex);
            for (int i = 0; i < columnCount; i++) {
                try (DirectUtf8Sink name = new DirectUtf8Sink(16)) {
                    name.put("col_").put(i);
                    ParquetMetaFileWriter.addColumn(writerPtr, name.ptr(), name.size(), i, 5, 0, 0, 0, 0, 0);
                }
            }
            for (long numRows : rowGroupSizes) {
                ParquetMetaFileWriter.addRowGroup(writerPtr, numRows);
            }
            ParquetMetaFileWriter.setParquetFooter(writerPtr, 0, 0);
            long resultPtr = ParquetMetaFileWriter.finish(writerPtr);
            return new ParquetMetaTestFile(resultPtr);
        } finally {
            ParquetMetaFileWriter.destroyWriter(writerPtr);
        }
    }

    private static ParquetMetaTestFile buildFileWithBloomFilter(int columnCount, long... rowGroupSizes) {
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
                        Unsafe.putByte(bitsetAddr + b, (byte) 0xFF);
                    }
                    ParquetMetaFileWriter.addBloomFilter(writerPtr, 0, bitsetAddr, 32);
                } finally {
                    Unsafe.free(bitsetAddr, 32, MemoryTag.NATIVE_DEFAULT);
                }
            }
            ParquetMetaFileWriter.setParquetFooter(writerPtr, 0, 0);
            long resultPtr = ParquetMetaFileWriter.finish(writerPtr);
            return new ParquetMetaTestFile(resultPtr);
        } finally {
            ParquetMetaFileWriter.destroyWriter(writerPtr);
        }
    }

    private static final class CountingColumnIdReader extends ParquetMetaFileReader {
        private int columnIdReadCount;

        @Override
        public int getColumnId(int columnIndex) {
            columnIdReadCount++;
            return super.getColumnId(columnIndex);
        }

        private int getColumnIdReadCount() {
            return columnIdReadCount;
        }
    }

    /**
     * Recomputes and patches the CRC32 in a {@code _pm} snapshot whose footer
     * ends at byte offset {@code snapshotEnd} from {@code addr}.
     * <p>
     * The CRC field lives at {@code [snapshotEnd - 8, snapshotEnd - 4)} and the
     * trailer at {@code [snapshotEnd - 4, snapshotEnd)}. The CRC covers
     * {@code [HEADER_CRC_AREA_OFF=8, snapshotEnd - 8)} -- everything after the
     * mutable {@code parquet_meta_file_size} field at offset 0.
     * <p>
     * Tests that hand-build {@code _pm} bytes (or corrupt fields inside the CRC
     * region) call this so the reader's up-front checksum step
     * accepts the file and the test's specific structural validation can fire.
     */
    private static void patchCrc(long addr, long snapshotEnd) {
        long crcStart = addr + 8;
        long crcFieldOff = addr + snapshotEnd - 8;
        int len = (int) (crcFieldOff - crcStart);
        byte[] buf = new byte[len];
        for (int i = 0; i < len; i++) {
            buf[i] = Unsafe.getByte(crcStart + i);
        }
        CRC32 crc = new CRC32();
        crc.update(buf);
        Unsafe.putInt(crcFieldOff, (int) crc.getValue());
    }

    private static class ParquetMetaTestFile implements AutoCloseable {
        final long dataLen;
        final long dataPtr;
        final long parquetMetaFileSize;
        final long resultPtr;

        ParquetMetaTestFile(long resultPtr) {
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
