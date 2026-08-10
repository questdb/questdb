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
import io.questdb.std.Vect;
import io.questdb.std.Zip;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Cross-implementation pin for the {@code _im} format, version 2. Every
 * fixture is built by the real Rust writer through JNI and read back with the
 * Java reader, so a layout change that touches only one side fails here.
 * <p>
 * The fixtures deliberately duplicate the ones in
 * {@code qdb-parquet-meta/src/index_meta.rs}, and the absolute byte offsets
 * asserted below are the offsets that file's tests pin. A layout change
 * applied to both readers at once would keep the round trip green; the
 * absolute offsets are what fails.
 */
public class IndexMetaFileReaderTest extends AbstractCairoTest {

    private static final int CODEC_SNAPPY = 1;
    private static final int CODEC_ZSTD = 6;
    private static final int ENC_BYTE_STREAM_SPLIT = 1 << 5;
    private static final int ENC_DELTA_BINARY_PACKED = 1 << 2;
    private static final int ENC_PLAIN = 1;
    private static final int ENC_RLE_DICTIONARY = 1 << 1;
    private static final int STAT_DISTINCT_COUNT_PRESENT = 1 << 6;
    private static final int STAT_MAX_EXACT = 1 << 5;
    private static final int STAT_MAX_INLINED = 1 << 4;
    private static final int STAT_MAX_PRESENT = 1 << 3;
    private static final int STAT_MIN_EXACT = 1 << 2;
    private static final int STAT_MIN_INLINED = 1 << 1;
    private static final int STAT_MIN_PRESENT = 1;
    private static final int STAT_NULL_COUNT_PRESENT = 1 << 7;
    // QuestDB column type tags, spelled out so the fixtures do not depend on
    // ColumnType's ordering, exactly as the Rust fixtures do.
    private static final int TYPE_DOUBLE = 10;
    private static final int TYPE_INT = 5;
    private static final int TYPE_LONG = 6;
    private static final int TYPE_UUID = 19;

    /**
     * The complementary alignment case to
     * {@link #testAbsoluteByteLayoutWithPaddedNameSection()}: the names total
     * 16 bytes so the name section adds no padding, and the odd row group
     * count moves the padding from RG_FIRST_KEY to RG_BLOCK_OFFSET. Version 1
     * only ever exercised one of the two, which is how an alignment bug could
     * hide.
     */
    @Test
    public void testAbsoluteByteLayoutWithAlignedNameSection() throws Exception {
        assertMemoryLeak(() -> withAlignedSample(reader -> {
            final long addr = reader.getAddr();
            Assert.assertEquals(836, reader.getFileSize());
            Assert.assertEquals(3, Unsafe.getUnsafe().getInt(addr + 36)); // INDEX_RG_COUNT
            Assert.assertEquals(776, Unsafe.getUnsafe().getLong(addr + 56)); // INDEX_SECTIONS_OFFSET
            Assert.assertEquals(776, reader.getIndexSectionsOffset());

            // Names: 16 bytes at 160..176, no padding.
            TestUtils.assertEquals("pxpx", reader.getColumnName(2));
            Assert.assertEquals(160, Unsafe.getUnsafe().getLong(addr + 64));
            Assert.assertEquals(172, Unsafe.getUnsafe().getLong(addr + 128));

            // Blocks start immediately at 176.
            Assert.assertEquals(100, Unsafe.getUnsafe().getLong(addr + 176));
            Assert.assertEquals(100, Unsafe.getUnsafe().getLong(addr + 376));
            Assert.assertEquals(100, Unsafe.getUnsafe().getLong(addr + 576));

            // RG_BLOCK_OFFSET at 776: 3 entries (12 bytes) then 4 bytes of padding.
            Assert.assertEquals(176 >> 3, Unsafe.getUnsafe().getInt(addr + 776));
            Assert.assertEquals(376 >> 3, Unsafe.getUnsafe().getInt(addr + 780));
            Assert.assertEquals(576 >> 3, Unsafe.getUnsafe().getInt(addr + 784));
            Assert.assertEquals(0, Unsafe.getUnsafe().getInt(addr + 788));

            // RG_FIRST_KEY at 792: 4 entries (16 bytes), already aligned.
            Assert.assertEquals(0, Unsafe.getUnsafe().getInt(addr + 792));
            Assert.assertEquals(300, Unsafe.getUnsafe().getInt(addr + 796));
            Assert.assertEquals(700, Unsafe.getUnsafe().getInt(addr + 800));
            Assert.assertEquals(900, Unsafe.getUnsafe().getInt(addr + 804)); // sentinel

            // DATA_RG_BOUNDARY at 808, CRC at 832.
            Assert.assertEquals(0, Unsafe.getUnsafe().getLong(addr + 808));
            Assert.assertEquals(300, Unsafe.getUnsafe().getLong(addr + 824));

            // The reader resolves the same sections it was pinned against.
            Assert.assertEquals(100, reader.getRowGroupNumRows(2));
            Assert.assertEquals(700, reader.getRowGroupFirstKey(2));
            Assert.assertEquals(900, reader.getRowGroupFirstKey(3));
            Assert.assertEquals(2, reader.getRowGroupLoForKey(700));
            Assert.assertEquals(2, reader.getRowGroupHiForKey(700));
            Assert.assertEquals(300, reader.getDataRowGroupBoundary(2));
        }));
    }

    /**
     * Pins every section's absolute offset so an edit cannot shift one
     * undetected. The names total 17 bytes (padded to 24) and the row group
     * count is even, so RG_BLOCK_OFFSET lands 8-aligned and RG_FIRST_KEY is
     * padded.
     * <pre>
     * 0    header (64 bytes)
     * 64   column descriptors, 3 x 32 bytes
     * 160  name strings, 17 bytes, padded to 184
     * 184  row group blocks, 4 x (8 + 3 * 64) bytes
     * 984  RG_BLOCK_OFFSET, 4 x 4 bytes, already aligned
     * 1000 RG_FIRST_KEY, 5 x 4 bytes, padded to 1024
     * 1024 DATA_RG_BOUNDARY, 3 x 8 bytes
     * 1048 CRC32
     * 1052 total
     * </pre>
     */
    @Test
    public void testAbsoluteByteLayoutWithPaddedNameSection() throws Exception {
        assertMemoryLeak(() -> withSample(reader -> {
            final long addr = reader.getAddr();
            Assert.assertEquals(1_052, reader.getFileSize());
            Assert.assertEquals(1_052, Unsafe.getUnsafe().getLong(addr)); // IM_FILE_SIZE
            Assert.assertEquals(0x0200_5844_4942_4451L, Unsafe.getUnsafe().getLong(addr + 8)); // IM_MAGIC
            Assert.assertEquals(0, Unsafe.getUnsafe().getLong(addr + 16)); // FEATURE_FLAGS
            Assert.assertEquals(2, Unsafe.getUnsafe().getInt(addr + 24)); // FORMAT_VERSION
            Assert.assertEquals(0, Unsafe.getUnsafe().getInt(addr + 28)); // PAYLOAD_KIND
            Assert.assertEquals(3, Unsafe.getUnsafe().getInt(addr + 32)); // COLUMN_COUNT
            Assert.assertEquals(4, Unsafe.getUnsafe().getInt(addr + 36)); // INDEX_RG_COUNT
            Assert.assertEquals(2, Unsafe.getUnsafe().getInt(addr + 40)); // DATA_RG_COUNT
            Assert.assertEquals(11_405, Unsafe.getUnsafe().getInt(addr + 44)); // KEY_COUNT
            Assert.assertEquals(0, Unsafe.getUnsafe().getInt(addr + 48)); // KEY_ID_COLUMN
            Assert.assertEquals(1, Unsafe.getUnsafe().getInt(addr + 52)); // ROW_ID_COLUMN
            Assert.assertEquals(984, Unsafe.getUnsafe().getLong(addr + 56)); // INDEX_SECTIONS_OFFSET
            Assert.assertEquals(984, reader.getIndexSectionsOffset());

            // Descriptors: 64 + 3 * 32 = 160.
            Assert.assertEquals(160, Unsafe.getUnsafe().getLong(addr + 64)); // col 0 name offset
            Assert.assertEquals(6, Unsafe.getUnsafe().getInt(addr + 88)); // col 0 name length
            Assert.assertEquals(166, Unsafe.getUnsafe().getLong(addr + 96)); // col 1 name offset
            Assert.assertEquals(172, Unsafe.getUnsafe().getLong(addr + 128)); // col 2 name offset
            Assert.assertEquals(5, Unsafe.getUnsafe().getInt(addr + 152)); // col 2 name length

            // Names: 160..177, then 7 bytes of padding to 184.
            for (long i = 177; i < 184; i++) {
                Assert.assertEquals(0, Unsafe.getByte(addr + i));
            }

            // Blocks: 8 + 3 * 64 = 200 bytes each, from 184.
            Assert.assertEquals(100_000, Unsafe.getUnsafe().getLong(addr + 184)); // block 0 NUM_ROWS
            Assert.assertEquals(58_000, Unsafe.getUnsafe().getLong(addr + 384)); // block 1 NUM_ROWS
            Assert.assertEquals(82_001, Unsafe.getUnsafe().getLong(addr + 584)); // block 2 NUM_ROWS
            Assert.assertEquals(759_999, Unsafe.getUnsafe().getLong(addr + 784)); // block 3 NUM_ROWS
            // Block 3, column 2 (price): NUM_ROWS + 2 chunks + the 8-byte prefix.
            Assert.assertEquals(759_999, Unsafe.getUnsafe().getLong(addr + 784 + 8 + 2 * 64 + 8));
            Assert.assertEquals(7_096, Unsafe.getUnsafe().getLong(addr + 784 + 8 + 2 * 64 + 16));

            // RG_BLOCK_OFFSET at 984: 4 entries, no padding needed afterwards.
            Assert.assertEquals(184 >> 3, Unsafe.getUnsafe().getInt(addr + 984));
            Assert.assertEquals(384 >> 3, Unsafe.getUnsafe().getInt(addr + 988));
            Assert.assertEquals(584 >> 3, Unsafe.getUnsafe().getInt(addr + 992));
            Assert.assertEquals(784 >> 3, Unsafe.getUnsafe().getInt(addr + 996));

            // RG_FIRST_KEY at 1000: 5 entries (20 bytes) then 4 bytes of padding.
            Assert.assertEquals(0, Unsafe.getUnsafe().getInt(addr + 1_000));
            Assert.assertEquals(11_403, Unsafe.getUnsafe().getInt(addr + 1_004));
            Assert.assertEquals(11_403, Unsafe.getUnsafe().getInt(addr + 1_008));
            Assert.assertEquals(11_404, Unsafe.getUnsafe().getInt(addr + 1_012));
            Assert.assertEquals(11_405, Unsafe.getUnsafe().getInt(addr + 1_016)); // sentinel
            Assert.assertEquals(0, Unsafe.getUnsafe().getInt(addr + 1_020));

            // DATA_RG_BOUNDARY at 1024, CRC at 1048.
            Assert.assertEquals(0, Unsafe.getUnsafe().getLong(addr + 1_024));
            Assert.assertEquals(500_000, Unsafe.getUnsafe().getLong(addr + 1_032));
            Assert.assertEquals(1_000_000, Unsafe.getUnsafe().getLong(addr + 1_040));
            Assert.assertEquals(Zip.crc32(0, addr + 8, 1_040), Unsafe.getUnsafe().getInt(addr + 1_048));

            // The reader must land on the same bytes it was pinned against.
            Assert.assertEquals(759_999, reader.getRowGroupNumRows(3));
            Assert.assertEquals(7_096, reader.getChunkByteRangeStart(3, 2));
            Assert.assertEquals(11_405, reader.getRowGroupFirstKey(4));
            Assert.assertEquals(1_000_000, reader.getDataRowGroupBoundary(2));
        }));
    }

    /**
     * A required cover column is resolved to a parquet column index through
     * the descriptor ID, which carries the covered column's QuestDB writer
     * index. A writer index no column carries must miss.
     */
    @Test
    public void testColumnLookupByWriterIndex() throws Exception {
        assertMemoryLeak(() -> withSample(reader -> {
            Assert.assertEquals(2, reader.getColumnIndexById(7));
            Assert.assertEquals(-1, reader.getColumnIndexById(99));
            Assert.assertEquals(-1, reader.getColumnIndexById(0));
            Assert.assertEquals(-1, reader.getColumnIndexById(Integer.MAX_VALUE));
            // The synthetic columns carry -1 and are located through the header.
            Assert.assertEquals(0, reader.getKeyIdColumn());
            Assert.assertEquals(1, reader.getRowIdColumn());
        }));
    }

    /**
     * A key below the first row group's first key is absent even though it is
     * below KEY_COUNT: no row group can hold it.
     */
    @Test
    public void testKeyBelowFirstEntryIsAbsent() throws Exception {
        assertMemoryLeak(() -> withReader(IndexMetaFileReaderTest::buildBelowFirstKeySample, reader -> {
            Assert.assertEquals(-1, reader.getRowGroupLoForKey(0));
            Assert.assertEquals(-1, reader.getRowGroupHiForKey(0));
            Assert.assertEquals(-1, reader.getRowGroupLoForKey(4));
            Assert.assertEquals(0, reader.getRowGroupLoForKey(5));
            Assert.assertEquals(0, reader.getRowGroupHiForKey(5));
            Assert.assertEquals(0, reader.getRowGroupLoForKey(7));
            Assert.assertEquals(0, reader.getRowGroupHiForKey(7));
            Assert.assertEquals(1, reader.getRowGroupLoForKey(9));
            Assert.assertEquals(1, reader.getRowGroupHiForKey(9));
            Assert.assertEquals(1, reader.getRowGroupLoForKey(50));
            Assert.assertEquals(1, reader.getRowGroupHiForKey(50));
        }));
    }

    /**
     * The worked example from the specification's "Key lookup" section:
     * {@code RG_FIRST_KEY = [0, 11_403, 11_403, 11_404, KEY_COUNT]}.
     */
    @Test
    public void testKeyLookupWorkedExample() throws Exception {
        assertMemoryLeak(() -> withSample(reader -> {
            // Exact match at index 0.
            Assert.assertEquals(0, reader.getRowGroupLoForKey(0));
            Assert.assertEquals(0, reader.getRowGroupHiForKey(0));
            // No exact match; packed inside row group 0.
            Assert.assertEquals(0, reader.getRowGroupLoForKey(5));
            Assert.assertEquals(0, reader.getRowGroupHiForKey(5));
            // Exact match; spans two dedicated row groups.
            Assert.assertEquals(1, reader.getRowGroupLoForKey(11_403));
            Assert.assertEquals(2, reader.getRowGroupHiForKey(11_403));
            // Exact match at index 3.
            Assert.assertEquals(3, reader.getRowGroupLoForKey(11_404));
            Assert.assertEquals(3, reader.getRowGroupHiForKey(11_404));
            // KEY_COUNT and above are absent. The comparison is unsigned, so
            // -1 read as a u32 is above KEY_COUNT rather than below zero.
            Assert.assertEquals(-1, reader.getRowGroupLoForKey(11_405));
            Assert.assertEquals(-1, reader.getRowGroupHiForKey(11_405));
            Assert.assertEquals(-1, reader.getRowGroupLoForKey(-1));
            Assert.assertEquals(-1, reader.getRowGroupHiForKey(-1));
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
                // Offset 184 is the first row group block, inside the CRC area.
                flipByte(ff, path.$(), 184);
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
                    Assert.assertEquals(1_052, reader.getFileSize());
                    Assert.assertEquals(984, reader.getIndexSectionsOffset());
                    Assert.assertEquals(0, reader.getPayloadKind());
                    Assert.assertEquals(3, reader.getColumnCount());
                    Assert.assertEquals(11_405, reader.getKeyCount());
                    Assert.assertEquals(4, reader.getIndexRowGroupCount());
                    Assert.assertEquals(2, reader.getDataRowGroupCount());
                    TestUtils.assertEquals("price", reader.getColumnName(2));
                    Assert.assertEquals(2, reader.getColumnIndexById(7));
                    Assert.assertEquals(1, reader.getRowGroupLoForKey(11_403));
                    Assert.assertEquals(2, reader.getRowGroupHiForKey(11_403));
                    Assert.assertEquals(82_001, reader.getRowGroupNumRows(2));
                    Assert.assertEquals(158_000, reader.getChunkMinStat(2, 1));
                    Assert.assertEquals(240_000, reader.getChunkMaxStat(2, 1));
                    Assert.assertEquals(6_096, reader.getChunkByteRangeStart(2, 2));
                    Assert.assertEquals(1_000_000, reader.getDataRowGroupBoundary(2));
                }
            }
        }));
    }

    /**
     * A file whose committed IM_FILE_SIZE exceeds its length is corruption:
     * mapping IM_FILE_SIZE bytes and reading the trailer would run past EOF
     * and SIGBUS the JVM, so the length check has to precede the mapping.
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
     * A {@code _pm} file carries FEATURE_FLAGS where {@code _im} carries the
     * magic, so the magic is what keeps one from being read as the other.
     */
    @Test
    public void testOpenAndMapROWrongMagic() throws Exception {
        assertMemoryLeak(() -> withPatchedSample("wrong-magic._im", 8, 0L, Long.BYTES, path -> {
            final FilesFacade ff = configuration.getFilesFacade();
            try (IndexMetaFileReader reader = new IndexMetaFileReader()) {
                IndexMetaFileReader.openAndMapRO(ff, path, reader);
                Assert.fail("expected CairoException from the magic check");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "bad _im IM_MAGIC");
            }
        }));
    }

    /**
     * Version 1 was the interim layout that carried no column descriptors and
     * no column chunks. It is not readable by this format.
     */
    @Test
    public void testOpenAndMapROWrongVersion() throws Exception {
        assertMemoryLeak(() -> withPatchedSample("wrong-version._im", 24, 1L, Integer.BYTES, path -> {
            final FilesFacade ff = configuration.getFilesFacade();
            try (IndexMetaFileReader reader = new IndexMetaFileReader()) {
                IndexMetaFileReader.openAndMapRO(ff, path, reader);
                Assert.fail("expected CairoException from the version check");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "unsupported _im FORMAT_VERSION");
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

    /**
     * A covered UUID column's min and max exceed the 8 inline bytes, so they
     * go to the block's out-of-line region as {@code (offset << 16) | length}.
     */
    @Test
    public void testOutOfLineStatsForWideCoveredColumn() throws Exception {
        assertMemoryLeak(() -> withReader(IndexMetaFileReaderTest::buildOutOfLineStatSample, reader -> {
            // Header 64, descriptors 96, names "key_idrow_iduid" padded 15 -> 16,
            // one block of 8 + 3 * 64 plus 32 out-of-line bytes, then the index
            // sections: RG_BLOCK_OFFSET 4 padded to 8, RG_FIRST_KEY 8,
            // DATA_RG_BOUNDARY 16, CRC 4.
            Assert.assertEquals(444, reader.getFileSize());
            Assert.assertEquals(2, reader.getColumnIndexById(4));

            Assert.assertTrue(reader.hasChunkMinStat(0, 2));
            Assert.assertTrue(reader.hasChunkMaxStat(0, 2));
            Assert.assertFalse(reader.isChunkMinStatInline(0, 2));
            Assert.assertFalse(reader.isChunkMaxStatInline(0, 2));
            Assert.assertEquals(16, reader.getChunkMinStatLength(0, 2));
            Assert.assertEquals(16, reader.getChunkMaxStatLength(0, 2));
            final long minAddr = reader.getChunkMinStatAddr(0, 2);
            final long maxAddr = reader.getChunkMaxStatAddr(0, 2);
            for (int i = 0; i < 16; i++) {
                Assert.assertEquals((byte) 0x11, Unsafe.getByte(minAddr + i));
                Assert.assertEquals((byte) 0xEE, Unsafe.getByte(maxAddr + i));
            }

            // The synthetic columns keep their inline stats in the same block.
            Assert.assertTrue(reader.isChunkMinStatInline(0, 0));
            Assert.assertEquals(7, reader.getChunkMinStat(0, 0));
            Assert.assertEquals(63, reader.getChunkMaxStat(0, 1));
        }));
    }

    @Test
    public void testRoundTripColumnChunks() throws Exception {
        assertMemoryLeak(() -> withSample(reader -> {
            Assert.assertEquals(82_001, reader.getRowGroupNumRows(2));

            Assert.assertEquals(CODEC_ZSTD, reader.getChunkCodec(2, 0));
            Assert.assertEquals(ENC_RLE_DICTIONARY, reader.getChunkEncodings(2, 0));
            Assert.assertEquals(4, reader.getChunkMinStatSize(2, 0));
            Assert.assertEquals(4, reader.getChunkMaxStatSize(2, 0));
            Assert.assertEquals(11_403, reader.getChunkMinStat(2, 0));
            Assert.assertEquals(11_403, reader.getChunkMaxStat(2, 0));

            Assert.assertEquals(ENC_DELTA_BINARY_PACKED, reader.getChunkEncodings(2, 1));
            Assert.assertEquals(158_000, reader.getChunkMinStat(2, 1));
            Assert.assertEquals(240_000, reader.getChunkMaxStat(2, 1));

            Assert.assertEquals(CODEC_SNAPPY, reader.getChunkCodec(2, 2));
            Assert.assertEquals(ENC_PLAIN | ENC_BYTE_STREAM_SPLIT, reader.getChunkEncodings(2, 2));
            final int flags = reader.getChunkStatFlags(2, 2);
            Assert.assertEquals(
                    STAT_MIN_PRESENT | STAT_MIN_INLINED | STAT_MIN_EXACT
                            | STAT_MAX_PRESENT | STAT_MAX_INLINED
                            | STAT_NULL_COUNT_PRESENT | STAT_DISTINCT_COUNT_PRESENT,
                    flags
            );
            Assert.assertEquals(0, flags & STAT_MAX_EXACT);
            Assert.assertTrue(reader.hasChunkMinStat(2, 2));
            Assert.assertTrue(reader.isChunkMinStatInline(2, 2));
            Assert.assertEquals(8, reader.getChunkMinStatSize(2, 2));
            Assert.assertEquals(8, reader.getChunkMaxStatSize(2, 2));
            Assert.assertEquals(82_001, reader.getChunkNumValues(2, 2));
            Assert.assertEquals(6_096, reader.getChunkByteRangeStart(2, 2));
            Assert.assertEquals(514, reader.getChunkTotalCompressed(2, 2));
            Assert.assertEquals(2, reader.getChunkNullCount(2, 2));
            Assert.assertEquals(42, reader.getChunkDistinctCount(2, 2));
            Assert.assertEquals(102, reader.getChunkMinStat(2, 2));
            Assert.assertEquals(902, reader.getChunkMaxStat(2, 2));
        }));
    }

    @Test
    public void testRoundTripDescriptorsAndNames() throws Exception {
        assertMemoryLeak(() -> withSample(reader -> {
            Assert.assertEquals(3, reader.getColumnCount());

            Assert.assertEquals(-1, reader.getColumnId(0));
            Assert.assertEquals(TYPE_INT, reader.getColumnType(0));
            Assert.assertEquals(1, reader.getColumnPhysicalType(0));
            Assert.assertEquals(0, reader.getColumnFlags(0));
            TestUtils.assertEquals("key_id", reader.getColumnName(0));

            Assert.assertEquals(-1, reader.getColumnId(1));
            Assert.assertEquals(TYPE_LONG, reader.getColumnType(1));
            TestUtils.assertEquals("row_id", reader.getColumnName(1));

            Assert.assertEquals(7, reader.getColumnId(2));
            Assert.assertEquals(TYPE_DOUBLE, reader.getColumnType(2));
            TestUtils.assertEquals("price", reader.getColumnName(2));
        }));
    }

    @Test
    public void testRoundTripHeaderFields() throws Exception {
        assertMemoryLeak(() -> withSample(reader -> {
            Assert.assertEquals(1_052, reader.getFileSize());
            Assert.assertEquals(0, reader.getFeatureFlags());
            Assert.assertEquals(IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, reader.getPayloadKind());
            Assert.assertEquals(3, reader.getColumnCount());
            Assert.assertEquals(4, reader.getIndexRowGroupCount());
            Assert.assertEquals(2, reader.getDataRowGroupCount());
            Assert.assertEquals(11_405, reader.getKeyCount());
            Assert.assertEquals(0, reader.getKeyIdColumn());
            Assert.assertEquals(1, reader.getRowIdColumn());
            Assert.assertEquals(984, reader.getIndexSectionsOffset());
        }));
    }

    @Test
    public void testRoundTripIndexSections() throws Exception {
        assertMemoryLeak(() -> withSample(reader -> {
            // RG_BLOCK_OFFSET resolves each block, and every block's key id
            // chunk agrees with the key directory it duplicates.
            for (int i = 0, n = reader.getIndexRowGroupCount(); i < n; i++) {
                Assert.assertEquals(reader.getRowGroupFirstKey(i), reader.getChunkMinStat(i, 0));
            }
            Assert.assertEquals(0, reader.getRowGroupFirstKey(0));
            Assert.assertEquals(11_403, reader.getRowGroupFirstKey(1));
            Assert.assertEquals(11_403, reader.getRowGroupFirstKey(2));
            Assert.assertEquals(11_404, reader.getRowGroupFirstKey(3));
            // The sentinel is KEY_COUNT.
            Assert.assertEquals(11_405, reader.getRowGroupFirstKey(4));

            Assert.assertEquals(100_000, reader.getRowGroupNumRows(0));
            Assert.assertEquals(58_000, reader.getRowGroupNumRows(1));
            Assert.assertEquals(82_001, reader.getRowGroupNumRows(2));
            Assert.assertEquals(759_999, reader.getRowGroupNumRows(3));

            Assert.assertEquals(0, reader.getDataRowGroupBoundary(0));
            Assert.assertEquals(500_000, reader.getDataRowGroupBoundary(1));
            Assert.assertEquals(1_000_000, reader.getDataRowGroupBoundary(2));
        }));
    }

    /**
     * An index with no row groups at all: every key is absent and only the
     * RG_FIRST_KEY sentinel is present.
     */
    @Test
    public void testZeroRowGroupsIsAbsent() throws Exception {
        assertMemoryLeak(() -> withReader(IndexMetaFileReaderTest::buildZeroRowGroupSample, reader -> {
            Assert.assertEquals(0, reader.getIndexRowGroupCount());
            Assert.assertEquals(-1, reader.getRowGroupLoForKey(0));
            Assert.assertEquals(-1, reader.getRowGroupHiForKey(0));
            Assert.assertEquals(-1, reader.getRowGroupLoForKey(50));
            Assert.assertEquals(-1, reader.getRowGroupHiForKey(50));
            // Only the sentinel is present.
            Assert.assertEquals(100, reader.getRowGroupFirstKey(0));
        }));
    }

    private static void addColumn(long writerPtr, String name, int id, int colType) {
        final int nameLen = name.length();
        final long namePtr = Unsafe.malloc(nameLen, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < nameLen; i++) {
                Unsafe.putByte(namePtr + i, (byte) name.charAt(i));
            }
            // flags 0, fixedByteLen 0, physicalType 1, maxRepLevel 0, maxDefLevel 1,
            // matching the Rust fixtures' descriptor().
            IndexMetaFileWriter.addColumn(writerPtr, namePtr, nameLen, id, colType, 0, 0, 1, 0, 1);
        } finally {
            Unsafe.free(namePtr, nameLen, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void addKeyAndRowIdRowGroup(long writerPtr, int firstKey, long rows, long rowIdMin, long rowIdMax) {
        final long chunksSize = 2L * IndexMetaFileWriter.CHUNK_SIZE;
        final long chunksPtr = Unsafe.calloc(chunksSize, MemoryTag.NATIVE_DEFAULT);
        try {
            putKeyIdChunk(chunksPtr, 0, firstKey, firstKey, rows);
            putRowIdChunk(chunksPtr, 1, rowIdMin, rowIdMax, rows);
            IndexMetaFileWriter.addRowGroup(writerPtr, firstKey, rows, chunksPtr, 2);
        } finally {
            Unsafe.free(chunksPtr, chunksSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Three row groups whose names are already 8-aligned, so the padding moves
     * from the name section and RG_FIRST_KEY to RG_BLOCK_OFFSET. Mirrors the
     * Rust {@code build_aligned_sample}.
     */
    private static void buildAlignedSample(long writerPtr) {
        IndexMetaFileWriter.setPayload(writerPtr, IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, 900);
        addColumn(writerPtr, "key_id", -1, TYPE_INT);
        addColumn(writerPtr, "row_id", -1, TYPE_LONG);
        addColumn(writerPtr, "pxpx", 3, TYPE_DOUBLE);
        final int[] firstKeys = {0, 300, 700};
        for (int i = 0; i < firstKeys.length; i++) {
            final long chunksSize = 3L * IndexMetaFileWriter.CHUNK_SIZE;
            final long chunksPtr = Unsafe.calloc(chunksSize, MemoryTag.NATIVE_DEFAULT);
            try {
                putKeyIdChunk(chunksPtr, 0, firstKeys[i], firstKeys[i] + 99, 100);
                putRowIdChunk(chunksPtr, 1, i * 100L, i * 100L + 99, 100);
                IndexMetaFileWriter.addRowGroup(writerPtr, firstKeys[i], 100, chunksPtr, 3);
            } finally {
                Unsafe.free(chunksPtr, chunksSize, MemoryTag.NATIVE_DEFAULT);
            }
        }
        setDataRowGroupBoundaries(writerPtr, 0L, 150L, 300L);
    }

    /**
     * Two row groups whose first keys are 5 and 9, so keys 0 to 4 sort below
     * the directory even though they are below KEY_COUNT.
     */
    private static void buildBelowFirstKeySample(long writerPtr) {
        IndexMetaFileWriter.setPayload(writerPtr, IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, 100);
        addColumn(writerPtr, "key_id", -1, TYPE_INT);
        addColumn(writerPtr, "row_id", -1, TYPE_LONG);
        addKeyAndRowIdRowGroup(writerPtr, 5, 10, 0, 99);
        addKeyAndRowIdRowGroup(writerPtr, 9, 10, 0, 99);
        setDataRowGroupBoundaries(writerPtr, 0L, 20L);
    }

    /**
     * A single row group whose covered UUID column carries 16-byte min and max
     * statistics, which do not fit in the 8 inline bytes. Mirrors the Rust
     * {@code test_out_of_line_stats_for_wide_covered_column}.
     */
    private static void buildOutOfLineStatSample(long writerPtr) {
        IndexMetaFileWriter.setPayload(writerPtr, IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, 50);
        addColumn(writerPtr, "key_id", -1, TYPE_INT);
        addColumn(writerPtr, "row_id", -1, TYPE_LONG);
        addColumn(writerPtr, "uid", 4, TYPE_UUID);
        final long chunksSize = 3L * IndexMetaFileWriter.CHUNK_SIZE;
        final long chunksPtr = Unsafe.calloc(chunksSize, MemoryTag.NATIVE_DEFAULT);
        try {
            putKeyIdChunk(chunksPtr, 0, 7, 7, 64);
            putRowIdChunk(chunksPtr, 1, 0, 63, 64);
            putChunk(chunksPtr, 2, CODEC_ZSTD, 0,
                    STAT_MIN_PRESENT | STAT_MIN_EXACT | STAT_MAX_PRESENT | STAT_MAX_EXACT,
                    0, 64, 0, 0, 0, 0, 0, 0);
            IndexMetaFileWriter.addRowGroup(writerPtr, 7, 64, chunksPtr, 3);
        } finally {
            Unsafe.free(chunksPtr, chunksSize, MemoryTag.NATIVE_DEFAULT);
        }
        putOutOfLineStat(writerPtr, 2, true, (byte) 0x11, 16);
        putOutOfLineStat(writerPtr, 2, false, (byte) 0xEE, 16);
        setDataRowGroupBoundaries(writerPtr, 0L, 64L);
    }

    /**
     * The fixture the absolute byte offset assertions pin, mirroring the Rust
     * {@code sample_writer}: 3 columns whose names total 17 bytes, so the name
     * section needs 7 bytes of padding, and 4 row groups, so RG_BLOCK_OFFSET
     * is already 8-aligned while RG_FIRST_KEY needs 4 bytes of padding.
     */
    private static void buildSample(long writerPtr) {
        IndexMetaFileWriter.setPayload(writerPtr, IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, 11_405);
        addColumn(writerPtr, "key_id", -1, TYPE_INT);
        addColumn(writerPtr, "row_id", -1, TYPE_LONG);
        addColumn(writerPtr, "price", 7, TYPE_DOUBLE);
        // firstKey, lastKey, rowIdMin, rowIdMax, rows
        final long[][] specs = {
                {0, 11_402, 0, 99_999, 100_000},
                {11_403, 11_403, 100_000, 157_999, 58_000},
                {11_403, 11_403, 158_000, 240_000, 82_001},
                {11_404, 11_404, 240_001, 999_999, 759_999},
        };
        for (int i = 0; i < specs.length; i++) {
            final long[] spec = specs[i];
            final long rows = spec[4];
            final long chunksSize = 3L * IndexMetaFileWriter.CHUNK_SIZE;
            final long chunksPtr = Unsafe.calloc(chunksSize, MemoryTag.NATIVE_DEFAULT);
            try {
                putKeyIdChunk(chunksPtr, 0, (int) spec[0], (int) spec[1], rows);
                putRowIdChunk(chunksPtr, 1, spec[2], spec[3], rows);
                // Fully populated covered-column chunk: every field carries a
                // distinct value so the round trip pins all of them.
                putChunk(
                        chunksPtr,
                        2,
                        CODEC_SNAPPY,
                        ENC_PLAIN | ENC_BYTE_STREAM_SPLIT,
                        STAT_MIN_PRESENT | STAT_MIN_INLINED | STAT_MIN_EXACT
                                | STAT_MAX_PRESENT | STAT_MAX_INLINED
                                | STAT_NULL_COUNT_PRESENT | STAT_DISTINCT_COUNT_PRESENT,
                        encodeStatSizes(8, 8),
                        rows,
                        4_096 + i * 1_000L,
                        512 + i,
                        i,
                        40 + i,
                        100 + i,
                        900 + i
                );
                IndexMetaFileWriter.addRowGroup(writerPtr, (int) spec[0], rows, chunksPtr, 3);
            } finally {
                Unsafe.free(chunksPtr, chunksSize, MemoryTag.NATIVE_DEFAULT);
            }
        }
        setDataRowGroupBoundaries(writerPtr, 0L, 500_000L, 1_000_000L);
    }

    private static void buildZeroRowGroupSample(long writerPtr) {
        IndexMetaFileWriter.setPayload(writerPtr, IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, 100);
        addColumn(writerPtr, "key_id", -1, TYPE_INT);
        addColumn(writerPtr, "row_id", -1, TYPE_LONG);
        setDataRowGroupBoundaries(writerPtr, 0L, 20L);
    }

    private static int encodeStatSizes(int minSize, int maxSize) {
        return (minSize & 0x0F) | ((maxSize & 0x0F) << 4);
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

    private static void putChunk(
            long chunksPtr,
            int column,
            int codec,
            int encodings,
            int statFlags,
            int statSizes,
            long numValues,
            long byteRangeStart,
            long totalCompressed,
            long nullCount,
            long distinctCount,
            long minStat,
            long maxStat
    ) {
        final long a = chunksPtr + (long) column * IndexMetaFileWriter.CHUNK_SIZE;
        Unsafe.putByte(a + IndexMetaFileWriter.CHUNK_CODEC_OFF, (byte) codec);
        Unsafe.putByte(a + IndexMetaFileWriter.CHUNK_ENCODINGS_OFF, (byte) encodings);
        Unsafe.putByte(a + IndexMetaFileWriter.CHUNK_STAT_FLAGS_OFF, (byte) statFlags);
        Unsafe.putByte(a + IndexMetaFileWriter.CHUNK_STAT_SIZES_OFF, (byte) statSizes);
        Unsafe.getUnsafe().putLong(a + IndexMetaFileWriter.CHUNK_NUM_VALUES_OFF, numValues);
        Unsafe.getUnsafe().putLong(a + IndexMetaFileWriter.CHUNK_BYTE_RANGE_START_OFF, byteRangeStart);
        Unsafe.getUnsafe().putLong(a + IndexMetaFileWriter.CHUNK_TOTAL_COMPRESSED_OFF, totalCompressed);
        Unsafe.getUnsafe().putLong(a + IndexMetaFileWriter.CHUNK_NULL_COUNT_OFF, nullCount);
        Unsafe.getUnsafe().putLong(a + IndexMetaFileWriter.CHUNK_DISTINCT_COUNT_OFF, distinctCount);
        Unsafe.getUnsafe().putLong(a + IndexMetaFileWriter.CHUNK_MIN_STAT_OFF, minStat);
        Unsafe.getUnsafe().putLong(a + IndexMetaFileWriter.CHUNK_MAX_STAT_OFF, maxStat);
    }

    /**
     * A {@code key_id} chunk whose min stat is the row group's first key, as
     * the redundancy invariant the writer enforces requires.
     */
    private static void putKeyIdChunk(long chunksPtr, int column, int firstKey, int lastKey, long rows) {
        putChunk(
                chunksPtr,
                column,
                CODEC_ZSTD,
                ENC_RLE_DICTIONARY,
                STAT_MIN_PRESENT | STAT_MIN_INLINED | STAT_MIN_EXACT
                        | STAT_MAX_PRESENT | STAT_MAX_INLINED | STAT_MAX_EXACT
                        | STAT_NULL_COUNT_PRESENT,
                encodeStatSizes(4, 4),
                rows,
                0,
                0,
                0,
                0,
                firstKey,
                lastKey
        );
    }

    private static void putOutOfLineStat(long writerPtr, int column, boolean isMin, byte fill, int length) {
        final long dataPtr = Unsafe.malloc(length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < length; i++) {
                Unsafe.putByte(dataPtr + i, fill);
            }
            IndexMetaFileWriter.addOutOfLineStat(writerPtr, column, isMin, dataPtr, length);
        } finally {
            Unsafe.free(dataPtr, length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void putRowIdChunk(long chunksPtr, int column, long min, long max, long rows) {
        putChunk(
                chunksPtr,
                column,
                CODEC_ZSTD,
                ENC_DELTA_BINARY_PACKED,
                STAT_MIN_PRESENT | STAT_MIN_INLINED | STAT_MIN_EXACT
                        | STAT_MAX_PRESENT | STAT_MAX_INLINED | STAT_MAX_EXACT,
                encodeStatSizes(8, 8),
                rows,
                0,
                0,
                0,
                0,
                min,
                max
        );
    }

    private static void setDataRowGroupBoundaries(long writerPtr, long... boundaries) {
        final long size = (long) boundaries.length * Long.BYTES;
        final long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < boundaries.length; i++) {
                Unsafe.getUnsafe().putLong(ptr + (long) i * Long.BYTES, boundaries[i]);
            }
            IndexMetaFileWriter.setDataRowGroupBoundaries(writerPtr, ptr, boundaries.length);
        } finally {
            Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
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

    private void withAlignedSample(SampleAssertion assertion) {
        withReader(IndexMetaFileReaderTest::buildAlignedSample, assertion);
    }

    /**
     * Builds an _im file with the Rust writer and hands the finished bytes to
     * {@code assertion}. The buffer is owned by the native result and freed on
     * every path, including the exceptional one.
     */
    private void withBytes(SampleBuilder builder, BytesAssertion assertion) {
        // The Rust writer takes the payload kind, key count and the synthetic
        // column indices up front; setPayload can correct the first two later.
        final long writerPtr = IndexMetaFileWriter.create(
                IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, 0, 0, 1);
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

    /**
     * Writes the sample to a file with one header field overwritten and the
     * CRC repaired, so the reader reaches the check under test instead of
     * failing the checksum first.
     */
    private void withPatchedSample(String fileName, long offset, long value, int width, PathAssertion assertion) {
        withSampleBytes((dataPtr, dataLen) -> {
            final FilesFacade ff = configuration.getFilesFacade();
            final long copyPtr = Unsafe.malloc(dataLen, MemoryTag.NATIVE_DEFAULT);
            try {
                Vect.memcpy(copyPtr, dataPtr, dataLen);
                if (width == Long.BYTES) {
                    Unsafe.getUnsafe().putLong(copyPtr + offset, value);
                } else {
                    Unsafe.getUnsafe().putInt(copyPtr + offset, (int) value);
                }
                // The CRC covers [8, size - 4), so a patched header field
                // invalidates it unless it is recomputed here.
                Unsafe.getUnsafe().putInt(
                        copyPtr + dataLen - 4,
                        Zip.crc32(0, copyPtr + 8, (int) (dataLen - 12))
                );
                try (Path path = new Path()) {
                    path.of(root).concat(fileName);
                    writeFile(ff, path.$(), copyPtr, dataLen);
                    assertion.run(path.$());
                }
            } finally {
                Unsafe.free(copyPtr, dataLen, MemoryTag.NATIVE_DEFAULT);
            }
        });
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
    private interface PathAssertion {
        void run(LPSZ path);
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
