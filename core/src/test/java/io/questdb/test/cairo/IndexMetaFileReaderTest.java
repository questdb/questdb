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
import io.questdb.std.Numbers;
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
    // Parquet physical types as the raw descriptor byte, from the Rust
    // physical_type_to_u8.
    private static final int PHYSICAL_FIXED_LEN_BYTE_ARRAY = 7;
    private static final int PHYSICAL_INT32 = 1;
    private static final int PHYSICAL_INT64 = 2;
    private static final int STAT_DISTINCT_COUNT_PRESENT = 1 << 6;
    private static final int STAT_MAX_EXACT = 1 << 5;
    private static final int STAT_MAX_INLINED = 1 << 4;
    private static final int STAT_MAX_PRESENT = 1 << 3;
    private static final int STAT_MIN_EXACT = 1 << 2;
    private static final int STAT_MIN_INLINED = 1 << 1;
    private static final int STAT_MIN_PRESENT = 1;
    private static final int STAT_NULL_COUNT_PRESENT = 1 << 7;
    // Absolute layout of buildTwoBlockOutOfLineStatSample, pinned by
    // testTwoBlockOutOfLineStatSampleLayout so the crafted offsets below keep
    // addressing what they are meant to address. These are the offsets the
    // Rust test_two_block_out_of_line_sample_layout pins.
    private static final int TWO_BLOCK_0_OFF = 176;
    private static final int TWO_BLOCK_1_OFF = 408;
    private static final int TWO_BLOCK_FILE_LEN = 684;
    private static final byte TWO_BLOCK_MAX_FILL_0 = (byte) 0xEE;
    private static final byte TWO_BLOCK_MAX_FILL_1 = (byte) 0xDD;
    private static final byte TWO_BLOCK_MIN_FILL_0 = 0x11;
    private static final byte TWO_BLOCK_MIN_FILL_1 = 0x22;
    // Out-of-line region of each block: a 16-byte min followed by a 16-byte
    // max, the second of which ends exactly at the block's end.
    private static final int TWO_BLOCK_OOL_SIZE = 32;
    private static final int TWO_BLOCK_SECTIONS_OFF = 640;
    // MAX_STAT of the uid chunk, relative to the block start: past NUM_ROWS
    // and the two preceding chunks, then 56 into the chunk.
    private static final int TWO_BLOCK_UID_MAX_STAT = 8 + 2 * 64 + 56;
    // QuestDB column type tags, spelled out so the fixtures do not depend on
    // ColumnType's ordering, exactly as the Rust fixtures do.
    private static final int TYPE_DOUBLE = 10;
    private static final int TYPE_INT = 5;
    private static final int TYPE_LONG = 6;
    private static final int TYPE_LONG256 = 13;
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
     * The chunk count alone used to decide how far the native side read, and
     * the buffer's real length never crossed JNI: a count one too high read 64
     * bytes of heap past the allocation and wrote them into the file, with no
     * error on either side. The length now crosses with the pointer and must
     * account for the count exactly.
     */
    @Test
    public void testAddRowGroupRejectsChunkBufferLengthMismatch() throws Exception {
        assertMemoryLeak(() -> {
            final long writerPtr = IndexMetaFileWriter.create(
                    IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, 0, 0, 1);
            long resultPtr = 0;
            try {
                IndexMetaFileWriter.setPayload(writerPtr, IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, 50);
                addColumn(writerPtr, "key_id", -1, TYPE_INT);
                addColumn(writerPtr, "row_id", -1, TYPE_LONG);
                final long chunksSize = 2L * IndexMetaFileWriter.CHUNK_SIZE;
                final long chunksPtr = Unsafe.calloc(chunksSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    putKeyIdChunk(chunksPtr, 0, 7, 7, 64);
                    putRowIdChunk(chunksPtr, 1, 0, 63, 64);
                    // One chunk more than the buffer holds: the third chunk
                    // would come from past the end of the allocation.
                    try {
                        IndexMetaFileWriter.addRowGroup(writerPtr, 7, 64, chunksPtr, chunksSize, 3);
                        Assert.fail("expected CairoException from the chunk buffer length check");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "column chunk buffer length");
                    }
                    // A buffer longer than the count claims is a mismatch too:
                    // it means the two sides disagree about the layout.
                    try {
                        IndexMetaFileWriter.addRowGroup(writerPtr, 7, 64, chunksPtr, chunksSize, 1);
                        Assert.fail("expected CairoException from the chunk buffer length check");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "column chunk buffer length");
                    }
                    // The negative count guard still comes first.
                    try {
                        IndexMetaFileWriter.addRowGroup(writerPtr, 7, 64, chunksPtr, chunksSize, -1);
                        Assert.fail("expected CairoException from the negative count check");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "count is negative");
                    }
                    // The matching length is accepted, and the row group it
                    // builds still produces a readable file.
                    IndexMetaFileWriter.addRowGroup(writerPtr, 7, 64, chunksPtr, chunksSize, 2);
                } finally {
                    Unsafe.free(chunksPtr, chunksSize, MemoryTag.NATIVE_DEFAULT);
                }
                setDataRowGroupBoundaries(writerPtr, 0L, 64L);
                resultPtr = IndexMetaFileWriter.finish(writerPtr);
                try (IndexMetaFileReader reader = new IndexMetaFileReader()) {
                    reader.ofAddress(
                            IndexMetaFileWriter.resultDataPtr(resultPtr),
                            IndexMetaFileWriter.resultDataLen(resultPtr)
                    );
                    Assert.assertEquals(2, reader.getColumnCount());
                    Assert.assertEquals(1, reader.getIndexRowGroupCount());
                    Assert.assertEquals(64, reader.getRowGroupNumRows(0));
                    Assert.assertEquals(64, reader.getChunkNumValues(0, 1));
                }
            } finally {
                if (resultPtr != 0) {
                    IndexMetaFileWriter.destroyResult(resultPtr);
                }
                IndexMetaFileWriter.destroyWriter(writerPtr);
            }
        });
    }

    /**
     * A column index reaches an address computation, so the chunk accessors
     * bound it rather than trusting the caller: an out-of-range one would land
     * hundreds of megabytes past a mapping of IM_FILE_SIZE bytes, and an
     * {@code assert} does not fire in production. The Rust reader's
     * {@code column_chunk} returns an error for the same index.
     */
    @Test
    public void testChunkAccessorRejectsOutOfRangeColumn() throws Exception {
        assertMemoryLeak(() -> withSample(reader -> {
            try {
                reader.getChunkMinStat(0, 10_000_000);
                Assert.fail("expected CairoException from the column index bound");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "_im column index out of range");
            }
            try {
                reader.getChunkByteRangeStart(0, -1);
                Assert.fail("expected CairoException from the column index bound");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "_im column index out of range");
            }
            // The fixture has 3 columns, so the last valid index still reads.
            Assert.assertEquals(4_096, reader.getChunkByteRangeStart(0, 2));
        }));
    }

    /**
     * The descriptor accessors bound their column index for the same reason
     * the chunk accessors do: with assertions off an out-of-range index is an
     * address hundreds of megabytes past a mapping of IM_FILE_SIZE bytes. The
     * Rust reader's {@code column_descriptor} returns an error for the same
     * index.
     */
    @Test
    public void testColumnDescriptorAccessorRejectsOutOfRangeColumn() throws Exception {
        assertMemoryLeak(() -> withSample(reader -> {
            try {
                reader.getColumnId(10_000_000);
                Assert.fail("expected CairoException from the column index bound");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "_im column index out of range");
            }
            try {
                reader.getColumnName(-1);
                Assert.fail("expected CairoException from the column index bound");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "_im column index out of range");
            }
            try {
                reader.getColumnType(3);
                Assert.fail("expected CairoException from the column index bound");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "_im column index out of range");
            }
            // The fixture has 3 columns, so the last valid index still reads.
            Assert.assertEquals(7, reader.getColumnId(2));
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
            // -1 is the synthetic columns' sentinel, not a lookup key: it must
            // miss rather than return the first of them.
            Assert.assertEquals(-1, reader.getColumnIndexById(-1));
            Assert.assertEquals(-1, reader.getColumnIndexById(Integer.MIN_VALUE));
            // The synthetic columns carry -1 and are located through the header.
            Assert.assertEquals(0, reader.getKeyIdColumn());
            Assert.assertEquals(1, reader.getRowIdColumn());
        }));
    }

    /**
     * KEY_ID_COLUMN is the only sanctioned route to the synthetic
     * {@code key_id} column, so a caller hands it straight to a chunk
     * accessor. A crafted one with a repaired CRC used to open cleanly and
     * then index hundreds of megabytes past the mapping. Mirrors the Rust
     * {@code test_crafted_key_id_column_is_rejected_at_open}.
     */
    @Test
    public void testCraftedKeyIdColumnIsRejectedAtOpen() throws Exception {
        assertMemoryLeak(() -> {
            // KEY_ID_COLUMN is at offset 48 and the sample has 3 columns.
            assertOpenRejected(48, 10_000_000, "_im KEY_ID_COLUMN out of range");
            // -1 is the descriptor sentinel for a synthetic column, never an index.
            assertOpenRejected(48, -1, "_im KEY_ID_COLUMN out of range");
            assertOpenRejected(48, 3, "_im KEY_ID_COLUMN out of range");
            // The last valid index is accepted.
            withPatchedBytes(IndexMetaFileReaderTest::buildSample, 48, 2, Integer.BYTES, (dataPtr, dataLen) -> {
                try (IndexMetaFileReader reader = new IndexMetaFileReader()) {
                    reader.ofAddress(dataPtr, dataLen);
                    Assert.assertEquals(2, reader.getKeyIdColumn());
                }
            });
        });
    }

    /**
     * PAYLOAD_KIND decides whether ROW_ID_COLUMN may be absent, so a kind
     * neither reader knows leaves that rule undecidable. Mirrors the Rust
     * {@code test_crafted_payload_kind_is_rejected_at_open}.
     */
    @Test
    public void testCraftedPayloadKindIsRejectedAtOpen() throws Exception {
        // PAYLOAD_KIND is at offset 28.
        assertMemoryLeak(() -> assertOpenRejected(28, 2, "unknown _im PAYLOAD_KIND"));
    }

    /**
     * ROW_ID_COLUMN reaches the same address computation as KEY_ID_COLUMN, and
     * it is {@code -1} exactly under the row-per-key payload. Mirrors the Rust
     * {@code test_crafted_row_id_column_is_rejected_at_open}.
     */
    @Test
    public void testCraftedRowIdColumnIsRejectedAtOpen() throws Exception {
        assertMemoryLeak(() -> {
            // ROW_ID_COLUMN is at offset 52 and the sample has 3 columns.
            assertOpenRejected(52, 10_000_000, "_im ROW_ID_COLUMN is invalid");
            // -1 says "no row id column at all", which only the row-per-key
            // payload may say, and the sample is row-per-posting.
            assertOpenRejected(52, -1, "_im ROW_ID_COLUMN is invalid");
            // The converse: a row-per-key file must not name a row id column.
            assertOpenRejected(28, IndexMetaFileWriter.PAYLOAD_ROW_PER_KEY, "_im ROW_ID_COLUMN is invalid");
        });
    }

    /**
     * DATA_RG_BOUNDARY has {@code DATA_RG_COUNT + 1} entries, so the sentinel
     * index is valid and one past it is not. The bound is a real check rather
     * than an assert because assertions are off in production and the index
     * reaches an address computation; the Rust reader's
     * {@code data_row_group_boundary} returns an error for the same index.
     */
    @Test
    public void testDataBoundaryAccessorRejectsOutOfRangeIndex() throws Exception {
        assertMemoryLeak(() -> withSample(reader -> {
            Assert.assertEquals(2, reader.getDataRowGroupCount());
            try {
                reader.getDataRowGroupBoundary(3);
                Assert.fail("expected CairoException from the data boundary index bound");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "_im data boundary index out of range");
            }
            try {
                reader.getDataRowGroupBoundary(-1);
                Assert.fail("expected CairoException from the data boundary index bound");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "_im data boundary index out of range");
            }
            try {
                reader.getDataRowGroupBoundary(10_000_000);
                Assert.fail("expected CairoException from the data boundary index bound");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "_im data boundary index out of range");
            }
            // The sentinel entry is the last valid index and still reads.
            Assert.assertEquals(1_000_000, reader.getDataRowGroupBoundary(2));
        }));
    }

    /**
     * RG_FIRST_KEY has {@code INDEX_RG_COUNT + 1} entries, so the sentinel
     * index is valid and one past it is not. The bound is a real check rather
     * than an assert because assertions are off in production and the index
     * reaches an address computation; the Rust reader's
     * {@code row_group_first_key} returns an error for the same index.
     */
    @Test
    public void testFirstKeyAccessorRejectsOutOfRangeIndex() throws Exception {
        assertMemoryLeak(() -> withSample(reader -> {
            Assert.assertEquals(4, reader.getIndexRowGroupCount());
            try {
                reader.getRowGroupFirstKey(5);
                Assert.fail("expected CairoException from the first key index bound");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "_im first key index out of range");
            }
            try {
                reader.getRowGroupFirstKey(-1);
                Assert.fail("expected CairoException from the first key index bound");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "_im first key index out of range");
            }
            try {
                reader.getRowGroupFirstKey(10_000_000);
                Assert.fail("expected CairoException from the first key index bound");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "_im first key index out of range");
            }
            // The sentinel entry is the last valid index and still reads.
            Assert.assertEquals(11_405, reader.getRowGroupFirstKey(4));
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
     * A block's extent is
     * {@code [RG_BLOCK_OFFSET[i], RG_BLOCK_OFFSET[i + 1])}, so an entry that
     * does not ascend leaves a block with an empty or inverted extent and no
     * meaningful bound for its out-of-line stats. Rejecting the file at open
     * time -- rather than per block on first access -- is the call both
     * readers make, because it is what lets every later extent computation be
     * trusted. Mirrors the Rust
     * {@code test_non_ascending_block_offset_is_rejected_at_open}.
     */
    @Test
    public void testNonAscendingRowGroupBlockOffsetIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            // Entry 1 below entry 0. RG_BLOCK_OFFSET is at 984 in the sample
            // and holds 184 >> 3, 384 >> 3, 584 >> 3, 784 >> 3.
            assertOpenRejected(988, (184 >> 3) - 1, "_im RG_BLOCK_OFFSET entries must ascend");
            // Two blocks sharing an offset: the first would have an empty extent.
            assertOpenRejected(988, 184 >> 3, "_im RG_BLOCK_OFFSET entries must ascend");
            // A huge entry in front of the others is non-ascending too, so it
            // no longer has to be caught later by the per-block bound.
            assertOpenRejected(984, -1, "_im RG_BLOCK_OFFSET entries must ascend");
        });
    }

    /**
     * A failed bind must not leave the reader claiming to be open. Its column
     * count, row group count and section offsets are all zero at that point,
     * so a direct {@code ofAddress} caller that catches CairoException would
     * hold a reader that says it is open and answers nonsense.
     * {@link IndexMetaFileReader#openAndMapRO} masks this with its own
     * {@code clear()}, but the object itself must not lie. The Rust reader
     * cannot reach this state at all: its constructor returns a Result.
     */
    @Test
    public void testOfAddressFailureLeavesReaderClosed() throws Exception {
        // IM_MAGIC is at offset 8, and it is checked before anything is
        // resolved, so the reader fails with its fields still zeroed.
        assertMemoryLeak(() -> withPatchedBytes(IndexMetaFileReaderTest::buildSample, 8, 0, Long.BYTES, (dataPtr, dataLen) -> {
            try (IndexMetaFileReader reader = new IndexMetaFileReader()) {
                try {
                    reader.ofAddress(dataPtr, dataLen);
                    Assert.fail("expected CairoException from the IM_MAGIC check");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "bad _im IM_MAGIC");
                }
                Assert.assertFalse(reader.isOpen());
                Assert.assertEquals(0, reader.getAddr());
                Assert.assertEquals(0, reader.getFileSize());
            }
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
     * The legitimate case the bound must not break: each block's max stat
     * occupies the last 16 bytes of its own out-of-line region, so
     * {@code offset + length} lands exactly on the block's end. A bound
     * written with {@code >=} instead of {@code >} rejects this.
     */
    @Test
    public void testOutOfLineStatAtBlockEndIsAccepted() throws Exception {
        assertMemoryLeak(() -> withReader(IndexMetaFileReaderTest::buildTwoBlockOutOfLineStatSample, reader -> {
            Assert.assertEquals(TWO_BLOCK_FILE_LEN, reader.getFileSize());
            Assert.assertEquals(2, reader.getIndexRowGroupCount());
            final byte[] minFills = {TWO_BLOCK_MIN_FILL_0, TWO_BLOCK_MIN_FILL_1};
            final byte[] maxFills = {TWO_BLOCK_MAX_FILL_0, TWO_BLOCK_MAX_FILL_1};
            for (int rg = 0; rg < 2; rg++) {
                Assert.assertFalse(reader.isChunkMinStatInline(rg, 2));
                Assert.assertFalse(reader.isChunkMaxStatInline(rg, 2));
                Assert.assertEquals(16, reader.getChunkMinStatLength(rg, 2));
                Assert.assertEquals(16, reader.getChunkMaxStatLength(rg, 2));
                // The max stat ends exactly at the end of the block's region.
                final long blockAddr = reader.getAddr() + (rg == 0 ? TWO_BLOCK_0_OFF : TWO_BLOCK_1_OFF);
                final long regionStart = blockAddr + 8 + 3L * 64;
                final long maxAddr = reader.getChunkMaxStatAddr(rg, 2);
                Assert.assertEquals(TWO_BLOCK_OOL_SIZE, maxAddr - regionStart + reader.getChunkMaxStatLength(rg, 2));

                final long minAddr = reader.getChunkMinStatAddr(rg, 2);
                for (int i = 0; i < 16; i++) {
                    Assert.assertEquals(minFills[rg], Unsafe.getByte(minAddr + i));
                    Assert.assertEquals(maxFills[rg], Unsafe.getByte(maxAddr + i));
                }
            }
        }));
    }

    /**
     * Row group 0's max stat is repointed just past its own out-of-line
     * region, which is where row group 1's block begins. Bounded only by the
     * end of the whole row group region this resolves happily and hands back
     * another row group's bytes as this one's statistic -- a silently wrong
     * stat, and stats drive query pruning.
     */
    @Test
    public void testOutOfLineStatIntoNextBlockIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            final long patchOffset = TWO_BLOCK_0_OFF + TWO_BLOCK_UID_MAX_STAT;
            // Exactly the first 16 bytes of block 1.
            assertOutOfLineStatRejected(patchOffset, encodeOutOfLineStat(TWO_BLOCK_OOL_SIZE, 16), 0);
            // Straddling the boundary: the first 8 bytes are this block's, the
            // last 8 belong to the next one.
            assertOutOfLineStatRejected(patchOffset, encodeOutOfLineStat(TWO_BLOCK_OOL_SIZE - 8, 16), 0);
            // One byte past the block's end is the off-by-one case.
            assertOutOfLineStatRejected(patchOffset, encodeOutOfLineStat(TWO_BLOCK_OOL_SIZE, 1), 0);
        });
    }

    /**
     * The last block's extent ends at {@code INDEX_SECTIONS_OFFSET}, so a
     * reference past its own region would address the key directory.
     */
    @Test
    public void testOutOfLineStatPastIndexSectionsIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            final long patchOffset = TWO_BLOCK_1_OFF + TWO_BLOCK_UID_MAX_STAT;
            assertOutOfLineStatRejected(patchOffset, encodeOutOfLineStat(TWO_BLOCK_OOL_SIZE, 16), 1);
            // An offset large enough to overflow a naive offset + length sum is
            // rejected by the same comparison.
            assertOutOfLineStatRejected(patchOffset, -1L, 1);
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

    /**
     * A {@code UUID}, {@code LONG256} or {@code VARCHAR} covered column is a
     * parquet FIXED_LEN_BYTE_ARRAY, and its width is recorded only in the
     * descriptor's FIXED_BYTE_LEN. Without it a reader cannot decode the chunk
     * without the parquet footer, which is the whole point of {@code _im}.
     * MAX_REP_LEVEL and MAX_DEF_LEVEL are the two adjacent bytes after
     * PHYSICAL_TYPE, so the fixture gives every column a different pair: a
     * reader that transposed them would read a plausible value and pass.
     */
    @Test
    public void testRoundTripFixedLenByteArrayDescriptors() throws Exception {
        assertMemoryLeak(() -> withReader(IndexMetaFileReaderTest::buildFixedLenByteArraySample, reader -> {
            Assert.assertEquals(4, reader.getColumnCount());

            Assert.assertEquals(PHYSICAL_INT32, reader.getColumnPhysicalType(0));
            Assert.assertEquals(0, reader.getColumnFixedByteLen(0));
            Assert.assertEquals(0, reader.getColumnMaxRepLevel(0));
            Assert.assertEquals(1, reader.getColumnMaxDefLevel(0));

            Assert.assertEquals(PHYSICAL_INT64, reader.getColumnPhysicalType(1));
            Assert.assertEquals(0, reader.getColumnFixedByteLen(1));
            Assert.assertEquals(0, reader.getColumnMaxRepLevel(1));
            Assert.assertEquals(0, reader.getColumnMaxDefLevel(1));

            // The 16-byte UUID: the width the parquet footer would otherwise
            // have to supply.
            Assert.assertEquals(TYPE_UUID, reader.getColumnType(2));
            Assert.assertEquals(PHYSICAL_FIXED_LEN_BYTE_ARRAY, reader.getColumnPhysicalType(2));
            Assert.assertEquals(16, reader.getColumnFixedByteLen(2));
            Assert.assertEquals(2, reader.getColumnMaxRepLevel(2));
            Assert.assertEquals(3, reader.getColumnMaxDefLevel(2));

            // The 32-byte LONG256, so the width is read and not assumed.
            Assert.assertEquals(TYPE_LONG256, reader.getColumnType(3));
            Assert.assertEquals(PHYSICAL_FIXED_LEN_BYTE_ARRAY, reader.getColumnPhysicalType(3));
            Assert.assertEquals(32, reader.getColumnFixedByteLen(3));
            Assert.assertEquals(1, reader.getColumnMaxRepLevel(3));
            Assert.assertEquals(2, reader.getColumnMaxDefLevel(3));

            // Against the raw bytes the Rust writer laid down: FIXED_BYTE_LEN
            // at descriptor offset 20, PHYSICAL_TYPE 28, MAX_REP_LEVEL 29,
            // MAX_DEF_LEVEL 30.
            final long uidDesc = reader.getAddr() + 64 + 2 * 32;
            Assert.assertEquals(16, Unsafe.getUnsafe().getInt(uidDesc + 20));
            Assert.assertEquals(PHYSICAL_FIXED_LEN_BYTE_ARRAY, Unsafe.getByte(uidDesc + 28));
            Assert.assertEquals(2, Unsafe.getByte(uidDesc + 29));
            Assert.assertEquals(3, Unsafe.getByte(uidDesc + 30));

            // The accessors bound their column index like every other
            // descriptor accessor.
            try {
                reader.getColumnFixedByteLen(4);
                Assert.fail("expected CairoException from the column index bound");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "_im column index out of range");
            }
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
     * Every block accessor resolves its row group through RG_BLOCK_OFFSET, so
     * an out-of-range row group reads an entry from outside that array and the
     * bounds then applied to the "offset" it yields prove nothing. The bound is
     * a real check rather than an assert because assertions are off in
     * production; the Rust reader's {@code row_group_block_extent} returns an
     * error for the same index.
     */
    @Test
    public void testRowGroupAccessorRejectsOutOfRangeRowGroup() throws Exception {
        assertMemoryLeak(() -> withSample(reader -> {
            Assert.assertEquals(4, reader.getIndexRowGroupCount());
            try {
                reader.getRowGroupNumRows(4);
                Assert.fail("expected CairoException from the row group index bound");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "_im row group index out of range");
            }
            try {
                reader.getRowGroupNumRows(-1);
                Assert.fail("expected CairoException from the row group index bound");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "_im row group index out of range");
            }
            // The chunk accessors resolve their block the same way, so they
            // reject the same row group.
            try {
                reader.getChunkNumValues(10_000_000, 0);
                Assert.fail("expected CairoException from the row group index bound");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "_im row group index out of range");
            }
            // The last valid row group still reads.
            Assert.assertEquals(759_999, reader.getRowGroupNumRows(3));
        }));
    }

    /**
     * The paired accessor answers the lookup hot path in one pass, and the two
     * single-bound accessors delegate to it, so every one of the cases below
     * must give the same answer through either route. Mirrors the Rust
     * {@code row_group_range_for_key}, which returns the pair from one call.
     */
    @Test
    public void testRowGroupRangeForKeyPairsBothBounds() throws Exception {
        assertMemoryLeak(() -> {
            withSample(reader -> {
                // The specification's worked example:
                // RG_FIRST_KEY = [0, 11_403, 11_403, 11_404, KEY_COUNT].
                assertRangeForKey(reader, 0, 0, 0); // exact match at index 0
                assertRangeForKey(reader, 5, 0, 0); // packed inside row group 0
                assertRangeForKey(reader, 11_403, 1, 2); // spans two row groups
                assertRangeForKey(reader, 11_404, 3, 3); // exact match at index 3
                // KEY_COUNT and above are absent, and the comparison is
                // unsigned, so -1 read as a u32 is above KEY_COUNT.
                assertKeyAbsent(reader, 11_405);
                assertKeyAbsent(reader, -1);
            });
            // A key below the first row group's first key, and a file with no
            // row groups at all, are absent through both routes too.
            withReader(IndexMetaFileReaderTest::buildBelowFirstKeySample, reader -> {
                assertKeyAbsent(reader, 0);
                assertKeyAbsent(reader, 4);
                assertRangeForKey(reader, 5, 0, 0);
                assertRangeForKey(reader, 7, 0, 0);
                assertRangeForKey(reader, 50, 1, 1);
            });
            withReader(IndexMetaFileReaderTest::buildZeroRowGroupSample, reader -> {
                assertKeyAbsent(reader, 0);
                assertKeyAbsent(reader, 50);
            });
        });
    }

    /**
     * Pins the fixture the crafted out-of-line references patch, so a layout
     * change cannot quietly turn them into harmless offsets. These are the
     * offsets the Rust {@code test_two_block_out_of_line_sample_layout} pins.
     */
    @Test
    public void testTwoBlockOutOfLineStatSampleLayout() throws Exception {
        assertMemoryLeak(() -> withReader(IndexMetaFileReaderTest::buildTwoBlockOutOfLineStatSample, reader -> {
            final long addr = reader.getAddr();
            Assert.assertEquals(TWO_BLOCK_FILE_LEN, reader.getFileSize());
            Assert.assertEquals(TWO_BLOCK_SECTIONS_OFF, reader.getIndexSectionsOffset());
            Assert.assertEquals(TWO_BLOCK_0_OFF >> 3, Unsafe.getUnsafe().getInt(addr + TWO_BLOCK_SECTIONS_OFF));
            Assert.assertEquals(TWO_BLOCK_1_OFF >> 3, Unsafe.getUnsafe().getInt(addr + TWO_BLOCK_SECTIONS_OFF + 4));
            // Block 1 is the last one, so its extent ends at the index sections.
            Assert.assertEquals(TWO_BLOCK_SECTIONS_OFF - TWO_BLOCK_1_OFF, TWO_BLOCK_1_OFF - TWO_BLOCK_0_OFF);
            Assert.assertEquals(64, reader.getRowGroupNumRows(0));
            Assert.assertEquals(64, reader.getRowGroupNumRows(1));
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
        // flags 0, fixedByteLen 0, physicalType 1, maxRepLevel 0, maxDefLevel 1,
        // matching the Rust fixtures' descriptor().
        addColumn(writerPtr, name, id, colType, 0, 1, 0, 1);
    }

    private static void addColumn(
            long writerPtr,
            String name,
            int id,
            int colType,
            int fixedByteLen,
            int physicalType,
            int maxRepLevel,
            int maxDefLevel
    ) {
        final int nameLen = name.length();
        final long namePtr = Unsafe.malloc(nameLen, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < nameLen; i++) {
                Unsafe.putByte(namePtr + i, (byte) name.charAt(i));
            }
            IndexMetaFileWriter.addColumn(
                    writerPtr, namePtr, nameLen, id, colType, 0, fixedByteLen, physicalType, maxRepLevel, maxDefLevel);
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
            IndexMetaFileWriter.addRowGroup(writerPtr, firstKey, rows, chunksPtr, chunksSize, 2);
        } finally {
            Unsafe.free(chunksPtr, chunksSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * A key outside the covered key space answers absent through the paired
     * accessor and through both single-bound accessors.
     */
    private static void assertKeyAbsent(IndexMetaFileReader reader, int key) {
        Assert.assertEquals(IndexMetaFileReader.KEY_ABSENT, reader.getRowGroupRangeForKey(key));
        Assert.assertEquals(-1, reader.getRowGroupLoForKey(key));
        Assert.assertEquals(-1, reader.getRowGroupHiForKey(key));
    }

    /**
     * The paired accessor and the two single-bound accessors must give the
     * same inclusive range, since the latter delegate to the former.
     */
    private static void assertRangeForKey(IndexMetaFileReader reader, int key, int expectedLo, int expectedHi) {
        final long range = reader.getRowGroupRangeForKey(key);
        Assert.assertEquals(expectedLo, Numbers.decodeLowInt(range));
        Assert.assertEquals(expectedHi, Numbers.decodeHighInt(range));
        Assert.assertEquals(expectedLo, reader.getRowGroupLoForKey(key));
        Assert.assertEquals(expectedHi, reader.getRowGroupHiForKey(key));
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
                IndexMetaFileWriter.addRowGroup(writerPtr, firstKeys[i], 100, chunksPtr, chunksSize, 3);
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
     * Two covered columns of parquet type FIXED_LEN_BYTE_ARRAY, whose widths
     * live only in the descriptor's FIXED_BYTE_LEN: a 16-byte UUID and a
     * 32-byte LONG256. Every descriptor carries a different MAX_REP_LEVEL and
     * MAX_DEF_LEVEL, so a reader that transposed the two adjacent bytes fails
     * rather than reading a plausible value.
     */
    private static void buildFixedLenByteArraySample(long writerPtr) {
        IndexMetaFileWriter.setPayload(writerPtr, IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, 50);
        addColumn(writerPtr, "key_id", -1, TYPE_INT, 0, PHYSICAL_INT32, 0, 1);
        addColumn(writerPtr, "row_id", -1, TYPE_LONG, 0, PHYSICAL_INT64, 0, 0);
        addColumn(writerPtr, "uid", 4, TYPE_UUID, 16, PHYSICAL_FIXED_LEN_BYTE_ARRAY, 2, 3);
        addColumn(writerPtr, "l256", 9, TYPE_LONG256, 32, PHYSICAL_FIXED_LEN_BYTE_ARRAY, 1, 2);
        final long chunksSize = 4L * IndexMetaFileWriter.CHUNK_SIZE;
        final long chunksPtr = Unsafe.calloc(chunksSize, MemoryTag.NATIVE_DEFAULT);
        try {
            putKeyIdChunk(chunksPtr, 0, 7, 7, 64);
            putRowIdChunk(chunksPtr, 1, 0, 63, 64);
            putChunk(chunksPtr, 2, CODEC_ZSTD, ENC_PLAIN, 0, 0, 64, 0, 0, 0, 0, 0, 0);
            putChunk(chunksPtr, 3, CODEC_ZSTD, ENC_PLAIN, 0, 0, 64, 0, 0, 0, 0, 0, 0);
            IndexMetaFileWriter.addRowGroup(writerPtr, 7, 64, chunksPtr, chunksSize, 4);
        } finally {
            Unsafe.free(chunksPtr, chunksSize, MemoryTag.NATIVE_DEFAULT);
        }
        setDataRowGroupBoundaries(writerPtr, 0L, 64L);
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
            IndexMetaFileWriter.addRowGroup(writerPtr, 7, 64, chunksPtr, chunksSize, 3);
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
                IndexMetaFileWriter.addRowGroup(writerPtr, (int) spec[0], rows, chunksPtr, chunksSize, 3);
            } finally {
                Unsafe.free(chunksPtr, chunksSize, MemoryTag.NATIVE_DEFAULT);
            }
        }
        setDataRowGroupBoundaries(writerPtr, 0L, 500_000L, 1_000_000L);
    }

    /**
     * Two row groups, each carrying a 16-byte out-of-line min and a 16-byte
     * out-of-line max for a covered UUID column, so every block has a 32-byte
     * out-of-line region and the last stat of a block ends exactly at that
     * block's end. That is what makes the per-block bound testable: an
     * off-by-one loosening lets block 0 address block 1. Mirrors the Rust
     * {@code build_two_block_out_of_line_sample}.
     */
    private static void buildTwoBlockOutOfLineStatSample(long writerPtr) {
        IndexMetaFileWriter.setPayload(writerPtr, IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, 50);
        addColumn(writerPtr, "key_id", -1, TYPE_INT);
        addColumn(writerPtr, "row_id", -1, TYPE_LONG);
        addColumn(writerPtr, "uid", 4, TYPE_UUID);
        final int[] firstKeys = {7, 20};
        final byte[] minFills = {TWO_BLOCK_MIN_FILL_0, TWO_BLOCK_MIN_FILL_1};
        final byte[] maxFills = {TWO_BLOCK_MAX_FILL_0, TWO_BLOCK_MAX_FILL_1};
        for (int i = 0; i < firstKeys.length; i++) {
            final long chunksSize = 3L * IndexMetaFileWriter.CHUNK_SIZE;
            final long chunksPtr = Unsafe.calloc(chunksSize, MemoryTag.NATIVE_DEFAULT);
            try {
                putKeyIdChunk(chunksPtr, 0, firstKeys[i], firstKeys[i], 64);
                putRowIdChunk(chunksPtr, 1, i * 64L, i * 64L + 63, 64);
                putChunk(chunksPtr, 2, CODEC_ZSTD, 0,
                        STAT_MIN_PRESENT | STAT_MIN_EXACT | STAT_MAX_PRESENT | STAT_MAX_EXACT,
                        0, 64, 0, 0, 0, 0, 0, 0);
                IndexMetaFileWriter.addRowGroup(writerPtr, firstKeys[i], 64, chunksPtr, chunksSize, 3);
            } finally {
                Unsafe.free(chunksPtr, chunksSize, MemoryTag.NATIVE_DEFAULT);
            }
            // The out-of-line stats patch the row group that was just added.
            putOutOfLineStat(writerPtr, 2, true, minFills[i], 16);
            putOutOfLineStat(writerPtr, 2, false, maxFills[i], 16);
        }
        setDataRowGroupBoundaries(writerPtr, 0L, 128L);
    }

    private static void buildZeroRowGroupSample(long writerPtr) {
        IndexMetaFileWriter.setPayload(writerPtr, IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, 100);
        addColumn(writerPtr, "key_id", -1, TYPE_INT);
        addColumn(writerPtr, "row_id", -1, TYPE_LONG);
        setDataRowGroupBoundaries(writerPtr, 0L, 20L);
    }

    /**
     * The {@code (offset << 16) | length} encoding of an out-of-line stat
     * reference, relative to its row group block's out-of-line region.
     */
    private static long encodeOutOfLineStat(long offset, long length) {
        return (offset << 16) | length;
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

    /**
     * Patches a u32 of the standard sample and asserts the reader refuses to
     * bind to the result, leaving nothing mapped.
     */
    private void assertOpenRejected(long patchOffset, int value, String expectedMessage) {
        withPatchedBytes(IndexMetaFileReaderTest::buildSample, patchOffset, value, Integer.BYTES, (dataPtr, dataLen) -> {
            try (IndexMetaFileReader reader = new IndexMetaFileReader()) {
                reader.ofAddress(dataPtr, dataLen);
                Assert.fail("expected CairoException from the header validation");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), expectedMessage);
            }
        });
    }

    /**
     * Patches the {@code uid} chunk's out-of-line max stat reference of the
     * two-block sample and asserts that resolving it is refused. The file
     * itself stays valid, so the rejection is the bound talking and not a
     * broken header.
     */
    private void assertOutOfLineStatRejected(long patchOffset, long encoded, int rowGroup) {
        withPatchedBytes(
                IndexMetaFileReaderTest::buildTwoBlockOutOfLineStatSample,
                patchOffset,
                encoded,
                Long.BYTES,
                (dataPtr, dataLen) -> {
                    try (IndexMetaFileReader reader = new IndexMetaFileReader()) {
                        reader.ofAddress(dataPtr, dataLen);
                        try {
                            reader.getChunkMaxStatAddr(rowGroup, 2);
                            Assert.fail("expected CairoException from the out of line stat bound");
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "_im out of line stat out of bounds");
                        }
                        // The block's own min stat is untouched and still resolves.
                        Assert.assertNotEquals(0L, reader.getChunkMinStatAddr(rowGroup, 2));
                    }
                }
        );
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
     * Copies a sample with one field overwritten and the CRC repaired, so the
     * reader reaches the check under test instead of failing the checksum
     * first, and hands the copy to {@code assertion}. The copy is freed on
     * every path, including the exceptional one.
     */
    private void withPatchedBytes(SampleBuilder builder, long offset, long value, int width, BytesAssertion assertion) {
        withBytes(builder, (dataPtr, dataLen) -> {
            final long copyPtr = Unsafe.malloc(dataLen, MemoryTag.NATIVE_DEFAULT);
            try {
                Vect.memcpy(copyPtr, dataPtr, dataLen);
                if (width == Long.BYTES) {
                    Unsafe.getUnsafe().putLong(copyPtr + offset, value);
                } else {
                    Unsafe.getUnsafe().putInt(copyPtr + offset, (int) value);
                }
                // The CRC covers [8, size - 4), so a patched field invalidates
                // it unless it is recomputed here.
                Unsafe.getUnsafe().putInt(
                        copyPtr + dataLen - 4,
                        Zip.crc32(0, copyPtr + 8, (int) (dataLen - 12))
                );
                assertion.run(copyPtr, dataLen);
            } finally {
                Unsafe.free(copyPtr, dataLen, MemoryTag.NATIVE_DEFAULT);
            }
        });
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
