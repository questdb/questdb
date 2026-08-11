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
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Cross-implementation pin for the {@code _im} format, version 3. Every
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
    // Descriptor index of cover slot 0 in every fixture that carries the two
    // synthetic columns first: descriptor order is key_id, row_id, then the
    // covered columns in cover-slot order.
    private static final int FIRST_COVER_COLUMN = 2;
    // The values every byte of the file is set to in turn by the hostile
    // sweep: cleared, made non-zero, and set to all ones.
    private static final int[] HOSTILE_BYTE_VALUES = {0x00, 0x01, 0xFF};
    // Floors on the work the hostile sweep must actually do, so it can never
    // go vacuous: a mutation that made every case fail the header validation
    // would drive the full case count and never reach an accessor at all, and
    // an oracle that never runs proves nothing. Floors rather than exact
    // counts, because tightening a reader check legitimately moves both.
    private static final int HOSTILE_BOUND_CASE_MIN = 3_000;
    // The number of cases the hostile sweep drives, pinned so a refactor that
    // silently stops enumerating one of the families fails rather than passing
    // with less coverage than the day it was written.
    private static final int HOSTILE_CASE_COUNT = 5_293;
    private static final int HOSTILE_CHECKED_ADDRESS_MIN = 12_000;
    // Tally slots the sweep accumulates into: cases driven, cases the reader
    // bound to, and addresses the oracle bounded.
    private static final int HOSTILE_TALLY_ADDRESSES = 2;
    private static final int HOSTILE_TALLY_BOUND = 1;
    private static final int HOSTILE_TALLY_CASES = 0;
    // The boundary values every header field is set to in turn: zero, one, the
    // two u32 halves, the two i32 extremes, 2^63 and u64::MAX. -1L is u64::MAX
    // and Long.MIN_VALUE is 2^63; narrowed to a u32 field they are 0xFFFF_FFFF
    // and 0.
    private static final long[] HOSTILE_FIELD_VALUES = {
            0L, 1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE,
            0x8000_0000L, 0xFFFF_FFFFL, Integer.MIN_VALUE, Integer.MAX_VALUE
    };
    // The u32 header fields, in offset order: FORMAT_VERSION, PAYLOAD_KIND,
    // COLUMN_COUNT, INDEX_RG_COUNT, DATA_RG_COUNT, KEY_SPACE_SIZE,
    // KEY_ID_COLUMN, ROW_ID_COLUMN, PIDX_FOOTER_LENGTH, FIRST_COVER_COLUMN.
    private static final int[] HOSTILE_HEADER_INT_FIELDS = {24, 28, 32, 36, 40, 44, 48, 52, 72, 76};
    // The u64 header fields, in offset order: IM_FILE_SIZE, IM_MAGIC,
    // FEATURE_FLAGS, INDEX_SECTIONS_OFFSET, PIDX_FOOTER_OFFSET, and the first
    // 8 bytes of RESERVED, which a reader must ignore.
    private static final int[] HOSTILE_HEADER_LONG_FIELDS = {0, 8, 16, 56, 64, 80};
    // Lengths crafted into an (offset << 16) | length out-of-line stat
    // reference: the two-block fixture's region is 32 bytes holding a 16-byte
    // min and a 16-byte max.
    private static final int[] HOSTILE_STAT_LENGTHS = {0, 1, 16, 17, 32, 0xFFFF};
    // Offsets crafted into the same reference, straddling the start, the two
    // stat boundaries and the end of that 32-byte region.
    private static final int[] HOSTILE_STAT_OFFSETS = {0, 1, 15, 16, 17, 31, 32, 33, 0xFFFF};
    // Parquet physical types as the raw descriptor byte, from the Rust
    // physical_type_to_u8.
    private static final int PHYSICAL_FIXED_LEN_BYTE_ARRAY = 7;
    private static final int PHYSICAL_INT32 = 1;
    private static final int PHYSICAL_INT64 = 2;
    // The index parquet's own footer, recorded so its committed size is
    // derivable without an ff.length() call. The writer rejects a zero in
    // either field, so every fixture records one.
    private static final int SAMPLE_PIDX_FOOTER_LEN = 2_048;
    private static final long SAMPLE_PIDX_FOOTER_OFF = 1_048_576;
    // Filler for the permitted gap between the end of DATA_RG_BOUNDARY and the
    // CRC, the same byte the Rust with_slack fixture pads with.
    private static final byte SLACK_FILL = (byte) 0xA5;
    // The sparse fixture's key space: an exclusive bound on key ids, not a
    // count of the three distinct keys it holds.
    private static final int SPARSE_KEY_SPACE_SIZE = 12_001;
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
    // Rust test_two_block_out_of_line_sample_layout pins: the 128-byte header,
    // 3 descriptors of 32, the 15 name bytes padded to 240, then two blocks of
    // 8 + 3 * 64 + 32 = 232 bytes, then the index sections.
    private static final int TWO_BLOCK_0_OFF = 240;
    private static final int TWO_BLOCK_1_OFF = 472;
    // The sections from 704: RG_BLOCK_OFFSET 8, RG_FIRST_KEY 12 padded to 16,
    // RG_ROW_ID_MIN 16, RG_ROW_ID_MAX 16, DATA_RG_BOUNDARY 16, CRC 4.
    private static final int TWO_BLOCK_FILE_LEN = 780;
    private static final byte TWO_BLOCK_MAX_FILL_0 = (byte) 0xEE;
    private static final byte TWO_BLOCK_MAX_FILL_1 = (byte) 0xDD;
    private static final byte TWO_BLOCK_MIN_FILL_0 = 0x11;
    private static final byte TWO_BLOCK_MIN_FILL_1 = 0x22;
    // Out-of-line region of each block: a 16-byte min followed by a 16-byte
    // max, the second of which ends exactly at the block's end.
    private static final int TWO_BLOCK_OOL_SIZE = 32;
    private static final int TWO_BLOCK_SECTIONS_OFF = 704;
    // MAX_STAT of the uid chunk, relative to the block start: past NUM_ROWS
    // and the two preceding chunks, then 56 into the chunk.
    private static final int TWO_BLOCK_UID_MAX_STAT = 8 + 2 * 64 + 56;
    // MIN_STAT of the same chunk, 8 bytes ahead of its MAX_STAT.
    private static final int TWO_BLOCK_UID_MIN_STAT = 8 + 2 * 64 + 48;
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
            Assert.assertEquals(948, reader.getFileSize());
            Assert.assertEquals(3, Unsafe.getUnsafe().getInt(addr + 36)); // INDEX_RG_COUNT
            Assert.assertEquals(840, Unsafe.getUnsafe().getLong(addr + 56)); // INDEX_SECTIONS_OFFSET
            Assert.assertEquals(840, reader.getIndexSectionsOffset());

            // Names: 16 bytes at 224..240, no padding.
            TestUtils.assertEquals("pxpx", reader.getColumnName(2));
            Assert.assertEquals(224, Unsafe.getUnsafe().getLong(addr + 128));
            Assert.assertEquals(236, Unsafe.getUnsafe().getLong(addr + 192));

            // Blocks start immediately at 240.
            Assert.assertEquals(100, Unsafe.getUnsafe().getLong(addr + 240));
            Assert.assertEquals(100, Unsafe.getUnsafe().getLong(addr + 440));
            Assert.assertEquals(100, Unsafe.getUnsafe().getLong(addr + 640));

            // RG_BLOCK_OFFSET at 840: 3 entries (12 bytes) then 4 bytes of padding.
            Assert.assertEquals(240 >> 3, Unsafe.getUnsafe().getInt(addr + 840));
            Assert.assertEquals(440 >> 3, Unsafe.getUnsafe().getInt(addr + 844));
            Assert.assertEquals(640 >> 3, Unsafe.getUnsafe().getInt(addr + 848));
            Assert.assertEquals(0, Unsafe.getUnsafe().getInt(addr + 852));

            // RG_FIRST_KEY at 856: 4 entries (16 bytes), already aligned.
            Assert.assertEquals(0, Unsafe.getUnsafe().getInt(addr + 856));
            Assert.assertEquals(300, Unsafe.getUnsafe().getInt(addr + 860));
            Assert.assertEquals(700, Unsafe.getUnsafe().getInt(addr + 864));
            Assert.assertEquals(900, Unsafe.getUnsafe().getInt(addr + 868)); // sentinel

            // RG_ROW_ID_MIN at 872 and RG_ROW_ID_MAX at 896: 3 i64 each, both
            // already 8-aligned.
            Assert.assertEquals(0, Unsafe.getUnsafe().getLong(addr + 872));
            Assert.assertEquals(100, Unsafe.getUnsafe().getLong(addr + 880));
            Assert.assertEquals(200, Unsafe.getUnsafe().getLong(addr + 888));
            Assert.assertEquals(99, Unsafe.getUnsafe().getLong(addr + 896));
            Assert.assertEquals(199, Unsafe.getUnsafe().getLong(addr + 904));
            Assert.assertEquals(299, Unsafe.getUnsafe().getLong(addr + 912));

            // DATA_RG_BOUNDARY at 920, CRC at 944.
            Assert.assertEquals(0, Unsafe.getUnsafe().getLong(addr + 920));
            Assert.assertEquals(300, Unsafe.getUnsafe().getLong(addr + 936));

            // The reader resolves the same sections it was pinned against.
            Assert.assertEquals(100, reader.getRowGroupNumRows(2));
            Assert.assertEquals(700, reader.getRowGroupFirstKey(2));
            Assert.assertEquals(900, reader.getRowGroupFirstKey(3));
            Assert.assertEquals(2, reader.getRowGroupLoForKey(700));
            Assert.assertEquals(2, reader.getRowGroupHiForKey(700));
            Assert.assertEquals(200, reader.getRowGroupRowIdMin(2));
            Assert.assertEquals(299, reader.getRowGroupRowIdMax(2));
            Assert.assertEquals(300, reader.getDataRowGroupBoundary(2));
        }));
    }

    /**
     * Pins every section's absolute offset so an edit cannot shift one
     * undetected. The names total 17 bytes (padded to 24) and the row group
     * count is even, so RG_BLOCK_OFFSET lands 8-aligned and RG_FIRST_KEY is
     * padded.
     * <pre>
     * 0    header (128 bytes)
     * 128  column descriptors, 3 x 32 bytes
     * 224  name strings, 17 bytes, padded to 248
     * 248  row group blocks, 4 x (8 + 3 * 64) bytes
     * 1048 RG_BLOCK_OFFSET, 4 x 4 bytes, already aligned
     * 1064 RG_FIRST_KEY, 5 x 4 bytes, padded to 1088
     * 1088 RG_ROW_ID_MIN, 4 x 8 bytes
     * 1120 RG_ROW_ID_MAX, 4 x 8 bytes
     * 1152 DATA_RG_BOUNDARY, 3 x 8 bytes
     * 1176 CRC32
     * 1180 total
     * </pre>
     */
    @Test
    public void testAbsoluteByteLayoutWithPaddedNameSection() throws Exception {
        assertMemoryLeak(() -> withSample(reader -> {
            final long addr = reader.getAddr();
            Assert.assertEquals(1_180, reader.getFileSize());
            Assert.assertEquals(1_180, Unsafe.getUnsafe().getLong(addr)); // IM_FILE_SIZE
            Assert.assertEquals(0x0300_5844_4942_4451L, Unsafe.getUnsafe().getLong(addr + 8)); // IM_MAGIC
            Assert.assertEquals(0, Unsafe.getUnsafe().getLong(addr + 16)); // FEATURE_FLAGS
            Assert.assertEquals(3, Unsafe.getUnsafe().getInt(addr + 24)); // FORMAT_VERSION
            Assert.assertEquals(0, Unsafe.getUnsafe().getInt(addr + 28)); // PAYLOAD_KIND
            Assert.assertEquals(3, Unsafe.getUnsafe().getInt(addr + 32)); // COLUMN_COUNT
            Assert.assertEquals(4, Unsafe.getUnsafe().getInt(addr + 36)); // INDEX_RG_COUNT
            Assert.assertEquals(2, Unsafe.getUnsafe().getInt(addr + 40)); // DATA_RG_COUNT
            Assert.assertEquals(11_405, Unsafe.getUnsafe().getInt(addr + 44)); // KEY_SPACE_SIZE
            Assert.assertEquals(0, Unsafe.getUnsafe().getInt(addr + 48)); // KEY_ID_COLUMN
            Assert.assertEquals(1, Unsafe.getUnsafe().getInt(addr + 52)); // ROW_ID_COLUMN
            Assert.assertEquals(1_048, Unsafe.getUnsafe().getLong(addr + 56)); // INDEX_SECTIONS_OFFSET
            Assert.assertEquals(1_048, reader.getIndexSectionsOffset());
            Assert.assertEquals(SAMPLE_PIDX_FOOTER_OFF, Unsafe.getUnsafe().getLong(addr + 64)); // PIDX_FOOTER_OFFSET
            Assert.assertEquals(SAMPLE_PIDX_FOOTER_LEN, Unsafe.getUnsafe().getInt(addr + 72)); // PIDX_FOOTER_LENGTH
            Assert.assertEquals(FIRST_COVER_COLUMN, Unsafe.getUnsafe().getInt(addr + 76)); // FIRST_COVER_COLUMN
            // RESERVED exists so the next field does not cost a format version,
            // and a zero there is what lets a later writer spend it.
            for (long i = 80; i < 128; i++) {
                Assert.assertEquals(0, Unsafe.getByte(addr + i));
            }

            // Descriptors: 128 + 3 * 32 = 224.
            Assert.assertEquals(224, Unsafe.getUnsafe().getLong(addr + 128)); // col 0 name offset
            Assert.assertEquals(6, Unsafe.getUnsafe().getInt(addr + 152)); // col 0 name length
            Assert.assertEquals(230, Unsafe.getUnsafe().getLong(addr + 160)); // col 1 name offset
            Assert.assertEquals(236, Unsafe.getUnsafe().getLong(addr + 192)); // col 2 name offset
            Assert.assertEquals(5, Unsafe.getUnsafe().getInt(addr + 216)); // col 2 name length

            // Names: 224..241, then 7 bytes of padding to 248.
            for (long i = 241; i < 248; i++) {
                Assert.assertEquals(0, Unsafe.getByte(addr + i));
            }

            // Blocks: 8 + 3 * 64 = 200 bytes each, from 248.
            Assert.assertEquals(100_000, Unsafe.getUnsafe().getLong(addr + 248)); // block 0 NUM_ROWS
            Assert.assertEquals(58_000, Unsafe.getUnsafe().getLong(addr + 448)); // block 1 NUM_ROWS
            Assert.assertEquals(82_001, Unsafe.getUnsafe().getLong(addr + 648)); // block 2 NUM_ROWS
            Assert.assertEquals(759_999, Unsafe.getUnsafe().getLong(addr + 848)); // block 3 NUM_ROWS
            // Block 3, column 2 (price): NUM_ROWS + 2 chunks + the 8-byte prefix.
            Assert.assertEquals(759_999, Unsafe.getUnsafe().getLong(addr + 848 + 8 + 2 * 64 + 8));
            Assert.assertEquals(7_096, Unsafe.getUnsafe().getLong(addr + 848 + 8 + 2 * 64 + 16));

            // RG_BLOCK_OFFSET at 1048: 4 entries, no padding needed afterwards.
            Assert.assertEquals(248 >> 3, Unsafe.getUnsafe().getInt(addr + 1_048));
            Assert.assertEquals(448 >> 3, Unsafe.getUnsafe().getInt(addr + 1_052));
            Assert.assertEquals(648 >> 3, Unsafe.getUnsafe().getInt(addr + 1_056));
            Assert.assertEquals(848 >> 3, Unsafe.getUnsafe().getInt(addr + 1_060));

            // RG_FIRST_KEY at 1064: 5 entries (20 bytes) then 4 bytes of padding.
            Assert.assertEquals(0, Unsafe.getUnsafe().getInt(addr + 1_064));
            Assert.assertEquals(11_403, Unsafe.getUnsafe().getInt(addr + 1_068));
            Assert.assertEquals(11_403, Unsafe.getUnsafe().getInt(addr + 1_072));
            Assert.assertEquals(11_404, Unsafe.getUnsafe().getInt(addr + 1_076));
            Assert.assertEquals(11_405, Unsafe.getUnsafe().getInt(addr + 1_080)); // sentinel
            Assert.assertEquals(0, Unsafe.getUnsafe().getInt(addr + 1_084));

            // RG_ROW_ID_MIN at 1088 and RG_ROW_ID_MAX at 1120: 4 i64 each, and
            // both are written whatever the payload kind, so row per key has
            // time pruning too.
            Assert.assertEquals(0, Unsafe.getUnsafe().getLong(addr + 1_088));
            Assert.assertEquals(100_000, Unsafe.getUnsafe().getLong(addr + 1_096));
            Assert.assertEquals(158_000, Unsafe.getUnsafe().getLong(addr + 1_104));
            Assert.assertEquals(240_001, Unsafe.getUnsafe().getLong(addr + 1_112));
            Assert.assertEquals(99_999, Unsafe.getUnsafe().getLong(addr + 1_120));
            Assert.assertEquals(157_999, Unsafe.getUnsafe().getLong(addr + 1_128));
            Assert.assertEquals(240_000, Unsafe.getUnsafe().getLong(addr + 1_136));
            Assert.assertEquals(999_999, Unsafe.getUnsafe().getLong(addr + 1_144));

            // DATA_RG_BOUNDARY at 1152, CRC at 1176.
            Assert.assertEquals(0, Unsafe.getUnsafe().getLong(addr + 1_152));
            Assert.assertEquals(500_000, Unsafe.getUnsafe().getLong(addr + 1_160));
            Assert.assertEquals(1_000_000, Unsafe.getUnsafe().getLong(addr + 1_168));
            Assert.assertEquals(Zip.crc32(0, addr + 8, 1_168), Unsafe.getUnsafe().getInt(addr + 1_176));

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
                    IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, 0, 0, 1, FIRST_COVER_COLUMN);
            long resultPtr = 0;
            try {
                IndexMetaFileWriter.setPayload(writerPtr, IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, 50);
                IndexMetaFileWriter.setPidxFooter(writerPtr, 4_096, 512);
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
                        IndexMetaFileWriter.addRowGroup(writerPtr, 7, 0, 63, 64, chunksPtr, chunksSize, 3);
                        Assert.fail("expected CairoException from the chunk buffer length check");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "column chunk buffer length");
                    }
                    // A buffer longer than the count claims is a mismatch too:
                    // it means the two sides disagree about the layout.
                    try {
                        IndexMetaFileWriter.addRowGroup(writerPtr, 7, 0, 63, 64, chunksPtr, chunksSize, 1);
                        Assert.fail("expected CairoException from the chunk buffer length check");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "column chunk buffer length");
                    }
                    // The negative count guard still comes first.
                    try {
                        IndexMetaFileWriter.addRowGroup(writerPtr, 7, 0, 63, 64, chunksPtr, chunksSize, -1);
                        Assert.fail("expected CairoException from the negative count check");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "count is negative");
                    }
                    // The matching length is accepted, and the row group it
                    // builds still produces a readable file.
                    IndexMetaFileWriter.addRowGroup(writerPtr, 7, 0, 63, 64, chunksPtr, chunksSize, 2);
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
     * A query's {@code requiredCoverColumns} are cover slots - ordinals into
     * this index's own INCLUDE list - and not writer indices, so the accessor
     * that maps a slot to a descriptor is the one the query path actually
     * uses. The two spaces are easy to confuse and confusing them resolves to
     * a different covered column with no error, so both the mapping and the
     * out-of-range rejection are pinned. Mirrors the Rust
     * {@code test_cover_slot_round_trip} and
     * {@code test_cover_slots_are_positional_not_writer_indices}.
     */
    @Test
    public void testCoverColumnIndexRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            withReader(IndexMetaFileReaderTest::buildFixedLenByteArraySample, reader -> {
                Assert.assertEquals(FIRST_COVER_COLUMN, reader.getFirstCoverColumn());
                Assert.assertEquals(4, reader.getColumnCount());

                // Cover slot 0 is the first covered column, whatever its
                // writer index happens to be.
                Assert.assertEquals(2, reader.getCoverColumnIndex(0));
                TestUtils.assertEquals("uid", reader.getColumnName(reader.getCoverColumnIndex(0)));
                Assert.assertEquals(4, reader.getColumnId(reader.getCoverColumnIndex(0)));
                Assert.assertEquals(TYPE_UUID, reader.getColumnType(reader.getCoverColumnIndex(0)));

                Assert.assertEquals(3, reader.getCoverColumnIndex(1));
                TestUtils.assertEquals("l256", reader.getColumnName(reader.getCoverColumnIndex(1)));
                Assert.assertEquals(9, reader.getColumnId(reader.getCoverColumnIndex(1)));
                Assert.assertEquals(TYPE_LONG256, reader.getColumnType(reader.getCoverColumnIndex(1)));

                // The two spaces disagree on this fixture, which is what makes
                // it able to catch the confusion: uid's writer index is 4 and
                // its cover slot is 0.
                Assert.assertEquals(2, reader.getColumnIndexById(4));
                Assert.assertEquals(3, reader.getColumnIndexById(9));

                // There are two cover slots, so slot 2 is past the end: it must
                // be refused rather than resolve to something.
                assertCoverSlotRejected(reader, 2);
                // A writer index passed where a slot belongs is the mistake the
                // bound has to catch.
                assertCoverSlotRejected(reader, 4);
                assertCoverSlotRejected(reader, 9);
                // Neither a negative slot nor one near the top of the u32 range
                // may wrap back into range.
                assertCoverSlotRejected(reader, -1);
                assertCoverSlotRejected(reader, Integer.MIN_VALUE);
                assertCoverSlotRejected(reader, Integer.MAX_VALUE);
            });

            // FIRST_COVER_COLUMN is read from the header rather than assumed:
            // a row-per-key index has no row_id column, so cover slot 0 is
            // descriptor 1 here and descriptor 2 above.
            withRowPerKeyReader(IndexMetaFileReaderTest::buildRowPerKeySample, reader -> {
                Assert.assertEquals(1, reader.getFirstCoverColumn());
                Assert.assertEquals(1, reader.getCoverColumnIndex(0));
                TestUtils.assertEquals("price", reader.getColumnName(reader.getCoverColumnIndex(0)));
                Assert.assertEquals(7, reader.getColumnId(reader.getCoverColumnIndex(0)));
                assertCoverSlotRejected(reader, 1);
            });
        });
    }

    /**
     * A header claiming {@code u32::MAX} row groups or columns makes every
     * section size product enormous; unchecked, the sums wrap and the section
     * offsets land inside the header. Mirrors the Rust
     * {@code test_crafted_counts_are_rejected_by_checked_arithmetic}. The two
     * readers reject the same files here by different routes: Java refuses a
     * count above Integer.MAX_VALUE outright, where Rust carries it and fails
     * the section fit a few lines later.
     */
    @Test
    public void testCraftedCountsAreRejected() throws Exception {
        assertMemoryLeak(() -> {
            // COLUMN_COUNT at 32, INDEX_RG_COUNT at 36, DATA_RG_COUNT at 40.
            assertOpenRejected(32, -1, "invalid _im COLUMN_COUNT");
            assertOpenRejected(36, -1, "invalid _im INDEX_RG_COUNT");
            assertOpenRejected(40, -1, "invalid _im DATA_RG_COUNT");
            // Descriptors alone overrunning the index sections is rejected by
            // the section fit instead.
            assertOpenRejected(32, 1_000, "_im sections do not fit");
        });
    }

    /**
     * A name entry is bounded at open, before any accessor can turn it into an
     * address: below the descriptors it would alias them, and a u64 offset near
     * the top of the range would sign-extend into a wild address. Mirrors the
     * Rust {@code test_crafted_name_offset_is_rejected}.
     */
    @Test
    public void testCraftedNameOffsetIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            // Column 0's NAME_OFFSET is the first 8 bytes of the first
            // descriptor, at 128. u64::MAX would overflow a naive
            // offset + length sum.
            assertOpenRejected(128, -1L, Long.BYTES, "invalid _im column name pointer");
            // A name starting inside the descriptors is rejected the same way.
            assertOpenRejected(128, 128L, Long.BYTES, "invalid _im column name pointer");
        });
    }

    /**
     * RG_BLOCK_OFFSET entries that still ascend pass the ascent check, so the
     * per-block bound is what has to catch an entry addressing the header or
     * running past the block region. Every entry is bounded when the reader
     * binds, not on first access, so a crafted file never opens at all -- a
     * caller must not be handed a row group range for blocks it can never
     * resolve. Mirrors the Rust
     * {@code test_block_bounds_predicates_are_enforced_at_open}.
     */
    @Test
    public void testCraftedRowGroupBlockOffsetIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            // RG_BLOCK_OFFSET is at 1048 and holds 248 >> 3, 448 >> 3,
            // 648 >> 3, 848 >> 3. Point row group 0 at the header: the entries
            // still ascend, so only the per-block bound catches it.
            assertOpenRejected(1_048, 0, "_im row group block extent is outside the block region");
            // The last descriptor byte is still too early by one 8-byte unit.
            assertOpenRejected(1_048, (224 >> 3) - 1, "_im row group block extent is outside the block region");
            // Point the last row group past the end of the block region. The
            // block faulted is row group 2, whose extent runs to row group 3's
            // start, and not row group 3 itself.
            assertOpenRejected(1_060, (1_048 >> 3) + 1, "rowGroup=2");
            // An extent below the 8 + COLUMN_COUNT * 64 bytes a block needs for
            // NUM_ROWS and its chunks. Entry 1 one 8-byte unit past entry 0
            // still ascends, so only the size predicate catches it.
            assertOpenRejected(
                    1_052,
                    (248 >> 3) + 1,
                    "_im row group block extent is below the bytes its column chunks need");
            // The last block is bounded by INDEX_SECTIONS_OFFSET rather than by
            // a successor, so its extent needs the same check from the other
            // side.
            assertOpenRejected(
                    1_060,
                    (1_048 >> 3) - 1,
                    "_im row group block extent is below the bytes its column chunks need");
        });
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
     * The permanent hostile-input sweep over the version 3 surface. Every case
     * starts from an {@code _im} file the real Rust writer produced, mutates
     * it, repairs the CRC so the reader reaches the check under test instead of
     * failing the checksum first, binds a reader to the result and drives every
     * accessor.
     * <p>
     * Two things are asserted of every case. First, the reader either answers
     * or raises {@link CairoException}: any other throwable fails the sweep,
     * including an {@code AssertionError} from a bound that is only checked
     * under {@code -ea} and so would not exist in production. Second - and this
     * is what a sweep asserting only "no crash" would miss - every accessor
     * that returns an address returns one lying wholly inside the committed
     * file, so a reader handing back a pointer into unrelated memory fails here
     * rather than passing quietly.
     * <p>
     * The cases are enumerated rather than sampled, so a failure reproduces
     * exactly and its message names the byte and the value that produced it:
     * every byte position of the file at three values, every header field at
     * the boundary values, every RG_BLOCK_OFFSET entry, crafted out-of-line
     * stat references over both blocks of the two-block fixture, and every
     * truncation length from 0 to the file length.
     */
    @Test
    public void testHostileInputSweep() throws Exception {
        assertMemoryLeak(() -> {
            final int[] tally = new int[3];
            withSampleBytes((dataPtr, dataLen) -> {
                sweepSingleBytes(tally, dataPtr, dataLen);
                sweepHeaderFields(tally, dataPtr, dataLen);
                sweepRowGroupBlockOffsets(tally, dataPtr, dataLen);
                sweepTruncations(tally, dataPtr, dataLen);
            });
            withBytes(
                    IndexMetaFileReaderTest::buildTwoBlockOutOfLineStatSample,
                    (dataPtr, dataLen) -> sweepOutOfLineStatReferences(tally, dataPtr, dataLen)
            );
            Assert.assertEquals(HOSTILE_CASE_COUNT, tally[HOSTILE_TALLY_CASES]);
            Assert.assertTrue(
                    "the sweep bound only " + tally[HOSTILE_TALLY_BOUND] + " of its "
                            + tally[HOSTILE_TALLY_CASES] + " cases, so the accessors were barely reached",
                    tally[HOSTILE_TALLY_BOUND] >= HOSTILE_BOUND_CASE_MIN
            );
            Assert.assertTrue(
                    "the address oracle bounded only " + tally[HOSTILE_TALLY_ADDRESSES] + " addresses",
                    tally[HOSTILE_TALLY_ADDRESSES] >= HOSTILE_CHECKED_ADDRESS_MIN
            );
        });
    }

    /**
     * The names run 224..241 in the sample, so 232 is 8-aligned and past the
     * descriptors but still inside the name blob: the sections would overlap
     * the strings the descriptors point at. Mirrors the Rust
     * {@code test_index_sections_offset_inside_name_strings_is_rejected}. The
     * two readers reject the same two files; the Java message differs for the
     * name case because the name bound is the check that fires first.
     */
    @Test
    public void testIndexSectionsOffsetInsideDescriptorsOrNamesIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            // INDEX_SECTIONS_OFFSET is at 56.
            assertOpenRejected(56, 232L, Long.BYTES, "invalid _im column name pointer");
            // Inside the descriptors themselves is rejected by the bound that
            // puts the sections at or after them.
            assertOpenRejected(56, 192L, Long.BYTES, "_im sections do not fit");
        });
    }

    /**
     * The sample's sections need 128 bytes; pointing at the CRC, or anywhere
     * with less than that ahead of it, leaves them nowhere to fit. Mirrors the
     * Rust {@code test_index_sections_offset_leaving_no_room_is_rejected}.
     */
    @Test
    public void testIndexSectionsOffsetLeavingNoRoomIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            // At the CRC.
            assertOpenRejected(56, 1_176L, Long.BYTES, "_im sections do not fit");
            // 8 bytes short of the 128 the five sections occupy.
            assertOpenRejected(56, 1_056L, Long.BYTES, "_im sections do not fit");
            // Past the committed size entirely.
            assertOpenRejected(56, 2_048L, Long.BYTES, "_im sections do not fit");
        });
    }

    /**
     * An 8-aligned offset one section-size below {@code u64::MAX}: added to the
     * section sizes unchecked it wraps to a small value that passes every
     * bound, so the reader would resolve the sections inside the header. It
     * reads back negative in Java, which the same guard rejects. Mirrors the
     * Rust {@code test_index_sections_offset_overflowing_the_size_sum_is_rejected}.
     */
    @Test
    public void testIndexSectionsOffsetOverflowingTheSizeSumIsRejected() throws Exception {
        // u64::MAX - 7, which is 8-aligned and so passes the alignment check
        // in both readers.
        assertMemoryLeak(() -> assertOpenRejected(56, -8L, Long.BYTES, "_im sections do not fit"));
    }

    /**
     * A key below the first row group's first key is absent even though it is
     * below KEY_SPACE_SIZE: no row group can hold it.
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
     * {@code RG_FIRST_KEY = [0, 11_403, 11_403, 11_404, KEY_SPACE_SIZE]}.
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
            // KEY_SPACE_SIZE and above are absent. The comparison is unsigned, so
            // -1 read as a u32 is above KEY_SPACE_SIZE rather than below zero.
            Assert.assertEquals(-1, reader.getRowGroupLoForKey(11_405));
            Assert.assertEquals(-1, reader.getRowGroupHiForKey(11_405));
            Assert.assertEquals(-1, reader.getRowGroupLoForKey(-1));
            Assert.assertEquals(-1, reader.getRowGroupHiForKey(-1));
        }));
    }

    /**
     * The positive control for the block extent bound. The sample's blocks
     * carry no out-of-line region, so every extent is exactly the
     * {@code 8 + COLUMN_COUNT * 64} bytes NUM_ROWS and the chunks need: the
     * bound is {@code <} and not {@code <=} for that reason, and tightening it
     * would reject the file the writer had just produced while the Rust reader
     * went on accepting it. Every one of the three rejecting predicates is
     * pinned by {@link #testCraftedRowGroupBlockOffsetIsRejected()}; this is
     * the accepting tail of the Rust
     * {@code test_block_bounds_predicates_are_enforced_at_open}.
     */
    @Test
    public void testMinimumSizedBlockExtentIsAccepted() throws Exception {
        assertMemoryLeak(() -> withSample(reader -> {
            // Every term is read from the file rather than from the constants
            // above, or the comparison could not fail.
            final long addr = reader.getAddr();
            final long sectionsOffset = reader.getIndexSectionsOffset();
            final int rowGroupCount = reader.getIndexRowGroupCount();
            final long minBlockSize = 8 + (long) reader.getColumnCount() * 64;
            final long entry0 = Integer.toUnsignedLong(Unsafe.getUnsafe().getInt(addr + sectionsOffset));
            final long entry1 = Integer.toUnsignedLong(Unsafe.getUnsafe().getInt(addr + sectionsOffset + 4));
            Assert.assertEquals(minBlockSize, (entry1 - entry0) << 3);
            // The last block is bounded by INDEX_SECTIONS_OFFSET rather than by
            // a successor, so it exercises the bound from the other side, and
            // it is exactly the minimum size too.
            final long lastEntry = Integer.toUnsignedLong(
                    Unsafe.getUnsafe().getInt(addr + sectionsOffset + (long) (rowGroupCount - 1) * 4));
            Assert.assertEquals(minBlockSize, sectionsOffset - (lastEntry << 3));
            // And the reader accepts it: a bound written with <= would have
            // refused this file at open, before any of these reads.
            Assert.assertEquals(100_000, reader.getRowGroupNumRows(0));
            Assert.assertEquals(759_999, reader.getRowGroupNumRows(rowGroupCount - 1));
        }));
    }

    /**
     * Every section starts 8-byte aligned, and the row group block a
     * misaligned first section would address is read as 8-byte fields. Mirrors
     * the Rust {@code test_misaligned_index_sections_offset_is_rejected}.
     */
    @Test
    public void testMisalignedIndexSectionsOffsetIsRejected() throws Exception {
        // INDEX_SECTIONS_OFFSET is at 56 and is 1048 in the sample.
        assertMemoryLeak(() -> assertOpenRejected(
                56, 1_049L, Long.BYTES, "_im INDEX_SECTIONS_OFFSET is not 8 byte aligned"));
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
            // Entry 1 below entry 0. RG_BLOCK_OFFSET is at 1048 in the sample
            // and holds 248 >> 3, 448 >> 3, 648 >> 3, 848 >> 3.
            assertOpenRejected(1_052, (248 >> 3) - 1, "_im RG_BLOCK_OFFSET entries must ascend");
            // Two blocks sharing an offset: the first would have an empty extent.
            assertOpenRejected(1_052, 248 >> 3, "_im RG_BLOCK_OFFSET entries must ascend");
            // A huge entry in front of the others is non-ascending too, so it
            // no longer has to be caught later by the per-block bound.
            assertOpenRejected(1_048, -1, "_im RG_BLOCK_OFFSET entries must ascend");
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
     * {@code ofAddress} binds to a caller-owned buffer, so the caller's length
     * is the only thing standing between a short buffer and reads past its
     * end. Mirrors the Rust {@code IndexMetaReader::new} rejecting a slice
     * shorter than the header plus the trailer, and its
     * {@code IM_FILE_SIZE is outside the buffer} bound.
     */
    @Test
    public void testOfAddressRejectsShortBuffer() throws Exception {
        assertMemoryLeak(() -> withSampleBytes((dataPtr, dataLen) -> {
            try (IndexMetaFileReader reader = new IndexMetaFileReader()) {
                // One byte short of the fixed header plus the CRC trailer.
                try {
                    reader.ofAddress(
                            dataPtr,
                            IndexMetaFileReader.IM_HEADER_SIZE + IndexMetaFileReader.IM_TRAILER_SIZE - 1);
                    Assert.fail("expected CairoException from the buffer size check");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "invalid _im buffer size");
                }
                Assert.assertFalse(reader.isOpen());
                try {
                    reader.ofAddress(dataPtr, 0);
                    Assert.fail("expected CairoException from the buffer size check");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "invalid _im buffer size");
                }
                // Long enough for the header, but shorter than the committed
                // IM_FILE_SIZE the header claims.
                try {
                    reader.ofAddress(
                            dataPtr,
                            IndexMetaFileReader.IM_HEADER_SIZE + IndexMetaFileReader.IM_TRAILER_SIZE);
                    Assert.fail("expected CairoException from the IM_FILE_SIZE check");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "invalid _im IM_FILE_SIZE");
                }
                // A null address is not a buffer at all.
                try {
                    reader.ofAddress(0, dataLen);
                    Assert.fail("expected CairoException from the address check");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "invalid _im mapping address");
                }
                Assert.assertFalse(reader.isOpen());
                // The same buffer at its real length still binds.
                reader.ofAddress(dataPtr, dataLen);
                Assert.assertTrue(reader.isOpen());
                Assert.assertEquals(1_180, reader.getFileSize());
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
                // Offset 248 is the first row group block, inside the CRC area.
                flipByte(ff, path.$(), 248);
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
                    Assert.assertEquals(1_180, reader.getFileSize());
                    Assert.assertEquals(1_048, reader.getIndexSectionsOffset());
                    Assert.assertEquals(0, reader.getPayloadKind());
                    Assert.assertEquals(3, reader.getColumnCount());
                    Assert.assertEquals(11_405, reader.getKeySpaceSize());
                    Assert.assertEquals(4, reader.getIndexRowGroupCount());
                    Assert.assertEquals(2, reader.getDataRowGroupCount());
                    TestUtils.assertEquals("price", reader.getColumnName(2));
                    Assert.assertEquals(2, reader.getColumnIndexById(7));
                    Assert.assertEquals(2, reader.getCoverColumnIndex(0));
                    Assert.assertEquals(1, reader.getRowGroupLoForKey(11_403));
                    Assert.assertEquals(2, reader.getRowGroupHiForKey(11_403));
                    Assert.assertEquals(82_001, reader.getRowGroupNumRows(2));
                    Assert.assertEquals(158_000, reader.getChunkMinStat(2, 1));
                    Assert.assertEquals(240_000, reader.getChunkMaxStat(2, 1));
                    Assert.assertEquals(158_000, reader.getRowGroupRowIdMin(2));
                    Assert.assertEquals(240_000, reader.getRowGroupRowIdMax(2));
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
     * Version 2 was the interim layout that keyed the column projection on the
     * writer index, defined the key field as a distinct-key count and carried
     * no row-id zone maps. It is not readable by this format.
     */
    @Test
    public void testOpenAndMapROWrongVersion() throws Exception {
        assertMemoryLeak(() -> withPatchedSample("wrong-version._im", 24, 2L, Integer.BYTES, path -> {
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
            // Header 128, descriptors 96, names "key_idrow_iduid" padded 15 -> 16,
            // one block of 8 + 3 * 64 plus 32 out-of-line bytes, then the index
            // sections: RG_BLOCK_OFFSET 4 padded to 8, RG_FIRST_KEY 8,
            // RG_ROW_ID_MIN 8, RG_ROW_ID_MAX 8, DATA_RG_BOUNDARY 16, CRC 4.
            Assert.assertEquals(524, reader.getFileSize());
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

    /**
     * The committed IM_FILE_SIZE is the only boundary a reader has: bytes past
     * it belong to a later, unpublished write and must reach neither an answer
     * the reader gives nor the range whose CRC it verifies. Every other
     * fixture hands {@code ofAddress} a buffer whose length is exactly
     * IM_FILE_SIZE, so a reader that bound itself by the buffer instead would
     * pass the whole suite.
     * <p>
     * The trailing bytes are deliberately shaped like a continuation of the
     * file - a second DATA_RG_BOUNDARY array followed by four bytes where a
     * trailer would sit - so a reader taking its bounds from the buffer finds
     * something plausible rather than obvious rubbish. The four trailing bytes
     * are the committed file's own CRC value, which is exactly what a naive
     * "the checksum is the last four bytes" reader picks up. Mirrors the Rust
     * {@code test_reader_ignores_bytes_past_committed_size}.
     */
    @Test
    public void testReaderIsBoundedByCommittedSize() throws Exception {
        assertMemoryLeak(() -> withSampleBytes((dataPtr, dataLen) -> {
            // Three more i64 boundaries and a four-byte trailer.
            final long trailingLen = 3L * Long.BYTES + Integer.BYTES;
            final long extendedLen = dataLen + trailingLen;
            final long copyPtr = Unsafe.malloc(extendedLen, MemoryTag.NATIVE_DEFAULT);
            try {
                Vect.memcpy(copyPtr, dataPtr, dataLen);
                Unsafe.getUnsafe().putLong(copyPtr + dataLen, 0L);
                Unsafe.getUnsafe().putLong(copyPtr + dataLen + 8, 7_000_000L);
                Unsafe.getUnsafe().putLong(copyPtr + dataLen + 16, 9_000_000L);
                final int committedCrc = Unsafe.getUnsafe().getInt(copyPtr + dataLen - 4);
                Unsafe.getUnsafe().putInt(copyPtr + extendedLen - 4, committedCrc);

                // What makes those bytes load-bearing: over the longer range the
                // stored trailer does not verify, so a reader bounded by the
                // buffer rejects this file outright rather than answering from
                // it. IM_FILE_SIZE itself is untouched and still the committed
                // length.
                Assert.assertNotEquals(
                        Zip.crc32(0, copyPtr + 8, (int) (extendedLen - 12)),
                        Unsafe.getUnsafe().getInt(copyPtr + extendedLen - 4)
                );
                Assert.assertEquals(dataLen, Unsafe.getUnsafe().getLong(copyPtr));

                try (IndexMetaFileReader reader = new IndexMetaFileReader()) {
                    reader.ofAddress(copyPtr, extendedLen);
                    // The reader's world is the committed image, not the buffer.
                    Assert.assertEquals(dataLen, reader.getFileSize());
                    // And every answer is the one the exact-sized image gives,
                    // so the appended DATA_RG_BOUNDARY copy is invisible.
                    Assert.assertEquals(1_048, reader.getIndexSectionsOffset());
                    Assert.assertEquals(2, reader.getDataRowGroupCount());
                    Assert.assertEquals(0, reader.getDataRowGroupBoundary(0));
                    Assert.assertEquals(500_000, reader.getDataRowGroupBoundary(1));
                    Assert.assertEquals(1_000_000, reader.getDataRowGroupBoundary(2));
                    Assert.assertEquals(4, reader.getIndexRowGroupCount());
                    Assert.assertEquals(11_405, reader.getRowGroupFirstKey(4));
                    Assert.assertEquals(999_999, reader.getRowGroupRowIdMax(3));
                    Assert.assertEquals(759_999, reader.getRowGroupNumRows(3));
                    assertRangeForKey(reader, 11_403, 1, 2);
                    // The boundary accessor still stops at DATA_RG_COUNT rather
                    // than walking into the appended array.
                    try {
                        reader.getDataRowGroupBoundary(3);
                        Assert.fail("expected CairoException from the data boundary bound");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "_im data boundary index out of range");
                    }
                }
            } finally {
                Unsafe.free(copyPtr, extendedLen, MemoryTag.NATIVE_DEFAULT);
            }
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
            final long uidDesc = reader.getAddr() + 128 + 2 * 32;
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
            Assert.assertEquals(1_180, reader.getFileSize());
            Assert.assertEquals(0, reader.getFeatureFlags());
            Assert.assertEquals(IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, reader.getPayloadKind());
            Assert.assertEquals(3, reader.getColumnCount());
            Assert.assertEquals(4, reader.getIndexRowGroupCount());
            Assert.assertEquals(2, reader.getDataRowGroupCount());
            Assert.assertEquals(11_405, reader.getKeySpaceSize());
            Assert.assertEquals(0, reader.getKeyIdColumn());
            Assert.assertEquals(1, reader.getRowIdColumn());
            Assert.assertEquals(FIRST_COVER_COLUMN, reader.getFirstCoverColumn());
            Assert.assertEquals(SAMPLE_PIDX_FOOTER_OFF, reader.getPidxFooterOffset());
            Assert.assertEquals(SAMPLE_PIDX_FOOTER_LEN, reader.getPidxFooterLength());
            // The index parquet's committed size: the footer range plus the
            // 4-byte footer length and the PAR1 magic.
            Assert.assertEquals(
                    SAMPLE_PIDX_FOOTER_OFF + SAMPLE_PIDX_FOOTER_LEN + 8,
                    reader.getPidxFileSize());
            Assert.assertEquals(1_048, reader.getIndexSectionsOffset());
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
            // The sentinel is KEY_SPACE_SIZE.
            Assert.assertEquals(11_405, reader.getRowGroupFirstKey(4));

            Assert.assertEquals(100_000, reader.getRowGroupNumRows(0));
            Assert.assertEquals(58_000, reader.getRowGroupNumRows(1));
            Assert.assertEquals(82_001, reader.getRowGroupNumRows(2));
            Assert.assertEquals(759_999, reader.getRowGroupNumRows(3));

            // The row-id zone maps duplicate the row_id chunk's statistics
            // under this payload kind, and the writer cross-checks them, so the
            // fast path has an independent oracle.
            for (int i = 0, n = reader.getIndexRowGroupCount(); i < n; i++) {
                Assert.assertEquals(reader.getRowGroupRowIdMin(i), reader.getChunkMinStat(i, 1));
                Assert.assertEquals(reader.getRowGroupRowIdMax(i), reader.getChunkMaxStat(i, 1));
            }
            Assert.assertEquals(0, reader.getRowGroupRowIdMin(0));
            Assert.assertEquals(999_999, reader.getRowGroupRowIdMax(3));

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
                // RG_FIRST_KEY = [0, 11_403, 11_403, 11_404, KEY_SPACE_SIZE].
                assertRangeForKey(reader, 0, 0, 0); // exact match at index 0
                assertRangeForKey(reader, 5, 0, 0); // packed inside row group 0
                assertRangeForKey(reader, 11_403, 1, 2); // spans two row groups
                assertRangeForKey(reader, 11_404, 3, 3); // exact match at index 3
                // KEY_SPACE_SIZE and above are absent, and the comparison is
                // unsigned, so -1 read as a u32 is above KEY_SPACE_SIZE.
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
     * Every other fixture is row-per-posting, so PAYLOAD_ROW_PER_KEY never
     * crossed JNI and neither {@code getPayloadKind() == 1} nor the
     * {@code getRowIdColumn() == -1} that goes with it was pinned on the Java
     * side. A row-per-key index has one row per key and no {@code row_id}
     * column at all, which is exactly the pair of header values the reader
     * validates against each other at open.
     */
    @Test
    public void testRowPerKeyPayloadRoundTrip() throws Exception {
        assertMemoryLeak(() -> withRowPerKeyReader(IndexMetaFileReaderTest::buildRowPerKeySample, reader -> {
            final long addr = reader.getAddr();
            Assert.assertEquals(IndexMetaFileWriter.PAYLOAD_ROW_PER_KEY, reader.getPayloadKind());
            Assert.assertEquals(1, reader.getPayloadKind());
            Assert.assertEquals(-1, reader.getRowIdColumn());
            Assert.assertEquals(0, reader.getKeyIdColumn());
            // Straight off the header bytes the Rust writer laid down:
            // PAYLOAD_KIND at 28, KEY_ID_COLUMN at 48, ROW_ID_COLUMN at 52.
            Assert.assertEquals(1, Unsafe.getUnsafe().getInt(addr + 28));
            Assert.assertEquals(0, Unsafe.getUnsafe().getInt(addr + 48));
            Assert.assertEquals(-1, Unsafe.getUnsafe().getInt(addr + 52));
            // There is no row_id column: the schema is key_id plus the covered
            // column only.
            Assert.assertEquals(2, reader.getColumnCount());
            TestUtils.assertEquals("key_id", reader.getColumnName(0));
            TestUtils.assertEquals("price", reader.getColumnName(1));
            Assert.assertEquals(1, reader.getColumnIndexById(7));
            // Key lookup is unaffected by the payload kind.
            Assert.assertEquals(50, reader.getKeySpaceSize());
            assertRangeForKey(reader, 7, 0, 0);
            assertRangeForKey(reader, 20, 1, 1);
            assertKeyAbsent(reader, 50);
        }));
    }

    /**
     * The boundary count alone used to decide how far the native side read,
     * and the buffer's real length never crossed JNI: a 16-byte allocation
     * passed with a count of 50_000_000 made the native side read 400MB and
     * killed the JVM with a SIGSEGV inside the copy. The length now crosses
     * with the pointer and must account for the count exactly.
     */
    @Test
    public void testSetDataRowGroupBoundariesRejectsBufferLengthMismatch() throws Exception {
        assertMemoryLeak(() -> {
            final long writerPtr = IndexMetaFileWriter.create(
                    IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, 0, 0, 1, FIRST_COVER_COLUMN);
            long resultPtr = 0;
            try {
                IndexMetaFileWriter.setPayload(writerPtr, IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, 50);
                IndexMetaFileWriter.setPidxFooter(writerPtr, 4_096, 512);
                addColumn(writerPtr, "key_id", -1, TYPE_INT);
                addColumn(writerPtr, "row_id", -1, TYPE_LONG);
                final long chunksSize = 2L * IndexMetaFileWriter.CHUNK_SIZE;
                final long chunksPtr = Unsafe.calloc(chunksSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    putKeyIdChunk(chunksPtr, 0, 7, 7, 64);
                    putRowIdChunk(chunksPtr, 1, 0, 63, 64);
                    IndexMetaFileWriter.addRowGroup(writerPtr, 7, 0, 63, 64, chunksPtr, chunksSize, 2);
                } finally {
                    Unsafe.free(chunksPtr, chunksSize, MemoryTag.NATIVE_DEFAULT);
                }
                final long size = 2L * Long.BYTES;
                final long ptr = Unsafe.calloc(size, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().putLong(ptr, 0L);
                    Unsafe.getUnsafe().putLong(ptr + Long.BYTES, 64L);
                    // The count that used to read 400MB past a 16-byte buffer.
                    try {
                        IndexMetaFileWriter.setDataRowGroupBoundaries(writerPtr, ptr, size, 50_000_000);
                        Assert.fail("expected CairoException from the boundary buffer length check");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "boundary buffer length");
                    }
                    // One boundary more than the buffer holds.
                    try {
                        IndexMetaFileWriter.setDataRowGroupBoundaries(writerPtr, ptr, size, 3);
                        Assert.fail("expected CairoException from the boundary buffer length check");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "boundary buffer length");
                    }
                    // A buffer longer than the count claims is a mismatch too:
                    // it means the two sides disagree about the layout.
                    try {
                        IndexMetaFileWriter.setDataRowGroupBoundaries(writerPtr, ptr, size, 1);
                        Assert.fail("expected CairoException from the boundary buffer length check");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "boundary buffer length");
                    }
                    // The negative count guard still comes first.
                    try {
                        IndexMetaFileWriter.setDataRowGroupBoundaries(writerPtr, ptr, size, -1);
                        Assert.fail("expected CairoException from the negative count check");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "count is negative");
                    }
                    // The matching length is accepted, and the boundaries it
                    // sets still produce a readable file.
                    IndexMetaFileWriter.setDataRowGroupBoundaries(writerPtr, ptr, size, 2);
                } finally {
                    Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
                }
                resultPtr = IndexMetaFileWriter.finish(writerPtr);
                try (IndexMetaFileReader reader = new IndexMetaFileReader()) {
                    reader.ofAddress(
                            IndexMetaFileWriter.resultDataPtr(resultPtr),
                            IndexMetaFileWriter.resultDataLen(resultPtr)
                    );
                    Assert.assertEquals(1, reader.getDataRowGroupCount());
                    Assert.assertEquals(0, reader.getDataRowGroupBoundary(0));
                    Assert.assertEquals(64, reader.getDataRowGroupBoundary(1));
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
     * Slack between the end of DATA_RG_BOUNDARY and the CRC is permitted:
     * readers bound the sections with {@code sectionsEnd <= crcEnd}, not
     * equality. No writer output has any, so without this fixture the
     * comparison could be tightened to {@code !=} and the whole suite would
     * still pass - and the two readers would then disagree about which files
     * are valid. Mirrors the Rust
     * {@code test_slack_before_the_crc_is_accepted}.
     */
    @Test
    public void testSlackBeforeCrcIsAccepted() throws Exception {
        assertMemoryLeak(() -> {
            for (int slack : new int[]{8, 16, 64}) {
                withSlackBytes(IndexMetaFileReaderTest::buildSample, slack, (dataPtr, dataLen) -> {
                    Assert.assertEquals(1_180 + slack, dataLen);
                    // The sections stop where they did; only the gap ahead of
                    // the CRC grew.
                    Assert.assertEquals(1_048, Unsafe.getUnsafe().getLong(dataPtr + 56));
                    for (int i = 0; i < slack; i++) {
                        Assert.assertEquals(SLACK_FILL, Unsafe.getByte(dataPtr + 1_180 - 4 + i));
                    }

                    try (IndexMetaFileReader reader = new IndexMetaFileReader()) {
                        reader.ofAddress(dataPtr, dataLen);
                        Assert.assertEquals(dataLen, reader.getFileSize());
                        // Every answer is the one the exact-sized image gives.
                        Assert.assertEquals(1_048, reader.getIndexSectionsOffset());
                        Assert.assertEquals(11_405, reader.getKeySpaceSize());
                        Assert.assertEquals(11_405, reader.getRowGroupFirstKey(4));
                        Assert.assertEquals(999_999, reader.getRowGroupRowIdMax(3));
                        Assert.assertEquals(1_000_000, reader.getDataRowGroupBoundary(2));
                        Assert.assertEquals(759_999, reader.getRowGroupNumRows(3));
                        assertRangeForKey(reader, 11_403, 1, 2);
                    }
                });
            }
        });
    }

    /**
     * Posting-index keys are a dense key space with sparse occupancy, so a
     * partition holding {@code {5, 900, 12_000}} has three distinct keys and a
     * key space of at least 12_001. v2 defined the header field as a count of
     * distinct keys, which made keys 900 and 12_000 fail the
     * {@code key >= KEY_SPACE_SIZE} test, report absent, and the query return
     * no rows with no error anywhere. Mirrors the Rust
     * {@code test_sparse_key_set_round_trip}.
     */
    @Test
    public void testSparseKeySetRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            withReader(IndexMetaFileReaderTest::buildSparseKeySample, reader -> {
                // The key space bound is the id bound, not the occupancy count.
                Assert.assertEquals(SPARSE_KEY_SPACE_SIZE, reader.getKeySpaceSize());
                Assert.assertEquals(3, reader.getIndexRowGroupCount());

                // Every key present resolves to its own row group, and the key
                // directory agrees with the key id chunk it duplicates.
                final int[] keys = {5, 900, 12_000};
                for (int i = 0; i < keys.length; i++) {
                    assertRangeForKey(reader, keys[i], i, i);
                    Assert.assertEquals(keys[i], reader.getRowGroupFirstKey(i));
                    Assert.assertEquals(keys[i], reader.getChunkMinStat(i, 0));
                    Assert.assertEquals(i * 10L, reader.getRowGroupRowIdMin(i));
                    Assert.assertEquals(i * 10L + 9, reader.getRowGroupRowIdMax(i));
                }
                // The sentinel is the key space bound, so the last row group's
                // key id range reads as [12_000, 12_001).
                Assert.assertEquals(SPARSE_KEY_SPACE_SIZE, reader.getRowGroupFirstKey(3));

                // An unoccupied id inside a row group's key range still
                // resolves: the directory answers which row groups could hold
                // it, not whether it is present.
                assertRangeForKey(reader, 6, 0, 0);
                assertRangeForKey(reader, 899, 0, 0);
                assertRangeForKey(reader, 11_999, 1, 1);
                // Only ids below the first entry or outside the key space are
                // absent.
                assertKeyAbsent(reader, 4);
                assertKeyAbsent(reader, SPARSE_KEY_SPACE_SIZE);
                assertKeyAbsent(reader, Integer.MAX_VALUE);
            });

            // Writing the distinct-key count instead is not merely wrong, it is
            // rejected: the last row group's first key would be unreachable.
            final long writerPtr = IndexMetaFileWriter.create(
                    IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, 0, 0, 1, FIRST_COVER_COLUMN);
            try {
                buildSparseKeySample(writerPtr);
                IndexMetaFileWriter.setPayload(writerPtr, IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, 3);
                try {
                    IndexMetaFileWriter.finish(writerPtr);
                    Assert.fail("expected CairoException from the key space size check");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "must be below key space size");
                }
            } finally {
                IndexMetaFileWriter.destroyWriter(writerPtr);
            }
        });
    }

    /**
     * A file cut short and re-committed at its new length, with the header's
     * counts and INDEX_SECTIONS_OFFSET untouched -- what a torn write leaves
     * behind: a header describing a body that is not there. Every one of these
     * must be rejected, and rejected by a bound rather than by an address
     * computation. The order in {@code parse()} is what makes that true:
     * {@code namesStart <= INDEX_SECTIONS_OFFSET <= sectionsEnd <= crcEnd}
     * runs before the loop that reads each descriptor's name entry, so a file
     * cut anywhere between the header and the end of the descriptors never
     * reads descriptor bytes it does not hold. The pre-fix Rust reader bounded
     * the name entries first and panicked on a slice range for every length
     * below the end of its descriptors. Mirrors the Rust
     * {@code test_truncated_and_recommitted_file_is_rejected_not_panicked}.
     */
    @Test
    public void testTruncatedAndRecommittedFileIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            // 132 is the smallest length the fixed-size check admits; 224 is
            // the end of the sample's 3 descriptors, 248 the end of the padded
            // name blob, 448 the end of the first row group block and 1048 the
            // start of the index sections. The rest walk the descriptors, the
            // names, the blocks and the sections, and each one leaves the
            // header claiming more than the file holds.
            for (int len : new int[]{
                    132, 136, 140, 152, 160, 168, 176, 192, 208, 216, 220, 224, 232, 240,
                    248, 256, 400, 448, 1_000, 1_048, 1_056, 1_172
            }) {
                assertTruncationRejected(len, "_im sections do not fit");
            }
            // Below the fixed header plus the trailer the reader refuses on the
            // size check, before it dereferences anything at all.
            for (int len : new int[]{12, 64, 68, 96, 128, 131}) {
                assertTruncationRejected(len, "invalid _im buffer size");
            }
            // The negative control: the sample at its own committed length is
            // the one image in this set that binds, so the rejections above are
            // the truncation talking and not something the fixture does anyway.
            withTruncatedBytes(IndexMetaFileReaderTest::buildSample, 1_180, (dataPtr, dataLen) -> {
                try (IndexMetaFileReader reader = new IndexMetaFileReader()) {
                    reader.ofAddress(dataPtr, dataLen);
                    Assert.assertEquals(1_180, reader.getFileSize());
                    Assert.assertEquals(759_999, reader.getRowGroupNumRows(3));
                }
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
            // Block 1 is the last one, so its extent ends at the index
            // sections. Both blocks carry the same three chunks and the same
            // 32 out-of-line bytes, so the two extents must match - and every
            // term here is read from the file, not from the constants above,
            // or the comparison could not fail.
            final long block0Off = (long) Unsafe.getUnsafe().getInt(addr + TWO_BLOCK_SECTIONS_OFF) << 3;
            final long block1Off = (long) Unsafe.getUnsafe().getInt(addr + TWO_BLOCK_SECTIONS_OFF + 4) << 3;
            final long sectionsOff = reader.getIndexSectionsOffset();
            Assert.assertEquals(block1Off - block0Off, sectionsOff - block1Off);
            Assert.assertEquals(8 + 3 * 64 + TWO_BLOCK_OOL_SIZE, sectionsOff - block1Off);
            Assert.assertEquals(64, reader.getRowGroupNumRows(0));
            Assert.assertEquals(64, reader.getRowGroupNumRows(1));
        }));
    }

    /**
     * FEATURE_FLAGS bits 32-63 are required: a reader that does not know one
     * must refuse the file rather than read it with a feature it cannot honour
     * silently disabled. Bits 0-31 are optional and must be ignored. Mirrors
     * the Rust {@code test_unknown_required_feature_bit_is_rejected}.
     */
    @Test
    public void testUnknownRequiredFeatureBitIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            // FEATURE_FLAGS is at 16.
            assertOpenRejected(16, 1L << 32, Long.BYTES, "unsupported required _im FEATURE_FLAGS");
            assertOpenRejected(16, 1L << 63, Long.BYTES, "unsupported required _im FEATURE_FLAGS");
            // An unknown optional bit is carried through, not rejected.
            withPatchedBytes(IndexMetaFileReaderTest::buildSample, 16, 1L << 7, Long.BYTES, (dataPtr, dataLen) -> {
                try (IndexMetaFileReader reader = new IndexMetaFileReader()) {
                    reader.ofAddress(dataPtr, dataLen);
                    Assert.assertEquals(1L << 7, reader.getFeatureFlags());
                }
            });
        });
    }

    /**
     * PIDX_FOOTER_OFFSET is a u64 the reader takes as given, so the bound falls
     * on the size derived from it rather than on the header field. At or above
     * 2^63 the offset reads back negative here and the derived size has no long
     * to live in, while the Rust reader can still form the u64 sum: the two
     * readers would answer differently for the same file. Both refuse it
     * instead, because cold-storage upload and orphan validation each need a
     * usable number and a plausible, wrong size is worse than an error. Mirrors
     * the Rust {@code test_unrepresentable_pidx_file_size_is_rejected}.
     */
    @Test
    public void testUnrepresentablePidxFileSizeIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            // Exactly 2^63, which is where the two readers used to part company.
            assertPidxFileSizeRejected(Long.MIN_VALUE);
            // One below 2^63 leaves the offset representable but not the sum.
            assertPidxFileSizeRejected(Long.MAX_VALUE);
            // And the top of the u64 range, where the sum wraps rather than
            // merely leaving the signed range.
            assertPidxFileSizeRejected(-1L);
            // The positive control: the largest offset whose derived size still
            // fits a long is accepted and yields exactly Long.MAX_VALUE. A bound
            // one step tighter would reject a file the other reader accepts.
            final long offset = Long.MAX_VALUE - SAMPLE_PIDX_FOOTER_LEN - 8;
            withPatchedBytes(IndexMetaFileReaderTest::buildSample, 64, offset, Long.BYTES, (dataPtr, dataLen) -> {
                try (IndexMetaFileReader reader = new IndexMetaFileReader()) {
                    reader.ofAddress(dataPtr, dataLen);
                    Assert.assertEquals(offset, reader.getPidxFooterOffset());
                    Assert.assertEquals(Long.MAX_VALUE, reader.getPidxFileSize());
                }
            });
        });
    }

    /**
     * The writer's validations run inside Rust, and until now none of them was
     * pinned from Java: a validation that stopped firing, or an error that
     * stopped crossing the JNI boundary as a CairoException, would leave every
     * Java fixture green. Two are exercised here, both of which produce a
     * silently wrong answer if they stop rejecting - out-of-order first keys
     * break the directory's binary search, and a missing pidx footer makes the
     * index parquet's derived committed size wrong.
     * <p>
     * The failing {@code finish} throws instead of returning a result, so the
     * caller is left holding a null result pointer. That is the path a leak
     * hides on: the writer must still be destroyable, and releasing the result
     * that was never produced must be a no-op rather than a crash.
     */
    @Test
    public void testWriterValidationFailureCrossesJni() throws Exception {
        assertMemoryLeak(() -> {
            long writerPtr = IndexMetaFileWriter.create(
                    IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, 0, 0, 1, FIRST_COVER_COLUMN);
            long resultPtr = 0;
            try {
                IndexMetaFileWriter.setPayload(writerPtr, IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, 100);
                IndexMetaFileWriter.setPidxFooter(writerPtr, 1_024, 128);
                addColumn(writerPtr, "key_id", -1, TYPE_INT);
                addColumn(writerPtr, "row_id", -1, TYPE_LONG);
                // Row group 1 starts below row group 0, so RG_FIRST_KEY would
                // not be non-decreasing and the lookup's binary search would
                // answer nonsense.
                addKeyAndRowIdRowGroup(writerPtr, 10, 5, 0, 99);
                addKeyAndRowIdRowGroup(writerPtr, 4, 5, 0, 99);
                setDataRowGroupBoundaries(writerPtr, 0L, 10L);
                try {
                    resultPtr = IndexMetaFileWriter.finish(writerPtr);
                    Assert.fail("expected CairoException from the first key ordering check");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "non-decreasing at index 1");
                }
                // Nothing was produced, so resultPtr is still the 0 it was
                // initialised with -- asserting that here could not fail, since
                // the only assignment to it is the call that threw. Releasing
                // the result that was never produced is the part that can:
                // destroyResult(0) must be a no-op rather than a crash.
                IndexMetaFileWriter.destroyResult(0);
            } finally {
                if (resultPtr != 0) {
                    IndexMetaFileWriter.destroyResult(resultPtr);
                }
                IndexMetaFileWriter.destroyWriter(writerPtr);
            }

            // A writer that never recorded the index parquet's footer is
            // refused for a different reason, so the surfacing is the
            // validation talking and not one message for every failure.
            writerPtr = IndexMetaFileWriter.create(
                    IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, 0, 0, 1, FIRST_COVER_COLUMN);
            try {
                IndexMetaFileWriter.setPayload(writerPtr, IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, 100);
                addColumn(writerPtr, "key_id", -1, TYPE_INT);
                addColumn(writerPtr, "row_id", -1, TYPE_LONG);
                addKeyAndRowIdRowGroup(writerPtr, 4, 5, 0, 99);
                setDataRowGroupBoundaries(writerPtr, 0L, 10L);
                try {
                    IndexMetaFileWriter.finish(writerPtr);
                    Assert.fail("expected CairoException from the pidx footer check");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "pidx footer offset 0 and length 0");
                }
                // The same writer finishes once the footer is recorded, so the
                // rejection is the missing value and not a poisoned writer.
                IndexMetaFileWriter.setPidxFooter(writerPtr, 1_024, 128);
                resultPtr = IndexMetaFileWriter.finish(writerPtr);
                try (IndexMetaFileReader reader = new IndexMetaFileReader()) {
                    reader.ofAddress(
                            IndexMetaFileWriter.resultDataPtr(resultPtr),
                            IndexMetaFileWriter.resultDataLen(resultPtr)
                    );
                    Assert.assertEquals(1_024, reader.getPidxFooterOffset());
                    Assert.assertEquals(128, reader.getPidxFooterLength());
                    Assert.assertEquals(1, reader.getIndexRowGroupCount());
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
     * v2's magic differed from v3's only in its top byte, and v2 files are not
     * readable, so the version byte inside the magic must not be ignored. The
     * magic is checked before FORMAT_VERSION and is what disambiguates
     * {@code _im} from {@code _pm} - which carries FEATURE_FLAGS at the same
     * offset - so a reader comparing only the seven low bytes would take a v2
     * file for a v3 one all the way to the FORMAT_VERSION field. The fixture
     * leaves that field at 3 precisely so the magic is the only thing that can
     * reject it. Mirrors the second half of the Rust
     * {@code test_wrong_magic_is_rejected}.
     */
    @Test
    public void testWrongMagicVersionByteIsRejected() throws Exception {
        // IM_MAGIC is at offset 8; this is v2's, the bytes QDBIDX\0\2.
        assertMemoryLeak(() -> assertOpenRejected(8, 0x0200_5844_4942_4451L, Long.BYTES, "bad _im IM_MAGIC"));
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
            IndexMetaFileWriter.addRowGroup(
                    writerPtr, firstKey, rowIdMin, rowIdMax, rows, chunksPtr, chunksSize, 2);
        } finally {
            Unsafe.free(chunksPtr, chunksSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * A cover slot outside {@code [0, coverCount)} must be refused rather than
     * resolve to a descriptor: the caller would otherwise read another
     * column's chunk as the one it asked for.
     */
    private static void assertCoverSlotRejected(IndexMetaFileReader reader, int slot) {
        try {
            reader.getCoverColumnIndex(slot);
            Assert.fail("expected CairoException from the cover slot bound");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "_im cover slot out of range");
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
        IndexMetaFileWriter.setPidxFooter(writerPtr, 4_096, 512);
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
                IndexMetaFileWriter.addRowGroup(
                        writerPtr, firstKeys[i], i * 100L, i * 100L + 99, 100, chunksPtr, chunksSize, 3);
            } finally {
                Unsafe.free(chunksPtr, chunksSize, MemoryTag.NATIVE_DEFAULT);
            }
        }
        setDataRowGroupBoundaries(writerPtr, 0L, 150L, 300L);
    }

    /**
     * Two row groups whose first keys are 5 and 9, so keys 0 to 4 sort below
     * the directory even though they are below KEY_SPACE_SIZE.
     */
    private static void buildBelowFirstKeySample(long writerPtr) {
        IndexMetaFileWriter.setPayload(writerPtr, IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, 100);
        IndexMetaFileWriter.setPidxFooter(writerPtr, 1_024, 128);
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
        IndexMetaFileWriter.setPidxFooter(writerPtr, 8_192, 256);
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
            IndexMetaFileWriter.addRowGroup(writerPtr, 7, 0, 63, 64, chunksPtr, chunksSize, 4);
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
        IndexMetaFileWriter.setPidxFooter(writerPtr, 8_192, 256);
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
            IndexMetaFileWriter.addRowGroup(writerPtr, 7, 0, 63, 64, chunksPtr, chunksSize, 3);
        } finally {
            Unsafe.free(chunksPtr, chunksSize, MemoryTag.NATIVE_DEFAULT);
        }
        putOutOfLineStat(writerPtr, 2, true, (byte) 0x11, 16);
        putOutOfLineStat(writerPtr, 2, false, (byte) 0xEE, 16);
        setDataRowGroupBoundaries(writerPtr, 0L, 64L);
    }

    /**
     * A row-per-key index: one index row per key, so there is no {@code row_id}
     * column and ROW_ID_COLUMN is the {@code -1} sentinel. The writer is
     * created with that pair by {@link #withRowPerKeyReader}.
     */
    private static void buildRowPerKeySample(long writerPtr) {
        IndexMetaFileWriter.setPayload(writerPtr, IndexMetaFileWriter.PAYLOAD_ROW_PER_KEY, 50);
        IndexMetaFileWriter.setPidxFooter(writerPtr, 2_048, 96);
        addColumn(writerPtr, "key_id", -1, TYPE_INT);
        addColumn(writerPtr, "price", 7, TYPE_DOUBLE);
        final int[] firstKeys = {7, 20};
        for (int i = 0; i < firstKeys.length; i++) {
            final long chunksSize = 2L * IndexMetaFileWriter.CHUNK_SIZE;
            final long chunksPtr = Unsafe.calloc(chunksSize, MemoryTag.NATIVE_DEFAULT);
            try {
                putKeyIdChunk(chunksPtr, 0, firstKeys[i], firstKeys[i], 64);
                putChunk(chunksPtr, 1, CODEC_SNAPPY, ENC_PLAIN, 0, 0, 64, 4_096, 512, 0, 0, 0, 0);
                // The row-id zone maps are unconditional: this payload has no
                // row_id column to take a range from, so the writer is given
                // one directly and it is the only source of time pruning here.
                IndexMetaFileWriter.addRowGroup(
                        writerPtr, firstKeys[i], i * 1_000L, i * 1_000L + 999, 64, chunksPtr, chunksSize, 2);
            } finally {
                Unsafe.free(chunksPtr, chunksSize, MemoryTag.NATIVE_DEFAULT);
            }
        }
        setDataRowGroupBoundaries(writerPtr, 0L, 128L);
    }

    /**
     * The fixture the absolute byte offset assertions pin, mirroring the Rust
     * {@code sample_writer}: 3 columns whose names total 17 bytes, so the name
     * section needs 7 bytes of padding, and 4 row groups, so RG_BLOCK_OFFSET
     * is already 8-aligned while RG_FIRST_KEY needs 4 bytes of padding.
     */
    private static void buildSample(long writerPtr) {
        IndexMetaFileWriter.setPayload(writerPtr, IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, 11_405);
        IndexMetaFileWriter.setPidxFooter(writerPtr, SAMPLE_PIDX_FOOTER_OFF, SAMPLE_PIDX_FOOTER_LEN);
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
                IndexMetaFileWriter.addRowGroup(
                        writerPtr, (int) spec[0], spec[2], spec[3], rows, chunksPtr, chunksSize, 3);
            } finally {
                Unsafe.free(chunksPtr, chunksSize, MemoryTag.NATIVE_DEFAULT);
            }
        }
        setDataRowGroupBoundaries(writerPtr, 0L, 500_000L, 1_000_000L);
    }

    /**
     * Three row groups holding keys 5, 900 and 12_000 over a key space of
     * 12_001: three distinct keys, and an id bound four thousand times larger.
     * A KEY_SPACE_SIZE written as the distinct-key count would make the last
     * two report absent.
     */
    private static void buildSparseKeySample(long writerPtr) {
        IndexMetaFileWriter.setPayload(
                writerPtr, IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, SPARSE_KEY_SPACE_SIZE);
        IndexMetaFileWriter.setPidxFooter(writerPtr, 16_384, 512);
        addColumn(writerPtr, "key_id", -1, TYPE_INT);
        addColumn(writerPtr, "row_id", -1, TYPE_LONG);
        final int[] keys = {5, 900, 12_000};
        for (int i = 0; i < keys.length; i++) {
            addKeyAndRowIdRowGroup(writerPtr, keys[i], 10, i * 10L, i * 10L + 9);
        }
        setDataRowGroupBoundaries(writerPtr, 0L, 30L);
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
        IndexMetaFileWriter.setPidxFooter(writerPtr, 8_192, 256);
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
                IndexMetaFileWriter.addRowGroup(
                        writerPtr, firstKeys[i], i * 64L, i * 64L + 63, 64, chunksPtr, chunksSize, 3);
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
        IndexMetaFileWriter.setPidxFooter(writerPtr, 1_024, 128);
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

    /**
     * Drives every accessor of a reader the hostile sweep managed to bind, and
     * applies the address oracle to the ones that answer with an address. The
     * index probes are the boundaries of each index space rather than a full
     * enumeration: one out of range index rejects on the first accessor that
     * takes it, so the battery below runs only for the combinations the file
     * claims are addressable.
     */
    private static void exerciseAccessors(int[] tally, String caseName, IndexMetaFileReader reader, long base, long size) {
        final long fileSize = reader.getFileSize();
        if (fileSize < IndexMetaFileReader.IM_HEADER_SIZE + IndexMetaFileReader.IM_TRAILER_SIZE || fileSize > size) {
            Assert.fail("_im hostile case [" + caseName + "] bound to a committed size outside the buffer [fileSize="
                    + fileSize + ", size=" + size + ']');
        }
        // Every address the reader answers with must land in the committed
        // file, not merely in the buffer: the bytes past IM_FILE_SIZE are not
        // part of the file the reader was bound to.
        final long hi = base + fileSize;
        Assert.assertEquals(base, reader.getAddr());
        Assert.assertTrue(reader.isOpen());
        reader.getFeatureFlags();
        reader.getIndexSectionsOffset();
        reader.getPayloadKind();
        reader.getPidxFooterLength();
        reader.getPidxFooterOffset();
        probeValue(caseName, "getPidxFileSize", reader::getPidxFileSize);

        final int columnCount = reader.getColumnCount();
        final int dataRowGroupCount = reader.getDataRowGroupCount();
        final int indexRowGroupCount = reader.getIndexRowGroupCount();
        final int keySpaceSize = reader.getKeySpaceSize();
        final int firstCoverColumn = reader.getFirstCoverColumn();
        final int[] columns = {
                -1, 0, columnCount - 1, columnCount, columnCount + 1,
                reader.getKeyIdColumn(), reader.getRowIdColumn(), Integer.MIN_VALUE, Integer.MAX_VALUE
        };
        final int[] rowGroups = {
                -1, 0, indexRowGroupCount - 1, indexRowGroupCount, Integer.MIN_VALUE, Integer.MAX_VALUE
        };
        for (int column : columns) {
            probeValue(caseName, "getColumnFixedByteLen", () -> reader.getColumnFixedByteLen(column));
            probeValue(caseName, "getColumnFlags", () -> reader.getColumnFlags(column));
            probeValue(caseName, "getColumnId", () -> reader.getColumnId(column));
            probeValue(caseName, "getColumnMaxDefLevel", () -> reader.getColumnMaxDefLevel(column));
            probeValue(caseName, "getColumnMaxRepLevel", () -> reader.getColumnMaxRepLevel(column));
            probeValue(caseName, "getColumnPhysicalType", () -> reader.getColumnPhysicalType(column));
            probeValue(caseName, "getColumnType", () -> reader.getColumnType(column));
            probeName(tally, caseName, reader, base, hi, column);
            probeColumnIndexById(caseName, reader, columnCount, column);
        }
        // The cover slot indirection: slot 0, the two FIRST_COVER_COLUMN
        // boundaries, the slot that lands exactly on COLUMN_COUNT, and beyond.
        final int[] coverSlots = {
                -1, 0, 1, firstCoverColumn, columnCount - firstCoverColumn - 1, columnCount - firstCoverColumn,
                columnCount, columnCount + 1, Integer.MIN_VALUE, Integer.MAX_VALUE
        };
        for (int slot : coverSlots) {
            probeCoverColumnIndex(caseName, reader, columnCount, slot);
        }
        for (int rowGroup : rowGroups) {
            probeValue(caseName, "getRowGroupFirstKey", () -> reader.getRowGroupFirstKey(rowGroup));
            probeValue(caseName, "getRowGroupNumRows", () -> reader.getRowGroupNumRows(rowGroup));
            probeValue(caseName, "getRowGroupRowIdMax", () -> reader.getRowGroupRowIdMax(rowGroup));
            probeValue(caseName, "getRowGroupRowIdMin", () -> reader.getRowGroupRowIdMin(rowGroup));
            for (int column : columns) {
                exerciseChunk(tally, caseName, reader, base, hi, rowGroup, column);
            }
        }
        final int[] boundaries = {-1, 0, dataRowGroupCount, dataRowGroupCount + 1, Integer.MIN_VALUE, Integer.MAX_VALUE};
        for (int boundary : boundaries) {
            probeValue(caseName, "getDataRowGroupBoundary", () -> reader.getDataRowGroupBoundary(boundary));
        }
        final int[] keys = {
                -1, 0, 1, keySpaceSize - 1, keySpaceSize, keySpaceSize + 1, Integer.MIN_VALUE, Integer.MAX_VALUE
        };
        for (int key : keys) {
            probeValue(caseName, "getRowGroupHiForKey", () -> reader.getRowGroupHiForKey(key));
            probeValue(caseName, "getRowGroupLoForKey", () -> reader.getRowGroupLoForKey(key));
            probeRowGroupRange(caseName, reader, indexRowGroupCount, key);
        }
    }

    /**
     * Drives every column chunk accessor for one row group and column, taking
     * the stat flags off the file as a caller would: only the accessors the
     * flags declare valid are called, so an assert that guards an inline stat
     * read is not tripped by the sweep itself.
     */
    private static void exerciseChunk(
            int[] tally,
            String caseName,
            IndexMetaFileReader reader,
            long base,
            long hi,
            int rowGroup,
            int column
    ) {
        final int statFlags;
        try {
            statFlags = reader.getChunkStatFlags(rowGroup, column);
        } catch (CairoException e) {
            // The chunk is not addressable at all, and every accessor below
            // would reject on the same bound. Anything other than a
            // CairoException propagates to the sweep and fails the case.
            return;
        }
        probeValue(caseName, "getChunkByteRangeStart", () -> reader.getChunkByteRangeStart(rowGroup, column));
        probeValue(caseName, "getChunkCodec", () -> reader.getChunkCodec(rowGroup, column));
        probeValue(caseName, "getChunkDistinctCount", () -> reader.getChunkDistinctCount(rowGroup, column));
        probeValue(caseName, "getChunkEncodings", () -> reader.getChunkEncodings(rowGroup, column));
        probeValue(caseName, "getChunkMaxStatLength", () -> reader.getChunkMaxStatLength(rowGroup, column));
        probeValue(caseName, "getChunkMaxStatSize", () -> reader.getChunkMaxStatSize(rowGroup, column));
        probeValue(caseName, "getChunkMinStatLength", () -> reader.getChunkMinStatLength(rowGroup, column));
        probeValue(caseName, "getChunkMinStatSize", () -> reader.getChunkMinStatSize(rowGroup, column));
        probeValue(caseName, "getChunkNullCount", () -> reader.getChunkNullCount(rowGroup, column));
        probeValue(caseName, "getChunkNumValues", () -> reader.getChunkNumValues(rowGroup, column));
        probeValue(caseName, "getChunkTotalCompressed", () -> reader.getChunkTotalCompressed(rowGroup, column));
        probeValue(caseName, "hasChunkMaxStat", () -> reader.hasChunkMaxStat(rowGroup, column) ? 1 : 0);
        probeValue(caseName, "hasChunkMinStat", () -> reader.hasChunkMinStat(rowGroup, column) ? 1 : 0);
        probeValue(caseName, "isChunkMaxStatInline", () -> reader.isChunkMaxStatInline(rowGroup, column) ? 1 : 0);
        probeValue(caseName, "isChunkMinStatInline", () -> reader.isChunkMinStatInline(rowGroup, column) ? 1 : 0);
        if ((statFlags & STAT_MIN_PRESENT) != 0) {
            if ((statFlags & STAT_MIN_INLINED) != 0) {
                probeValue(caseName, "getChunkMinStat", () -> reader.getChunkMinStat(rowGroup, column));
            } else {
                // The declared length comes off the same crafted reference, so
                // the oracle bounds the whole stat and not just its first byte.
                final int length = reader.getChunkMinStatLength(rowGroup, column);
                probeAddress(
                        tally, caseName, "getChunkMinStatAddr", base, hi, length,
                        () -> reader.getChunkMinStatAddr(rowGroup, column));
            }
        }
        if ((statFlags & STAT_MAX_PRESENT) != 0) {
            if ((statFlags & STAT_MAX_INLINED) != 0) {
                probeValue(caseName, "getChunkMaxStat", () -> reader.getChunkMaxStat(rowGroup, column));
            } else {
                final int length = reader.getChunkMaxStatLength(rowGroup, column);
                probeAddress(
                        tally, caseName, "getChunkMaxStatAddr", base, hi, length,
                        () -> reader.getChunkMaxStatAddr(rowGroup, column));
            }
        }
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

    /**
     * The address oracle: an accessor that answers with an address must answer
     * with one lying wholly inside the committed file, together with the length
     * the file declares for it. A sweep that asserted only "nothing threw"
     * would pass a reader that handed back a pointer into unrelated memory,
     * which is the failure mode that matters here.
     */
    private static void probeAddress(
            int[] tally,
            String caseName,
            String accessor,
            long base,
            long hi,
            long length,
            HostileProbe probe
    ) {
        final long addr;
        try {
            addr = probe.run();
        } catch (CairoException e) {
            return;
        }
        if (addr < base || addr > hi || length < 0 || length > hi - addr) {
            Assert.fail("_im hostile case [" + caseName + "] answered an address outside the mapping from "
                    + accessor + " [addr=" + addr + ", length=" + length + ", base=" + base + ", hi=" + hi + ']');
        }
        tally[HOSTILE_TALLY_ADDRESSES]++;
    }

    /**
     * A writer index either misses or resolves to a descriptor of this file.
     */
    private static void probeColumnIndexById(String caseName, IndexMetaFileReader reader, int columnCount, int id) {
        final int index;
        try {
            index = reader.getColumnIndexById(id);
        } catch (CairoException e) {
            return;
        }
        if (index != -1 && (index < 0 || index >= columnCount)) {
            Assert.fail("_im hostile case [" + caseName + "] resolved a writer index outside the descriptors [id="
                    + id + ", index=" + index + ", columnCount=" + columnCount + ']');
        }
    }

    /**
     * A cover slot either is refused or resolves to a descriptor of this file:
     * the caller passes what comes back straight to a column chunk accessor.
     */
    private static void probeCoverColumnIndex(String caseName, IndexMetaFileReader reader, int columnCount, int slot) {
        final int index;
        try {
            index = reader.getCoverColumnIndex(slot);
        } catch (CairoException e) {
            return;
        }
        if (index < 0 || index >= columnCount) {
            Assert.fail("_im hostile case [" + caseName + "] resolved a cover slot outside the descriptors [slot="
                    + slot + ", index=" + index + ", columnCount=" + columnCount + ']');
        }
    }

    /**
     * The name flyweight is a pair of addresses over the mapped file, so both
     * ends of it get the address oracle.
     */
    private static void probeName(int[] tally, String caseName, IndexMetaFileReader reader, long base, long hi, int column) {
        final DirectUtf8String name;
        try {
            name = reader.getColumnName(column);
        } catch (CairoException e) {
            return;
        }
        final long lo = name.ptr();
        final long end = lo + name.size();
        if (lo < base || end < lo || end > hi) {
            Assert.fail("_im hostile case [" + caseName + "] answered a column name outside the mapping [column="
                    + column + ", lo=" + lo + ", end=" + end + ", base=" + base + ", hi=" + hi + ']');
        }
        tally[HOSTILE_TALLY_ADDRESSES]++;
    }

    /**
     * A row group range that is not the absent sentinel is fed straight back
     * into the row group accessors by a caller, so it must name row groups this
     * file has.
     */
    private static void probeRowGroupRange(String caseName, IndexMetaFileReader reader, int indexRowGroupCount, int key) {
        final long range;
        try {
            range = reader.getRowGroupRangeForKey(key);
        } catch (CairoException e) {
            return;
        }
        if (range == IndexMetaFileReader.KEY_ABSENT) {
            return;
        }
        final int lo = Numbers.decodeLowInt(range);
        final int hi = Numbers.decodeHighInt(range);
        if (lo < 0 || hi < lo || hi >= indexRowGroupCount) {
            Assert.fail("_im hostile case [" + caseName + "] answered a row group range outside the file [key="
                    + key + ", lo=" + lo + ", hi=" + hi + ", indexRowGroupCount=" + indexRowGroupCount + ']');
        }
    }

    /**
     * Runs one accessor and tolerates the only exception the reader is allowed
     * to raise. Anything else - an AssertionError from a bound that only exists
     * under {@code -ea}, an unchecked exception, a JVM error - fails the sweep
     * and names the case and the accessor that produced it.
     */
    private static void probeValue(String caseName, String accessor, HostileProbe probe) {
        try {
            probe.run();
        } catch (CairoException e) {
            // The documented rejection path.
        } catch (Throwable th) {
            throw new AssertionError("_im hostile case [" + caseName + "] raised " + th + " from " + accessor, th);
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

    /**
     * Re-commits a crafted buffer so the reader reaches the check under test
     * instead of failing the checksum first: the CRC covers
     * {@code [8, IM_FILE_SIZE - 4)} and any mutation invalidates it. A crafted
     * IM_FILE_SIZE that still addresses the buffer is honoured rather than
     * repaired, so the reader validates the mutated size; one that does not is
     * left to the size bound, with the checksum written where the untouched
     * file keeps it.
     */
    private static void repairCrc(long ptr, long bufLen) {
        final long committed = Unsafe.getUnsafe().getLong(ptr);
        final boolean addressable = committed >= IndexMetaFileReader.IM_HEADER_SIZE + IndexMetaFileReader.IM_TRAILER_SIZE
                && committed <= bufLen;
        final long crcEnd = (addressable ? committed : bufLen) - IndexMetaFileReader.IM_TRAILER_SIZE;
        Unsafe.getUnsafe().putInt(ptr + crcEnd, Zip.crc32(0, ptr + 8, (int) (crcEnd - 8)));
    }

    private static void setDataRowGroupBoundaries(long writerPtr, long... boundaries) {
        final long size = (long) boundaries.length * Long.BYTES;
        final long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < boundaries.length; i++) {
                Unsafe.getUnsafe().putLong(ptr + (long) i * Long.BYTES, boundaries[i]);
            }
            IndexMetaFileWriter.setDataRowGroupBoundaries(writerPtr, ptr, size, boundaries.length);
        } finally {
            Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Runs one hostile case: binds a reader to the crafted buffer and, if it
     * binds, drives every accessor. A file the reader refuses is a pass - a
     * crafted file is allowed to be rejected - and the reader is closed on
     * every path, so a leak on the rejection path shows up as a failed memory
     * leak check rather than as nothing at all.
     */
    private static void sweepCase(int[] tally, String caseName, long ptr, long size) {
        tally[HOSTILE_TALLY_CASES]++;
        try (IndexMetaFileReader reader = new IndexMetaFileReader()) {
            try {
                reader.ofAddress(ptr, size);
            } catch (CairoException e) {
                return;
            } catch (Throwable th) {
                throw new AssertionError("_im hostile case [" + caseName + "] raised " + th + " from ofAddress", th);
            }
            tally[HOSTILE_TALLY_BOUND]++;
            try {
                exerciseAccessors(tally, caseName, reader, ptr, size);
            } catch (AssertionError e) {
                throw e;
            } catch (Throwable th) {
                // The individual probes tolerate CairoException, so one
                // reaching here escaped an accessor that was not probed.
                throw new AssertionError("_im hostile case [" + caseName + "] raised " + th, th);
            }
        }
    }

    /**
     * Every header field, including the ones the widened version 3 header
     * added - PIDX_FOOTER_OFFSET, PIDX_FOOTER_LENGTH, FIRST_COVER_COLUMN and
     * the first reserved word - set in turn to the boundary values and to the
     * offsets of this file that a crafted field is most likely to name.
     */
    private static void sweepHeaderFields(int[] tally, long dataPtr, long dataLen) {
        final long sectionsOffset = Unsafe.getUnsafe().getLong(dataPtr + 56);
        final long[] values = {
                HOSTILE_FIELD_VALUES[0], HOSTILE_FIELD_VALUES[1], HOSTILE_FIELD_VALUES[2], HOSTILE_FIELD_VALUES[3],
                HOSTILE_FIELD_VALUES[4], HOSTILE_FIELD_VALUES[5], HOSTILE_FIELD_VALUES[6], HOSTILE_FIELD_VALUES[7],
                HOSTILE_FIELD_VALUES[8],
                // Misaligned, the header itself, either side of the real
                // sections offset, and either side of the committed size.
                7L, IndexMetaFileReader.IM_HEADER_SIZE, sectionsOffset - 8, sectionsOffset, sectionsOffset + 8,
                dataLen - 4, dataLen, dataLen + 8
        };
        final long copyPtr = Unsafe.malloc(dataLen, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int field : HOSTILE_HEADER_LONG_FIELDS) {
                for (long value : values) {
                    Vect.memcpy(copyPtr, dataPtr, dataLen);
                    Unsafe.getUnsafe().putLong(copyPtr + field, value);
                    repairCrc(copyPtr, dataLen);
                    sweepCase(tally, "header u64 offset=" + field + " value=" + value, copyPtr, dataLen);
                }
            }
            for (int field : HOSTILE_HEADER_INT_FIELDS) {
                for (long value : values) {
                    Vect.memcpy(copyPtr, dataPtr, dataLen);
                    Unsafe.getUnsafe().putInt(copyPtr + field, (int) value);
                    repairCrc(copyPtr, dataLen);
                    sweepCase(tally, "header u32 offset=" + field + " value=" + (int) value, copyPtr, dataLen);
                }
            }
        } finally {
            Unsafe.free(copyPtr, dataLen, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Crafted {@code (offset << 16) | length} out-of-line stat references over
     * both stats of both blocks of the two-block fixture, so a bound taken from
     * the wrong end lets one block address the next. The raw values include
     * {@code -1}, which is every bit set in both halves.
     */
    private static void sweepOutOfLineStatReferences(int[] tally, long dataPtr, long dataLen) {
        final int[] positions = {
                TWO_BLOCK_0_OFF + TWO_BLOCK_UID_MIN_STAT, TWO_BLOCK_0_OFF + TWO_BLOCK_UID_MAX_STAT,
                TWO_BLOCK_1_OFF + TWO_BLOCK_UID_MIN_STAT, TWO_BLOCK_1_OFF + TWO_BLOCK_UID_MAX_STAT
        };
        final long[] raw = {-1L, 0L, Long.MIN_VALUE, Long.MAX_VALUE};
        final long copyPtr = Unsafe.malloc(dataLen, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int position : positions) {
                for (int offset : HOSTILE_STAT_OFFSETS) {
                    for (int length : HOSTILE_STAT_LENGTHS) {
                        final long encoded = encodeOutOfLineStat(offset, length);
                        Vect.memcpy(copyPtr, dataPtr, dataLen);
                        Unsafe.getUnsafe().putLong(copyPtr + position, encoded);
                        repairCrc(copyPtr, dataLen);
                        sweepCase(
                                tally,
                                "stat ref offset=" + position + " statOffset=" + offset + " statLength=" + length,
                                copyPtr,
                                dataLen
                        );
                    }
                }
                for (long encoded : raw) {
                    Vect.memcpy(copyPtr, dataPtr, dataLen);
                    Unsafe.getUnsafe().putLong(copyPtr + position, encoded);
                    repairCrc(copyPtr, dataLen);
                    sweepCase(tally, "stat ref offset=" + position + " encoded=" + encoded, copyPtr, dataLen);
                }
            }
        } finally {
            Unsafe.free(copyPtr, dataLen, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Every RG_BLOCK_OFFSET entry in turn, at the boundary values and at the
     * unit offsets a crafted entry would use to move a block onto the header,
     * onto the name strings or onto the index sections.
     */
    private static void sweepRowGroupBlockOffsets(int[] tally, long dataPtr, long dataLen) {
        final int columnCount = Unsafe.getUnsafe().getInt(dataPtr + 32);
        final int rowGroupCount = Unsafe.getUnsafe().getInt(dataPtr + 36);
        final long sectionsOffset = Unsafe.getUnsafe().getLong(dataPtr + 56);
        final long namesStart = IndexMetaFileReader.IM_HEADER_SIZE + (long) columnCount * 32;
        // Entries are byte offsets shifted right by 3.
        final long[] values = {
                HOSTILE_FIELD_VALUES[0], HOSTILE_FIELD_VALUES[1], HOSTILE_FIELD_VALUES[2], HOSTILE_FIELD_VALUES[3],
                HOSTILE_FIELD_VALUES[4], HOSTILE_FIELD_VALUES[5], HOSTILE_FIELD_VALUES[6], HOSTILE_FIELD_VALUES[7],
                HOSTILE_FIELD_VALUES[8],
                2L, 3L, (namesStart >> 3) - 1, namesStart >> 3, (sectionsOffset >> 3) - 1, sectionsOffset >> 3,
                (sectionsOffset >> 3) + 1, 0x1FFF_FFFFL
        };
        final long copyPtr = Unsafe.malloc(dataLen, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < rowGroupCount; i++) {
                final long entryOffset = sectionsOffset + (long) i * Integer.BYTES;
                for (long value : values) {
                    Vect.memcpy(copyPtr, dataPtr, dataLen);
                    Unsafe.getUnsafe().putInt(copyPtr + entryOffset, (int) value);
                    repairCrc(copyPtr, dataLen);
                    sweepCase(tally, "block offset entry=" + i + " value=" + (int) value, copyPtr, dataLen);
                }
            }
        } finally {
            Unsafe.free(copyPtr, dataLen, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Every byte position of the file in turn, cleared, made non-zero and set
     * to all ones. This is the family that reaches the bytes no named field
     * covers: the descriptor name pointers, the column chunks, the row-id zone
     * maps and the padding between sections.
     */
    private static void sweepSingleBytes(int[] tally, long dataPtr, long dataLen) {
        final long copyPtr = Unsafe.malloc(dataLen, MemoryTag.NATIVE_DEFAULT);
        try {
            for (long offset = 0; offset < dataLen; offset++) {
                for (int value : HOSTILE_BYTE_VALUES) {
                    Vect.memcpy(copyPtr, dataPtr, dataLen);
                    Unsafe.putByte(copyPtr + offset, (byte) value);
                    repairCrc(copyPtr, dataLen);
                    sweepCase(tally, "byte offset=" + offset + " value=" + value, copyPtr, dataLen);
                }
            }
        } finally {
            Unsafe.free(copyPtr, dataLen, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Every truncation length from 0 to the file length, each re-committed at
     * that length with the CRC repaired - the counts and INDEX_SECTIONS_OFFSET
     * left describing the whole file, which is what a torn write leaves behind.
     * Lengths below 12 have no CRC range to re-commit over and are handed to
     * the reader as they are, which is the size bound's job.
     */
    private static void sweepTruncations(int[] tally, long dataPtr, long dataLen) {
        final long copyPtr = Unsafe.malloc(dataLen, MemoryTag.NATIVE_DEFAULT);
        try {
            for (long len = 0; len <= dataLen; len++) {
                Vect.memcpy(copyPtr, dataPtr, dataLen);
                if (len >= 12) {
                    Unsafe.getUnsafe().putLong(copyPtr, len);
                    repairCrc(copyPtr, len);
                }
                sweepCase(tally, "truncation len=" + len, copyPtr, len);
            }
        } finally {
            Unsafe.free(copyPtr, dataLen, MemoryTag.NATIVE_DEFAULT);
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
        assertOpenRejected(patchOffset, value, Integer.BYTES, expectedMessage);
    }

    /**
     * The 8-byte-field form: INDEX_SECTIONS_OFFSET, FEATURE_FLAGS and a
     * descriptor's NAME_OFFSET are all u64.
     */
    private void assertOpenRejected(long patchOffset, long value, int width, String expectedMessage) {
        withPatchedBytes(IndexMetaFileReaderTest::buildSample, patchOffset, value, width, (dataPtr, dataLen) -> {
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
                        // The block's own min stat is untouched and still
                        // resolves, and to the address the reference names:
                        // this block's out-of-line region start plus the
                        // encoded offset. An address is never 0, so asserting
                        // the value is what makes the resolution testable -
                        // taking the region from the start of the block instead
                        // lands 8 + COLUMN_COUNT * 64 bytes early, inside the
                        // column chunks, and still looks like a valid address.
                        final long addr = reader.getAddr();
                        final long blockAddr = addr + ((long) Unsafe.getUnsafe().getInt(
                                addr + TWO_BLOCK_SECTIONS_OFF + (long) rowGroup * Integer.BYTES) << 3);
                        final long regionStart = blockAddr + 8 + (long) reader.getColumnCount() * 64;
                        final long minRef = Unsafe.getUnsafe().getLong(blockAddr + TWO_BLOCK_UID_MIN_STAT);
                        Assert.assertEquals(
                                regionStart + (minRef >>> 16),
                                reader.getChunkMinStatAddr(rowGroup, 2));
                    }
                }
        );
    }

    /**
     * Patches PIDX_FOOTER_OFFSET of the standard sample and asserts that the
     * file still opens -- the reader takes the footer fields as given, so the
     * rejection below is the derived size talking and not a broken header --
     * while the size derived from that offset is refused.
     */
    private void assertPidxFileSizeRejected(long footerOffset) {
        // PIDX_FOOTER_OFFSET is at 64.
        withPatchedBytes(IndexMetaFileReaderTest::buildSample, 64, footerOffset, Long.BYTES, (dataPtr, dataLen) -> {
            try (IndexMetaFileReader reader = new IndexMetaFileReader()) {
                reader.ofAddress(dataPtr, dataLen);
                Assert.assertEquals(footerOffset, reader.getPidxFooterOffset());
                try {
                    reader.getPidxFileSize();
                    Assert.fail("expected CairoException from the pidx file size bound");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "_im PIDX footer range overflows");
                }
            }
        });
    }

    /**
     * Cuts the standard sample to {@code len} bytes, re-commits it at that
     * length with the CRC repaired -- so the reader reaches the check under
     * test instead of failing the checksum first -- and asserts the reader
     * refuses to bind to the result, leaving nothing mapped.
     */
    private void assertTruncationRejected(int len, String expectedMessage) {
        withTruncatedBytes(IndexMetaFileReaderTest::buildSample, len, (dataPtr, dataLen) -> {
            try (IndexMetaFileReader reader = new IndexMetaFileReader()) {
                reader.ofAddress(dataPtr, dataLen);
                Assert.fail("expected CairoException from the truncation to " + len + " bytes");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), expectedMessage);
            }
        });
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
        withBytes(builder, IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING, 0, 1, FIRST_COVER_COLUMN, assertion);
    }

    /**
     * The form that names the synthetic column indices and cover slot 0's
     * descriptor index, for a fixture whose payload kind is not row-per-posting:
     * {@code rowIdColumn} and {@code firstCoverColumn} have no setter on the
     * Rust writer, so they can only be given at construction.
     */
    private void withBytes(
            SampleBuilder builder,
            int payloadKind,
            int keyIdColumn,
            int rowIdColumn,
            int firstCoverColumn,
            BytesAssertion assertion
    ) {
        // The Rust writer takes the payload kind, key space size, the synthetic
        // column indices and FIRST_COVER_COLUMN up front; setPayload can
        // correct the first two later.
        final long writerPtr = IndexMetaFileWriter.create(
                payloadKind, 0, keyIdColumn, rowIdColumn, firstCoverColumn);
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

    /**
     * Builds a row-per-key fixture, which has no {@code row_id} column and so
     * carries the {@code -1} ROW_ID_COLUMN sentinel, and reads it back.
     */
    private void withRowPerKeyReader(SampleBuilder builder, SampleAssertion assertion) {
        // One synthetic column and no row_id, so cover slot 0 is descriptor 1.
        withBytes(builder, IndexMetaFileWriter.PAYLOAD_ROW_PER_KEY, 0, -1, 1, (dataPtr, dataLen) -> {
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

    /**
     * Copies a sample with {@code slack} filler bytes inserted between the end
     * of DATA_RG_BOUNDARY and the CRC, IM_FILE_SIZE grown to match and the CRC
     * repaired, and hands the copy to {@code assertion}. The spec permits the
     * gap and no writer emits one, so it has to be crafted here. The Java
     * spelling of the Rust {@code with_slack}. The copy is freed on every
     * path, including the exceptional one.
     */
    private void withSlackBytes(SampleBuilder builder, int slack, BytesAssertion assertion) {
        withBytes(builder, (dataPtr, dataLen) -> {
            final long paddedLen = dataLen + slack;
            final long copyPtr = Unsafe.malloc(paddedLen, MemoryTag.NATIVE_DEFAULT);
            try {
                final long crcOffset = dataLen - 4;
                Vect.memcpy(copyPtr, dataPtr, crcOffset);
                for (int i = 0; i < slack; i++) {
                    Unsafe.putByte(copyPtr + crcOffset + i, SLACK_FILL);
                }
                // IM_FILE_SIZE is at 0 and is the committed length, so it grows
                // with the file; the CRC then covers [8, paddedLen - 4).
                Unsafe.getUnsafe().putLong(copyPtr, paddedLen);
                Unsafe.getUnsafe().putInt(
                        copyPtr + paddedLen - 4,
                        Zip.crc32(0, copyPtr + 8, (int) (paddedLen - 12))
                );
                assertion.run(copyPtr, paddedLen);
            } finally {
                Unsafe.free(copyPtr, paddedLen, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Copies the first {@code len} bytes of a sample and re-commits the copy at
     * that length: IM_FILE_SIZE becomes {@code len} and the CRC covers the
     * shorter range. The header's counts and INDEX_SECTIONS_OFFSET are left
     * alone, which is what a torn write leaves behind. The Java spelling of the
     * Rust {@code truncate_to}. The copy is freed on every path, including the
     * exceptional one.
     */
    private void withTruncatedBytes(SampleBuilder builder, int len, BytesAssertion assertion) {
        withBytes(builder, (dataPtr, dataLen) -> {
            // The CRC area starts at 8 and the trailer is 4 bytes, so anything
            // shorter than 12 has no range to re-commit over.
            Assert.assertTrue(len >= 12 && len <= dataLen);
            final long copyPtr = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                Vect.memcpy(copyPtr, dataPtr, len);
                // IM_FILE_SIZE is at 0 and is the committed length, so it
                // shrinks with the file; the CRC then covers [8, len - 4).
                Unsafe.getUnsafe().putLong(copyPtr, len);
                Unsafe.getUnsafe().putInt(copyPtr + len - 4, Zip.crc32(0, copyPtr + 8, len - 12));
                assertion.run(copyPtr, len);
            } finally {
                Unsafe.free(copyPtr, len, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @FunctionalInterface
    private interface BytesAssertion {
        void run(long dataPtr, long dataLen);
    }

    /**
     * One accessor call of the hostile sweep. Accessors answering an int, a
     * boolean or nothing widen into the long the probe ignores.
     */
    @FunctionalInterface
    private interface HostileProbe {
        long run();
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
