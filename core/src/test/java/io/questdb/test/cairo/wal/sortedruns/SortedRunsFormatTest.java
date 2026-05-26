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

package io.questdb.test.cairo.wal.sortedruns;

import io.questdb.cairo.wal.sortedruns.SortedRunsFormat;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Golden-byte tests pinning the on-disk layout of <code>_sortedruns</code>.
 * If a layout constant in {@link SortedRunsFormat} changes, these tests
 * fail, alerting the author that the on-disk format is being changed.
 */
public class SortedRunsFormatTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testEmptyFileHeaderBytes() throws IOException {
        final byte[] actual = new byte[SortedRunsFormat.HEADER_SIZE_BYTES];
        writeHeader(actual, 0);

        final byte[] expected = new byte[SortedRunsFormat.HEADER_SIZE_BYTES];
        // magic "QTCS" little-endian = 0x53435451
        expected[0] = 'Q';
        expected[1] = 'T';
        expected[2] = 'C';
        expected[3] = 'S';
        // version = 1
        expected[4] = 0x01;
        expected[5] = 0x00;
        // flags = 0 (positions 6-7); remaining bytes 8..63 zero

        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void testRecordRoundTripsViaDecoder() throws IOException {
        final long minTs = 1_000_000L;
        final long maxTs = 1_500_000L;
        final long physRowStart = 0L;
        final int rowCount = 42;
        final int flags = 1 << SortedRunsFormat.FLAG_SORTED_BY_TS;

        final byte[] bytes = new byte[SortedRunsFormat.HEADER_SIZE_BYTES + SortedRunsFormat.RECORD_HEADER_SIZE_BYTES];
        writeHeader(bytes, 0);
        writeRecord(bytes, SortedRunsFormat.HEADER_SIZE_BYTES, physRowStart, rowCount, flags, minTs, maxTs, 0);

        final Path file = folder.newFile("_sortedruns").toPath();
        Files.write(file, bytes);

        final List<SortedRunsDecoder.Record> records = SortedRunsDecoder.readAll(file);
        Assert.assertEquals(1, records.size());
        final SortedRunsDecoder.Record r = records.get(0);
        Assert.assertEquals(physRowStart, r.physRowStart);
        Assert.assertEquals(rowCount, r.rowCount);
        Assert.assertEquals(flags, r.flags);
        Assert.assertEquals(minTs, r.minTs);
        Assert.assertEquals(maxTs, r.maxTs);
        Assert.assertEquals(0, r.extLength);
        Assert.assertTrue(r.hasFlag(SortedRunsFormat.FLAG_SORTED_BY_TS));
        Assert.assertFalse(r.hasFlag(SortedRunsFormat.FLAG_HAS_EXTENSIONS));
        Assert.assertEquals(SortedRunsFormat.HEADER_SIZE_BYTES, r.offsetInFile);
    }

    @Test
    public void testSingleRecordGoldenBytes() {
        // Pin the byte layout for one fully-populated record (no extensions).
        // physRowStart=0x0102030405060708, rowCount=0x11121314, flags=0x2122,
        // minTs=0x3132333435363738, maxTs=0x4142434445464748, extLength=0.
        final byte[] actual = new byte[SortedRunsFormat.RECORD_HEADER_SIZE_BYTES];
        writeRecord(
                actual,
                0,
                0x0102030405060708L,
                0x11121314,
                0x2122,
                0x3132333435363738L,
                0x4142434445464748L,
                0
        );

        // Expected (little-endian per-field; reserved u16 at [14..15] = 0):
        final byte[] expected = new byte[]{
                // physRowStart i64 [0..7]
                0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
                // rowCount u32 [8..11]
                0x14, 0x13, 0x12, 0x11,
                // flags u16 [12..13]
                0x22, 0x21,
                // reserved u16 [14..15]
                0x00, 0x00,
                // minTs i64 [16..23]
                0x38, 0x37, 0x36, 0x35, 0x34, 0x33, 0x32, 0x31,
                // maxTs i64 [24..31]
                0x48, 0x47, 0x46, 0x45, 0x44, 0x43, 0x42, 0x41,
                // extLength u32 [32..35]
                0x00, 0x00, 0x00, 0x00
        };
        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void testThreeRecordFileRoundTrips() throws IOException {
        final int count = 3;
        final byte[] bytes = new byte[SortedRunsFormat.HEADER_SIZE_BYTES + count * SortedRunsFormat.RECORD_HEADER_SIZE_BYTES];
        writeHeader(bytes, 0);

        int pos = SortedRunsFormat.HEADER_SIZE_BYTES;
        for (int i = 0; i < count; i++) {
            writeRecord(
                    bytes,
                    pos,
                    i * 100L,        // physRowStart
                    100,             // rowCount
                    1 << SortedRunsFormat.FLAG_SORTED_BY_TS,
                    1_000_000L + i * 100_000L,    // minTs
                    1_000_000L + i * 100_000L + 50_000L,  // maxTs
                    0
            );
            pos += SortedRunsFormat.RECORD_HEADER_SIZE_BYTES;
        }

        final Path file = folder.newFile("_sortedruns").toPath();
        Files.write(file, bytes);

        SortedRunsDecoder.assertRecordCount(file, count);
        final List<SortedRunsDecoder.Record> records = SortedRunsDecoder.readAll(file);
        for (int i = 0; i < count; i++) {
            final SortedRunsDecoder.Record r = records.get(i);
            Assert.assertEquals("phys start", i * 100L, r.physRowStart);
            Assert.assertEquals("row count", 100, r.rowCount);
            Assert.assertTrue(r.hasFlag(SortedRunsFormat.FLAG_SORTED_BY_TS));
            Assert.assertEquals(1_000_000L + i * 100_000L, r.minTs);
            Assert.assertEquals(1_000_000L + i * 100_000L + 50_000L, r.maxTs);
            Assert.assertEquals(0, r.extLength);
        }
    }

    private static void writeHeader(byte[] dst, int offset) {
        final ByteBuffer buf = ByteBuffer.wrap(dst).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(offset + SortedRunsFormat.HEADER_OFFSET_MAGIC, SortedRunsFormat.MAGIC);
        buf.putShort(offset + SortedRunsFormat.HEADER_OFFSET_VERSION, (short) SortedRunsFormat.VERSION_1);
        buf.putShort(offset + SortedRunsFormat.HEADER_OFFSET_FLAGS, (short) 0);
    }

    private static void writeRecord(
            byte[] dst,
            int offset,
            long physRowStart,
            int rowCount,
            int flags,
            long minTs,
            long maxTs,
            int extLength
    ) {
        final ByteBuffer buf = ByteBuffer.wrap(dst).order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(offset + SortedRunsFormat.RECORD_OFFSET_PHYS_ROW_START, physRowStart);
        buf.putInt(offset + SortedRunsFormat.RECORD_OFFSET_ROW_COUNT, rowCount);
        buf.putShort(offset + SortedRunsFormat.RECORD_OFFSET_FLAGS, (short) flags);
        buf.putShort(offset + 14, (short) 0); // reserved
        buf.putLong(offset + SortedRunsFormat.RECORD_OFFSET_MIN_TS, minTs);
        buf.putLong(offset + SortedRunsFormat.RECORD_OFFSET_MAX_TS, maxTs);
        buf.putInt(offset + SortedRunsFormat.RECORD_OFFSET_EXT_LENGTH, extLength);
    }
}
