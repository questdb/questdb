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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Test-only decoder over a complete {@code _sortedruns} file. Reads the
 * whole file into memory (tests are small) and exposes a simple POJO view
 * of each record. Used to assert format correctness in PR-1 tests.
 */
public final class SortedRunsDecoder {

    private SortedRunsDecoder() {
    }

    public static void assertRecordCount(Path commitsFile, int expected) throws IOException {
        final List<Record> records = readAll(commitsFile);
        Assert.assertEquals("CommitStat record count", expected, records.size());
    }

    public static void assertRecordCount(Path commitsFile, long validSize, int expected) throws IOException {
        final List<Record> records = readAll(commitsFile, validSize);
        Assert.assertEquals("CommitStat record count", expected, records.size());
    }

    public static String dump(Path commitsFile) throws IOException {
        final ByteBuffer buf = ByteBuffer.wrap(Files.readAllBytes(commitsFile)).order(ByteOrder.LITTLE_ENDIAN);
        final StringBuilder sb = new StringBuilder();
        sb.append("Header { magic=0x").append(Integer.toHexString(buf.getInt(SortedRunsFormat.HEADER_OFFSET_MAGIC)))
                .append(", version=").append(buf.getShort(SortedRunsFormat.HEADER_OFFSET_VERSION) & 0xFFFF)
                .append(", flags=").append(buf.getShort(SortedRunsFormat.HEADER_OFFSET_FLAGS) & 0xFFFF)
                .append(" }\n");
        final List<Record> records = readAll(commitsFile);
        for (int i = 0; i < records.size(); i++) {
            sb.append('[').append(i).append("] ").append(records.get(i)).append('\n');
        }
        return sb.toString();
    }

    public static List<Record> readAll(Path commitsFile) throws IOException {
        return readAll(commitsFile, -1L);
    }

    public static List<Record> readAll(Path commitsFile, long validSize) throws IOException {
        final byte[] fileBytes = Files.readAllBytes(commitsFile);
        final int limit;
        if (validSize >= 0) {
            limit = (int) Math.min(validSize, fileBytes.length);
        } else {
            limit = fileBytes.length;
        }
        final byte[] bytes = limit == fileBytes.length ? fileBytes : java.util.Arrays.copyOf(fileBytes, limit);
        final ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        final List<Record> out = new ArrayList<>();
        if (bytes.length < SortedRunsFormat.HEADER_SIZE_BYTES) {
            return out;
        }
        final int magic = buf.getInt(SortedRunsFormat.HEADER_OFFSET_MAGIC);
        Assert.assertEquals("magic", SortedRunsFormat.MAGIC, magic);
        int pos = SortedRunsFormat.HEADER_SIZE_BYTES;
        while (pos + SortedRunsFormat.RECORD_HEADER_SIZE_BYTES <= bytes.length) {
            final long physRowStart = buf.getLong(pos + SortedRunsFormat.RECORD_OFFSET_PHYS_ROW_START);
            final int rowCount = buf.getInt(pos + SortedRunsFormat.RECORD_OFFSET_ROW_COUNT);
            final int flags = buf.getShort(pos + SortedRunsFormat.RECORD_OFFSET_FLAGS) & 0xFFFF;
            final long minTs = buf.getLong(pos + SortedRunsFormat.RECORD_OFFSET_MIN_TS);
            final long maxTs = buf.getLong(pos + SortedRunsFormat.RECORD_OFFSET_MAX_TS);
            final int extLength = buf.getInt(pos + SortedRunsFormat.RECORD_OFFSET_EXT_LENGTH);
            final byte[] ext = new byte[extLength];
            if (extLength > 0 && pos + SortedRunsFormat.RECORD_HEADER_SIZE_BYTES + extLength <= bytes.length) {
                System.arraycopy(bytes, pos + SortedRunsFormat.RECORD_HEADER_SIZE_BYTES, ext, 0, extLength);
            }
            out.add(new Record(pos, physRowStart, rowCount, flags, minTs, maxTs, extLength, ext));
            pos += SortedRunsFormat.RECORD_HEADER_SIZE_BYTES + extLength;
        }
        return out;
    }

    public static Record recordAt(Path commitsFile, int index) throws IOException {
        final List<Record> records = readAll(commitsFile);
        Assert.assertTrue("record index " + index + " out of range (count=" + records.size() + ")",
                index >= 0 && index < records.size());
        return records.get(index);
    }

    public static final class Record {
        public final int extLength;
        public final byte[] extensions;
        public final int flags;
        public final long maxTs;
        public final long minTs;
        public final long offsetInFile;
        public final long physRowStart;
        public final int rowCount;

        public Record(
                long offsetInFile,
                long physRowStart,
                int rowCount,
                int flags,
                long minTs,
                long maxTs,
                int extLength,
                byte[] extensions
        ) {
            this.offsetInFile = offsetInFile;
            this.physRowStart = physRowStart;
            this.rowCount = rowCount;
            this.flags = flags;
            this.minTs = minTs;
            this.maxTs = maxTs;
            this.extLength = extLength;
            this.extensions = extensions;
        }

        public boolean hasFlag(int flagBit) {
            return (flags & (1 << flagBit)) != 0;
        }

        @Override
        public String toString() {
            return "Record { offset=" + offsetInFile
                    + ", physRowStart=" + physRowStart
                    + ", rowCount=" + rowCount
                    + ", flags=0x" + Integer.toHexString(flags)
                    + ", minTs=" + minTs
                    + ", maxTs=" + maxTs
                    + ", extLength=" + extLength
                    + " }";
        }
    }
}
