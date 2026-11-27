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
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.PartitionGeometry;
import io.questdb.cairo.PartitionGeometryFile;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;

/**
 * The {@code _geometry} record codec: the dedup sort version of each piece round-trips, a record of the format
 * written before the field existed still reads with every claim unset, and the two formats keep their own
 * on-disk sizes, which the next record's offset depends on.
 */
public class PartitionGeometryFileTest extends AbstractTest {
    private static final long UNSET = PartitionGeometryFile.DEDUP_SORT_VERSION_UNSET;

    @Test
    public void testFoldedDedupSortVersion() {
        // The same version and no shared timestamp: the claim survives.
        Assert.assertEquals(7, PartitionGeometry.foldedDedupSortVersion(7, 10, 20, 7, 5, 21));
        // Two sorted runs that share a timestamp joined back to back are not sorted.
        Assert.assertEquals(UNSET, PartitionGeometry.foldedDedupSortVersion(7, 10, 20, 7, 5, 20));
        // Different versions, or one side unset.
        Assert.assertEquals(UNSET, PartitionGeometry.foldedDedupSortVersion(7, 10, 20, 8, 5, 21));
        Assert.assertEquals(UNSET, PartitionGeometry.foldedDedupSortVersion(UNSET, 10, 20, 7, 5, 21));
        Assert.assertEquals(UNSET, PartitionGeometry.foldedDedupSortVersion(7, 10, 20, UNSET, 5, 21));
        // An empty piece has no claim of its own and does not disturb the other side's.
        Assert.assertEquals(7, PartitionGeometry.foldedDedupSortVersion(7, 10, 20, UNSET, 0, 20));
        Assert.assertEquals(7, PartitionGeometry.foldedDedupSortVersion(UNSET, 0, Long.MIN_VALUE, 7, 5, 20));
        Assert.assertEquals(UNSET, PartitionGeometry.foldedDedupSortVersion(UNSET, 0, 0, UNSET, 5, 20));
    }

    @Test
    public void testLegacyRecordReadsWithUnsetVersions() throws Exception {
        final FilesFacade ff = FilesFacadeImpl.INSTANCE;
        final java.io.File dir = temp.newFolder("legacy");
        // A two-piece record in the 48-byte piece layout, checksummed as the legacy writer did.
        // writerTxn, physicalRows, liveRows, (checksum), lastWriteMicros, seqTxn
        final long[] header = {3, 100, 60, 0, 1_000, 11};
        final long[][] pieces = {{10, 20, 0, 30, 3, 500}, {25, 40, 40, 30, 3, 900}};
        final ByteBuffer record = ByteBuffer.allocate(PartitionGeometryFile.HEADER_SIZE + 2 * PartitionGeometryFile.LEGACY_PIECE_SIZE)
                .order(ByteOrder.LITTLE_ENDIAN);
        record.putInt(PartitionGeometryFile.LEGACY_MAGIC);
        record.putInt(pieces.length);
        record.putLong(header[0]);   // writer txn
        record.putLong(header[1]);   // physical rows
        record.putLong(header[2]);   // live rows
        record.putLong(0);           // checksum, patched below
        record.putLong(header[4]);   // last write micros
        record.putLong(header[5]);   // seq txn
        for (long[] piece : pieces) {
            for (long field : piece) {
                record.putLong(field);
            }
        }
        long h = 0xcbf29ce484222325L;
        h = mix(h, PartitionGeometryFile.LEGACY_MAGIC);
        h = mix(h, pieces.length);
        h = mix(h, header[0]);
        h = mix(h, header[1]);
        h = mix(h, header[2]);
        h = mix(h, header[4]);
        h = mix(h, header[5]);
        for (long[] piece : pieces) {
            for (long field : piece) {
                h = mix(h, field);
            }
        }
        record.putLong(PartitionGeometryFile.HEADER_OFFSET_CHECKSUM_64, h);
        Files.write(new java.io.File(dir, "_geometry.0").toPath(), record.array());

        try (PartitionGeometryFile file = new PartitionGeometryFile(MemoryTag.NATIVE_DEFAULT); Path path = new Path()) {
            path.of(dir.getAbsolutePath());
            Assert.assertEquals(header[0], file.readWriterTxn(ff, path, 0, 0));
            file.read(ff, path, 0, 0);
            Assert.assertEquals(2, file.getPieceCount());
            Assert.assertEquals(header[0], file.getWriterTxn());
            Assert.assertEquals(header[1], file.getPhysicalRows());
            Assert.assertEquals(header[2], file.getLiveRows());
            Assert.assertEquals(header[4], file.getLastWriteMicros());
            Assert.assertEquals(header[5], file.getSeqTxn());
            for (int p = 0; p < pieces.length; p++) {
                Assert.assertEquals(pieces[p][0], file.getPieceTimestampLo(p));
                Assert.assertEquals(pieces[p][1], file.getPieceTimestampHi(p));
                Assert.assertEquals(pieces[p][2], file.getPieceRowOffset(p));
                Assert.assertEquals(pieces[p][3], file.getPieceRowCount(p));
                Assert.assertEquals(pieces[p][4], file.getPieceWriterTxn(p));
                Assert.assertEquals(pieces[p][5], file.getPieceLastWriteMicros(p));
                Assert.assertEquals(UNSET, file.getPieceDedupSortVersion(p));
            }
            // The legacy record's own size, so the next record appends right after it.
            Assert.assertEquals(PartitionGeometryFile.HEADER_SIZE + 2L * PartitionGeometryFile.LEGACY_PIECE_SIZE, file.getReadRecordSize());
            // A file with one legacy one-piece record is the shortest that still occupies its generation.
            Assert.assertEquals(PartitionGeometryFile.HEADER_SIZE + PartitionGeometryFile.LEGACY_PIECE_SIZE, PartitionGeometryFile.MIN_RECORD_SIZE);
            Assert.assertTrue(PartitionGeometryFile.MIN_RECORD_SIZE < PartitionGeometryFile.recordSize(1));
            Assert.assertEquals(PartitionGeometryFile.recordSize(2), file.getRecordSize());

            // A corrupt legacy piece fails the legacy checksum.
            final ByteBuffer corrupt = ByteBuffer.wrap(record.array().clone()).order(ByteOrder.LITTLE_ENDIAN);
            corrupt.putLong(PartitionGeometryFile.HEADER_SIZE, 11);
            Files.write(new java.io.File(dir, "_geometry.1").toPath(), corrupt.array());
            try {
                file.read(ff, path, 1, 0);
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "partition geometry checksum mismatch");
            }
        }
    }

    @Test
    public void testRecordRoundTripsDedupSortVersions() throws Exception {
        final FilesFacade ff = FilesFacadeImpl.INSTANCE;
        final java.io.File dir = temp.newFolder("current");
        try (PartitionGeometryFile file = new PartitionGeometryFile(MemoryTag.NATIVE_DEFAULT); Path path = new Path()) {
            path.of(dir.getAbsolutePath());
            file.beginRecord(5, 17, 3);
            file.addPiece(10, 20, 0, 30, 5, 100, 7);
            file.addPiece(25, 40, 40, 30, 5, 200, UNSET);
            file.addPiece(50, 60, 70, 10, 5, 300, 0);
            file.setPhysicalRows(80);
            file.setLiveRows(70);
            file.setLastWriteMicros(400);
            final long size = file.append(ff, path, 2, 0, CommitMode.NOSYNC);
            Assert.assertEquals(PartitionGeometryFile.HEADER_SIZE + 3L * PartitionGeometryFile.PIECE_SIZE, size);
            Assert.assertEquals(size, file.getRecordSize());
            // A second record appends past the first.
            file.beginRecord(6, 18, 1);
            file.addPiece(10, 60, 0, 70, 6, 500, 9);
            final long second = file.append(ff, path, 2, size, CommitMode.NOSYNC);

            file.clear();
            file.read(ff, path, 2, 0);
            Assert.assertEquals(3, file.getPieceCount());
            Assert.assertEquals(17, file.getSeqTxn());
            Assert.assertEquals(80, file.getPhysicalRows());
            Assert.assertEquals(70, file.getLiveRows());
            Assert.assertEquals(400, file.getLastWriteMicros());
            Assert.assertEquals(7, file.getPieceDedupSortVersion(0));
            Assert.assertEquals(UNSET, file.getPieceDedupSortVersion(1));
            Assert.assertEquals(0, file.getPieceDedupSortVersion(2));
            Assert.assertEquals(25, file.getPieceTimestampLo(1));
            Assert.assertEquals(200, file.getPieceLastWriteMicros(1));
            Assert.assertEquals(size, file.getReadRecordSize());

            file.read(ff, path, 2, size);
            Assert.assertEquals(1, file.getPieceCount());
            Assert.assertEquals(9, file.getPieceDedupSortVersion(0));
            Assert.assertEquals(second, file.getReadRecordSize());

            // The version is checksummed: flipping it fails the read.
            final byte[] bytes = Files.readAllBytes(new java.io.File(dir, "_geometry.2").toPath());
            final ByteBuffer corrupt = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
            corrupt.putLong(PartitionGeometryFile.HEADER_SIZE + PartitionGeometryFile.PIECE_OFFSET_DEDUP_SORT_VERSION_64, 8);
            Files.write(new java.io.File(dir, "_geometry.3").toPath(), bytes);
            try {
                file.read(ff, path, 3, 0);
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "partition geometry checksum mismatch");
            }
        }
    }

    private static long mix(long h, long v) {
        h ^= v;
        h *= 0x100000001b3L;
        return h;
    }
}
