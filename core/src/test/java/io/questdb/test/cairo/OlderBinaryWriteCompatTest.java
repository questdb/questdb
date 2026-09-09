/*+*****************************************************************************
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

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.cairo.wal.seq.TableTransactionLogV2;
import io.questdb.cairo.wal.seq.TxnLogCrcSidecar;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * Rolling downgrade: a table this binary created, that an OLDER QuestDB then wrote to, must still open
 * when this binary reads it again. The sequence is routine in the field -- upgrade, hit an unrelated
 * problem, roll back, keep ingesting, roll forward.
 * <p>
 * This binary adds checksums to files an older one does not know about. Where such a file also records a
 * promise that "a checksum is guaranteed from here on", the older binary cannot see the promise and cannot
 * withdraw it, so every record it writes reads back as damaged. That is the class of fault these tests
 * exist to catch.
 * <p>
 * <b>How an older binary is reproduced.</b> Its commit path is byte-for-byte this one minus the checksum
 * store, so a commit is made here and the checksum bytes are then restored to what they held beforehand.
 * The reasoning per artifact lives on the helper that does it -- see
 * {@link TxnCorruptionUtils#writeBodyChecksumSlots}. This is a reproduction, not the real binary: it holds
 * only while that helper's description of the older write path stays true.
 */
public class OlderBinaryWriteCompatTest extends AbstractCairoTest {

    private static final long CHECKSUM_CAPABILITY_MAGIC = 0x54584E434B533031L; // TXNCKS01, TableTransactionLogV2
    private static final int SEQ_PART_TXN_COUNT = 16;
    private static final String TABLE = "dg_txn";

    /**
     * {@code _txn} carries a body checksum plus a capability marker naming the txn from which a checksum is
     * guaranteed. An older binary commits past that watermark without writing checksums, and leaves the
     * marker standing.
     * <p>
     * Both A and B areas are committed the older way, because a single old commit still leaves the other
     * area checksummed and the A/B fallback would mask the fault.
     */
    @Test
    public void testTxnWrittenByOlderBinaryStillOpens() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE " + TABLE + " (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO " + TABLE + " VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();

            // Precondition: this binary has written a checksum, so the file is in the state a rollback would
            // actually find it. Without this the test would pass on a file that never had one.
            Assert.assertTrue(
                    "precondition: a body checksum must be written before the downgrade",
                    TxnCorruptionUtils.readBodyChecksumSlotA(engine, TABLE) != 0
                            || TxnCorruptionUtils.readBodyChecksumSlotB(engine, TABLE) != 0
            );

            commitAsOlderBinary("INSERT INTO " + TABLE + " VALUES ('2024-01-02T00:00:00.000000Z', 2)");
            commitAsOlderBinary("INSERT INTO " + TABLE + " VALUES ('2024-01-03T00:00:00.000000Z', 3)");

            // Back on this binary: the table must open and every row must be there.
            TxnCorruptionUtils.forceReload(engine, TABLE);
            assertQuery("SELECT count() FROM " + TABLE)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n3\n");
        });
    }

    /**
     * Non-vacuity control for {@link #testTxnWrittenByOlderBinaryStillOpens}. Runs the identical sequence --
     * same writer releases, same slot reads, same slot writes -- but puts the CURRENT checksums back rather
     * than the previous ones, so the poke is a faithful no-op and the commits stay this binary's own.
     * <p>
     * If this fails, the harness damages the file on its own and the sibling test proves nothing about
     * downgrade.
     */
    @Test
    public void testHarnessAloneDoesNotDamageTheFile() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE " + TABLE + " (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO " + TABLE + " VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();

            commitRewritingSlotsWithTheirOwnValues("INSERT INTO " + TABLE + " VALUES ('2024-01-02T00:00:00.000000Z', 2)");
            commitRewritingSlotsWithTheirOwnValues("INSERT INTO " + TABLE + " VALUES ('2024-01-03T00:00:00.000000Z', 3)");

            TxnCorruptionUtils.forceReload(engine, TABLE);
            assertQuery("SELECT count() FROM " + TABLE)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n3\n");
        });
    }

    /**
     * A WAL segment whose {@code _event.c} sidecar is missing while {@code _event} still carries the
     * feature bit. Not strictly a downgrade -- segments are per-segment and immutable, so an older binary
     * writes its own sidecar-less segments -- but the same state arrives whenever a copy of the table
     * loses a file the header still promises: a backup that skipped it, or replication that never shipped
     * it, since the enterprise uploader enumerates segment files from a hardcoded list.
     */
    @Test
    public void testWalSegmentMissingItsEventChecksumSidecarStillApplies() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE dg_evt (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO dg_evt VALUES ('2024-01-01T00:00:00.000000Z', 1)");

            // Not drained yet, so the segment is still unapplied when its sidecar goes missing.
            final TableToken tt = engine.verifyTableName("dg_evt");
            engine.releaseInactive();
            Files.delete(findWalFile(tt.getDirName(), WalUtils.EVENT_CHECKSUM_FILE_NAME));

            drainWalQueue();
            Assert.assertFalse(
                    "a segment missing its event checksum sidecar must not suspend the table",
                    engine.getTableSequencerAPI().isSuspended(tt)
            );
            assertQuery("SELECT count() FROM dg_evt")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n1\n");
        });
    }

    /**
     * The V1 sequencer log keeps its record checksums in a separate {@code _txnlog.c}. An older binary
     * appends to {@code _txnlog} and leaves that sidecar at its old length, so the newest records have no
     * entry behind them.
     * <p>
     * Truncating the sidecar to its header reproduces that state for every record at once. V1 is the
     * default sequencer format, so this is the common case.
     */
    @Test
    public void testV1SequencerLogWithoutSidecarEntriesStillOpens() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE dg_v1 (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO dg_v1 VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            execute("INSERT INTO dg_v1 VALUES ('2024-01-02T00:00:00.000000Z', 2)");
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("dg_v1");
            engine.releaseInactive();
            engine.getTableSequencerAPI().releaseAll();

            final Path sidecar = Paths.get(
                    engine.getConfiguration().getDbRoot().toString(),
                    tt.getDirName(),
                    WalUtils.SEQ_DIR,
                    WalUtils.TXNLOG_CRC_FILE_NAME
            );
            Assert.assertTrue("precondition: V1 must have written a sidecar", Files.exists(sidecar));
            try (RandomAccessFile raf = new RandomAccessFile(sidecar.toFile(), "rw")) {
                raf.setLength(TxnLogCrcSidecar.BODY_OFFSET);
            }

            execute("INSERT INTO dg_v1 VALUES ('2024-01-03T00:00:00.000000Z', 3)");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
            assertQuery("SELECT count() FROM dg_v1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n3\n");
        });
    }

    /**
     * {@code _meta} carries a body checksum gated on the format's minor version. An older binary rewrites
     * {@code _meta} at a lower minor version and never writes a checksum, so the gate should close and the
     * stale checksum bytes should be ignored rather than read as damage.
     * <p>
     * Poking the minor version down, while keeping the low short that validates it, is how the sibling
     * upgrade tests simulate an older writer.
     */
    @Test
    public void testMetaAtOlderMinorVersionIgnoresItsStaleChecksum() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE dg_meta (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO dg_meta VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("dg_meta");
            engine.releaseInactive();
            final Path meta = Paths.get(
                    engine.getConfiguration().getDbRoot().toString(),
                    tt.getDirName(),
                    TableUtils.META_FILE_NAME
            );
            downgradeMetaMinorVersion(meta, TableUtils.META_FORMAT_MINOR_VERSION_ENROLLED_COMMIT_MODE);

            engine.releaseInactive();
            assertQuery("SELECT count() FROM dg_meta")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n1\n");
        });
    }

    /**
     * The V2 sequencer log keeps each record's checksum in a reserved slot inside the record itself. An
     * older binary writes a literal zero there and never touches the header's capability magic, so records
     * it appends past the recorded watermark carry no checksum while the file still promises one.
     */
    @Test
    public void testV2SequencerLogWrittenByOlderBinaryStillOpens() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, SEQ_PART_TXN_COUNT);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE dg_v2 (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO dg_v2 VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("dg_v2");
            final Path part = Paths.get(
                    engine.getConfiguration().getDbRoot().toString(),
                    tt.getDirName(),
                    WalUtils.SEQ_DIR,
                    WalUtils.TXNLOG_PARTS_DIR,
                    "0"
            );

            // Genuine commits, then their checksum slots zeroed -- exactly what the older binary's
            // addEntry() leaves behind, since it writes putLong(0L) into that slot.
            // Deliberately NOT drained: the records must still be unapplied when this binary reads them,
            // which is the case that matters -- an older binary that wrote and then stopped, leaving its
            // tail for the newer binary to apply. Records it already applied are never re-read.
            execute("INSERT INTO dg_v2 VALUES ('2024-01-02T00:00:00.000000Z', 2)");
            execute("INSERT INTO dg_v2 VALUES ('2024-01-03T00:00:00.000000Z', 3)");

            engine.releaseInactive();
            engine.getTableSequencerAPI().releaseAll();

            // Preconditions, without which this test would pass without ever reaching the verifier:
            // the capability must be stamped, and the records being zeroed must sit at or above the
            // watermark that makes a missing checksum count as damage.
            final Path log = Paths.get(
                    engine.getConfiguration().getDbRoot().toString(),
                    tt.getDirName(),
                    WalUtils.SEQ_DIR,
                    WalUtils.TXNLOG_FILE_NAME
            );
            final long magicOffset = TableTransactionLogFile.HEADER_SEQ_PART_SIZE_32 + Integer.BYTES;
            Assert.assertEquals(
                    "precondition: the V2 capability magic must be stamped",
                    CHECKSUM_CAPABILITY_MAGIC,
                    readLongAt(log, magicOffset)
            );
            final long watermark = readLongAt(log, magicOffset + Long.BYTES);
            Assert.assertTrue(
                    "precondition: the zeroed records must be at or above the capability watermark [watermark="
                            + watermark + ']',
                    watermark <= 2
            );

            zeroV2ChecksumSlotsFrom(part, 1);

            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
            assertQuery("SELECT count() FROM dg_v2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n3\n");
        });
    }

    /**
     * {@code _cv} appends a 16-byte magic+checksum trailer past each area. An older binary reserves no room
     * for it and writes none, so the area simply ends where its data ends.
     * <p>
     * Truncating the file to the end of the live area reproduces that. This covers the ordinary downgrade
     * shape only; the narrower case where an older binary's area happens to end exactly on a magic left by
     * an earlier commit needs that binary's placement arithmetic and is not simulated here.
     */
    @Test
    public void testCvWithoutItsTrailerStillOpens() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE dg_cv (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO dg_cv VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            execute("ALTER TABLE dg_cv ADD COLUMN s SYMBOL");
            execute("INSERT INTO dg_cv VALUES ('2024-01-02T00:00:00.000000Z', 2, 'a')");
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("dg_cv");
            engine.releaseInactive();
            final Path cv = Paths.get(
                    engine.getConfiguration().getDbRoot().toString(),
                    tt.getDirName(),
                    TableUtils.COLUMN_VERSION_FILE_NAME
            );
            truncateCvToEndOfLiveArea(cv);

            engine.releaseInactive();
            assertQuery("SELECT count() FROM dg_cv")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n2\n");
        });
    }

    private static long readLongAt(Path file, long offset) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            final byte[] buf = new byte[Long.BYTES];
            raf.seek(offset);
            raf.readFully(buf);
            return ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getLong();
        }
    }

    /**
     * Zeroes the reserved checksum slot of every V2 record from {@code fromRecord} onward, leaving the
     * header's capability magic and watermark standing.
     */
    private static void zeroV2ChecksumSlotsFrom(Path part, int fromRecord) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(part.toFile(), "rw")) {
            final long records = raf.length() / TableTransactionLogV2.RECORD_SIZE;
            final byte[] zeros = new byte[Long.BYTES];
            for (long i = fromRecord; i < records; i++) {
                raf.seek(i * TableTransactionLogV2.RECORD_SIZE + TableTransactionLogV2.RESERVED_OFFSET);
                raf.write(zeros);
            }
        }
    }

    /**
     * Cuts {@code _cv} back to the last byte of its version-selected area, removing that area's trailer the
     * way a binary that never wrote one would leave the file.
     */
    private static void truncateCvToEndOfLiveArea(Path cv) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(cv.toFile(), "rw")) {
            final byte[] header = new byte[ColumnVersionReader.HEADER_SIZE];
            raf.seek(0);
            raf.readFully(header);
            final ByteBuffer bb = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN);
            final long version = bb.getLong(ColumnVersionReader.OFFSET_VERSION_64);
            final boolean isA = (version & 1L) == 0L;
            final long offset = bb.getLong(isA ? ColumnVersionReader.OFFSET_OFFSET_A_64 : ColumnVersionReader.OFFSET_OFFSET_B_64);
            final long size = bb.getLong(isA ? ColumnVersionReader.OFFSET_SIZE_A_64 : ColumnVersionReader.OFFSET_SIZE_B_64);
            Assert.assertTrue("precondition: the live _cv area must carry a trailer", raf.length() >= offset + size + 16);
            raf.setLength(offset + size);
        }
    }

    /**
     * Lowers {@code _meta}'s format minor version (the high short) while keeping the low short that
     * validates it, so the file reads as one an older binary wrote rather than as a corrupt one.
     */
    private static void downgradeMetaMinorVersion(Path meta, short target) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(meta.toFile(), "rw")) {
            final byte[] buf = new byte[Integer.BYTES];
            raf.seek(TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION);
            raf.readFully(buf);
            final int current = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getInt();
            Assert.assertEquals(
                    "precondition: _meta must carry the body-checksum minor version before it is downgraded",
                    TableUtils.META_FORMAT_MINOR_VERSION_BODY_CHECKSUM,
                    Numbers.decodeHighShort(current)
            );
            final int downgraded = Numbers.encodeLowHighShorts(Numbers.decodeLowShort(current), target);
            raf.seek(TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION);
            raf.write(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(downgraded).array());
        }
    }

    private static Path findWalFile(CharSequence tableDirName, String name) throws Exception {
        Path tableDir = Paths.get(engine.getConfiguration().getDbRoot().toString(), tableDirName.toString());
        try (Stream<Path> s = Files.walk(tableDir)) {
            return s.filter(p -> p.getFileName().toString().equals(name))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("no " + name + " under " + tableDir));
        }
    }

    /**
     * Commits {@code sql} the way an older QuestDB would: the commit itself is genuine, then the two
     * body-checksum slots go back to the values they held before it, since that binary never writes them.
     * Writers are released around the poke so the edit lands on the file rather than under a live mapping.
     */
    private void commitAsOlderBinary(String sql) throws Exception {
        engine.releaseInactive();
        final long slotA = TxnCorruptionUtils.readBodyChecksumSlotA(engine, TABLE);
        final long slotB = TxnCorruptionUtils.readBodyChecksumSlotB(engine, TABLE);
        final int stampA = TxnCorruptionUtils.readChecksumStampA(engine, TABLE);
        final int stampB = TxnCorruptionUtils.readChecksumStampB(engine, TABLE);
        execute(sql);
        drainWalQueue();
        engine.releaseInactive();
        TxnCorruptionUtils.writeBodyChecksumSlots(engine, TABLE, slotA, slotB, stampA, stampB);
    }

    /**
     * The control's commit: identical machinery to {@link #commitAsOlderBinary}, except the slots are read
     * AFTER the commit and written straight back, which cannot change a byte.
     */
    private void commitRewritingSlotsWithTheirOwnValues(String sql) throws Exception {
        engine.releaseInactive();
        TxnCorruptionUtils.readBodyChecksumSlotA(engine, TABLE);
        TxnCorruptionUtils.readBodyChecksumSlotB(engine, TABLE);
        execute(sql);
        drainWalQueue();
        engine.releaseInactive();
        TxnCorruptionUtils.writeBodyChecksumSlots(
                engine,
                TABLE,
                TxnCorruptionUtils.readBodyChecksumSlotA(engine, TABLE),
                TxnCorruptionUtils.readBodyChecksumSlotB(engine, TABLE),
                TxnCorruptionUtils.readChecksumStampA(engine, TABLE),
                TxnCorruptionUtils.readChecksumStampB(engine, TABLE)
        );
    }
}
