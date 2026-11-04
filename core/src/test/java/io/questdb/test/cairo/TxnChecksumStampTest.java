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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SymbolCountProvider;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

/**
 * A bare {@code stored == 0} sentinel cannot tell a legacy record (written before the checksum existed)
 * from one whose checksum slot a torn page write zeroed, so a torn {@code _txn} would be served as healthy.
 * The stamp beside the checksum settles it: it names the txn the checksum was computed for, so a slot that
 * names this record was written for it and its absence is tearing, while a slot naming another record -- or
 * none at all -- says nothing about this one.
 * <p>
 * The stamp is what a file-level "guaranteed from here on" watermark cannot be: withdrawable. A binary that
 * predates the checksum overwrites a record's body in place and leaves the previous occupant's stamp
 * behind, which no longer names the new record, so the file still opens.
 */
public class TxnChecksumStampTest extends AbstractCairoTest {

    /**
     * A released binary never wrote the gap the stamp sits in, and its allocator places an area over the bytes
     * of older, larger records, so a released record's gap can hold a stale count. A count equal to the
     * record's own txn must not read as a stamp: the stale bytes beside it would condemn a healthy table.
     */
    @Test
    public void testReleasedRecordWithStaleGapBytesLoadsUnverified() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table txn_released_gap (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into txn_released_gap values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            execute("insert into txn_released_gap values ('2024-01-02T00:00:00.000000Z', 2)");
            drainWalQueue();
            engine.releaseInactive();

            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            try (Path path = new Path()) {
                txnPath(path, "txn_released_gap");
                for (long areaOffsetField : new long[]{TableUtils.TX_BASE_OFFSET_A_32, TableUtils.TX_BASE_OFFSET_B_32}) {
                    final long areaOffset = RawFileAccess.peekInt(ff, path.$(), areaOffsetField);
                    final long areaTxn = RawFileAccess.peekLong(ff, path.$(), areaOffset + TableUtils.TX_OFFSET_TXN_64);
                    RawFileAccess.pokeLong(ff, path.$(), areaOffset + TableUtils.TX_OFFSET_BODY_CHECKSUM_64, 42);
                    RawFileAccess.pokeInt(ff, path.$(), areaOffset + TableUtils.TX_OFFSET_BODY_CHECKSUM_STAMP_32, (int) areaTxn);
                }
            }

            TxnCorruptionUtils.forceReload(engine, "txn_released_gap");
            execute("insert into txn_released_gap values ('2024-01-03T00:00:00.000000Z', 3)");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("txn_released_gap")));
            assertQuery("select count() from txn_released_gap")
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n3\n");
        });
    }

    /**
     * Releases before 10.1.0 never write the gap at offset 116, and their allocator moves an area 16 bytes down
     * over an older record, so the low word of that record's unset lagMinTimestamp (Long.MAX_VALUE) reads there
     * as -1. A record without a stamp is not torn by what its gap holds: it must load as the live record,
     * unverified, and take writes.
     */
    @Test
    public void testReleasedRecordWithNegativeGeometrySlotLoads() throws Exception {
        assertMemoryLeak(() ->
                assertReleasedWalTableServesAndTakesWrites("txn_released_neg_live", Long.MAX_VALUE, 0L)
        );
    }

    /**
     * 10.1.0 stores geometryVersion at offset 116 and copies the value it loaded into every record it writes, so
     * a table it wrote after loading a negative stale slot carries that value in both areas. With no intact
     * area to fall back to, reading it as tearing fails every read with a checksum mismatch in both areas.
     */
    @Test
    public void testReleasedRecordsWithNegativeGeometrySlotInBothAreasLoad() throws Exception {
        assertMemoryLeak(() ->
                assertReleasedWalTableServesAndTakesWrites("txn_released_neg_both", Long.MAX_VALUE, Long.MAX_VALUE)
        );
    }

    @Test
    public void testReleasedNonWalRecordWithNegativeGeometrySlotTakesInsert() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE txn_released_neg_bypass (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO txn_released_neg_bypass VALUES (1, '2024-01-01T00:00:00.000000Z')");
            execute("INSERT INTO txn_released_neg_bypass VALUES (2, '2024-01-02T00:00:00.000000Z')");
            engine.releaseInactive();
            TxnCorruptionUtils.makeAreasLookReleased(engine, "txn_released_neg_bypass", Long.MAX_VALUE, 0L);
            Assert.assertTrue(
                    "precondition: the live record's legacy slot must be negative",
                    TxnCorruptionUtils.readLiveAreaLegacyGeometrySlot(engine, "txn_released_neg_bypass") < 0
            );

            // A non-WAL insert opens the table writer itself, and the writer refuses to write over a torn live area.
            execute("INSERT INTO txn_released_neg_bypass VALUES (3, '2024-01-03T00:00:00.000000Z')");
            assertQuery("SELECT count(), sum(x) FROM txn_released_neg_bypass")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count\tsum
                            3\t6
                            """);
        });
    }

    /**
     * The geometry version is a cache key that changes with every composite geometry change, and a reader takes a
     * negative one for a torn record. A version loaded from a released record can start anywhere in
     * [0, Integer.MAX_VALUE]; Integer.MAX_VALUE is the high word of an unset minTimestamp. Incrementing it must
     * wrap within that range instead of overflowing into a torn verdict.
     */
    @Test
    public void testGeometryVersionWrapsInsteadOfTurningNegative() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            // Small enough for the backfill below to cut the day into pieces, which makes the partition composite.
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "8K");
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 64);
            execute("""
                    CREATE TABLE txn_geometry_wrap AS (
                        SELECT x::INT i, '0123456789012345678901234567890123456789' s,
                            timestamp_sequence('2020-02-03', 15 * 1_000_000L) ts
                        FROM long_sequence(5760)
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            // A later day, so the backfill goes through the O3 path instead of an append to the open partition.
            execute("""
                    INSERT INTO txn_geometry_wrap
                    SELECT x::INT + 90_000 i, 'n' s, timestamp_sequence('2020-02-06', 60 * 1_000_000L) ts
                    FROM long_sequence(50)
                    """);
            drainWalQueue();
            engine.releaseInactive();
            TxnCorruptionUtils.makeAreasLookReleased(engine, "txn_geometry_wrap", Integer.MAX_VALUE, 0L);

            execute("""
                    INSERT INTO txn_geometry_wrap
                    SELECT x::INT + 70_000 i, 'b' s, timestamp_sequence('2020-02-03T04:00:07', 5 * 1_000_000L) ts
                    FROM long_sequence(200)
                    """);
            drainWalQueue();
            final TableToken token = engine.verifyTableName("txn_geometry_wrap");
            Assert.assertFalse(
                    "the apply job must not suspend the table",
                    engine.getTableSequencerAPI().isSuspended(token)
            );
            engine.releaseInactive();
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertTrue(
                        "precondition: the backfill must make the partition composite",
                        reader.getGeometry().getPieceCount(0) > 1
                );
                final int geometryVersion = reader.getTxFile().getGeometryVersion();
                Assert.assertNotEquals("the geometry version must change", Integer.MAX_VALUE, geometryVersion);
                Assert.assertTrue("the geometry version must stay non-negative: " + geometryVersion, geometryVersion >= 0);
            }
            assertQuery("SELECT count() FROM txn_geometry_wrap")
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n6010\n");
        });
    }

    /**
     * A checksummed record whose base-header geometry stamp names another txn fails the load path, which falls
     * back to the previous record. The diagnosis path must call the same file torn, or a reader that cannot
     * acquire the live txn times out looking like contention.
     */
    @Test
    public void testStaleGeometryStampIsTornOnBothPaths() throws Exception {
        assertMemoryLeak(() -> {
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            final String tableName = "txn_geometry_stamp";
            final int timestampType = createTwoCommitTable(tableName, ff);
            final long liveTxn = TxnCorruptionUtils.readLiveAreaTxn(engine, tableName);
            TxnCorruptionUtils.tearLiveAreaGeometryStamp(engine, tableName);

            try (Path path = new Path(); TxReader txReader = new TxReader(ff)) {
                txnPath(path, tableName);
                txReader.ofRO(path.$(), timestampType, PartitionBy.HOUR);
                Assert.assertTrue("the intact other area must still load", txReader.unsafeLoadAll());
                Assert.assertEquals("the load path must fall back to the previous record", liveTxn - 1, txReader.getTxn());
                Assert.assertTrue("a stale geometry stamp under a stable version is torn, not contention",
                        txReader.unsafeIsLiveAreaTorn());
            }
        });
    }

    /**
     * Pins where the stamp lives. It must sit in the record's reserved tail gap, immediately after the
     * checksum and entirely below the next occupied field -- otherwise it clobbers a live value, and no
     * record may grow.
     */
    @Test
    public void testStampOccupiesTheReservedRecordGap() {
        Assert.assertEquals(
                "the stamp must sit immediately after the checksum",
                TableUtils.TX_OFFSET_BODY_CHECKSUM_64 + Long.BYTES,
                TableUtils.TX_OFFSET_BODY_CHECKSUM_STAMP_32
        );
        Assert.assertTrue(
                "the stamp must stay inside the record header",
                TableUtils.TX_OFFSET_BODY_CHECKSUM_STAMP_32 + Integer.BYTES <= TableUtils.TX_OFFSET_MAP_WRITER_COUNT_32
        );
    }

    @Test
    public void testChecksumIsStampedWithTheRecordItDescribes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table txn_stamp (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into txn_stamp values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();

            // The stamp must name the live record, and must not be the 0 that means "no stamp": a 0 there
            // would leave the record permanently unverified.
            final long liveTxn = TxnCorruptionUtils.readLiveAreaTxn(engine, "txn_stamp");
            final int liveStamp = TxnCorruptionUtils.readLiveAreaChecksumStamp(engine, "txn_stamp");
            Assert.assertNotEquals("a committed record must carry a stamp", 0, liveStamp);
            Assert.assertEquals("the stamp must name the record and checksum format",
                    (int) liveTxn ^ TableUtils.TX_BODY_CHECKSUM_STAMP_XOR, liveStamp);
        });
    }

    /**
     * The fast commit path republishes the previous-but-one area in place under a new txn: it rewrites the
     * body-checksum stamp but used to leave the base-header geometry stamp naming the txn that area was last
     * published under. A reader takes a checksummed record whose geometry stamp names another txn for a torn
     * live area, falls back to the previous record and can never acquire the live txn, so it spins until the
     * spin-lock timeout on every reload. Two consecutive no-change commits (a zero-row WAL commit, a TRUNCATE of
     * an empty table) are enough to reach that path.
     */
    @Test
    public void testFastPathCommitRestampsTheGeometrySlot() throws Exception {
        assertMemoryLeak(() -> {
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            final String tableName = "txn_fast_path";
            final int timestampType = createTwoCommitTable(tableName, ff);
            final ObjList<SymbolCountProvider> symbolCounts = new ObjList<>();
            try (Path path = new Path(); TxWriter txWriter = new TxWriter(ff, configuration)) {
                txnPath(path, tableName);
                txWriter.ofRW(path.$(), timestampType, PartitionBy.HOUR);
                // The second commit of createTwoCommitTable changed a partition size, so this one still takes the
                // full-record path; the next one, with the record structure unchanged twice in a row, is the fast path.
                txWriter.commit(symbolCounts);
                txWriter.commit(symbolCounts);
            }

            final long liveTxn = TxnCorruptionUtils.readLiveAreaTxn(engine, tableName);
            Assert.assertEquals("the stamp must name the live record",
                    (int) liveTxn ^ TableUtils.TX_GEOMETRY_VERSION_STAMP_XOR,
                    TxnCorruptionUtils.readLiveAreaGeometryStamp(engine, tableName));

            TxReader.resetBodyChecksumFallbackCount();
            try (Path path = new Path(); TxReader txReader = new TxReader(ff)) {
                txnPath(path, tableName);
                txReader.ofRO(path.$(), timestampType, PartitionBy.HOUR);
                Assert.assertTrue(txReader.unsafeLoadAll());
                Assert.assertEquals("the reader must load the live record, not fall back to the previous one",
                        liveTxn, txReader.getTxn());
                Assert.assertEquals(0L, TxReader.getBodyChecksumFallbackCount());
                Assert.assertFalse(txReader.unsafeIsLiveAreaTorn());
            }
        });
    }

    @Test
    public void testLegacyTxnWithoutChecksumStillLoads() throws Exception {
        // The false-positive control. A _txn written before the checksum existed has neither a stamp nor a
        // body checksum and must still load, or every existing database fails to open. TableUtils.CV_CHECKSUM_MAGIC
        // warns about the same failure mode for _cv.
        assertMemoryLeak(() -> {
            execute("create table txn_legacy (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into txn_legacy values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            execute("insert into txn_legacy values ('2024-01-02T00:00:00.000000Z', 2)");
            drainWalQueue();

            TxnCorruptionUtils.makeAreasLookLegacy(engine, "txn_legacy");

            TxnCorruptionUtils.forceReload(engine, "txn_legacy"); // must not throw
            assertQuery("select count() from txn_legacy")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n2\n");
        });
    }

    /**
     * The load path ({@code unsafeVerifyBodyChecksum}) and the diagnosis path ({@code unsafeIsLiveAreaTorn},
     * which tells a torn {@code _txn} from reader contention) must reach the same verdict. If only one of
     * them honoured the stamp, the same file would be reported as corruption on one path and as
     * contention on the other.
     */
    @Test
    public void testLiveAreaTornDiagnosisAgreesWithTheLoadPath() throws Exception {
        assertMemoryLeak(() -> {
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            final String tableName = "txn_diag";
            final int timestampType = createTwoCommitTable(tableName, ff);

            Assert.assertNotEquals(
                    "precondition: the writer must have stamped the checksum",
                    0,
                    TxnCorruptionUtils.readLiveAreaChecksumStamp(engine, tableName)
            );

            // Tear the live area's checksum only, leaving its stamp: the other area stays intact so
            // unsafeLoadAll() can fall back to it and the reader survives long enough to be diagnosed.
            TxnCorruptionUtils.tearLiveAreaChecksumSlot(engine, tableName);
            Assert.assertEquals(
                    "precondition: the live record's stamp must still name it",
                    (int) TxnCorruptionUtils.readLiveAreaTxn(engine, tableName) ^ TableUtils.TX_BODY_CHECKSUM_STAMP_XOR,
                    TxnCorruptionUtils.readLiveAreaChecksumStamp(engine, tableName)
            );

            try (Path path = new Path(); TxReader txReader = new TxReader(ff)) {
                txnPath(path, tableName);
                txReader.ofRO(path.$(), timestampType, PartitionBy.HOUR);
                Assert.assertTrue("the intact other area must still load", txReader.unsafeLoadAll());
                Assert.assertTrue(
                        "a zeroed checksum whose stamp still names the record is torn, not contention",
                        txReader.unsafeIsLiveAreaTorn()
                );
            }

            // Same bytes, stamp erased: a legacy record, which must not be called torn.
            TxnCorruptionUtils.makeAreasLookLegacy(engine, tableName);
            try (Path path = new Path(); TxReader txReader = new TxReader(ff)) {
                txnPath(path, tableName);
                txReader.ofRO(path.$(), timestampType, PartitionBy.HOUR);
                Assert.assertTrue(txReader.unsafeLoadAll());
                Assert.assertFalse(
                        "without a stamp an absent checksum is legacy, not tearing",
                        txReader.unsafeIsLiveAreaTorn()
                );
            }
        });
    }

    @Test
    public void testZeroedChecksumSlotWithALiveStampIsTorn() throws Exception {
        // The case the stamp exists for: a zeroed checksum slot under a stamp that names the record is a torn
        // write, not a legacy record, and the reader must not serve it.
        assertMemoryLeak(() -> {
            execute("create table txn_cap (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into txn_cap values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            // A second commit so both A/B areas carry a stamped record; otherwise the A/B fallback would
            // legitimately land on an unstamped (legacy) record and this would not exercise the torn verdict.
            execute("insert into txn_cap values ('2024-01-02T00:00:00.000000Z', 2)");
            drainWalQueue();

            Assert.assertNotEquals(
                    "precondition: area A must carry a stamp",
                    0, TxnCorruptionUtils.readChecksumStampA(engine, "txn_cap")
            );
            Assert.assertNotEquals(
                    "precondition: area B must carry a stamp",
                    0, TxnCorruptionUtils.readChecksumStampB(engine, "txn_cap")
            );

            // Zero both areas' checksum slots, leaving their stamps intact. That is what a partial
            // page write leaves behind: the stamp still names the record whose checksum has gone.
            TxnCorruptionUtils.tearChecksumSlots(engine, "txn_cap");

            try {
                TxnCorruptionUtils.forceReload(engine, "txn_cap");
                Assert.fail("expected a zeroed checksum slot with a live stamp to be rejected");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "checksum");
            }
        });
    }

    @Test
    public void testWriterRejectsTornLiveAreaWithoutChangingTheFile() throws Exception {
        assertMemoryLeak(() -> {
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            final String tableName = "txn_writer_torn";
            final int timestampType = createTwoCommitTable(tableName, ff);
            TxnCorruptionUtils.tearLiveAreaChecksumSlot(engine, tableName);
            try (Path path = new Path()) {
                txnPath(path, tableName);
                final java.nio.file.Path file = Paths.get(path.toString());
                final byte[] before = Files.readAllBytes(file);
                try (TxWriter writer = new TxWriter(ff, configuration)) {
                    writer.ofRW(path.$(), timestampType, PartitionBy.HOUR);
                    Assert.fail("a writer must not adopt the previous committed record");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "refusing to write");
                }
                Assert.assertArrayEquals(before, Files.readAllBytes(file));
            }
        });
    }

    // Creates a WAL table with two committed rows and shapes its _txn as a released binary leaves it. Then checks
    // that a fresh reader serves the live record, that the WAL apply job writes to the table, and that the first
    // commit moves the geometry version into the stamped base-header slot.
    private void assertReleasedWalTableServesAndTakesWrites(
            String tableName,
            long liveAreaGap,
            long otherAreaGap
    ) throws Exception {
        execute("CREATE TABLE " + tableName + " (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO " + tableName + " VALUES (1, '2024-01-01T00:00:00.000000Z')");
        drainWalQueue();
        execute("INSERT INTO " + tableName + " VALUES (2, '2024-01-02T00:00:00.000000Z')");
        drainWalQueue();
        engine.releaseInactive();
        final long liveTxn = TxnCorruptionUtils.readLiveAreaTxn(engine, tableName);
        TxnCorruptionUtils.makeAreasLookReleased(engine, tableName, liveAreaGap, otherAreaGap);
        Assert.assertTrue(
                "precondition: the live record's legacy slot must be negative",
                TxnCorruptionUtils.readLiveAreaLegacyGeometrySlot(engine, tableName) < 0
        );

        final TableToken token = engine.verifyTableName(tableName);
        try (TableReader reader = engine.getReader(token)) {
            final TxReader txFile = reader.getTxFile();
            Assert.assertEquals(
                    "the reader must serve the live record, not fall back to the previous one",
                    liveTxn,
                    txFile.getTxn()
            );
            Assert.assertEquals("a negative released slot reads as geometry version 0", 0, txFile.getGeometryVersion());
            Assert.assertFalse(txFile.unsafeIsLiveAreaTorn());
        }

        execute("INSERT INTO " + tableName + " VALUES (3, '2024-01-03T00:00:00.000000Z')");
        drainWalQueue();
        Assert.assertFalse(
                "the apply job must not suspend the table",
                engine.getTableSequencerAPI().isSuspended(token)
        );
        assertQuery("SELECT count(), sum(x) FROM " + tableName)
                .noRandomAccess()
                .expectSize()
                .returns("""
                        count\tsum
                        3\t6
                        """);
        Assert.assertEquals(
                "the first commit must stamp the live record's base-header geometry slot",
                (int) TxnCorruptionUtils.readLiveAreaTxn(engine, tableName) ^ TableUtils.TX_GEOMETRY_VERSION_STAMP_XOR,
                TxnCorruptionUtils.readLiveAreaGeometryStamp(engine, tableName)
        );
    }

    // Creates an HOUR-partitioned table and commits twice through TxWriter, so A and B each hold a
    // complete, body-checksummed record. Returns the timestamp type for TxReader.ofRO.
    private int createTwoCommitTable(String tableName, FilesFacade ff) {
        TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR);
        model.timestamp();
        AbstractCairoTest.create(model);
        final int timestampType = TableUtils.getTimestampType(model);
        final ObjList<SymbolCountProvider> symbolCounts = new ObjList<>();
        try (Path path = new Path(); TxWriter txWriter = new TxWriter(ff, configuration)) {
            txnPath(path, tableName);
            txWriter.ofRW(path.$(), timestampType, PartitionBy.HOUR);
            txWriter.updatePartitionSizeByTimestamp(0, 42);
            txWriter.updatePartitionSizeByTimestamp(Micros.HOUR_MICROS, 43);
            txWriter.setMaxTimestamp(Micros.HOUR_MICROS);
            txWriter.commit(symbolCounts);
            txWriter.updatePartitionSizeByTimestamp(Micros.HOUR_MICROS, 44);
            txWriter.setMaxTimestamp(Micros.HOUR_MICROS);
            txWriter.commit(symbolCounts);
        }
        return timestampType;
    }

    private void txnPath(Path path, String tableName) {
        final TableToken token = engine.verifyTableName(tableName);
        path.of(configuration.getDbRoot()).concat(token).concat(TXN_FILE_NAME).$();
    }
}
