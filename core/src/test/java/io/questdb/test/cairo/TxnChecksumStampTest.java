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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SymbolCountProvider;
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

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

/**
 * A bare {@code stored == 0} sentinel cannot tell a legacy record (written before the checksum existed)
 * from one whose checksum slot a torn page write zeroed, so a torn {@code _txn} would be served as healthy.
 * The stamp beside the checksum settles it: it names the txn the checksum was computed for, so a slot that
 * names THIS record was written for it and its absence is tearing, while a slot naming another record -- or
 * none at all -- says nothing about this one.
 * <p>
 * The stamp is what a file-level "guaranteed from here on" watermark cannot be: withdrawable. A binary that
 * predates the checksum overwrites a record's body in place and leaves the previous occupant's stamp
 * behind, which no longer names the new record, so the file still opens. See
 * {@code OlderBinaryWriteCompatTest}.
 */
public class TxnChecksumStampTest extends AbstractCairoTest {

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
            Assert.assertEquals("the stamp must name the record it sits in", (int) liveTxn, liveStamp);
        });
    }

    @Test
    public void testLegacyTxnWithoutChecksumStillLoads() throws Exception {
        // The false-positive control. A _txn written before the capability existed has neither the marker
        // nor a body checksum, and MUST still load -- this is the failure mode TableUtils.CV_CHECKSUM_MAGIC
        // warns about for _cv. If this fails, every existing database is being condemned.
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

            // Tear the LIVE area's checksum only, leaving its stamp: the other area stays intact so
            // unsafeLoadAll() can fall back to it and the reader survives long enough to be diagnosed.
            TxnCorruptionUtils.tearLiveAreaChecksumSlot(engine, tableName);
            Assert.assertEquals(
                    "precondition: the live record's stamp must still name it",
                    (int) TxnCorruptionUtils.readLiveAreaTxn(engine, tableName),
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

            // Same bytes, stamp erased: now it is a legacy record and must NOT be called torn.
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
        // THE regression this design exists for. Without the stamp a zeroed checksum slot read as "legacy,
        // skip the check" and the torn record was served.
        assertMemoryLeak(() -> {
            execute("create table txn_cap (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into txn_cap values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            // A second commit so BOTH A/B areas carry a stamped record; otherwise the A/B fallback would
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

            // Zero BOTH areas' checksum slots, leaving their stamps intact. That is exactly what a partial
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
