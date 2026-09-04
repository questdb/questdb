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
 * The {@code _txn} body checksum used to be gated on a bare {@code stored == 0} sentinel, which cannot
 * tell a legacy record (written before the checksum existed) from one whose checksum slot was zeroed by a
 * torn page write -- so a torn {@code _txn} was served as healthy. The capability marker in the base
 * header settles it: at or beyond the recorded watermark a checksum was guaranteed written, so its absence
 * is tearing; below it, absence is simply legacy.
 */
public class TxnCapabilityChecksumTest extends AbstractCairoTest {

    /**
     * Pins where the capability lives. It must sit in the base header's unused tail padding: above the last
     * occupied slot, 8-byte aligned, and entirely below {@code TX_BASE_HEADER_SIZE} -- otherwise it either
     * clobbers a live field or spills into area A, and no file may grow.
     */
    @Test
    public void testCapabilityOccupiesUnusedBaseHeaderPadding() {
        final long firstFree = TableUtils.TX_BASE_OFFSET_PARTITIONS_SIZE_B_32 + Integer.BYTES;
        Assert.assertTrue(
                "capability magic overlaps an occupied base-header slot",
                TableUtils.TX_BASE_OFFSET_CAPABILITY_MAGIC_64 >= firstFree
        );
        Assert.assertEquals(0, TableUtils.TX_BASE_OFFSET_CAPABILITY_MAGIC_64 % Long.BYTES);
        Assert.assertEquals(0, TableUtils.TX_BASE_OFFSET_CAPABILITY_WATERMARK_64 % Long.BYTES);
        Assert.assertTrue(
                "capability slots must not overlap each other",
                Math.abs(TableUtils.TX_BASE_OFFSET_CAPABILITY_MAGIC_64 - TableUtils.TX_BASE_OFFSET_CAPABILITY_WATERMARK_64) >= Long.BYTES
        );
        Assert.assertTrue(
                "capability must stay inside the base header, before area A",
                Math.max(TableUtils.TX_BASE_OFFSET_CAPABILITY_MAGIC_64, TableUtils.TX_BASE_OFFSET_CAPABILITY_WATERMARK_64) + Long.BYTES
                        <= TableUtils.TX_BASE_HEADER_SIZE
        );
    }

    @Test
    public void testCapabilityStampedWithTheCommittedTxnNotZero() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table txn_stamp (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into txn_stamp values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();

            Assert.assertEquals(
                    TableUtils.TX_CHECKSUM_CAPABILITY_MAGIC,
                    TxnCorruptionUtils.readCapabilityMagic(engine, "txn_stamp")
            );

            // The watermark must be the txn that first carried a checksum, NEVER 0. A 0 watermark would
            // cover the records already on disk -- which were written without a checksum -- and condemn
            // every pre-existing database as torn.
            final long watermark = TxnCorruptionUtils.readCapabilityWatermark(engine, "txn_stamp");
            Assert.assertTrue(
                    "the watermark must be above the initial checksum-free txn, was " + watermark,
                    watermark > TableUtils.INITIAL_TXN
            );
            Assert.assertTrue(
                    "the watermark must not run ahead of the live record, was " + watermark,
                    watermark <= TxnCorruptionUtils.readLiveAreaTxn(engine, "txn_stamp")
            );
        });
    }

    @Test
    public void testLegacyTxnWithoutCapabilityStillLoads() throws Exception {
        // The false-positive control. A _txn written before the capability existed has neither the marker
        // nor a body checksum, and MUST still load -- this is the failure mode TableUtils.CV_CHECKSUM_MAGIC
        // warns about for _cv. If this fails, every existing database is being condemned.
        assertMemoryLeak(() -> {
            execute("create table txn_legacy (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into txn_legacy values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            execute("insert into txn_legacy values ('2024-01-02T00:00:00.000000Z', 2)");
            drainWalQueue();

            TxnCorruptionUtils.clearCapabilityMarker(engine, "txn_legacy");
            TxnCorruptionUtils.zeroBodyChecksumSlots(engine, "txn_legacy");

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
     * them honoured the capability, the same file would be reported as corruption on one path and as
     * contention on the other.
     */
    @Test
    public void testLiveAreaTornDiagnosisAgreesWithTheLoadPath() throws Exception {
        assertMemoryLeak(() -> {
            final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            final String tableName = "txn_diag";
            final int timestampType = createTwoCommitTable(tableName, ff);

            Assert.assertEquals(
                    "precondition: the writer must have stamped the capability",
                    TableUtils.TX_CHECKSUM_CAPABILITY_MAGIC,
                    TxnCorruptionUtils.readCapabilityMagic(engine, tableName)
            );

            // Zero the LIVE area's checksum only: the other area stays intact so unsafeLoadAll() can fall
            // back to it and the reader survives long enough to be asked for a diagnosis.
            TxnCorruptionUtils.zeroLiveAreaBodyChecksumSlot(engine, tableName);
            Assert.assertTrue(
                    "precondition: the live record must be covered by the capability",
                    TxnCorruptionUtils.readLiveAreaTxn(engine, tableName)
                            >= TxnCorruptionUtils.readCapabilityWatermark(engine, tableName)
            );

            try (Path path = new Path(); TxReader txReader = new TxReader(ff)) {
                txnPath(path, tableName);
                txReader.ofRO(path.$(), timestampType, PartitionBy.HOUR);
                Assert.assertTrue("the intact other area must still load", txReader.unsafeLoadAll());
                Assert.assertTrue(
                        "a zeroed checksum past the watermark must be diagnosed as torn, not as contention",
                        txReader.unsafeIsLiveAreaTorn()
                );
            }

            // Same bytes, capability erased: now it is a legacy record and must NOT be called torn.
            TxnCorruptionUtils.clearCapabilityMarker(engine, tableName);
            try (Path path = new Path(); TxReader txReader = new TxReader(ff)) {
                txnPath(path, tableName);
                txReader.ofRO(path.$(), timestampType, PartitionBy.HOUR);
                Assert.assertTrue(txReader.unsafeLoadAll());
                Assert.assertFalse(
                        "without a capability marker an absent checksum is legacy, not tearing",
                        txReader.unsafeIsLiveAreaTorn()
                );
            }
        });
    }

    @Test
    public void testZeroedChecksumSlotBeyondWatermarkIsTorn() throws Exception {
        // THE regression this task exists for. Before the capability marker a zeroed checksum slot read as
        // "legacy, skip the check" and the torn record was served.
        assertMemoryLeak(() -> {
            execute("create table txn_cap (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into txn_cap values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            // A second commit so BOTH A/B areas carry a txn at or beyond the watermark; otherwise the A/B
            // fallback would legitimately land on a pre-watermark (legacy) record and this would not be
            // exercising the torn verdict at all.
            execute("insert into txn_cap values ('2024-01-02T00:00:00.000000Z', 2)");
            drainWalQueue();

            final long watermark = TxnCorruptionUtils.readCapabilityWatermark(engine, "txn_cap");
            final long liveTxn = TxnCorruptionUtils.readLiveAreaTxn(engine, "txn_cap");
            final long otherTxn = TxnCorruptionUtils.readOtherAreaTxn(engine, "txn_cap");
            Assert.assertTrue(
                    "precondition: both A/B areas must be covered by the capability [watermark=" + watermark
                            + ", liveTxn=" + liveTxn + ", otherTxn=" + otherTxn + ']',
                    watermark > 0 && liveTxn >= watermark && otherTxn >= watermark
            );

            // Zero BOTH areas' body-checksum slots, leaving the capability marker in the base header intact.
            // That is exactly what a partial page write can leave behind.
            TxnCorruptionUtils.zeroBodyChecksumSlots(engine, "txn_cap");

            try {
                TxnCorruptionUtils.forceReload(engine, "txn_cap");
                Assert.fail("expected a zeroed checksum slot beyond the watermark to be rejected");
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
