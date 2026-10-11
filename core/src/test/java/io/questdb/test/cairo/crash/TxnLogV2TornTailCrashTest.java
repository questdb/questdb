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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.TableTransactionLogV2;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * A power loss can put the header of a V2 sequencer log on the disk ahead of the part page that holds the records
 * it counts: under a group window, where the part and the header are flushed together only by the batch flush, or
 * after an earlier NOSYNC or ASYNC run. In ADAPTIVE the next open must drop every record that is not wholly on disk,
 * wherever it sits in the last part. The log must then end at the last whole record, the sequencer must keep its
 * schema, the table must apply without suspending, and it must take new rows and schema changes.
 * <p>
 * Each case commits records that are not applied yet, loses power with some of them torn in the part file (zeroed,
 * as a page that never reached the disk, on one side of a sector boundary, or past the fields a writeback caught
 * stored), and restarts. A record damaged after it reached the disk is not a tear and must not be dropped:
 * {@code TableTransactionLogV2CrcTest} and {@code TableTransactionLogV2TornTailTest} cover that.
 */
@RunWith(Parameterized.class)
public class TxnLogV2TornTailCrashTest extends AbstractAdaptiveCrashTest {
    private static final int PART_TXN_COUNT = 10;
    private static final int RECORD_SIZE = (int) TableTransactionLogV2.RECORD_SIZE;
    private final int appliedTxns;
    private final int pendingTxns;
    private final int tornBytes;
    private final int tornOffsetInRecord;
    private final int tornTxn;

    public TxnLogV2TornTailCrashTest(String name, int appliedTxns, int pendingTxns, int tornTxn, int tornOffsetInRecord, int tornBytes) {
        this.appliedTxns = appliedTxns;
        this.pendingTxns = pendingTxns;
        this.tornTxn = tornTxn;
        this.tornOffsetInRecord = tornOffsetInRecord;
        this.tornBytes = tornBytes;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        // name, txns applied before the power loss, txns committed and not applied, the first torn txn, where in that
        // record the tear starts, how many bytes from there are lost. Txn 9 is the record in part slot 8, at bytes
        // 480-539 of the part, so a 512-byte sector boundary splits it after its 32nd byte.
        final int sectorSplit = 512 - 8 * RECORD_SIZE;
        return Arrays.asList(new Object[][]{
                {"the last record", 5, 1, 6, 0, RECORD_SIZE},
                {"the last two records", 5, 2, 6, 0, 2 * RECORD_SIZE},
                {"a record before the last", 5, 2, 6, 0, RECORD_SIZE},
                {"the last record without its second sector", 8, 1, 9, sectorSplit, RECORD_SIZE - sectorSplit},
                {"the last record without its first sector", 8, 1, 9, 0, sectorSplit},
                {"the last record half-stored", 5, 1, 6, 12, RECORD_SIZE - 12},
                {"the first record of a new part", PART_TXN_COUNT, 1, PART_TXN_COUNT + 1, 0, RECORD_SIZE}
        });
    }

    @Test
    public void testTornTailIsDroppedAtOpen() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // The batch flush is still pending when the power goes: only the tear decides what the part holds.
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 10_000_000L);
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, PART_TXN_COUNT);
        node1.setProperty(PropertyKey.CAIRO_WAL_COMMIT_WRITEBACK_DRAIN, false);
        runWithCrashFacade(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // A schema change in the log: reading a torn record's structure version would revert it.
            execute("ALTER TABLE t ADD COLUMN c INT");
            for (int txn = 2; txn <= appliedTxns; txn++) {
                execute("INSERT INTO t VALUES (" + txn + ", " + txn + ", " + txn + ")");
            }
            drainWalQueue();
            final TableToken token = engine.verifyTableName("t");
            Assert.assertEquals(appliedTxns, engine.getTableSequencerAPI().lastTxn(token));
            markDurableBaseline();

            try (WalWriter writer = getWalWriter("t")) {
                for (int i = 1; i <= pendingTxns; i++) {
                    final TableWriter.Row row = writer.newRow(appliedTxns + i);
                    row.putLong(1, appliedTxns + i);
                    row.putInt(2, appliedTxns + i);
                    row.append();
                    writer.commit();
                }
            }
            final long lastTxn = appliedTxns + pendingTxns;
            Assert.assertEquals(lastTxn, engine.getTableSequencerAPI().lastTxn(token));
            // The header that counts the records reaches the disk; the torn bytes of the part do not.
            try (Path part = new Path()) {
                part.of(root).concat(token.getDirName()).concat(WalUtils.SEQ_DIR).concat(WalUtils.TXNLOG_PARTS_DIR)
                        .slash().put((tornTxn - 1) / PART_TXN_COUNT);
                crashFf.tornTail(part.$(), (long) ((tornTxn - 1) % PART_TXN_COUNT) * RECORD_SIZE + tornOffsetInRecord, tornBytes);
            }
            simulateRestart(new TableToken[]{token});

            Assert.assertEquals("the log must end at the last whole record", tornTxn - 1, engine.getTableSequencerAPI().lastTxn(token));
            Assert.assertEquals("version=1 columns=ts,v,c", sequencerSchema(token));
            new RecoveryCoordinator(engine).recover();
            engine.notifyWalTxnRepublisher(token);
            drainWalQueue();
            Assert.assertFalse(
                    "must not be suspended: " + engine.getTableSequencerAPI().getTxnTracker(token).getErrorMessage(),
                    engine.getTableSequencerAPI().isSuspended(token)
            );

            // New rows and schema changes follow the kept records.
            execute("INSERT INTO t VALUES (100, 100, 100)");
            execute("ALTER TABLE t ADD COLUMN d INT");
            execute("INSERT INTO t VALUES (101, 101, 101, 101)");
            drainWalQueue();
            Assert.assertFalse(
                    "must not be suspended after new writes: " + engine.getTableSequencerAPI().getTxnTracker(token).getErrorMessage(),
                    engine.getTableSequencerAPI().isSuspended(token)
            );
            Assert.assertEquals("version=2 columns=ts,v,c,d", sequencerSchema(token));
            final StringBuilder expected = new StringBuilder("v\tc\td\n");
            for (int txn = 2; txn < tornTxn; txn++) {
                expected.append(txn).append('\t').append(txn).append("\tnull\n");
            }
            expected.append("100\t100\tnull\n101\t101\t101\n");
            assertQuery("SELECT v, c, d FROM t ORDER BY v").expectSize().returns(expected.toString());
            releaseEngineHandles();
        });
    }

    private String sequencerSchema(TableToken token) {
        try (TableRecordMetadata metadata = engine.getSequencerMetadata(token)) {
            final StringBuilder sb = new StringBuilder("version=").append(metadata.getMetadataVersion()).append(" columns=");
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (metadata.getColumnType(i) > 0) {
                    sb.append(i > 0 ? "," : "").append(metadata.getColumnName(i));
                }
            }
            return sb.toString();
        }
    }
}
