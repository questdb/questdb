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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.wal.DefaultWalDirectoryPolicy;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cairo.wal.seq.TableTransactionLogV2;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collection;

import static io.questdb.cairo.wal.seq.TableTransactionLogFile.MAX_TXN_OFFSET_64;
import static io.questdb.cairo.wal.seq.TableTransactionLogV2.UNSYNCED_FROM_TXN_OFFSET_64;

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
    public void testAppendCrashAtEveryDurabilityOp() throws Exception {
        assumeCrashHarnessSupported();
        assertMemoryLeak(() -> {
            for (long window : new long[]{0, 10_000_000}) {
                final int count = appendAtCrashPoint(window, 0);
                Assert.assertTrue("an ADAPTIVE append must flush", count > 0);
                for (int k = 1; k <= count; k++) {
                    appendAtCrashPoint(window, k);
                }
            }
        });
    }

    /**
     * The startup scan opens a torn log; the open cuts it and then fails, the sequencer's {@code _meta} does not
     * open. The scan must publish the txn the cut log keeps, not the count the header held before the open, or the
     * table shows a lag that never closes.
     */
    @Test
    public void testScanOpenFailingAfterTheCut() throws Exception {
        runWithScanOpenFault("txn_seq" + Files.SEPARATOR + "_meta", token -> {
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            Assert.assertEquals("the scan must publish the txn the cut log keeps", tornTxn - 1, tracker.getSeqTxn());
            assertApplyGoesIdle(token);
            Assert.assertEquals(tornTxn - 1, engine.getTableSequencerAPI().lastTxn(token));
            Assert.assertFalse("must not be suspended: " + tracker.getErrorMessage(), tracker.isSuspended());
        });
    }

    /**
     * The startup scan opens a torn log, and the open fails before it reads the log, so the scan publishes the
     * header's count. The apply job's open then cuts the torn records. The tracker must come down with the log's end,
     * or the table shows a lag that never closes while idle. No torn record may be applied, and the next commit must
     * take the first cut txn and be applied.
     */
    @Test
    public void testScanOpenFailingBeforeTheCut() throws Exception {
        runWithScanOpenFault("txn_seq" + Files.SEPARATOR + WalUtils.WAL_INDEX_FILE_NAME, token -> {
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            Assert.assertEquals("the scan publishes the header's count", appliedTxns + pendingTxns, tracker.getSeqTxn());
            assertApplyGoesIdle(token);
            Assert.assertEquals(tornTxn - 1, engine.getTableSequencerAPI().lastTxn(token));
            Assert.assertEquals("the cut must lower the tracker to the log's end", appliedTxns, tracker.getSeqTxn());
            Assert.assertFalse("must not be suspended: " + tracker.getErrorMessage(), tracker.isSuspended());
            Assert.assertEquals(appliedTxns, tracker.getWriterTxn());

            execute("INSERT INTO t VALUES (100, 100, 100)");
            drainWalQueue();
            Assert.assertFalse("must not be suspended: " + tracker.getErrorMessage(), tracker.isSuspended());
            Assert.assertEquals(tornTxn, tracker.getWriterTxn());
            final StringBuilder expected = new StringBuilder("v\tc\n");
            for (int txn = 2; txn <= appliedTxns; txn++) {
                expected.append(txn).append('\t').append(txn).append('\n');
            }
            expected.append("100\t100\n");
            assertQuery("SELECT v, c FROM t ORDER BY v").expectSize().returns(expected.toString());
        });
    }

    /**
     * A NOSYNC run on a log that ADAPTIVE kept durable applies every commit, and a power loss keeps the table's
     * {@code _txn} and the header but tears the same records as {@link #testTornTailIsDroppedAtOpen}. The ADAPTIVE
     * restart scans the records the NOSYNC run wrote. It must keep the ones the table applied, with the table's
     * schema, so new commits get new txns and every one of them is applied.
     */
    @Test
    public void testTornAppliedTailIsKeptAfterNosyncRun() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 10_000_000L);
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, PART_TXN_COUNT);
        node1.setProperty(PropertyKey.CAIRO_WAL_COMMIT_WRITEBACK_DRAIN, false);
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        runWithCrashFacade(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("ALTER TABLE t ADD COLUMN c INT");
            for (int txn = 2; txn <= appliedTxns; txn++) {
                execute("INSERT INTO t VALUES (" + txn + ", " + txn + ", " + txn + ")");
            }
            drainWalQueue();
            final TableToken token = engine.verifyTableName("t");
            markDurableBaseline();

            // A clean restart in NOSYNC: its first open of the log ends the field's promise.
            node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            Assert.assertEquals(CommitMode.NOSYNC, engine.getConfiguration().getCommitMode());
            simulateRestart(new TableToken[]{token});
            new RecoveryCoordinator(engine).recover();
            new CheckWalTransactionsJob(engine).run();
            try (WalWriter writer = getWalWriter("t")) {
                for (int i = 1; i <= pendingTxns; i++) {
                    final TableWriter.Row row = writer.newRow(appliedTxns + i);
                    row.putLong(1, appliedTxns + i);
                    row.putInt(2, appliedTxns + i);
                    row.append();
                    writer.commit();
                }
            }
            drainWalQueue();
            final long lastTxn = appliedTxns + pendingTxns;
            Assert.assertEquals(lastTxn, engine.getTableSequencerAPI().getTxnTracker(token).getWriterTxn());
            // The kernel wrote every page back, the table's _txn and the header included, but the torn bytes.
            markDurableBaseline();
            try (Path part = new Path()) {
                part.of(root).concat(token.getDirName()).concat(WalUtils.SEQ_DIR).concat(WalUtils.TXNLOG_PARTS_DIR)
                        .slash().put((tornTxn - 1) / PART_TXN_COUNT);
                crashFf.tornTail(part.$(), (long) ((tornTxn - 1) % PART_TXN_COUNT) * RECORD_SIZE + tornOffsetInRecord, tornBytes);
            }
            node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            simulateRestart(new TableToken[]{token});
            new RecoveryCoordinator(engine).recover();
            new CheckWalTransactionsJob(engine).run();
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            Assert.assertEquals("the scan must keep the txns the table applied", lastTxn, tracker.getSeqTxn());
            Assert.assertEquals("the log must keep the txns the table applied", lastTxn, engine.getTableSequencerAPI().lastTxn(token));
            Assert.assertEquals("version=1 columns=ts,v,c", sequencerSchema(token));
            Assert.assertFalse("must not be suspended: " + tracker.getErrorMessage(), tracker.isSuspended());
            engine.notifyWalTxnRepublisher(token);
            try (ApplyWal2TableJob apply = createWalApplyJob()) {
                // A bounded count, not a drain: a tracker above the log re-queues the table on every run.
                int runs = 0;
                while (apply.run()) {
                    Assert.assertTrue("the apply job must go idle", ++runs < 10);
                }
            }

            execute("INSERT INTO t VALUES (100, 100, 100)");
            execute("ALTER TABLE t ADD COLUMN d INT");
            execute("INSERT INTO t VALUES (101, 101, 101, 101)");
            drainWalQueue();
            Assert.assertFalse("must not be suspended after new writes: " + tracker.getErrorMessage(), tracker.isSuspended());
            Assert.assertEquals(lastTxn + 3, tracker.getWriterTxn());
            Assert.assertEquals("version=2 columns=ts,v,c,d", sequencerSchema(token));
            final StringBuilder expected = new StringBuilder("v\tc\td\n");
            for (long txn = 2; txn <= lastTxn; txn++) {
                expected.append(txn).append('\t').append(txn).append("\tnull\n");
            }
            expected.append("100\t100\tnull\n101\t101\t101\n");
            assertQuery("SELECT v, c, d FROM t ORDER BY v").expectSize().returns(expected.toString());
            releaseEngineHandles();
        });
    }

    @Test
    public void testTornTailIsDroppedAtOpen() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // The batch flush is still pending when the power goes: only the tear decides what the part holds.
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 10_000_000L);
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, PART_TXN_COUNT);
        node1.setProperty(PropertyKey.CAIRO_WAL_COMMIT_WRITEBACK_DRAIN, false);
        // No save point after the creation baseline, as after a power loss that follows applies past the last save
        // point: the restart restores the table and replays its WAL, and recovery does not open the sequencer.
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
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

            // CairoEngine.completeInit order: recovery, then the worker pools run the check job before any apply.
            // Nothing opens the sequencer before the check job's scan, so the scan is the first to read the log.
            new RecoveryCoordinator(engine).recover();
            new CheckWalTransactionsJob(engine).run();
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            Assert.assertEquals("the scan must seed the txn the log keeps", tornTxn - 1, tracker.getSeqTxn());
            engine.notifyWalTxnRepublisher(token);
            try (ApplyWal2TableJob apply = createWalApplyJob()) {
                // A bounded count, not a drain: a tracker above the log re-queues the table on every run.
                int runs = 0;
                while (apply.run()) {
                    Assert.assertTrue("the apply job must go idle", ++runs < 10);
                }
            }
            Assert.assertEquals("the log must end at the last whole record", tornTxn - 1, engine.getTableSequencerAPI().lastTxn(token));
            Assert.assertEquals("version=1 columns=ts,v,c", sequencerSchema(token));
            Assert.assertFalse(
                    "must not be suspended: " + tracker.getErrorMessage(),
                    engine.getTableSequencerAPI().isSuspended(token)
            );

            // The group flush covers the records in the log, not the torn header's count: a commit sequenced after
            // a flushed one stays non-durable until its own flush.
            try (WalWriter writer = getWalWriter("t")) {
                final TableWriter.Row row = writer.newRow(50);
                row.putLong(1, 50);
                row.putInt(2, 50);
                row.append();
                writer.commit();
            }
            try (WalWriter writer = getWalWriter("t")) {
                final TableWriter.Row row = writer.newRow(51);
                row.putLong(1, 51);
                row.putInt(2, 51);
                row.append();
                writer.commit();
                Assert.assertEquals(tornTxn + 1, engine.getTableSequencerAPI().lastTxn(token));
                Assert.assertTrue(
                        "a commit pending its flush must not be durable, frontier=" + tracker.getLocalDurableSeqTxn(),
                        tracker.getLocalDurableSeqTxn() < tornTxn + 1
                );
            }
            drainWalQueue();

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
            expected.append("50\t50\tnull\n51\t51\tnull\n100\t100\tnull\n101\t101\t101\n");
            assertQuery("SELECT v, c, d FROM t ORDER BY v").expectSize().returns(expected.toString());
            releaseEngineHandles();
        });
    }

    private int appendAtCrashPoint(long window, int k) throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        ff.modelSharedJournal = false;
        final String dir = temp.newFolder().getAbsolutePath();
        final CairoConfiguration config = new CairoConfigurationWrapper(configuration) {
            @Override
            public long getAdaptiveCommitGroupWindowUs() {
                return window;
            }

            @Override
            public int getCommitMode() {
                return CommitMode.ADAPTIVE;
            }

            @Override
            public FilesFacade getFilesFacade() {
                return ff;
            }
        };
        final String headerPath = dir + "/" + WalUtils.TXNLOG_FILE_NAME;
        final String partPath = dir + "/" + WalUtils.TXNLOG_PARTS_DIR + "/0";
        final int count;
        final byte[] expectedPart;
        try (Path path = new Path().of(dir)) {
            final TableTransactionLogV2 log = new TableTransactionLogV2(config, PART_TXN_COUNT, DefaultWalDirectoryPolicy.INSTANCE);
            try {
                log.create(path, 0);
                log.open(path);
                // A pre-existing part makes the count pass and all armed passes identical.
                log.addEntry(0, 1, 0, 0, 1001, 1000, 2000, 1);
                log.fdatasyncTxnLog();
                ff.markDurableBaseline(dir);
                final int before = ff.durabilityOpCount();
                if (k > 0) {
                    ff.armCrashAt(before + k);
                }
                try {
                    log.addEntry(0, 1, 0, 1, 1002, 1000, 2000, 1);
                    if (window > 0) {
                        log.fdatasyncTxnLog();
                    }
                    Assert.assertEquals("armed crash must fire", 0, k);
                } catch (CrashSimulationError expected) {
                    Assert.assertTrue(k > 0);
                    Assert.assertFalse(ff.isCrashArmed());
                }
                count = ff.durabilityOpCount() - before;
                expectedPart = java.nio.file.Files.readAllBytes(java.nio.file.Path.of(partPath));
                ff.powerLoss();
            } finally {
                log.close();
            }
            // Inspect the frozen durable image, before crash() installs a new baseline or open repairs a tail.
            final byte[] header = ff.durableContentOf(headerPath);
            final byte[] part = ff.durableContentOf(partPath);
            final ByteBuffer h = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN);
            final long maxTxn = h.getLong((int) MAX_TXN_OFFSET_64);
            final long field = h.getLong((int) UNSYNCED_FROM_TXN_OFFSET_64);
            final String what = "window=" + window + ", k=" + k;
            Assert.assertTrue(what + ": field ahead of durable header", field <= maxTxn + 1);
            for (int txn = 1; txn <= maxTxn; txn++) {
                Assert.assertArrayEquals(what + ": header counts a non-durable part record at txn " + txn,
                        Arrays.copyOfRange(expectedPart, (txn - 1) * RECORD_SIZE, txn * RECORD_SIZE),
                        Arrays.copyOfRange(part, (txn - 1) * RECORD_SIZE, txn * RECORD_SIZE));
            }
            ff.crash(dir);
            try (TableTransactionLogV2 reopened = new TableTransactionLogV2(config, PART_TXN_COUNT, DefaultWalDirectoryPolicy.INSTANCE)) {
                reopened.open(path);
                Assert.assertEquals(what + ": every header-trusted record survives open", maxTxn, reopened.lastTxn());
            }
        }
        return count;
    }

    // A bounded count, not a drain: a tracker above the log re-queues the table on every run.
    private void assertApplyGoesIdle(TableToken token) {
        engine.notifyWalTxnRepublisher(token);
        try (ApplyWal2TableJob apply = createWalApplyJob()) {
            int runs = 0;
            while (apply.run()) {
                Assert.assertTrue("the apply job must go idle", ++runs < 10);
            }
        }
    }

    /**
     * Commits records that are not applied yet, tears them as {@link #testTornTailIsDroppedAtOpen} does, restarts,
     * and runs the check job's startup scan while every open of a sequencer file whose path ends with
     * {@code faultSuffix} fails. Then checks the state with the fault gone.
     */
    private void runWithScanOpenFault(String faultSuffix, ScanCheck check) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 10_000_000L);
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, PART_TXN_COUNT);
        node1.setProperty(PropertyKey.CAIRO_WAL_COMMIT_WRITEBACK_DRAIN, false);
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        assumeCrashHarnessSupported();
        final OpenFaultFacade ff = new OpenFaultFacade(faultSuffix);
        crashFf = ff;
        crashFf.setDbRoot(root);
        try {
            assertMemoryLeak(crashFf, () -> {
                execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("ALTER TABLE t ADD COLUMN c INT");
                for (int txn = 2; txn <= appliedTxns; txn++) {
                    execute("INSERT INTO t VALUES (" + txn + ", " + txn + ", " + txn + ")");
                }
                drainWalQueue();
                final TableToken token = engine.verifyTableName("t");
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
                try (Path part = new Path()) {
                    part.of(root).concat(token.getDirName()).concat(WalUtils.SEQ_DIR).concat(WalUtils.TXNLOG_PARTS_DIR)
                            .slash().put((tornTxn - 1) / PART_TXN_COUNT);
                    crashFf.tornTail(part.$(), (long) ((tornTxn - 1) % PART_TXN_COUNT) * RECORD_SIZE + tornOffsetInRecord, tornBytes);
                }
                simulateRestart(new TableToken[]{token});
                new RecoveryCoordinator(engine).recover();

                ff.armed = true;
                try {
                    new CheckWalTransactionsJob(engine).run();
                } finally {
                    ff.armed = false;
                }
                Assert.assertTrue("the scan's open must fail", ff.fired > 0);
                check.run(token);
                releaseEngineHandles();
            });
        } finally {
            crashFf.reset();
            engine.releaseCrashOrphanedWalWriters();
            engine.releaseCrashOrphanedWriters();
        }
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

    @FunctionalInterface
    private interface ScanCheck {
        void run(TableToken token) throws Exception;
    }

    // Fails every open of a file whose path ends with the suffix, with EIO, while armed.
    private static final class OpenFaultFacade extends CrashFaultFilesFacade {
        private final String suffix;
        private boolean armed;
        private int fired;
        private int pendingErrno;

        OpenFaultFacade(String suffix) {
            this.suffix = suffix;
        }

        @Override
        public int errno() {
            final int errno = pendingErrno;
            if (errno != 0) {
                pendingErrno = 0;
                return errno;
            }
            return super.errno();
        }

        @Override
        public long openRO(LPSZ name) {
            return isHit(name) ? -1 : super.openRO(name);
        }

        @Override
        public long openRONoCache(LPSZ name) {
            return isHit(name) ? -1 : super.openRONoCache(name);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            return isHit(name) ? -1 : super.openRW(name, opts);
        }

        @Override
        public long openRWNoCache(LPSZ name, int opts) {
            return isHit(name) ? -1 : super.openRWNoCache(name, opts);
        }

        private boolean isHit(LPSZ name) {
            if (armed && Utf8s.endsWithAscii(name, suffix)) {
                fired++;
                pendingErrno = 5; // EIO
                return true;
            }
            return false;
        }
    }
}
