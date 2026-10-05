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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.cairo.wal.seq.TableTransactionLogV1;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.cairo.wal.seq.TxnLogCrcSidecar;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.crash.CrashFaultFilesFacade;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;

/**
 * The V1 sidecar stamps an entry with a txn NUMBER, and a crash can leave the entry of a txn the header
 * never published. Reusing that number must not let the old CRC condemn the new, intact record, in any
 * commit mode, however the two files are written back, and across sequencer close/reopen and lineage
 * resets. Each scenario proves the surviving record is intact by comparing the whole durable txnlog with
 * the image that was written, so a rejection can only be a false one.
 */
@RunWith(Parameterized.class)
public class TxnLogCrcSidecarReuseTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(TxnLogCrcSidecarReuseTest.class);
    private final int commitMode;
    private final long groupWindowUs;

    public TxnLogCrcSidecarReuseTest(String name, int commitMode, long groupWindowUs) {
        this.commitMode = commitMode;
        this.groupWindowUs = groupWindowUs;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        // ADAPTIVE with a group window defers the sidecar flush to a batch that never runs here: the
        // harshest schedule for this bug.
        return Arrays.asList(new Object[][]{
                {"nosync", CommitMode.NOSYNC, 0L},
                {"async", CommitMode.ASYNC, 0L},
                {"sync", CommitMode.SYNC, 0L},
                {"adaptive", CommitMode.ADAPTIVE, 0L},
                {"adaptive-deferred", CommitMode.ADAPTIVE, 50_000L}
        });
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        Assume.assumeFalse(Os.isWindows());
    }

    @Test
    public void testCorruptedReusedRecordIsStillDetected() throws Exception {
        // Retiring stale entries must not cost detection: once the reused txn's own CRC is durable, a
        // damaged record is still rejected.
        assertMemoryLeak(() -> {
            final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
            try (Path path = newDir(ff, "corrupt_reused")) {
                final String dir = path.toString();
                leaveStaleEntries(ff, path, 1, 1);
                try (TableTransactionLogV1 v1 = new TableTransactionLogV1(cfg(ff))) {
                    v1.open(path);
                    Assert.assertEquals(2, v1.addEntry(0, 20, 0, 0, 20, 0, 0, 1));
                    ff.markFileDurable(dir + "/" + WalUtils.TXNLOG_FILE_NAME);
                    ff.markFileDurable(dir + "/" + WalUtils.TXNLOG_CRC_FILE_NAME);
                }
                ff.crash(dir);
                assertWalIds(path, 1, 20);

                final java.nio.file.Path log = java.nio.file.Path.of(dir, WalUtils.TXNLOG_FILE_NAME);
                final byte[] bytes = Files.readAllBytes(log);
                bytes[(int) (TableTransactionLogFile.HEADER_SIZE + TableTransactionLogV1.RECORD_SIZE + 4)] ^= 1;
                Files.write(log, bytes);
                try {
                    assertWalIds(path, 1, 20);
                    Assert.fail("a damaged record with a durable CRC must be rejected");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "torn sequencer txnlog record [txn=2");
                }
            }
        });
    }

    @Test
    public void testLineageResetOverSurvivingFiles() throws Exception {
        // create() over a directory whose previous _txnlog and _txnlog.c survived (WAL to non-WAL and back,
        // when removing the old sequencer failed). The new lineage restarts at txn 1, so neither the old
        // CRCs nor the old records may be read against the other lineage.
        assertMemoryLeak(() -> {
            final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
            try (Path path = newDir(ff, "lineage")) {
                final String dir = path.toString();
                try (TableTransactionLogV1 v1 = new TableTransactionLogV1(cfg(ff))) {
                    v1.create(path, 1);
                    v1.addEntry(0, 1, 0, 0, 1, 0, 0, 1);
                    v1.addEntry(0, 2, 0, 0, 2, 0, 0, 1);
                }
                ff.markDurableBaseline(dir);
                final byte[] expected;
                try (TableTransactionLogV1 v1 = new TableTransactionLogV1(cfg(ff))) {
                    v1.create(path, 1);
                    Assert.assertEquals(1, v1.addEntry(0, 7, 0, 0, 5, 0, 0, 1));
                    expected = persistTxnLog(ff, dir);
                }
                ff.crash(dir);
                assertDurableTxnLog(dir, expected);
                assertWalIds(path, 7);
            }
        });
    }

    @Test
    public void testLineageResetThenReopen() throws Exception {
        // Replacing txn 1 of the new lineage is not enough: after close and reopen, txn 2 reuses a slot
        // the old lineage stamped, and close() truncates it from the mapping without syncing.
        assertMemoryLeak(() -> {
            final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
            try (Path path = newDir(ff, "lineage_reopen")) {
                final String dir = path.toString();
                try (TableTransactionLogV1 v1 = new TableTransactionLogV1(cfg(ff))) {
                    v1.create(path, 1);
                    v1.addEntry(0, 1, 0, 0, 1, 0, 0, 1);
                    v1.addEntry(0, 2, 0, 0, 2, 0, 0, 1);
                    v1.addEntry(0, 3, 0, 0, 3, 0, 0, 1);
                }
                ff.markDurableBaseline(dir);
                try (TableTransactionLogV1 v1 = new TableTransactionLogV1(cfg(ff))) {
                    v1.create(path, 1);
                    Assert.assertEquals(1, v1.addEntry(0, 7, 0, 0, 5, 0, 0, 1));
                }
                final byte[] expected;
                try (TableTransactionLogV1 v1 = new TableTransactionLogV1(cfg(ff))) {
                    v1.open(path);
                    Assert.assertEquals(2, v1.addEntry(0, 8, 0, 0, 6, 0, 0, 1));
                    expected = persistTxnLog(ff, dir);
                }
                ff.crash(dir);
                assertDurableTxnLog(dir, expected);
                assertWalIds(path, 7, 8);
            }
        });
    }

    @Test
    public void testRandomCrashSchedules() throws Exception {
        // Random appends, structural changes, independent writeback of either file, batch flushes,
        // close/reopen, crashes and lineage resets. The crash model persists whole-file images, so the
        // published records are always intact: every rejection is false. After each crash, damage one
        // checksummed record to prove detection still holds.
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            for (int schedule = 0; schedule < 10; schedule++) {
                runRandomSchedule(rnd, schedule);
            }
        });
    }

    @Test
    public void testReusedTxnAfterCrash() throws Exception {
        assertMemoryLeak(() -> {
            final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
            try (Path path = newDir(ff, "reused")) {
                final String dir = path.toString();
                leaveStaleEntries(ff, path, 1, 1);
                final byte[] expected;
                try (TableTransactionLogV1 v1 = new TableTransactionLogV1(cfg(ff))) {
                    v1.open(path);
                    Assert.assertEquals(1, v1.lastTxn());
                    Assert.assertEquals(2, v1.addEntry(0, 20, 0, 0, 20, 0, 0, 1));
                    expected = persistTxnLog(ff, dir);
                }
                ff.crash(dir);
                assertDurableTxnLog(dir, expected);
                assertWalIds(path, 1, 20);
            }
        });
    }

    @Test
    public void testStaleEntriesSurviveCloseAndReopen() throws Exception {
        // Two stale entries. Reusing txn 2 and closing truncates the mapping past it without syncing, so
        // an append-time check would miss the stale txn 3 that is still on disk.
        assertMemoryLeak(() -> {
            final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
            try (Path path = newDir(ff, "two_stale")) {
                final String dir = path.toString();
                leaveStaleEntries(ff, path, 1, 2);
                try (TableTransactionLogV1 v1 = new TableTransactionLogV1(cfg(ff))) {
                    v1.open(path);
                    Assert.assertEquals(2, v1.addEntry(0, 20, 0, 0, 20, 0, 0, 1));
                }
                final byte[] expected;
                try (TableTransactionLogV1 v1 = new TableTransactionLogV1(cfg(ff))) {
                    v1.open(path);
                    Assert.assertEquals(3, v1.addEntry(0, 30, 0, 0, 30, 0, 0, 1));
                    expected = persistTxnLog(ff, dir);
                }
                ff.crash(dir);
                assertDurableTxnLog(dir, expected);
                assertWalIds(path, 1, 20, 30);
            }
        });
    }

    private static void assertDurableTxnLog(String dir, byte[] expected) throws Exception {
        Assert.assertArrayEquals(
                "the whole txnlog survived the crash as written",
                expected,
                Files.readAllBytes(java.nio.file.Path.of(dir, WalUtils.TXNLOG_FILE_NAME))
        );
    }

    private static long readLong(byte[] bytes, long offset) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong((int) offset);
    }

    private void assertWalIds(Path path, int... walIds) {
        try (
                TableTransactionLogV1 reader = new TableTransactionLogV1(configuration);
                TransactionLogCursor cursor = reader.getCursor(0, path)
        ) {
            for (int walId : walIds) {
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(walId, cursor.getWalId());
            }
            Assert.assertFalse(cursor.hasNext());
        }
    }

    private CairoConfiguration cfg(FilesFacade ff) {
        return cfg(ff, commitMode, groupWindowUs);
    }

    private CairoConfiguration cfg(FilesFacade ff, int mode, long windowUs) {
        return new CairoConfigurationWrapper(configuration) {
            @Override
            public long getAdaptiveCommitGroupWindowUs() {
                return windowUs;
            }

            @Override
            public int getCommitMode() {
                return mode;
            }

            @Override
            public FilesFacade getFilesFacade() {
                return ff;
            }
        };
    }

    /**
     * Publishes {@code published} txns durably, then writes {@code stale} more whose CRCs reach storage
     * while their header update does not, and crashes: the sequencer reopens at txn {@code published}
     * with stamped entries past it. The first session runs NOSYNC, as it would before a mode change;
     * under the other modes a per-commit header flush makes this state a narrow window instead.
     */
    private void leaveStaleEntries(CrashFaultFilesFacade ff, Path path, int published, int stale) {
        final String dir = path.toString();
        try (TableTransactionLogV1 v1 = new TableTransactionLogV1(cfg(ff, CommitMode.NOSYNC, 0))) {
            v1.create(path, 1);
            for (int i = 1; i <= published; i++) {
                v1.addEntry(0, i, 0, 0, i, 0, 0, 1);
            }
            ff.markDurableBaseline(dir);
            for (int i = 1; i <= stale; i++) {
                v1.addEntry(0, 100 + i, 0, 0, 100 + i, 0, 0, 1);
            }
            ff.markFileDurable(dir + "/" + WalUtils.TXNLOG_CRC_FILE_NAME);
        }
        ff.crash(dir);
    }

    private Path newDir(FilesFacade ff, String name) {
        final Path path = new Path().of(root).concat(name);
        Assert.assertEquals(0, ff.mkdir(path.$(), configuration.getMkDirMode()));
        return path;
    }

    /**
     * Kernel writeback of the whole txnlog, which is free to happen before the sidecar's in every mode
     * that has not flushed it yet. Returns the image that must survive.
     */
    private byte[] persistTxnLog(CrashFaultFilesFacade ff, String dir) throws Exception {
        final byte[] image = Files.readAllBytes(java.nio.file.Path.of(dir, WalUtils.TXNLOG_FILE_NAME));
        ff.markFileDurable(dir + "/" + WalUtils.TXNLOG_FILE_NAME);
        return image;
    }

    private void runRandomSchedule(Rnd rnd, int schedule) throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final boolean isDeferred = commitMode == CommitMode.ADAPTIVE && groupWindowUs > 0;
        try (Path path = newDir(ff, "schedule_" + schedule)) {
            final String dir = path.toString();
            TableTransactionLogV1 v1 = new TableTransactionLogV1(cfg(ff));
            try {
                v1.create(path, 1);
                ff.markDurableBaseline(dir);
                int walId = 0;
                for (int step = 0; step < 60; step++) {
                    final int op = rnd.nextInt(100);
                    if (op < 40) {
                        v1.addEntry(0, ++walId, rnd.nextInt(10), rnd.nextInt(100), rnd.nextLong(), 0, 0, 1);
                    } else if (op < 45) {
                        // TableTransactionLog.endMetadataChangeEntry() flushes the log around the V1 call.
                        v1.beginMetadataChangeEntry(0, null, null, rnd.nextLong());
                        v1.fullSync();
                        v1.endMetadataChangeEntry();
                        v1.fullSync();
                    } else if (op < 52) {
                        if (isDeferred) {
                            v1.fdatasyncTxnLog();
                        }
                    } else if (op < 64) {
                        ff.markFileDurable(dir + "/" + WalUtils.TXNLOG_FILE_NAME);
                    } else if (op < 76) {
                        ff.markFileDurable(dir + "/" + WalUtils.TXNLOG_CRC_FILE_NAME);
                    } else if (op < 84) {
                        v1.close();
                        v1 = new TableTransactionLogV1(cfg(ff));
                        v1.open(path);
                    } else if (op < 97) {
                        v1.close();
                        ff.crash(dir);
                        verifyAfterCrash(path, rnd, schedule);
                        v1 = new TableTransactionLogV1(cfg(ff));
                        v1.open(path);
                    } else {
                        v1.close();
                        v1 = new TableTransactionLogV1(cfg(ff));
                        v1.create(path, rnd.nextLong());
                    }
                }
                v1.close();
                ff.crash(dir);
                verifyAfterCrash(path, rnd, schedule);
            } finally {
                v1.close();
            }
        }
    }

    private void verifyAfterCrash(Path path, Rnd rnd, int schedule) throws Exception {
        final java.nio.file.Path logPath = java.nio.file.Path.of(path.toString(), WalUtils.TXNLOG_FILE_NAME);
        final java.nio.file.Path crcPath = java.nio.file.Path.of(path.toString(), WalUtils.TXNLOG_CRC_FILE_NAME);
        final byte[] log = Files.readAllBytes(logPath);
        final long maxTxn = readLong(log, TableTransactionLogFile.MAX_TXN_OFFSET_64);
        try (
                TableTransactionLogV1 reader = new TableTransactionLogV1(configuration);
                TransactionLogCursor cursor = reader.getCursor(0, path)
        ) {
            long count = 0;
            while (cursor.hasNext()) {
                count++;
            }
            Assert.assertEquals(maxTxn, count);
        } catch (CairoException e) {
            throw new AssertionError("intact record rejected [schedule=" + schedule + ", error=" + e.getFlyweightMessage() + ']', e);
        }

        // Damage one published record whose sidecar entry is stamped: it must be rejected.
        final byte[] crc = Files.exists(crcPath) ? Files.readAllBytes(crcPath) : new byte[0];
        if (crc.length < TxnLogCrcSidecar.BODY_OFFSET || readLong(crc, 0) != TxnLogCrcSidecar.MAGIC) {
            return;
        }
        final long firstCovered = readLong(crc, 16);
        long checkedTxn = -1;
        int stamped = 0;
        for (long txn = Math.max(1, firstCovered); txn <= maxTxn; txn++) {
            final long offset = TxnLogCrcSidecar.BODY_OFFSET + (txn - firstCovered) * TxnLogCrcSidecar.ENTRY_SIZE;
            if (offset + TxnLogCrcSidecar.ENTRY_SIZE <= crc.length
                    && readLong(crc, offset + TxnLogCrcSidecar.ENTRY_STAMP_OFFSET) == txn
                    && rnd.nextInt(++stamped) == 0) {
                checkedTxn = txn;
            }
        }
        if (checkedTxn < 0) {
            return;
        }
        final byte[] damaged = log.clone();
        damaged[(int) (TableTransactionLogFile.HEADER_SIZE + (checkedTxn - 1) * TableTransactionLogV1.RECORD_SIZE
                + rnd.nextInt((int) TableTransactionLogV1.RECORD_SIZE))] ^= (byte) (1 << rnd.nextInt(8));
        Files.write(logPath, damaged);
        try (
                TableTransactionLogV1 reader = new TableTransactionLogV1(configuration);
                TransactionLogCursor cursor = reader.getCursor(0, path)
        ) {
            while (cursor.hasNext()) {
                // the damaged record throws
            }
            Assert.fail("damaged record with a stamped CRC was not rejected [schedule=" + schedule + ", txn=" + checkedTxn + ']');
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "torn sequencer txnlog record");
        } finally {
            Files.write(logPath, log);
        }
    }
}
