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

package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.cairo.TableToken;
import io.questdb.client.QuestDB;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.TransactionTooLargeException;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Batches of any size and transactions over QWP/WebSocket, driven by the real
 * client against the real server: the client's byte trigger against the
 * server's advertised frame cap, strict transactions against the advertised
 * {@code qwp.max.uncommitted.rows}, and the rollback control frame.
 * <p>
 * Unless a test says otherwise, the server under test accepts frames of at most
 * 64 KiB, so every transaction here spans many frames.
 */
public class QwpSenderTransactionE2ETest extends AbstractQwpWebSocketTest {
    // ~100 encoded bytes per row.
    private static final String PAD = "x".repeat(90);
    // The server's only report that qwp.max.uncommitted.rows split a transaction.
    private static final String SPLIT_ADVISORY = "deferred-commit group committed in parts";

    @Before
    public void pinFragmentation() {
        // These tests move hundreds of kilobytes; one-byte fragments would make
        // them wire-throughput tests. Fragmentation has its own coverage.
        sendChunk = Integer.MAX_VALUE;
        recvChunk = Integer.MAX_VALUE;
    }

    @Test
    public void testDefaultSenderSplitsWideRowsAtTheServerCap() throws Exception {
        // 1000 rows of ~400 bytes are six times the server's frame cap. The
        // default row trigger alone used to send them as one batch, which the
        // client refused with BatchTooLargeForCapException.
        final String wide = "w".repeat(390);
        runInContext(port -> {
            try (Sender sender = Sender.fromConfig(conf(port))) {
                for (int i = 0; i < 1_000; i++) {
                    sender.table("t").longColumn("v", i).stringColumn("s", wide).atNow();
                }
                sender.flush();
                Assert.assertTrue(sender.drain(60_000));
            }
            drainWalQueue();
            assertQuery("SELECT count(), count_distinct(v) FROM t").noLeakCheck().noRandomAccess().expectSize().returns("count\tcount_distinct\n1000\t1000\n");
        });
    }

    @Test
    public void testNoAutoFlushTriggerAddingColumnLogsNoSplit() throws Exception {
        assertAddingColumnLogsNoSplit("auto_flush=off;");
    }

    @Test
    public void testNoAutoFlushTriggerCommitsOnlyOnFlush() throws Exception {
        // auto_flush_rows=off with auto_flush_interval=off is auto_flush=off. It used to
        // send each flush as one frame, which the client refused past the server's frame
        // cap, and to commit on close().
        assertCommitsOnlyOnFlush("auto_flush_rows=off;auto_flush_interval=off;");
    }

    @Test
    public void testPooledStrictLeaseReturnBeforeFirstConnectKeepsEarlierCommits() throws Exception {
        // The server is down when the pool starts. Returning a strict lease whose rows
        // auto-flush had already sent used to close the pooled sender, as if the server
        // could not roll them back, and with it the rows an earlier lease had committed,
        // which had not reached the server yet.
        assertMemoryLeak(() -> {
            int port = RestartableQwpServer.pickFreePort();
            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                try (QuestDB db = QuestDB.connect(outageConf(port) + "transaction=strict;lazy_connect=true;")) {
                    try (Sender lease = db.borrowSender()) {
                        writeRows(lease, 0, 10);
                        lease.flush();
                    }
                    try (Sender lease = db.borrowSender()) {
                        // Returned without flush(): the pool discards this transaction.
                        writeRows(lease, 1_000, 600);
                    }
                    server.start();
                    try (Sender lease = db.borrowSender()) {
                        writeRows(lease, 10_000, 5);
                        lease.flush();
                        Assert.assertTrue(lease.drain(60_000));
                    }
                }
            }
            drainWalQueue();
            assertQuery("SELECT count(), min(v), max(v) FROM t").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\tmin\tmax\n15\t0\t10004\n");
        });
    }

    @Test
    public void testPooledStrictBatchesOfOneFrameKeepTheWholeBuffer() throws Exception {
        // Batches of 600 rows fit one frame of this server's 64 KiB cap and one 64 KiB
        // segment, as a sender with no auto-flush trigger sent them before transactions
        // spanned frames. Such a sender is strict now, and its byte trigger sends 90% of
        // a frame as a deferred frame and the rest with the commit. Four leases share
        // 256 KiB, less than a segment each, yet every batch commits: a transaction whose
        // frames fit one segment counts on the whole buffer, not on a share.
        runInContext(port -> {
            try (QuestDB db = QuestDB.connect(conf(port) + "auto_flush_rows=off;auto_flush_interval=off;"
                    + "sf_max_segment_bytes=64k;sf_max_total_bytes=256k;sf_append_deadline_millis=30000;"
                    + "query_pool_min=0;")) {
                Sender[] leases = new Sender[4];
                for (int i = 0; i < leases.length; i++) {
                    leases[i] = db.borrowSender();
                }
                runOnThreads(leases.length, lease -> {
                    for (int batch = 0; batch < 20; batch++) {
                        writeRows(leases[lease], "p" + lease, batch * 600, 600);
                        leases[lease].flush();
                    }
                });
                for (Sender lease : leases) {
                    Assert.assertTrue(lease.drain(60_000));
                    lease.close();
                }
            }
            drainWalQueue();
            for (int i = 0; i < 4; i++) {
                assertQuery("SELECT count(), count_distinct(v), min(v), max(v) FROM p" + i).noLeakCheck()
                        .noRandomAccess().expectSize()
                        .returns("count\tcount_distinct\tmin\tmax\n12000\t12000\t0\t11999\n");
            }
        });
    }

    @Test
    public void testPooledStrictLeasesAtTheirShareCommitTogether() throws Exception {
        // Two senders share 1 MiB, so each can count on eight segments. Two leases fill
        // their strict transactions on two threads at once until each share refuses a
        // row, and both commit as one transaction each without waiting for the other.
        // Sized against the whole buffer, together they would have filled it, and each
        // commit would have waited for room only the other commit frees.
        runInContext(port -> {
            String[] tables = {"t", "u"};
            int[] refused = new int[2];
            long[] txnBefore = new long[2];
            try (QuestDB db = QuestDB.connect(conf(port) + "transaction=strict;auto_flush_interval=off;"
                    + "sf_max_segment_bytes=64k;sf_max_total_bytes=1m;sf_append_deadline_millis=30000;"
                    + "sender_pool_max=2;query_pool_min=0;")) {
                Sender[] leases = {db.borrowSender(), db.borrowSender()};
                CyclicBarrier filled = new CyclicBarrier(2);
                long start = System.nanoTime();
                runOnThreads(2, lease -> {
                    refused[lease] = writeUntilShareRefused(leases[lease], tables[lease], 2);
                    txnBefore[lease] = engine.getTableSequencerAPI().lastTxn(awaitTable(tables[lease]));
                    // Both transactions are open, each at its largest, before either commits.
                    filled.await(60, TimeUnit.SECONDS);
                    leases[lease].flush();
                });
                Assert.assertTrue("must not wait for sf_append_deadline_millis",
                        System.nanoTime() - start < TimeUnit.SECONDS.toNanos(10));
                for (int i = 0; i < 2; i++) {
                    Assert.assertTrue(leases[i].drain(60_000));
                    Assert.assertEquals("one transaction", txnBefore[i] + 1,
                            engine.getTableSequencerAPI().lastTxn(engine.verifyTableName(tables[i])));
                    leases[i].close();
                }
            }
            drainWalQueue();
            for (int i = 0; i < 2; i++) {
                assertQuery("SELECT count(), min(v), max(v) FROM " + tables[i]).noLeakCheck().noRandomAccess()
                        .expectSize().returns("count\tmin\tmax\n" + refused[i] + "\t0\t" + (refused[i] - 1) + "\n");
            }
        });
    }

    @Test
    public void testPooledStrictLeasesRefuseRowsPastTheirShare() throws Exception {
        // The four senders of a default pool share sf_max_total_bytes, and each may hold
        // a strict transaction until the server commits it. Two leases used to size their
        // transactions against the whole 512 KiB: both sent frames until together they
        // filled it, then each commit waited for room the other held, threw after
        // sf_append_deadline_millis, and nothing committed. Each counts only on its share
        // now, two of the eight segments, so the second frame of this server's cap, a
        // segment each, refuses a row, and each lease commits the rows before it, as one
        // transaction, without waiting.
        runInContext(port -> {
            String[] tables = {"t", "u"};
            int[] refused = new int[2];
            long[] txnBefore = new long[2];
            try (QuestDB db = QuestDB.connect(conf(port) + "transaction=strict;auto_flush_interval=off;"
                    + "sf_max_segment_bytes=64k;sf_max_total_bytes=512k;sf_append_deadline_millis=30000;"
                    + "query_pool_min=0;")) {
                Sender[] leases = {db.borrowSender(), db.borrowSender()};
                long start = System.nanoTime();
                // Both transactions are open before either commits.
                for (int i = 0; i < 2; i++) {
                    refused[i] = writeUntilShareRefused(leases[i], tables[i], 4);
                    txnBefore[i] = engine.getTableSequencerAPI().lastTxn(awaitTable(tables[i]));
                }
                for (Sender lease : leases) {
                    lease.flush();
                }
                Assert.assertTrue("must not wait for sf_append_deadline_millis",
                        System.nanoTime() - start < TimeUnit.SECONDS.toNanos(10));
                for (int i = 0; i < 2; i++) {
                    Assert.assertTrue(leases[i].drain(60_000));
                    Assert.assertEquals("one transaction", txnBefore[i] + 1,
                            engine.getTableSequencerAPI().lastTxn(engine.verifyTableName(tables[i])));
                    leases[i].close();
                }
            }
            drainWalQueue();
            for (int i = 0; i < 2; i++) {
                assertQuery("SELECT count(), min(v), max(v) FROM " + tables[i]).noLeakCheck().noRandomAccess()
                        .expectSize().returns("count\tmin\tmax\n" + refused[i] + "\t0\t" + (refused[i] - 1) + "\n");
            }
        });
    }

    @Test
    public void testResetBeforeFirstConnectDiscardsStrictTransaction() throws Exception {
        assertDiscardBeforeFirstConnect("transaction=strict;", Sender::reset);
    }

    @Test
    public void testResetBeforeFirstConnectDiscardsTransaction() throws Exception {
        assertDiscardBeforeFirstConnect("transaction=on;", Sender::reset);
    }

    @Test
    public void testPooledStrictLeaseReturnDiscardsSplitFailedDuringOutage() throws Exception {
        // Returning the lease discards the strict transaction with no rollback() call: the
        // table the split sent must not commit with the next borrower's transaction.
        assertMemoryLeak(() -> {
            int port = RestartableQwpServer.pickFreePort();
            int fillers;
            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                server.start();
                try (QuestDB db = QuestDB.connect(splitOutageConf(port) + "transaction=strict;sender_pool_max=1;"
                        + "query_pool_min=0;")) {
                    Sender lease = db.borrowSender();
                    server.stop();
                    fillers = fillRingDuringOutage(lease);
                    sendPartOfSplitDuringOutage(lease);
                    lease.close();
                    server.start();
                    try (Sender next = db.borrowSender()) {
                        writeSymbolRows(next, "t", 10_000, 10);
                        next.flush();
                        Assert.assertTrue(next.drain(60_000));
                    }
                }
            }
            assertSplitDiscarded(fillers);
        });
    }

    @Test
    public void testResetDiscardsSplitFailedDuringOutage() throws Exception {
        assertSplitFailedDuringOutageDiscarded("transaction=on;", Sender::reset);
    }

    @Test
    public void testResetDuringServerOutageDiscardsTransaction() throws Exception {
        // reset() used to wait 30 s for the server to ack the rollback, then abort the
        // transaction, so every later row and flush threw even after the server came back.
        assertDiscardDuringOutage("transaction=on;", Sender::reset);
    }

    @Test
    public void testRollbackBeforeFirstConnectDiscardsStrictTransaction() throws Exception {
        assertDiscardBeforeFirstConnect("transaction=strict;", Sender::rollback);
    }

    @Test
    public void testRollbackBeforeFirstConnectDiscardsTransaction() throws Exception {
        assertDiscardBeforeFirstConnect("transaction=on;", Sender::rollback);
    }

    @Test
    public void testRollbackDiscardsTransactionAlreadySent() throws Exception {
        runInContext(port -> {
            try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(conf(port) + "transaction=on;")) {
                Assert.assertTrue("the server confirms rollback", sender.isServerRollbackEnabled());
                writeRows(sender, 0, 3_000);
                awaitTable();
                sender.rollback();
                writeRows(sender, 10_000, 10);
                sender.flush();
                Assert.assertTrue(sender.drain(60_000));
            }
            drainWalQueue();
            assertQuery("SELECT count(), min(v), max(v) FROM t").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\tmin\tmax\n10\t10000\t10009\n");
        });
    }

    @Test
    public void testRollbackDiscardsTransactionOfEveryTable() throws Exception {
        // The frames of the transaction carry rows of two tables: the rollback discards the
        // rows of both. The next transaction writes both tables over several frames too,
        // and its commit commits every row the server holds uncommitted: only its own.
        runInContext(port -> {
            try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(conf(port) + "transaction=on;")) {
                Assert.assertTrue("the server confirms rollback", sender.isServerRollbackEnabled());
                writeRowsOfTwoTables(sender, 0, 3_000);
                awaitTable("ta");
                awaitTable("tb");
                sender.rollback();
                writeRowsOfTwoTables(sender, 10_000, 3_000);
                sender.flush();
                Assert.assertTrue(sender.drain(60_000));
            }
            drainWalQueue();
            assertQuery("SELECT count(), min(v), max(v) FROM ta").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\tmin\tmax\n1500\t10000\t12998\n");
            assertQuery("SELECT count(), min(v), max(v) FROM tb").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\tmin\tmax\n1500\t10001\t12999\n");
        });
    }

    @Test
    public void testRetryOfSplitFailedDuringOutageCommitsEachRowOnce() throws Exception {
        // flush() of a split that failed partway used to send the whole batch again, so
        // the table that had reached the client buffer committed twice.
        assertRetryOfSplitFailedDuringOutageCommitsEachRowOnce("transaction=on;");
    }

    @Test
    public void testRollbackDiscardsSplitFailedDuringOutage() throws Exception {
        // The server is down and the client buffer full, so a split sends its first
        // table's frame into the room left in the last segment and fails on the second.
        // rollback() used to find no open transaction and publish nothing, so once the
        // server was back, the next transaction's commit committed the first table.
        assertSplitFailedDuringOutageDiscarded("transaction=on;", Sender::rollback);
    }

    @Test
    public void testRollbackDuringServerOutageDiscardsTransaction() throws Exception {
        assertDiscardDuringOutage("transaction=strict;", Sender::rollback);
    }

    @Test
    public void testServerLogsOnceForEachTransactionItSplits() throws Exception {
        // The client cannot see the server split its transaction at
        // qwp.max.uncommitted.rows, so the server logs it, once per transaction. The cap
        // splits each transaction of 2,500 and 3,000 rows in two or three frames, and the
        // 500-row one not at all. It commits the last rows of a 3,000-row transaction, so
        // the frame that closes it has nothing left to commit: the next transaction must
        // still log.
        qwpMaxUncommittedRows = 1_000;
        runInContext(port -> {
            final LogCapture capture = new LogCapture();
            capture.start();
            try (Sender sender = Sender.fromConfig(conf(port) + "transaction=on;auto_flush_rows=100;")) {
                int from = 0;
                for (int rows : new int[]{2_500, 2_500, 3_000, 3_000, 500}) {
                    final long txnBefore = from == 0 ? 0 : lastTxn("t");
                    writeRows(sender, from, rows);
                    sender.flush();
                    Assert.assertTrue(sender.drain(60_000));
                    capture.stop();
                    // One WAL txn per 1,000 rows at the cap, and one for the rest on flush().
                    Assert.assertEquals("WAL txns of the transaction from row " + from, rows > 1_000 ? 3 : 1,
                            lastTxn("t") - txnBefore);
                    if (rows > 1_000) {
                        capture.assertOnlyOnce(SPLIT_ADVISORY);
                    } else {
                        capture.assertNotLogged(SPLIT_ADVISORY);
                    }
                    // The next transaction's log starts empty.
                    capture.start();
                    from += rows;
                }
            } finally {
                capture.stop();
            }
            drainWalQueue();
            assertQuery("SELECT count(), count_distinct(v) FROM t").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\tcount_distinct\n11500\t11500\n");
        });
    }

    @Test
    public void testServerLogsSplitOfTransactionThatAlsoAddsColumn() throws Exception {
        // The transaction adds column x to t, which commits an ALTER and none of its rows,
        // and the cap splits its rows of u: the server logs the split, once.
        qwpMaxUncommittedRows = 1_000;
        runInContext(port -> {
            final LogCapture capture = new LogCapture();
            capture.start();
            try (Sender sender = Sender.fromConfig(conf(port) + "transaction=on;auto_flush_rows=100;")) {
                writeRows(sender, "t", 0, 300);
                writeRowsWithColumnX(sender, "t", 300, 300);
                writeRows(sender, "u", 0, 1_500);
                sender.flush();
                Assert.assertTrue(sender.drain(60_000));
                capture.stop();
                capture.assertOnlyOnce(SPLIT_ADVISORY);
            } finally {
                capture.stop();
            }
            // t: the ALTER, then all 600 rows. u: 1,000 rows at the cap, then the other 500.
            Assert.assertEquals(2, lastTxn("t"));
            Assert.assertEquals(2, lastTxn("u"));
            drainWalQueue();
            assertQuery("SELECT count(), count(x) FROM t").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\tcount1\n600\t300\n");
            assertQuery("SELECT count() FROM u").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n1500\n");
        });
    }

    @Test
    public void testStrictResetDiscardsSplitFailedDuringOutage() throws Exception {
        assertSplitFailedDuringOutageDiscarded("transaction=strict;", Sender::reset);
    }

    @Test
    public void testStrictRetryOfSplitFailedDuringOutageCommitsEachRowOnce() throws Exception {
        assertRetryOfSplitFailedDuringOutageCommitsEachRowOnce("transaction=strict;");
    }

    @Test
    public void testStrictRollbackDiscardsSplitFailedDuringOutage() throws Exception {
        assertSplitFailedDuringOutageDiscarded("transaction=strict;", Sender::rollback);
    }

    @Test
    public void testStrictCloseWithoutFlushDiscardsTransaction() throws Exception {
        runInContext(port -> {
            try (Sender sender = Sender.fromConfig(conf(port) + "transaction=strict;")) {
                writeRows(sender, 0, 3_000);
                awaitTable();
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM t").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
        });
    }

    @Test
    public void testStrictResetAfterRefusedFirstAutoFlushStartsNewTransaction() throws Exception {
        // The server's default 2 MiB receive buffer sets the default frame cap. An 8 MiB
        // client buffer is then too small for a strict transaction's first deferred frame
        // next to the room it keeps for its commit: the first auto-flush is refused, and
        // nothing is sent. reset() used to keep that refusal, so the sender refused every
        // later row until rollback() or close().
        runInContext(port -> {
            try (Sender sender = Sender.fromConfig(conf(port) + "transaction=strict;sf_max_total_bytes=8m;")) {
                Assert.assertTrue("the first auto-flush must be refused", writeUntilRefused(sender) > 0);
                Assert.assertNull("nothing was sent", engine.getTableTokenIfExists("t"));
                sender.reset();
                writeRows(sender, 10_000, 10);
                sender.flush();
                Assert.assertTrue(sender.drain(60_000));
            }
            drainWalQueue();
            assertQuery("SELECT count(), min(v), max(v) FROM t").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\tmin\tmax\n10\t10000\t10009\n");
        }, 2 * 1024 * 1024);
    }

    @Test
    public void testStrictTransactionAddingColumnLogsNoSplit() throws Exception {
        assertAddingColumnLogsNoSplit("transaction=strict;auto_flush_rows=100;");
    }

    @Test
    public void testStrictTransactionCommitsOnceAcrossFrames() throws Exception {
        assertCommitsOnlyOnFlush("auto_flush=off;");
    }

    @Test
    public void testStrictTransactionRefusesRowsPastClientBuffer() throws Exception {
        // As at the server's row limit, the row that takes the transaction past the
        // client buffer is not part of it: flush() commits the rows before it, and
        // only those, in one transaction.
        runInContext(port -> {
            int refused = -1;
            try (Sender sender = Sender.fromConfig(conf(port) + "transaction=strict;"
                    + "sf_max_segment_bytes=64k;sf_max_total_bytes=256k;")) {
                for (int i = 0; i < 100_000 && refused < 0; i++) {
                    try {
                        sender.table("t").longColumn("v", i).stringColumn("s", PAD).atNow();
                    } catch (TransactionTooLargeException e) {
                        TestUtils.assertContains(e.getMessage(), "; the client holds a strict transaction until the "
                                + "server commits it, and sf_max_total_bytes cannot hold this one, so the row was not "
                                + "added. Raise sf_max_total_bytes, or set sf_dir to keep the transaction on disk");
                        refused = i;
                    }
                }
                Assert.assertTrue("the client buffer must refuse a row", refused > 0);
                TableToken token = awaitTable();
                long txnBefore = engine.getTableSequencerAPI().lastTxn(token);
                sender.flush();
                Assert.assertTrue(sender.drain(60_000));
                Assert.assertEquals("one transaction", txnBefore + 1, engine.getTableSequencerAPI().lastTxn(token));
            }
            drainWalQueue();
            assertQuery("SELECT count(), min(v), max(v) FROM t").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\tmin\tmax\n" + refused + "\t0\t" + (refused - 1) + "\n");
        });
    }

    @Test
    public void testStrictTransactionRefusesRowsPastServerCap() throws Exception {
        qwpMaxUncommittedRows = 1_000;
        runInContext(port -> {
            try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(conf(port) + "transaction=strict;")) {
                Assert.assertEquals(1_000, sender.getServerMaxUncommittedRows());
                writeRows(sender, 0, 999);
                try {
                    writeRows(sender, 999, 1);
                    Assert.fail("the server would commit the 1000th row on its own");
                } catch (TransactionTooLargeException e) {
                    Assert.assertEquals("transaction too large for the server [table=t, rows=1000, "
                            + "maxUncommittedRows=1000]; the server commits a table's rows on its own once "
                            + "qwp.max.uncommitted.rows of them are uncommitted, which would split the transaction, "
                            + "so the row was not added. Raise qwp.max.uncommitted.rows on the server", e.getMessage());
                }
                TableToken token = awaitTable();
                long txnBefore = engine.getTableSequencerAPI().lastTxn(token);
                sender.flush();
                Assert.assertTrue(sender.drain(60_000));
                Assert.assertEquals("one transaction", txnBefore + 1, engine.getTableSequencerAPI().lastTxn(token));
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM t").noLeakCheck().noRandomAccess().expectSize().returns("count\n999\n");
        });
    }

    @Test
    public void testStrictTransactionRefusesRowsPastServerCapUnderEverySpelling() throws Exception {
        // The server writes the rows of the names the engine resolves to one table to one
        // writer, and commits it on its own once 1000 of its rows are uncommitted. The
        // engine folds each char with Character.toLowerCase(), so "t" and "T" are one
        // table, so are "t\u00e4" and "t\u00c4", and so are "ti" and "t\u0130", whose
        // \u0130 folds to an ASCII i. Each spelling used to keep its own count, so 999 rows
        // of each were accepted and the server committed 1158 of them before flush(): the
        // transaction landed in two parts. A sender with no auto-flush trigger is strict
        // too.
        for (String[] spellings : new String[][]{{"t", "T"}, {"t\u00e4", "t\u00c4"}, {"ti", "t\u0130"}}) {
            for (String mode : new String[]{"transaction=strict;", "auto_flush=off;"}) {
                qwpMaxUncommittedRows = 1_000;
                runInContext(port -> {
                    try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(conf(port) + mode)) {
                        Assert.assertTrue(mode, sender.isStrictTransactional());
                        writeRows(sender, spellings[0], 0, 500);
                        writeRows(sender, spellings[1], 500, 499);
                        for (String spelling : spellings) {
                            try {
                                writeRows(sender, spelling, 999, 1);
                                Assert.fail(mode + " the server would commit the 1000th row on its own");
                            } catch (TransactionTooLargeException e) {
                                Assert.assertEquals("transaction too large for the server [table=" + spelling
                                        + ", rows=1000, maxUncommittedRows=1000]; the server commits a table's rows on "
                                        + "its own once qwp.max.uncommitted.rows of them are uncommitted, which would "
                                        + "split the transaction, so the row was not added. Rows sent under names that "
                                        + "differ only in letter case count together: the server counts them as one "
                                        + "table. Raise qwp.max.uncommitted.rows on the server", e.getMessage());
                            }
                        }
                        TableToken token = awaitTable(spellings[0]);
                        long txnBefore = engine.getTableSequencerAPI().lastTxn(token);
                        sender.flush();
                        Assert.assertTrue(sender.drain(60_000));
                        Assert.assertEquals(mode + spellings[1] + " one transaction", txnBefore + 1,
                                engine.getTableSequencerAPI().lastTxn(token));
                    }
                    drainWalQueue();
                    assertQuery("SELECT count(), count_distinct(v), min(v), max(v) FROM \"" + spellings[1] + "\"")
                            .noLeakCheck().noRandomAccess().expectSize()
                            .returns("count\tcount_distinct\tmin\tmax\n999\t999\t0\t998\n");
                    execute("DROP TABLE \"" + spellings[0] + "\"");
                });
            }
        }
    }

    @Test
    public void testStrictTransactionRollbackUnderEverySpellingKeepsNothing() throws Exception {
        // The rows alternate between "t" and "T", one table on the server. The server
        // used to commit the first 1158 of them on its own, and rollback() kept them.
        qwpMaxUncommittedRows = 1_000;
        runInContext(port -> {
            try (Sender sender = Sender.fromConfig(conf(port) + "transaction=strict;")) {
                int refused = -1;
                for (int i = 0; i < 1_998 && refused < 0; i++) {
                    try {
                        writeRows(sender, i % 2 == 0 ? "t" : "T", i, 1);
                    } catch (TransactionTooLargeException e) {
                        refused = i;
                    }
                }
                Assert.assertEquals("the 1000th row of the table is refused", 999, refused);
                TableToken token = awaitTable();
                sender.rollback();
                Assert.assertTrue(sender.drain(60_000));
                Assert.assertEquals("nothing committed", 0, engine.getTableSequencerAPI().lastTxn(token));
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM t").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
        });
    }

    @Test
    public void testTransactionOverSpellingsOfOneTableCommitsOnce() throws Exception {
        // The client keeps a buffer per spelling of a table name, so the frames of one
        // transaction carry blocks for "tä" and "tÄ", which the engine resolves to one
        // table. The server folded only ASCII case, kept a writer per spelling, and
        // committed the transaction as two WAL transactions.
        runInContext(port -> {
            try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(conf(port) + "transaction=on;")) {
                Assert.assertTrue("the server confirms rollback", sender.isServerRollbackEnabled());
                writeRowsUnderSpellings(sender, 0, 3_000);
                TableToken token = awaitTable("tä");
                sender.rollback();
                long txnBefore = engine.getTableSequencerAPI().lastTxn(token);
                writeRowsUnderSpellings(sender, 10_000, 3_000);
                sender.flush();
                Assert.assertTrue(sender.drain(60_000));
                Assert.assertEquals("one transaction", txnBefore + 1, engine.getTableSequencerAPI().lastTxn(token));
            }
            drainWalQueue();
            assertQuery("SELECT count(), count_distinct(v), min(v), max(v) FROM \"tÄ\"").noLeakCheck().noRandomAccess()
                    .expectSize().returns("count\tcount_distinct\tmin\tmax\n3000\t3000\t10000\t12999\n");
        });
    }

    @Test
    public void testTransactionAddingColumnLogsNoSplit() throws Exception {
        assertAddingColumnLogsNoSplit("transaction=on;auto_flush_rows=100;");
    }

    @Test
    public void testTransactionalModeCommitsInPartsPastClientBuffer() throws Exception {
        // The client holds a transaction until the server commits it. transaction=on
        // used to block the row that took its transaction past sf_max_total_bytes for
        // sf_append_deadline_millis (30 s) and then throw, and closing the sender
        // committed the rows already sent and dropped the rest. Now the sender commits
        // the rows already sent and carries on, so every row lands, in parts.
        runInContext(port -> {
            try (Sender sender = Sender.fromConfig(conf(port) + "transaction=on;"
                    + "sf_max_segment_bytes=64k;sf_max_total_bytes=256k;")) {
                long start = System.nanoTime();
                // ~1 MB over two tables, four times the client buffer.
                for (int i = 0; i < 10_000; i++) {
                    sender.table(i % 2 == 0 ? "t" : "u").longColumn("v", i).stringColumn("s", PAD).atNow();
                }
                sender.flush();
                Assert.assertTrue("must not wait for sf_append_deadline_millis",
                        System.nanoTime() - start < TimeUnit.SECONDS.toNanos(20));
                Assert.assertTrue(sender.drain(60_000));
            }
            drainWalQueue();
            for (String table : new String[]{"t", "u"}) {
                Assert.assertTrue(table + " committed in parts",
                        engine.getTableSequencerAPI().lastTxn(engine.verifyTableName(table)) >= 2);
            }
            assertQuery("SELECT count(), count_distinct(v), min(v), max(v) FROM t").noLeakCheck().noRandomAccess()
                    .expectSize().returns("count\tcount_distinct\tmin\tmax\n5000\t5000\t0\t9998\n");
            assertQuery("SELECT count(), count_distinct(v), min(v), max(v) FROM u").noLeakCheck().noRandomAccess()
                    .expectSize().returns("count\tcount_distinct\tmin\tmax\n5000\t5000\t1\t9999\n");
        });
    }

    @Test
    public void testTransactionalModeCommitsInPartsPastServerCap() throws Exception {
        // transaction=on keeps its behaviour: the server commits a table on its own
        // at qwp.max.uncommitted.rows, so a larger transaction lands in parts.
        qwpMaxUncommittedRows = 1_000;
        runInContext(port -> {
            try (Sender sender = Sender.fromConfig(conf(port) + "transaction=on;")) {
                writeRows(sender, 0, 3_000);
                TableToken token = awaitTable();
                sender.flush();
                Assert.assertTrue(sender.drain(60_000));
                Assert.assertTrue("committed in parts", engine.getTableSequencerAPI().lastTxn(token) >= 2);
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM t").noLeakCheck().noRandomAccess().expectSize().returns("count\n3000\n");
        });
    }

    private static String conf(int port) {
        return "ws::addr=localhost:" + port + ";close_flush_timeout_millis=300000;";
    }

    // With the server down, commits fillers of ~78 KB until the client buffer refuses one,
    // discards that one, and returns how many reached the ring. Each filler takes a
    // segment of its own and leaves ~52 KB of it free. They are the sender's first
    // frames, so the last one has FSN fillers - 1.
    private static int fillRingDuringOutage(Sender sender) {
        for (int frames = 0; frames < 100; frames++) {
            writeRows(sender, "filler", frames * 780, 780);
            try {
                sender.flush();
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "cursor SF append failed");
                sender.reset();
                Assert.assertTrue("the ring must take a filler", frames > 0);
                return frames;
            }
        }
        Assert.fail("the ring must fill");
        return -1;
    }

    // Writes split_a (~39 KB), split_b (~78 KB) and split_c (~30 KB), past the server's
    // 128 KiB frame cap, into a ring fillRingDuringOutage filled. The split sends split_a
    // into the room left in the last segment, and split_b needs a segment the full ring
    // cannot add. The fillers carry no symbols, so split_a's frame carries the batch's
    // new symbols, which the other tables and the next transaction reference.
    private static void sendPartOfSplitDuringOutage(Sender sender) {
        writeSymbolRows(sender, "split_a", 0, 390);
        writeSymbolRows(sender, "split_b", 0, 780);
        writeSymbolRows(sender, "split_c", 0, 300);
        try {
            sender.flush();
            Assert.fail("the split's second frame cannot reach the full ring");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "cursor SF append failed");
            TestUtils.assertContains(e.getCause().getMessage(), "cursor ring backpressured");
        }
    }

    // A server that goes down after the handshake, and no auto-flush trigger that cuts a
    // batch: flush() splits a batch past the server's 128 KiB frame cap per table. 512 KiB
    // of 128 KiB segments hold a few fillers, and a refused append fails after 500 ms.
    private static String splitOutageConf(int port) {
        return "ws::addr=localhost:" + port + ";close_flush_timeout_millis=1000;"
                + "reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=50;"
                + "auto_flush_bytes=off;auto_flush_rows=100000;auto_flush_interval=off;"
                + "sf_max_segment_bytes=128k;sf_max_total_bytes=512k;sf_append_deadline_millis=500;";
    }

    // For a server that is down when the sender starts: reconnects within milliseconds once
    // the server is up, and keeps frames well within the server's cap before the sender has
    // learned it. Each test drains before it closes, so the short close drain only bounds
    // how long a failing test takes.
    private static String outageConf(int port) {
        return "ws::addr=localhost:" + port + ";close_flush_timeout_millis=1000;"
                + "reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=50;auto_flush_rows=200;";
    }

    // A deferred frame that adds column x commits the ALTER as a sequencer txn of its
    // own and none of the transaction's rows, so the transaction commits whole, and the
    // server must not log that the cap split it.
    private void assertAddingColumnLogsNoSplit(String mode) throws Exception {
        runInContext(port -> {
            final LogCapture capture = new LogCapture();
            capture.start();
            try (Sender sender = Sender.fromConfig(conf(port) + mode)) {
                writeRows(sender, 0, 1_000);
                writeRowsWithColumnX(sender, "t", 1_000, 1_000);
                sender.flush();
                Assert.assertTrue(sender.drain(60_000));
                capture.stop();
                capture.assertNotLogged(SPLIT_ADVISORY);
            } finally {
                capture.stop();
            }
            // Sequencer txn 1 is the ALTER, txn 2 commits all 2,000 rows.
            Assert.assertEquals(2, lastTxn("t"));
            drainWalQueue();
            assertQuery("SELECT count(), count(x), min(x), max(x) FROM t").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\tcount1\tmin\tmax\n2000\t1000\t1000\t1999\n");
        });
    }

    // A sender with no auto-flush trigger sends a transaction of any size in deferred
    // frames, commits it once on flush(), and closing it discards a transaction that
    // was never flushed, frames already sent included.
    private void assertCommitsOnlyOnFlush(String mode) throws Exception {
        runInContext(port -> {
            try (Sender sender = Sender.fromConfig(conf(port) + mode)) {
                writeRows(sender, 0, 5_000);
                // The deferred frames reached the server and created the table, yet
                // nothing is committed before flush().
                TableToken token = awaitTable();
                drainWalQueue();
                assertQuery("SELECT count() FROM t").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
                long txnBefore = engine.getTableSequencerAPI().lastTxn(token);
                sender.flush();
                Assert.assertTrue(sender.drain(60_000));
                Assert.assertEquals("one transaction", txnBefore + 1, engine.getTableSequencerAPI().lastTxn(token));
                writeRows(sender, 10_000, 3_000);
            }
            drainWalQueue();
            assertQuery("SELECT count(), count_distinct(v), max(v) FROM t").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\tcount_distinct\tmax\n5000\t5000\t4999\n");
        });
    }

    // The server is down when the sender starts, so the sender cannot know yet whether it
    // supports rollback. It used to assume it did not: rollback() threw and a strict reset()
    // aborted the transaction, so every later row and flush threw even once the server was
    // up, and reset() of a transaction=on transaction kept the rows already sent, so the next
    // flush committed them. Only the transaction committed after the discard may land.
    private void assertDiscardBeforeFirstConnect(String mode, Consumer<Sender> discard) throws Exception {
        assertMemoryLeak(() -> {
            int port = RestartableQwpServer.pickFreePort();
            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                try (Sender sender = Sender.fromConfig(outageConf(port) + "initial_connect_retry=async;" + mode)) {
                    writeRows(sender, 0, 600);
                    discard.accept(sender);
                    writeRows(sender, 10_000, 10);
                    sender.flush();
                    server.start();
                    Assert.assertTrue(sender.drain(60_000));
                }
            }
            drainWalQueue();
            assertQuery("SELECT count(), min(v), max(v) FROM t").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\tmin\tmax\n10\t10000\t10009\n");
        });
    }

    // The server goes down after the first frames of a transaction reached it, the
    // transaction is discarded and the next one committed while the server is down, and
    // the server comes back on the same port. The sender must keep working throughout:
    // it replays the discarded frames with the rollback frame behind them, so only the
    // next transaction commits.
    private void assertDiscardDuringOutage(String mode, Consumer<Sender> discard) throws Exception {
        assertMemoryLeak(() -> {
            int port = RestartableQwpServer.pickFreePort();
            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                server.start();
                try (Sender sender = Sender.fromConfig(conf(port)
                        + "reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=50;" + mode)) {
                    writeRows(sender, 0, 3_000);
                    awaitTable();
                    server.stop();
                    discard.accept(sender);
                    writeRows(sender, 10_000, 10);
                    sender.flush();
                    server.start();
                    Assert.assertTrue(sender.drain(60_000));
                }
            }
            drainWalQueue();
            assertQuery("SELECT count(), min(v), max(v) FROM t").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\tmin\tmax\n10\t10000\t10009\n");
        });
    }

    // The split's first table reached the server, and was discarded with the transaction.
    // Only the fillers and the transaction after the discard commit.
    private void assertSplitDiscarded(int fillers) throws Exception {
        drainWalQueue();
        // split_a's frame created the table, but none of its rows committed, and the other
        // two tables never reached the server.
        assertQuery("SELECT count() FROM split_a").noLeakCheck().noRandomAccess().expectSize()
                .returns("count\n0\n");
        Assert.assertNull(engine.getTableTokenIfExists("split_b"));
        Assert.assertNull(engine.getTableTokenIfExists("split_c"));
        // The next transaction's symbols were registered by split_a's frame, which the
        // rollback discarded: its rows commit with their own symbols.
        StringBuilder expected = new StringBuilder("v\tk\n");
        for (int v = 10_000; v < 10_010; v++) {
            expected.append(v).append("\tk").append(v % 7).append('\n');
        }
        assertQuery("SELECT v, k FROM t ORDER BY v").noLeakCheck().expectSize().returns(expected.toString());
        assertQuery("SELECT count(), count_distinct(v) FROM filler").noLeakCheck().noRandomAccess().expectSize()
                .returns("count\tcount_distinct\n" + fillers * 780 + '\t' + fillers * 780 + '\n');
    }

    // Each of the table's rows committed once, with its own symbol.
    private void assertSplitTableCommittedOnce(String table, int rows) throws Exception {
        assertQuery("SELECT count(), count_distinct(v), min(v), max(v), count_distinct(k) FROM " + table).noLeakCheck()
                .noRandomAccess().expectSize()
                .returns("count\tcount_distinct\tmin\tmax\tcount_distinct1\n" + rows + '\t' + rows + "\t0\t" + (rows - 1)
                        + "\t7\n");
        assertQuery("SELECT count() FROM " + table + " WHERE k <> concat('k', v % 7)").noLeakCheck().noRandomAccess()
                .expectSize().returns("count\n0\n");
    }

    // The server goes down after the handshake, fillers fill the client buffer, a split
    // fails after its first frame, and the transaction is discarded. The server comes
    // back, and the next transaction commits.
    private void assertSplitFailedDuringOutageDiscarded(String mode, Consumer<Sender> discard) throws Exception {
        assertMemoryLeak(() -> {
            int port = RestartableQwpServer.pickFreePort();
            int fillers;
            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                server.start();
                try (Sender sender = Sender.fromConfig(splitOutageConf(port) + mode)) {
                    server.stop();
                    fillers = fillRingDuringOutage(sender);
                    sendPartOfSplitDuringOutage(sender);
                    // Must not wait for the server: the rollback frame fits the room the
                    // first table's frame left in the last segment.
                    discard.accept(sender);
                    server.start();
                    writeSymbolRows(sender, "t", 10_000, 10);
                    sender.flush();
                    Assert.assertTrue(sender.drain(60_000));
                }
            }
            assertSplitDiscarded(fillers);
        });
    }

    // As assertSplitFailedDuringOutageDiscarded, but flush() retries the split once the
    // server is back: every row of the batch commits once.
    private void assertRetryOfSplitFailedDuringOutageCommitsEachRowOnce(String mode) throws Exception {
        assertMemoryLeak(() -> {
            int port = RestartableQwpServer.pickFreePort();
            int fillers;
            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                server.start();
                try (Sender sender = Sender.fromConfig(splitOutageConf(port) + mode)) {
                    server.stop();
                    fillers = fillRingDuringOutage(sender);
                    sendPartOfSplitDuringOutage(sender);
                    server.start();
                    // The ring replays, and the server's acks of the fillers make room.
                    Assert.assertTrue(sender.awaitAckedFsn(fillers - 1, 60_000));
                    sender.flush();
                    Assert.assertTrue(sender.drain(60_000));
                }
            }
            drainWalQueue();
            // The retry sends split_b and split_c, whose symbols split_a's frame registered.
            assertSplitTableCommittedOnce("split_a", 390);
            assertSplitTableCommittedOnce("split_b", 780);
            assertSplitTableCommittedOnce("split_c", 300);
            assertQuery("SELECT count() FROM filler").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n" + fillers * 780 + '\n');
        });
    }

    // Runs task(0) .. task(threads - 1) on as many threads at once and rethrows the
    // first failure. A failing thread breaks the others' barriers, so none waits for it.
    private static void runOnThreads(int threads, ThreadTask task) throws Exception {
        Throwable[] errors = new Throwable[threads];
        CyclicBarrier start = new CyclicBarrier(threads);
        Thread[] workers = new Thread[threads];
        for (int i = 0; i < threads; i++) {
            final int index = i;
            workers[i] = new Thread(() -> {
                try {
                    start.await(60, TimeUnit.SECONDS);
                    task.run(index);
                } catch (Throwable t) {
                    errors[index] = t;
                    start.reset();
                }
            });
            workers[i].start();
        }
        for (Thread worker : workers) {
            worker.join();
        }
        for (Throwable error : errors) {
            if (error != null) {
                throw new AssertionError(error);
            }
        }
    }

    private static void writeRows(Sender sender, int from, int count) {
        writeRows(sender, "t", from, count);
    }

    private static void writeRows(Sender sender, String table, int from, int count) {
        for (int i = from, n = from + count; i < n; i++) {
            sender.table(table).longColumn("v", i).stringColumn("s", PAD).atNow();
        }
    }

    // As writeRows, with a LONG column x equal to v.
    private static void writeRowsWithColumnX(Sender sender, String table, int from, int count) {
        for (int i = from, n = from + count; i < n; i++) {
            sender.table(table).longColumn("v", i).longColumn("x", i).stringColumn("s", PAD).atNow();
        }
    }

    // As writeRows, with a SYMBOL column k of seven values, derived from v.
    private static void writeSymbolRows(Sender sender, String table, int from, int count) {
        for (int i = from, n = from + count; i < n; i++) {
            sender.table(table).symbol("k", "k" + (i % 7)).longColumn("v", i).stringColumn("s", PAD).atNow();
        }
    }

    // Writes the rows alternately to tables ta and tb: even values to ta, odd to tb.
    private static void writeRowsOfTwoTables(Sender sender, int from, int count) {
        for (int i = from, n = from + count; i < n; i++) {
            sender.table((i & 1) == 0 ? "ta" : "tb").longColumn("v", i).stringColumn("s", PAD).atNow();
        }
    }

    // Writes the rows alternately under two spellings of one table's name.
    private static void writeRowsUnderSpellings(Sender sender, int from, int count) {
        for (int i = from, n = from + count; i < n; i++) {
            sender.table((i & 1) == 0 ? "tä" : "tÄ").longColumn("v", i).stringColumn("s", PAD).atNow();
        }
    }

    // Writes rows 0, 1, ... of the table until the sender's share of a client buffer
    // the pool's senders share refuses one, and returns the refused row.
    private static int writeUntilShareRefused(Sender sender, String table, int senders) {
        for (int i = 0; i < 100_000; i++) {
            try {
                sender.table(table).longColumn("v", i).stringColumn("s", PAD).atNow();
            } catch (TransactionTooLargeException e) {
                TestUtils.assertContains(e.getMessage(), "transaction too large for this sender's share of the client "
                        + "buffer");
                TestUtils.assertContains(e.getMessage(), ", senders=" + senders + ",");
                TestUtils.assertContains(e.getMessage(), "so the row was not added. Raise sf_max_total_bytes, lower "
                        + "sender_pool_max, or set sf_dir to keep the transaction on disk");
                return i;
            }
        }
        Assert.fail("the sender's share of the client buffer must refuse a row");
        return -1;
    }

    // Writes rows 0, 1, ... until the client buffer refuses one, and returns the
    // refused row.
    private static int writeUntilRefused(Sender sender) {
        for (int i = 0; i < 100_000; i++) {
            try {
                sender.table("t").longColumn("v", i).stringColumn("s", PAD).atNow();
            } catch (TransactionTooLargeException e) {
                TestUtils.assertContains(e.getMessage(), "transaction too large for the client buffer");
                TestUtils.assertContains(e.getMessage(), "so the row was not added");
                return i;
            }
        }
        Assert.fail("the client buffer must refuse a row");
        return -1;
    }

    private static long lastTxn(String table) {
        return engine.getTableSequencerAPI().lastTxn(engine.verifyTableName(table));
    }

    private TableToken awaitTable() throws Exception {
        return awaitTable("t");
    }

    private static TableToken awaitTable(String table) throws Exception {
        TestUtils.assertEventually(() -> Assert.assertNotNull(engine.getTableTokenIfExists(table)));
        return engine.verifyTableName(table);
    }

    @FunctionalInterface
    private interface ThreadTask {
        void run(int index) throws Exception;
    }
}
