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
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.TransactionTooLargeException;
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
    public void testRollbackDuringServerOutageDiscardsTransaction() throws Exception {
        assertDiscardDuringOutage("transaction=strict;", Sender::rollback);
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

    // For a server that is down when the sender starts: reconnects within milliseconds once
    // the server is up, and keeps frames well within the server's cap before the sender has
    // learned it. Each test drains before it closes, so the short close drain only bounds
    // how long a failing test takes.
    private static String outageConf(int port) {
        return "ws::addr=localhost:" + port + ";close_flush_timeout_millis=1000;"
                + "reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=50;auto_flush_rows=200;";
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
