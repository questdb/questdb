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
import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.TransactionTooLargeException;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.function.Consumer;

/**
 * Batches of any size and transactions over QWP/WebSocket, driven by the real
 * client against the real server: the client's byte trigger against the
 * server's advertised frame cap, strict transactions against the advertised
 * {@code qwp.max.uncommitted.rows}, and the rollback control frame.
 * <p>
 * The server under test accepts frames of at most 64 KiB, so every transaction
 * here spans many frames.
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
    public void testResetDuringServerOutageDiscardsTransaction() throws Exception {
        // reset() used to wait 30 s for the server to ack the rollback, then abort the
        // transaction, so every later row and flush threw even after the server came back.
        assertDiscardDuringOutage("transaction=on;", Sender::reset);
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
    public void testStrictTransactionCommitsOnceAcrossFrames() throws Exception {
        assertCommitsOnlyOnFlush("auto_flush=off;");
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
                    TestUtils.assertContains(e.getMessage(), "[table=t, rows=1000, maxUncommittedRows=1000]");
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

    private static void writeRows(Sender sender, int from, int count) {
        for (int i = from, n = from + count; i < n; i++) {
            sender.table("t").longColumn("v", i).stringColumn("s", PAD).atNow();
        }
    }

    private TableToken awaitTable() throws Exception {
        TestUtils.assertEventually(() -> Assert.assertNotNull(engine.getTableTokenIfExists("t")));
        return engine.verifyTableName("t");
    }
}
