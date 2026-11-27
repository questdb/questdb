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
import io.questdb.client.LineSenderServerException;
import io.questdb.client.QuestDB;
import io.questdb.client.Sender;
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.TransactionTooLargeException;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Strict transactions whose rows reach a server with a lower qwp.max.uncommitted.rows
 * than the sender checked them against: rows it accepted before its first connection,
 * or before a restart or a failover lowered the limit. The server commits a table on its
 * own at that limit, which used to commit such a transaction in parts, so that a
 * rollback, close() or pooled-lease return then left most of the transaction committed.
 * The server now refuses a strict transaction at the limit instead, and the sender
 * discards it when the producer rolls it back, or halts when the producer committed it.
 * Drives the pinned client against the real server.
 */
public class QwpStrictTransactionRefusalE2ETest extends AbstractQwpWebSocketTest {
    private static final String PAD = "x".repeat(90);
    // The server's refusal of a transaction that reaches a limit of 1000 on table t, as the
    // sender reports it.
    private static final String SERVER_REFUSAL = "transaction refused by the server: strict transaction too "
            + "large [table=t, rows=1000, maxUncommittedRows=1000]; the server commits a table on its own once "
            + "qwp.max.uncommitted.rows of its rows are uncommitted, which would split the transaction, so it "
            + "discarded the transaction's uncommitted rows instead. Raise qwp.max.uncommitted.rows on the server. "
            + "The sender accepted the transaction's rows before it learned the server's qwp.max.uncommitted.rows, "
            + "and the transaction cannot commit as one. Call rollback() to discard it";
    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Before
    public void pinFragmentation() {
        sendChunk = Integer.MAX_VALUE;
        recvChunk = Integer.MAX_VALUE;
    }

    @Test
    public void testAsyncConnectRaceNeverCommitsInParts() throws Exception {
        // With the server up, the first connection of an asynchronous sender completes
        // before the producer's rows or after some of them. Either the sender learns the
        // limit in time and refuses the row that would reach it, or the transaction is
        // refused as a whole: rollback() leaves nothing of it either way.
        assertMemoryLeak(() -> {
            int port = RestartableQwpServer.pickFreePort();
            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                server.setQwpMaxUncommittedRows(1_000);
                server.start();
                for (int round = 0; round < 3; round++) {
                    String table = "r" + round;
                    try (Sender sender = Sender.fromConfig(asyncConf(port) + "transaction=strict;auto_flush_rows=100;")) {
                        writeRowsRefusingSome(sender, table, 3_000);
                        sender.rollback();
                        writeRows(sender, table, 10_000, 1);
                        sender.flush();
                        Assert.assertTrue(sender.drain(60_000));
                    }
                }
            }
            drainWalQueue();
            for (int round = 0; round < 3; round++) {
                assertQuery("SELECT count(), min(v) FROM r" + round).noLeakCheck().noRandomAccess().expectSize()
                        .returns("count\tmin\n1\t10000\n");
            }
        });
    }

    @Test
    public void testAutoFlushOffTransactionIsRefusedAsAWhole() throws Exception {
        // A sender with no auto-flush trigger is strict too. Small segments make its byte
        // trigger ship frames while the server is down: the server refuses them at the
        // limit, and rollback() leaves nothing of them.
        assertMemoryLeak(() -> {
            int port = RestartableQwpServer.pickFreePort();
            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                server.setQwpMaxUncommittedRows(1_000);
                try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(asyncConf(port)
                        + "auto_flush=off;sf_max_segment_bytes=64k;")) {
                    Assert.assertTrue(sender.isStrictTransactional());
                    writeRows(sender, "t", 0, 2_500);
                    server.start();
                    TestUtils.assertEventually(() -> Assert.assertEquals(1_000, sender.getServerMaxUncommittedRows()));
                    sender.rollback();
                    writeRows(sender, "t", 10_000, 10);
                    sender.flush();
                    Assert.assertTrue(sender.drain(60_000));
                }
            }
            assertOnlyTheNextTransactionCommitted(10);
        });
    }

    @Test
    public void testCloseDiscardsTheRefusedTransaction() throws Exception {
        // close() without flush() discards a strict transaction: the server refused this
        // one, and nothing of it commits.
        assertMemoryLeak(() -> {
            int port = RestartableQwpServer.pickFreePort();
            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                server.setQwpMaxUncommittedRows(1_000);
                try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(asyncConf(port)
                        + "transaction=strict;auto_flush_rows=100;")) {
                    writeRows(sender, "t", 0, 2_500);
                    server.start();
                    TestUtils.assertEventually(() -> Assert.assertEquals(1_000, sender.getServerMaxUncommittedRows()));
                    awaitTable("t");
                }
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM t").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
        });
    }

    @Test
    public void testCommittedRefusedTransactionHaltsTheSenderAndStaysInTheLog() throws Exception {
        // flush() published the commit of a transaction the server then refuses: being
        // strict, it can commit neither as one there nor in parts, so the sender halts and
        // keeps the transaction's frames. Once the server's limit is raised, a sender that
        // recovers the log commits the transaction as one.
        assertMemoryLeak(() -> {
            int port = RestartableQwpServer.pickFreePort();
            String sfConf = "sf_dir=" + temp.newFolder("strict-refusal-sf").getAbsolutePath() + ';';
            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                server.setQwpMaxUncommittedRows(1_000);
                Sender sender = Sender.fromConfig(asyncConf(port) + sfConf + "transaction=strict;auto_flush_rows=100;");
                try {
                    writeRows(sender, "t", 0, 2_500);
                    sender.flush();
                    server.start();
                    try {
                        sender.drain(60_000);
                        Assert.fail("the sender must halt");
                    } catch (LineSenderServerException e) {
                        SenderError err = e.getServerError();
                        Assert.assertEquals(SenderError.Category.TRANSACTION_TOO_LARGE, err.getCategory());
                        Assert.assertEquals(SenderError.Policy.TERMINAL, err.getAppliedPolicy());
                        TestUtils.assertContains(err.getServerMessage(),
                                "strict transaction too large [table=t, rows=1000, maxUncommittedRows=1000]");
                    }
                } finally {
                    sender.close();
                }
                drainWalQueue();
                assertQuery("SELECT count() FROM t").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");

                server.stop();
                server.setQwpMaxUncommittedRows(10_000);
                server.start();
                try (Sender recovered = Sender.fromConfig(syncConf(port) + sfConf + "transaction=strict;")) {
                    Assert.assertTrue(recovered.drain(60_000));
                }
            }
            drainWalQueue();
            assertQuery("SELECT count(), min(v), max(v) FROM t").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\tmin\tmax\n2500\t0\t2499\n");
            Assert.assertEquals("one transaction", 1, engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("t")));
        });
    }

    @Test
    public void testFailoverToLowerLimitRefusesTheOpenTransaction() throws Exception {
        // The sender checked its rows against the 1,000,000 of the first endpoint, which
        // goes away mid-transaction, and fails over to the second, whose limit is 1,000:
        // it refuses the replayed transaction instead of committing it in parts. The
        // endpoints share the engine, so only one of them runs at a time.
        assertMemoryLeak(() -> {
            int first = RestartableQwpServer.pickFreePort();
            int second = RestartableQwpServer.pickFreePort();
            try (
                    RestartableQwpServer a = new RestartableQwpServer(engine, configuration, first);
                    RestartableQwpServer b = new RestartableQwpServer(engine, configuration, second)
            ) {
                b.setQwpMaxUncommittedRows(1_000);
                a.start();
                try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig("ws::addr=localhost:" + first
                        + ",localhost:" + second + ";close_flush_timeout_millis=60000;reconnect_initial_backoff_millis=10;"
                        + "reconnect_max_backoff_millis=50;transaction=strict;auto_flush_rows=100;")) {
                    Assert.assertEquals(1_000_000, sender.getServerMaxUncommittedRows());
                    writeRows(sender, "t", 0, 2_500);
                    awaitTable("t");
                    a.stop();
                    b.start();
                    TestUtils.assertEventually(() -> Assert.assertEquals(1_000, sender.getServerMaxUncommittedRows()));
                    assertTransactionRefused(sender, 2_500);
                    sender.rollback();
                    writeRows(sender, "t", 10_000, 10);
                    sender.flush();
                    Assert.assertTrue(sender.drain(60_000));
                }
            }
            assertOnlyTheNextTransactionCommitted(10);
        });
    }

    @Test
    public void testPooledLeaseReturnDiscardsTheRefusedTransaction() throws Exception {
        // A lazily connecting pool starts while the server is down. A strict lease writes
        // 2,500 rows and goes back to the pool without flush(), which discards them: the
        // server refuses them at its limit of 1,000, and nothing of them commits.
        assertMemoryLeak(() -> {
            int port = RestartableQwpServer.pickFreePort();
            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                server.setQwpMaxUncommittedRows(1_000);
                try (QuestDB db = QuestDB.connect(syncConf(port) + "transaction=strict;lazy_connect=true;auto_flush_rows=100;")) {
                    try (Sender lease = db.borrowSender()) {
                        writeRows(lease, "t", 0, 2_500);
                    }
                    server.start();
                    try (Sender lease = db.borrowSender()) {
                        writeRows(lease, "t", 10_000, 10);
                        lease.flush();
                        Assert.assertTrue(lease.drain(60_000));
                    }
                }
            }
            assertOnlyTheNextTransactionCommitted(10);
        });
    }

    @Test
    public void testRefusedTransactionFailsUntilRollback() throws Exception {
        // The sender learns the limit of 1,000 only after it accepted 2,500 rows of t. The
        // transaction cannot commit as one: its rows and its flush() fail, whether the
        // sender finds that itself or hears of the server's refusal first, until
        // rollback() discards it. The sender then goes on.
        assertMemoryLeak(() -> {
            int port = RestartableQwpServer.pickFreePort();
            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                server.setQwpMaxUncommittedRows(1_000);
                try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(asyncConf(port)
                        + "transaction=strict;auto_flush_rows=100;")) {
                    writeRows(sender, "t", 0, 2_500);
                    server.start();
                    TestUtils.assertEventually(() -> Assert.assertEquals(1_000, sender.getServerMaxUncommittedRows()));
                    String refusal = assertTransactionRefused(sender, 2_500);
                    try {
                        sender.flush();
                        Assert.fail("the transaction must be refused");
                    } catch (TransactionTooLargeException e) {
                        Assert.assertEquals(refusal, e.getMessage());
                    }
                    sender.rollback();
                    writeRows(sender, "t", 10_000, 10);
                    sender.flush();
                    Assert.assertTrue(sender.drain(60_000));
                }
            }
            assertOnlyTheNextTransactionCommitted(10);
        });
    }

    @Test
    public void testRestartWithLowerLimitRefusesTheOpenTransaction() throws Exception {
        // The sender checked its rows against the 1,000,000 the server advertised, and the
        // server restarts with a limit of 1,000 mid-transaction. It refuses the replayed
        // transaction instead of committing 2,000 of its rows, so rollback() discards all.
        assertMemoryLeak(() -> {
            int port = RestartableQwpServer.pickFreePort();
            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                server.start();
                try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(syncConf(port)
                        + "transaction=strict;auto_flush_rows=100;")) {
                    Assert.assertEquals(1_000_000, sender.getServerMaxUncommittedRows());
                    writeRows(sender, "t", 0, 2_500);
                    awaitTable("t");
                    server.stop();
                    server.setQwpMaxUncommittedRows(1_000);
                    server.start();
                    TestUtils.assertEventually(() -> Assert.assertEquals(1_000, sender.getServerMaxUncommittedRows()));
                    assertTransactionRefused(sender, 2_500);
                    sender.rollback();
                    writeRows(sender, "t", 10_000, 10);
                    sender.flush();
                    Assert.assertTrue(sender.drain(60_000));
                }
            }
            assertOnlyTheNextTransactionCommitted(10);
        });
    }

    @Test
    public void testRollbackBeforeFirstConnectDiscardsTheRefusedTransaction() throws Exception {
        // Rows and rollback() both come before the server is up. The server refuses the
        // replayed transaction at its limit of 1,000, where it used to commit its first
        // 2,000 rows, and the rollback frame behind it then has nothing left to discard.
        assertMemoryLeak(() -> {
            int port = RestartableQwpServer.pickFreePort();
            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                server.setQwpMaxUncommittedRows(1_000);
                try (Sender sender = Sender.fromConfig(asyncConf(port) + "transaction=strict;auto_flush_rows=100;")) {
                    writeRows(sender, "t", 0, 2_500);
                    sender.rollback();
                    server.start();
                    writeRows(sender, "t", 10_000, 10);
                    sender.flush();
                    Assert.assertTrue(sender.drain(60_000));
                }
            }
            assertOnlyTheNextTransactionCommitted(10);
        });
    }

    @Test
    public void testSymbolsOfADiscardedTransactionResolveInTheNext() throws Exception {
        // The refused transaction's last frames register five symbols, which the next
        // transaction references without registering them again. Whether those frames
        // went out before the refusal or never did, they reach the next connection, so
        // the next transaction's rows carry the right symbols.
        assertMemoryLeak(() -> {
            int port = RestartableQwpServer.pickFreePort();
            try (RestartableQwpServer server = new RestartableQwpServer(engine, configuration, port)) {
                server.setQwpMaxUncommittedRows(1_000);
                try (Sender sender = Sender.fromConfig(asyncConf(port) + "transaction=strict;auto_flush_rows=100;")) {
                    for (int i = 0; i < 2_500; i++) {
                        String symbol = i < 2_000 ? "early" + (i % 7) : "late" + (i % 5);
                        sender.table("t").symbol("k", symbol).longColumn("v", i).stringColumn("s", PAD).atNow();
                    }
                    sender.rollback();
                    server.start();
                    for (int i = 0; i < 5; i++) {
                        sender.table("t").symbol("k", "late" + i).longColumn("v", 10_000 + i).stringColumn("s", PAD).atNow();
                    }
                    sender.table("t").symbol("k", "new").longColumn("v", 10_005).stringColumn("s", PAD).atNow();
                    sender.flush();
                    Assert.assertTrue(sender.drain(60_000));
                }
            }
            drainWalQueue();
            assertQuery("SELECT k, v FROM t ORDER BY v").noLeakCheck().expectSize().returns("""
                    k\tv
                    late0\t10000
                    late1\t10001
                    late2\t10002
                    late3\t10003
                    late4\t10004
                    new\t10005
                    """);
        });
    }

    @Test
    public void testTransactionOnStillCommitsInPartsAtTheLimit() throws Exception {
        // transaction=on accepts that the server commits a transaction in parts at
        // qwp.max.uncommitted.rows: its frames do not ask for strictness, so the server
        // commits at the limit as before instead of refusing.
        qwpMaxUncommittedRows = 1_000;
        runInContext(port -> {
            try (Sender sender = Sender.fromConfig(syncConf(port) + "transaction=on;auto_flush_rows=100;")) {
                writeRows(sender, "t", 0, 2_500);
                sender.flush();
                Assert.assertTrue(sender.drain(60_000));
            }
            drainWalQueue();
            assertQuery("SELECT count(), min(v), max(v) FROM t").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\tmin\tmax\n2500\t0\t2499\n");
            Assert.assertEquals("committed at 1,000 and 2,000 rows, then by flush()", 3,
                    engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("t")));
        });
    }

    private static String asyncConf(int port) {
        return syncConf(port) + "initial_connect_retry=async;";
    }

    // Asserts the next row of the open transaction fails because it cannot commit as one,
    // and returns the refusal. The sender either finds itself that t holds rows past the
    // limit it learned, or reports the server's refusal of the transaction, which reaches
    // it from the I/O thread.
    private static String assertTransactionRefused(Sender sender, int rows) {
        try {
            writeRows(sender, "t", rows, 1);
            Assert.fail("the transaction must be refused");
            return null;
        } catch (TransactionTooLargeException e) {
            String overLimit = "transaction too large for the server [table=t, rows=" + rows
                    + ", maxUncommittedRows=1000]; the sender accepted these rows of the table before it learned "
                    + "the server's qwp.max.uncommitted.rows, at which the server commits a table's rows on its own, "
                    + "so the transaction cannot commit as one. Call rollback() to discard it, and raise "
                    + "qwp.max.uncommitted.rows on the server";
            if (!overLimit.equals(e.getMessage())) {
                Assert.assertEquals(SERVER_REFUSAL, e.getMessage());
            }
            return e.getMessage();
        }
    }

    private static TableToken awaitTable(String table) throws Exception {
        TestUtils.assertEventually(() -> Assert.assertNotNull(engine.getTableTokenIfExists(table)));
        return engine.verifyTableName(table);
    }

    // Reconnects within milliseconds once the server is back.
    private static String syncConf(int port) {
        return "ws::addr=localhost:" + port + ";close_flush_timeout_millis=60000;"
                + "reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=50;";
    }

    private static void writeRows(Sender sender, String table, int from, int count) {
        for (int i = from, n = from + count; i < n; i++) {
            sender.table(table).longColumn("v", i).stringColumn("s", PAD).atNow();
        }
    }

    // Writes count rows; the rows the transaction refuses are not part of it.
    private static void writeRowsRefusingSome(Sender sender, String table, int count) {
        for (int i = 0; i < count; i++) {
            try {
                sender.table(table).longColumn("v", i).stringColumn("s", PAD).atNow();
            } catch (TransactionTooLargeException ignore) {
                // the row is not part of the transaction
            }
        }
    }

    // Table t holds exactly the rows 10000.. of the transaction after the refused one,
    // which committed as one WAL transaction, and nothing of the refused one.
    private void assertOnlyTheNextTransactionCommitted(int rows) throws Exception {
        drainWalQueue();
        assertQuery("SELECT count(), min(v), max(v) FROM t").noLeakCheck().noRandomAccess().expectSize()
                .returns("count\tmin\tmax\n" + rows + "\t10000\t" + (10_000 + rows - 1) + "\n");
        Assert.assertEquals("one transaction", 1, engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("t")));
    }
}
