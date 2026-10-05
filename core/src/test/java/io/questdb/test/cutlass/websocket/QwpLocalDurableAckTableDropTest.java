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

package io.questdb.test.cutlass.websocket;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.std.Os;
import io.questdb.test.TestServerMain;
import io.questdb.test.cutlass.qwp.AbstractQwpBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * End-to-end coverage of the {@code local} durable-ack tier when a table the sender has committed to is
 * dropped -- or dropped and re-created under the same name -- before the connection's local ack covers
 * the commit. Drives the real pinned client against a live ADAPTIVE server.
 * <p>
 * The client pops its pending OK entries in order and stops at the first entry the per-table durable
 * watermarks do not cover. A dropped table's frontier never advances again, so without special handling
 * one stranded entry blocks the trim of every later frame on the connection: the store-and-forward buffer
 * grows until {@code flush()} fails, {@code close()} times out with "data may be lost", and a restarted
 * sender replays the frames, re-creating the dropped table.
 */
public class QwpLocalDurableAckTableDropTest extends AbstractQwpBootstrapTest {

    // Long enough that no local ack can cover a commit before the test drops its table.
    private static final String LONG_GROUP_WINDOW_US = "3000000";

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testDropAfterFrontierCoversBeforeServerPollDoesNotStall() throws Exception {
        // The tracker already covers A, but the server reads the registry only on an inbound frame or a
        // keepalive PING, so the drop lands before the connection ever reports A as durable.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startAdaptive(null)) {
                createTables(serverMain, "p_a", "p_b");
                try (QwpWebSocketSender sender = localSender(serverMain, "durable_ack_keepalive_interval_millis=3000;")) {
                    commitRows(sender, "p_a", 5);
                    final TableToken tokenA = serverMain.getEngine().verifyTableName("p_a");
                    final SeqTxnTracker trackerA = serverMain.getEngine().getTableSequencerAPI().getTxnTracker(tokenA);
                    Assert.assertTrue("the local frontier must cover A",
                            waitFor(() -> trackerA.getLocalDurableSeqTxn() >= 1, 10_000));
                    Assert.assertEquals("the server must not have reported A yet",
                            0, sender.cursorSendLoopForTest().getTotalLocalDurableAcks());

                    serverMain.execute("DROP TABLE p_a");
                    assertTrimmed(sender, commitRows(sender, "p_b", 5));
                }
                assertCount(serverMain, "p_b", 5);
            }
        });
    }

    @Test
    public void testDropBeforeLocalAckDoesNotStallOrReplay() throws Exception {
        // A is committed but not yet locally durable when it is dropped. B commits after the drop. Every
        // frame must trim, close() must drain, and a sender restarted on the same sf_dir must find nothing
        // to replay, so the dropped table stays dropped and B holds its rows once.
        TestUtils.assertMemoryLeak(() -> {
            final String sfDir = temp.newFolder("sf-local-ack-drop").getAbsolutePath();
            try (TestServerMain serverMain = startAdaptive(LONG_GROUP_WINDOW_US)) {
                createTables(serverMain, "d_a", "d_b");
                try (QwpWebSocketSender sender = localSender(serverMain, "sf_dir=" + sfDir + ";")) {
                    commitRows(sender, "d_a", 5);
                    Assert.assertEquals("no local ack may precede the drop",
                            0, sender.cursorSendLoopForTest().getTotalLocalDurableAcks());

                    serverMain.execute("DROP TABLE d_a");
                    assertTrimmed(sender, commitRows(sender, "d_b", 5));
                }

                try (QwpWebSocketSender sender = localSender(serverMain, "sf_dir=" + sfDir + ";")) {
                    // The restarted sender recovers sf_dir. Everything was trimmed, so nothing replays.
                    Os.sleep(500);
                    final CursorWebSocketSendLoop loop = sender.cursorSendLoopForTest();
                    Assert.assertTrue("nothing may replay", loop == null || loop.getTotalFramesReplayed() == 0);
                }
                Assert.assertNull("the dropped table must not be re-created",
                        serverMain.getEngine().getTableTokenIfExists("d_a"));
                assertCount(serverMain, "d_b", 5);
            }
        });
    }

    @Test
    public void testDropThenWriteSameNameDoesNotStall() throws Exception {
        // The producer keeps writing to the dropped name: the next frame auto-creates a new table (a new
        // directory) under the old name. The old incarnation's frames reached seqTxn 3; the new
        // incarnation starts again at seqTxn 1 and must not leave the old entries behind.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startAdaptive(LONG_GROUP_WINDOW_US)) {
                createTables(serverMain, "r_a", "r_b");
                try (QwpWebSocketSender sender = localSender(serverMain, "")) {
                    for (int i = 0; i < 3; i++) {
                        commitRows(sender, "r_a", 2);
                    }
                    Assert.assertEquals("no local ack may precede the drop",
                            0, sender.cursorSendLoopForTest().getTotalLocalDurableAcks());

                    serverMain.execute("DROP TABLE r_a");
                    sender.table("r_a").longColumn("value", 42).at(1_000_000_000_000L, ChronoUnit.MICROS);
                    for (int i = 0; i < 5; i++) {
                        sender.table("r_b").longColumn("value", i).at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                    }
                    assertTrimmed(sender, sender.flushAndGetSequence());
                }
                assertCount(serverMain, "r_a", 1);
                assertCount(serverMain, "r_b", 5);
            }
        });
    }

    @Test
    public void testRecreatedTableIsNotTrimmedBeforeItIsDurable() throws Exception {
        // The client keys its durable watermarks by table name. A was acked through seqTxn 3, then dropped
        // and auto-created again. The new incarnation's first commit is seqTxn 1 on the server; if the wire
        // carried that raw value, the old watermark of 3 would cover it and the client would trim the frame
        // on its OK, before the commit is locally durable.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startAdaptive(LONG_GROUP_WINDOW_US)) {
                createTables(serverMain, "x_a");
                try (QwpWebSocketSender sender = localSender(serverMain, "")) {
                    long fsn = -1;
                    for (int i = 0; i < 3; i++) {
                        fsn = commitRows(sender, "x_a", 2);
                    }
                    Assert.assertTrue("A must be acked through seqTxn 3", sender.awaitAckedFsn(fsn, 15_000));

                    serverMain.execute("DROP TABLE x_a");
                    final CursorWebSocketSendLoop loop = sender.cursorSendLoopForTest();
                    final long okBefore = loop.getTotalAcks();
                    sender.table("x_a").longColumn("value", 42).at(1_000_000_000_000L, ChronoUnit.MICROS);
                    final long recreatedFsn = sender.flushAndGetSequence();
                    Assert.assertTrue("OK for the re-created table", waitFor(() -> loop.getTotalAcks() > okBefore, 10_000));
                    // The client drains its pending entries right after it enqueues an OK; let that run.
                    Os.sleep(300);
                    final long acked = sender.getAckedFsn();
                    final TableToken tokenA2 = serverMain.getEngine().verifyTableName("x_a");
                    final long frontierA2 = serverMain.getEngine().getTableSequencerAPI().getTxnTracker(tokenA2).getLocalDurableSeqTxn();
                    Assert.assertTrue("test setup: the new incarnation must not be locally durable yet [frontier="
                            + frontierA2 + ']', frontierA2 < 1);
                    Assert.assertTrue("the re-created table's frame was trimmed before it was durable [acked="
                            + acked + ", recreatedFsn=" + recreatedFsn + ']', acked < recreatedFsn);

                    assertTrimmed(sender, recreatedFsn);
                }
                assertCount(serverMain, "x_a", 1);
            }
        });
    }

    private static void assertCount(TestServerMain serverMain, String table, int expected) {
        serverMain.awaitTable(table);
        serverMain.assertSql("SELECT count() FROM " + table, "count\n" + expected + '\n');
    }

    private static void assertTrimmed(QwpWebSocketSender sender, long lastFsn) {
        Assert.assertTrue("test setup: nothing was published", lastFsn >= 0);
        Assert.assertTrue("the trim is stuck [ackedFsn=" + sender.getAckedFsn() + ", lastFsn=" + lastFsn + ']',
                sender.awaitAckedFsn(lastFsn, 15_000));
    }

    // Publishes one frame of rows to the table and waits for its OK. Returns the frame's FSN.
    private static long commitRows(QwpWebSocketSender sender, String table, int rows) {
        final CursorWebSocketSendLoop before = sender.cursorSendLoopForTest();
        final long okBefore = before == null ? 0 : before.getTotalAcks();
        for (int i = 0; i < rows; i++) {
            sender.table(table).longColumn("value", i).at(1_000_000_000_000L + i, ChronoUnit.MICROS);
        }
        final long fsn = sender.flushAndGetSequence();
        final CursorWebSocketSendLoop loop = sender.cursorSendLoopForTest();
        Assert.assertNotNull(loop);
        Assert.assertTrue("OK for " + table, waitFor(() -> loop.getTotalAcks() > okBefore, 10_000));
        return fsn;
    }

    private static void createTables(TestServerMain serverMain, String... tables) {
        for (String table : tables) {
            serverMain.execute("CREATE TABLE " + table + " (value LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        }
    }

    private static QwpWebSocketSender localSender(TestServerMain serverMain, String extra) {
        return (QwpWebSocketSender) Sender.fromConfig("ws::addr=localhost:" + serverMain.getHttpServerPort()
                + ";request_durable_ack=local;close_flush_timeout_millis=10000;" + extra);
    }

    private static boolean waitFor(BooleanSupplier condition, long millis) {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Os.sleep(10);
        }
        return condition.getAsBoolean();
    }

    // Durable epochs are disabled: a table's first apply publishes one at once, which would advance the
    // local frontier within milliseconds and race the drop. The group-commit window alone drives it here.
    private TestServerMain startAdaptive(String groupWindowUs) {
        if (groupWindowUs == null) {
            return startFragmented(
                    PropertyKey.CAIRO_COMMIT_MODE.getEnvVarName(), "adaptive",
                    PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL.getEnvVarName(), "-1"
            );
        }
        return startFragmented(
                PropertyKey.CAIRO_COMMIT_MODE.getEnvVarName(), "adaptive",
                PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL.getEnvVarName(), "-1",
                PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW.getEnvVarName(), groupWindowUs
        );
    }
}
