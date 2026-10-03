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

import io.questdb.client.Sender;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

public class QwpSchemaFeedbackE2ETest extends AbstractQwpWebSocketTest {

    @Test
    public void testReservedAckCapacityReturnsEveryNamedResult() throws Exception {
        assertReservedCapacity(Integer.MAX_VALUE, false);
    }

    @Test
    public void testReservedAckCapacityReturnsEveryNamedResultWhenFragmented() throws Exception {
        assertReservedCapacity(1, false);
    }

    @Test
    public void testReservedNackCapacityReturnsEveryNamedResult() throws Exception {
        assertReservedCapacity(Integer.MAX_VALUE, true);
    }

    @Test
    public void testReservedNackCapacityReturnsEveryNamedResultWhenFragmented() throws Exception {
        assertReservedCapacity(1, true);
    }

    @Test
    public void testReservedAckCapacityPreservesUnrelatedSenderCache() throws Exception {
        createWideTable("feedback_a", 11, true);
        createWideTable("feedback_b", 11, true);
        execute("CREATE TABLE feedback_retained (n LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        runInContext(port -> {
            for (boolean isReversed : new boolean[]{false, true}) {
                String first = isReversed ? "feedback_b" : "feedback_a";
                String second = isReversed ? "feedback_a" : "feedback_b";
                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port
                        + ";auto_flush_rows=2147483647;auto_flush_bytes=0;auto_flush_interval=2147483646;"
                        + "schema_mode=auto;close_flush_timeout_millis=10000;")) {
                    sender.table("feedback_retained").longColumn("n", 7).atNow();
                    long fsn = sender.flushAndGetSequence();
                    Assert.assertEquals(0, fsn);
                    Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    CursorWebSocketSendLoop loop = senderLoop(ws);
                    QwpSchemaResponse retained = loop.peekSchema("feedback_retained");
                    Assert.assertNotNull(retained);
                    Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, retained.getResult());

                    sender.table(first).longColumn("n", 1).atNow();
                    sender.table(second).longColumn("n", 2).atNow();
                    Assert.assertTrue(loop.peekSchema(first).getRequestId() > 0);
                    Assert.assertTrue(loop.peekSchema(second).getRequestId() > 0);
                    fsn = sender.flushAndGetSequence();
                    Assert.assertEquals(1, fsn);
                    Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
                    Assert.assertSame("capacity must preserve unrelated cache", retained, loop.peekSchema("feedback_retained"));
                    Assert.assertNull(loop.peekSchema(first));
                    Assert.assertNull(loop.peekSchema(second));

                    sender.table(first).longColumn("n", 3).atNow();
                    sender.table(second).longColumn("n", 4).atNow();
                    Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, loop.peekSchema(first).getResult());
                    Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, loop.peekSchema(second).getResult());
                    Assert.assertTrue(loop.peekSchema(first).getRequestId() > 0);
                    Assert.assertTrue(loop.peekSchema(second).getRequestId() > 0);
                    Assert.assertNotNull(ws.getTableBuffer(first).getSchemaBinding());
                    Assert.assertNotNull(ws.getTableBuffer(second).getSchemaBinding());
                    fsn = sender.flushAndGetSequence();
                    Assert.assertEquals(2, fsn);
                    Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
                    Assert.assertSame(retained, loop.peekSchema("feedback_retained"));
                }
            }
            drainWalQueue();
            assertQuery("SELECT count(), sum(n) FROM (SELECT n FROM feedback_a UNION ALL SELECT n FROM feedback_b)")
                    .noLeakCheck().expectSize().noRandomAccess().returns("count\tsum\n8\t20\n");
            assertQuery("SELECT n FROM feedback_retained").noLeakCheck().expectSize().returns("n\n7\n7\n");
        }, 65_536, 1, 1, 512, null);
    }

    @Test
    public void testSharedAckCapacityReturnsTransientNamedResult() throws Exception {
        assertSharedAckCapacity(Integer.MAX_VALUE);
    }

    @Test
    public void testSharedAckCapacityReturnsTransientNamedResultWhenFragmented() throws Exception {
        assertSharedAckCapacity(1);
    }

    @Test
    public void testSharedAckCapacityRebindsOnlyAffectedSenderTable() throws Exception {
        createWideTable("feedback_a", 9, true);
        createWideTable("feedback_b", 9, true);
        runInContext(port -> {
            for (boolean isReversed : new boolean[]{false, true}) {
                String first = isReversed ? "feedback_b" : "feedback_a";
                String second = isReversed ? "feedback_a" : "feedback_b";
                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port
                        + ";auto_flush_rows=2147483647;auto_flush_bytes=0;auto_flush_interval=2147483646;"
                        + "schema_mode=auto;close_flush_timeout_millis=10000;")) {
                    sender.table(first).longColumn("n", 1).atNow();
                    sender.table(second).longColumn("n", 2).atNow();
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    Assert.assertNotNull(ws.getTableBuffer(first).getSchemaBinding());
                    Assert.assertNotNull(ws.getTableBuffer(second).getSchemaBinding());
                    CursorWebSocketSendLoop loop = senderLoop(ws);
                    Assert.assertTrue(loop.peekSchema(first).getRequestId() > 0);
                    Assert.assertTrue(loop.peekSchema(second).getRequestId() > 0);
                    long fsn = sender.flushAndGetSequence();
                    Assert.assertEquals(0, fsn);
                    Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));

                    QwpSchemaResponse firstCached = loop.peekSchema(first);
                    QwpSchemaResponse secondCached = loop.peekSchema(second);
                    Assert.assertTrue("capacity must evict exactly one table", (firstCached == null) != (secondCached == null));
                    String retainedTable = firstCached != null ? first : second;
                    String affectedTable = firstCached == null ? first : second;
                    QwpSchemaResponse retained = loop.peekSchema(retainedTable);
                    Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, retained.getResult());
                    Assert.assertEquals(0, retained.getRequestId());

                    for (int batch = 1; batch <= 3; batch++) {
                        sender.table(affectedTable).longColumn("n", batch + 2).atNow();
                        QwpSchemaResponse restored = loop.peekSchema(affectedTable);
                        Assert.assertNotNull(restored);
                        Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, restored.getResult());
                        Assert.assertTrue("ordinary lookup must restore the binding", restored.getRequestId() > 0);
                        Assert.assertNotNull(ws.getTableBuffer(affectedTable).getSchemaBinding());
                        sender.table(retainedTable).longColumn("n", batch + 5).atNow();
                        Assert.assertSame("unrelated cache must survive lookup and later ACKs", retained, loop.peekSchema(retainedTable));
                        fsn = sender.flushAndGetSequence();
                        Assert.assertEquals(batch, fsn);
                        Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
                        Assert.assertSame(retained, loop.peekSchema(retainedTable));
                    }
                }
            }
            drainWalQueue();
            assertQuery("SELECT count(), sum(n) FROM (SELECT n FROM feedback_a UNION ALL SELECT n FROM feedback_b)")
                    .noLeakCheck().expectSize().noRandomAccess().returns("count\tsum\n16\t72\n");
        }, 65_536, 1, 1, 512, null);
    }

    @Test
    public void testSharedNackCapacityReturnsTransientNamedResult() throws Exception {
        createWideTable("feedback_a", 9, true);
        createWideTable("feedback_b", 9, false);
        runInContext(port -> {
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer accepted = longTable("feedback_a", 1);
                 QwpTableBuffer rejected = longTable("feedback_b", 2)) {
                describe(client, 80, "feedback_a");
                describe(client, 81, "feedback_b");
                encoder.beginMessage(2, new GlobalSymbolDictionary(), -1, -1);
                encoder.addTable(accepted);
                encoder.addTable(rejected);
                int length = encoder.finishMessage();
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                WebSocketResponse nack = receive(client);
                Assert.assertFalse(nack.isSuccess());
                Assert.assertEquals(0, nack.getSequence());
                Assert.assertFalse(nack.isSchemaInvalidation());
                Assert.assertEquals(2, nack.getSchemaUpdateCount());
                assertContainsUpdate(nack, "feedback_a", QwpSchemaProtocol.RESULT_KNOWN);
                assertContainsUpdate(nack, "feedback_b", QwpSchemaProtocol.RESULT_UNAVAILABLE);
                describe(client, 82, "feedback_b");
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM feedback_a").noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");
            assertQuery("SELECT count() FROM feedback_b").noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");
        }, 65_536, 1, 1, 512, null);
    }

    @Test
    public void testDeferredTwoTableCommitReturnsBothUpdates() throws Exception {
        execute("create table feedback_deferred_a (n long, ts timestamp) timestamp(ts) partition by day wal");
        execute("create table feedback_deferred_b (n long, ts timestamp) timestamp(ts) partition by day wal");
        runInContext(port -> {
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer a = longTable("feedback_deferred_a", 1);
                 QwpTableBuffer b = longTable("feedback_deferred_b", 2)) {
                encoder.setDeferCommit(true);
                encoder.beginMessage(2, new GlobalSymbolDictionary(), -1, -1);
                encoder.addTable(a);
                encoder.addTable(b);
                int length = encoder.finishMessage();
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                sendCommit(client, encoder);
                WebSocketResponse ack = receive(client);
                Assert.assertTrue(ack.isSuccess());
                Assert.assertEquals(1, ack.getSequence());
                Assert.assertEquals(2, ack.getSchemaUpdateCount());
                assertContainsUpdate(ack, "feedback_deferred_a");
                assertContainsUpdate(ack, "feedback_deferred_b");
            }
        });
    }

    @Test
    public void testDeferredFeedbackIsSampledAfterAlterAtResponseTime() throws Exception {
        execute("create table feedback_fresh (n long, ts timestamp) timestamp(ts) partition by day wal");
        runInContext(port -> {
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = longTable("feedback_fresh", 1)) {
                QwpSchemaResponse before = describe(client, 70, "feedback_fresh");
                encoder.setDeferCommit(true);
                sendSchema(client, encoder, table);
                QwpSchemaResponse queued = describe(client, 71, "feedback_fresh");
                Assert.assertEquals(before.getMetadataVersion(), queued.getMetadataVersion());
                execute("alter table feedback_fresh add column later long");
                QwpSchemaResponse after = describe(client, 72, "feedback_fresh");
                Assert.assertTrue(after.getMetadataVersion() > before.getMetadataVersion());
                Assert.assertEquals("later", after.getColumnName(2));
                sendCommit(client, encoder);
                WebSocketResponse ack = receive(client);
                Assert.assertEquals(1, ack.getSequence());
                Assert.assertEquals(1, ack.getSchemaUpdateCount());
                Assert.assertEquals(after.getMetadataVersion(), ack.getSchemaUpdate(0).getMetadataVersion());
                Assert.assertEquals("later", ack.getSchemaUpdate(0).getColumnName(2));
            }
        });
    }

    @Test
    public void testFragmentedAckThenNackKeepDistinctSchemaSets() throws Exception {
        execute("create table feedback_ack (n long, ts timestamp) timestamp(ts) partition by day wal");
        execute("create table feedback_nack (n long)");
        runInContext(port -> {
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer accepted = longTable("feedback_ack", 1);
                 QwpTableBuffer rejected = longTable("feedback_nack", 2)) {
                sendSchema(client, encoder, accepted);
                sendSchema(client, encoder, rejected);

                WebSocketResponse ack = receive(client);
                Assert.assertTrue(ack.isSuccess());
                Assert.assertEquals(0, ack.getSequence());
                Assert.assertEquals(1, ack.getSchemaUpdateCount());
                Assert.assertEquals("feedback_ack", ack.getSchemaUpdateTableName(0));

                WebSocketResponse nack = receive(client);
                Assert.assertFalse(nack.isSuccess());
                Assert.assertEquals(1, nack.getSequence());
                Assert.assertEquals(1, nack.getSchemaUpdateCount());
                Assert.assertEquals("feedback_nack", nack.getSchemaUpdateTableName(0));
            }
            drainWalQueue();
            assertQuery("select n from feedback_ack").noLeakCheck().expectSize().returns("n\n1\n");
            assertQuery("select count() from feedback_nack").noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");
        }, 65_536, Integer.MAX_VALUE, 1);
    }

    @Test
    public void testDropAndRecreateReportsNewTableOnSameConnection() throws Exception {
        execute("create table feedback_recreate (n long, ts timestamp) timestamp(ts) partition by day wal");
        runInContext(port -> {
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = longTable("feedback_recreate", 1)) {
                WebSocketResponse first = assertFeedback(client, encoder, table, 0, 1);
                int originalTableId = first.getSchemaUpdate(0).getTableId();
                assertFeedback(client, encoder, table, 1, 0);

                execute("drop table feedback_recreate");
                execute("create table feedback_recreate (n long, ts timestamp) timestamp(ts) partition by day wal");
                WebSocketResponse recreated = assertFeedback(client, encoder, table, 2, 1);
                Assert.assertNotEquals(originalTableId, recreated.getSchemaUpdate(0).getTableId());
                assertFeedback(client, encoder, table, 3, 0);
            }
            drainWalQueue();
            assertQuery("select n from feedback_recreate").noLeakCheck().expectSize().returns("n\n1\n1\n");
        });
    }

    @Test
    public void testFramesReportEachSchemaVersionOnce() throws Exception {
        execute("create table feedback_once (n long, ts timestamp) timestamp(ts) partition by day wal");
        runInContext(port -> {
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = longTable("feedback_once", 1)) {
                // The first frame of a table on a connection earns the schema; later
                // frames earn it again only when the version changes.
                assertFeedback(client, encoder, table, 0, 1);
                assertFeedback(client, encoder, table, 1, 0);
                assertFeedback(client, encoder, table, 2, 0);

                // A column added through a data frame is a new version: report it once.
                QwpTableBuffer widened = new QwpTableBuffer("feedback_once");
                try {
                    widened.getOrCreateColumn("n", QwpConstants.TYPE_LONG, true).addLong(2);
                    widened.getOrCreateColumn("extra", QwpConstants.TYPE_LONG, true).addLong(3);
                    widened.nextRow();
                    WebSocketResponse ack = assertFeedback(client, encoder, widened, 3, 1);
                    Assert.assertEquals("extra", ack.getSchemaUpdate(0).getColumnName(2));
                } finally {
                    widened.close();
                }
                assertFeedback(client, encoder, table, 4, 0);
            }
            // A new connection starts without reported versions.
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = longTable("feedback_once", 1)) {
                assertFeedback(client, encoder, table, 0, 1);
                assertFeedback(client, encoder, table, 1, 0);
            }
        });
    }

    @Test
    public void testOversizedColumnCountPreservesMixedFeedback() throws Exception {
        // The designated timestamp adds one more column than the protocol limit.
        assertOversizedSnapshotPreservesMixedFeedback(QwpConstants.MAX_COLUMNS_PER_TABLE, 1_048_576);
    }

    @Test
    public void testOversizedSnapshotPreservesMixedFeedback() throws Exception {
        assertOversizedSnapshotPreservesMixedFeedback(20, 512);
    }

    @Test
    public void testOversizedSnapshotReturnsNamedResultOnNack() throws Exception {
        createWideTable("feedback_non_wal_wide", 20, false);
        runInContext(port -> {
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = longTable("feedback_non_wal_wide", 1)) {
                sendSchema(client, encoder, table);
                WebSocketResponse response = receive(client);
                Assert.assertFalse(response.isSuccess());
                Assert.assertEquals(0, response.getSequence());
                Assert.assertFalse(response.isSchemaInvalidation());
                Assert.assertEquals(1, response.getSchemaUpdateCount());
                assertContainsUpdate(response, "feedback_non_wal_wide", QwpSchemaProtocol.RESULT_TOO_LARGE);
            }
        }, 65_536, 65_536, 65_536, 512, null);
        assertQuery("SELECT count() FROM feedback_non_wal_wide").expectSize().noRandomAccess().returns("count\n0\n");
    }

    @Test
    public void testExactCapacityAckFallsBackToInvalidationWithoutLosingTables() throws Exception {
        runInContext(port -> {
            String tableA = repeat('a', 110);
            String tableB = repeat('b', 111);
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer a = longTable(tableA, 1);
                 QwpTableBuffer b = longTable(tableB, 2)) {
                encoder.beginMessage(2, new GlobalSymbolDictionary(), -1, -1);
                encoder.addTable(a);
                encoder.addTable(b);
                int length = encoder.finishMessage();
                client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                WebSocketResponse response = receive(client);
                Assert.assertTrue(response.isSuccess());
                Assert.assertTrue(response.isSchemaInvalidation());
                Assert.assertFalse(response.hasSchemaUpdates());
                Assert.assertEquals(2, response.getTableEntryCount());
                Assert.assertTrue(tableA.equals(response.getTableName(0)) || tableA.equals(response.getTableName(1)));
                Assert.assertTrue(tableB.equals(response.getTableName(0)) || tableB.equals(response.getTableName(1)));
            }
            drainWalQueue();
            assertQuery("select n from \"" + tableA + "\"").noLeakCheck().expectSize().returns("n\n1\n");
            assertQuery("select n from \"" + tableB + "\"").noLeakCheck().expectSize().returns("n\n2\n");
        }, 65_536, 65_536, 65_536, 256, null);
    }

    @Test
    public void testPreTableUpdateFailureReturnsCurrentAuthorizedSchemaOnNack() throws Exception {
        execute("create table feedback_non_wal (n long)");
        runInContext(port -> {
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer table = longTable("feedback_non_wal", 7)) {
                sendSchema(client, encoder, table);
                WebSocketResponse response = receive(client);
                Assert.assertFalse(response.isSuccess());
                Assert.assertEquals(0, response.getSequence());
                Assert.assertTrue(response.hasSchemaUpdates());
                Assert.assertEquals(1, response.getSchemaUpdateCount());
                Assert.assertEquals("feedback_non_wal", response.getSchemaUpdateTableName(0));
                Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, response.getSchemaUpdate(0).getResult());
                Assert.assertEquals("n", response.getSchemaUpdate(0).getColumnName(0));
            }
        }, 65_536, Integer.MAX_VALUE, 1);
        assertQuery("select count() from feedback_non_wal").noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");
    }

    private static WebSocketClient connect(int port) {
        WebSocketClient client = WebSocketClientFactory.newPlainTextInstance();
        boolean success = false;
        try {
            client.requestQwpSchema();
            client.connect("127.0.0.1", port);
            client.upgrade("/write/v4", null);
            Assert.assertTrue(client.isQwpSchemaEnabled());
            success = true;
            return client;
        } finally {
            if (!success) {
                client.close();
            }
        }
    }

    private static CursorWebSocketSendLoop senderLoop(QwpWebSocketSender sender) throws Exception {
        // The sender has no public cache surface; use its real loop's stable
        // cache-only API rather than inspecting the coordinator's map.
        Field field = QwpWebSocketSender.class.getDeclaredField("cursorSendLoop");
        field.setAccessible(true);
        return (CursorWebSocketSendLoop) field.get(sender);
    }

    private void assertSharedAckCapacity(int fragmentSize) throws Exception {
        createWideTable("feedback_a", 9, true);
        createWideTable("feedback_b", 9, true);
        runInContext(port -> {
            for (boolean isReversed : new boolean[]{false, true}) {
                String first = isReversed ? "feedback_b" : "feedback_a";
                String second = isReversed ? "feedback_a" : "feedback_b";
                try (WebSocketClient client = connect(port);
                     QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                     QwpTableBuffer a = longTable(first, 1);
                     QwpTableBuffer b = longTable(second, 2)) {
                    describe(client, 80, first);
                    describe(client, 81, second);
                    encoder.beginMessage(2, new GlobalSymbolDictionary(), -1, -1);
                    encoder.addTable(a);
                    encoder.addTable(b);
                    int length = encoder.finishMessage();
                    client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                    WebSocketResponse ack = receive(client);
                    Assert.assertTrue(ack.isSuccess());
                    Assert.assertEquals(0, ack.getSequence());
                    Assert.assertEquals(2, ack.getTableEntryCount());
                    Assert.assertEquals(first, ack.getTableName(0));
                    Assert.assertEquals(second, ack.getTableName(1));
                    Assert.assertTrue(ack.getTableSeqTxn(0) >= 0);
                    Assert.assertTrue(ack.getTableSeqTxn(1) >= 0);
                    Assert.assertFalse(ack.isSchemaInvalidation());
                    Assert.assertEquals(2, ack.getSchemaUpdateCount());
                    Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, ack.getSchemaUpdate(0).getResult());
                    Assert.assertEquals(QwpSchemaProtocol.RESULT_UNAVAILABLE, ack.getSchemaUpdate(1).getResult());
                    Assert.assertNotEquals(ack.getSchemaUpdateTableName(0), ack.getSchemaUpdateTableName(1));
                    assertContainsUpdate(ack, first, ack.getSchemaUpdateTableName(0).equals(first)
                            ? QwpSchemaProtocol.RESULT_KNOWN : QwpSchemaProtocol.RESULT_UNAVAILABLE);
                    assertContainsUpdate(ack, second, ack.getSchemaUpdateTableName(0).equals(second)
                            ? QwpSchemaProtocol.RESULT_KNOWN : QwpSchemaProtocol.RESULT_UNAVAILABLE);
                    describe(client, 82, ack.getSchemaUpdateTableName(1));
                }
            }
            drainWalQueue();
            assertQuery("SELECT n FROM feedback_a ORDER BY n").noLeakCheck().expectSize().returns("n\n1\n2\n");
            assertQuery("SELECT n FROM feedback_b ORDER BY n").noLeakCheck().expectSize().returns("n\n1\n2\n");
        }, 65_536, fragmentSize, fragmentSize, 512, null);
    }

    private static WebSocketResponse assertFeedback(
            WebSocketClient client,
            QwpWebSocketEncoder encoder,
            QwpTableBuffer table,
            long expectedSequence,
            int expectedUpdates
    ) {
        int length = encoder.encode(table);
        client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
        WebSocketResponse ack = receive(client);
        Assert.assertTrue(ack.isSuccess());
        Assert.assertEquals(expectedSequence, ack.getSequence());
        Assert.assertFalse(ack.isSchemaInvalidation());
        Assert.assertEquals("seq=" + expectedSequence, expectedUpdates, ack.getSchemaUpdateCount());
        if (expectedUpdates > 0) {
            assertContainsUpdate(ack, table.getTableName());
        }
        return ack;
    }

    private static void assertContainsUpdate(WebSocketResponse response, String tableName) {
        assertContainsUpdate(response, tableName, QwpSchemaProtocol.RESULT_KNOWN);
    }

    private static void assertContainsUpdate(WebSocketResponse response, String tableName, int result) {
        for (int i = 0; i < response.getSchemaUpdateCount(); i++) {
            if (tableName.equals(response.getSchemaUpdateTableName(i))) {
                Assert.assertEquals(result, response.getSchemaUpdate(i).getResult());
                return;
            }
        }
        Assert.fail("missing schema feedback for " + tableName);
    }

    private static QwpSchemaResponse describe(WebSocketClient client, long requestId, String tableName) {
        byte[] request = QwpSchemaProtocol.encodeDescribe(requestId, tableName);
        long address = Unsafe.malloc(request.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < request.length; i++) {
                Unsafe.putByte(address + i, request[i]);
            }
            client.sendBinary(address, request.length);
            final QwpSchemaResponse[] response = new QwpSchemaResponse[1];
            Assert.assertTrue(client.receiveFrame(new WebSocketFrameHandler() {
                @Override
                public void onBinaryMessage(long payloadPtr, int payloadLen) {
                    response[0] = QwpSchemaProtocol.decodeResponse(payloadPtr, payloadLen);
                }

                @Override
                public void onClose(int code, String reason) {
                    Assert.fail("unexpected close [code=" + code + ", reason=" + reason + ']');
                }
            }, 5_000));
            Assert.assertNotNull(response[0]);
            Assert.assertEquals(requestId, response[0].getRequestId());
            Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, response[0].getResult());
            return response[0];
        } finally {
            Unsafe.free(address, request.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static QwpTableBuffer longTable(String tableName, long value) {
        QwpTableBuffer table = new QwpTableBuffer(tableName);
        table.getOrCreateColumn("n", QwpConstants.TYPE_LONG, true).addLong(value);
        table.nextRow();
        return table;
    }

    private static WebSocketResponse receive(WebSocketClient client) {
        WebSocketResponse response = new WebSocketResponse();
        Assert.assertTrue(client.receiveFrame(new WebSocketFrameHandler() {
            @Override
            public void onBinaryMessage(long payloadPtr, int payloadLen) {
                Assert.assertTrue(response.readFrom(payloadPtr, payloadLen, true));
            }

            @Override
            public void onClose(int code, String reason) {
                Assert.fail("unexpected close [code=" + code + ", reason=" + reason + ']');
            }
        }, 5_000));
        return response;
    }

    private static void sendCommit(WebSocketClient client, QwpWebSocketEncoder encoder) {
        encoder.getBuffer().reset();
        encoder.setDeferCommit(false);
        encoder.writeHeader(0, 0);
        client.sendBinary(encoder.getBuffer().getBufferPtr(), encoder.getBuffer().getPosition());
    }

    private static void sendSchema(WebSocketClient client, QwpWebSocketEncoder encoder, QwpTableBuffer table) {
        int length = encoder.encode(table);
        client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
    }

    private static String repeat(char c, int count) {
        StringBuilder sink = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            sink.append(c);
        }
        return sink.toString();
    }

    private void assertOversizedSnapshotPreservesMixedFeedback(int columnCount, int sendBufferSize) throws Exception {
        createWideTable("feedback_wide", columnCount, true);
        execute("CREATE TABLE feedback_small (n LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        runInContext(port -> {
            try (WebSocketClient client = connect(port);
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer wide = longTable("feedback_wide", 1);
                 QwpTableBuffer small = longTable("feedback_small", 2)) {
                for (int i = 0; i < 3; i++) {
                    encoder.beginMessage(2, new GlobalSymbolDictionary(), -1, -1);
                    encoder.addTable(i % 2 == 0 ? wide : small);
                    encoder.addTable(i % 2 == 0 ? small : wide);
                    int length = encoder.finishMessage();
                    client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                    WebSocketResponse response = receive(client);
                    Assert.assertTrue(response.isSuccess());
                    Assert.assertEquals(i, response.getSequence());
                    Assert.assertFalse(response.isSchemaInvalidation());
                    // The server reports a table once per schema version per connection.
                    if (i == 0) {
                        Assert.assertEquals(2, response.getSchemaUpdateCount());
                        assertContainsUpdate(response, "feedback_wide", QwpSchemaProtocol.RESULT_TOO_LARGE);
                        assertContainsUpdate(response, "feedback_small");
                    } else {
                        Assert.assertEquals(0, response.getSchemaUpdateCount());
                    }
                }
                sendSchema(client, encoder, small);
                WebSocketResponse response = receive(client);
                Assert.assertTrue(response.isSuccess());
                Assert.assertEquals(3, response.getSequence());
                Assert.assertEquals(0, response.getSchemaUpdateCount());
            }
            drainWalQueue();
            assertQuery("SELECT n FROM feedback_wide").noLeakCheck().expectSize().returns("n\n1\n1\n1\n");
            assertQuery("SELECT n FROM feedback_small").noLeakCheck().expectSize().returns("n\n2\n2\n2\n2\n");
        }, 65_536, 65_536, 65_536, sendBufferSize, null);
    }

    private void assertReservedCapacity(int fragmentSize, boolean isNack) throws Exception {
        // Each standalone schema is 426 bytes. A two-table ACK has 457
        // suffix bytes: enough for one full entry, but not its next minimum.
        createWideTable("feedback_a", 11, true);
        createWideTable("feedback_b", 11, !isNack);
        runInContext(port -> {
            for (boolean isReversed : new boolean[]{false, true}) {
                String first = isReversed ? "feedback_b" : "feedback_a";
                String second = isReversed ? "feedback_a" : "feedback_b";
                try (WebSocketClient client = connect(port);
                     QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                     QwpTableBuffer a = longTable(first, 1);
                     QwpTableBuffer b = longTable(second, 2)) {
                    Assert.assertEquals(12, describe(client, 80, first).getColumnCount());
                    Assert.assertEquals(12, describe(client, 81, second).getColumnCount());
                    encoder.beginMessage(2, new GlobalSymbolDictionary(), -1, -1);
                    encoder.addTable(a);
                    encoder.addTable(b);
                    int length = encoder.finishMessage();
                    client.sendBinary(encoder.getBuffer().getBufferPtr(), length);
                    WebSocketResponse response = receive(client);
                    Assert.assertEquals(!isNack, response.isSuccess());
                    Assert.assertEquals(0, response.getSequence());
                    Assert.assertFalse("earlier schema must not starve a later named result", response.isSchemaInvalidation());
                    if (isNack && isReversed) {
                        // Rejection of the first table stops processing before the second.
                        Assert.assertEquals(1, response.getSchemaUpdateCount());
                        assertContainsUpdate(response, first, QwpSchemaProtocol.RESULT_KNOWN);
                    } else {
                        Assert.assertEquals(2, response.getSchemaUpdateCount());
                        assertContainsUpdate(response, first, QwpSchemaProtocol.RESULT_UNAVAILABLE);
                        assertContainsUpdate(response, second, QwpSchemaProtocol.RESULT_UNAVAILABLE);
                    }
                    if (!isNack) {
                        Assert.assertEquals(2, response.getTableEntryCount());
                        Assert.assertEquals(first, response.getTableName(0));
                        Assert.assertEquals(second, response.getTableName(1));
                        Assert.assertTrue(response.getTableSeqTxn(0) >= 0);
                        Assert.assertTrue(response.getTableSeqTxn(1) >= 0);
                    }
                    Assert.assertEquals(12, describe(client, 82, second).getColumnCount());
                }
            }
            drainWalQueue();
            if (isNack) {
                assertQuery("SELECT count() FROM feedback_a").noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");
                assertQuery("SELECT count() FROM feedback_b").noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");
            } else {
                assertQuery("SELECT n FROM feedback_a ORDER BY n").noLeakCheck().expectSize().returns("n\n1\n2\n");
                assertQuery("SELECT n FROM feedback_b ORDER BY n").noLeakCheck().expectSize().returns("n\n1\n2\n");
            }
        }, 65_536, fragmentSize, fragmentSize, 512, null);
    }

    private void createWideTable(String tableName, int columnCount, boolean isWal) throws Exception {
        StringBuilder ddl = new StringBuilder("CREATE TABLE ").append(tableName).append(" (n LONG");
        for (int i = 1; i < columnCount; i++) {
            ddl.append(",column_name_").append(i).append("_abcdefghijklmnop LONG");
        }
        ddl.append(",ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY ").append(isWal ? "WAL" : "BYPASS WAL");
        execute(ddl);
    }
}
