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
import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.std.ObjList;
import io.questdb.test.TestServerMain;
import io.questdb.test.cutlass.qwp.AbstractQwpBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

/**
 * Acknowledgements larger than {@code http.send.buffer.size}, with the real pinned
 * {@code java-questdb-client} against a live server. An OK ack names every table its frames
 * committed, and a durable ack every table whose durable frontier moved, so either can outgrow
 * a lowered send buffer. The server sends such a frame one buffer at a time. Were it to drop the
 * connection instead, the client would replay rows the server had already committed, and every
 * replay would add another copy of them.
 */
public class QwpIngressOversizeAckTest extends AbstractQwpBootstrapTest {
    private static final long ACK_TIMEOUT_MILLIS = 30_000;
    // The ack frames below carry 126 to 65_535 payload bytes, which a 4-byte WebSocket header frames.
    private static final int FRAME_HEADER_SIZE = 4;
    // cairo.max.file.name.length
    private static final int MAX_TABLE_NAME_LENGTH = 127;
    // Payload bytes of an OK ack before its tables: status (1), sequence (8) and table count (2).
    private static final int OK_ACK_FIXED_SIZE = 11;
    // Small, so that a few tables outgrow it: the server creates each table on its first row.
    private static final int SEND_BUFFER_SIZE = 1024;
    // Bytes a table takes in an ack besides its name: name length (2) and seqTxn (8).
    private static final int TABLE_ENTRY_OVERHEAD = 10;
    private int tableCounter;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    /**
     * A durable-ack client trims a frame only once a durable ack covers every table its OK ack
     * named, so both must arrive. A durable ack lacks the OK ack's sequence and is 8 bytes
     * shorter over the same tables: 3 times the send buffer keeps both well above it.
     */
    @Test
    public void testLocalDurableAckLargerThanSendBufferReachesClient() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startFragmented(
                    PropertyKey.HTTP_SEND_BUFFER_SIZE.getEnvVarName(), Integer.toString(SEND_BUFFER_SIZE),
                    PropertyKey.CAIRO_COMMIT_MODE.getEnvVarName(), "adaptive"
            )) {
                final ObjList<String> tables = tablesForOkAckFrame(3 * SEND_BUFFER_SIZE, 100);
                try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig("ws::addr=localhost:"
                        + serverMain.getHttpServerPort() + ";request_durable_ack=local;close_flush_timeout_millis=30000;")) {
                    final long fsn = publishOneFrame(sender, tables);
                    assertAcked(sender, fsn);
                    final CursorWebSocketSendLoop loop = sender.cursorSendLoopForTest();
                    Assert.assertTrue("the local durable ack must trim the frame", loop.getTotalLocalDurableAcks() > 0);
                    Assert.assertTrue(loop.getTotalDurableTrimAdvances() > 0);
                }
                assertOneRowEach(serverMain, tables);
            }
        });
    }

    /**
     * The scenario a commit batch makes common: a few tables per frame, many frames in one
     * receive pass, and one ack for the pass that names every table. Whether the frames share a
     * pass depends on timing, but the ack must reach the client however they group.
     */
    @Test
    public void testOkAckForManyFramesLargerThanSendBufferReachesClient() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_SEND_BUFFER_SIZE.getEnvVarName(), Integer.toString(SEND_BUFFER_SIZE),
                    PropertyKey.DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), Integer.toString(sendChunk)
            )) {
                final ObjList<String> tables = new ObjList<>();
                for (int i = 0; i < 40; i++) {
                    tables.add(tableName(100));
                }
                try (QwpWebSocketSender sender = connect(serverMain)) {
                    long fsn = -1;
                    for (int i = 0, n = tables.size(); i < n; i += 2) {
                        sender.table(tables.getQuick(i)).longColumn("v", 1).at(1_000_000L, ChronoUnit.MICROS);
                        sender.table(tables.getQuick(i + 1)).longColumn("v", 1).at(1_000_000L, ChronoUnit.MICROS);
                        fsn = sender.flushAndGetSequence();
                    }
                    assertAcked(sender, fsn);
                }
                assertOneRowEach(serverMain, tables);
            }
        });
    }

    /**
     * One frame that carries many tables, so that its ack alone outgrows the send buffer.
     * Each ack size gets its own frame and tables, acked before the next frame goes out:
     * exactly the buffer, one byte more, and several times the buffer.
     */
    @Test
    public void testOkAckForOneFrameLargerThanSendBufferReachesClient() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startFragmented(
                    PropertyKey.HTTP_SEND_BUFFER_SIZE.getEnvVarName(), Integer.toString(SEND_BUFFER_SIZE)
            )) {
                final ObjList<String> allTables = new ObjList<>();
                try (QwpWebSocketSender sender = connect(serverMain)) {
                    for (int frameSize : new int[]{SEND_BUFFER_SIZE, SEND_BUFFER_SIZE + 1, 2 * SEND_BUFFER_SIZE + 1, 4 * SEND_BUFFER_SIZE}) {
                        final ObjList<String> tables = tablesForOkAckFrame(frameSize, 100);
                        assertAcked(sender, publishOneFrame(sender, tables));
                        allTables.addAll(tables);
                    }
                }
                assertOneRowEach(serverMain, allTables);
            }
        });
    }

    private static void assertAcked(QwpWebSocketSender sender, long fsn) {
        Assert.assertTrue("test setup: nothing was published", fsn >= 0);
        final boolean isAcked = sender.awaitAckedFsn(fsn, ACK_TIMEOUT_MILLIS);
        final CursorWebSocketSendLoop loop = sender.cursorSendLoopForTest();
        Assert.assertEquals("the server must not drop the connection", 0, loop.getTotalReconnects());
        Assert.assertTrue("no ack covered the frames [ackedFsn=" + sender.getAckedFsn() + ", fsn=" + fsn + ']', isAcked);
        Assert.assertEquals(0, loop.getTotalServerErrors());
        Assert.assertEquals("nothing may be replayed", 0, loop.getTotalFramesReplayed());
    }

    private static void assertOneRowEach(TestServerMain serverMain, ObjList<String> tables) {
        for (int i = 0, n = tables.size(); i < n; i++) {
            final String table = tables.getQuick(i);
            serverMain.awaitTable(table);
            serverMain.assertSql("SELECT count() FROM '" + table + '\'', "count\n1\n");
        }
    }

    private static QwpWebSocketSender connect(TestServerMain serverMain) {
        return (QwpWebSocketSender) Sender.fromConfig("ws::addr=localhost:" + serverMain.getHttpServerPort()
                + ";close_flush_timeout_millis=30000;");
    }

    // Publishes one row to each table in a single frame and returns the frame's FSN.
    private static long publishOneFrame(QwpWebSocketSender sender, ObjList<String> tables) {
        for (int i = 0, n = tables.size(); i < n; i++) {
            sender.table(tables.getQuick(i)).longColumn("v", i).at(1_000_000L, ChronoUnit.MICROS);
        }
        return sender.flushAndGetSequence();
    }

    // A new table name of exactly `length` ASCII characters.
    private String tableName(int length) {
        final StringBuilder sb = new StringBuilder("t").append(tableCounter++).append('_');
        while (sb.length() < length) {
            sb.append('x');
        }
        return sb.toString();
    }

    /**
     * New tables whose OK ack frame takes exactly {@code frameSize} bytes: names of
     * {@code nameLength} characters, the first few longer to make up the remainder.
     */
    private ObjList<String> tablesForOkAckFrame(int frameSize, int nameLength) {
        final int entries = frameSize - FRAME_HEADER_SIZE - OK_ACK_FIXED_SIZE;
        final int entrySize = TABLE_ENTRY_OVERHEAD + nameLength;
        final int count = entries / entrySize;
        int remainder = entries - count * entrySize;
        final int maxExtra = MAX_TABLE_NAME_LENGTH - nameLength;
        assert count * maxExtra >= remainder;
        final ObjList<String> tables = new ObjList<>();
        for (int i = 0; i < count; i++) {
            final int extra = Math.min(maxExtra, remainder);
            remainder -= extra;
            tables.add(tableName(nameLength + extra));
        }
        return tables;
    }
}
