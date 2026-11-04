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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.wal.DurabilityTier;
import io.questdb.cairo.wal.DurableAckRegistry;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.server.QwpIngressProcessorState;
import io.questdb.cutlass.qwp.server.QwpIngressUpgradeProcessor;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static io.questdb.test.cutlass.websocket.QwpIngressWireHarness.*;

/**
 * Server frames larger than the send buffer: an ack names every table it covers, so it can
 * outgrow a lowered {@code http.send.buffer.size}. The server writes such a frame to staging
 * memory and sends it one buffer at a time, and while any of it is still on its way nothing
 * else may go out. The recording raw socket stands in for the network: a send it blocks keeps
 * its bytes, as the real send buffer does until the peer reads them. The real client's
 * acceptance of these frames is covered end to end by {@link QwpIngressOversizeAckTest}.
 */
public class QwpIngressStagedAckFrameTest extends AbstractCairoTest {
    private static final int BIG_RECV_BUFFER_SIZE = 8192;
    // Shortest table name stageAck uses, long enough for a unique prefix.
    private static final int MIN_NAME_LENGTH = 20;
    // Payload bytes of an OK ack before its tables: status (1), sequence (8), table count (2).
    private static final int OK_ACK_FIXED_SIZE = 11;
    private static final int SMALL_SEND_BUFFER_SIZE = 512;
    // Bytes a table takes in an ack besides its name: name length (2) and seqTxn (8).
    private static final int TABLE_ENTRY_OVERHEAD = 10;

    /**
     * The trigger a commit batch adds: one-row frames to many tables, all in one receive
     * pass, answered by one ack that names every table. The ack must leave whole, in pieces
     * that each fit the send buffer, and name every table with the seqTxn it committed.
     */
    @Test
    public void testBatchAckLargerThanSendBufferLeavesInPieces() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<String> tables = new ObjList<>();
            final ObjList<byte[]> frames = new ObjList<>();
            for (int i = 0; i < 20; i++) {
                final String table = name("batch", i, 60);
                execute("CREATE TABLE '" + table + "' (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                tables.add(table);
                frames.add(createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(table, i, 1_000_000L)));
            }
            try (Connection connection = new Connection(engine, SMALL_SEND_BUFFER_SIZE, false)) {
                connection.receiveInOnePass(frames);

                final int frameSize = 4 + OK_ACK_FIXED_SIZE + 20 * (TABLE_ENTRY_OVERHEAD + 60);
                Assert.assertEquals("the ack goes out in buffer-sized pieces",
                        Arrays.toString(new int[]{512, 512, frameSize - 1024}), Arrays.toString(connection.sendSizes()));
                final ObjList<byte[]> sent = parseFrames(connection.sentBytes());
                Assert.assertEquals(1, sent.size());
                final Map<String, Long> expected = new HashMap<>();
                for (int i = 0, n = tables.size(); i < n; i++) {
                    final TableToken token = engine.verifyTableName(tables.getQuick(i));
                    expected.put(tables.getQuick(i), engine.getTableSequencerAPI().lastTxn(token));
                }
                assertOkAck(sent.getQuick(0), 19, expected);
                Assert.assertTrue(connection.state.isSendReady());
                Assert.assertEquals(19, connection.state.getLastAckedSequence());
            }
            drainWalQueue();
            for (int i = 0, n = tables.size(); i < n; i++) {
                assertQuery("SELECT v FROM '" + tables.getQuick(i) + "'").noLeakCheck().expectSize().returns("v\n" + i + '\n');
            }
        });
    }

    /**
     * The durable acks of a pass name the same tables as its OK ack, so they outgrow the
     * send buffer together. A local|replicated grant sends the local frame and then the
     * replicated one: the second is staged after the first has left, and a block inside
     * the local frame parks the replicated one behind it.
     */
    @Test
    public void testDurableAcksLargerThanSendBufferLeaveInPieces() throws Exception {
        assertMemoryLeak(() -> {
            for (int blockedSend : new int[]{0, 5}) {
                final ObjList<String> tables = new ObjList<>();
                final ObjList<byte[]> frames = new ObjList<>();
                for (int i = 0; i < 20; i++) {
                    final String table = name("durable" + blockedSend, i, 60);
                    execute("CREATE TABLE '" + table + "' (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    tables.add(table);
                    frames.add(createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(table, i, 1_000_000L)));
                }
                try (CairoEngine durableEngine = newEngineWithEverythingDurable()) {
                    try (Connection connection = new Connection(durableEngine, SMALL_SEND_BUFFER_SIZE, true)) {
                        if (blockedSend > 0) {
                            // The OK ack takes sends 1 to 3, the local durable ack 4 to 6.
                            connection.rawSocket.blockedSends.add(blockedSend);
                        }
                        connection.receiveInOnePass(frames);
                        if (blockedSend > 0) {
                            Assert.assertTrue("the blocked local ack parks the send state", connection.state.isSending());
                            Assert.assertTrue("the local ack's last piece is still staged", connection.state.hasStagedFrame());
                            Assert.assertEquals(5, connection.sendSizes().length);
                            connection.processor.resumeSend(connection.context);
                        }

                        final Map<String, Long> expected = new HashMap<>();
                        for (int i = 0, n = tables.size(); i < n; i++) {
                            final TableToken token = durableEngine.verifyTableName(tables.getQuick(i));
                            expected.put(tables.getQuick(i), durableEngine.getTableSequencerAPI().lastTxn(token));
                        }
                        final ObjList<byte[]> sent = parseFrames(connection.sentBytes());
                        Assert.assertEquals("OK, local durable and durable ack", 3, sent.size());
                        assertOkAck(sent.getQuick(0), 19, expected);
                        assertDurableAck(sent.getQuick(1), QwpConstants.STATUS_LOCAL_DURABLE_ACK, expected);
                        assertDurableAck(sent.getQuick(2), QwpConstants.STATUS_DURABLE_ACK, expected);
                        for (int size : connection.sendSizes()) {
                            Assert.assertTrue(size <= SMALL_SEND_BUFFER_SIZE);
                        }
                        Assert.assertTrue(connection.state.isSendReady());
                        Assert.assertFalse(connection.state.hasStagedFrame());
                    }
                }
            }
        });
    }

    /**
     * An error queued behind a parked staged ack follows the whole ack, not the piece the
     * send buffer held.
     */
    @Test
    public void testErrorQueuedBehindStagedAckFollowsWholeAck() throws Exception {
        assertMemoryLeak(() -> {
            try (Connection connection = new Connection(engine, SMALL_SEND_BUFFER_SIZE, false)) {
                final Map<String, Long> tables = stageAck(connection, 3 * SMALL_SEND_BUFFER_SIZE);
                connection.rawSocket.blockedSends.add(1);
                assertBlocked(connection);

                connection.state.onErrorBlocked(QwpConstants.STATUS_PARSE_ERROR, 8, "bad frame");
                connection.processor.resumeSend(connection.context);

                final ObjList<byte[]> sent = parseFrames(connection.sentBytes());
                Assert.assertEquals(2, sent.size());
                assertOkAck(sent.getQuick(0), 7, tables);
                final byte[] error = sent.getQuick(1);
                Assert.assertEquals(QwpConstants.STATUS_PARSE_ERROR, error[0]);
                Assert.assertEquals(8, readLeLong(error, 1));
                Assert.assertEquals("bad frame", new String(error, 11, error.length - 11, StandardCharsets.UTF_8));
                Assert.assertEquals(7, connection.state.getLastAckedSequence());
                Assert.assertTrue(connection.state.isSendReady());
            }
        });
    }

    /**
     * Frames up to the send buffer go out whole; one byte more takes a second piece, and so
     * on, up to a frame whose payload needs the 8-byte WebSocket length. The staging memory
     * grows to the largest frame and then serves smaller ones without allocating.
     */
    @Test
    public void testFrameSizesAroundSendBuffer() throws Exception {
        assertMemoryLeak(() -> {
            final int b = SMALL_SEND_BUFFER_SIZE;
            final int[] frameSizes = {b - 1, b, b + 1, 2 * b, 2 * b + 1, 5 * b + 7, 70_000, b + 100, 3 * b};
            try (Connection connection = new Connection(engine, b, false)) {
                for (int frameSize : frameSizes) {
                    final int sendsBefore = connection.rawSocket.sentFrames.size();
                    final int bytesBefore = connection.sentBytes().length;
                    final Map<String, Long> tables = stageAck(connection, frameSize);
                    final long mallocsBefore = Unsafe.getMallocCount();
                    connection.processor.resumeSend(connection.context);
                    if (frameSize == b + 100 || frameSize == 3 * b) {
                        Assert.assertEquals("staging memory sized for a larger frame serves this one",
                                mallocsBefore, Unsafe.getMallocCount());
                    }

                    final int[] sends = connection.sendSizes();
                    final int pieces = (frameSize + b - 1) / b;
                    Assert.assertEquals("frame size " + frameSize, pieces, sends.length - sendsBefore);
                    for (int i = 0; i < pieces; i++) {
                        final int expected = i < pieces - 1 ? b : frameSize - (pieces - 1) * b;
                        Assert.assertEquals("frame size " + frameSize + ", piece " + i, expected, sends[sendsBefore + i]);
                    }
                    final byte[] bytes = connection.sentBytes();
                    final ObjList<byte[]> sent = parseFrames(Arrays.copyOfRange(bytes, bytesBefore, bytes.length));
                    Assert.assertEquals(1, sent.size());
                    assertOkAck(sent.getQuick(0), connection.ackSequence, tables);
                    Assert.assertEquals(connection.ackSequence, connection.state.getLastAckedSequence());
                    Assert.assertTrue(connection.state.isSendReady());
                    Assert.assertFalse(connection.state.hasStagedFrame());
                }
            }
        });
    }

    /**
     * Table names in an ack are UTF-8: a staged ack sizes and copies them by their bytes,
     * not their characters.
     */
    @Test
    public void testStagedAckCarriesMultiByteTableNames() throws Exception {
        assertMemoryLeak(() -> {
            try (Connection connection = new Connection(engine, 64, false)) {
                connection.state.setHighestProcessedSequence(7);
                connection.state.onAckBlocked(3);
                final String[] names = {"t\u00e9", "t\u65e5\u672c", "t\ud83c\udf89", "ascii_name", "\u0436\u0436\u0436\u0436\u0436\u0436\u0436\u0436"};
                final Map<String, Long> tables = new HashMap<>();
                for (int i = 0; i < names.length; i++) {
                    connection.state.getPendingAckSeqTxns().put(names[i], 100 + i);
                    tables.put(names[i], 100L + i);
                }
                int utf8Bytes = 0;
                for (String name : names) {
                    utf8Bytes += name.getBytes(StandardCharsets.UTF_8).length;
                }
                final int frameSize = 2 + OK_ACK_FIXED_SIZE + names.length * TABLE_ENTRY_OVERHEAD + utf8Bytes;
                Assert.assertTrue("test setup: the frame must outgrow the buffer", frameSize > 64 && frameSize - 2 <= 125);

                connection.processor.resumeSend(connection.context);

                final byte[] bytes = connection.sentBytes();
                Assert.assertEquals(frameSize, bytes.length);
                Assert.assertEquals(Arrays.toString(new int[]{64, frameSize - 64}), Arrays.toString(connection.sendSizes()));
                final ObjList<byte[]> sent = parseFrames(bytes);
                Assert.assertEquals(1, sent.size());
                assertOkAck(sent.getQuick(0), 7, tables);
            }
        });
    }

    /**
     * A blocked piece parks the ack until resumeSend, which sends the rest from where the
     * block left it, as many times as the peer blocks. A block on the last piece leaves
     * nothing staged, only the send buffer's part.
     */
    @Test
    public void testResumeSendsStagedAckFromWhereBlocksLeftIt() throws Exception {
        assertMemoryLeak(() -> {
            final int[][] blocks = {
                    {1},
                    {2},
                    {4},
                    {1, 2, 3},
                    {2, 4},
            };
            for (int[] blockedSends : blocks) {
                try (Connection connection = new Connection(engine, SMALL_SEND_BUFFER_SIZE, false)) {
                    // Four pieces: 512, 512, 512 and 100 bytes.
                    final Map<String, Long> tables = stageAck(connection, 3 * SMALL_SEND_BUFFER_SIZE + 100);
                    for (int blockedSend : blockedSends) {
                        connection.rawSocket.blockedSends.add(blockedSend);
                    }
                    assertBlocked(connection);
                    for (int i = 1; i < blockedSends.length; i++) {
                        try {
                            connection.processor.resumeSend(connection.context);
                            Assert.fail("send " + blockedSends[i] + " must block again");
                        } catch (PeerIsSlowToReadException expected) {
                            Assert.assertTrue(connection.state.isSending());
                            Assert.assertEquals(blockedSends[i], connection.sendSizes().length);
                        }
                    }
                    Assert.assertEquals("only a block before the last piece leaves some of it staged",
                            blockedSends[blockedSends.length - 1] < 4, connection.state.hasStagedFrame());
                    connection.processor.resumeSend(connection.context);

                    Assert.assertEquals(Arrays.toString(new int[]{512, 512, 512, 100}), Arrays.toString(connection.sendSizes()));
                    final ObjList<byte[]> sent = parseFrames(connection.sentBytes());
                    Assert.assertEquals(1, sent.size());
                    assertOkAck(sent.getQuick(0), 7, tables);
                    Assert.assertEquals(7, connection.state.getLastAckedSequence());
                    Assert.assertTrue(connection.state.isSendReady());
                }
            }
        });
    }

    /**
     * Staging memory the server cannot allocate drops the connection before any of the ack
     * goes out, as the server did for every ack larger than the send buffer before it could
     * stage one. A teardown that tries the ack again fails the same way and stays quiet.
     */
    @Test
    public void testStagingFailureDropsConnectionBeforeSendingAnything() throws Exception {
        assertMemoryLeak(() -> {
            try (Connection connection = new Connection(engine, SMALL_SEND_BUFFER_SIZE, false)) {
                final int frameSize = 3 * SMALL_SEND_BUFFER_SIZE;
                stageAck(connection, frameSize);
                final long rssMemLimit = Unsafe.getRssMemLimit();
                Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + frameSize - 1);
                try {
                    try {
                        connection.processor.resumeSend(connection.context);
                        Assert.fail("the connection must drop");
                    } catch (PeerDisconnectedException expected) {
                        Assert.assertEquals(0, connection.sendSizes().length);
                        Assert.assertFalse(connection.state.hasStagedFrame());
                        Assert.assertEquals(3, connection.state.getLastAckedSequence());
                    }
                    connection.processor.onConnectionClosed(connection.context);
                    Assert.assertEquals(0, connection.sendSizes().length);
                } finally {
                    Unsafe.setRssMemLimit(rssMemLimit);
                }
            }
        });
    }

    /**
     * A peer gone in the middle of a staged ack: the send fails, and the teardown that
     * follows tries the rest, fails the same way, stays quiet, and frees the staging memory.
     */
    @Test
    public void testPeerGoneInsideStagedAck() throws Exception {
        assertMemoryLeak(() -> {
            try (Connection connection = new Connection(engine, SMALL_SEND_BUFFER_SIZE, false)) {
                final long memBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_HTTP_CONN);
                stageAck(connection, 3 * SMALL_SEND_BUFFER_SIZE);
                connection.rawSocket.disconnectedSend = 2;
                try {
                    connection.processor.resumeSend(connection.context);
                    Assert.fail("the second piece must find the peer gone");
                } catch (PeerDisconnectedException expected) {
                    Assert.assertEquals(1, connection.sendSizes().length);
                    Assert.assertTrue(connection.state.hasStagedFrame());
                }
                connection.processor.onConnectionClosed(connection.context);
                Assert.assertEquals(1, connection.sendSizes().length);
                Assert.assertFalse(connection.state.hasStagedFrame());
                Assert.assertEquals("the teardown frees the staging memory", memBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_HTTP_CONN));
            }
        });
    }

    /**
     * A connection torn down while a staged ack is parked sends the rest of it, so a peer
     * still reading gets the ack whole, and frees the staging memory, which a pooled
     * context must not keep. The connection's next life stages afresh.
     */
    @Test
    public void testTeardownFinishesParkedStagedAck() throws Exception {
        assertMemoryLeak(() -> {
            try (Connection connection = new Connection(engine, SMALL_SEND_BUFFER_SIZE, false)) {
                final int frameSize = 3 * SMALL_SEND_BUFFER_SIZE;
                final long memBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_HTTP_CONN);
                final Map<String, Long> tables = stageAck(connection, frameSize);
                connection.rawSocket.blockedSends.add(1);
                assertBlocked(connection);
                Assert.assertEquals("staging memory", memBefore + frameSize, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_HTTP_CONN));

                connection.processor.onConnectionClosed(connection.context);
                ObjList<byte[]> sent = parseFrames(connection.sentBytes());
                Assert.assertEquals(1, sent.size());
                assertOkAck(sent.getQuick(0), 7, tables);
                Assert.assertFalse(connection.state.hasStagedFrame());
                Assert.assertTrue(connection.state.isSendReady());
                Assert.assertEquals("the teardown frees the staging memory", memBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_HTTP_CONN));

                final int bytesBefore = connection.sentBytes().length;
                final Map<String, Long> nextTables = stageAck(connection, 2 * SMALL_SEND_BUFFER_SIZE + 1);
                connection.processor.resumeSend(connection.context);
                final byte[] bytes = connection.sentBytes();
                sent = parseFrames(Arrays.copyOfRange(bytes, bytesBefore, bytes.length));
                Assert.assertEquals(1, sent.size());
                assertOkAck(sent.getQuick(0), connection.ackSequence, nextTables);
            }
        });
    }

    // Sends the ack that stageAck set up and asserts its first send blocks, parking it.
    private static void assertBlocked(Connection connection) throws Exception {
        try {
            connection.processor.resumeSend(connection.context);
            Assert.fail("the first piece must block");
        } catch (PeerIsSlowToReadException expected) {
            Assert.assertTrue(connection.state.isSending());
        }
    }

    private static void assertDurableAck(byte[] payload, byte status, Map<String, Long> tables) {
        Assert.assertEquals(status, payload[0]);
        Assert.assertEquals(tables, readTableEntries(payload, 1));
    }

    private static void assertOkAck(byte[] payload, long sequence, Map<String, Long> tables) {
        Assert.assertEquals(QwpConstants.STATUS_OK, payload[0]);
        Assert.assertEquals(sequence, readLeLong(payload, 1));
        Assert.assertEquals(tables, readTableEntries(payload, 9));
    }

    // A table name of exactly `length` ASCII characters.
    private static String name(String prefix, int index, int length) {
        final StringBuilder sb = new StringBuilder(prefix).append('_').append(index).append('_');
        while (sb.length() < length) {
            sb.append('x');
        }
        return sb.toString();
    }

    // Every table durable as soon as it commits, on both tiers.
    private static CairoEngine newEngineWithEverythingDurable() {
        return new CairoEngine(new DefaultTestCairoConfiguration(root)) {
            private final DurableAckRegistry registry = new DurableAckRegistry() {
                @Override
                public long getLocalDurableSeqTxn(CharSequence tableDirName) {
                    return Long.MAX_VALUE;
                }

                @Override
                public long getReplicatedDurableSeqTxn(CharSequence tableDirName) {
                    return Long.MAX_VALUE;
                }

                @Override
                public boolean isEnabled() {
                    return true;
                }

                @Override
                public boolean isTierSetAvailable(int tiers) {
                    return true;
                }

                @Override
                public void onTableDropped(TableToken tableToken) {
                }
            };

            @Override
            public @NotNull DurableAckRegistry getDurableAckRegistry() {
                return registry;
            }
        };
    }

    /**
     * Server WebSocket frames, unmasked and unfragmented, in the order they appear in
     * {@code bytes}, which must end at a frame boundary. Returns their payloads.
     */
    private static ObjList<byte[]> parseFrames(byte[] bytes) {
        final ObjList<byte[]> payloads = new ObjList<>();
        int pos = 0;
        while (pos < bytes.length) {
            Assert.assertEquals("FIN BINARY", 0x80 | WebSocketOpcode.BINARY, bytes[pos] & 0xFF);
            int length = bytes[pos + 1] & 0xFF;
            Assert.assertEquals("server frames are unmasked", 0, length & 0x80);
            int headerSize = 2;
            if (length == 126) {
                length = ((bytes[pos + 2] & 0xFF) << 8) | (bytes[pos + 3] & 0xFF);
                headerSize = 4;
            } else if (length == 127) {
                long length64 = 0;
                for (int i = 0; i < 8; i++) {
                    length64 = (length64 << 8) | (bytes[pos + 2 + i] & 0xFF);
                }
                Assert.assertTrue("test frames carry less than 2 GiB", length64 > 65_535 && length64 < Integer.MAX_VALUE);
                length = (int) length64;
                headerSize = 10;
            }
            Assert.assertTrue("truncated frame", pos + headerSize + length <= bytes.length);
            payloads.add(Arrays.copyOfRange(bytes, pos + headerSize, pos + headerSize + length));
            pos += headerSize + length;
        }
        return payloads;
    }

    // Table entries as the client reads them: count, then name length, name and seqTxn per table.
    private static Map<String, Long> readTableEntries(byte[] payload, int offset) {
        final int count = (payload[offset] & 0xFF) | ((payload[offset + 1] & 0xFF) << 8);
        int pos = offset + 2;
        final Map<String, Long> entries = new HashMap<>();
        for (int i = 0; i < count; i++) {
            final int nameLength = (payload[pos] & 0xFF) | ((payload[pos + 1] & 0xFF) << 8);
            pos += 2;
            final String name = new String(payload, pos, nameLength, StandardCharsets.UTF_8);
            pos += nameLength;
            Assert.assertNull("table named twice: " + name, entries.put(name, readLeLong(payload, pos)));
            pos += 8;
        }
        Assert.assertEquals("trailing bytes", payload.length, pos);
        return entries;
    }

    /**
     * Sets up the connection to owe an ack of exactly {@code frameSize} bytes for four frames,
     * behind a parked ack for the ones before, and returns its tables. The next
     * {@code resumeSend} finishes the parked ack and sends this one, for
     * {@link Connection#ackSequence}.
     */
    private static Map<String, Long> stageAck(Connection connection, int frameSize) {
        final QwpIngressProcessorState state = connection.state;
        final long parkedSequence = connection.ackSequence + 4;
        connection.ackSequence += 8;
        state.setHighestProcessedSequence(connection.ackSequence);
        state.onAckBlocked(parkedSequence);
        // Every frame here carries more than 125 payload bytes: a 4-byte header frames up to
        // 65_535 of them, a 10-byte header more.
        final int headerSize = frameSize - 4 <= 65_535 ? 4 : 10;
        int remaining = frameSize - headerSize - OK_ACK_FIXED_SIZE;
        final Map<String, Long> tables = new HashMap<>();
        int index = 0;
        while (remaining > 0) {
            // Names of 100 characters, and the last two shorter to fill the frame exactly;
            // the last at least MIN_NAME_LENGTH, to stay unique.
            int nameLength = remaining - TABLE_ENTRY_OVERHEAD;
            if (nameLength > 100) {
                final int rest = remaining - TABLE_ENTRY_OVERHEAD - 100;
                nameLength = rest < TABLE_ENTRY_OVERHEAD + MIN_NAME_LENGTH ? 100 - (TABLE_ENTRY_OVERHEAD + MIN_NAME_LENGTH - rest) : 100;
            }
            final String table = name("staged" + connection.ackSequence, index, nameLength);
            Assert.assertEquals(nameLength, table.length());
            state.getPendingAckSeqTxns().put(table, index + 1);
            tables.put(table, (long) index + 1);
            remaining -= TABLE_ENTRY_OVERHEAD + nameLength;
            index++;
        }
        Assert.assertEquals("test setup", frameSize, state.computeAckPayloadSize() + headerSize);
        return tables;
    }

    /**
     * One server connection over in-memory sockets: a send buffer of the given size and a
     * receive buffer large enough for every frame of a test.
     */
    private static class Connection implements AutoCloseable {
        final TestableContext context;
        final HttpFullFatServerConfiguration httpConfig;
        final PhasedNetworkFacade nf;
        final QwpIngressUpgradeProcessor processor;
        final RecordingRawSocket rawSocket;
        final long recvBuffer;
        final long sendBuffer;
        final int sendBufferSize;
        final QwpIngressProcessorState state;
        // Sequence of the ack the last stageAck set up.
        long ackSequence = -1;
        private byte[] wire = new byte[0];

        Connection(CairoEngine engine, int sendBufferSize, boolean isDurableAckEnabled) throws Exception {
            this.sendBufferSize = sendBufferSize;
            this.httpConfig = new DefaultHttpServerConfiguration(configuration);
            this.processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            this.nf = new PhasedNetworkFacade(new byte[0]) {
                @Override
                public int recvRaw(long fd, long buffer, int bufferLen) {
                    final int n = Math.min(bufferLen, wire.length);
                    for (int i = 0; i < n; i++) {
                        Unsafe.putByte(buffer + i, wire[i]);
                    }
                    wire = Arrays.copyOfRange(wire, n, wire.length);
                    return n;
                }
            };
            this.recvBuffer = Unsafe.malloc(BIG_RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long send = 0;
            TestableContext ctx = null;
            try {
                send = Unsafe.malloc(sendBufferSize, MemoryTag.NATIVE_DEFAULT);
                this.rawSocket = new RecordingRawSocket(send, sendBufferSize);
                ctx = new TestableContext(httpConfig, nf, rawSocket, recvBuffer, BIG_RECV_BUFFER_SIZE);
                this.state = new QwpIngressProcessorState(
                        BIG_RECV_BUFFER_SIZE,
                        httpConfig.getSendBufferSize(),
                        engine,
                        httpConfig.getLineHttpProcessorConfiguration()
                );
                state.of(-1, AllowAllSecurityContext.INSTANCE);
                if (isDurableAckEnabled) {
                    state.setDurableAckEnabled(true);
                    state.setDurableAckTiers(DurabilityTier.LOCAL | DurabilityTier.REPLICATED);
                }
                getLV().set(ctx, state);
            } catch (Throwable th) {
                if (ctx != null) {
                    ctx.close();
                }
                if (send != 0) {
                    Unsafe.free(send, sendBufferSize, MemoryTag.NATIVE_DEFAULT);
                }
                Unsafe.free(recvBuffer, BIG_RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                throw th;
            }
            this.sendBuffer = send;
            this.context = ctx;
        }

        @Override
        public void close() {
            context.close();
            Unsafe.free(sendBuffer, sendBufferSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(recvBuffer, BIG_RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        }

        // Delivers every frame in one receive pass, as from a client with many in flight.
        void receiveInOnePass(ObjList<byte[]> frames) throws Exception {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            for (int i = 0, n = frames.size(); i < n; i++) {
                out.writeBytes(frames.getQuick(i));
            }
            wire = out.toByteArray();
            Assert.assertTrue("test setup: the frames must fit one receive", wire.length <= BIG_RECV_BUFFER_SIZE);
            try {
                drive(processor, context, nf, 0);
            } catch (PeerIsSlowToReadException e) {
                // a blocked send parked the connection; the test resumes it
            }
            Assert.assertEquals("every frame was received", 0, wire.length);
        }

        byte[] sentBytes() {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            for (int i = 0, n = rawSocket.sentFrames.size(); i < n; i++) {
                out.writeBytes(rawSocket.sentFrames.getQuick(i));
            }
            return out.toByteArray();
        }

        int[] sendSizes() {
            final int[] sizes = new int[rawSocket.sentFrames.size()];
            for (int i = 0; i < sizes.length; i++) {
                sizes[i] = rawSocket.sentFrames.getQuick(i).length;
            }
            return sizes;
        }
    }
}
