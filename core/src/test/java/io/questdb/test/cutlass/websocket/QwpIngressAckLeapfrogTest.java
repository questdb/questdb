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

package io.questdb.test.cutlass.websocket;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.wal.DurableAckRegistry;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRawSocket;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.server.QwpIngressProcessorState;
import io.questdb.cutlass.qwp.server.QwpIngressUpgradeProcessor;
import io.questdb.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.log.Log;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.PlainSocket;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.cutlass.qwp.QwpWireTestFixtures;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Regression test for the cumulative-ACK leapfrog over a silently gate-rejected
 * frame during a deferred role-change close (in-place PRIMARY-to-REPLICA demote
 * that re-promotes within the durable-upload grace window).
 * <p>
 * Failure chain that was possible before the deferral gate in
 * {@code handleBinaryMessage} ({@link QwpIngressUpgradeProcessor}):
 * <ol>
 *   <li>Durable-ack connection commits frame seq=0 (durable upload lags, so
 *       the connection has un-covered durable work).</li>
 *   <li>Demote: frame seq=1 is rejected by the {@code engine.isReadOnlyMode()}
 *       gate. {@code roleChangeCloseWithUploadGrace} arms the bounded deferral
 *       and returns — seq=1 gets NO error response, yet its sequence number is
 *       consumed. The {@code finally}-block {@code state.clear()} re-arms the
 *       connection ({@code currentStatus=OK}, {@code roleChangeClosePending=false}).</li>
 *   <li>Re-promote within the grace window: frame seq=2 passes the live gate,
 *       commits, and {@code setHighestProcessedSequence(2)} is recorded.</li>
 *   <li>The client's durable-ack keepalive PING flushes a cumulative OK-ACK
 *       carrying seq=2.</li>
 * </ol>
 * The OK-ACK contract is cumulative — "the server has confirmed every FSN up
 * to and including this value" ({@code SegmentRing.acknowledge} in the Java
 * client) — so an ACK of seq=2 makes a store-and-forward client trim the
 * segment slot holding frame seq=1, whose rows the server refused, never
 * wrote, and never reported. Silent data loss.
 * <p>
 * Invariant asserted (fix-agnostic): a cumulative OK-ACK must not cover a
 * sequence that was neither committed nor answered with an error response.
 * The test stays green whether the fix NACKs the rejected frame, caps the
 * cumulative ACK below it, or refuses data frames while a role-change close
 * deferral is armed. The shipped fix does the latter (primary gate) plus the
 * ack-watermark clamp in {@code QwpIngressProcessorState} (last-resort
 * containment, unit-covered in {@code QwpIngressProcessorStateTest}).
 */
public class QwpIngressAckLeapfrogTest extends AbstractCairoTest {
    private static final byte[] DEFAULT_MASK_KEY = {0x12, 0x34, 0x56, 0x78};
    private static final int RECV_BUFFER_SIZE = 1024;
    private static final int SEND_BUFFER_SIZE = 1024;

    @Test
    public void testForeignTableCapCannotCommitBeforeRefusedBatch() throws Exception {
        assertForeignTableCapBoundary(false);
    }

    @Test
    public void testForeignTableCapSeparatePassControl() throws Exception {
        assertForeignTableCapBoundary(true);
    }

    @Test
    public void testRefusedBatchPreventsSingleBlockForeignWriterLookup() throws Exception {
        assertRefusedBatchPreventsForeignWriterLookup("b");
    }

    @Test
    public void testRefusedBatchPreventsRepeatedBlockForeignWriterLookup() throws Exception {
        assertRefusedBatchPreventsForeignWriterLookup("b", "b");
    }

    @Test
    public void testRefusedBatchPreventsMultiTableForeignWriterLookup() throws Exception {
        assertRefusedBatchPreventsForeignWriterLookup("b", "c");
    }

    @Test
    public void testSingleBlockFramesRetainOneCommit() throws Exception {
        assertConservativeBoundaryTransactions(1, 3,
                clientEncodedFrame(1, "a"), clientEncodedFrame(1, "a"), clientEncodedFrame(1, "a"));
    }

    @Test
    public void testRepeatedBlockFrameSeparatesCommitBatches() throws Exception {
        assertConservativeBoundaryTransactions(3, 4,
                clientEncodedFrame(1, "a"), clientEncodedFrame(1, "a", "a"), clientEncodedFrame(1, "a"));
    }

    @Test
    public void testMultiTableFrameStartingWithBatchTableIsStandalone() throws Exception {
        assertConservativeBoundaryTransactions(3, 3,
                clientEncodedFrame(1, "a"), clientEncodedFrame(1, "a", "b"));
    }

    @Test
    public void testForeignMultiTableFrameRetainsThreeCommits() throws Exception {
        assertConservativeBoundaryTransactions(3, 3,
                clientEncodedFrame(1, "a"), clientEncodedFrame(1, "b", "c"));
    }

    @Test
    public void testRowlessRepeatedBlockFrameSeparatesCommitBatches() throws Exception {
        assertConservativeBoundaryTransactions(2, 2,
                clientEncodedFrame(1, "a"), clientEncodedFrame(0, "a", "a"), clientEncodedFrame(1, "a"));
    }

    @Test
    public void testEmptyFrameSeparatesCommitBatches() throws Exception {
        assertConservativeBoundaryTransactions(2, 2,
                clientEncodedFrame(1, "a"), clientEncodedFrame(0), clientEncodedFrame(1, "a"));
    }

    @Test
    public void testRowlessSingletonSeparatesCommitBatches() throws Exception {
        assertConservativeBoundaryTransactions(2, 2,
                clientEncodedFrame(1, "a"), clientEncodedFrame(0, "a"), clientEncodedFrame(1, "a"));
    }

    @Test
    public void testRowlessThenRowBearingMultiBlockIsStandalone() throws Exception {
        assertConservativeBoundaryTransactions(3, 3,
                clientEncodedFrame(1, "a"),
                createMaskedFrame(WebSocketOpcode.BINARY, twoBlockMessage(
                        tableBlock("b".getBytes(java.nio.charset.StandardCharsets.UTF_8), new long[0]),
                        tableBlock("a".getBytes(java.nio.charset.StandardCharsets.UTF_8), new long[]{200, 2_000_000}))),
                clientEncodedFrame(1, "a"));
    }

    @Test
    public void testMalformedFirstSchemaSalvagesPendingPrefix() throws Exception {
        assertMalformedPreparationSalvagesPrefix(false);
    }

    @Test
    public void testMalformedHeaderSalvagesPendingPrefix() throws Exception {
        assertMalformedPreparationSalvagesPrefix(true);
    }

    private void assertMalformedPreparationSalvagesPrefix(boolean isHeader) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            byte[] malformed = oneRowMessage(200, 2_000_000);
            // Truncate either the header or first schema, before any table lookup.
            malformed = Arrays.copyOf(malformed, isHeader ? 3 : QwpConstants.HEADER_SIZE + 8);
            HttpFullFatServerConfiguration config = new DefaultHttpServerConfiguration(configuration);
            ObjList<byte[]> sent = ingestInOnePass(new QwpIngressUpgradeProcessor(engine, config), config,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100, 1_000_000)),
                    createMaskedFrame(WebSocketOpcode.BINARY, malformed),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(300, 3_000_000)));
            drainWalQueue();
            assertQuery("SELECT v FROM tab").noLeakCheck().expectSize().returns("v\n100\n");
            Assert.assertEquals(0, maxCumulativeOkAck(sent));
            Assert.assertTrue(hasErrorResponseForSeq(sent, 1));
            Assert.assertFalse(hasErrorResponseForSeq(sent, 0));
        });
    }

    @Test
    public void testDurableAckCarriesCommitBeforeCapBoundaryFailure() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration config = new DefaultHttpServerConfiguration(configuration) {
                @Override
                public LineHttpProcessorConfiguration getLineHttpProcessorConfiguration() {
                    return new DefaultLineHttpProcessorConfiguration(configuration) {
                        @Override
                        public long getQwpMaxUncommittedRows() {
                            return 2;
                        }
                    };
                }
            };
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            long before = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b"));
            ObjList<byte[]> sent = ingestDurableInOnePass(new QwpIngressUpgradeProcessor(engine, config), config,
                    denyCommitsAfterFirst("b"),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 100, 1_000_000)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200, 2_000_000)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 300, 3_000_000)));
            // With a cap of 2 no batch can reach the cap: frame 1 cannot join frame 0's
            // batch, so frame 0 commits on its own (the one allowed commit) and frame 1
            // opens a new batch; frame 2 cannot join it either, and frame 1's commit is
            // denied. The wire matches one commit per frame.
            drainWalQueue();
            assertQuery("SELECT v FROM b").noLeakCheck().expectSize().returns("v\n100\n");
            Assert.assertEquals(0, maxCumulativeOkAck(sent));
            Assert.assertEquals(before + 1, okAckSeqTxn(sent, 0, "b"));
            Assert.assertTrue(hasErrorResponseForSeq(sent, 1));
            Assert.assertFalse(hasErrorResponseForSeq(sent, 0));
            Assert.assertFalse(hasErrorResponseForSeq(sent, 2));
        });
    }

    private void assertConservativeBoundaryTransactions(long expectedTransactions, long expectedRows, byte[]... frames) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE c (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            long before = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("a"))
                    + engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b"))
                    + engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("c"));
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            ingestInOnePass(new QwpIngressUpgradeProcessor(engine, httpConfig), httpConfig, frames);
            drainWalQueue();
            try (TableReader a = engine.getReader("a"); TableReader b = engine.getReader("b"); TableReader c = engine.getReader("c")) {
                Assert.assertEquals(expectedRows, a.size() + b.size() + c.size());
            }
            long after = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("a"))
                    + engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b"))
                    + engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("c"));
            Assert.assertEquals("only single-block same-table frames may share a commit", expectedTransactions, after - before);
        });
    }

    private void assertForeignTableCapBoundary(boolean isSeparatePass) throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration) {
                @Override
                public LineHttpProcessorConfiguration getLineHttpProcessorConfiguration() {
                    return new DefaultLineHttpProcessorConfiguration(configuration) {
                        @Override
                        public long getQwpMaxUncommittedRows() {
                            return 2;
                        }
                    };
                }
            };
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            long bBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b"));
            byte[] first = clientEncodedFrame(1, "a");
            byte[] second = clientEncodedFrame(2, "b");
            byte[] wire = concat(first, second);
            PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                QwpIngressProcessorState state = new QwpIngressProcessorState(
                        RECV_BUFFER_SIZE, httpConfig.getSendBufferSize(), engine, httpConfig.getLineHttpProcessorConfiguration());
                state.of(-1, denyCommitTo("a"));
                getLV().set(context, state);
                if (isSeparatePass) {
                    drive(processor, context, nf, first.length);
                    drive(processor, context, nf, second.length);
                } else {
                    drive(processor, context, nf, wire.length);
                }
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
            // Inspect storage only, after teardown rolled back all open transactions.
            // The pinned encoder supplies input; no client reaction is inferred from this socket.
            drainWalQueue();
            try (TableReader a = engine.getReader("a"); TableReader b = engine.getReader("b")) {
                Assert.assertEquals(0, a.size());
                Assert.assertEquals("later B data must not survive refused A", 0, b.size());
            }
            Assert.assertEquals("later B must not publish a WAL transaction", bBefore,
                    engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b")));
        });
    }

    private void assertRefusedBatchPreventsForeignWriterLookup(String... tables) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE c (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            AtomicInteger foreignLookups = new AtomicInteger();
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            try (CairoEngine lookupEngine = new CairoEngine(new DefaultTestCairoConfiguration(root)) {
                @Override
                public WalWriter getWalWriter(TableToken token) {
                    if (!token.getTableName().equals("a")) {
                        foreignLookups.incrementAndGet();
                    }
                    return super.getWalWriter(token);
                }
            }) {
                ingestInOnePass(lookupEngine, new QwpIngressUpgradeProcessor(lookupEngine, httpConfig),
                        httpConfig, denyCommitTo("a"), clientEncodedFrame(1, "a"), clientEncodedFrame(1, tables));
            }
            Assert.assertEquals("refused batch must stop before foreign writer acquisition", 0, foreignLookups.get());
        });
    }

    private static byte[] clientEncodedFrame(int rowsPerBlock, String... tables) {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            encoder.setDeferCommit(false);
            encoder.beginMessage(tables.length, new GlobalSymbolDictionary(), -1, -1);
            for (String table : tables) {
                try (QwpTableBuffer buffer = new QwpTableBuffer(table)) {
                    var value = buffer.getOrCreateColumn("v", io.questdb.client.cutlass.qwp.protocol.QwpConstants.TYPE_LONG, false);
                    var timestamp = buffer.getOrCreateDesignatedTimestampColumn(io.questdb.client.cutlass.qwp.protocol.QwpConstants.TYPE_TIMESTAMP);
                    for (int i = 0; i < rowsPerBlock; i++) {
                        value.addLong(i + 1);
                        timestamp.addLong((i + 1L) * 1_000_000);
                        buffer.nextRow();
                    }
                    encoder.addTable(buffer);
                }
            }
            int length = encoder.finishMessage();
            byte[] payload = new byte[length];
            long address = encoder.getBuffer().getBufferPtr();
            for (int i = 0; i < length; i++) {
                payload[i] = Unsafe.getByte(address + i);
            }
            return createMaskedFrame(WebSocketOpcode.BINARY, payload);
        }
    }

    @Test
    public void testCumulativeAckMustNotLeapfrogGateRejectedFrame() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicBoolean readOnly = new AtomicBoolean(false);
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);

            // The demotable engine below shares the root with the static test
            // engine, which holds the table-registry lock. Pre-create the WAL
            // table through the lock holder so the QWP path only needs to
            // acquire a WAL writer, not register a new table.
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine demotableEngine = new CairoEngine(new DefaultTestCairoConfiguration(root)) {
                private final DurableAckRegistry laggingRegistry = new DurableAckRegistry() {
                    @Override
                    public long getDurablyUploadedSeqTxn(CharSequence tableDirName) {
                        // uploads never catch up: the deferral stays armed for
                        // the whole grace window
                        return -1L;
                    }

                    @Override
                    public boolean isEnabled() {
                        return true;
                    }
                };

                @Override
                public @NotNull DurableAckRegistry getDurableAckRegistry() {
                    return laggingRegistry;
                }

                @Override
                public boolean isReadOnlyMode() {
                    return readOnly.get();
                }
            }) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(demotableEngine, httpConfig);

                byte[] frame0 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L));
                byte[] frame1 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L));
                byte[] frame2 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(300L, 3_000_000L));
                byte[] ping = createMaskedFrame(WebSocketOpcode.PING, new byte[0]);
                byte[] wire = concat(frame0, frame1, frame2, ping);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = new QwpIngressProcessorState(
                            RECV_BUFFER_SIZE,
                            httpConfig.getSendBufferSize(),
                            demotableEngine,
                            httpConfig.getLineHttpProcessorConfiguration()
                    );
                    state.of(-1, AllowAllSecurityContext.INSTANCE);
                    // durable-ack opt-in, as negotiated via X-QWP-Request-Durable-Ack
                    state.setDurableAckEnabled(true);
                    getLV().set(context, state);

                    // Phase A: PRIMARY. seq=0 commits; durable upload lags behind.
                    drive(processor, context, nf, frame0.length);

                    // Phase B: in-place demote. seq=1 is gate-rejected; the
                    // role-change close is deferred awaiting upload coverage;
                    // no error response goes out for seq=1.
                    readOnly.set(true);
                    drive(processor, context, nf, frame1.length);
                    Assert.assertTrue(
                            "test setup: role-change close must be deferred awaiting durable upload coverage",
                            state.isRoleChangeCloseDeferred()
                    );

                    // Phase C: re-promote within the grace window. seq=2 passes
                    // the live read-only gate and commits.
                    readOnly.set(false);
                    drive(processor, context, nf, frame2.length);

                    // Phase D: durable-ack keepalive PING — documented flush
                    // point for pending cumulative ACKs.
                    drive(processor, context, nf, ping.length);

                    // The rejected frame's row must not have been written
                    // (the refusal itself is legitimate; acking it is the bug).
                    drainWalQueue(demotableEngine);
                    long ingestedRows;
                    try (TableReader reader = demotableEngine.getReader("tab")) {
                        ingestedRows = reader.size();
                    }
                    Assert.assertTrue("gate-rejected frame must not be committed", ingestedRows <= 2);

                    // INVARIANT: a cumulative OK-ACK confirms every sequence up
                    // to and including its value, so it must never cover a
                    // sequence that was neither committed nor refused with an
                    // error response.
                    long maxOkAck = maxCumulativeOkAck(rawSocket.sentFrames);
                    boolean seq1Refused = hasErrorResponseForSeq(rawSocket.sentFrames, 1);
                    if (!seq1Refused) {
                        Assert.assertTrue(
                                "SILENT DATA LOSS: cumulative OK-ACK confirms sequences 0.." + maxOkAck
                                        + ", but seq=1 was gate-rejected during the demote, never committed ("
                                        + ingestedRows + " rows in table for " + (maxOkAck + 1)
                                        + " acked frames) and never refused with an error response;"
                                        + " a durable-ack store-and-forward client trims its replay slot"
                                        + " for seq=1 on this ACK and the rows are lost",
                                maxOkAck < 1
                        );
                    }
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /**
     * A per-message error must break the ordered pipeline: a valid frame the
     * client already pipelined behind the failing one must be refused, never
     * committed, and the cumulative OK-ACK must not leapfrog the gap.
     */
    @Test
    public void testErrorRefusesPipelinedTailOnSameConnection() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            // seq=0 valid, seq=1 schema mismatch (VARCHAR "x" into LONG v),
            // seq=2 valid but pipelined behind the gap.
            ObjList<byte[]> sent = ingestOnFreshConnection(
                    processor,
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowVarcharMessage("x", 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(300L, 3_000_000L)),
                    createMaskedFrame(WebSocketOpcode.PING, new byte[0])
            );

            drainWalQueue();
            try (TableReader reader = engine.getReader("tab")) {
                Assert.assertEquals("only seq=0 must commit; the pipelined tail after the error is refused", 1, reader.size());
            }
            Assert.assertTrue("seq=1 must be refused with an error response", hasErrorResponseForSeq(sent, 1));
            long maxOkAck = maxCumulativeOkAck(sent);
            Assert.assertTrue("cumulative OK-ACK must not leapfrog the errored seq=1 (was " + maxOkAck + ")", maxOkAck < 1);
        });
    }

    /**
     * A frame refused because a prior error broke the pipeline is neither
     * committed nor acked, so a reconnecting client replays it from its acked
     * watermark and it lands exactly once -- no loss, no duplicate.
     */
    @Test
    public void testReconnectReplayLandsRefusedTailExactlyOnce() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            // Connection 1: A commits, B (schema mismatch) errors, C is refused.
            ingestOnFreshConnection(
                    processor,
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowVarcharMessage("x", 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(300L, 3_000_000L))
            );
            drainWalQueue();
            try (TableReader reader = engine.getReader("tab")) {
                Assert.assertEquals("first connection commits only A", 1, reader.size());
            }

            // Reconnect: the client replays its unacked tail from ackedFsn+1 --
            // the corrected B and the still-valid C.
            ingestOnFreshConnection(
                    processor,
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(300L, 3_000_000L))
            );
            drainWalQueue();
            try (TableReader reader = engine.getReader("tab")) {
                // A + B + C, C exactly once (never committed on connection 1).
                Assert.assertEquals("replayed tail must land exactly once, no duplicate", 3, reader.size());
            }
        });
    }

    /**
     * The appender reuses its column resolution for a repeated schema only while
     * the writer's metadata is unchanged: after a rename, the same column name in
     * the same schema must resolve again (here: auto-create a new {@code v}), not
     * land in the renamed column's slot.
     */
    @Test
    public void testColumnResolutionFollowsRenameBetweenFrames() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            byte[] frame0 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L));
            byte[] frame1 = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L));
            PhasedNetworkFacade nf = new PhasedNetworkFacade(concat(frame0, frame1));
            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                QwpIngressProcessorState state = new QwpIngressProcessorState(
                        RECV_BUFFER_SIZE,
                        httpConfig.getSendBufferSize(),
                        engine,
                        httpConfig.getLineHttpProcessorConfiguration()
                );
                state.of(-1, AllowAllSecurityContext.INSTANCE);
                getLV().set(context, state);

                drive(processor, context, nf, frame0.length);
                drainWalQueue();
                execute("alter table tab rename column v to w");
                drainWalQueue();
                drive(processor, context, nf, frame1.length);
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
            drainWalQueue();
            assertQuery("select w, v from tab").noLeakCheck().expectSize().returns("""
                    w\tv
                    100\tnull
                    null\t200
                    """);
        });
    }

    /**
     * Random frame sequences through the real handler in one receive pass, with random
     * faults: refused INSERTs, read-only flips (transient or persistent), a small
     * max-uncommitted-rows cap, durable acks, deferred groups, malformed and
     * schema-mismatched frames and PINGs. Every iteration checks the ack contract
     * against the tables' contents, with A the highest cumulative OK ack:
     * <ul>
     * <li>I1: every row of every frame at or below A is committed;</li>
     * <li>I2: no row of a frame above A is committed (the client replays from A + 1),
     * and no row is committed twice.</li>
     * </ul>
     * Iterations whose only faults are the frames' own data errors also check I3:
     * the frames before the first bad frame are committed and acked, and the bad
     * frame is NACKed with its own status, as one commit per frame would leave it.
     */
    @Test
    public void testCommitBatchAckContractFuzz() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final int iterations = 150;
            // The hook engine below shares the root but cannot create tables: create them first.
            for (int iteration = 0; iteration < iterations; iteration++) {
                execute("CREATE TABLE fa" + iteration + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("CREATE TABLE fb" + iteration + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            }
            final AtomicLong readOnlyCalls = new AtomicLong();
            final AtomicLong readOnlyFromCall = new AtomicLong(-1);
            final AtomicBoolean isReadOnlyTransient = new AtomicBoolean();
            try (CairoEngine hookEngine = new CairoEngine(new DefaultTestCairoConfiguration(root)) {
                @Override
                public boolean isReadOnlyMode() {
                    final long call = readOnlyCalls.incrementAndGet();
                    final long from = readOnlyFromCall.get();
                    return from > 0 && (isReadOnlyTransient.get() ? call == from : call >= from);
                }
            }) {
                for (int iteration = 0; iteration < iterations; iteration++) {
                    runCommitBatchFuzzIteration(rnd, hookEngine, iteration, readOnlyCalls, readOnlyFromCall, isReadOnlyTransient);
                }
            }
        });
    }

    @Test
    public void testRenameWindowMidBatchAcksNothing() throws Exception {
        assertRenameWindowMidBatchAcksNothing(false);
    }

    @Test
    public void testRenameWindowMidBatchWithRefusedInsertAcksNothing() throws Exception {
        assertRenameWindowMidBatchAcksNothing(true);
    }

    private void runCommitBatchFuzzIteration(
            Rnd rnd,
            CairoEngine hookEngine,
            int iteration,
            AtomicLong readOnlyCalls,
            AtomicLong readOnlyFromCall,
            AtomicBoolean isReadOnlyTransient
    ) throws Exception {
        final String[] tables = {"fa" + iteration, "fb" + iteration};
        final boolean isDataErrorsOnly = rnd.nextInt(3) == 0;
        final boolean isDurable = rnd.nextBoolean();
        final long cap = rnd.nextInt(3) == 0 ? 1 + rnd.nextInt(4) : 1_000_000;
        readOnlyCalls.set(0);
        readOnlyFromCall.set(!isDataErrorsOnly && rnd.nextInt(3) == 0 ? 1 + rnd.nextInt(30) : -1);
        isReadOnlyTransient.set(rnd.nextBoolean());
        final int refusedInsertCall = !isDataErrorsOnly && rnd.nextInt(3) == 0 ? 1 + rnd.nextInt(6) : -1;
        final boolean isRefusalPersistent = rnd.nextBoolean();
        final boolean isCommitFaulty = refusedInsertCall > 0 || readOnlyFromCall.get() > 0;
        final AtomicInteger insertCalls = new AtomicInteger();
        final SecurityContext securityContext = new AllowAllSecurityContext() {
            @Override
            public void authorizeInsert(TableToken tableToken) {
                final int call = insertCalls.incrementAndGet();
                if (refusedInsertCall > 0 && (isRefusalPersistent ? call >= refusedInsertCall : call == refusedInsertCall)) {
                    throw CairoException.authorization().put("fuzz: insert refused");
                }
            }
        };

        // Frame values are seq * 10 + row, so a committed value names its frame.
        final StringBuilder kinds = new StringBuilder();
        final ObjList<byte[]> wireFrames = new ObjList<>();
        final ObjList<LongList> frameValues = new ObjList<>();
        final LongList frameErrorStatus = new LongList();
        final int frameCount = 2 + rnd.nextInt(10);
        int seq = 0;
        for (int f = 0; f < frameCount; f++) {
            int kind = rnd.nextInt(isDataErrorsOnly ? 8 : 11);
            if ((cap < 1_000_000 && kind == 4) || (kind == 8 && (cap < 1_000_000 || isCommitFaulty))) {
                // Outside the commit batch's contract, and the same on master: a frame
                // of more than one block, or a deferred group, can be split by the
                // appender's own commit at a small cap, and commitAll commits a group's
                // tables one by one; a later commit of the same frame or group that then
                // fails NACKs rows already in the WAL.
                kind = 0;
            }
            final String table = tables[rnd.nextInt(2)];
            final long v = seq * 10L;
            final long ts = 1_000_000L + seq * 1_000L;
            final LongList values = new LongList();
            long errorStatus = -1;
            final byte[] message;
            kinds.append(kind).append(table.charAt(1)).append(' ');
            if (kind == 10) {
                // PING: a control frame, no sequence
                wireFrames.add(createMaskedFrame(WebSocketOpcode.PING, new byte[0]));
                continue;
            } else if (kind <= 3) {
                message = oneRowMessage(table, v, ts);
                values.add(v);
            } else if (kind == 4) {
                message = twoBlockMessage(table, new long[]{v, ts}, new long[]{v + 1, ts + 1});
                values.add(v);
                values.add(v + 1);
            } else if (kind == 5) {
                message = Arrays.copyOf(oneRowMessage(table, v, ts), QwpConstants.HEADER_SIZE + 8);
                errorStatus = QwpConstants.STATUS_PARSE_ERROR;
            } else if (kind == 6) {
                message = oneRowVarcharMessage(table, "x", ts);
                errorStatus = QwpConstants.STATUS_SCHEMA_MISMATCH;
            } else if (kind == 7) {
                message = oneRowMessage(table, v, ts);
                values.add(v);
            } else if (kind == 8) {
                message = deferred(oneRowMessage(table, v, ts));
                values.add(v);
            } else {
                message = rowlessDeferredMessage();
            }
            wireFrames.add(createMaskedFrame(WebSocketOpcode.BINARY, message));
            frameValues.add(values);
            frameErrorStatus.add(errorStatus);
            seq++;
        }
        final int lastSeq = seq - 1;
        final byte[][] frames = new byte[wireFrames.size()][];
        for (int i = 0, n = wireFrames.size(); i < n; i++) {
            frames[i] = wireFrames.getQuick(i);
        }

        final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration) {
            private final LineHttpProcessorConfiguration lineConfig = new DefaultLineHttpProcessorConfiguration(configuration) {
                @Override
                public long getQwpMaxUncommittedRows() {
                    return cap;
                }
            };

            @Override
            public LineHttpProcessorConfiguration getLineHttpProcessorConfiguration() {
                return lineConfig;
            }
        };
        final ObjList<byte[]> sent = new ObjList<>();
        try {
            ingestInOnePass(hookEngine, new QwpIngressUpgradeProcessor(hookEngine, httpConfig), httpConfig,
                    securityContext, isDurable, sent, frames);
        } catch (ServerDisconnectException ignore) {
            // a role-change CLOSE tells the dispatcher to drop the fd
        }
        drainWalQueue(hookEngine);

        final String context = "iteration=" + iteration + ", frames=" + frameCount + ", dataErrorsOnly=" + isDataErrorsOnly
                + ", durable=" + isDurable + ", cap=" + cap + ", readOnlyFrom=" + readOnlyFromCall.get()
                + ", readOnlyTransient=" + isReadOnlyTransient.get() + ", refusedInsert=" + refusedInsertCall
                + ", refusalPersistent=" + isRefusalPersistent + ", kinds=" + kinds;
        final long acked = maxCumulativeOkAck(sent);
        final LongList committed = new LongList();
        for (String table : tables) {
            try (
                    TableReader reader = hookEngine.getReader(table);
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                while (cursor.hasNext()) {
                    committed.add(cursor.getRecord().getLong(0));
                }
            }
        }
        committed.sort();
        for (int i = 0, n = committed.size(); i < n; i++) {
            final long value = committed.getQuick(i);
            Assert.assertTrue("I2: a row committed twice [" + context + ", value=" + value + "]",
                    i == 0 || committed.getQuick(i - 1) != value);
            Assert.assertTrue("I2: a row of a frame above the ack is committed [" + context + ", value=" + value
                    + ", acked=" + acked + "]", value / 10 <= acked);
        }
        for (int s = 0; s <= acked; s++) {
            final LongList values = frameValues.getQuick(s);
            for (int i = 0, n = values.size(); i < n; i++) {
                Assert.assertTrue("I1: an acked frame's row is not committed [" + context + ", seq=" + s + "]",
                        committed.binarySearch(values.getQuick(i), Vect.BIN_SEARCH_SCAN_UP) >= 0);
            }
        }
        if (isDataErrorsOnly) {
            int firstError = -1;
            for (int s = 0; s <= lastSeq && firstError < 0; s++) {
                if (frameErrorStatus.getQuick(s) >= 0) {
                    firstError = s;
                }
            }
            Assert.assertEquals("I3: frames before the first bad frame are acked [" + context + "]",
                    firstError < 0 ? lastSeq : firstError - 1, acked);
            for (int s = 0; s < (firstError < 0 ? lastSeq + 1 : firstError); s++) {
                Assert.assertFalse("I3: a good frame before the bad one is NACKed [" + context + ", seq=" + s + "]",
                        hasErrorResponseForSeq(sent, s));
            }
            if (firstError >= 0) {
                Assert.assertEquals("I3: the bad frame gets its own status [" + context + "]",
                        frameErrorStatus.getQuick(firstError), errorStatusForSeq(sent, firstError));
            }
        }
    }

    private static long errorStatusForSeq(ObjList<byte[]> frames, long seq) {
        for (int i = 0, n = frames.size(); i < n; i++) {
            byte[] f = frames.getQuick(i);
            if (isBinaryFrame(f) && f[2] != QwpConstants.STATUS_OK && readLeLong(f, 3) == seq) {
                return f[2];
            }
        }
        return -1;
    }

    /**
     * Like {@link #oneRowMessage(String, long, long)} but declares {@code v} as VARCHAR,
     * which a LONG column rejects as SCHEMA_MISMATCH.
     */
    private static byte[] oneRowVarcharMessage(String table, String value, long tsMicros) {
        byte[] name = table.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] utf8 = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] payload = new byte[1 + name.length + 7 + 1 + 8 + utf8.length + 1 + 8];
        int i = 0;
        payload[i++] = (byte) name.length;
        System.arraycopy(name, 0, payload, i, name.length);
        i += name.length;
        payload[i++] = 1; // rowCount
        payload[i++] = 2; // columnCount
        payload[i++] = 1; // column name length
        payload[i++] = 'v';
        payload[i++] = QwpConstants.TYPE_VARCHAR;
        payload[i++] = 0; // empty name = designated timestamp
        payload[i++] = QwpConstants.TYPE_TIMESTAMP;
        payload[i++] = 0; // no null bitmap
        i = writeLeInt(payload, i, 0);
        i = writeLeInt(payload, i, utf8.length);
        System.arraycopy(utf8, 0, payload, i, utf8.length);
        i += utf8.length;
        payload[i++] = 0; // no null bitmap
        writeLeLong(payload, i, tsMicros);

        byte[] message = new byte[QwpConstants.HEADER_SIZE + payload.length];
        message[0] = 'Q';
        message[1] = 'W';
        message[2] = 'P';
        message[3] = '1';
        message[4] = QwpConstants.VERSION;
        message[5] = 0;
        message[6] = 1;
        message[7] = 0;
        message[8] = (byte) payload.length;
        System.arraycopy(payload, 0, message, QwpConstants.HEADER_SIZE, payload.length);
        return message;
    }

    /**
     * A RENAME TABLE lands while a commit batch is pending, frozen in the window where
     * the sequencer already logged it but the name registry still maps the old name
     * (CairoEngine.rename publishes the sequencer txn first). The next frame is a batch
     * boundary: the rename moved the table's structure version. The batch's own commit
     * is then refused -- by the sequencer's token check, or by an INSERT refusal that
     * lands with the rename -- so nothing of the batch may be acked; the client replays
     * it by name.
     */
    private void assertRenameWindowMidBatchAcksNothing(boolean isInsertRefused) throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");
            final AtomicBoolean isFrozen = new AtomicBoolean();
            final AtomicBoolean isDenied = new AtomicBoolean();
            final AtomicLong renameTxn = new AtomicLong(-1);
            final QwpIngressProcessorState[] stateRef = new QwpIngressProcessorState[1];
            try (CairoEngine hookEngine = new CairoEngine(new DefaultTestCairoConfiguration(root)) {
                @Override
                public boolean isReadOnlyMode() {
                    // The first gate check with a batch pending is frame 1's preparation.
                    if (!isFrozen.get() && stateRef[0] != null && stateRef[0].isCommitBatchPending()) {
                        isFrozen.set(true);
                        try (WalWriter w = getWalWriter(verifyTableName("tab"))) {
                            renameTxn.set(w.renameTable("tab", "tab_new", AllowAllSecurityContext.INSTANCE));
                        }
                        isDenied.set(isInsertRefused);
                    }
                    return false;
                }
            }) {
                final TableToken token = hookEngine.verifyTableName("tab");
                final long txnBefore = hookEngine.getTableSequencerAPI().lastTxn(token);
                final SecurityContext securityContext = new AllowAllSecurityContext() {
                    @Override
                    public void authorizeInsert(TableToken tableToken) {
                        if (isDenied.get()) {
                            throw CairoException.authorization().put("test: insert denied on ").put(tableToken.getTableName());
                        }
                    }
                };
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(hookEngine, httpConfig);
                byte[] wire = concat(
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L))
                );
                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = new QwpIngressProcessorState(
                            RECV_BUFFER_SIZE,
                            httpConfig.getSendBufferSize(),
                            hookEngine,
                            httpConfig.getLineHttpProcessorConfiguration()
                    );
                    state.of(-1, securityContext);
                    stateRef[0] = state;
                    getLV().set(context, state);
                    drive(processor, context, nf, wire.length);
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
                Assert.assertTrue("test setup: the rename window must have been frozen", isFrozen.get());
                Assert.assertEquals("test setup: the rename is the only txn", txnBefore + 1, renameTxn.get());
                Assert.assertEquals("no data txn: the batch's commit was refused",
                        renameTxn.get(), hookEngine.getTableSequencerAPI().lastTxn(token));
                Assert.assertEquals("nothing committed may be acked", -1, maxCumulativeOkAck(rawSocket.sentFrames));
                Assert.assertTrue("the batch is refused at its first frame", hasErrorResponseForSeq(rawSocket.sentFrames, 0));
                Assert.assertFalse("the boundary frame is dropped and replayed with the batch",
                        hasErrorResponseForSeq(rawSocket.sentFrames, 1));
            }
        });
    }

    /**
     * A commit batch holds frames of one table only: a frame for another table
     * commits the batch's table first. One writer's commit is all or nothing,
     * so no failure can commit part of a batch.
     */
    @Test
    public void testCommitBatchNeverSpansTables() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table a (v long, ts timestamp) timestamp(ts) partition by day wal");
            execute("create table b (v long, ts timestamp) timestamp(ts) partition by day wal");
            final long aBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("a"));
            final long bBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b"));

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            ObjList<byte[]> sent = ingestInOnePass(
                    processor,
                    httpConfig,
                    AllowAllSecurityContext.INSTANCE,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 300L, 3_000_000L))
            );

            drainWalQueue();
            assertQuery("select v from a").noLeakCheck().expectSize().returns("v\n100\n300\n");
            assertQuery("select v from b").noLeakCheck().expectSize().returns("v\n200\n");
            Assert.assertEquals("a's frames are split by b's: two commits",
                    2, engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("a")) - aBefore);
            Assert.assertEquals(1, engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b")) - bBefore);
            Assert.assertEquals(2, maxCumulativeOkAck(sent));
        });
    }

    /**
     * A commit failure on one table must not commit another table's rows from
     * frames the client will replay. Here b's commit is refused: frame 0 (a) is
     * committed and acked on its own, frame 1 (b) is NACKed, and frame 2 (a),
     * behind the gap, is neither committed nor acked -- one commit per frame
     * would leave exactly this, so the replay duplicates nothing.
     */
    @Test
    public void testCommitFailureOnOneTableCommitsNoLaterFramesOfAnother() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table a (v long, ts timestamp) timestamp(ts) partition by day wal");
            execute("create table b (v long, ts timestamp) timestamp(ts) partition by day wal");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            ObjList<byte[]> sent = ingestInOnePass(
                    processor,
                    httpConfig,
                    denyCommitTo("b"),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 300L, 3_000_000L))
            );

            drainWalQueue();
            assertQuery("select v from a").noLeakCheck().expectSize().returns("v\n100\n");
            assertQuery("select count() from b").noLeakCheck().inferRandomAccess().sizeMayVary().returns("count\n0\n");
            Assert.assertEquals(0, maxCumulativeOkAck(sent));
            Assert.assertTrue("the failed commit must be NACKed at the batch's first frame", hasErrorResponseForSeq(sent, 1));
        });
    }

    // A plain frame closing a rowless deferred group commits and acks on its own, so a later NACK cannot strand its rows.
    @Test
    public void testPlainFrameAfterRowlessDeferredFrameIsAckedBeforeLaterNack() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("CREATE TABLE a (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE b (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            ObjList<byte[]> sent = ingestInOnePass(
                    processor,
                    httpConfig,
                    denyCommitTo("b"),
                    createMaskedFrame(WebSocketOpcode.BINARY, rowlessDeferredMessage()),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L))
            );

            Assert.assertEquals("the ack must cover the committed frame and stop at the refused one", 1, maxCumulativeOkAck(sent));
            Assert.assertTrue("the refused frame must be NACKed by its own sequence", hasErrorResponseForSeq(sent, 2));
            Assert.assertFalse("a NACK at seq=1 would replay committed rows", hasErrorResponseForSeq(sent, 1));
            int ackIndex = indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 1);
            int errorIndex = -1;
            for (int i = 0, n = sent.size(); i < n && errorIndex < 0; i++) {
                byte[] f = sent.getQuick(i);
                if (isBinaryFrame(f) && f[2] != QwpConstants.STATUS_OK && readLeLong(f, 3) == 2) {
                    errorIndex = i;
                }
            }
            Assert.assertTrue("the committed frame must be acked", ackIndex >= 0);
            Assert.assertTrue("the ack must precede the error", ackIndex < errorIndex);

            drainWalQueue();
            assertQuery("SELECT v FROM a").noLeakCheck().expectSize().returns("v\n100\n");
        });
    }

    /**
     * The appender commits a writer on its own once its uncommitted rows reach
     * maxUncommittedRows. A commit batch never lets that happen: a frame whose rows
     * would reach the cap is a boundary, so the batch commits first. With a cap of 2
     * and every commit after the first denied, the wire matches one commit per frame:
     * frame 0 committed and acked, frame 1 NACKed, frame 2 refused behind it.
     */
    @Test
    public void testBatchCommitsBeforeReachingCap() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration) {
                private final LineHttpProcessorConfiguration lineConfig = new DefaultLineHttpProcessorConfiguration(configuration) {
                    @Override
                    public long getQwpMaxUncommittedRows() {
                        return 2;
                    }
                };

                @Override
                public LineHttpProcessorConfiguration getLineHttpProcessorConfiguration() {
                    return lineConfig;
                }
            };
            execute("create table b (v long, ts timestamp) timestamp(ts) partition by day wal");
            final long before = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b"));

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            ObjList<byte[]> sent = ingestInOnePass(
                    processor,
                    httpConfig,
                    denyCommitsAfterFirst("b"),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 300L, 3_000_000L))
            );

            drainWalQueue();
            assertQuery("select v from b").noLeakCheck().expectSize().returns("v\n100\n");
            Assert.assertEquals("only frame 0 committed", 1, engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b")) - before);
            Assert.assertEquals("the committed frame must be acked", 0, maxCumulativeOkAck(sent));
            Assert.assertTrue("the denied commit must be NACKed at its batch's first frame", hasErrorResponseForSeq(sent, 1));
            Assert.assertFalse(hasErrorResponseForSeq(sent, 0));
            Assert.assertFalse("a frame behind the refused batch gets no response", hasErrorResponseForSeq(sent, 2));
        });
    }

    /**
     * A multi-block frame resolves the prefix first. That boundary is the first
     * successful commit; the standalone frame's first block reaches the cap and
     * its second commit attempt is denied. Only the prefix survives.
     */
    @Test
    public void testStandaloneFrameCapFailureIsNackedAfterBoundaryAck() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration) {
                private final LineHttpProcessorConfiguration lineConfig = new DefaultLineHttpProcessorConfiguration(configuration) {
                    @Override
                    public long getQwpMaxUncommittedRows() {
                        return 2;
                    }
                };

                @Override
                public LineHttpProcessorConfiguration getLineHttpProcessorConfiguration() {
                    return lineConfig;
                }
            };
            execute("create table b (v long, ts timestamp) timestamp(ts) partition by day wal");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            ObjList<byte[]> sent = ingestInOnePass(
                    processor,
                    httpConfig,
                    denyCommitsAfterFirst("b"),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, twoBlockMessage(
                            "b",
                            new long[]{200L, 2_000_000L, 300L, 3_000_000L},
                            new long[]{400L, 4_000_000L}
                    ))
            );

            drainWalQueue();
            assertQuery("select v from b").noLeakCheck().expectSize().returns("v\n100\n");
            Assert.assertEquals("the boundary-committed prefix must be acked", 0, maxCumulativeOkAck(sent));
            Assert.assertTrue("the denied frame must be NACKed by its own sequence", hasErrorResponseForSeq(sent, 1));
            Assert.assertFalse("a NACK at seq=0 would replay committed rows", hasErrorResponseForSeq(sent, 0));
        });
    }

    /**
     * Durable-ack connections batch commits too: the single ack after the batch
     * commit names that commit's seqTxn, which a durable-ack client waits on for
     * every frame the ack covers.
     */
    @Test
    public void testDurableAckFramesShareOneCommit() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");
            final long txnBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("tab"));

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            ObjList<byte[]> sent = ingestDurableInOnePass(
                    processor,
                    httpConfig,
                    AllowAllSecurityContext.INSTANCE,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(300L, 3_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(400L, 4_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(500L, 5_000_000L))
            );

            drainWalQueue();
            try (TableReader reader = engine.getReader("tab")) {
                Assert.assertEquals(5, reader.size());
            }
            Assert.assertEquals("five frames in one receive pass must commit as one WAL txn",
                    txnBefore + 1, engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("tab")));
            Assert.assertEquals(4, maxCumulativeOkAck(sent));
            Assert.assertEquals("one ack after the batch commit", 1, countOkAcks(sent));
            Assert.assertEquals("the ack must name the batch's seqTxn", txnBefore + 1, okAckSeqTxn(sent, 4, "tab"));
        });
    }

    /**
     * The explicit boundary commits the prefix before a standalone multi-block
     * frame reaches the cap and fails. The prefix ACK must report that boundary's
     * seqTxn even though the current frame fails before its ordinary commit.
     */
    @Test
    public void testDurableAckCarriesBoundaryCommitBeforeStandaloneCapFailure() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration) {
                private final LineHttpProcessorConfiguration lineConfig = new DefaultLineHttpProcessorConfiguration(configuration) {
                    @Override
                    public long getQwpMaxUncommittedRows() {
                        return 2;
                    }
                };

                @Override
                public LineHttpProcessorConfiguration getLineHttpProcessorConfiguration() {
                    return lineConfig;
                }
            };
            execute("create table b (v long, ts timestamp) timestamp(ts) partition by day wal");
            final long txnBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("b"));

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            ObjList<byte[]> sent = ingestDurableInOnePass(
                    processor,
                    httpConfig,
                    denyCommitsAfterFirst("b"),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, twoBlockMessage(
                            "b",
                            new long[]{200L, 2_000_000L, 300L, 3_000_000L},
                            new long[]{400L, 4_000_000L}
                    ))
            );

            drainWalQueue();
            assertQuery("select v from b").noLeakCheck().expectSize().returns("v\n100\n");
            Assert.assertEquals("the boundary-committed prefix must be acked", 0, maxCumulativeOkAck(sent));
            Assert.assertTrue("the denied frame must be NACKed by its own sequence", hasErrorResponseForSeq(sent, 1));
            Assert.assertTrue("the prefix ack must name the boundary commit",
                    okAckSeqTxn(sent, 0, "b") > txnBefore);
        });
    }

    /**
     * As {@link #testDurableAckCarriesBoundaryCommitBeforeStandaloneCapFailure}, but
     * the standalone frame cap-commits its first block, then fails its second
     * block's schema. Its partial commit survives, independently of the prefix.
     */
    @Test
    public void testDurableAckCarriesBoundaryCommitBeforeStandaloneSchemaFailure() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration) {
                private final LineHttpProcessorConfiguration lineConfig = new DefaultLineHttpProcessorConfiguration(configuration) {
                    @Override
                    public long getQwpMaxUncommittedRows() {
                        return 2;
                    }
                };

                @Override
                public LineHttpProcessorConfiguration getLineHttpProcessorConfiguration() {
                    return lineConfig;
                }
            };
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");
            final long txnBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("tab"));
            final byte[] varcharMessage = oneRowVarcharMessage("not-a-long", 4_000_000L);
            final byte[] mismatchedBlock = Arrays.copyOfRange(varcharMessage, QwpConstants.HEADER_SIZE, varcharMessage.length);

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            ObjList<byte[]> sent = ingestDurableInOnePass(
                    processor,
                    httpConfig,
                    AllowAllSecurityContext.INSTANCE,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, twoBlockMessage(
                            tableBlock("tab".getBytes(java.nio.charset.StandardCharsets.UTF_8), new long[]{200L, 2_000_000L, 300L, 3_000_000L}),
                            mismatchedBlock
                    ))
            );

            drainWalQueue();
            assertQuery("select v from tab").noLeakCheck().expectSize().returns("v\n100\n200\n300\n");
            Assert.assertEquals("the boundary-committed prefix must be acked", 0, maxCumulativeOkAck(sent));
            Assert.assertTrue("the mismatched frame must be NACKed by its own sequence", hasErrorResponseForSeq(sent, 1));
            Assert.assertTrue("the prefix ack must name the boundary commit",
                    okAckSeqTxn(sent, 0, "tab") > txnBefore);
        });
    }

    /**
     * A failed batch commit is NACKed at the batch's first frame: nothing from it
     * on is committed, and it is the frame the client replays first, so a
     * deterministic failure keeps striking the same frame.
     */
    @Test
    public void testFailedBatchCommitIsNackedAtFirstFrame() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table b (v long, ts timestamp) timestamp(ts) partition by day wal");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            ObjList<byte[]> sent = ingestInOnePass(
                    processor,
                    httpConfig,
                    denyCommitTo("b"),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("b", 300L, 3_000_000L))
            );

            Assert.assertEquals(-1, maxCumulativeOkAck(sent));
            Assert.assertTrue("NACK must name the batch's first frame", hasErrorResponseForSeq(sent, 0));
            Assert.assertFalse(hasErrorResponseForSeq(sent, 2));
        });
    }

    private static SecurityContext denyCommitsAfterFirst(String tableName) {
        // On the QWP path authorizeInsert runs only at commit, and only for a commit
        // with rows (TableUpdateDetails.commit): the first such commit passes.
        return new AllowAllSecurityContext() {
            private int commits;

            @Override
            public void authorizeInsert(TableToken tableToken) {
                if (tableToken.getTableName().equals(tableName) && ++commits > 1) {
                    throw CairoException.nonCritical().put("test: commit denied for ").put(tableName);
                }
            }
        };
    }

    private static SecurityContext denyCommitTo(String tableName) {
        // On the QWP path authorizeInsert runs only at commit (TableUpdateDetails.authorizeCommit).
        return new AllowAllSecurityContext() {
            @Override
            public void authorizeInsert(TableToken tableToken) {
                if (tableToken.getTableName().equals(tableName)) {
                    throw CairoException.nonCritical().put("test: commit denied for ").put(tableName);
                }
            }
        };
    }

    /**
     * One message with two table blocks for the same table; each block is a flat
     * array of (v, ts) pairs.
     */
    private static byte[] twoBlockMessage(String table, long[] firstBlock, long[] secondBlock) {
        byte[] name = table.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assert name.length < 128;
        return twoBlockMessage(tableBlock(name, firstBlock), tableBlock(name, secondBlock));
    }

    private static byte[] twoBlockMessage(byte[] first, byte[] second) {
        byte[] message = new byte[QwpConstants.HEADER_SIZE + first.length + second.length];
        message[0] = 'Q';
        message[1] = 'W';
        message[2] = 'P';
        message[3] = '1';
        message[4] = QwpConstants.VERSION;
        message[5] = 0; // flags
        message[6] = 2; // tableCount lo
        message[7] = 0; // tableCount hi
        final int payloadLength = first.length + second.length;
        assert payloadLength < 256;
        message[8] = (byte) payloadLength;
        System.arraycopy(first, 0, message, QwpConstants.HEADER_SIZE, first.length);
        System.arraycopy(second, 0, message, QwpConstants.HEADER_SIZE + first.length, second.length);
        return message;
    }

    private static byte[] tableBlock(byte[] name, long[] pairs) {
        final int rows = pairs.length / 2;
        assert rows < 128;
        byte[] block = new byte[1 + name.length + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 8 * rows + 1 + 8 * rows];
        int i = 0;
        block[i++] = (byte) name.length;
        System.arraycopy(name, 0, block, i, name.length);
        i += name.length;
        block[i++] = (byte) rows; // rowCount
        block[i++] = 2; // columnCount
        block[i++] = 1; // column name length
        block[i++] = 'v';
        block[i++] = QwpConstants.TYPE_LONG;
        block[i++] = 0; // empty name = designated timestamp
        block[i++] = QwpConstants.TYPE_TIMESTAMP;
        block[i++] = 0; // no null bitmap
        for (int r = 0; r < rows; r++) {
            i = writeLeLong(block, i, pairs[2 * r]);
        }
        block[i++] = 0; // no null bitmap
        for (int r = 0; r < rows; r++) {
            i = writeLeLong(block, i, pairs[2 * r + 1]);
        }
        assert i == block.length;
        return block;
    }

    private static byte[] oneRowMessage(String table, long value, long tsMicros) {
        byte[] name = table.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assert name.length < 128;
        byte[] payload = new byte[1 + name.length + 25];
        int i = 0;
        payload[i++] = (byte) name.length;
        System.arraycopy(name, 0, payload, i, name.length);
        i += name.length;
        payload[i++] = 1; // rowCount
        payload[i++] = 2; // columnCount
        payload[i++] = 1; // column name length
        payload[i++] = 'v';
        payload[i++] = QwpConstants.TYPE_LONG;
        payload[i++] = 0; // empty name = designated timestamp
        payload[i++] = QwpConstants.TYPE_TIMESTAMP;
        payload[i++] = 0; // no null bitmap
        i = writeLeLong(payload, i, value);
        payload[i++] = 0; // no null bitmap
        writeLeLong(payload, i, tsMicros);

        byte[] message = new byte[QwpConstants.HEADER_SIZE + payload.length];
        message[0] = 'Q';
        message[1] = 'W';
        message[2] = 'P';
        message[3] = '1';
        message[4] = QwpConstants.VERSION;
        message[5] = 0; // flags
        message[6] = 1; // tableCount lo
        message[7] = 0; // tableCount hi
        message[8] = (byte) payload.length;
        System.arraycopy(payload, 0, message, QwpConstants.HEADER_SIZE, payload.length);
        return message;
    }

    /**
     * A fatal CLOSE for an oversized frame flushes the acks of the frames before
     * it; those frames must have been committed first, as one commit per frame
     * would have done.
     */
    @Test
    public void testOversizedFrameCommitsAndAcksTheBatchBeforeClosing() throws Exception {
        final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
        assertBatchCommittedBeforeFatalClose(oversizedFrameHeader(httpConfig.getRecvBufferSize() + 1L));
    }

    /**
     * Same for a fragmented BINARY frame, which is refused with a fatal CLOSE.
     */
    @Test
    public void testFragmentedFrameCommitsAndAcksTheBatchBeforeClosing() throws Exception {
        assertBatchCommittedBeforeFatalClose(createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(300L, 3_000_000L), false));
    }

    private void assertBatchCommittedBeforeFatalClose(byte[] badFrame) throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            final ObjList<byte[]> sent = new ObjList<>();
            try {
                ingestInOnePass(
                        engine,
                        processor,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        sent,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                        badFrame
                );
            } catch (ServerDisconnectException ignore) {
                // the fatal CLOSE ends with a drain and disconnect
            }
            int closeIndex = -1;
            for (int i = 0, n = sent.size(); i < n && closeIndex < 0; i++) {
                if ((sent.getQuick(i)[0] & 0x0F) == WebSocketOpcode.CLOSE) {
                    closeIndex = i;
                }
            }
            Assert.assertTrue("test setup: the bad frame must draw a fatal CLOSE", closeIndex >= 0);
            drainWalQueue();
            assertQuery("select v from tab").noLeakCheck().expectSize().returns("v\n100\n200\n");
            Assert.assertEquals("the frames before the bad one must be acked ahead of the CLOSE", 1, maxCumulativeOkAck(sent));
            Assert.assertTrue("the ack must precede the CLOSE", indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 1) < closeIndex);
        });
    }

    private static byte[] oversizedFrameHeader(long len) {
        // BINARY, FIN, masked, 64-bit length above the configured receive buffer;
        // the payload never needs to arrive: the header alone is refused.
        byte[] header = new byte[2 + 8 + 4];
        header[0] = (byte) (0x80 | WebSocketOpcode.BINARY);
        header[1] = (byte) (0x80 | 127);
        for (int i = 0; i < 8; i++) {
            header[2 + i] = (byte) (len >>> (56 - 8 * i));
        }
        System.arraycopy(DEFAULT_MASK_KEY, 0, header, 10, 4);
        return header;
    }

    /**
     * Plain frames already in the receive buffer share one WAL commit, and the
     * single ack after it covers all of them.
     */
    @Test
    public void testPipelinedFramesShareOneCommit() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");
            final long txnBefore = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("tab"));

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            ObjList<byte[]> sent = ingestInOnePass(
                    processor,
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(300L, 3_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(400L, 4_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(500L, 5_000_000L))
            );

            drainWalQueue();
            try (TableReader reader = engine.getReader("tab")) {
                Assert.assertEquals(5, reader.size());
            }
            Assert.assertEquals("five frames in one receive pass must commit as one WAL txn",
                    1, engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("tab")) - txnBefore);
            Assert.assertEquals(4, maxCumulativeOkAck(sent));
            Assert.assertEquals("one ack after the batch commit", 1, countOkAcks(sent));
        });
    }

    /**
     * A frame that fails right behind a commit batch costs only its own rows. Its
     * schema differs from the batch's, so it cannot join: the batch commits and is
     * acked before the frame's error goes out, as one commit per frame would leave
     * it. The client's poison detector strikes the frame a NACK names and replays
     * from its acked watermark, so losing the batch here would replay good frames
     * behind a NACK for a later one.
     */
    @Test
    public void testFailedFrameInCommitBatchKeepsEarlierFramesCommittedAndAcked() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            // seq=0,1 valid, seq=2 schema mismatch, seq=3 valid but behind the gap.
            ObjList<byte[]> sent = ingestInOnePass(
                    processor,
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowVarcharMessage("x", 3_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(400L, 4_000_000L))
            );

            drainWalQueue();
            assertQuery("select v from tab").noLeakCheck().expectSize().returns("v\n100\n200\n");
            int ackIndex = indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 1);
            int errorIndex = -1;
            for (int i = 0, n = sent.size(); i < n && errorIndex < 0; i++) {
                byte[] f = sent.getQuick(i);
                if (isBinaryFrame(f) && f[2] != QwpConstants.STATUS_OK && readLeLong(f, 3) == 2) {
                    errorIndex = i;
                }
            }
            Assert.assertTrue("the frames before the failed one must be acked", ackIndex >= 0);
            Assert.assertTrue("the failed frame must be NACKed by its own sequence", errorIndex >= 0);
            Assert.assertTrue("the ack must precede the error", ackIndex < errorIndex);
            Assert.assertEquals("no ack may cover the failed frame or the tail behind it", 1, maxCumulativeOkAck(sent));
        });
    }

    /**
     * As {@link #testFailedFrameInCommitBatchKeepsEarlierFramesCommittedAndAcked},
     * with a two-block failing frame and a batch whose rows start past an earlier
     * commit in the same WAL segment. A two-block frame cannot join, so the batch
     * commits and is acked first; the frame then appends 300 and fails on its
     * second block, and its rollback must discard 300 and nothing the batch
     * committed.
     */
    @Test
    public void testFailedFrameAfterCommitInSegmentDropsOnlyItsOwnRows() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");
            final byte[] varcharMessage = oneRowVarcharMessage("not-a-long", 4_000_000L);
            final byte[] mismatchedBlock = Arrays.copyOfRange(varcharMessage, QwpConstants.HEADER_SIZE, varcharMessage.length);

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            // seq=0 arrives alone and commits: the segment's open txn now starts at
            // row 1. seq=1 and seq=2 arrive together; seq=1 opens the batch, seq=2
            // appends 300 and then fails on its second block.
            ObjList<byte[]> sent = ingestInTwoPasses(
                    processor,
                    httpConfig,
                    1,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, twoBlockMessage(
                            tableBlock("tab".getBytes(java.nio.charset.StandardCharsets.UTF_8), new long[]{300L, 3_000_000L}),
                            mismatchedBlock
                    ))
            );

            drainWalQueue();
            assertQuery("select v from tab").noLeakCheck().expectSize().returns("v\n100\n200\n");
            Assert.assertEquals("the batch must be acked through the frame before the failed one", 1, maxCumulativeOkAck(sent));
            Assert.assertTrue("the failed frame must be NACKed by its own sequence", hasErrorResponseForSeq(sent, 2));
            Assert.assertFalse("the batch's frame must not be NACKed", hasErrorResponseForSeq(sent, 1));
        });
    }

    /**
     * The multi-table shape of
     * {@link #testFailedFrameAfterCommitInSegmentDropsOnlyItsOwnRows}: the
     * pinned client's flush packs every table with rows into one message, so a
     * frame can append a good block to the batch's table and then fail on
     * another table's block. The frame cannot join, so the batch commits first,
     * and the frame's rollback must drop its good block from the batch's table
     * while the batch's rows stay committed.
     */
    @Test
    public void testFailedMultiTableFrameDropsItsBlockFromBatchTable() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table a (v long, ts timestamp) timestamp(ts) partition by day wal");
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");
            final byte[] varcharMessage = oneRowVarcharMessage("not-a-long", 4_000_000L);
            final byte[] mismatchedBlock = Arrays.copyOfRange(varcharMessage, QwpConstants.HEADER_SIZE, varcharMessage.length);

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            ObjList<byte[]> sent = ingestInTwoPasses(
                    processor,
                    httpConfig,
                    1,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage("a", 200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, twoBlockMessage(
                            tableBlock("a".getBytes(java.nio.charset.StandardCharsets.UTF_8), new long[]{300L, 3_000_000L}),
                            mismatchedBlock
                    ))
            );

            drainWalQueue();
            assertQuery("select v from a").noLeakCheck().expectSize().returns("v\n100\n200\n");
            assertQuery("select count() from tab").noLeakCheck().expectSize().noRandomAccess().returns("count\n0\n");
            Assert.assertEquals("the batch must be acked through the frame before the failed one", 1, maxCumulativeOkAck(sent));
            Assert.assertTrue("the failed frame must be NACKed by its own sequence", hasErrorResponseForSeq(sent, 2));
            Assert.assertFalse("the batch's frame must not be NACKed", hasErrorResponseForSeq(sent, 1));
        });
    }

    /**
     * The read-only gate refuses a frame with a batch pending, and the demote
     * that tripped the gate reverts before the recovery commit: the batch's
     * commit succeeds, so the batch must be acked before the role-change CLOSE
     * goes out. A close without that ack makes the client replay rows the
     * server holds, and a demote that reverts this fast is an enterprise
     * in-place role switch, which OSS cannot produce -- hence the scripted
     * engine below.
     */
    @Test
    public void testBatchCommittedAfterTransientGateRefusalIsAckedBeforeRoleChangeClose() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicInteger readOnlyChecks = new AtomicInteger();
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine demotableEngine = new CairoEngine(new DefaultTestCairoConfiguration(root)) {
                @Override
                public boolean isReadOnlyMode() {
                    // seq=0 checks at preparation and before its lookup (calls 1, 2)
                    // and opens the batch; seq=1 joins it, checked only at
                    // preparation (call 3). seq=2's preparation sees the demote
                    // (call 4); the batch commit's checks see it reverted, so the
                    // prefix commits.
                    return readOnlyChecks.incrementAndGet() == 4;
                }
            }) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(demotableEngine, httpConfig);
                final ObjList<byte[]> sent = new ObjList<>();
                try {
                    ingestInOnePass(
                            demotableEngine,
                            processor,
                            httpConfig,
                            AllowAllSecurityContext.INSTANCE,
                            false,
                            sent,
                            createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                            createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                            createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(300L, 3_000_000L))
                    );
                } catch (ServerDisconnectException ignore) {
                    // the role-change CLOSE tells the dispatcher to drop the fd
                }

                Assert.assertTrue("test setup: the batch commit must have re-checked the gate after the refusal",
                        readOnlyChecks.get() >= 5);
                drainWalQueue(demotableEngine);
                try (TableReader reader = demotableEngine.getReader("tab")) {
                    Assert.assertEquals("the batch commits once the demote reverts", 2, reader.size());
                }
                final int closeIndex = indexOfCloseFrame(sent);
                Assert.assertTrue("the gate refusal must still close the connection", closeIndex >= 0);
                Assert.assertFalse("a role-change refusal sends no error response", hasErrorResponseForSeq(sent, 2));
                final int ackIndex = indexOfBinaryFrame(sent, QwpConstants.STATUS_OK, 1);
                Assert.assertTrue("the committed batch must be acked, or the client replays it after the close", ackIndex >= 0);
                Assert.assertTrue("the ack must precede the CLOSE", ackIndex < closeIndex);
                Assert.assertEquals("no ack may cover the refused frame", 1, maxCumulativeOkAck(sent));
            }
        });
    }

    @Test
    public void testNothingFollowsRoleChangeCloseAtClientClose() throws Exception {
        assertNothingFollowsRoleChangeClose(clientCloseFrame());
    }

    @Test
    public void testNothingFollowsRoleChangeCloseAtContinuationFrame() throws Exception {
        assertNothingFollowsRoleChangeClose(createMaskedFrame(WebSocketOpcode.CONTINUATION, new byte[]{'x'}));
    }

    @Test
    public void testNothingFollowsRoleChangeCloseAtFragmentedFrame() throws Exception {
        assertNothingFollowsRoleChangeClose(createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(300L, 3_000_000L), false));
    }

    @Test
    public void testNothingFollowsRoleChangeCloseAtOversizedFrame() throws Exception {
        assertNothingFollowsRoleChangeClose(oversizedFrameHeader(new DefaultHttpServerConfiguration(configuration).getRecvBufferSize() + 1L));
    }

    @Test
    public void testNothingFollowsRoleChangeCloseAtPing() throws Exception {
        assertNothingFollowsRoleChangeClose(createMaskedFrame(WebSocketOpcode.PING, new byte[0]));
    }

    @Test
    public void testNothingFollowsRoleChangeCloseAtText() throws Exception {
        assertNothingFollowsRoleChangeClose(createMaskedFrame(WebSocketOpcode.TEXT, new byte[]{'x'}));
    }

    /**
     * A control frame, a fragment or an oversized frame behind a commit batch makes
     * the batch commit first. When a demote refuses that commit, the server sends its
     * role-change CLOSE and half-closes; without durable acks the connection then
     * drains instead of waiting for the echo. Nothing may follow that CLOSE: on a real
     * socket a write after the half-close fails, and the abortive teardown can discard
     * the client's unread final ack.
     */
    private void assertNothingFollowsRoleChangeClose(byte[] trailingFrame) throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");
            try (DemotableEngine demotableEngine = new DemotableEngine()) {
                demotableEngine.demoteAtNextCommit();
                try (Connection connection = new Connection(
                        demotableEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        false,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                        trailingFrame
                )) {
                    connection.deliverFrames(3);
                    Assert.assertTrue("test setup: the demote must land at the batch commit", demotableEngine.isReadOnlyMode());
                    assertOnlyRoleChangeCloseSent(connection.sentFrames());
                }
                assertTableEmpty(demotableEngine);
            }
        });
    }

    /**
     * On a durable-ack connection with no work awaiting upload, the server waits for
     * the client to echo its role-change CLOSE before tearing the connection down: the
     * echo proves the client read the frames before that CLOSE. A CLOSE the client
     * pipelined before the server's CLOSE reached it proves nothing of the kind, so it
     * must not complete the handshake, whether it directly follows the refused batch
     * or sits behind another frame.
     */
    @Test
    public void testClientClosePipelinedBeforeRoleChangeCloseIsNotTheEcho() throws Exception {
        assertPipelinedClientCloseIsNotTheEcho(clientCloseFrame());
    }

    @Test
    public void testClientClosePipelinedBehindPingBeforeRoleChangeCloseIsNotTheEcho() throws Exception {
        assertPipelinedClientCloseIsNotTheEcho(createMaskedFrame(WebSocketOpcode.PING, new byte[0]), clientCloseFrame());
    }

    private void assertPipelinedClientCloseIsNotTheEcho(byte[]... pipelinedFrames) throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");
            final ObjList<byte[]> frames = new ObjList<>();
            frames.add(createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)));
            frames.add(createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)));
            for (byte[] frame : pipelinedFrames) {
                frames.add(frame);
            }
            frames.add(clientCloseFrame()); // the echo, sent after the client read the server's CLOSE
            try (DemotableEngine demotableEngine = new DemotableEngine()) {
                demotableEngine.demoteAtNextCommit();
                try (Connection connection = new Connection(demotableEngine, httpConfig, AllowAllSecurityContext.INSTANCE, true, toArray(frames))) {
                    Assert.assertFalse(
                            "a CLOSE pipelined before the server's CLOSE must not complete the handshake",
                            connection.deliverFrames(frames.size() - 1)
                    );
                    Assert.assertTrue("test setup: the demote must land at the batch commit", demotableEngine.isReadOnlyMode());
                    assertOnlyRoleChangeCloseSent(connection.sentFrames());
                    Assert.assertTrue("the client's echo completes the handshake", connection.deliverFrames(1));
                    assertOnlyRoleChangeCloseSent(connection.sentFrames());
                }
                assertTableEmpty(demotableEngine);
            }
        });
    }

    /**
     * As {@link #testClientClosePipelinedBeforeRoleChangeCloseIsNotTheEcho}, but the
     * client's pipelined CLOSE straddles two reads: the batch commits at the end of the
     * first read with the head of that CLOSE buffered, and the demote refuses the
     * commit. Completing that CLOSE in the next read must not complete the handshake.
     */
    @Test
    public void testClientCloseStraddlingReadsBeforeRoleChangeCloseIsNotTheEcho() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");
            final byte[] close = clientCloseFrame();
            try (DemotableEngine demotableEngine = new DemotableEngine()) {
                demotableEngine.demoteAtNextCommit();
                try (Connection connection = new Connection(
                        demotableEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        true,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                        Arrays.copyOfRange(close, 0, close.length - 1),
                        Arrays.copyOfRange(close, close.length - 1, close.length)
                )) {
                    Assert.assertFalse(connection.deliverFrames(3));
                    Assert.assertTrue("test setup: the demote must land at the batch commit", demotableEngine.isReadOnlyMode());
                    assertOnlyRoleChangeCloseSent(connection.sentFrames());
                    Assert.assertFalse(
                            "a CLOSE begun before the server's CLOSE must not complete the handshake",
                            connection.deliverFrames(1)
                    );
                    assertOnlyRoleChangeCloseSent(connection.sentFrames());
                }
                assertTableEmpty(demotableEngine);
            }
        });
    }

    /**
     * An oversized frame behind the batch on a durable-ack connection: the demote
     * refuses the batch commit and the server sends its role-change CLOSE, not a
     * second one for the oversized frame. That frame's payload can never be
     * buffered, so the bytes the client streams as its payload must not be parsed as
     * frames; here they spell a CLOSE that would complete the handshake.
     */
    @Test
    public void testOversizedFramePayloadIsNotParsedAfterRoleChangeClose() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");
            try (DemotableEngine demotableEngine = new DemotableEngine()) {
                demotableEngine.demoteAtNextCommit();
                try (Connection connection = new Connection(
                        demotableEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        true,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                        oversizedFrameHeader(httpConfig.getRecvBufferSize() + 1L),
                        clientCloseFrame()
                )) {
                    Assert.assertFalse(connection.deliverFrames(3));
                    Assert.assertTrue("test setup: the demote must land at the batch commit", demotableEngine.isReadOnlyMode());
                    assertOnlyRoleChangeCloseSent(connection.sentFrames());
                    Assert.assertFalse(
                            "payload bytes of the oversized frame must not be parsed as a CLOSE",
                            connection.deliverFrames(1)
                    );
                    assertOnlyRoleChangeCloseSent(connection.sentFrames());
                }
                assertTableEmpty(demotableEngine);
            }
        });
    }

    /**
     * Control for the role-change tests above: a batch commit that fails for a reason
     * other than a demote sends a NACK, not a CLOSE, so the frame behind the batch is
     * still handled -- here a PING gets its PONG after the NACK.
     */
    @Test
    public void testPingBehindFailedBatchCommitIsAnsweredAfterTheNack() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");
            try (Connection connection = new Connection(
                    engine,
                    httpConfig,
                    denyCommitTo("tab"),
                    false,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.PING, new byte[0])
            )) {
                Assert.assertFalse(connection.deliverFrames(3));
                final ObjList<byte[]> sent = connection.sentFrames();
                Assert.assertEquals("the batch's NACK, then the PONG", 2, sent.size());
                Assert.assertEquals("the NACK names the batch's first frame", 0, indexOfErrorResponse(sent, 0));
                Assert.assertEquals("the PING must get its PONG", WebSocketOpcode.PONG, sent.getQuick(1)[0] & 0x0F);
            }
            drainWalQueue();
            assertQuery("select v from tab").noLeakCheck().expectSize().returns("v\n");
        });
    }

    /**
     * Same control for an oversized frame behind the batch: the batch's NACK goes out,
     * then the server closes for the oversized frame.
     */
    @Test
    public void testOversizedFrameBehindFailedBatchCommitIsClosedAfterTheNack() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");
            try (Connection connection = new Connection(
                    engine,
                    httpConfig,
                    denyCommitTo("tab"),
                    false,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                    oversizedFrameHeader(httpConfig.getRecvBufferSize() + 1L)
            )) {
                connection.deliverFrames(3);
                final ObjList<byte[]> sent = connection.sentFrames();
                Assert.assertEquals("the batch's NACK, then the CLOSE", 2, sent.size());
                Assert.assertEquals("the NACK names the batch's first frame", 0, indexOfErrorResponse(sent, 0));
                Assert.assertEquals("the oversized frame must still be closed", WebSocketOpcode.CLOSE, sent.getQuick(1)[0] & 0x0F);
                Assert.assertEquals(WebSocketCloseCode.MESSAGE_TOO_BIG, closeCode(sent.getQuick(1)));
            }
            drainWalQueue();
            assertQuery("select v from tab").noLeakCheck().expectSize().returns("v\n");
        });
    }

    /**
     * Control for the role-change tests above: a demote that refuses the batch commit
     * while earlier committed work still awaits durable upload defers the role-change
     * CLOSE. No CLOSE is on the wire, so the connection keeps serving the client until
     * the final durable ack can go out, and the PING behind the batch gets its PONG.
     */
    @Test
    public void testPingBehindDemoteRefusedBatchIsAnsweredWhileCloseIsDeferred() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");
            try (DemotableEngine demotableEngine = new DemotableEngine()) {
                try (Connection connection = new Connection(
                        demotableEngine,
                        httpConfig,
                        AllowAllSecurityContext.INSTANCE,
                        true,
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                        createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(300L, 3_000_000L)),
                        createMaskedFrame(WebSocketOpcode.PING, new byte[0])
                )) {
                    // The first frame commits on its own; its durable upload never catches up.
                    Assert.assertFalse(connection.deliverFrames(1));
                    demotableEngine.demoteAtNextCommit();
                    Assert.assertFalse(connection.deliverFrames(3));
                    Assert.assertTrue("test setup: the demote must land at the batch commit", demotableEngine.isReadOnlyMode());
                    final ObjList<byte[]> sent = connection.sentFrames();
                    Assert.assertEquals("a deferred close sends no CLOSE", -1, indexOfCloseFrame(sent));
                    Assert.assertEquals("the PING behind the refused batch must get its PONG", WebSocketOpcode.PONG, sent.getLast()[0] & 0x0F);
                    Assert.assertEquals("the refused batch must not be acked", 0, maxCumulativeOkAck(sent));
                }
                drainWalQueue(demotableEngine);
                try (TableReader reader = demotableEngine.getReader("tab")) {
                    Assert.assertEquals("only the first frame is committed", 1, reader.size());
                }
            }
        });
    }

    private static void assertOnlyRoleChangeCloseSent(ObjList<byte[]> sent) {
        Assert.assertEquals("the server's role-change CLOSE must be the only frame it sends", 1, sent.size());
        Assert.assertEquals("the frame must be a CLOSE", WebSocketOpcode.CLOSE, sent.getQuick(0)[0] & 0x0F);
        Assert.assertEquals("a role-change CLOSE is reconnect-eligible", WebSocketCloseCode.NORMAL_CLOSURE, closeCode(sent.getQuick(0)));
    }

    private static void assertTableEmpty(CairoEngine engine) {
        drainWalQueue(engine);
        try (TableReader reader = engine.getReader("tab")) {
            Assert.assertEquals("nothing of the refused batch may be committed", 0, reader.size());
        }
    }

    private static byte[] clientCloseFrame() {
        return createMaskedFrame(WebSocketOpcode.CLOSE, new byte[]{0x03, (byte) 0xE8});
    }

    private static int closeCode(byte[] serverCloseFrame) {
        // server frames are unmasked and CLOSE payloads are short: the code is at offset 2
        return (serverCloseFrame[2] & 0xFF) << 8 | (serverCloseFrame[3] & 0xFF);
    }

    private static int indexOfErrorResponse(ObjList<byte[]> frames, long seq) {
        for (int i = 0, n = frames.size(); i < n; i++) {
            byte[] f = frames.getQuick(i);
            if (isBinaryFrame(f) && f[2] != QwpConstants.STATUS_OK && readLeLong(f, 3) == seq) {
                return i;
            }
        }
        return -1;
    }

    private static byte[][] toArray(ObjList<byte[]> frames) {
        byte[][] array = new byte[frames.size()][];
        for (int i = 0, n = frames.size(); i < n; i++) {
            array[i] = frames.getQuick(i);
        }
        return array;
    }

    /**
     * The control for
     * {@link #testBatchCommittedAfterTransientGateRefusalIsAckedBeforeRoleChangeClose}:
     * the demote holds, so the recovery commit is refused too. Nothing is
     * committed, nothing is acked, and the connection closes for the role
     * change; the client replays the whole batch on a primary.
     */
    @Test
    public void testBatchRefusedAfterGateRefusalIsNotAckedBeforeRoleChangeClose() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicInteger readOnlyChecks = new AtomicInteger();
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine demotableEngine = new CairoEngine(new DefaultTestCairoConfiguration(root)) {
                @Override
                public boolean isReadOnlyMode() {
                    // the demote lands at seq=2's gate check (call 4) and stays
                    return readOnlyChecks.incrementAndGet() >= 4;
                }
            }) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(demotableEngine, httpConfig);
                final ObjList<byte[]> sent = new ObjList<>();
                try {
                    ingestInOnePass(
                            demotableEngine,
                            processor,
                            httpConfig,
                            AllowAllSecurityContext.INSTANCE,
                            false,
                            sent,
                            createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                            createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                            createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(300L, 3_000_000L))
                    );
                } catch (ServerDisconnectException ignore) {
                    // the role-change CLOSE tells the dispatcher to drop the fd
                }

                Assert.assertTrue("test setup: the batch commit must have re-checked the gate after the refusal",
                        readOnlyChecks.get() >= 5);
                drainWalQueue(demotableEngine);
                try (TableReader reader = demotableEngine.getReader("tab")) {
                    Assert.assertEquals("a refused recovery commit leaves nothing in the table", 0, reader.size());
                }
                Assert.assertTrue("the refusal must close the connection", indexOfCloseFrame(sent) >= 0);
                Assert.assertFalse("a role-change refusal sends no error response", hasErrorResponseForSeq(sent, 0));
                Assert.assertFalse("a role-change refusal sends no error response", hasErrorResponseForSeq(sent, 2));
                Assert.assertEquals("nothing committed, nothing acked", -1, maxCumulativeOkAck(sent));
            }
        });
    }

    /**
     * A deferred frame starts a client transaction: the commit batch before it
     * must commit on its own, never inside the client's group.
     */
    @Test
    public void testDeferredFrameCommitsPendingBatchFirst() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            ObjList<byte[]> sent = ingestInOnePass(
                    processor,
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L)),
                    createMaskedFrame(WebSocketOpcode.BINARY, deferred(oneRowMessage(300L, 3_000_000L)))
            );

            drainWalQueue();
            assertQuery("select v from tab").noLeakCheck().expectSize().returns("v\n100\n200\n");
            Assert.assertEquals("the open deferred frame must stay unacked", 1, maxCumulativeOkAck(sent));
        });
    }

    private static int countOkAcks(ObjList<byte[]> frames) {
        int count = 0;
        for (int i = 0, n = frames.size(); i < n; i++) {
            byte[] f = frames.getQuick(i);
            if (isBinaryFrame(f) && f[2] == QwpConstants.STATUS_OK) {
                count++;
            }
        }
        return count;
    }

    private static byte[] concat(byte[]... arrays) {
        int len = 0;
        for (byte[] a : arrays) {
            len += a.length;
        }
        byte[] out = new byte[len];
        int pos = 0;
        for (byte[] a : arrays) {
            System.arraycopy(a, 0, out, pos, a.length);
            pos += a.length;
        }
        return out;
    }

    private static byte[] createMaskedFrame(int opcode, byte[] payload) {
        return createMaskedFrame(opcode, payload, true);
    }

    private static byte[] createMaskedFrame(int opcode, byte[] payload, boolean isFin) {
        // all test frames are tiny; single-byte payload length is sufficient
        assert payload.length <= 125;
        byte[] frame = new byte[2 + 4 + payload.length];
        int offset = 0;
        frame[offset++] = (byte) ((isFin ? 0x80 : 0) | (opcode & 0x0F));
        frame[offset++] = (byte) (0x80 | payload.length);
        System.arraycopy(DEFAULT_MASK_KEY, 0, frame, offset, 4);
        offset += 4;
        for (int i = 0; i < payload.length; i++) {
            frame[offset + i] = (byte) (payload[i] ^ DEFAULT_MASK_KEY[i % 4]);
        }
        return frame;
    }

    /**
     * A cumulative OK ack must not cover a rowless deferred frame, even once a
     * LATER deferred frame's own rows have become durable.
     * <p>
     * This drives the real {@code handleBinaryMessage} and reads the acks off
     * the wire, so it covers the processor's wiring -- the state-level test can
     * only call the decision by hand. Deleting the processor's
     * {@code withholdDeferredFrame} call leaves every state-level test green.
     */
    @Test
    public void testCumulativeAckMustNotCoverRowlessDeferredFrame() throws Exception {
        assertMemoryLeak(() -> {
            // Cap 1 so the row-bearing deferred frame's rows are force-committed
            // during the append, making that frame ack-coverable on its own
            // merits. Without that the clamp would hold for the ordinary reason
            // and the test would say nothing about the rowless one.
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration) {
                @Override
                public LineHttpProcessorConfiguration getLineHttpProcessorConfiguration() {
                    final LineHttpProcessorConfiguration delegate = super.getLineHttpProcessorConfiguration();
                    return new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration) {
                        @Override
                        public long getQwpMaxUncommittedRows() {
                            return 1;
                        }
                    };
                }
            };

            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine engine2 = new CairoEngine(new DefaultTestCairoConfiguration(root))) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine2, httpConfig);

                byte[] chunk = createMaskedFrame(WebSocketOpcode.BINARY, rowlessDeferredMessage());
                byte[] data = createMaskedFrame(WebSocketOpcode.BINARY, deferred(oneRowMessage(100L, 1_000_000L)));
                byte[] commit = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L));
                byte[] wire = concat(chunk, data, commit);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = new QwpIngressProcessorState(
                            RECV_BUFFER_SIZE,
                            httpConfig.getSendBufferSize(),
                            engine2,
                            httpConfig.getLineHttpProcessorConfiguration()
                    );
                    state.of(-1, AllowAllSecurityContext.INSTANCE);
                    getLV().set(context, state);

                    // seq=0: rowless deferred -- withheld, and its sequence must
                    // stay uncovered until the group commits.
                    drive(processor, context, nf, chunk.length);
                    Assert.assertEquals("a rowless deferred frame must not be acked",
                            -1, maxCumulativeOkAck(rawSocket.sentFrames));

                    // seq=1: deferred, but the cap force-commits its row, so it
                    // is ack-coverable on its own. Covering it would also cover
                    // seq=0, which the client needs retained.
                    drive(processor, context, nf, data.length);
                    Assert.assertEquals(
                            "a cumulative ack must not reach the rowless frame at seq=0, even though"
                                    + " seq=1's own rows are durable -- the client trims on the cumulative"
                                    + " ack and its data frames reference what seq=0 registered",
                            -1,
                            maxCumulativeOkAck(rawSocket.sentFrames)
                    );

                    // seq=2: the group-closing commit covers everything.
                    drive(processor, context, nf, commit.length);
                    Assert.assertEquals("the group-closing commit must cover the whole group",
                            2, maxCumulativeOkAck(rawSocket.sentFrames));
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testDurableAckPollAdvancesAckOnCleanConnection() throws Exception {
        // Counterpart to the deferred-group case below. With nothing deferred,
        // the poll consumes its own message sequence and the cumulative OK ack
        // must name it. Without that advance a client's acked watermark falls
        // one behind the sequence it issued for every keepalive poll it sends,
        // and its store-and-forward records never retire.
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            byte[] first = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L));
            byte[] poll = createMaskedFrame(WebSocketOpcode.BINARY, QwpWireTestFixtures.durableAckPollMessage());
            byte[] second = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L));
            byte[] wire = concat(first, poll, second);

            PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                QwpIngressProcessorState state = new QwpIngressProcessorState(
                        RECV_BUFFER_SIZE,
                        httpConfig.getSendBufferSize(),
                        engine,
                        httpConfig.getLineHttpProcessorConfiguration()
                );
                state.of(-1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                getLV().set(context, state);

                drive(processor, context, nf, first.length);
                Assert.assertEquals("the first data frame must be acknowledged", 0,
                        maxCumulativeOkAck(rawSocket.sentFrames));

                drive(processor, context, nf, poll.length);
                Assert.assertEquals(
                        "a poll on a clean connection must advance the cumulative ack over its own sequence",
                        1,
                        maxCumulativeOkAck(rawSocket.sentFrames)
                );

                drive(processor, context, nf, second.length);
                Assert.assertEquals("the trailing data frame must be acknowledged", 2,
                        maxCumulativeOkAck(rawSocket.sentFrames));
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }

            drainWalQueue();
            try (TableReader reader = engine.getReader("tab")) {
                Assert.assertEquals("the poll must neither add nor drop rows", 2, reader.size());
            }
        });
    }

    /**
     * The poll exists so a browser that cannot send WebSocket PINGs can still
     * pull durable progress. The registry is armed only after the data frame's
     * own flush has gone out, so the poll is the sole flush point able to carry
     * the new watermark.
     */
    @Test
    public void testDurableAckPollFlushesDurableProgress() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicLong durableWatermark = new AtomicLong(-1L);
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");

            try (CairoEngine durableEngine = new CairoEngine(new DefaultTestCairoConfiguration(root)) {
                private final DurableAckRegistry uploadingRegistry = new DurableAckRegistry() {
                    @Override
                    public long getDurablyUploadedSeqTxn(CharSequence tableDirName) {
                        return durableWatermark.get();
                    }

                    @Override
                    public boolean isEnabled() {
                        return true;
                    }
                };

                @Override
                public @NotNull DurableAckRegistry getDurableAckRegistry() {
                    return uploadingRegistry;
                }
            }) {
                QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(durableEngine, httpConfig);

                byte[] data = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L));
                byte[] poll = createMaskedFrame(WebSocketOpcode.BINARY, QwpWireTestFixtures.durableAckPollMessage());
                byte[] wire = concat(data, poll);

                PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                    QwpIngressProcessorState state = new QwpIngressProcessorState(
                            RECV_BUFFER_SIZE,
                            httpConfig.getSendBufferSize(),
                            durableEngine,
                            httpConfig.getLineHttpProcessorConfiguration()
                    );
                    state.of(-1, AllowAllSecurityContext.INSTANCE);
                    state.setDurableAckEnabled(true);
                    getLV().set(context, state);

                    // Uploads still lag: the commit is acknowledged, but no
                    // durable watermark exists to report.
                    drive(processor, context, nf, data.length);
                    Assert.assertEquals("the data frame must be acknowledged", 0,
                            maxCumulativeOkAck(rawSocket.sentFrames));
                    Assert.assertFalse(
                            "nothing is durably uploaded yet, so no durable ACK may be sent",
                            hasDurableAckFrame(rawSocket.sentFrames)
                    );

                    // The upload completes between the two frames.
                    durableWatermark.set(1L);
                    drive(processor, context, nf, poll.length);
                    Assert.assertTrue(
                            "a durable ACK poll must flush the durable watermark that landed since the last frame",
                            hasDurableAckFrame(rawSocket.sentFrames)
                    );
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }

                drainWalQueue(durableEngine);
                try (TableReader reader = durableEngine.getReader("tab")) {
                    Assert.assertEquals("the poll must neither add nor drop rows", 1, reader.size());
                }
            }
        });
    }

    @Test
    public void testDurableAckPollMustNotCommitDeferredGroup() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            byte[] deferred = createMaskedFrame(
                    WebSocketOpcode.BINARY,
                    deferred(oneRowMessage(100L, 1_000_000L))
            );
            byte[] poll = createMaskedFrame(WebSocketOpcode.BINARY, QwpWireTestFixtures.durableAckPollMessage());
            byte[] commit = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L));
            byte[] wire = concat(deferred, poll, commit);

            PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                // The observable ack is identical whether the poll arm's
                // hasUncommittedDeferredRows() guard withholds the advance or
                // the last-resort clamp inside setHighestProcessedSequence
                // refuses it: both leave the watermark untouched, and the clamp
                // only differs by a LOG.critical() line. Count the calls so the
                // guard itself is what this test holds -- the clamp is
                // documented as containment for a regression of exactly this
                // path, so a green test that leans on it proves nothing.
                AtomicLong watermarkAdvanceAttempts = new AtomicLong();
                QwpIngressProcessorState state = new QwpIngressProcessorState(
                        RECV_BUFFER_SIZE,
                        httpConfig.getSendBufferSize(),
                        engine,
                        httpConfig.getLineHttpProcessorConfiguration()
                ) {
                    @Override
                    public void setHighestProcessedSequence(long highestProcessedSequence) {
                        watermarkAdvanceAttempts.incrementAndGet();
                        super.setHighestProcessedSequence(highestProcessedSequence);
                    }
                };
                state.of(-1, AllowAllSecurityContext.INSTANCE);
                state.setDurableAckEnabled(true);
                getLV().set(context, state);

                drive(processor, context, nf, deferred.length);
                Assert.assertEquals("deferred rows must remain unacknowledged", -1, maxCumulativeOkAck(rawSocket.sentFrames));

                long attemptsBeforePoll = watermarkAdvanceAttempts.get();
                drive(processor, context, nf, poll.length);
                Assert.assertEquals(
                        "a durable ACK poll must not even ask the watermark to advance while a deferred group is open",
                        attemptsBeforePoll,
                        watermarkAdvanceAttempts.get()
                );
                Assert.assertEquals(
                        "a durable ACK poll must not commit or acknowledge an in-progress deferred group",
                        -1,
                        maxCumulativeOkAck(rawSocket.sentFrames)
                );

                drive(processor, context, nf, commit.length);
                Assert.assertEquals("the real commit frame must cover the deferred group and poll", 2,
                        maxCumulativeOkAck(rawSocket.sentFrames));
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }

            drainWalQueue();
            try (TableReader reader = engine.getReader("tab")) {
                Assert.assertEquals("both real data frames must commit exactly once", 2, reader.size());
            }
        });
    }

    /**
     * The reject arm must also clamp the pipelined tail. A refused poll is
     * consumed without an ack of its own, so {@code markSequenceUnresolved}
     * has to run before the flush: without it a frame pipelined behind the
     * refused poll commits and the cumulative OK ack jumps over the refused
     * sequence. A store-and-forward sender that treats STATUS_PARSE_ERROR as
     * terminal tears down before reading that ack, replays from its old
     * watermark, and duplicates the tail frame's rows.
     */
    @Test
    public void testDurableAckPollRejectClampsPipelinedTail() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            byte[] data = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L));
            byte[] poll = createMaskedFrame(WebSocketOpcode.BINARY, QwpWireTestFixtures.durableAckPollMessage());
            byte[] tail = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L));
            byte[] wire = concat(data, poll, tail);

            PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                QwpIngressProcessorState state = new QwpIngressProcessorState(
                        RECV_BUFFER_SIZE,
                        httpConfig.getSendBufferSize(),
                        engine,
                        httpConfig.getLineHttpProcessorConfiguration()
                );
                state.of(-1, AllowAllSecurityContext.INSTANCE);
                // Durable ack deliberately left off: this is the reject arm.
                getLV().set(context, state);

                // All three frames in one pass, so the tail is already buffered
                // when the poll at seq=1 is refused.
                drive(processor, context, nf, wire.length);

                Assert.assertTrue(
                        "the refused poll must receive STATUS_PARSE_ERROR",
                        indexOfBinaryFrame(rawSocket.sentFrames, QwpConstants.STATUS_PARSE_ERROR, 1) >= 0
                );
                Assert.assertEquals(
                        "the cumulative ack must stop at the frame before the refused poll",
                        0,
                        maxCumulativeOkAck(rawSocket.sentFrames)
                );
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }

            drainWalQueue();
            try (TableReader reader = engine.getReader("tab")) {
                Assert.assertEquals(
                        "the frame pipelined behind the refused poll must not commit",
                        1,
                        reader.size()
                );
            }
        });
    }

    /**
     * The reject arm must flush the pending cumulative ack before the error,
     * exactly like the error arm at the tail of {@code handleBinaryMessage}.
     * Both frames go out either way, but a store-and-forward sender that treats
     * the error as terminal tears the connection down on reading it; anything
     * still behind the error on the wire is never read, so those frames stay in
     * its replay queue and reconnect duplicates their rows.
     */
    @Test
    public void testDurableAckPollRejectFlushesPendingAckFirst() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            execute("create table tab (v long, ts timestamp) timestamp(ts) partition by day wal");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            byte[] data = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L));
            byte[] poll = createMaskedFrame(WebSocketOpcode.BINARY, QwpWireTestFixtures.durableAckPollMessage());
            byte[] wire = concat(data, poll);

            PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                QwpIngressProcessorState state = new QwpIngressProcessorState(
                        RECV_BUFFER_SIZE,
                        httpConfig.getSendBufferSize(),
                        engine,
                        httpConfig.getLineHttpProcessorConfiguration()
                );
                state.of(-1, AllowAllSecurityContext.INSTANCE);
                // Durable ack deliberately left off: this is the reject arm.
                getLV().set(context, state);

                // Both frames in one pass, so the data frame's ack is still
                // pending (fewer frames than ACK_BATCH_SIZE) when the poll is refused.
                drive(processor, context, nf, wire.length);

                int ackIndex = indexOfBinaryFrame(rawSocket.sentFrames, QwpConstants.STATUS_OK, 0);
                int errorIndex = indexOfBinaryFrame(rawSocket.sentFrames, QwpConstants.STATUS_PARSE_ERROR, 1);
                Assert.assertTrue("the committed data frame must be acknowledged", ackIndex >= 0);
                Assert.assertTrue("the refused poll must receive STATUS_PARSE_ERROR", errorIndex >= 0);
                Assert.assertTrue(
                        "the cumulative ack must precede the error on the wire, got ack at "
                                + ackIndex + " and error at " + errorIndex,
                        ackIndex < errorIndex
                );
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }

            drainWalQueue();
            try (TableReader reader = engine.getReader("tab")) {
                Assert.assertEquals("the refused poll must neither add nor drop rows", 1, reader.size());
            }
        });
    }

    @Test
    public void testDurableAckPollRequiresNegotiation() throws Exception {
        assertMemoryLeak(() -> {
            final HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            ObjList<byte[]> sent = ingestOnFreshConnection(
                    processor,
                    httpConfig,
                    createMaskedFrame(WebSocketOpcode.BINARY, QwpWireTestFixtures.durableAckPollMessage())
            );

            Assert.assertTrue(
                    "an unnegotiated durable ACK poll must receive STATUS_PARSE_ERROR",
                    indexOfBinaryFrame(sent, QwpConstants.STATUS_PARSE_ERROR, 0) >= 0
            );
        });
    }

    private static void drive(
            QwpIngressUpgradeProcessor processor,
            HttpConnectionContext context,
            PhasedNetworkFacade nf,
            int bytes
    ) throws Exception {
        nf.release(bytes);
        try {
            processor.resumeRecv(context);
        } catch (PeerIsSlowToWriteException e) {
            // all released bytes consumed; the dispatcher would re-arm for read
        }
    }

    @SuppressWarnings("unchecked")
    private static LocalValue<QwpIngressProcessorState> getLV() throws Exception {
        Field lvField = QwpIngressUpgradeProcessor.class.getDeclaredField("LV");
        lvField.setAccessible(true);
        return (LocalValue<QwpIngressProcessorState>) lvField.get(null);
    }

    /**
     * True if any server-to-client BINARY frame is a STATUS_DURABLE_ACK frame.
     * Durable acks carry per-table watermarks rather than a sequence, so there
     * is nothing to match on beyond the status byte.
     */
    private static boolean hasDurableAckFrame(ObjList<byte[]> frames) {
        for (int i = 0, n = frames.size(); i < n; i++) {
            byte[] f = frames.getQuick(i);
            if (isBinaryFrame(f) && f[2] == QwpConstants.STATUS_DURABLE_ACK) {
                return true;
            }
        }
        return false;
    }

    /**
     * True if any server-to-client BINARY frame is an error response
     * (status != OK) carrying the given sequence. Error frames share the
     * ACK's [status][seq LE] payload prefix.
     */
    private static boolean hasErrorResponseForSeq(ObjList<byte[]> frames, long seq) {
        for (int i = 0, n = frames.size(); i < n; i++) {
            byte[] f = frames.getQuick(i);
            if (!isBinaryFrame(f)) {
                continue;
            }
            if (f[2] != QwpConstants.STATUS_OK && readLeLong(f, 3) == seq) {
                return true;
            }
        }
        return false;
    }

    /**
     * Position of the first server-to-client BINARY frame carrying the given
     * status and sequence, or -1 when none was sent. Order matters where an ack
     * and an error leave in the same pass.
     */
    private static int indexOfCloseFrame(ObjList<byte[]> frames) {
        for (int i = 0, n = frames.size(); i < n; i++) {
            byte[] f = frames.getQuick(i);
            if (f.length >= 2 && (f[0] & 0x0F) == WebSocketOpcode.CLOSE) {
                return i;
            }
        }
        return -1;
    }

    private static int indexOfBinaryFrame(ObjList<byte[]> frames, byte status, long seq) {
        for (int i = 0, n = frames.size(); i < n; i++) {
            byte[] f = frames.getQuick(i);
            if (isBinaryFrame(f) && f[2] == status && readLeLong(f, 3) == seq) {
                return i;
            }
        }
        return -1;
    }

    private static boolean isBinaryFrame(byte[] frame) {
        // server frames are unmasked; all frames in this test have single-byte
        // payload lengths, so the payload starts at offset 2
        return frame.length >= 11
                && (frame[0] & 0x0F) == WebSocketOpcode.BINARY
                && (frame[1] & 0x80) == 0
                && (frame[1] & 0x7F) < 126;
    }

    /**
     * Highest sequence carried by any cumulative OK-ACK frame sent to the
     * client, or -1 when none was sent.
     */
    private static long maxCumulativeOkAck(ObjList<byte[]> frames) {
        long max = -1;
        for (int i = 0, n = frames.size(); i < n; i++) {
            byte[] f = frames.getQuick(i);
            if (!isBinaryFrame(f)) {
                continue;
            }
            if (f[2] == QwpConstants.STATUS_OK) {
                max = Math.max(max, readLeLong(f, 3));
            }
        }
        return max;
    }

    /**
     * QWP v1 message: table "tab", one row, schema
     * [{@code v} LONG, {@code ""} TIMESTAMP (designated)].
     */
    /// A frame carrying no table blocks at all, with FLAG_DEFER_COMMIT. The
    /// client emits exactly this shape for its symbol-dictionary chunks and its
    /// reconnect catch-up, and the data frames after them reference the ids
    /// those frames register.
    private static byte[] rowlessDeferredMessage() {
        byte[] payload = new byte[2];
        payload[0] = 0; // delta_start (varint)
        payload[1] = 0; // new_symbols count (varint)

        byte[] message = new byte[QwpConstants.HEADER_SIZE + payload.length];
        message[0] = 'Q';
        message[1] = 'W';
        message[2] = 'P';
        message[3] = '1';
        message[4] = QwpConstants.VERSION;
        message[5] = QwpConstants.FLAG_DEFER_COMMIT;
        message[6] = 0; // tableCount lo
        message[7] = 0; // tableCount hi
        message[8] = (byte) payload.length;
        System.arraycopy(payload, 0, message, QwpConstants.HEADER_SIZE, payload.length);
        return message;
    }

    private static byte[] deferred(byte[] message) {
        byte[] copy = message.clone();
        copy[5] |= QwpConstants.FLAG_DEFER_COMMIT;
        return copy;
    }

    private static byte[] oneRowMessage(long value, long tsMicros) {
        byte[] payload = new byte[29];
        int i = 0;
        // table header
        payload[i++] = 3; // table name length (varint)
        payload[i++] = 't';
        payload[i++] = 'a';
        payload[i++] = 'b';
        payload[i++] = 1; // rowCount (varint)
        payload[i++] = 2; // columnCount (varint)
        // schema
        payload[i++] = 1; // column name length (varint)
        payload[i++] = 'v';
        payload[i++] = QwpConstants.TYPE_LONG;
        payload[i++] = 0; // empty name = designated timestamp
        payload[i++] = QwpConstants.TYPE_TIMESTAMP;
        // column data: v
        payload[i++] = 0; // no null bitmap
        i = writeLeLong(payload, i, value);
        // column data: designated timestamp
        payload[i++] = 0; // no null bitmap
        writeLeLong(payload, i, tsMicros);

        byte[] message = new byte[QwpConstants.HEADER_SIZE + payload.length];
        message[0] = 'Q';
        message[1] = 'W';
        message[2] = 'P';
        message[3] = '1';
        message[4] = QwpConstants.VERSION;
        message[5] = 0; // flags
        message[6] = 1; // tableCount lo
        message[7] = 0; // tableCount hi
        message[8] = (byte) payload.length;
        message[9] = 0;
        message[10] = 0;
        message[11] = 0;
        System.arraycopy(payload, 0, message, QwpConstants.HEADER_SIZE, payload.length);
        return message;
    }

    /**
     * Like {@link #oneRowMessage} but declares {@code v} as VARCHAR carrying a
     * non-numeric string, which the LONG column rejects as SCHEMA_MISMATCH.
     */
    private static byte[] oneRowVarcharMessage(String value, long tsMicros) {
        byte[] utf8 = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] payload = new byte[11 + 1 + 8 + utf8.length + 1 + 8];
        int i = 0;
        payload[i++] = 3;
        payload[i++] = 't';
        payload[i++] = 'a';
        payload[i++] = 'b';
        payload[i++] = 1; // rowCount
        payload[i++] = 2; // columnCount
        payload[i++] = 1; // column name length
        payload[i++] = 'v';
        payload[i++] = QwpConstants.TYPE_VARCHAR;
        payload[i++] = 0; // empty name = designated timestamp
        payload[i++] = QwpConstants.TYPE_TIMESTAMP;
        // column data: v -- [no-null-bitmap flag][offset array][utf8 bytes]
        payload[i++] = 0;
        i = writeLeInt(payload, i, 0);
        i = writeLeInt(payload, i, utf8.length);
        System.arraycopy(utf8, 0, payload, i, utf8.length);
        i += utf8.length;
        // column data: designated timestamp
        payload[i++] = 0;
        writeLeLong(payload, i, tsMicros);

        byte[] message = new byte[QwpConstants.HEADER_SIZE + payload.length];
        message[0] = 'Q';
        message[1] = 'W';
        message[2] = 'P';
        message[3] = '1';
        message[4] = QwpConstants.VERSION;
        message[5] = 0;
        message[6] = 1;
        message[7] = 0;
        message[8] = (byte) payload.length;
        message[9] = 0;
        message[10] = 0;
        message[11] = 0;
        System.arraycopy(payload, 0, message, QwpConstants.HEADER_SIZE, payload.length);
        return message;
    }

    /**
     * The seqTxn that the cumulative OK-ACK for {@code seq} names for
     * {@code table}, or -1 when there is no such ack or it has no entry for the
     * table. After [status][seq LE] an OK-ACK carries [count u16 LE], then per
     * table [name length u16 LE][name UTF-8][seqTxn i64 LE].
     */
    private static long okAckSeqTxn(ObjList<byte[]> frames, long seq, String table) {
        final byte[] name = table.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        for (int i = 0, n = frames.size(); i < n; i++) {
            byte[] f = frames.getQuick(i);
            if (!isBinaryFrame(f) || f[2] != QwpConstants.STATUS_OK || readLeLong(f, 3) != seq) {
                continue;
            }
            int pos = 13;
            for (int entry = 0, count = readLeShort(f, 11); entry < count; entry++) {
                int nameLen = readLeShort(f, pos);
                pos += 2;
                boolean isMatch = Arrays.equals(f, pos, pos + nameLen, name, 0, name.length);
                pos += nameLen;
                if (isMatch) {
                    return readLeLong(f, pos);
                }
                pos += 8;
            }
        }
        return -1;
    }

    private static int readLeShort(byte[] buf, int offset) {
        return (buf[offset] & 0xFF) | (buf[offset + 1] & 0xFF) << 8;
    }

    private static long readLeLong(byte[] buf, int offset) {
        long v = 0;
        for (int i = 7; i >= 0; i--) {
            v = (v << 8) | (buf[offset + i] & 0xFFL);
        }
        return v;
    }

    private static int writeLeInt(byte[] buf, int offset, int value) {
        for (int i = 0; i < 4; i++) {
            buf[offset++] = (byte) (value >>> (i * 8));
        }
        return offset;
    }

    private static int writeLeLong(byte[] buf, int offset, long value) {
        for (int i = 0; i < 8; i++) {
            buf[offset++] = (byte) (value >>> (i * 8));
        }
        return offset;
    }

    private ObjList<byte[]> ingestInOnePass(
            QwpIngressUpgradeProcessor processor,
            HttpFullFatServerConfiguration httpConfig,
            byte[]... frames
    ) throws Exception {
        return ingestInOnePass(processor, httpConfig, AllowAllSecurityContext.INSTANCE, frames);
    }

    private ObjList<byte[]> ingestInOnePass(
            QwpIngressUpgradeProcessor processor,
            HttpFullFatServerConfiguration httpConfig,
            SecurityContext securityContext,
            byte[]... frames
    ) throws Exception {
        return ingestInOnePass(engine, processor, httpConfig, securityContext, frames);
    }

    /**
     * Like {@link #ingestInOnePass(QwpIngressUpgradeProcessor, HttpFullFatServerConfiguration, byte[]...)}
     * but the connection state is bound to {@code stateEngine}, whose
     * {@code isReadOnlyMode()} the gate and the commit path consult.
     */
    private ObjList<byte[]> ingestInOnePass(
            CairoEngine stateEngine,
            QwpIngressUpgradeProcessor processor,
            HttpFullFatServerConfiguration httpConfig,
            byte[]... frames
    ) throws Exception {
        return ingestInOnePass(stateEngine, processor, httpConfig, AllowAllSecurityContext.INSTANCE, frames);
    }

    private ObjList<byte[]> ingestInOnePass(
            CairoEngine stateEngine,
            QwpIngressUpgradeProcessor processor,
            HttpFullFatServerConfiguration httpConfig,
            SecurityContext securityContext,
            byte[]... frames
    ) throws Exception {
        final ObjList<byte[]> sent = new ObjList<>();
        ingestInOnePass(stateEngine, processor, httpConfig, securityContext, false, sent, frames);
        return sent;
    }

    private ObjList<byte[]> ingestDurableInOnePass(
            QwpIngressUpgradeProcessor processor,
            HttpFullFatServerConfiguration httpConfig,
            SecurityContext securityContext,
            byte[]... frames
    ) throws Exception {
        final ObjList<byte[]> sent = new ObjList<>();
        ingestInOnePass(engine, processor, httpConfig, securityContext, true, sent, frames);
        return sent;
    }

    private void ingestInOnePass(
            CairoEngine stateEngine,
            QwpIngressUpgradeProcessor processor,
            HttpFullFatServerConfiguration httpConfig,
            SecurityContext securityContext,
            boolean isDurableAckEnabled,
            ObjList<byte[]> sent,
            byte[]... frames
    ) throws Exception {
        byte[] wire = concat(frames);
        assert wire.length <= RECV_BUFFER_SIZE;
        PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
        long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
        try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
            QwpIngressProcessorState state = new QwpIngressProcessorState(
                    RECV_BUFFER_SIZE,
                    httpConfig.getSendBufferSize(),
                    stateEngine,
                    httpConfig.getLineHttpProcessorConfiguration()
            );
            state.of(-1, securityContext);
            state.setDurableAckEnabled(isDurableAckEnabled);
            getLV().set(context, state);
            // Every frame arrives in one receive pass, as from a client with many in flight.
            drive(processor, context, nf, wire.length);
        } finally {
            sent.addAll(rawSocket.sentFrames);
            Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Like {@link #ingestInOnePass} but the first {@code firstPassFrameCount}
     * frames arrive in one receive pass and the rest in a second one on the same
     * connection, so the first pass's commit batch is committed -- and its rows
     * sit committed in the cached writers' segments -- before the second pass's
     * batch forms.
     */
    private ObjList<byte[]> ingestInTwoPasses(
            QwpIngressUpgradeProcessor processor,
            HttpFullFatServerConfiguration httpConfig,
            int firstPassFrameCount,
            byte[]... frames
    ) throws Exception {
        byte[] wire = concat(frames);
        assert wire.length <= RECV_BUFFER_SIZE;
        int firstPassBytes = 0;
        for (int i = 0; i < firstPassFrameCount; i++) {
            firstPassBytes += frames[i].length;
        }
        PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
        long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
        try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
            QwpIngressProcessorState state = new QwpIngressProcessorState(
                    RECV_BUFFER_SIZE,
                    httpConfig.getSendBufferSize(),
                    engine,
                    httpConfig.getLineHttpProcessorConfiguration()
            );
            state.of(-1, AllowAllSecurityContext.INSTANCE);
            getLV().set(context, state);
            drive(processor, context, nf, firstPassBytes);
            drive(processor, context, nf, wire.length - firstPassBytes);
        } finally {
            Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
        return rawSocket.sentFrames;
    }

    private ObjList<byte[]> ingestOnFreshConnection(
            QwpIngressUpgradeProcessor processor,
            HttpFullFatServerConfiguration httpConfig,
            byte[]... frames
    ) throws Exception {
        byte[] wire = concat(frames);
        PhasedNetworkFacade nf = new PhasedNetworkFacade(wire);
        long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
        try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
            QwpIngressProcessorState state = new QwpIngressProcessorState(
                    RECV_BUFFER_SIZE,
                    httpConfig.getSendBufferSize(),
                    engine,
                    httpConfig.getLineHttpProcessorConfiguration()
            );
            state.of(-1, AllowAllSecurityContext.INSTANCE);
            getLV().set(context, state);
            for (byte[] frame : frames) {
                drive(processor, context, nf, frame.length);
            }
        } finally {
            Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
        return rawSocket.sentFrames;
    }

    /**
     * One server connection fed a scripted client byte stream, frame by frame, over a
     * socket whose half-close succeeds, as a real socket's does: a server CLOSE leaves
     * the connection draining or awaiting the client's echo instead of disconnecting at
     * once.
     */
    private static final class Connection implements QuietCloseable {
        private final TestableContext context;
        private final byte[][] frames;
        private final HalfClosingNetworkFacade nf;
        private final QwpIngressUpgradeProcessor processor;
        private final RecordingRawSocket rawSocket;
        private final long recvBuf;
        private final long sendBuf;
        private int nextFrame;

        Connection(
                CairoEngine engine,
                HttpFullFatServerConfiguration httpConfig,
                SecurityContext securityContext,
                boolean isDurableAckEnabled,
                byte[]... frames
        ) throws Exception {
            this.frames = frames;
            this.processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            this.nf = new HalfClosingNetworkFacade(concat(frames));
            this.recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            this.sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            this.rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
            this.context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE);
            QwpIngressProcessorState state = new QwpIngressProcessorState(
                    RECV_BUFFER_SIZE,
                    httpConfig.getSendBufferSize(),
                    engine,
                    httpConfig.getLineHttpProcessorConfiguration()
            );
            state.of(-1, securityContext);
            state.setDurableAckEnabled(isDurableAckEnabled);
            getLV().set(context, state);
        }

        @Override
        public void close() {
            context.close();
            Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        }

        /**
         * Delivers the client's next {@code count} frames in one receive pass. Returns
         * true when the server tore the connection down.
         */
        boolean deliverFrames(int count) throws Exception {
            int bytes = 0;
            for (int i = 0; i < count; i++) {
                bytes += frames[nextFrame++].length;
            }
            try {
                drive(processor, context, nf, bytes);
                return false;
            } catch (ServerDisconnectException e) {
                return true;
            }
        }

        ObjList<byte[]> sentFrames() {
            return rawSocket.sentFrames;
        }
    }

    /**
     * An engine the test demotes to read-only. {@link #demoteAtNextCommit()} arms the
     * demote, and it lands when the next commit takes the role-switch read lock. That
     * commit's in-lock read-only re-check then refuses it -- the race
     * {@code TableUpdateDetails.commit} guards against -- while every frame prepared
     * before it passed the live read-only gate. Durable uploads never catch up, so
     * committed work stays pending a durable ack.
     */
    private static final class DemotableEngine extends CairoEngine {
        private final AtomicBoolean isDemoteArmed = new AtomicBoolean();
        private final AtomicBoolean isReadOnly = new AtomicBoolean();
        private final DurableAckRegistry laggingRegistry = new DurableAckRegistry() {
            @Override
            public long getDurablyUploadedSeqTxn(CharSequence tableDirName) {
                return -1L;
            }

            @Override
            public boolean isEnabled() {
                return true;
            }
        };
        private Lock roleSwitchReadLock;

        DemotableEngine() {
            super(new DefaultTestCairoConfiguration(root));
        }

        @Override
        public @NotNull DurableAckRegistry getDurableAckRegistry() {
            return laggingRegistry;
        }

        @Override
        public Lock getRoleSwitchReadLock() {
            if (roleSwitchReadLock == null) {
                roleSwitchReadLock = new DemotingLock(super.getRoleSwitchReadLock());
            }
            return roleSwitchReadLock;
        }

        @Override
        public boolean isReadOnlyMode() {
            return isReadOnly.get();
        }

        void demoteAtNextCommit() {
            isDemoteArmed.set(true);
        }

        private final class DemotingLock implements Lock {
            private final Lock delegate;

            DemotingLock(Lock delegate) {
                this.delegate = delegate;
            }

            @Override
            public void lock() {
                delegate.lock();
                onAcquired();
            }

            @Override
            public void lockInterruptibly() throws InterruptedException {
                delegate.lockInterruptibly();
                onAcquired();
            }

            @Override
            public @NotNull Condition newCondition() {
                return delegate.newCondition();
            }

            @Override
            public boolean tryLock() {
                if (delegate.tryLock()) {
                    onAcquired();
                    return true;
                }
                return false;
            }

            @Override
            public boolean tryLock(long time, @NotNull TimeUnit unit) throws InterruptedException {
                if (delegate.tryLock(time, unit)) {
                    onAcquired();
                    return true;
                }
                return false;
            }

            @Override
            public void unlock() {
                delegate.unlock();
            }

            private void onAcquired() {
                if (isDemoteArmed.compareAndSet(true, false)) {
                    isReadOnly.set(true);
                }
            }
        }
    }

    /**
     * Network facade that releases the client's wire bytes in explicit phases
     * so the engine's read-only flag can be flipped between frames — the
     * demote/re-promote race distilled to its deterministic core.
     */
    private static class PhasedNetworkFacade extends NetworkFacadeImpl {
        private final byte[] data;
        private int limit;
        private int pos;

        PhasedNetworkFacade(byte[] data) {
            this.data = data;
        }

        @Override
        public void close(long fd, Log log) {
            // no-op for test
        }

        @Override
        public int recvRaw(long fd, long buffer, int bufferLen) {
            if (pos >= limit) {
                return 0; // would block
            }
            int n = Math.min(bufferLen, limit - pos);
            for (int i = 0; i < n; i++) {
                Unsafe.putByte(buffer + i, data[pos++]);
            }
            return n;
        }

        void release(int bytes) {
            limit = Math.min(data.length, limit + bytes);
        }
    }

    /**
     * A facade whose half-close succeeds, as a real socket's does, so a fatal CLOSE
     * leaves the connection draining instead of disconnecting at once.
     */
    private static class HalfClosingNetworkFacade extends PhasedNetworkFacade {
        HalfClosingNetworkFacade(byte[] data) {
            super(data);
        }

        @Override
        public int shutdown(long fd, int how) {
            return 0;
        }
    }

    /**
     * Captures every frame the server sends, in order.
     */
    private static class RecordingRawSocket implements HttpRawSocket {
        final ObjList<byte[]> sentFrames = new ObjList<>();
        private final long bufferAddress;
        private final int bufferSize;

        RecordingRawSocket(long bufferAddress, int bufferSize) {
            this.bufferAddress = bufferAddress;
            this.bufferSize = bufferSize;
        }

        @Override
        public long getBufferAddress() {
            return bufferAddress;
        }

        @Override
        public int getBufferSize() {
            return bufferSize;
        }

        @Override
        public void send(int size) {
            byte[] copy = new byte[size];
            for (int i = 0; i < size; i++) {
                copy[i] = Unsafe.getByte(bufferAddress + i);
            }
            sentFrames.add(copy);
        }
    }

    /**
     * Same shape as the contexts in
     * {@code QwpIngressUpgradeProcessorResumeRecvTest}: overrides the I/O
     * access points with the test doubles.
     */
    private static class TestableContext extends HttpConnectionContext {
        private final RecordingRawSocket rawSocket;
        private final long testRecvBuffer;
        private final int testRecvBufferSize;

        TestableContext(
                HttpServerConfiguration config,
                PhasedNetworkFacade nf,
                RecordingRawSocket rawSocket,
                long recvBuffer,
                int recvBufferSize
        ) {
            super(config, (_, log) -> new PlainSocket(nf, log));
            this.rawSocket = rawSocket;
            this.testRecvBuffer = recvBuffer;
            this.testRecvBufferSize = recvBufferSize;
        }

        @Override
        public HttpRawSocket getRawResponseSocket() {
            return rawSocket;
        }

        @Override
        public long getRecvBuffer() {
            return testRecvBuffer;
        }

        @Override
        public int getRecvBufferSize() {
            return testRecvBufferSize;
        }
    }
}
