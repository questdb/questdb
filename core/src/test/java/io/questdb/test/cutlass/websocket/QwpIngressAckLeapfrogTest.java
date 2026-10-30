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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.pool.WalWriterPool;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.wal.DurabilityTier;
import io.questdb.cairo.wal.DurableAckRegistry;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.server.QwpIngressProcessorState;
import io.questdb.cutlass.qwp.server.QwpIngressUpgradeProcessor;
import io.questdb.cutlass.qwp.server.QwpTudCache;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.cutlass.qwp.QwpWireTestFixtures;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.test.cutlass.websocket.QwpIngressWireHarness.*;

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
                    public long getLocalDurableSeqTxn(CharSequence tableDirName) {
                        return -1L;
                    }

                    @Override
                    public long getReplicatedDurableSeqTxn(CharSequence tableDirName) {
                        // uploads never catch up: the deferral stays armed for
                        // the whole grace window
                        return -1L;
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
     * A PING that arrives at least qwp.min.idle.ms.before.writer.release after
     * the last data frame returns the idle WAL writer to the pool. The next data
     * frame acquires a fresh writer, commits, and is acked as usual.
     */
    @Test
    public void testIdleWriterReleaseOnPingAfterIdleThreshold() throws Exception {
        assertMemoryLeak(() -> {
            final long[] nowMicros = {1_000_000};
            final HttpFullFatServerConfiguration httpConfig = idleWriterReleaseHttpConfig(nowMicros, 30_000);
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            byte[] first = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L));
            byte[] ping = createMaskedFrame(WebSocketOpcode.PING, new byte[0]);
            byte[] second = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L));

            PhasedNetworkFacade nf = new PhasedNetworkFacade(concat(first, ping, second));
            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                QwpIngressProcessorState state = openIdleWriterReleaseState(httpConfig, context);

                drive(processor, context, nf, first.length);
                Assert.assertEquals(1, tudCacheSize(state));
                Assert.assertEquals(1, busyWalWriterCount());

                nowMicros[0] += 30_000_000;
                drive(processor, context, nf, ping.length);
                Assert.assertEquals("idle writer must leave the connection cache", 0, tudCacheSize(state));
                Assert.assertEquals("idle writer must return to the pool", 0, busyWalWriterCount());

                drive(processor, context, nf, second.length);
                Assert.assertEquals(1, tudCacheSize(state));
                Assert.assertEquals(1, maxCumulativeOkAck(rawSocket.sentFrames));
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }

            drainWalQueue();
            assertQuery("SELECT v, ts FROM tab")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            v\tts
                            100\t1970-01-01T00:00:01.000000Z
                            200\t1970-01-01T00:00:02.000000Z
                            """);
        });
    }

    /**
     * The release must not touch a table holding rows of an open
     * FLAG_DEFER_COMMIT group: freeing its writer would roll the rows back.
     * The group-closing frame then commits them.
     */
    @Test
    public void testIdleWriterReleaseRetainsUncommittedDeferredRows() throws Exception {
        assertMemoryLeak(() -> {
            final long[] nowMicros = {1_000_000};
            final HttpFullFatServerConfiguration httpConfig = idleWriterReleaseHttpConfig(nowMicros, 30_000);
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            byte[] data = createMaskedFrame(WebSocketOpcode.BINARY, deferred(oneRowMessage(100L, 1_000_000L)));
            byte[] ping = createMaskedFrame(WebSocketOpcode.PING, new byte[0]);
            byte[] commit = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L));

            PhasedNetworkFacade nf = new PhasedNetworkFacade(concat(data, ping, commit));
            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                QwpIngressProcessorState state = openIdleWriterReleaseState(httpConfig, context);

                drive(processor, context, nf, data.length);
                Assert.assertEquals(1, tudCacheSize(state));
                Assert.assertEquals(-1, maxCumulativeOkAck(rawSocket.sentFrames));

                nowMicros[0] += 60_000_000;
                drive(processor, context, nf, ping.length);
                Assert.assertEquals("writer with uncommitted deferred rows must stay cached", 1, tudCacheSize(state));
                Assert.assertEquals(1, busyWalWriterCount());

                drive(processor, context, nf, commit.length);
                Assert.assertEquals(1, maxCumulativeOkAck(rawSocket.sentFrames));
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }

            drainWalQueue();
            assertQuery("SELECT v, ts FROM tab")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            v\tts
                            100\t1970-01-01T00:00:01.000000Z
                            200\t1970-01-01T00:00:02.000000Z
                            """);
        });
    }

    /**
     * A PING before the idle threshold releases nothing, and every data frame
     * restarts the idle clock. A PING exactly at the threshold releases.
     */
    @Test
    public void testIdleWriterReleaseSkippedBeforeIdleThreshold() throws Exception {
        assertMemoryLeak(() -> {
            final long[] nowMicros = {1_000_000};
            final HttpFullFatServerConfiguration httpConfig = idleWriterReleaseHttpConfig(nowMicros, 30_000);
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            byte[] first = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L));
            byte[] second = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(200L, 2_000_000L));
            byte[] ping = createMaskedFrame(WebSocketOpcode.PING, new byte[0]);

            PhasedNetworkFacade nf = new PhasedNetworkFacade(concat(first, second, ping, ping));
            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                QwpIngressProcessorState state = openIdleWriterReleaseState(httpConfig, context);

                drive(processor, context, nf, first.length);
                nowMicros[0] += 20_000_000;
                drive(processor, context, nf, second.length);
                // 40s since the first frame, but only 20s since the second one
                nowMicros[0] += 20_000_000;
                drive(processor, context, nf, ping.length);
                Assert.assertEquals("PING before the idle threshold must not release", 1, tudCacheSize(state));
                Assert.assertEquals(1, busyWalWriterCount());

                nowMicros[0] += 10_000_000;
                drive(processor, context, nf, ping.length);
                Assert.assertEquals(0, tudCacheSize(state));
                Assert.assertEquals(0, busyWalWriterCount());
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testIdleWriterReleaseDisabledByZero() throws Exception {
        assertMemoryLeak(() -> {
            final long[] nowMicros = {1_000_000};
            final HttpFullFatServerConfiguration httpConfig = idleWriterReleaseHttpConfig(nowMicros, 0);
            execute("CREATE TABLE tab (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);
            byte[] data = createMaskedFrame(WebSocketOpcode.BINARY, oneRowMessage(100L, 1_000_000L));
            byte[] ping = createMaskedFrame(WebSocketOpcode.PING, new byte[0]);

            PhasedNetworkFacade nf = new PhasedNetworkFacade(concat(data, ping));
            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            RecordingRawSocket rawSocket = new RecordingRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(httpConfig, nf, rawSocket, recvBuf, RECV_BUFFER_SIZE)) {
                QwpIngressProcessorState state = openIdleWriterReleaseState(httpConfig, context);

                drive(processor, context, nf, data.length);
                nowMicros[0] += 3_600_000_000L;
                drive(processor, context, nf, ping.length);
                Assert.assertEquals("release disabled: writer must stay cached", 1, tudCacheSize(state));
                Assert.assertEquals(1, busyWalWriterCount());
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
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
                    public long getLocalDurableSeqTxn(CharSequence tableDirName) {
                        return -1L;
                    }

                    @Override
                    public long getReplicatedDurableSeqTxn(CharSequence tableDirName) {
                        return durableWatermark.get();
                    }

                    @Override
                    public boolean isEnabled() {
                        return true;
                    }

                    @Override
                    public boolean isTierSetAvailable(int tiers) {
                        return !DurabilityTier.hasLocal(tiers);
                    }

                    @Override
                    public void onTableDropped(TableToken tableToken) {
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
                    state.setDurableAckTiers(DurabilityTier.REPLICATED);
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

    private static int busyWalWriterCount() throws Exception {
        Field f = CairoEngine.class.getDeclaredField("walWriterPool");
        f.setAccessible(true);
        return ((WalWriterPool) f.get(engine)).getBusyCount();
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

    private static HttpFullFatServerConfiguration idleWriterReleaseHttpConfig(long[] nowMicros, long minIdleMs) {
        final LineHttpProcessorConfiguration lineConfig =
                new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration) {
                    @Override
                    public MicrosecondClock getMicrosecondClock() {
                        return () -> nowMicros[0];
                    }

                    @Override
                    public long getQwpMinIdleMsBeforeWriterRelease() {
                        return minIdleMs;
                    }
                };
        return new DefaultHttpServerConfiguration(configuration) {
            @Override
            public LineHttpProcessorConfiguration getLineHttpProcessorConfiguration() {
                return lineConfig;
            }
        };
    }

    private static QwpIngressProcessorState openIdleWriterReleaseState(
            HttpFullFatServerConfiguration httpConfig,
            TestableContext context
    ) throws Exception {
        QwpIngressProcessorState state = new QwpIngressProcessorState(
                RECV_BUFFER_SIZE,
                httpConfig.getSendBufferSize(),
                engine,
                httpConfig.getLineHttpProcessorConfiguration()
        );
        state.of(-1, AllowAllSecurityContext.INSTANCE);
        getLV().set(context, state);
        return state;
    }

    private static int tudCacheSize(QwpIngressProcessorState state) throws Exception {
        Field f = QwpIngressProcessorState.class.getDeclaredField("tudCache");
        f.setAccessible(true);
        return ((QwpTudCache) f.get(state)).size();
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
}
