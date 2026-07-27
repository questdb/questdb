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

import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRawSocket;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.cutlass.http.processors.LineHttpProcessorConfiguration;
import io.questdb.cutlass.qwp.server.QwpIngressProcessorState;
import io.questdb.cutlass.qwp.server.QwpIngressUpgradeProcessor;
import io.questdb.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.log.Log;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import io.questdb.network.PlainSocket;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;

/**
 * Tests for {@link QwpIngressUpgradeProcessor} covering network edge cases:
 * partial reads, oversized frames, send backpressure, buffer-too-small, and
 * state machine transitions that are hard to trigger from E2E tests.
 * <p>
 * Uses {@link MockNetworkFacade} (extending {@link NetworkFacadeImpl}) for recv
 * control and {@link MockRawSocket} for send control, following the established
 * patterns from {@code BaseLineTcpContextTest.LineTcpNetworkFacade} and
 * {@code QwpWebSocketUpgradeProcessorOnHeadersReadyTest.MockRawSocket}.
 */
public class QwpIngressUpgradeProcessorResumeRecvTest extends AbstractCairoTest {
    private static final byte[] DEFAULT_MASK_KEY = {0x12, 0x34, 0x56, 0x78};
    private static final int RECV_BUFFER_SIZE = 1024;
    private static final int SEND_BUFFER_SIZE = 256;

    @Test
    public void testAckBlocked() throws Exception {
        // When a PING arrives and the ACK flush gets PeerIsSlowToReadException,
        // handlePing propagates the exception so the framework parks the
        // connection for write and resumeSend can drain the residual ACK
        // bytes. The send state machine sits in RESUME_ACK until the drain
        // completes, and the pong is not attempted on this recv cycle.
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            // Feed a PING frame to trigger processWebSocketFrames → flushPendingAck
            byte[] pingFrame = createMaskedFrame(WebSocketOpcode.PING, new byte[0]);
            MockNetworkFacade mockNf = new MockNetworkFacade(pingFrame);

            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            MockRawSocket mockRawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
            // First send = ACK (via flushPendingAck in handlePing). Throw on ACK.
            mockRawSocket.throwSlowToReadOnCall = 1;
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf, mockRawSocket, recvBuf, RECV_BUFFER_SIZE
            )) {
                QwpIngressProcessorState state = setupState(httpConfig, context);
                // Set up pending ACK: highestProcessed > lastAcked
                state.setHighestProcessedSequence(5);

                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected PeerIsSlowToReadException");
                } catch (PeerIsSlowToReadException e) {
                    // expected: ack send parked, framework will reschedule for write
                }
                Assert.assertTrue(state.isSending());
                // Deferred error should NOT be set (ACK only, no error)
                Assert.assertEquals(-1, state.getDeferredErrorSequence());
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testAckBufferTooSmall() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            byte[] pingFrame = createMaskedFrame(WebSocketOpcode.PING, new byte[0]);
            MockNetworkFacade mockNf = new MockNetworkFacade(pingFrame);

            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            // ACK frame = 11 bytes (2 header + 9 payload). Use 10-byte buffer.
            int tinyBufSize = 10;
            long sendBuf = Unsafe.malloc(tinyBufSize, MemoryTag.NATIVE_DEFAULT);
            MockRawSocket mockRawSocket = new MockRawSocket(sendBuf, tinyBufSize);
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf, mockRawSocket, recvBuf, RECV_BUFFER_SIZE
            )) {
                QwpIngressProcessorState state = setupState(httpConfig, context);
                state.setHighestProcessedSequence(5);

                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected ServerDisconnectException");
                } catch (ServerDisconnectException e) {
                    // expected: buffer too small for ACK (PeerDisconnectedException
                    // caught by resumeRecv's Throwable handler → ServerDisconnectException)
                }
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, tinyBufSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testBadRequestSendSlowToRead() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            MockRawSocket mockRawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
            mockRawSocket.throwSlowToReadOnCall = 1;
            try (
                    MockHttpRequestHeader header = new MockHttpRequestHeader();
                    TestableContext context = new TestableContext(
                            httpConfig, new MockNetworkFacade(new byte[0]),
                            mockRawSocket, header, 0, 0
                    )
            ) {
                // Missing Sec-WebSocket-Key → 400 Bad Request
                header.setHeader("Upgrade", "websocket");
                header.setHeader("Connection", "Upgrade");
                header.setHeader("Sec-WebSocket-Version", "13");

                // onHeadersReady stages the 400 body without sending: the
                // contract forbids PeerIsSlowToReadException here, so the
                // actual rawSocket.send is deferred to onRequestComplete.
                processor.onHeadersReady(context);
                Assert.assertEquals("onHeadersReady must not call rawSocket.send",
                        0, mockRawSocket.sendCallCount);

                // The first rawSocket.send is configured to throw PISR. The
                // fix lets that propagate from onRequestComplete (the
                // framework then parks-on-write and re-fires resumeSend).
                try {
                    processor.onRequestComplete(context);
                    Assert.fail("Expected PeerIsSlowToReadException to propagate from deferred reject flush");
                } catch (PeerIsSlowToReadException ignored) {
                }
                Assert.assertEquals("onRequestComplete must have attempted the deferred send",
                        1, mockRawSocket.sendCallCount);
            } finally {
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCloseDrainCapsBytesAndRecvRequests() throws Exception {
        assertMemoryLeak(() -> {
            int byteBudget = 256 * 1024;
            int recvBufferSize = 200_000;
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration) {
                @Override
                public int getRecvBufferSize() {
                    return recvBufferSize;
                }
            };
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            AlwaysReadableNetworkFacade mockNf = new AlwaysReadableNetworkFacade();
            long recvBuf = Unsafe.malloc(recvBufferSize, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf,
                    new MockRawSocket(sendBuf, SEND_BUFFER_SIZE),
                    recvBuf, recvBufferSize
            )) {
                QwpIngressProcessorState state = setupState(httpConfig, context);
                state.beginCloseDrain();

                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected PeerIsSlowToWriteException (drain byte-budget yield)");
                } catch (PeerIsSlowToWriteException e) {
                    // expected: fixed byte budget yielded to the dispatcher
                }

                Assert.assertEquals(recvBufferSize, mockNf.firstRequestSize);
                Assert.assertEquals(byteBudget - recvBufferSize, mockNf.secondRequestSize);
                Assert.assertEquals(byteBudget, mockNf.totalBytesReceived);
                Assert.assertEquals(2, mockNf.recvCount);
            } finally {
                Unsafe.free(recvBuf, recvBufferSize, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCloseDrainPollsExpiryAfterPositiveRecv() throws Exception {
        assertMemoryLeak(() -> {
            long[] nowMicros = {0};
            HttpFullFatServerConfiguration httpConfig = createHttpConfiguration(nowMicros);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            AlwaysReadableNetworkFacade mockNf = new AlwaysReadableNetworkFacade(nowMicros);
            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf,
                    new MockRawSocket(sendBuf, SEND_BUFFER_SIZE),
                    recvBuf, RECV_BUFFER_SIZE
            )) {
                QwpIngressProcessorState state = setupState(httpConfig, context);
                state.beginCloseDrain();
                Assert.assertFalse(state.isCloseDrainExpired());

                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected ServerDisconnectException after in-loop expiry");
                } catch (ServerDisconnectException e) {
                    // expected: first positive recv advanced the clock to the deadline
                } catch (PeerIsSlowToWriteException e) {
                    Assert.fail("close drain must poll expiry after each positive recv");
                }
                Assert.assertEquals("expiry must stop later receives", 1, mockNf.recvCount);
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCloseDrainYieldsWithinQuantumWhenPeerStaysReadable() throws Exception {
        // Regression: the post-CLOSE read-drain must not spin unbounded while
        // the peer keeps the socket readable. The syscall quantum complements
        // the fixed byte budget and yields via PeerIsSlowToWriteException so the
        // worker can service other connections before the drain resumes.
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            int quantum = getCloseDrainMaxRecvPerDispatch();
            AlwaysReadableNetworkFacade mockNf = new AlwaysReadableNetworkFacade();
            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf,
                    new MockRawSocket(sendBuf, SEND_BUFFER_SIZE),
                    recvBuf, RECV_BUFFER_SIZE
            )) {
                QwpIngressProcessorState state = setupState(httpConfig, context);
                // Arm the post-CLOSE read-drain (fatal CLOSE + FIN already out).
                state.beginCloseDrain();
                Assert.assertTrue(state.isCloseDraining());
                Assert.assertFalse(state.isCloseDrainExpired());

                // First dispatch: peer floods the socket. Without the quantum
                // this call would never return; with it, it yields after
                // exactly one quantum of receives.
                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected PeerIsSlowToWriteException (drain quantum yield)");
                } catch (PeerIsSlowToWriteException e) {
                    // expected: bounded drain quantum yielded to the dispatcher
                }
                Assert.assertEquals(quantum, mockNf.recvCount);
                Assert.assertTrue("drain must stay armed after a yield", state.isCloseDraining());

                // Second dispatch yields again: the drain keeps making bounded
                // progress rather than monopolizing the worker in one dispatch.
                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected PeerIsSlowToWriteException on the second drain dispatch");
                } catch (PeerIsSlowToWriteException e) {
                    // expected
                }
                Assert.assertEquals(2 * quantum, mockNf.recvCount);

                // When the peer finally stops (would-block), the drain returns
                // normally and stays parked for the next read -- no disconnect.
                mockNf.wouldBlock = true;
                processor.resumeRecv(context);
                Assert.assertTrue("would-block keeps the drain parked", state.isCloseDraining());

                // When the peer closes, the drain tears the connection down.
                mockNf.wouldBlock = false;
                mockNf.closed = true;
                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected ServerDisconnectException on peer close during drain");
                } catch (ServerDisconnectException e) {
                    // expected
                }
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCloseEchoWaitDiscardedFrameFloodPollsDeadlineOncePerDispatch() throws Exception {
        // The close-echo wait discards every non-CLOSE frame, and the read cap
        // lets one worker turn admit CLOSE_ECHO_FRAME_BYTE_BUDGET / 6 = 43_690
        // minimum-size masked frames. Polling the grace deadline per discarded
        // frame therefore costs 43_690 Os.currentTimeMicros() JNI transitions
        // per turn (~15 ns each on an Apple M-series host, ~0.7 ms of pure
        // clock reads -- more than the 256 KiB recv syscall that admitted the
        // bytes) and buys nothing: processWebSocketFrames polls the same
        // deadline once on entry, before the parse loop, and that entry poll is
        // on EVERY path that can reach the discard gate (resumeRecv and
        // drainBufferedFrames are its only callers). The parse loop always
        // returns to the dispatcher, so the poll it would add can only fire
        // sooner within a bounded, sub-millisecond turn.
        //
        // Asserts an operation count, never elapsed time: the counting clock
        // records every read the processor makes through
        // LineHttpProcessorConfiguration.getMicrosecondClock(), which is where
        // all of this path's deadlines come from.
        assertMemoryLeak(() -> {
            long[] nowMicros = {0};
            long[] clockReads = {0};
            // Deliberately larger than the byte budget: the read cap must bind,
            // so one turn admits a full budget's worth of discardable frames.
            int recvBufferSize = 300_000;
            HttpFullFatServerConfiguration httpConfig = createHttpConfiguration(nowMicros, clockReads, recvBufferSize);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            byte[] ping = createMaskedFrame(WebSocketOpcode.PING, new byte[0]);
            Assert.assertEquals("test setup: this must be the minimum masked client frame", 6, ping.length);
            int pipelinedFrames = 44_000; // 264_000 bytes -- outlasts one budget
            byte[] wire = new byte[pipelinedFrames * ping.length];
            for (int i = 0; i < pipelinedFrames; i++) {
                System.arraycopy(ping, 0, wire, i * ping.length, ping.length);
            }
            int framesPerTurn = QwpIngressUpgradeProcessor.CLOSE_ECHO_FRAME_BYTE_BUDGET / ping.length;
            Assert.assertEquals("test setup: one turn must admit a full budget of minimum frames", 43_690, framesPerTurn);

            RecordingNetworkFacade mockNf = new RecordingNetworkFacade(wire);
            long recvBuf = Unsafe.malloc(recvBufferSize, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf,
                    new MockRawSocket(sendBuf, SEND_BUFFER_SIZE),
                    recvBuf, recvBufferSize
            )) {
                QwpIngressProcessorState state = setupState(httpConfig, context);
                state.initiateRoleChangeClose();
                state.beginCloseEchoWait();
                Assert.assertTrue(state.isAwaitingCloseEcho());
                // Arming read the clock once to compute the deadline; count
                // only what the dispatch below spends.
                clockReads[0] = 0;

                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected PeerIsSlowToWriteException (close-echo read cap yield)");
                } catch (PeerIsSlowToWriteException e) {
                    // expected: the fixed byte budget yielded the worker
                }

                Assert.assertEquals("test setup: the flood must be admitted in a single capped recv", 1, mockNf.recvCalls);
                Assert.assertEquals(
                        "one worker turn must poll the echo deadline ONCE, on processWebSocketFrames entry;"
                                + " a poll per discarded frame costs " + (framesPerTurn + 1)
                                + " JNI clock transitions per turn for zero added deadline accuracy",
                        1, clockReads[0]
                );

                // The next turn gets its own single entry poll: the cost is
                // one clock read per dispatch, not per frame.
                clockReads[0] = 0;
                try {
                    processor.resumeRecv(context);
                } catch (PeerIsSlowToWriteException e) {
                    // either outcome is fine: the flood has fewer than a
                    // budget's worth of bytes left
                }
                Assert.assertEquals(
                        "the second dispatch must poll the deadline once too, and only on"
                                + " processWebSocketFrames entry: no path through the discard gate may add a poll",
                        1, clockReads[0]
                );
            } finally {
                Unsafe.free(recvBuf, recvBufferSize, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCloseEchoWaitDrainsPipelinedTailInOneDispatch() throws Exception {
        // The close-echo wait must read the client's pipelined tail at
        // min(recvBufferSize, CLOSE_ECHO_FRAME_BYTE_BUDGET) per dispatch and
        // not at a frame-count-derived 6 KiB. The echo sits BEHIND that tail
        // in the same TCP stream, so the server cannot observe it before it
        // has consumed everything the client sent earlier: every extra
        // dispatcher turn spent draining is added latency on the delivery
        // confirmation the wait exists to collect, and added epoll re-arm /
        // worker-dispatch work per connection on a fleet-wide demote.
        // Asserts operation counts, never elapsed time: 10_000 minimum-size
        // pipelined frames plus the echo (60_008 bytes, one recv buffer's
        // worth) must cost ONE recv and ONE dispatch. The count-derived
        // 6 KiB cap needed ten of each.
        assertMemoryLeak(() -> {
            long[] nowMicros = {0};
            int recvBufferSize = 64 * 1024;
            HttpFullFatServerConfiguration httpConfig = createHttpConfiguration(nowMicros, recvBufferSize);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            byte[] ping = createMaskedFrame(WebSocketOpcode.PING, new byte[0]);
            Assert.assertEquals("test setup: this must be the minimum masked client frame", 6, ping.length);
            byte[] echo = roleChangeCloseEchoFrame();
            int pipelinedFrames = 10_000;
            byte[] wire = new byte[pipelinedFrames * ping.length + echo.length];
            for (int i = 0; i < pipelinedFrames; i++) {
                System.arraycopy(ping, 0, wire, i * ping.length, ping.length);
            }
            System.arraycopy(echo, 0, wire, pipelinedFrames * ping.length, echo.length);
            Assert.assertTrue(
                    "test setup: the whole pipelined tail must fit one recv buffer",
                    wire.length <= recvBufferSize
            );

            RecordingNetworkFacade mockNf = new RecordingNetworkFacade(wire);
            long recvBuf = Unsafe.malloc(recvBufferSize, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf,
                    new MockRawSocket(sendBuf, SEND_BUFFER_SIZE),
                    recvBuf, recvBufferSize
            )) {
                QwpIngressProcessorState state = setupState(httpConfig, context);
                // The ROLE_CHANGE CLOSE is on the wire; the connection now
                // only waits for the echo that proves the client read it.
                state.initiateRoleChangeClose();
                state.beginCloseEchoWait();
                Assert.assertTrue(state.isAwaitingCloseEcho());

                int dispatches = 0;
                boolean hasObservedEcho = false;
                while (dispatches < 64) {
                    dispatches++;
                    try {
                        processor.resumeRecv(context);
                    } catch (PeerIsSlowToWriteException e) {
                        // the read cap yielded the worker; the dispatcher
                        // re-arms READ and re-enters on the next turn
                        continue;
                    } catch (ServerDisconnectException e) {
                        hasObservedEcho = true;
                        break;
                    }
                }

                Assert.assertTrue(
                        "the close echo must be reached within the dispatch allowance",
                        hasObservedEcho
                );
                Assert.assertEquals(
                        "reaching the echo must cost one dispatcher turn: each extra turn is an epoll re-arm,"
                                + " an epoll_wait wakeup and a worker dispatch of added echo latency",
                        1, dispatches
                );
                Assert.assertEquals(
                        "two receives total: ONE that consumes the whole pipelined tail plus the echo,"
                                + " and gracefulCloseAndDisconnect's pre-close drain probe",
                        2, mockNf.recvCalls
                );
                Assert.assertEquals(
                        "the close-echo read must be capped at min(recvBufferSize, CLOSE_ECHO_FRAME_BYTE_BUDGET);"
                                + " a frame-count-derived cap throttles the drain to 6 KiB a turn and delays the echo",
                        recvBufferSize, mockNf.firstRecvRequestSize
                );
                Assert.assertEquals(
                        "the parse loop must leave nothing buffered once the echo has been parsed",
                        0, state.getRecvBufferLen()
                );
            } finally {
                Unsafe.free(recvBuf, recvBufferSize, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCloseEchoWaitYieldsWithinByteBudgetOnFloodedTail() throws Exception {
        // Fairness twin of testCloseEchoWaitDrainsPipelinedTailInOneDispatch:
        // the raised read cap must stay a FIXED budget rather than the
        // configured recv buffer, and one dispatch must still yield the
        // worker. A peer flooding minimum-size frames therefore costs one
        // recv of CLOSE_ECHO_FRAME_BYTE_BUDGET bytes and at most
        // BYTE_BUDGET / 6 = 43_690 header parses per worker turn, whatever
        // the operator configured http.recv.buffer.size to.
        //
        // It also pins the no-stall invariant the recv-time cap exists for:
        // whatever the parse loop leaves in the recv buffer is an INCOMPLETE
        // frame (four bytes here), never a complete one. A complete frame
        // parked in user space behind an emptied kernel socket would need an
        // inbound event to be re-parsed, and the dispatcher's edge-triggered
        // READ re-arm never delivers one -- the connection would stall until
        // the idle reaper collected it.
        assertMemoryLeak(() -> {
            long[] nowMicros = {0};
            // Deliberately larger than the byte budget: the cap must bind.
            int recvBufferSize = 300_000;
            HttpFullFatServerConfiguration httpConfig = createHttpConfiguration(nowMicros, recvBufferSize);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            byte[] ping = createMaskedFrame(WebSocketOpcode.PING, new byte[0]);
            int pipelinedFrames = 44_000; // 264_000 bytes -- outlasts one budget
            byte[] wire = new byte[pipelinedFrames * ping.length];
            for (int i = 0; i < pipelinedFrames; i++) {
                System.arraycopy(ping, 0, wire, i * ping.length, ping.length);
            }
            Assert.assertTrue(
                    "test setup: the flood must exceed one byte budget",
                    wire.length > QwpIngressUpgradeProcessor.CLOSE_ECHO_FRAME_BYTE_BUDGET
            );

            RecordingNetworkFacade mockNf = new RecordingNetworkFacade(wire);
            long recvBuf = Unsafe.malloc(recvBufferSize, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf,
                    new MockRawSocket(sendBuf, SEND_BUFFER_SIZE),
                    recvBuf, recvBufferSize
            )) {
                QwpIngressProcessorState state = setupState(httpConfig, context);
                state.initiateRoleChangeClose();
                state.beginCloseEchoWait();

                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected PeerIsSlowToWriteException (close-echo read cap yield)");
                } catch (PeerIsSlowToWriteException e) {
                    // expected: the fixed budget yielded the worker
                }
                Assert.assertEquals(
                        "one worker turn must admit exactly CLOSE_ECHO_FRAME_BYTE_BUDGET bytes,"
                                + " independently of the configured recv buffer",
                        QwpIngressUpgradeProcessor.CLOSE_ECHO_FRAME_BYTE_BUDGET, mockNf.firstRecvRequestSize
                );
                Assert.assertEquals("the capped read must be a single recv", 1, mockNf.recvCalls);
                Assert.assertEquals(
                        "STALL RISK: the parse loop must park only an incomplete frame; a complete frame left"
                                + " in user space behind an emptied kernel socket never gets re-parsed",
                        QwpIngressUpgradeProcessor.CLOSE_ECHO_FRAME_BYTE_BUDGET % ping.length,
                        state.getRecvBufferLen()
                );
                Assert.assertTrue(
                        "the buffered remainder must be shorter than one minimum frame",
                        state.getRecvBufferLen() < ping.length
                );

                // The next dispatch resumes with a fresh budget of the same
                // fixed size -- progress is bounded per turn, not per buffer.
                try {
                    processor.resumeRecv(context);
                } catch (PeerIsSlowToWriteException e) {
                    // either outcome is fine: the flood has fewer than a
                    // budget's worth of bytes left
                }
                Assert.assertEquals(
                        "every close-echo dispatch must request the same fixed budget",
                        QwpIngressUpgradeProcessor.CLOSE_ECHO_FRAME_BYTE_BUDGET, mockNf.secondRecvRequestSize
                );
            } finally {
                Unsafe.free(recvBuf, recvBufferSize, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCloseWhenBufferBusy() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            // CLOSE frame with code 1000 (normal closure)
            byte[] closePayload = {0x03, (byte) 0xE8}; // 1000 big-endian
            byte[] closeFrame = createMaskedFrame(WebSocketOpcode.CLOSE, closePayload);

            MockNetworkFacade mockNf = new MockNetworkFacade(closeFrame);
            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            MockRawSocket mockRawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf, mockRawSocket, recvBuf, RECV_BUFFER_SIZE
            )) {
                QwpIngressProcessorState state = setupState(httpConfig, context);
                // Set send state to non-READY
                state.onAckBlocked(0);

                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected ServerDisconnectException");
                } catch (ServerDisconnectException e) {
                    // expected: CLOSE always disconnects
                }
                // Close response skipped because buffer busy
                Assert.assertEquals(0, mockRawSocket.sendCallCount);
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testErrorBufferTooSmall() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            // Garbage binary → triggers error response
            byte[] garbageBinary = createMaskedFrame(WebSocketOpcode.BINARY, new byte[]{0x00, 0x01, 0x02});
            MockNetworkFacade mockNf = new MockNetworkFacade(garbageBinary);

            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            // Error frame minimum = 13 bytes (2 header + 1 status + 8 seq + 2 msglen).
            // Use 12-byte buffer.
            int tinyBufSize = 12;
            long sendBuf = Unsafe.malloc(tinyBufSize, MemoryTag.NATIVE_DEFAULT);
            MockRawSocket mockRawSocket = new MockRawSocket(sendBuf, tinyBufSize);
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf, mockRawSocket, recvBuf, RECV_BUFFER_SIZE
            )) {
                setupState(httpConfig, context);

                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected ServerDisconnectException");
                } catch (ServerDisconnectException e) {
                    // expected: buffer too small for error (PeerDisconnectedException
                    // caught by resumeRecv's Throwable handler → ServerDisconnectException)
                }
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, tinyBufSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testErrorSendBlocked() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            byte[] garbageBinary = createMaskedFrame(WebSocketOpcode.BINARY, new byte[]{0x00, 0x01, 0x02});
            MockNetworkFacade mockNf = new MockNetworkFacade(garbageBinary);

            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            MockRawSocket mockRawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
            // Throw PeerIsSlowToRead on first send (error response)
            mockRawSocket.throwSlowToReadOnCall = 1;
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf, mockRawSocket, recvBuf, RECV_BUFFER_SIZE
            )) {
                QwpIngressProcessorState state = setupState(httpConfig, context);

                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected PeerIsSlowToReadException");
                } catch (PeerIsSlowToReadException e) {
                    // expected: error send blocked
                }
                Assert.assertTrue(state.isSending());
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testErrorSendBlockedWithPendingAck() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            // Binary frame with garbage data -- processing fails, triggers error response
            byte[] garbageBinary = createMaskedFrame(WebSocketOpcode.BINARY, new byte[]{0x00, 0x01, 0x02});
            MockNetworkFacade mockNf = new MockNetworkFacade(garbageBinary);

            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            MockRawSocket mockRawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
            // handleBinaryMessage's error path first tries to ACK all successful
            // messages (trySendAck at processor line 532), then sends the error.
            // Throw PeerIsSlowToRead on the first send (the ACK attempt).
            // trySendAck catches it → onAckBlocked, then handleBinaryMessage
            // catches it → onErrorBlocked, producing RESUME_ACK_THEN_ERROR.
            mockRawSocket.throwSlowToReadOnCall = 1;
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf, mockRawSocket, recvBuf, RECV_BUFFER_SIZE
            )) {
                QwpIngressProcessorState state = setupState(httpConfig, context);
                // Pending ACK: highestProcessed > lastAcked, so hasPendingAck() is true
                state.setHighestProcessedSequence(5);

                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected PeerIsSlowToReadException");
                } catch (PeerIsSlowToReadException e) {
                    // expected: error send blocked
                }
                Assert.assertTrue(state.isSending());
                Assert.assertTrue(state.getDeferredErrorSequence() >= 0);
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testErrorWhenSendNotReady() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            byte[] garbageBinary = createMaskedFrame(WebSocketOpcode.BINARY, new byte[]{0x00, 0x01, 0x02});
            MockNetworkFacade mockNf = new MockNetworkFacade(garbageBinary);

            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            MockRawSocket mockRawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf, mockRawSocket, recvBuf, RECV_BUFFER_SIZE
            )) {
                QwpIngressProcessorState state = setupState(httpConfig, context);
                // Set send state to non-READY before error triggers
                state.onAckBlocked(0);

                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected PeerIsSlowToReadException");
                } catch (PeerIsSlowToReadException e) {
                    // expected: error blocked because send not ready
                }
                // Error deferred: sending state with deferred error
                Assert.assertTrue(state.isSending());
                Assert.assertTrue(state.getDeferredErrorSequence() >= 0);
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testFrameParseError() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            // Frame with RSV1 bit set (0x40 | 0x82 = 0xC2): RSV bits trigger parse error
            byte[] badFrame = {(byte) 0xC2, (byte) 0x80, 0x00, 0x00, 0x00, 0x00};

            MockNetworkFacade mockNf = new MockNetworkFacade(badFrame);
            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf,
                    new MockRawSocket(sendBuf, SEND_BUFFER_SIZE),
                    recvBuf, RECV_BUFFER_SIZE
            )) {
                setupState(httpConfig, context);

                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected ServerDisconnectException");
                } catch (ServerDisconnectException e) {
                    // expected: frame parse error (RSV bits set)
                }
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testFrameTooLargeForRecvBuffer() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            // Craft a masked binary frame with 64-bit extended length exceeding the
            // config's recvBufferSize (default 131072). Use 64-bit encoding (length field = 127).
            // Header: FIN+BINARY(0x82), MASK+127(0xFF), 8-byte big-endian length, mask[4]
            long declaredLen = httpConfig.getRecvBufferSize() + 100L;
            byte[] frame = new byte[14]; // 2 + 8 (length) + 4 (mask)
            frame[0] = (byte) 0x82; // FIN + BINARY
            frame[1] = (byte) 0xFF; // MASK + 127 (64-bit extended)
            // 8-byte big-endian length
            for (int i = 0; i < 8; i++) {
                frame[2 + i] = (byte) ((declaredLen >> ((7 - i) * 8)) & 0xFF);
            }
            // mask key
            frame[10] = 0x12;
            frame[11] = 0x34;
            frame[12] = 0x56;
            frame[13] = 0x78;

            MockNetworkFacade mockNf = new MockNetworkFacade(frame);
            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            MockRawSocket mockRawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf, mockRawSocket, recvBuf, RECV_BUFFER_SIZE
            )) {
                setupState(httpConfig, context);

                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected ServerDisconnectException");
                } catch (ServerDisconnectException e) {
                    // expected: frame too large
                }
                // Should have sent a CLOSE frame with code 1009 (MESSAGE_TOO_BIG)
                Assert.assertTrue(mockRawSocket.sendCallCount > 0);
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testFrameTooLargeWhenSendBusy() throws Exception {
        // Regression: previously the CLOSE frame was silently skipped when an
        // ACK was in flight, so the client saw an ECONNRESET with no protocol
        // diagnostic. Now the fatal CLOSE is deferred via the send state
        // machine and emitted once the pending ACK drains via resumeSend.
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            long declaredLen = httpConfig.getRecvBufferSize() + 100L;
            byte[] frame = new byte[14];
            frame[0] = (byte) 0x82;
            frame[1] = (byte) 0xFF;
            for (int i = 0; i < 8; i++) {
                frame[2 + i] = (byte) ((declaredLen >> ((7 - i) * 8)) & 0xFF);
            }
            frame[10] = 0x12;
            frame[11] = 0x34;
            frame[12] = 0x56;
            frame[13] = 0x78;

            MockNetworkFacade mockNf = new MockNetworkFacade(frame);
            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            MockRawSocket mockRawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf, mockRawSocket, recvBuf, RECV_BUFFER_SIZE
            )) {
                QwpIngressProcessorState state = setupState(httpConfig, context);
                // Force send state to non-READY (an ACK is in flight). With
                // the old behaviour the CLOSE would be skipped; now it is
                // queued for the resume path.
                state.onAckBlocked(0);

                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected PeerIsSlowToReadException (deferred CLOSE)");
                } catch (PeerIsSlowToReadException e) {
                    // expected: CLOSE was deferred behind the in-flight ACK.
                }
                // No bytes written yet — CLOSE is queued.
                Assert.assertEquals(0, mockRawSocket.sendCallCount);
                // State transitioned to RESUME_ACK_THEN_CLOSE (== 7).
                Assert.assertEquals(7, state.getSendState());

                // Dispatcher comes back via resumeSend: pending ACK drains,
                // then the deferred CLOSE frame is written and the framework
                // raises ServerDisconnect.
                try {
                    processor.resumeSend(context);
                    Assert.fail("Expected ServerDisconnectException after deferred CLOSE flush");
                } catch (ServerDisconnectException e) {
                    // expected
                }
                Assert.assertTrue(
                        "CLOSE frame must be sent on resume",
                        mockRawSocket.sendCallCount >= 1
                );
                // First two payload bytes of the CLOSE frame carry the close
                // code in network order. Confirm it is 1009 (MESSAGE_TOO_BIG).
                int headerSize = (Unsafe.getByte(sendBuf + 1) & 0x7F) <= 125 ? 2 : 4;
                int hi = Unsafe.getByte(sendBuf + headerSize) & 0xFF;
                int lo = Unsafe.getByte(sendBuf + headerSize + 1) & 0xFF;
                Assert.assertEquals(1009, (hi << 8) | lo);
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testFrameTooLargeForRecvBufferWhenSendBusy() throws Exception {
        // Regression: the recvBufferLen >= recvBufferSize path in resumeRecv
        // previously threw ServerDisconnect without sending any CLOSE frame
        // (even when the send buffer was clear). It now routes through
        // sendFatalClose, deferring behind an in-flight ACK if needed.
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            MockRawSocket mockRawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(
                    httpConfig, new MockNetworkFacade(new byte[0]),
                    mockRawSocket, recvBuf, RECV_BUFFER_SIZE
            )) {
                QwpIngressProcessorState state = setupState(httpConfig, context);
                // Mark the recv buffer as full so the "frame too large for
                // recv buffer" branch fires before recvRaw is consulted.
                state.setRecvBufferLen(RECV_BUFFER_SIZE);
                state.onAckBlocked(0);

                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected PeerIsSlowToReadException (deferred CLOSE)");
                } catch (PeerIsSlowToReadException e) {
                    // expected
                }
                Assert.assertEquals(0, mockRawSocket.sendCallCount);
                Assert.assertEquals(7, state.getSendState()); // RESUME_ACK_THEN_CLOSE

                try {
                    processor.resumeSend(context);
                    Assert.fail("Expected ServerDisconnectException");
                } catch (ServerDisconnectException e) {
                    // expected
                }
                Assert.assertTrue(mockRawSocket.sendCallCount >= 1);
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testHandshakeSendSlowToRead() throws Exception {
        // The 101 send is deferred from onHeadersReady to onRequestComplete so a
        // partial-write PeerIsSlowToReadException can propagate to the framework's
        // park-on-write path rather than being converted to a fatal HttpException.
        // The handshake bytes are staged on state in onHeadersReady; PISR fires
        // only from onRequestComplete.
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            MockRawSocket mockRawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
            mockRawSocket.throwSlowToReadOnCall = 1;
            try (
                    MockHttpRequestHeader header = new MockHttpRequestHeader();
                    TestableContext context = new TestableContext(
                            httpConfig, new MockNetworkFacade(new byte[0]),
                            mockRawSocket, header, 0, 0
                    )
            ) {
                header.setHeader("Upgrade", "websocket");
                header.setHeader("Connection", "Upgrade");
                header.setHeader("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");
                header.setHeader("Sec-WebSocket-Version", "13");

                // onHeadersReady stages bytes; it must NOT touch the socket.
                processor.onHeadersReady(context);
                Assert.assertEquals("onHeadersReady must not send",
                        0, mockRawSocket.sendCallCount);
                QwpIngressProcessorState state = getLV().get(context);
                Assert.assertNotNull(state);
                Assert.assertTrue("handshake flush must be pending after onHeadersReady",
                        state.isHandshakeFlushPending());
                Assert.assertTrue("staged byte count must be > 0",
                        state.getPendingHandshakeBytes() > 0);
                Assert.assertFalse("protocol switch must wait for finalize",
                        context.isSwitchProtocolCalled());

                // onRequestComplete drives the send; PISR propagates to the framework
                // (NOT swallowed into HttpException as the old onHeadersReady path did).
                try {
                    processor.onRequestComplete(context);
                    Assert.fail("Expected PeerIsSlowToReadException");
                } catch (PeerIsSlowToReadException expected) {
                    // expected -- framework parks the connection for write and
                    // schedules resumeSend.
                }
                Assert.assertEquals(1, mockRawSocket.sendCallCount);
                Assert.assertFalse("protocol switch must not happen on a parked send",
                        context.isSwitchProtocolCalled());
                Assert.assertTrue("handshake flush stays pending until resumeSend completes it",
                        state.isHandshakeFlushPending());
            } finally {
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testOnClosedInvalidState() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (TestableContext context = new TestableContext(
                    httpConfig, new MockNetworkFacade(new byte[0]),
                    new MockRawSocket(sendBuf, SEND_BUFFER_SIZE),
                    0, 0
            )) {
                QwpIngressProcessorState state = setupState(httpConfig, context);
                Field sendStateField = QwpIngressProcessorState.class.getDeclaredField("sendState");
                sendStateField.setAccessible(true);
                sendStateField.setInt(state, 99);

                // Should not throw — onConnectionClosed catches exceptions internally
                processor.onConnectionClosed(context);
            } finally {
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testOnClosedResumeAck() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (TestableContext context = new TestableContext(
                    httpConfig, new MockNetworkFacade(new byte[0]),
                    new MockRawSocket(sendBuf, SEND_BUFFER_SIZE),
                    0, 0
            )) {
                QwpIngressProcessorState state = setupState(httpConfig, context);
                state.onAckBlocked(3);
                Assert.assertTrue(state.isSending());

                processor.onConnectionClosed(context);

                Assert.assertTrue(context.isResumeResponseSendCalled());
            } finally {
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testOnClosedResumeAckThenError() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            MockRawSocket mockRawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(
                    httpConfig, new MockNetworkFacade(new byte[0]),
                    mockRawSocket, 0, 0
            )) {
                QwpIngressProcessorState state = setupState(httpConfig, context);
                state.onAckBlocked(3);
                state.onErrorBlocked((byte) 5, 4L, "test");
                Assert.assertTrue(state.isSending());
                Assert.assertEquals(4L, state.getDeferredErrorSequence());

                processor.onConnectionClosed(context);

                Assert.assertTrue(context.isResumeResponseSendCalled());
            } finally {
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testOnClosedResumeError() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (TestableContext context = new TestableContext(
                    httpConfig, new MockNetworkFacade(new byte[0]),
                    new MockRawSocket(sendBuf, SEND_BUFFER_SIZE),
                    0, 0
            )) {
                QwpIngressProcessorState state = setupState(httpConfig, context);
                state.onErrorBlocked((byte) 5, 4L, "test error");
                Assert.assertTrue(state.isSending());

                processor.onConnectionClosed(context);

                Assert.assertTrue(context.isResumeResponseSendCalled());
            } finally {
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPartialFrameCompaction() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            // Two PONG frames back-to-back. Deliver the first frame fully plus
            // 3 bytes of the second frame, then block. processWebSocketFrames
            // processes the first PONG (advancing pos past it), then hits
            // NEED_MORE on the partial second frame. In the finally block,
            // remaining > 0 && pos > buffer → memmove compacts the 3 trailing
            // bytes to buffer start.
            byte[] pongFrame = createMaskedFrame(WebSocketOpcode.PONG, new byte[0]);
            byte[] twoFrames = new byte[pongFrame.length + pongFrame.length];
            System.arraycopy(pongFrame, 0, twoFrames, 0, pongFrame.length);
            System.arraycopy(pongFrame, 0, twoFrames, pongFrame.length, pongFrame.length);

            int firstFrameLen = pongFrame.length; // 6 bytes
            int deliverBytes = firstFrameLen + 3;  // full first frame + 3 bytes of second

            MockNetworkFacade mockNf = new MockNetworkFacade(twoFrames);
            mockNf.maxBytesPerRecv = deliverBytes;
            mockNf.wouldBlockAfter = deliverBytes;

            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf,
                    new MockRawSocket(sendBuf, SEND_BUFFER_SIZE),
                    recvBuf, RECV_BUFFER_SIZE
            )) {
                QwpIngressProcessorState state = setupState(httpConfig, context);

                // first PONG processed
                processor.resumeRecv(context);
                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected PeerIsSlowToWriteException");
                } catch (PeerIsSlowToWriteException e) {
                    // expected: second PONG partial → NEED_MORE,
                    // memmove compacts 3 bytes to start, then recv returns 0
                }
                // 3 bytes of the second frame compacted at buffer start
                Assert.assertEquals(3, state.getRecvBufferLen());

                // Deliver remaining bytes to complete the second frame
                mockNf.wouldBlockAfter = Integer.MAX_VALUE;
                mockNf.maxBytesPerRecv = Integer.MAX_VALUE;

                processor.resumeRecv(context);
                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected ServerDisconnectException");
                } catch (ServerDisconnectException e) {
                    // expected: second PONG completed, then recv returns -1
                }
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPartialFrameNeedMore() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            // Create a valid masked PONG frame (6 bytes: 2 header + 4 mask, 0 payload)
            byte[] pongFrame = createMaskedFrame(WebSocketOpcode.PONG, new byte[0]);

            // First recv returns only 3 bytes (partial header).
            // Parser returns consumed=0 / STATE_NEED_MORE. The partial bytes
            // are already at buffer start (pos never advanced), so no memmove
            // is needed — only recvBufferLen is updated.
            MockNetworkFacade mockNf = new MockNetworkFacade(pongFrame);
            mockNf.maxBytesPerRecv = 3;
            // After 3 bytes, return 0 (would block) so we get PeerIsSlowToWrite
            mockNf.wouldBlockAfter = 3;

            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf,
                    new MockRawSocket(sendBuf, SEND_BUFFER_SIZE),
                    recvBuf, RECV_BUFFER_SIZE
            )) {
                QwpIngressProcessorState state = setupState(httpConfig, context);

                processor.resumeRecv(context);
                try {
                    // Read partial frame → NEED_MORE → PeerIsSlowToWrite
                    processor.resumeRecv(context);
                    Assert.fail("Expected PeerIsSlowToWriteException");
                } catch (PeerIsSlowToWriteException e) {
                    // expected: socket returned 0 after partial frame
                }
                // 3 partial bytes tracked at buffer start
                Assert.assertEquals(3, state.getRecvBufferLen());

                // Reset facade: deliver remaining bytes
                mockNf.wouldBlockAfter = Integer.MAX_VALUE;
                mockNf.maxBytesPerRecv = Integer.MAX_VALUE;

                // remaining bytes arrive, PONG frame completes
                processor.resumeRecv(context);
                try {
                    // recv returns -1 (data exhausted) → disconnect
                    processor.resumeRecv(context);
                    Assert.fail("Expected ServerDisconnectException");
                } catch (ServerDisconnectException e) {
                    // expected: frame processed, then socket returns -1
                }
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPingSendFails() throws Exception {
        // When the pong send hits PeerDisconnectedException, handlePing
        // propagates it; resumeRecv's PeerDisconnectedException handler
        // converts it to ServerDisconnectException so the dispatcher tears
        // the connection down on the same recv cycle.
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            byte[] pingFrame = createMaskedFrame(WebSocketOpcode.PING, new byte[]{1, 2, 3});
            MockNetworkFacade mockNf = new MockNetworkFacade(pingFrame);

            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            MockRawSocket mockRawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
            mockRawSocket.throwDisconnectedOnCall = 1; // fail on first send (pong)
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf, mockRawSocket, recvBuf, RECV_BUFFER_SIZE
            )) {
                setupState(httpConfig, context);

                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected ServerDisconnectException");
                } catch (ServerDisconnectException e) {
                    // expected: pong send hit PeerDisconnectedException,
                    // resumeRecv converts to ServerDisconnectException
                }
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPingWhenBufferBusy() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            byte[] pingFrame = createMaskedFrame(WebSocketOpcode.PING, new byte[]{1, 2, 3});
            MockNetworkFacade mockNf = new MockNetworkFacade(pingFrame);

            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            MockRawSocket mockRawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf, mockRawSocket, recvBuf, RECV_BUFFER_SIZE
            )) {
                QwpIngressProcessorState state = setupState(httpConfig, context);
                // Set send state to non-READY so pong is skipped
                state.onAckBlocked(0);

                // handlePing skips pong because buffer is busy
                processor.resumeRecv(context);
                // recv continues, socket returns -1 → disconnect
                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected ServerDisconnectException");
                } catch (ServerDisconnectException e) {
                    // expected: pong skipped, then socket returns -1
                }
                Assert.assertEquals(0, mockRawSocket.sendCallCount);
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPingWhileDurableAckBlockedDoesNotReEnterTrySendDurableAck() throws Exception {
        // Regression for missing isSendReady() guard in
        // QwpWebSocketUpgradeProcessor.flushPendingAck. Before the fix:
        //
        //   if (state.hasPendingAck()) {
        //       trySendAck(...);                       // guarded via hasPendingAck()
        //   }
        //   if (state.isDurableAckEnabled()) {
        //       trySendDurableAck(...);                // BUG: no isSendReady() check
        //   }
        //
        // trySendDurableAck has `assert state.isSendReady()` which fires in
        // -ea (default for mvn test), so entering it while in
        // SEND_STATE_RESUME_DURABLE_ACK throws AssertionError. In production
        // (no -ea), it would run collectDurableProgress and clobber the
        // retained durableProgressSnapshot that onResumeDurableAckComplete
        // depends on to update lastDurableSeqTxns for the in-flight frame.
        //
        // Scenario: a durable-ack send was blocked (state = 4), and the
        // client sends a PING. handlePing -> flushPendingAck -> without the
        // isSendReady guard, the durable branch fires again in a wrong state.
        //
        // Fix:
        //   if (state.isDurableAckEnabled() && state.isSendReady()) { ... }
        //
        // With the fix: flushPendingAck skips both branches (ACK via
        // hasPendingAck, durable via isSendReady). handlePing's own
        // !isSendReady guard then skips the pong. No send happens, state
        // stays at RESUME_DURABLE_ACK until the real drain path resumes it.
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            byte[] pingFrame = createMaskedFrame(WebSocketOpcode.PING, new byte[0]);
            MockNetworkFacade mockNf = new MockNetworkFacade(pingFrame);

            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            MockRawSocket mockRawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf, mockRawSocket, recvBuf, RECV_BUFFER_SIZE
            )) {
                QwpIngressProcessorState state = setupState(httpConfig, context);
                state.setDurableAckEnabled(true);
                // Enter SEND_STATE_RESUME_DURABLE_ACK = 4 to simulate a
                // durable-ack send blocked on OS backpressure.
                state.onDurableAckBlocked();
                Assert.assertEquals(4, state.getSendState());

                // Feed the PING. Without the fix, flushPendingAck calls
                // trySendDurableAck which asserts isSendReady -> AssertionError
                // propagates out of resumeRecv. With the fix, the guard
                // skips the durable branch and the test reaches the
                // ServerDisconnectException below.
                processor.resumeRecv(context);
                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected ServerDisconnectException");
                } catch (ServerDisconnectException e) {
                    // expected: ping handled without re-entering the send
                    // path, recv then returns -1
                }

                Assert.assertEquals(
                        "sendState must stay RESUME_DURABLE_ACK; only the real drain path may change it",
                        4, state.getSendState()
                );
                Assert.assertEquals(
                        "flushPendingAck must not send anything while sendState is RESUME_DURABLE_ACK",
                        0, mockRawSocket.sendCallCount
                );
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testPongFrame() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            byte[] pongFrame = createMaskedFrame(WebSocketOpcode.PONG, new byte[0]);
            MockNetworkFacade mockNf = new MockNetworkFacade(pongFrame);

            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf,
                    new MockRawSocket(sendBuf, SEND_BUFFER_SIZE),
                    recvBuf, RECV_BUFFER_SIZE
            )) {
                setupState(httpConfig, context);

                // PONG is logged and ignored
                processor.resumeRecv(context);
                try {
                    // recv continues, socket returns -1 → disconnect
                    processor.resumeRecv(context);
                    Assert.fail("Expected ServerDisconnectException");
                } catch (ServerDisconnectException e) {
                    // expected: PONG processed, then socket returns -1
                }
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testResumeRecvBufferFull() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            MockNetworkFacade mockNf = new MockNetworkFacade(new byte[0]);
            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf,
                    new MockRawSocket(sendBuf, SEND_BUFFER_SIZE),
                    recvBuf, RECV_BUFFER_SIZE
            )) {
                QwpIngressProcessorState state = setupState(httpConfig, context);
                // Buffer already full — recv guard fires before socket.recv()
                state.setRecvBufferLen(RECV_BUFFER_SIZE);

                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected ServerDisconnectException");
                } catch (ServerDisconnectException e) {
                    // expected: buffer full, frame exceeds capacity
                }
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testResumeRecvNoState() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            MockNetworkFacade mockNf = new MockNetworkFacade(new byte[0]);
            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf,
                    new MockRawSocket(sendBuf, SEND_BUFFER_SIZE),
                    recvBuf, RECV_BUFFER_SIZE
            )) {
                // Don't set state via LV
                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected ServerDisconnectException");
                } catch (ServerDisconnectException e) {
                    // expected: no state
                }
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testResumeSendInvalidState() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (TestableContext context = new TestableContext(
                    httpConfig, new MockNetworkFacade(new byte[0]),
                    new MockRawSocket(sendBuf, SEND_BUFFER_SIZE),
                    0, 0
            )) {
                QwpIngressProcessorState state = setupState(httpConfig, context);
                // Set invalid send state via reflection
                Field sendStateField = QwpIngressProcessorState.class.getDeclaredField("sendState");
                sendStateField.setAccessible(true);
                sendStateField.setInt(state, 99);

                try {
                    processor.resumeSend(context);
                    Assert.fail("Expected ServerDisconnectException");
                } catch (ServerDisconnectException e) {
                    // expected: invalid send state
                }
            } finally {
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testResumeSendResumeError() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            MockRawSocket mockRawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(
                    httpConfig, new MockNetworkFacade(new byte[0]),
                    mockRawSocket, 0, 0
            )) {
                QwpIngressProcessorState state = setupState(httpConfig, context);
                // Set SEND_STATE_RESUME_ERROR (error blocked without prior ACK block)
                state.onErrorBlocked((byte) 5, 3L, "test error");

                Assert.assertTrue(state.isSending());
                Assert.assertEquals(3L, state.getDeferredErrorSequence());
                Assert.assertEquals(5, state.getDeferredErrorStatus());

                processor.resumeSend(context);

                Assert.assertTrue(context.isResumeResponseSendCalled());
                Assert.assertTrue(state.isSendReady());
            } finally {
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testStateReuse() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpIngressUpgradeProcessor processor = new QwpIngressUpgradeProcessor(engine, httpConfig);

            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            MockRawSocket mockRawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (
                    MockHttpRequestHeader header = new MockHttpRequestHeader();
                    TestableContext context = new TestableContext(
                            httpConfig, new MockNetworkFacade(new byte[0]),
                            mockRawSocket, header, 0, 0
                    )
            ) {
                header.setHeader("Upgrade", "websocket");
                header.setHeader("Connection", "Upgrade");
                header.setHeader("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");
                header.setHeader("Sec-WebSocket-Version", "13");

                // First call creates state. onHeadersReady stages the 101 bytes;
                // the actual send + protocol switch happens in onRequestComplete.
                processor.onHeadersReady(context);
                Assert.assertFalse("switchProtocol must wait for onRequestComplete",
                        context.isSwitchProtocolCalled());
                processor.onRequestComplete(context);
                Assert.assertTrue(context.isSwitchProtocolCalled());

                LocalValue<QwpIngressProcessorState> lv = getLV();
                QwpIngressProcessorState state = lv.get(context);
                Assert.assertNotNull(state);

                // Reset context for second call
                context.resetSwitchProtocolCalled();
                mockRawSocket.reset();

                // Second call reuses (clears) existing state. Same two-step
                // lifecycle: onHeadersReady stages, onRequestComplete flushes.
                processor.onHeadersReady(context);
                Assert.assertFalse(context.isSwitchProtocolCalled());
                processor.onRequestComplete(context);
                Assert.assertTrue(context.isSwitchProtocolCalled());

                QwpIngressProcessorState state2 = lv.get(context);
                Assert.assertSame(state, state2);
            } finally {
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static HttpFullFatServerConfiguration createHttpConfiguration(long[] nowMicros) {
        LineHttpProcessorConfiguration lineConfig =
                new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration) {
                    @Override
                    public MicrosecondClock getMicrosecondClock() {
                        return () -> nowMicros[0];
                    }
                };
        return new DefaultHttpServerConfiguration(configuration) {
            @Override
            public LineHttpProcessorConfiguration getLineHttpProcessorConfiguration() {
                return lineConfig;
            }
        };
    }

    private static HttpFullFatServerConfiguration createHttpConfiguration(long[] nowMicros, int recvBufferSize) {
        HttpFullFatServerConfiguration delegate = createHttpConfiguration(nowMicros);
        return new DefaultHttpServerConfiguration(configuration) {
            @Override
            public LineHttpProcessorConfiguration getLineHttpProcessorConfiguration() {
                return delegate.getLineHttpProcessorConfiguration();
            }

            @Override
            public int getRecvBufferSize() {
                return recvBufferSize;
            }
        };
    }

    /**
     * Frozen clock that also counts how many times the processor reads it.
     * Every deadline this path evaluates -- the close-echo grace, the
     * role-change deferral, the post-CLOSE drain -- goes through
     * {@code LineHttpProcessorConfiguration.getMicrosecondClock()}, so
     * {@code clockReads} is an exact operation count of the
     * {@code Os.currentTimeMicros()} JNI transitions production would make.
     */
    private static HttpFullFatServerConfiguration createHttpConfiguration(long[] nowMicros, long[] clockReads, int recvBufferSize) {
        MicrosecondClock countingClock = () -> {
            clockReads[0]++;
            return nowMicros[0];
        };
        LineHttpProcessorConfiguration lineConfig =
                new DefaultHttpServerConfiguration.DefaultLineHttpProcessorConfiguration(configuration) {
                    @Override
                    public MicrosecondClock getMicrosecondClock() {
                        return countingClock;
                    }
                };
        return new DefaultHttpServerConfiguration(configuration) {
            @Override
            public LineHttpProcessorConfiguration getLineHttpProcessorConfiguration() {
                return lineConfig;
            }

            @Override
            public int getRecvBufferSize() {
                return recvBufferSize;
            }
        };
    }

    private static byte[] createMaskedFrame(int opcode, byte[] payload) {
        int payloadLen = payload.length;
        int headerLen;

        if (payloadLen <= 125) {
            headerLen = 2 + 4; // 2 byte header + 4 byte mask
        } else if (payloadLen <= 65_535) {
            headerLen = 4 + 4;
        } else {
            headerLen = 10 + 4;
        }

        byte[] frame = new byte[headerLen + payloadLen];
        int offset = 0;

        frame[offset++] = (byte) (0x80 | (opcode & 0x0F));

        if (payloadLen <= 125) {
            frame[offset++] = (byte) (0x80 | payloadLen);
        } else if (payloadLen <= 65_535) {
            frame[offset++] = (byte) (0x80 | 126);
            frame[offset++] = (byte) ((payloadLen >> 8) & 0xFF);
            frame[offset++] = (byte) (payloadLen & 0xFF);
        } else {
            frame[offset++] = (byte) (0x80 | 127);
            for (int i = 7; i >= 0; i--) {
                frame[offset++] = (byte) (((long) payloadLen >> (i * 8)) & 0xFF);
            }
        }

        System.arraycopy(DEFAULT_MASK_KEY, 0, frame, offset, 4);
        offset += 4;

        for (int i = 0; i < payloadLen; i++) {
            frame[offset + i] = (byte) (payload[i] ^ DEFAULT_MASK_KEY[i % 4]);
        }

        return frame;
    }

    private static int getCloseDrainMaxRecvPerDispatch() throws Exception {
        Field f = QwpIngressUpgradeProcessor.class.getDeclaredField("CLOSE_DRAIN_MAX_RECV_PER_DISPATCH");
        f.setAccessible(true);
        return f.getInt(null);
    }

    @SuppressWarnings("unchecked")
    private static LocalValue<QwpIngressProcessorState> getLV() throws Exception {
        Field lvField = QwpIngressUpgradeProcessor.class.getDeclaredField("LV");
        lvField.setAccessible(true);
        return (LocalValue<QwpIngressProcessorState>) lvField.get(null);
    }

    /**
     * The client's CLOSE echo of the server's ROLE_CHANGE close code, masked
     * like every client frame. Only this code completes the close-echo wait:
     * the client can have learned it only by reading the server's CLOSE.
     */
    private static byte[] roleChangeCloseEchoFrame() {
        return createMaskedFrame(WebSocketOpcode.CLOSE, new byte[]{
                (byte) (WebSocketCloseCode.ROLE_CHANGE >>> 8),
                (byte) WebSocketCloseCode.ROLE_CHANGE
        });
    }

    private static QwpIngressProcessorState setupState(
            HttpFullFatServerConfiguration httpConfig,
            TestableContext context
    ) throws Exception {
        LocalValue<QwpIngressProcessorState> lv = getLV();
        QwpIngressProcessorState state = new QwpIngressProcessorState(
                RECV_BUFFER_SIZE,
                httpConfig.getSendBufferSize(),
                engine,
                httpConfig.getLineHttpProcessorConfiguration()
        );
        state.of(-1, AllowAllSecurityContext.INSTANCE);
        lv.set(context, state);
        return state;
    }

    /**
     * Mock HttpRequestHeader — same pattern as in
     * {@code QwpWebSocketUpgradeProcessorOnHeadersReadyTest.MockHttpRequestHeader}.
     */
    private static class MockHttpRequestHeader implements HttpRequestHeader, AutoCloseable {
        private final ObjList<Long> allocatedMemory = new ObjList<>();
        private final ObjList<Utf8String> headerNames = new ObjList<>();
        private final ObjList<DirectUtf8String> headerValues = new ObjList<>();

        @Override
        public void close() {
            for (int i = 0; i < allocatedMemory.size(); i += 2) {
                long ptr = allocatedMemory.get(i);
                long len = allocatedMemory.get(i + 1);
                Unsafe.free(ptr, len, MemoryTag.NATIVE_DEFAULT);
            }
            allocatedMemory.clear();
            headerNames.clear();
            headerValues.clear();
        }

        @Override
        public DirectUtf8Sequence getBoundary() {
            return null;
        }

        @Override
        public DirectUtf8Sequence getCharset() {
            return null;
        }

        @Override
        public DirectUtf8Sequence getContentDisposition() {
            return null;
        }

        @Override
        public DirectUtf8Sequence getContentDispositionFilename() {
            return null;
        }

        @Override
        public DirectUtf8Sequence getContentDispositionName() {
            return null;
        }

        @Override
        public long getContentLength() {
            return -1;
        }

        @Override
        public DirectUtf8Sequence getContentType() {
            return null;
        }

        @Override
        public DirectUtf8Sequence getHeader(Utf8Sequence name) {
            for (int i = 0; i < headerNames.size(); i++) {
                if (name.toString().equalsIgnoreCase(headerNames.get(i).toString())) {
                    return headerValues.get(i);
                }
            }
            return null;
        }

        @Override
        public ObjList<? extends Utf8Sequence> getHeaderNames() {
            return headerNames;
        }

        @Override
        public DirectUtf8Sequence getMethod() {
            return null;
        }

        @Override
        public DirectUtf8Sequence getMethodLine() {
            return null;
        }

        @Override
        public @Nullable DirectUtf8String getQuery() {
            return null;
        }

        @Override
        public long getStatementTimeout() {
            return 0;
        }

        @Override
        public DirectUtf8String getUrl() {
            return null;
        }

        @Override
        public DirectUtf8Sequence getUrlParam(Utf8Sequence name) {
            return null;
        }

        @Override
        public boolean isGetRequest() {
            return false;
        }

        @Override
        public boolean isPostRequest() {
            return false;
        }

        @Override
        public boolean isPutRequest() {
            return false;
        }

        void setHeader(String name, String value) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            long ptr = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.putByte(ptr + i, bytes[i]);
            }
            allocatedMemory.add(ptr);
            allocatedMemory.add((long) bytes.length);

            DirectUtf8String directValue = new DirectUtf8String().of(ptr, ptr + bytes.length);
            for (int i = 0; i < headerNames.size(); i++) {
                if (name.equalsIgnoreCase(headerNames.get(i).toString())) {
                    headerValues.set(i, directValue);
                    return;
                }
            }
            headerNames.add(new Utf8String(name));
            headerValues.add(directValue);
        }
    }

    /**
     * NetworkFacade simulating a peer that keeps the socket continuously
     * readable: every non-blocking recv returns a full buffer of data until
     * {@link #wouldBlock} (returns 0) or {@link #closed} (returns -1) is set.
     * Used to exercise the post-CLOSE drain's per-dispatch quantum without
     * relying on wall-clock timing.
     */
    private static class AlwaysReadableNetworkFacade extends MockNetworkFacade {
        boolean closed;
        int firstRequestSize = -1;
        final long[] nowMicros;
        int recvCount;
        int secondRequestSize = -1;
        long totalBytesReceived;
        boolean wouldBlock;

        AlwaysReadableNetworkFacade() {
            this(null);
        }

        AlwaysReadableNetworkFacade(long[] nowMicros) {
            super(new byte[0]);
            this.nowMicros = nowMicros;
        }

        @Override
        public int recvRaw(long fd, long buffer, int bufferLen) {
            recvCount++;
            if (recvCount == 1) {
                firstRequestSize = bufferLen;
            } else if (recvCount == 2) {
                secondRequestSize = bufferLen;
            }
            if (closed) {
                return -1;
            }
            if (wouldBlock) {
                return 0;
            }
            // The drain discards these bytes unread, so we need not write them.
            totalBytesReceived += bufferLen;
            if (nowMicros != null && recvCount == 1) {
                nowMicros[0] = QwpIngressProcessorState.CLOSE_DRAIN_TIMEOUT_MICROS;
            }
            return bufferLen;
        }
    }

    /**
     * Mock NetworkFacade for controlling recv behavior.
     * Follows the pattern from {@code BaseLineTcpContextTest.LineTcpNetworkFacade}.
     */
    private static class MockNetworkFacade extends NetworkFacadeImpl {
        private final byte[] data;
        int disconnectAfter = Integer.MAX_VALUE;
        int maxBytesPerRecv = Integer.MAX_VALUE;
        int wouldBlockAfter = Integer.MAX_VALUE;
        private int pos;

        MockNetworkFacade(byte[] data) {
            this.data = data;
        }

        @Override
        public void close(long fd, Log log) {
            // no-op for test
        }

        @Override
        public int recvRaw(long fd, long buffer, int bufferLen) {
            if (pos >= data.length || pos >= disconnectAfter) {
                return -1;
            }
            if (pos >= wouldBlockAfter) {
                return 0;
            }
            int available = data.length - pos;
            int n = Math.min(bufferLen, Math.min(available, maxBytesPerRecv));
            for (int i = 0; i < n; i++) {
                Unsafe.putByte(buffer + i, data[pos++]);
            }
            return n;
        }
    }

    /**
     * Mock HttpRawSocket for controlling send behavior.
     * Same pattern as in {@code QwpWebSocketUpgradeProcessorOnHeadersReadyTest.MockRawSocket}
     * and {@code QwpWebSocketUpgradeProcessorResumeSendTest.MockRawSocket}, extended
     * with configurable exception throwing.
     */
    private static class MockRawSocket implements HttpRawSocket {
        private final long bufferAddress;
        private final int bufferSize;
        int sendCallCount;
        int sentSize;
        int throwDisconnectedOnCall = -1;
        int throwSlowToReadOnCall = -1;

        MockRawSocket(long bufferAddress, int bufferSize) {
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
        public void send(int size) throws PeerDisconnectedException, PeerIsSlowToReadException {
            sendCallCount++;
            if (sendCallCount == throwSlowToReadOnCall) {
                throw PeerIsSlowToReadException.INSTANCE;
            }
            if (sendCallCount == throwDisconnectedOnCall) {
                throw PeerDisconnectedException.INSTANCE;
            }
            sentSize = size;
        }

        void reset() {
            sendCallCount = 0;
            sentSize = 0;
        }
    }

    /**
     * {@link MockNetworkFacade} that records how many receives a dispatch
     * issued and how many bytes each one requested -- the operation counts
     * the close-echo read cap is asserted through.
     */
    private static class RecordingNetworkFacade extends MockNetworkFacade {
        int firstRecvRequestSize = -1;
        int recvCalls;
        int secondRecvRequestSize = -1;

        RecordingNetworkFacade(byte[] data) {
            super(data);
        }

        @Override
        public int recvRaw(long fd, long buffer, int bufferLen) {
            recvCalls++;
            if (recvCalls == 1) {
                firstRecvRequestSize = bufferLen;
            } else if (recvCalls == 2) {
                secondRecvRequestSize = bufferLen;
            }
            return super.recvRaw(fd, buffer, bufferLen);
        }
    }

    /**
     * Test HTTP connection context that overrides I/O access points.
     * Combines patterns from both existing test classes.
     */
    private static class TestableContext extends HttpConnectionContext {
        private final MockRawSocket rawSocket;
        private final MockHttpRequestHeader requestHeader;
        private final long testRecvBuffer;
        private final int testRecvBufferSize;
        private boolean resumeResponseSendCalled;
        private boolean switchProtocolCalled;

        TestableContext(
                HttpServerConfiguration config,
                MockNetworkFacade mockNf,
                MockRawSocket rawSocket,
                long recvBuffer,
                int recvBufferSize
        ) {
            this(config, mockNf, rawSocket, null, recvBuffer, recvBufferSize);
        }

        TestableContext(
                HttpServerConfiguration config,
                MockNetworkFacade mockNf,
                MockRawSocket rawSocket,
                MockHttpRequestHeader requestHeader,
                long recvBuffer,
                int recvBufferSize
        ) {
            super(config, (_, log) -> new PlainSocket(mockNf, log));
            this.rawSocket = rawSocket;
            this.requestHeader = requestHeader;
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

        @Override
        public HttpRequestHeader getRequestHeader() {
            return requestHeader != null ? requestHeader : super.getRequestHeader();
        }

        @Override
        public void resumeResponseSend() {
            resumeResponseSendCalled = true;
        }

        @Override
        public void switchProtocol() {
            switchProtocolCalled = true;
        }

        boolean isResumeResponseSendCalled() {
            return resumeResponseSendCalled;
        }

        boolean isSwitchProtocolCalled() {
            return switchProtocolCalled;
        }

        void resetSwitchProtocolCalled() {
            switchProtocolCalled = false;
        }
    }
}
