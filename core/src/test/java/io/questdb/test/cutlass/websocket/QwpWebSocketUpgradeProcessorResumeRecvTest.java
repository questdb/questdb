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
import io.questdb.cutlass.http.HttpException;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRawSocket;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.cutlass.qwp.server.QwpProcessorState;
import io.questdb.cutlass.qwp.server.QwpWebSocketUpgradeProcessor;
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
import io.questdb.std.Utf8SequenceObjHashMap;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;

/**
 * Tests for {@link QwpWebSocketUpgradeProcessor} covering network edge cases:
 * partial reads, oversized frames, send backpressure, buffer-too-small, and
 * state machine transitions that are hard to trigger from E2E tests.
 * <p>
 * Uses {@link MockNetworkFacade} (extending {@link NetworkFacadeImpl}) for recv
 * control and {@link MockRawSocket} for send control, following the established
 * patterns from {@code BaseLineTcpContextTest.LineTcpNetworkFacade} and
 * {@code QwpWebSocketUpgradeProcessorOnHeadersReadyTest.MockRawSocket}.
 */
public class QwpWebSocketUpgradeProcessorResumeRecvTest extends AbstractCairoTest {
    private static final byte[] DEFAULT_MASK_KEY = {0x12, 0x34, 0x56, 0x78};
    private static final int RECV_BUFFER_SIZE = 1024;
    private static final int SEND_BUFFER_SIZE = 256;

    @Test
    public void testAckBlocked() throws Exception {
        // When a PING arrives and the ACK flush gets PeerIsSlowToReadException,
        // handlePing swallows it (same pattern as handleClose). State ends up
        // in RESUME_ACK, pong is skipped, and resumeRecv completes normally.
        // recv returns -1 on the next call and throws ServerDisconnectException.
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

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
                QwpProcessorState state = setupState(httpConfig, context);
                // Set up pending ACK: highestProcessed > lastAcked
                state.setHighestProcessedSequence(5);

                processor.resumeRecv(context);
                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected ServerDisconnectException");
                } catch (ServerDisconnectException e) {
                    // expected: ack backpressure swallowed, recv returns -1
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
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

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
                QwpProcessorState state = setupState(httpConfig, context);
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
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

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

                try {
                    processor.onHeadersReady(context);
                    Assert.fail("Expected HttpException");
                } catch (HttpException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "WebSocket handshake rejected");
                }
            } finally {
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCloseWhenBufferBusy() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

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
                QwpProcessorState state = setupState(httpConfig, context);
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
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

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
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

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
                QwpProcessorState state = setupState(httpConfig, context);

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
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

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
                QwpProcessorState state = setupState(httpConfig, context);
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
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

            byte[] garbageBinary = createMaskedFrame(WebSocketOpcode.BINARY, new byte[]{0x00, 0x01, 0x02});
            MockNetworkFacade mockNf = new MockNetworkFacade(garbageBinary);

            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            MockRawSocket mockRawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf, mockRawSocket, recvBuf, RECV_BUFFER_SIZE
            )) {
                QwpProcessorState state = setupState(httpConfig, context);
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
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

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
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

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
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

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
                QwpProcessorState state = setupState(httpConfig, context);
                // Set send state to non-READY so CLOSE frame is skipped
                state.onAckBlocked(0);

                try {
                    processor.resumeRecv(context);
                    Assert.fail("Expected ServerDisconnectException");
                } catch (ServerDisconnectException e) {
                    // expected
                }
                // CLOSE frame skipped because send buffer busy
                Assert.assertEquals(0, mockRawSocket.sendCallCount);
            } finally {
                Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testHandshakeSendSlowToRead() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

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

                try {
                    processor.onHeadersReady(context);
                    Assert.fail("Expected HttpException");
                } catch (HttpException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "blocked");
                }
            } finally {
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testOnClosedInvalidState() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (TestableContext context = new TestableContext(
                    httpConfig, new MockNetworkFacade(new byte[0]),
                    new MockRawSocket(sendBuf, SEND_BUFFER_SIZE),
                    0, 0
            )) {
                QwpProcessorState state = setupState(httpConfig, context);
                Field sendStateField = QwpProcessorState.class.getDeclaredField("sendState");
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
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (TestableContext context = new TestableContext(
                    httpConfig, new MockNetworkFacade(new byte[0]),
                    new MockRawSocket(sendBuf, SEND_BUFFER_SIZE),
                    0, 0
            )) {
                QwpProcessorState state = setupState(httpConfig, context);
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
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            MockRawSocket mockRawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(
                    httpConfig, new MockNetworkFacade(new byte[0]),
                    mockRawSocket, 0, 0
            )) {
                QwpProcessorState state = setupState(httpConfig, context);
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
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (TestableContext context = new TestableContext(
                    httpConfig, new MockNetworkFacade(new byte[0]),
                    new MockRawSocket(sendBuf, SEND_BUFFER_SIZE),
                    0, 0
            )) {
                QwpProcessorState state = setupState(httpConfig, context);
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
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

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
                QwpProcessorState state = setupState(httpConfig, context);

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
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

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
                QwpProcessorState state = setupState(httpConfig, context);

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
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

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

                // handlePing catches the PeerDisconnectedException internally
                processor.resumeRecv(context);
                try {
                    // recv continues, socket returns -1 → disconnect
                    processor.resumeRecv(context);
                    Assert.fail("Expected ServerDisconnectException");
                } catch (ServerDisconnectException e) {
                    // expected: pong failure swallowed, then socket returns -1
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
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

            byte[] pingFrame = createMaskedFrame(WebSocketOpcode.PING, new byte[]{1, 2, 3});
            MockNetworkFacade mockNf = new MockNetworkFacade(pingFrame);

            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            MockRawSocket mockRawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf, mockRawSocket, recvBuf, RECV_BUFFER_SIZE
            )) {
                QwpProcessorState state = setupState(httpConfig, context);
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
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

            byte[] pingFrame = createMaskedFrame(WebSocketOpcode.PING, new byte[0]);
            MockNetworkFacade mockNf = new MockNetworkFacade(pingFrame);

            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            MockRawSocket mockRawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf, mockRawSocket, recvBuf, RECV_BUFFER_SIZE
            )) {
                QwpProcessorState state = setupState(httpConfig, context);
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
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

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
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

            MockNetworkFacade mockNf = new MockNetworkFacade(new byte[0]);
            long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (TestableContext context = new TestableContext(
                    httpConfig, mockNf,
                    new MockRawSocket(sendBuf, SEND_BUFFER_SIZE),
                    recvBuf, RECV_BUFFER_SIZE
            )) {
                QwpProcessorState state = setupState(httpConfig, context);
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
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

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
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (TestableContext context = new TestableContext(
                    httpConfig, new MockNetworkFacade(new byte[0]),
                    new MockRawSocket(sendBuf, SEND_BUFFER_SIZE),
                    0, 0
            )) {
                QwpProcessorState state = setupState(httpConfig, context);
                // Set invalid send state via reflection
                Field sendStateField = QwpProcessorState.class.getDeclaredField("sendState");
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
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

            long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            MockRawSocket mockRawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
            try (TestableContext context = new TestableContext(
                    httpConfig, new MockNetworkFacade(new byte[0]),
                    mockRawSocket, 0, 0
            )) {
                QwpProcessorState state = setupState(httpConfig, context);
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
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

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

                // First call creates state
                processor.onHeadersReady(context);
                Assert.assertTrue(context.isSwitchProtocolCalled());

                LocalValue<QwpProcessorState> lv = getLV();
                QwpProcessorState state = lv.get(context);
                Assert.assertNotNull(state);

                // Reset context for second call
                context.resetSwitchProtocolCalled();
                mockRawSocket.reset();

                // Second call reuses (clears) existing state
                processor.onHeadersReady(context);
                Assert.assertTrue(context.isSwitchProtocolCalled());

                QwpProcessorState state2 = lv.get(context);
                Assert.assertSame(state, state2);
            } finally {
                Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
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

    @SuppressWarnings("unchecked")
    private static LocalValue<QwpProcessorState> getLV() throws Exception {
        Field lvField = QwpWebSocketUpgradeProcessor.class.getDeclaredField("LV");
        lvField.setAccessible(true);
        return (LocalValue<QwpProcessorState>) lvField.get(null);
    }

    private static QwpProcessorState setupState(
            HttpFullFatServerConfiguration httpConfig,
            TestableContext context
    ) throws Exception {
        LocalValue<QwpProcessorState> lv = getLV();
        QwpProcessorState state = new QwpProcessorState(
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
        public Utf8SequenceObjHashMap<DirectUtf8String> getUrlParams() {
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
