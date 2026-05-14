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
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.cutlass.qwp.server.egress.QwpEgressProcessorState;
import io.questdb.cutlass.qwp.server.egress.QwpEgressUpgradeProcessor;
import io.questdb.log.Log;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.PlainSocket;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

/**
 * Tests that {@link QwpEgressUpgradeProcessor} emits a protocol-level CLOSE
 * frame before tearing the connection down on irrecoverable WebSocket-layer
 * errors. Without the CLOSE the client cannot distinguish "frame too big" or
 * "protocol violation" from a generic network failure.
 */
public class QwpEgressUpgradeProcessorResumeRecvTest extends AbstractCairoTest {

    private static final int RECV_BUFFER_SIZE = 4096;
    private static final int SEND_BUFFER_SIZE = 4096;

    @Test
    public void testFrameTooLargeForRecvBufferSendsCloseFrame() throws Exception {
        // Regression: resumeRecv hits the recvBufferLen >= recvBufferSize
        // branch when the parser still needs more bytes but the recv buffer is
        // saturated. The current code throws ServerDisconnect without any
        // CLOSE frame, so the client sees ECONNRESET with no diagnostic. After
        // the fix the server should emit a 1009 CLOSE before tearing down.
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            try (QwpEgressUpgradeProcessor processor = new QwpEgressUpgradeProcessor(engine, httpConfig, 1)) {
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                MockRawSocket mockRawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
                try (TestableContext context = new TestableContext(
                        httpConfig, new MockNetworkFacade(new byte[0]),
                        mockRawSocket, recvBuf, RECV_BUFFER_SIZE
                )) {
                    QwpEgressProcessorState state = setupState(context);
                    // Mark the recv buffer as full so the "frame too large for
                    // recv buffer" branch fires immediately.
                    state.setRecvBufferLen(RECV_BUFFER_SIZE);

                    try {
                        processor.resumeRecv(context);
                        Assert.fail("Expected ServerDisconnectException");
                    } catch (ServerDisconnectException e) {
                        // expected
                    }
                    Assert.assertTrue(
                            "Egress must emit a CLOSE frame before disconnect",
                            mockRawSocket.sendCallCount >= 1
                    );
                    int headerSize = (Unsafe.getByte(sendBuf + 1) & 0x7F) <= 125 ? 2 : 4;
                    int hi = Unsafe.getByte(sendBuf + headerSize) & 0xFF;
                    int lo = Unsafe.getByte(sendBuf + headerSize + 1) & 0xFF;
                    Assert.assertEquals(
                            "CLOSE frame must carry code 1009 (MESSAGE_TOO_BIG)",
                            1009, (hi << 8) | lo
                    );
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @SuppressWarnings("unchecked")
    private static LocalValue<QwpEgressProcessorState> getLV() throws Exception {
        Field lvField = QwpEgressUpgradeProcessor.class.getDeclaredField("LV");
        lvField.setAccessible(true);
        return (LocalValue<QwpEgressProcessorState>) lvField.get(null);
    }

    private static QwpEgressProcessorState setupState(TestableContext context) throws Exception {
        LocalValue<QwpEgressProcessorState> lv = getLV();
        QwpEgressProcessorState state = new QwpEgressProcessorState(configuration);
        state.of(-1, AllowAllSecurityContext.INSTANCE);
        lv.set(context, state);
        return state;
    }

    private static class MockNetworkFacade extends NetworkFacadeImpl {
        private final byte[] data;
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
            if (pos >= data.length) {
                return -1;
            }
            int available = data.length - pos;
            int n = Math.min(bufferLen, available);
            for (int i = 0; i < n; i++) {
                Unsafe.putByte(buffer + i, data[pos++]);
            }
            return n;
        }
    }

    private static class MockRawSocket implements HttpRawSocket {
        private final long bufferAddress;
        private final int bufferSize;
        int sendCallCount;
        int sentSize;

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
        public void send(int size) {
            sendCallCount++;
            sentSize = size;
        }
    }

    private static class TestableContext extends HttpConnectionContext {
        private final MockRawSocket rawSocket;
        private final long testRecvBuffer;
        private final int testRecvBufferSize;

        TestableContext(
                HttpServerConfiguration config,
                MockNetworkFacade mockNf,
                MockRawSocket rawSocket,
                long recvBuffer,
                int recvBufferSize
        ) {
            super(config, (_, log) -> new PlainSocket(mockNf, log));
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
