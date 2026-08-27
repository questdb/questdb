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

import io.questdb.PropertyKey;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpRawSocket;
import io.questdb.cutlass.http.HttpServerConfiguration;
import io.questdb.cutlass.http.LocalValue;
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.server.egress.QwpEgressProcessorState;
import io.questdb.cutlass.qwp.server.egress.QwpEgressUpgradeProcessor;
import io.questdb.log.Log;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PlainSocket;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.metrics.QueryTrace;
import io.questdb.mp.ConcurrentQueue;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;

/**
 * Tests that {@link QwpEgressUpgradeProcessor} emits a protocol-level CLOSE
 * frame before tearing the connection down on irrecoverable WebSocket-layer
 * errors. Without the CLOSE the client cannot distinguish "frame too big" or
 * "protocol violation" from a generic network failure.
 */
public class QwpEgressUpgradeProcessorResumeRecvTest extends AbstractCairoTest {

    private static final int RECV_BUFFER_SIZE = 4096;
    private static final int SEND_BUFFER_SIZE = 4096;

    @Before
    public void enableQueryTracing() {
        node1.getConfigurationOverrides().setProperty(PropertyKey.QUERY_TRACING_ENABLED, true);
    }

    @Test
    public void testInitialStreamingPISRCountsUntilClose() throws Exception {
        // A fake transport is permitted here solely to force PISR. The assertion is on
        // QueryProgress's stable trace output, not on an assumed client response.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE qwp_socket_timer AS (SELECT x, x::TIMESTAMP ts FROM long_sequence(50_000)) TIMESTAMP(ts) PARTITION BY DAY");
            final ConcurrentQueue<QueryTrace> queue = engine.getMessageBus().getQueryTraceQueue();
            drain(queue);

            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            try (QwpEgressUpgradeProcessor processor = new QwpEgressUpgradeProcessor(engine, httpConfig, 1)) {
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                try {
                    long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    try {
                        MockRawSocket rawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
                        rawSocket.setThrowPeerSlow(true);
                        String query = "SELECT * FROM qwp_socket_timer";
                        try (TestableContext context = new TestableContext(
                                httpConfig,
                                new MockNetworkFacade(buildMaskedQueryFrame(query)),
                                rawSocket,
                                recvBuf,
                                RECV_BUFFER_SIZE
                        )) {
                            QwpEgressProcessorState state = setupState(context);
                            currentMicros = 1_000;
                            try {
                                processor.resumeRecv(context);
                                Assert.fail("Expected PeerIsSlowToReadException");
                            } catch (PeerIsSlowToReadException expected) {
                                // The initial result send parked while state retained its live cursor.
                            }
                            Assert.assertTrue(state.isStreamingActive());

                            currentMicros = 2_000;
                            state.endStreaming();
                            QueryTrace trace = new QueryTrace();
                            Assert.assertTrue(queue.tryDequeue(trace));
                            Assert.assertEquals(query, trace.queryText);
                            Assert.assertEquals(1_000_000L, trace.waitNanos);
                            Assert.assertEquals(1_000_000L, trace.executionNanos);
                        }
                    } finally {
                        Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testCreditResumeForwardsTimer() throws Exception {
        // Transport fault injection proves that matching CREDIT resumes the retained
        // cursor before streamResults re-parks it on PeerIsSlowToReadException.
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            try (QwpEgressUpgradeProcessor processor = new QwpEgressUpgradeProcessor(engine, httpConfig, 1)) {
                long recvBuf = Unsafe.malloc(RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                try {
                    long sendBuf = Unsafe.malloc(SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    try {
                        MockRawSocket rawSocket = new MockRawSocket(sendBuf, SEND_BUFFER_SIZE);
                        try (TestableContext context = new TestableContext(
                                httpConfig,
                                new MockNetworkFacade(buildMaskedCreditFrame(1, 1_000_000)),
                                rawSocket,
                                recvBuf,
                                RECV_BUFFER_SIZE
                        )) {
                            rawSocket.setThrowPeerSlow(true);
                            QwpEgressProcessorState state = setupState(context);
                            TimerSpyRecordCursor cursor = new TimerSpyRecordCursor();
                            state.beginStreaming(1, null, cursor, 0, 1, null);
                            state.consumeStreamingCredit(1);
                            state.markStreamingCreditSuspended();
                            state.suspendStreamingTimer();

                            try {
                                processor.resumeRecv(context);
                                Assert.fail("Expected PeerIsSlowToReadException");
                            } catch (PeerIsSlowToReadException expected) {
                                // Expected: streamResults re-parked after matching CREDIT resumed it.
                            }

                            Assert.assertEquals(1, cursor.resumeCalls);
                            Assert.assertEquals(2, cursor.suspendCalls);
                            Assert.assertTrue(state.isStreamingActive());
                            state.endStreaming();
                            Assert.assertTrue(cursor.isClosed);
                        }
                    } finally {
                        Unsafe.free(sendBuf, SEND_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    Unsafe.free(recvBuf, RECV_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

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

    private static byte[] buildMaskedCreditFrame(long requestId, long credit) {
        byte[] payload = new byte[1 + Long.BYTES + 3];
        int p = 0;
        payload[p++] = QwpEgressMsgKind.CREDIT;
        for (int i = 0; i < Long.BYTES; i++) {
            payload[p++] = (byte) (requestId >>> (i * Byte.SIZE));
        }
        while ((credit & ~0x7fL) != 0) {
            payload[p++] = (byte) ((credit & 0x7f) | 0x80);
            credit >>>= 7;
        }
        payload[p++] = (byte) credit;
        return maskBinaryFrame(payload, p);
    }

    private static byte[] buildMaskedQueryFrame(String query) {
        byte[] sql = query.getBytes(StandardCharsets.UTF_8);
        Assert.assertTrue(sql.length < 128);
        byte[] payload = new byte[1 + Long.BYTES + 1 + sql.length + 2];
        int p = 0;
        payload[p++] = QwpEgressMsgKind.QUERY_REQUEST;
        for (int i = 0; i < Long.BYTES; i++) {
            payload[p++] = (byte) (1L >>> (i * Byte.SIZE));
        }
        payload[p++] = (byte) sql.length;
        System.arraycopy(sql, 0, payload, p, sql.length);
        p += sql.length;
        payload[p++] = 0; // initial_credit
        payload[p] = 0; // bind_count

        return maskBinaryFrame(payload, payload.length);
    }

    private static byte[] maskBinaryFrame(byte[] payload, int length) {
        Assert.assertTrue(length < 126);
        byte[] frame = new byte[2 + 4 + length];
        frame[0] = (byte) 0x82; // FIN + BINARY
        frame[1] = (byte) (0x80 | length);
        frame[2] = 1;
        frame[3] = 2;
        frame[4] = 3;
        frame[5] = 4;
        for (int i = 0; i < length; i++) {
            frame[6 + i] = (byte) (payload[i] ^ frame[2 + i % 4]);
        }
        return frame;
    }

    private static void drain(ConcurrentQueue<QueryTrace> queue) {
        QueryTrace trace = new QueryTrace();
        while (queue.tryDequeue(trace)) {
        }
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

    private static class TimerSpyRecordCursor implements RecordCursor {
        private boolean isClosed;
        private int resumeCalls;
        private int suspendCalls;

        @Override
        public void close() {
            isClosed = true;
        }

        @Override
        public Record getRecord() {
            return null;
        }

        @Override
        public Record getRecordB() {
            return null;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
        }

        @Override
        public void resumeTimer() {
            resumeCalls++;
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public void suspendTimer() {
            suspendCalls++;
        }

        @Override
        public void toTop() {
        }
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
        private boolean isThrowPeerSlow;

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
        public void send(int size) throws PeerIsSlowToReadException {
            sendCallCount++;
            sentSize = size;
            if (isThrowPeerSlow) {
                throw PeerIsSlowToReadException.INSTANCE;
            }
        }

        void setThrowPeerSlow(boolean isThrowPeerSlow) {
            this.isThrowPeerSlow = isThrowPeerSlow;
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
        public SecurityContext getSecurityContext() {
            return AllowAllSecurityContext.INSTANCE;
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
