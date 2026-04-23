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
import io.questdb.cutlass.qwp.server.QwpProcessorState;
import io.questdb.cutlass.qwp.server.QwpWebSocketUpgradeProcessor;
import io.questdb.network.PlainSocketFactory;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;

/**
 * Tests for {@link QwpWebSocketUpgradeProcessor#resumeSend} covering all branches:
 * <ol>
 *     <li>null state - throws ServerDisconnectException</li>
 *     <li>READY state (not SENDING) - no-op</li>
 *     <li>SENDING state, no pending ACK after resume - flushes and returns</li>
 *     <li>SENDING state, pending ACK after resume - flushes and sends additional ACK</li>
 * </ol>
 */
public class QwpWebSocketUpgradeProcessorResumeSendTest extends AbstractCairoTest {

    @Test
    public void testResumeSendBlockedAckThenDeferredError() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);
            LocalValue<QwpProcessorState> lv = getLV();

            long bufferAddr = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
            try (TestableContext context = new TestableContext(httpConfig, new MockRawSocket(bufferAddr, 256))) {
                QwpProcessorState state = new QwpProcessorState(
                        1024, 1024, engine,
                        httpConfig.getLineHttpProcessorConfiguration()
                );
                state.of(-1, AllowAllSecurityContext.INSTANCE);
                lv.set(context, state);

                state.setHighestProcessedSequence(3);
                state.onAckBlocked(3);
                state.onErrorBlocked((byte) 3, 4L, "bad batch");

                Assert.assertTrue(state.isSending());
                Assert.assertEquals(4L, state.getDeferredErrorSequence());
                Assert.assertEquals(3, state.getDeferredErrorStatus());
                Assert.assertEquals("bad batch", state.getDeferredErrorMessage().toString());

                processor.resumeSend(context);

                Assert.assertTrue(context.isResumeResponseSendCalled());
                Assert.assertTrue(state.isSendReady());
                Assert.assertEquals(3L, state.getLastAckedSequence());
                Assert.assertTrue(context.getMockRawSocket().sentSize > 0);
                assertErrorFrame(bufferAddr, context.getMockRawSocket().sentSize);
            } finally {
                Unsafe.free(bufferAddr, 256, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testResumeSendNullState() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);

            try (TestableContext context = new TestableContext(httpConfig)) {
                try {
                    processor.resumeSend(context);
                    Assert.fail("Expected ServerDisconnectException");
                } catch (ServerDisconnectException e) {
                    // expected
                }
            }
        });
    }

    @Test
    public void testResumeSendReadyState() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);
            LocalValue<QwpProcessorState> lv = getLV();

            try (TestableContext context = new TestableContext(httpConfig)) {
                QwpProcessorState state = new QwpProcessorState(
                        1024, 1024, engine,
                        httpConfig.getLineHttpProcessorConfiguration()
                );
                state.of(-1, AllowAllSecurityContext.INSTANCE);
                lv.set(context, state);

                // State is in default READY mode
                Assert.assertTrue(state.isSendReady());
                Assert.assertFalse(state.isSending());

                processor.resumeSend(context);

                // resumeResponseSend should NOT have been called (READY branch is a no-op)
                Assert.assertFalse(context.isResumeResponseSendCalled());
                Assert.assertTrue(state.isSendReady());
            }
        });
    }

    @Test
    public void testResumeSendSendingNoPendingAck() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);
            LocalValue<QwpProcessorState> lv = getLV();

            long bufferAddr = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
            try (TestableContext context = new TestableContext(httpConfig, new MockRawSocket(bufferAddr, 256))) {
                QwpProcessorState state = new QwpProcessorState(
                        1024, 1024, engine,
                        httpConfig.getLineHttpProcessorConfiguration()
                );
                state.of(-1, AllowAllSecurityContext.INSTANCE);
                lv.set(context, state);

                // Sequence 5 was blocked, highest processed is also 5 — no pending ACK after resume
                state.setHighestProcessedSequence(5);
                state.onAckBlocked(5);
                Assert.assertTrue(state.isSending());

                processor.resumeSend(context);

                Assert.assertTrue(context.isResumeResponseSendCalled());
                Assert.assertTrue(state.isSendReady());
                Assert.assertEquals(5, state.getLastAckedSequence());
                // No additional ACK sent via raw socket
                Assert.assertEquals(0, context.getMockRawSocket().sentSize);
            } finally {
                Unsafe.free(bufferAddr, 256, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testResumeSendSendingWithPendingAck() throws Exception {
        assertMemoryLeak(() -> {
            HttpFullFatServerConfiguration httpConfig = new DefaultHttpServerConfiguration(configuration);
            QwpWebSocketUpgradeProcessor processor = new QwpWebSocketUpgradeProcessor(engine, httpConfig);
            LocalValue<QwpProcessorState> lv = getLV();

            long bufferAddr = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
            try (TestableContext context = new TestableContext(httpConfig, new MockRawSocket(bufferAddr, 256))) {
                QwpProcessorState state = new QwpProcessorState(
                        1024, 1024, engine,
                        httpConfig.getLineHttpProcessorConfiguration()
                );
                state.of(-1, AllowAllSecurityContext.INSTANCE);
                lv.set(context, state);

                // Sequence 3 was blocked, but highest processed is 7 — pending ACK after resume
                state.setHighestProcessedSequence(7);
                state.onAckBlocked(3);
                Assert.assertTrue(state.isSending());

                processor.resumeSend(context);

                Assert.assertTrue(context.isResumeResponseSendCalled());
                Assert.assertTrue(state.isSendReady());
                // After the resumed ACK flush: lastAcked=3 (from the blocked ACK state)
                // Then trySendAck sends ACK for sequence 7: lastAcked=7
                Assert.assertEquals(7, state.getLastAckedSequence());
                Assert.assertTrue(context.getMockRawSocket().sentSize > 0);
            } finally {
                Unsafe.free(bufferAddr, 256, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static void assertErrorFrame(long bufferAddr, int frameSize) {
        byte[] expectedMessageBytes = "bad batch".getBytes(StandardCharsets.UTF_8);
        int expectedPayloadLen = 11 + expectedMessageBytes.length;

        Assert.assertEquals((byte) 0x82, Unsafe.getUnsafe().getByte(bufferAddr));
        Assert.assertEquals(expectedPayloadLen, Unsafe.getUnsafe().getByte(bufferAddr + 1) & 0x7F);
        Assert.assertEquals(expectedPayloadLen + 2, frameSize);

        long payloadAddr = bufferAddr + 2;
        Assert.assertEquals((byte) 3, Unsafe.getUnsafe().getByte(payloadAddr));
        Assert.assertEquals(4L, readLittleEndianLong(payloadAddr + 1));
        Assert.assertEquals(expectedMessageBytes.length, readLittleEndianShort(payloadAddr + 9));

        byte[] actualMessageBytes = new byte[expectedMessageBytes.length];
        for (int i = 0; i < actualMessageBytes.length; i++) {
            actualMessageBytes[i] = Unsafe.getUnsafe().getByte(payloadAddr + 11 + i);
        }
        Assert.assertArrayEquals(expectedMessageBytes, actualMessageBytes);
    }

    @SuppressWarnings("unchecked")
    private static LocalValue<QwpProcessorState> getLV() throws Exception {
        Field lvField = QwpWebSocketUpgradeProcessor.class.getDeclaredField("LV");
        lvField.setAccessible(true);
        return (LocalValue<QwpProcessorState>) lvField.get(null);
    }

    private static long readLittleEndianLong(long addr) {
        long value = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            value |= (long) (Unsafe.getUnsafe().getByte(addr + i) & 0xFF) << (i * 8);
        }
        return value;
    }

    private static int readLittleEndianShort(long addr) {
        return (Unsafe.getUnsafe().getByte(addr) & 0xFF)
                | ((Unsafe.getUnsafe().getByte(addr + 1) & 0xFF) << 8);
    }

    private static class MockRawSocket implements HttpRawSocket {
        private final long bufferAddress;
        private final int bufferSize;
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
            sentSize = size;
        }
    }

    private static class TestableContext extends HttpConnectionContext {
        private final MockRawSocket mockRawSocket;
        private boolean resumeResponseSendCalled;

        TestableContext(HttpServerConfiguration config) {
            this(config, null);
        }

        TestableContext(HttpServerConfiguration config, MockRawSocket mockRawSocket) {
            super(config, PlainSocketFactory.INSTANCE);
            this.mockRawSocket = mockRawSocket;
        }

        @Override
        public HttpRawSocket getRawResponseSocket() {
            return mockRawSocket;
        }

        @Override
        public void resumeResponseSend() {
            resumeResponseSendCalled = true;
        }

        MockRawSocket getMockRawSocket() {
            return mockRawSocket;
        }

        boolean isResumeResponseSendCalled() {
            return resumeResponseSendCalled;
        }
    }
}
