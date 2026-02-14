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

package io.questdb.test.cutlass.line.websocket;

import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.qwp.client.MicrobatchBuffer;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.WebSocketSendQueue;
import io.questdb.client.network.PlainSocketFactory;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Unit tests for QwpWebSocketSender.
 * These tests focus on state management and API validation without requiring a live server.
 */
public class QwpWebSocketSenderTest {

    @Test
    public void testConnectToClosedPort() {
        try {
            QwpWebSocketSender.connect("127.0.0.1", 1);
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to connect"));
        }
    }

    @Test
    public void testTableBeforeColumnsRequired() {
        // Create sender without connecting (we'll catch the error earlier)
        try {
            QwpWebSocketSender sender = createUnconnectedSender();
            sender.longColumn("x", 1);
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("table()"));
        }
    }

    @Test
    public void testTableBeforeAtRequired() {
        try {
            QwpWebSocketSender sender = createUnconnectedSender();
            sender.at(1000L, ChronoUnit.MICROS);
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("table()"));
        }
    }

    @Test
    public void testTableBeforeAtNowRequired() {
        try {
            QwpWebSocketSender sender = createUnconnectedSender();
            sender.atNow();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("table()"));
        }
    }

    @Test
    public void testCloseIdemponent() {
        QwpWebSocketSender sender = createUnconnectedSender();
        sender.close();
        sender.close(); // Should not throw
    }

    @Test
    public void testOperationsAfterCloseThrow() {
        QwpWebSocketSender sender = createUnconnectedSender();
        sender.close();

        try {
            sender.table("test");
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void testLongColumnAfterCloseThrows() {
        QwpWebSocketSender sender = createUnconnectedSender();
        sender.close();

        try {
            sender.longColumn("x", 1);
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void testDoubleColumnAfterCloseThrows() {
        QwpWebSocketSender sender = createUnconnectedSender();
        sender.close();

        try {
            sender.doubleColumn("x", 1.0);
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void testStringColumnAfterCloseThrows() {
        QwpWebSocketSender sender = createUnconnectedSender();
        sender.close();

        try {
            sender.stringColumn("x", "test");
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void testBoolColumnAfterCloseThrows() {
        QwpWebSocketSender sender = createUnconnectedSender();
        sender.close();

        try {
            sender.boolColumn("x", true);
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void testSymbolAfterCloseThrows() {
        QwpWebSocketSender sender = createUnconnectedSender();
        sender.close();

        try {
            sender.symbol("x", "test");
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void testTimestampColumnAfterCloseThrows() {
        QwpWebSocketSender sender = createUnconnectedSender();
        sender.close();

        try {
            sender.timestampColumn("x", 1000L, ChronoUnit.MICROS);
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void testTimestampColumnInstantAfterCloseThrows() {
        QwpWebSocketSender sender = createUnconnectedSender();
        sender.close();

        try {
            sender.timestampColumn("x", Instant.now());
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void testAtAfterCloseThrows() {
        QwpWebSocketSender sender = createUnconnectedSender();
        sender.close();

        try {
            sender.at(1000L, ChronoUnit.MICROS);
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void testAtInstantAfterCloseThrows() {
        QwpWebSocketSender sender = createUnconnectedSender();
        sender.close();

        try {
            sender.at(Instant.now());
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void testAtNowAfterCloseThrows() {
        QwpWebSocketSender sender = createUnconnectedSender();
        sender.close();

        try {
            sender.atNow();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void testCancelRowAfterCloseThrows() {
        QwpWebSocketSender sender = createUnconnectedSender();
        sender.close();

        try {
            sender.cancelRow();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void testResetAfterCloseThrows() {
        QwpWebSocketSender sender = createUnconnectedSender();
        sender.close();

        try {
            sender.reset();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void testBufferViewNotSupported() {
        try (QwpWebSocketSender sender = createUnconnectedSender()) {
            sender.bufferView();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("not supported"));
        }
    }

    @Test
    public void testGorillaEnabledByDefault() {
        try (QwpWebSocketSender sender = createUnconnectedSender()) {
            Assert.assertTrue(sender.isGorillaEnabled());
        }
    }

    @Test
    public void testSetGorillaEnabled() {
        try (QwpWebSocketSender sender = createUnconnectedSender()) {
            sender.setGorillaEnabled(false);
            Assert.assertFalse(sender.isGorillaEnabled());
            sender.setGorillaEnabled(true);
            Assert.assertTrue(sender.isGorillaEnabled());
        }
    }

    @Test
    public void testDoubleArrayAfterCloseThrows() {
        QwpWebSocketSender sender = createUnconnectedSender();
        sender.close();

        try {
            sender.doubleArray("x", new double[]{1.0, 2.0});
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void testLongArrayAfterCloseThrows() {
        QwpWebSocketSender sender = createUnconnectedSender();
        sender.close();

        try {
            sender.longArray("x", new long[]{1L, 2L});
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("closed"));
        }
    }

    @Test
    public void testNullArrayReturnsThis() {
        try (QwpWebSocketSender sender = createUnconnectedSender()) {
            // Null arrays should be no-ops and return sender
            Assert.assertSame(sender, sender.doubleArray("x", (double[]) null));
            Assert.assertSame(sender, sender.longArray("x", (long[]) null));
        }
    }

    @Test
    public void testSealAndSwapRollsBackOnEnqueueFailure() throws Exception {
        try (QwpWebSocketSender sender = createUnconnectedAsyncSender(); ThrowingOnceWebSocketSendQueue queue = new ThrowingOnceWebSocketSendQueue()) {
            setSendQueue(sender, queue);

            MicrobatchBuffer originalActive = getActiveBuffer(sender);
            originalActive.writeByte((byte) 7);
            originalActive.incrementRowCount();

            try {
                invokeSealAndSwapBuffer(sender);
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("Synthetic enqueue failure"));
            }

            // Failed enqueue must not strand the sealed buffer.
            Assert.assertSame(originalActive, getActiveBuffer(sender));
            Assert.assertTrue(originalActive.isFilling());
            Assert.assertTrue(originalActive.hasData());
            Assert.assertEquals(1, originalActive.getRowCount());

            // Retry should be possible on the same sender instance.
            invokeSealAndSwapBuffer(sender);
            Assert.assertNotSame(originalActive, getActiveBuffer(sender));
        }
    }

    /**
     * Creates a sender without connecting.
     * For unit tests that don't need actual connectivity.
     */
    private QwpWebSocketSender createUnconnectedSender() {
        return QwpWebSocketSender.createForTesting("localhost", 9000, 1);  // window=1 for sync
    }

    /**
     * Creates an async sender without connecting.
     */
    private QwpWebSocketSender createUnconnectedAsyncSender() {
        return QwpWebSocketSender.createForTesting("localhost", 9000,
                500, 0, 0L,  // autoFlushRows, autoFlushBytes, autoFlushIntervalNanos
                8, 16);      // inFlightWindowSize, sendQueueCapacity
    }

    /**
     * Creates an async sender with custom flow control settings without connecting.
     */
    private QwpWebSocketSender createUnconnectedAsyncSenderWithFlowControl(
            int autoFlushRows, int autoFlushBytes, long autoFlushIntervalNanos,
            int inFlightWindowSize, int sendQueueCapacity) {
        return QwpWebSocketSender.createForTesting("localhost", 9000,
                autoFlushRows, autoFlushBytes, autoFlushIntervalNanos,
                inFlightWindowSize, sendQueueCapacity);
    }

    private static MicrobatchBuffer getActiveBuffer(QwpWebSocketSender sender) throws Exception {
        Field field = QwpWebSocketSender.class.getDeclaredField("activeBuffer");
        field.setAccessible(true);
        return (MicrobatchBuffer) field.get(sender);
    }

    private static void setSendQueue(QwpWebSocketSender sender, WebSocketSendQueue queue) throws Exception {
        Field field = QwpWebSocketSender.class.getDeclaredField("sendQueue");
        field.setAccessible(true);
        field.set(sender, queue);
    }

    private static void invokeSealAndSwapBuffer(QwpWebSocketSender sender) throws Exception {
        Method method = QwpWebSocketSender.class.getDeclaredMethod("sealAndSwapBuffer");
        method.setAccessible(true);
        try {
            method.invoke(sender);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    private static class ThrowingOnceWebSocketSendQueue extends WebSocketSendQueue {
        private boolean failOnce = true;

        private ThrowingOnceWebSocketSendQueue() {
            super(new NoOpWebSocketClient(), null, 1, 50, 50);
        }

        @Override
        public boolean enqueue(MicrobatchBuffer buffer) {
            if (failOnce) {
                failOnce = false;
                throw new LineSenderException("Synthetic enqueue failure");
            }
            return true;
        }
    }

    private static class NoOpWebSocketClient extends WebSocketClient {
        private NoOpWebSocketClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        public boolean isConnected() {
            return false;
        }

        @Override
        public void sendBinary(long dataPtr, int length) {
            // no-op
        }

        @Override
        protected void ioWait(int timeout, int op) {
            // no-op
        }

        @Override
        protected void setupIoWait() {
            // no-op
        }
    }
}
