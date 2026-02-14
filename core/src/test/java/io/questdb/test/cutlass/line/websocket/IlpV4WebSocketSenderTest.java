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
import io.questdb.client.cutlass.ilpv4.client.MicrobatchBuffer;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.ilpv4.client.IlpV4WebSocketSender;
import io.questdb.client.cutlass.ilpv4.client.WebSocketSendQueue;
import io.questdb.client.network.PlainSocketFactory;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Unit tests for IlpV4WebSocketSender.
 * These tests focus on state management and API validation without requiring a live server.
 */
public class IlpV4WebSocketSenderTest {

    @Test
    public void testConnectToClosedPort() {
        try {
            IlpV4WebSocketSender.connect("127.0.0.1", 1);
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to connect"));
        }
    }

    @Test
    public void testTableBeforeColumnsRequired() {
        // Create sender without connecting (we'll catch the error earlier)
        try {
            IlpV4WebSocketSender sender = createUnconnectedSender();
            sender.longColumn("x", 1);
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("table()"));
        }
    }

    @Test
    public void testTableBeforeAtRequired() {
        try {
            IlpV4WebSocketSender sender = createUnconnectedSender();
            sender.at(1000L, ChronoUnit.MICROS);
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("table()"));
        }
    }

    @Test
    public void testTableBeforeAtNowRequired() {
        try {
            IlpV4WebSocketSender sender = createUnconnectedSender();
            sender.atNow();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("table()"));
        }
    }

    @Test
    public void testCloseIdemponent() {
        IlpV4WebSocketSender sender = createUnconnectedSender();
        sender.close();
        sender.close(); // Should not throw
    }

    @Test
    public void testOperationsAfterCloseThrow() {
        IlpV4WebSocketSender sender = createUnconnectedSender();
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
        IlpV4WebSocketSender sender = createUnconnectedSender();
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
        IlpV4WebSocketSender sender = createUnconnectedSender();
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
        IlpV4WebSocketSender sender = createUnconnectedSender();
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
        IlpV4WebSocketSender sender = createUnconnectedSender();
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
        IlpV4WebSocketSender sender = createUnconnectedSender();
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
        IlpV4WebSocketSender sender = createUnconnectedSender();
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
        IlpV4WebSocketSender sender = createUnconnectedSender();
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
        IlpV4WebSocketSender sender = createUnconnectedSender();
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
        IlpV4WebSocketSender sender = createUnconnectedSender();
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
        IlpV4WebSocketSender sender = createUnconnectedSender();
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
        IlpV4WebSocketSender sender = createUnconnectedSender();
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
        IlpV4WebSocketSender sender = createUnconnectedSender();
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
        IlpV4WebSocketSender sender = createUnconnectedSender();
        try {
            sender.bufferView();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            Assert.assertTrue(e.getMessage().contains("not supported"));
        } finally {
            sender.close();
        }
    }

    @Test
    public void testGorillaEnabledByDefault() {
        IlpV4WebSocketSender sender = createUnconnectedSender();
        try {
            Assert.assertTrue(sender.isGorillaEnabled());
        } finally {
            sender.close();
        }
    }

    @Test
    public void testSetGorillaEnabled() {
        IlpV4WebSocketSender sender = createUnconnectedSender();
        try {
            sender.setGorillaEnabled(false);
            Assert.assertFalse(sender.isGorillaEnabled());
            sender.setGorillaEnabled(true);
            Assert.assertTrue(sender.isGorillaEnabled());
        } finally {
            sender.close();
        }
    }

    @Test
    public void testDoubleArrayAfterCloseThrows() {
        IlpV4WebSocketSender sender = createUnconnectedSender();
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
        IlpV4WebSocketSender sender = createUnconnectedSender();
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
        IlpV4WebSocketSender sender = createUnconnectedSender();
        try {
            // Null arrays should be no-ops and return sender
            Assert.assertSame(sender, sender.doubleArray("x", (double[]) null));
            Assert.assertSame(sender, sender.longArray("x", (long[]) null));
        } finally {
            sender.close();
        }
    }

    @Test
    public void testSealAndSwapRollsBackOnEnqueueFailure() throws Exception {
        IlpV4WebSocketSender sender = createUnconnectedAsyncSender();
        ThrowingOnceWebSocketSendQueue queue = new ThrowingOnceWebSocketSendQueue();
        try {
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
        } finally {
            sender.close();
            queue.close();
        }
    }

    /**
     * Creates a sender without connecting.
     * For unit tests that don't need actual connectivity.
     */
    private IlpV4WebSocketSender createUnconnectedSender() {
        return IlpV4WebSocketSender.createForTesting("localhost", 9000, 1);  // window=1 for sync
    }

    /**
     * Creates an async sender without connecting.
     */
    private IlpV4WebSocketSender createUnconnectedAsyncSender() {
        return IlpV4WebSocketSender.createForTesting("localhost", 9000,
                500, 0, 0L,  // autoFlushRows, autoFlushBytes, autoFlushIntervalNanos
                8, 16);      // inFlightWindowSize, sendQueueCapacity
    }

    /**
     * Creates an async sender with custom flow control settings without connecting.
     */
    private IlpV4WebSocketSender createUnconnectedAsyncSenderWithFlowControl(
            int autoFlushRows, int autoFlushBytes, long autoFlushIntervalNanos,
            int inFlightWindowSize, int sendQueueCapacity) {
        return IlpV4WebSocketSender.createForTesting("localhost", 9000,
                autoFlushRows, autoFlushBytes, autoFlushIntervalNanos,
                inFlightWindowSize, sendQueueCapacity);
    }

    private static MicrobatchBuffer getActiveBuffer(IlpV4WebSocketSender sender) throws Exception {
        Field field = IlpV4WebSocketSender.class.getDeclaredField("activeBuffer");
        field.setAccessible(true);
        return (MicrobatchBuffer) field.get(sender);
    }

    private static void setSendQueue(IlpV4WebSocketSender sender, WebSocketSendQueue queue) throws Exception {
        Field field = IlpV4WebSocketSender.class.getDeclaredField("sendQueue");
        field.setAccessible(true);
        field.set(sender, queue);
    }

    private static void invokeSealAndSwapBuffer(IlpV4WebSocketSender sender) throws Exception {
        Method method = IlpV4WebSocketSender.class.getDeclaredMethod("sealAndSwapBuffer");
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
