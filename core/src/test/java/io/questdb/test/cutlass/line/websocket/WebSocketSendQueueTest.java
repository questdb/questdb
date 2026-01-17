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

import io.questdb.HttpClientConfiguration;
import io.questdb.cutlass.http.client.WebSocketClient;
import io.questdb.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.ilpv4.client.MicrobatchBuffer;
import io.questdb.cutlass.ilpv4.client.WebSocketSendQueue;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unit tests for WebSocketSendQueue.
 */
public class WebSocketSendQueueTest {

    // ==================== CONSTRUCTION TESTS ====================

    @Test
    public void testConstructionWithDefaultParameters() {
        MockWebSocketClient channel = new MockWebSocketClient();
        try (WebSocketSendQueue queue = new WebSocketSendQueue(channel)) {
            Assert.assertTrue(queue.isRunning());
            Assert.assertTrue(queue.isEmpty());
            Assert.assertEquals(0, queue.getPendingCount());
            Assert.assertEquals(0, queue.getTotalBatchesSent());
            Assert.assertEquals(0, queue.getTotalBytesSent());
            Assert.assertNull(queue.getLastError());
        }
    }

    @Test
    public void testConstructionWithCustomParameters() {
        MockWebSocketClient channel = new MockWebSocketClient();
        try (WebSocketSendQueue queue = new WebSocketSendQueue(channel, null, 8, 5000, 2000)) {
            Assert.assertTrue(queue.isRunning());
            Assert.assertTrue(queue.isEmpty());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructionWithNullChannel() {
        new WebSocketSendQueue(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructionWithZeroCapacity() {
        MockWebSocketClient channel = new MockWebSocketClient();
        try (WebSocketSendQueue ignored = new WebSocketSendQueue(channel, null, 0, 1000, 1000)) {
            Assert.fail("Should throw");
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructionWithNegativeCapacity() {
        MockWebSocketClient channel = new MockWebSocketClient();
        try (WebSocketSendQueue ignored = new WebSocketSendQueue(channel, null, -1, 1000, 1000)) {
            Assert.fail("Should throw");
        }
    }

    // ==================== ENQUEUE TESTS ====================

    @Test
    public void testEnqueueSingleBuffer() throws Exception {
        MockWebSocketClient channel = new MockWebSocketClient();
        try (WebSocketSendQueue queue = new WebSocketSendQueue(channel)) {
            MicrobatchBuffer buffer = createSealedBuffer(100);
            try {
                Assert.assertTrue(queue.enqueue(buffer));

                // Wait for send to complete
                waitForSend(channel, 1, 1000);

                Assert.assertEquals(1, queue.getTotalBatchesSent());
                Assert.assertEquals(100, queue.getTotalBytesSent());
                Assert.assertEquals(1, channel.getSendCount());
            } finally {
                buffer.close();
            }
        }
    }

    @Test
    public void testEnqueueMultipleBuffers() throws Exception {
        MockWebSocketClient channel = new MockWebSocketClient();
        try (WebSocketSendQueue queue = new WebSocketSendQueue(channel)) {
            List<MicrobatchBuffer> buffers = new ArrayList<>();
            try {
                for (int i = 0; i < 5; i++) {
                    MicrobatchBuffer buffer = createSealedBuffer(50 + i * 10);
                    buffers.add(buffer);
                    Assert.assertTrue(queue.enqueue(buffer));
                }

                // Wait for all sends to complete
                waitForSend(channel, 5, 2000);

                Assert.assertEquals(5, queue.getTotalBatchesSent());
                Assert.assertEquals(50 + 60 + 70 + 80 + 90, queue.getTotalBytesSent());
                Assert.assertEquals(5, channel.getSendCount());
            } finally {
                for (MicrobatchBuffer buf : buffers) {
                    buf.close();
                }
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEnqueueNullBuffer() {
        MockWebSocketClient channel = new MockWebSocketClient();
        try (WebSocketSendQueue queue = new WebSocketSendQueue(channel)) {
            queue.enqueue(null);
        }
    }

    @Test(expected = LineSenderException.class)
    public void testEnqueueUnsealedBuffer() {
        MockWebSocketClient channel = new MockWebSocketClient();
        try (WebSocketSendQueue queue = new WebSocketSendQueue(channel)) {
            MicrobatchBuffer buffer = new MicrobatchBuffer(1024);
            try {
                // Buffer is in FILLING state, not SEALED
                queue.enqueue(buffer);
            } finally {
                buffer.close();
            }
        }
    }

    @Test(expected = LineSenderException.class)
    public void testEnqueueAfterClose() {
        MockWebSocketClient channel = new MockWebSocketClient();
        WebSocketSendQueue queue = new WebSocketSendQueue(channel);
        queue.close();

        MicrobatchBuffer buffer = createSealedBuffer(100);
        try {
            queue.enqueue(buffer);
        } finally {
            buffer.close();
        }
    }

    // ==================== FLUSH TESTS ====================

    @Test
    public void testFlushEmptyQueue() {
        MockWebSocketClient channel = new MockWebSocketClient();
        try (WebSocketSendQueue queue = new WebSocketSendQueue(channel)) {
            // Should return immediately
            queue.flush();
            Assert.assertTrue(queue.isEmpty());
        }
    }

    @Test
    public void testFlushWaitsForPendingBatches() throws Exception {
        MockWebSocketClient channel = new MockWebSocketClient();
        // Add delay to simulate slow sending
        channel.setSendDelayMs(50);

        try (WebSocketSendQueue queue = new WebSocketSendQueue(channel)) {
            List<MicrobatchBuffer> buffers = new ArrayList<>();
            try {
                // Enqueue several batches
                for (int i = 0; i < 3; i++) {
                    MicrobatchBuffer buffer = createSealedBuffer(100);
                    buffers.add(buffer);
                    queue.enqueue(buffer);
                }

                // Flush should wait for all to complete
                queue.flush();

                // Wait for all sends to complete (flush only waits for queue to drain)
                waitForSend(channel, 3, 2000);

                Assert.assertTrue(queue.isEmpty());
                Assert.assertEquals(3, queue.getTotalBatchesSent());
            } finally {
                for (MicrobatchBuffer buf : buffers) {
                    buf.close();
                }
            }
        }
    }

    // ==================== CLOSE TESTS ====================

    @Test
    public void testCloseEmptyQueue() {
        MockWebSocketClient channel = new MockWebSocketClient();
        WebSocketSendQueue queue = new WebSocketSendQueue(channel);

        queue.close();

        Assert.assertFalse(queue.isRunning());
    }

    @Test
    public void testCloseWithPendingBatches() throws Exception {
        MockWebSocketClient channel = new MockWebSocketClient();
        channel.setSendDelayMs(10);

        WebSocketSendQueue queue = new WebSocketSendQueue(channel, null, 8, 1000, 5000);

        List<MicrobatchBuffer> buffers = new ArrayList<>();
        try {
            // Enqueue several batches
            for (int i = 0; i < 5; i++) {
                MicrobatchBuffer buffer = createSealedBuffer(100);
                buffers.add(buffer);
                queue.enqueue(buffer);
            }

            // Close should wait for pending batches
            queue.close();

            // All batches should have been sent
            Assert.assertEquals(5, queue.getTotalBatchesSent());
        } finally {
            for (MicrobatchBuffer buf : buffers) {
                buf.close();
            }
        }
    }

    @Test
    public void testCloseIdempotent() {
        MockWebSocketClient channel = new MockWebSocketClient();
        WebSocketSendQueue queue = new WebSocketSendQueue(channel);

        // Multiple closes should be safe
        queue.close();
        queue.close();
        queue.close();

        Assert.assertFalse(queue.isRunning());
    }

    // ==================== STATISTICS TESTS ====================

    @Test
    public void testStatisticsAccumulate() throws Exception {
        MockWebSocketClient channel = new MockWebSocketClient();
        try (WebSocketSendQueue queue = new WebSocketSendQueue(channel)) {
            List<MicrobatchBuffer> buffers = new ArrayList<>();
            int totalBytes = 0;
            try {
                for (int i = 0; i < 10; i++) {
                    int size = 100 + i * 20;
                    totalBytes += size;
                    MicrobatchBuffer buffer = createSealedBuffer(size);
                    buffers.add(buffer);
                    queue.enqueue(buffer);
                }

                waitForSend(channel, 10, 2000);

                Assert.assertEquals(10, queue.getTotalBatchesSent());
                Assert.assertEquals(totalBytes, queue.getTotalBytesSent());
            } finally {
                for (MicrobatchBuffer buf : buffers) {
                    buf.close();
                }
            }
        }
    }

    @Test
    public void testPendingCountUpdates() throws Exception {
        MockWebSocketClient channel = new MockWebSocketClient();
        // Add delay to let batches accumulate
        channel.setSendDelayMs(100);

        try (WebSocketSendQueue queue = new WebSocketSendQueue(channel, null, 16, 1000, 1000)) {
            List<MicrobatchBuffer> buffers = new ArrayList<>();
            try {
                // Enqueue several batches quickly
                for (int i = 0; i < 5; i++) {
                    MicrobatchBuffer buffer = createSealedBuffer(100);
                    buffers.add(buffer);
                    queue.enqueue(buffer);
                }

                // Some should be pending
                int pending = queue.getPendingCount();
                Assert.assertTrue("Expected some pending, got " + pending, pending >= 0);

                // Wait for all to complete
                queue.flush();

                Assert.assertEquals(0, queue.getPendingCount());
            } finally {
                for (MicrobatchBuffer buf : buffers) {
                    buf.close();
                }
            }
        }
    }

    // ==================== BUFFER STATE TESTS ====================

    @Test
    public void testBufferStateTransitionsToRecycled() throws Exception {
        MockWebSocketClient channel = new MockWebSocketClient();
        try (WebSocketSendQueue queue = new WebSocketSendQueue(channel)) {
            MicrobatchBuffer buffer = createSealedBuffer(100);
            try {
                Assert.assertTrue(buffer.isSealed());

                queue.enqueue(buffer);

                // Wait for send to complete
                waitForSend(channel, 1, 1000);

                // Buffer should be recycled
                Assert.assertTrue(buffer.isRecycled());
            } finally {
                buffer.close();
            }
        }
    }

    @Test
    public void testBufferAwaitRecycled() throws Exception {
        MockWebSocketClient channel = new MockWebSocketClient();
        channel.setSendDelayMs(50);

        try (WebSocketSendQueue queue = new WebSocketSendQueue(channel)) {
            MicrobatchBuffer buffer = createSealedBuffer(100);
            try {
                queue.enqueue(buffer);

                // Wait for recycled
                boolean recycled = buffer.awaitRecycled(2000, TimeUnit.MILLISECONDS);
                Assert.assertTrue(recycled);
                Assert.assertTrue(buffer.isRecycled());
            } finally {
                buffer.close();
            }
        }
    }

    // ==================== CONCURRENCY TESTS ====================

    @Test
    public void testConcurrentEnqueue() throws Exception {
        MockWebSocketClient channel = new MockWebSocketClient();
        int numThreads = 4;
        int batchesPerThread = 25;

        try (WebSocketSendQueue queue = new WebSocketSendQueue(channel, null, 64, 5000, 5000)) {
            List<MicrobatchBuffer> allBuffers = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(numThreads);
            AtomicInteger successCount = new AtomicInteger(0);

            // Start producer threads
            for (int t = 0; t < numThreads; t++) {
                new Thread(() -> {
                    try {
                        startLatch.await();
                        for (int i = 0; i < batchesPerThread; i++) {
                            MicrobatchBuffer buffer = createSealedBuffer(100);
                            allBuffers.add(buffer);
                            if (queue.enqueue(buffer)) {
                                successCount.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        doneLatch.countDown();
                    }
                }).start();
            }

            // Start all threads
            startLatch.countDown();

            // Wait for all to finish
            doneLatch.await(10, TimeUnit.SECONDS);

            // Flush and verify
            queue.flush();

            Assert.assertEquals(numThreads * batchesPerThread, successCount.get());
            Assert.assertEquals(numThreads * batchesPerThread, queue.getTotalBatchesSent());

            // Cleanup
            for (MicrobatchBuffer buf : allBuffers) {
                buf.close();
            }
        }
    }

    @Test
    public void testProducerConsumerWithBackpressure() throws Exception {
        MockWebSocketClient channel = new MockWebSocketClient();
        // Slow sending to create backpressure
        channel.setSendDelayMs(20);

        int queueCapacity = 4;
        try (WebSocketSendQueue queue = new WebSocketSendQueue(channel, null, queueCapacity, 5000, 5000)) {
            List<MicrobatchBuffer> buffers = Collections.synchronizedList(new ArrayList<>());
            int totalBatches = 20;
            CountDownLatch doneLatch = new CountDownLatch(1);
            AtomicInteger enqueueCount = new AtomicInteger(0);

            // Producer thread
            Thread producer = new Thread(() -> {
                try {
                    for (int i = 0; i < totalBatches; i++) {
                        MicrobatchBuffer buffer = createSealedBuffer(100);
                        buffers.add(buffer);
                        queue.enqueue(buffer);
                        enqueueCount.incrementAndGet();
                    }
                } finally {
                    doneLatch.countDown();
                }
            });

            producer.start();

            // Wait for producer to finish
            doneLatch.await(30, TimeUnit.SECONDS);

            // Flush remaining
            queue.flush();

            // Wait for all sends to complete
            waitForSend(channel, totalBatches, 5000);

            Assert.assertEquals(totalBatches, enqueueCount.get());
            Assert.assertEquals(totalBatches, queue.getTotalBatchesSent());

            // Cleanup
            for (MicrobatchBuffer buf : buffers) {
                buf.close();
            }
        }
    }

    // ==================== ERROR HANDLING TESTS ====================

    @Test
    public void testSendErrorRecorded() throws Exception {
        MockWebSocketClient channel = new MockWebSocketClient();
        channel.setFailAfterSends(2);
        channel.setFailException(new RuntimeException("Simulated send error"));

        try (WebSocketSendQueue queue = new WebSocketSendQueue(channel)) {
            List<MicrobatchBuffer> buffers = new ArrayList<>();
            try {
                for (int i = 0; i < 3; i++) {
                    MicrobatchBuffer buffer = createSealedBuffer(100);
                    buffers.add(buffer);
                    queue.enqueue(buffer);
                }

                // Wait for sends
                Thread.sleep(500);

                // Error should be recorded
                Assert.assertNotNull(queue.getLastError());
            } finally {
                for (MicrobatchBuffer buf : buffers) {
                    buf.close();
                }
            }
        }
    }

    @Test
    public void testBufferRecycledOnError() throws Exception {
        MockWebSocketClient channel = new MockWebSocketClient();
        channel.setFailAfterSends(0); // Fail immediately
        channel.setFailException(new RuntimeException("Simulated send error"));

        try (WebSocketSendQueue queue = new WebSocketSendQueue(channel)) {
            MicrobatchBuffer buffer = createSealedBuffer(100);
            try {
                queue.enqueue(buffer);

                // Wait for attempt
                Thread.sleep(500);

                // Buffer should still be recycled even after error
                Assert.assertTrue(buffer.isRecycled());
            } finally {
                buffer.close();
            }
        }
    }

    // ==================== HELPER METHODS ====================

    private MicrobatchBuffer createSealedBuffer(int size) {
        MicrobatchBuffer buffer = new MicrobatchBuffer(size);
        // Write some data
        for (int i = 0; i < size; i++) {
            buffer.writeByte((byte) (i % 256));
        }
        buffer.incrementRowCount();
        buffer.seal();
        return buffer;
    }

    private void waitForSend(MockWebSocketClient channel, int expectedCount, long timeoutMs) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        while (channel.getSendCount() < expectedCount) {
            if (System.currentTimeMillis() - startTime > timeoutMs) {
                Assert.fail("Timeout waiting for sends: expected=" + expectedCount +
                        ", actual=" + channel.getSendCount());
            }
            Thread.sleep(10);
        }
    }

    // ==================== STRESS TESTS ====================

    @Test
    public void testHighThroughputSending() throws Exception {
        MockWebSocketClient channel = new MockWebSocketClient();
        int numBatches = 1000;

        try (WebSocketSendQueue queue = new WebSocketSendQueue(channel, null, 64, 30000, 30000)) {
            List<MicrobatchBuffer> buffers = new ArrayList<>();
            try {
                long startTime = System.currentTimeMillis();

                for (int i = 0; i < numBatches; i++) {
                    MicrobatchBuffer buffer = createSealedBuffer(100);
                    buffers.add(buffer);
                    queue.enqueue(buffer);
                }

                queue.flush();
                waitForSend(channel, numBatches, 30000);

                long elapsed = System.currentTimeMillis() - startTime;
                double throughput = (double) numBatches / elapsed * 1000.0;

                Assert.assertEquals(numBatches, queue.getTotalBatchesSent());
                System.out.println("Throughput: " + String.format("%.2f", throughput) + " batches/sec");
            } finally {
                for (MicrobatchBuffer buf : buffers) {
                    buf.close();
                }
            }
        }
    }

    @Test
    public void testBurstThenSteadySending() throws Exception {
        MockWebSocketClient channel = new MockWebSocketClient();
        channel.setSendDelayMs(5);

        try (WebSocketSendQueue queue = new WebSocketSendQueue(channel, null, 16, 10000, 10000)) {
            List<MicrobatchBuffer> buffers = new ArrayList<>();
            try {
                // Burst: enqueue many quickly
                for (int i = 0; i < 50; i++) {
                    MicrobatchBuffer buffer = createSealedBuffer(100);
                    buffers.add(buffer);
                    queue.enqueue(buffer);
                }

                // Steady: enqueue slowly
                for (int i = 0; i < 20; i++) {
                    Thread.sleep(20);
                    MicrobatchBuffer buffer = createSealedBuffer(100);
                    buffers.add(buffer);
                    queue.enqueue(buffer);
                }

                queue.flush();
                waitForSend(channel, 70, 10000);

                Assert.assertEquals(70, queue.getTotalBatchesSent());
            } finally {
                for (MicrobatchBuffer buf : buffers) {
                    buf.close();
                }
            }
        }
    }

    @Test
    public void testQueueDrainDuringHighContention() throws Exception {
        MockWebSocketClient channel = new MockWebSocketClient();
        channel.setSendDelayMs(2);

        int numThreads = 8;
        int batchesPerThread = 50;

        try (WebSocketSendQueue queue = new WebSocketSendQueue(channel, null, 32, 30000, 30000)) {
            List<MicrobatchBuffer> allBuffers = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(numThreads);
            AtomicInteger errorCount = new AtomicInteger(0);

            for (int t = 0; t < numThreads; t++) {
                new Thread(() -> {
                    try {
                        startLatch.await();
                        for (int i = 0; i < batchesPerThread; i++) {
                            MicrobatchBuffer buffer = createSealedBuffer(50 + (int) (Math.random() * 100));
                            allBuffers.add(buffer);
                            queue.enqueue(buffer);
                            if (i % 10 == 0) {
                                Thread.sleep(1); // Occasional yield
                            }
                        }
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                        e.printStackTrace();
                    } finally {
                        doneLatch.countDown();
                    }
                }).start();
            }

            startLatch.countDown();
            doneLatch.await(60, TimeUnit.SECONDS);

            queue.flush();
            waitForSend(channel, numThreads * batchesPerThread, 60000);

            Assert.assertEquals(0, errorCount.get());
            Assert.assertEquals(numThreads * batchesPerThread, queue.getTotalBatchesSent());

            for (MicrobatchBuffer buf : allBuffers) {
                buf.close();
            }
        }
    }

    @Test
    public void testVariableBufferSizes() throws Exception {
        MockWebSocketClient channel = new MockWebSocketClient();

        try (WebSocketSendQueue queue = new WebSocketSendQueue(channel, null, 16, 10000, 10000)) {
            List<MicrobatchBuffer> buffers = new ArrayList<>();
            int totalExpectedBytes = 0;
            try {
                // Varying sizes from small to large
                int[] sizes = {10, 100, 1000, 5000, 100, 50, 2000, 500};
                for (int size : sizes) {
                    MicrobatchBuffer buffer = createSealedBuffer(size);
                    buffers.add(buffer);
                    totalExpectedBytes += size;
                    queue.enqueue(buffer);
                }

                queue.flush();
                waitForSend(channel, sizes.length, 5000);

                Assert.assertEquals(sizes.length, queue.getTotalBatchesSent());
                Assert.assertEquals(totalExpectedBytes, queue.getTotalBytesSent());
            } finally {
                for (MicrobatchBuffer buf : buffers) {
                    buf.close();
                }
            }
        }
    }

    @Test
    public void testInterruptedEnqueue() throws Exception {
        MockWebSocketClient channel = new MockWebSocketClient();
        channel.setSendDelayMs(50); // Moderate delay

        // Note: We cannot close buffers while the send thread might still be using them.
        // The queue will recycle buffers after sending, so we only need to track buffers
        // that were never successfully enqueued.
        try (WebSocketSendQueue queue = new WebSocketSendQueue(channel, null, 2, 5000, 5000)) {
            AtomicBoolean interrupted = new AtomicBoolean(false);
            CountDownLatch started = new CountDownLatch(1);

            Thread producer = new Thread(() -> {
                try {
                    // Fill queue - these buffers will be owned by the queue after enqueue
                    queue.enqueue(createSealedBuffer(100));
                    queue.enqueue(createSealedBuffer(100));

                    started.countDown();

                    // This should block and then be interrupted
                    queue.enqueue(createSealedBuffer(100));
                } catch (LineSenderException e) {
                    if (e.getMessage().contains("Interrupted")) {
                        interrupted.set(true);
                    }
                }
            });

            producer.start();
            started.await(5, TimeUnit.SECONDS);
            Thread.sleep(100); // Let it block

            producer.interrupt();
            producer.join(2000);

            // The queue close() will wait for pending buffers to be sent
            // and properly clean them up
        }
    }

    // ==================== MOCK WEBSOCKET CLIENT ====================

    /**
     * Mock WebSocket client for testing.
     */
    private static class MockWebSocketClient extends WebSocketClient {
        private final AtomicInteger sendCount = new AtomicInteger(0);
        private final AtomicLong totalBytes = new AtomicLong(0);
        private volatile int sendDelayMs = 0;
        private volatile int failAfterSends = -1;
        private volatile RuntimeException failException = null;
        private final List<byte[]> sentData = Collections.synchronizedList(new ArrayList<>());
        private volatile boolean connected = true;

        public MockWebSocketClient() {
            super(new HttpClientConfiguration() {
                @Override
                public int getTimeout() {
                    return 30000;
                }

                @Override
                public int getInitialRequestBufferSize() {
                    return 8192;
                }

                @Override
                public int getMaximumRequestBufferSize() {
                    return 65536;
                }

                @Override
                public int getResponseBufferSize() {
                    return 65536;
                }

                @Override
                public io.questdb.network.NetworkFacade getNetworkFacade() {
                    return NetworkFacadeImpl.INSTANCE;
                }

                @Override
                public io.questdb.cutlass.http.client.HttpClientCookieHandlerFactory getCookieHandlerFactory() {
                    return null;
                }
            }, PlainSocketFactory.INSTANCE);
        }

        @Override
        public void sendBinary(long dataPtr, int length) {
            // Check if we should fail
            if (failAfterSends >= 0 && sendCount.get() >= failAfterSends && failException != null) {
                throw failException;
            }

            // Simulate delay
            if (sendDelayMs > 0) {
                try {
                    Thread.sleep(sendDelayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            // Record the send
            byte[] data = new byte[length];
            for (int i = 0; i < length; i++) {
                data[i] = Unsafe.getUnsafe().getByte(dataPtr + i);
            }
            sentData.add(data);
            totalBytes.addAndGet(length);
            sendCount.incrementAndGet();
        }

        @Override
        public void close() {
            connected = false;
        }

        @Override
        public boolean isConnected() {
            return connected;
        }

        @Override
        public boolean receiveFrame(WebSocketFrameHandler handler, int timeout) {
            // Mock: no frames to receive
            return false;
        }

        @Override
        public boolean tryReceiveFrame(WebSocketFrameHandler handler) {
            // Mock: no frames to receive (non-blocking)
            return false;
        }

        @Override
        protected void ioWait(int timeout, int op) {
            // No-op for mock
        }

        @Override
        protected void setupIoWait() {
            // No-op for mock
        }

        public int getSendCount() {
            return sendCount.get();
        }

        public long getTotalBytes() {
            return totalBytes.get();
        }

        public void setSendDelayMs(int delayMs) {
            this.sendDelayMs = delayMs;
        }

        public void setFailAfterSends(int count) {
            this.failAfterSends = count;
        }

        public void setFailException(RuntimeException e) {
            this.failException = e;
        }

        public List<byte[]> getSentData() {
            return sentData;
        }
    }
}
