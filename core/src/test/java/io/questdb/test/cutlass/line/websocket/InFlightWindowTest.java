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

import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.ilpv4.client.InFlightWindow;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * Tests for InFlightWindow.
 * <p>
 * The window assumes sequential batch IDs and cumulative acknowledgments. It
 * tracks only the range [lastAcked+1, highestSent] rather than individual batch
 * IDs.
 */
public class InFlightWindowTest {

    @Test
    public void testBasicAddAndAcknowledge() {
        InFlightWindow window = new InFlightWindow(8, 1000);

        assertTrue(window.isEmpty());
        assertEquals(0, window.getInFlightCount());

        // Add a batch (sequential: 0)
        window.addInFlight(0);
        assertFalse(window.isEmpty());
        assertEquals(1, window.getInFlightCount());

        // Acknowledge it (cumulative ACK up to 0)
        assertTrue(window.acknowledge(0));
        assertTrue(window.isEmpty());
        assertEquals(0, window.getInFlightCount());
        assertEquals(1, window.getTotalAcked());
    }

    @Test
    public void testMultipleBatches() {
        InFlightWindow window = new InFlightWindow(8, 1000);

        // Add sequential batches 0-4
        for (long i = 0; i < 5; i++) {
            window.addInFlight(i);
        }
        assertEquals(5, window.getInFlightCount());

        // Cumulative ACK up to 2 (acknowledges 0, 1, 2)
        assertEquals(3, window.acknowledgeUpTo(2));
        assertEquals(2, window.getInFlightCount());

        // Cumulative ACK up to 4 (acknowledges 3, 4)
        assertEquals(2, window.acknowledgeUpTo(4));
        assertTrue(window.isEmpty());
        assertEquals(5, window.getTotalAcked());
    }

    @Test
    public void testAcknowledgeAlreadyAcked() {
        InFlightWindow window = new InFlightWindow(8, 1000);

        window.addInFlight(0);
        window.addInFlight(1);

        // ACK up to 1
        assertTrue(window.acknowledge(1));
        assertTrue(window.isEmpty());

        // ACK for already acknowledged sequence returns true (idempotent)
        assertTrue(window.acknowledge(0));
        assertTrue(window.acknowledge(1));
        assertTrue(window.isEmpty());
    }

    @Test
    public void testWindowFull() {
        InFlightWindow window = new InFlightWindow(3, 1000);

        // Fill the window
        window.addInFlight(0);
        window.addInFlight(1);
        window.addInFlight(2);

        assertTrue(window.isFull());
        assertEquals(3, window.getInFlightCount());

        // Free slots by ACKing
        window.acknowledgeUpTo(1);
        assertFalse(window.isFull());
        assertEquals(1, window.getInFlightCount());
    }

    @Test
    public void testWindowBlocksWhenFull() throws Exception {
        InFlightWindow window = new InFlightWindow(2, 5000);

        // Fill the window
        window.addInFlight(0);
        window.addInFlight(1);

        AtomicBoolean blocked = new AtomicBoolean(true);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);

        // Start thread that will block
        Thread addThread = new Thread(() -> {
            started.countDown();
            window.addInFlight(2);
            blocked.set(false);
            finished.countDown();
        });
        addThread.start();

        // Wait for thread to start and block
        assertTrue(started.await(1, TimeUnit.SECONDS));
        Thread.sleep(100); // Give time to block
        assertTrue(blocked.get());

        // Free a slot
        window.acknowledge(0);

        // Thread should complete
        assertTrue(finished.await(1, TimeUnit.SECONDS));
        assertFalse(blocked.get());
        assertEquals(2, window.getInFlightCount());
    }

    @Test
    public void testWindowBlocksTimeout() {
        InFlightWindow window = new InFlightWindow(2, 100); // 100ms timeout

        // Fill the window
        window.addInFlight(0);
        window.addInFlight(1);

        // Try to add another - should timeout
        long start = System.currentTimeMillis();
        try {
            window.addInFlight(2);
            fail("Expected timeout exception");
        } catch (LineSenderException e) {
            assertTrue(e.getMessage().contains("Timeout"));
        }
        long elapsed = System.currentTimeMillis() - start;
        assertTrue("Should have waited at least 100ms", elapsed >= 90);
    }

    @Test
    public void testAwaitEmpty() throws Exception {
        InFlightWindow window = new InFlightWindow(8, 5000);

        window.addInFlight(0);
        window.addInFlight(1);
        window.addInFlight(2);

        AtomicBoolean waiting = new AtomicBoolean(true);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);

        // Start thread waiting for empty
        Thread waitThread = new Thread(() -> {
            started.countDown();
            window.awaitEmpty();
            waiting.set(false);
            finished.countDown();
        });
        waitThread.start();

        assertTrue(started.await(1, TimeUnit.SECONDS));
        Thread.sleep(100);
        assertTrue(waiting.get());

        // Cumulative ACK all batches
        window.acknowledgeUpTo(2);
        assertTrue(finished.await(1, TimeUnit.SECONDS));
        assertFalse(waiting.get());
    }

    @Test
    public void testAwaitEmptyTimeout() {
        InFlightWindow window = new InFlightWindow(8, 100); // 100ms timeout

        window.addInFlight(0);

        long start = System.currentTimeMillis();
        try {
            window.awaitEmpty();
            fail("Expected timeout exception");
        } catch (LineSenderException e) {
            assertTrue(e.getMessage().contains("Timeout"));
        }
        long elapsed = System.currentTimeMillis() - start;
        assertTrue("Should have waited at least 100ms", elapsed >= 90);
    }

    @Test
    public void testAwaitEmptyAlreadyEmpty() {
        InFlightWindow window = new InFlightWindow(8, 1000);

        // Should return immediately
        window.awaitEmpty();
        assertTrue(window.isEmpty());
    }

    @Test
    public void testFailBatch() {
        InFlightWindow window = new InFlightWindow(8, 1000);

        window.addInFlight(0);
        window.addInFlight(1);

        // Fail batch 0
        RuntimeException error = new RuntimeException("Test error");
        window.fail(0, error);

        assertEquals(1, window.getTotalFailed());
        assertNotNull(window.getLastError());
    }

    @Test
    public void testFailPropagatesError() {
        InFlightWindow window = new InFlightWindow(8, 1000);

        window.addInFlight(0);
        window.fail(0, new RuntimeException("Test error"));

        // Subsequent operations should throw
        try {
            window.addInFlight(1);
            fail("Expected exception due to error");
        } catch (LineSenderException e) {
            assertTrue(e.getMessage().contains("failed"));
        }

        try {
            window.awaitEmpty();
            fail("Expected exception due to error");
        } catch (LineSenderException e) {
            assertTrue(e.getMessage().contains("failed"));
        }
    }

    @Test
    public void testFailAllPropagatesError() {
        InFlightWindow window = new InFlightWindow(8, 1000);

        window.addInFlight(0);
        window.addInFlight(1);
        window.failAll(new RuntimeException("Transport down"));

        try {
            window.awaitEmpty();
            fail("Expected exception due to failAll");
        } catch (LineSenderException e) {
            assertTrue(e.getMessage().contains("failed"));
            assertTrue(e.getMessage().contains("Transport down"));
        }
    }

    @Test
    public void testClearError() {
        InFlightWindow window = new InFlightWindow(8, 1000);

        window.addInFlight(0);
        window.fail(0, new RuntimeException("Test error"));

        assertNotNull(window.getLastError());

        window.clearError();
        assertNull(window.getLastError());

        // Should work again
        window.addInFlight(1);
        assertEquals(2, window.getInFlightCount()); // 0 and 1 both in window (fail doesn't remove)
    }

    @Test
    public void testReset() {
        InFlightWindow window = new InFlightWindow(8, 1000);

        window.addInFlight(0);
        window.addInFlight(1);
        window.fail(2, new RuntimeException("Test"));

        window.reset();

        assertTrue(window.isEmpty());
        assertNull(window.getLastError());
        assertEquals(0, window.getInFlightCount());
    }

    @Test
    public void testConcurrentAddAndAck() throws Exception {
        InFlightWindow window = new InFlightWindow(4, 5000);
        int numOperations = 100;
        CountDownLatch done = new CountDownLatch(2);
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicInteger highestAdded = new AtomicInteger(-1);

        // Sender thread
        Thread sender = new Thread(() -> {
            try {
                for (int i = 0; i < numOperations; i++) {
                    window.addInFlight(i);
                    highestAdded.set(i);
                    Thread.sleep(1); // Small delay
                }
            } catch (Throwable t) {
                error.set(t);
            } finally {
                done.countDown();
            }
        });

        // ACK thread (cumulative ACKs)
        Thread acker = new Thread(() -> {
            try {
                Thread.sleep(10); // Let sender get ahead
                int lastAcked = -1;
                while (lastAcked < numOperations - 1) {
                    int highest = highestAdded.get();
                    if (highest > lastAcked) {
                        window.acknowledgeUpTo(highest);
                        lastAcked = highest;
                    }
                    Thread.sleep(1);
                }
            } catch (Throwable t) {
                error.set(t);
            } finally {
                done.countDown();
            }
        });

        sender.start();
        acker.start();

        assertTrue(done.await(10, TimeUnit.SECONDS));
        assertNull(error.get());
        assertTrue(window.isEmpty());
        assertEquals(numOperations, window.getTotalAcked());
    }

    @Test
    public void testFailWakesBlockedAdder() throws Exception {
        InFlightWindow window = new InFlightWindow(2, 5000);

        // Fill the window
        window.addInFlight(0);
        window.addInFlight(1);

        CountDownLatch started = new CountDownLatch(1);
        AtomicReference<Throwable> caught = new AtomicReference<>();

        // Thread that will block on add
        Thread addThread = new Thread(() -> {
            started.countDown();
            try {
                window.addInFlight(2);
            } catch (LineSenderException e) {
                caught.set(e);
            }
        });
        addThread.start();

        assertTrue(started.await(1, TimeUnit.SECONDS));
        Thread.sleep(100); // Let it block

        // Fail a batch - should wake the blocked thread
        window.fail(0, new RuntimeException("Test error"));

        addThread.join(1000);
        assertFalse(addThread.isAlive());
        assertNotNull(caught.get());
        assertTrue(caught.get().getMessage().contains("failed"));
    }

    @Test
    public void testFailWakesAwaitEmpty() throws Exception {
        InFlightWindow window = new InFlightWindow(8, 5000);

        window.addInFlight(0);

        CountDownLatch started = new CountDownLatch(1);
        AtomicReference<Throwable> caught = new AtomicReference<>();

        // Thread waiting for empty
        Thread waitThread = new Thread(() -> {
            started.countDown();
            try {
                window.awaitEmpty();
            } catch (LineSenderException e) {
                caught.set(e);
            }
        });
        waitThread.start();

        assertTrue(started.await(1, TimeUnit.SECONDS));
        Thread.sleep(100); // Let it block

        // Fail a batch - should wake the blocked thread
        window.fail(0, new RuntimeException("Test error"));

        waitThread.join(1000);
        assertFalse(waitThread.isAlive());
        assertNotNull(caught.get());
        assertTrue(caught.get().getMessage().contains("failed"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidWindowSize() {
        new InFlightWindow(0, 1000);
    }

    @Test
    public void testGetMaxWindowSize() {
        InFlightWindow window = new InFlightWindow(16, 1000);
        assertEquals(16, window.getMaxWindowSize());
    }

    @Test
    public void testRapidAddAndAck() {
        InFlightWindow window = new InFlightWindow(16, 5000);

        // Rapid add and ack in same thread
        for (int i = 0; i < 10000; i++) {
            window.addInFlight(i);
            assertTrue(window.acknowledge(i));
        }

        assertTrue(window.isEmpty());
        assertEquals(10000, window.getTotalAcked());
    }

    @Test
    public void testFillAndDrainRepeatedly() {
        InFlightWindow window = new InFlightWindow(4, 1000);

        int batchId = 0;
        for (int cycle = 0; cycle < 100; cycle++) {
            // Fill
            int startBatch = batchId;
            for (int i = 0; i < 4; i++) {
                window.addInFlight(batchId++);
            }
            assertTrue(window.isFull());
            assertEquals(4, window.getInFlightCount());

            // Drain with cumulative ACK
            window.acknowledgeUpTo(batchId - 1);
            assertTrue(window.isEmpty());
        }

        assertEquals(400, window.getTotalAcked());
    }

    @Test
    public void testMultipleResets() {
        InFlightWindow window = new InFlightWindow(8, 1000);

        for (int cycle = 0; cycle < 10; cycle++) {
            window.addInFlight(cycle);
            window.reset();

            assertTrue(window.isEmpty());
            assertNull(window.getLastError());
        }
    }

    @Test
    public void testFailThenClearThenAdd() {
        InFlightWindow window = new InFlightWindow(8, 1000);

        window.addInFlight(0);
        window.fail(0, new RuntimeException("Error"));

        // Should not be able to add
        try {
            window.addInFlight(1);
            fail("Expected exception");
        } catch (LineSenderException e) {
            assertTrue(e.getMessage().contains("failed"));
        }

        // Clear error
        window.clearError();

        // Should work now
        window.addInFlight(1);
        assertEquals(2, window.getInFlightCount());
    }

    @Test
    public void testDefaultWindowSize() {
        InFlightWindow window = new InFlightWindow();
        assertEquals(InFlightWindow.DEFAULT_WINDOW_SIZE, window.getMaxWindowSize());
    }

    @Test
    public void testSmallestPossibleWindow() {
        InFlightWindow window = new InFlightWindow(1, 1000);

        window.addInFlight(0);
        assertTrue(window.isFull());

        window.acknowledge(0);
        assertFalse(window.isFull());
    }

    @Test
    public void testVeryLargeWindow() {
        InFlightWindow window = new InFlightWindow(10000, 1000);

        // Add many batches
        for (int i = 0; i < 5000; i++) {
            window.addInFlight(i);
        }
        assertEquals(5000, window.getInFlightCount());
        assertFalse(window.isFull());

        // ACK half
        window.acknowledgeUpTo(2499);
        assertEquals(2500, window.getInFlightCount());
    }

    @Test
    public void testZeroBatchId() {
        InFlightWindow window = new InFlightWindow(8, 1000);

        window.addInFlight(0);
        assertEquals(1, window.getInFlightCount());

        assertTrue(window.acknowledge(0));
        assertTrue(window.isEmpty());
    }

    // ==================== CUMULATIVE ACK TESTS ====================

    @Test
    public void testAcknowledgeUpToBasic() {
        InFlightWindow window = new InFlightWindow(16, 1000);

        // Add batches 0-9
        for (int i = 0; i < 10; i++) {
            window.addInFlight(i);
        }
        assertEquals(10, window.getInFlightCount());

        // ACK up to 5 (should remove 0-5, leaving 6-9)
        int acked = window.acknowledgeUpTo(5);
        assertEquals(6, acked);
        assertEquals(4, window.getInFlightCount());
        assertEquals(6, window.getTotalAcked());
    }

    @Test
    public void testAcknowledgeUpToIdempotent() {
        InFlightWindow window = new InFlightWindow(16, 1000);

        window.addInFlight(0);
        window.addInFlight(1);
        window.addInFlight(2);

        // First ACK
        assertEquals(3, window.acknowledgeUpTo(2));
        assertTrue(window.isEmpty());

        // Duplicate ACK - should be no-op
        assertEquals(0, window.acknowledgeUpTo(2));
        assertTrue(window.isEmpty());

        // ACK with lower sequence - should be no-op
        assertEquals(0, window.acknowledgeUpTo(1));
        assertTrue(window.isEmpty());
    }

    @Test
    public void testAcknowledgeUpToWakesBlockedAdder() throws Exception {
        InFlightWindow window = new InFlightWindow(3, 5000);

        // Fill the window
        window.addInFlight(0);
        window.addInFlight(1);
        window.addInFlight(2);
        assertTrue(window.isFull());

        AtomicBoolean blocked = new AtomicBoolean(true);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);

        // Start thread that will block
        Thread addThread = new Thread(() -> {
            started.countDown();
            window.addInFlight(3);
            blocked.set(false);
            finished.countDown();
        });
        addThread.start();

        assertTrue(started.await(1, TimeUnit.SECONDS));
        Thread.sleep(100); // Give time to block
        assertTrue(blocked.get());

        // Cumulative ACK frees multiple slots
        window.acknowledgeUpTo(1); // Removes 0 and 1

        // Thread should complete
        assertTrue(finished.await(1, TimeUnit.SECONDS));
        assertFalse(blocked.get());
        assertEquals(2, window.getInFlightCount()); // batch 2 and 3
    }

    @Test
    public void testAcknowledgeUpToWakesAwaitEmpty() throws Exception {
        InFlightWindow window = new InFlightWindow(16, 5000);

        window.addInFlight(0);
        window.addInFlight(1);
        window.addInFlight(2);

        AtomicBoolean waiting = new AtomicBoolean(true);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);

        // Start thread waiting for empty
        Thread waitThread = new Thread(() -> {
            started.countDown();
            window.awaitEmpty();
            waiting.set(false);
            finished.countDown();
        });
        waitThread.start();

        assertTrue(started.await(1, TimeUnit.SECONDS));
        Thread.sleep(100);
        assertTrue(waiting.get());

        // Single cumulative ACK clears all
        window.acknowledgeUpTo(2);

        assertTrue(finished.await(1, TimeUnit.SECONDS));
        assertFalse(waiting.get());
        assertTrue(window.isEmpty());
    }

    @Test
    public void testAcknowledgeUpToEmpty() {
        InFlightWindow window = new InFlightWindow(16, 1000);

        // ACK on empty window should be no-op
        assertEquals(0, window.acknowledgeUpTo(100));
        assertTrue(window.isEmpty());
    }

    @Test
    public void testAcknowledgeUpToAllBatches() {
        InFlightWindow window = new InFlightWindow(16, 1000);

        // Add batches
        for (int i = 0; i < 10; i++) {
            window.addInFlight(i);
        }

        // ACK all with high sequence
        int acked = window.acknowledgeUpTo(Long.MAX_VALUE);
        assertEquals(10, acked);
        assertTrue(window.isEmpty());
    }

    @Test
    public void testConcurrentAddAndCumulativeAck() throws Exception {
        InFlightWindow window = new InFlightWindow(100, 10000);
        int numBatches = 500;
        CountDownLatch done = new CountDownLatch(2);
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicInteger highestAdded = new AtomicInteger(-1);

        // Sender thread
        Thread sender = new Thread(() -> {
            try {
                for (int i = 0; i < numBatches; i++) {
                    window.addInFlight(i);
                    highestAdded.set(i);
                }
            } catch (Throwable t) {
                error.set(t);
            } finally {
                done.countDown();
            }
        });

        // ACK thread using cumulative ACKs
        Thread acker = new Thread(() -> {
            try {
                int lastAcked = -1;
                while (lastAcked < numBatches - 1) {
                    int highest = highestAdded.get();
                    if (highest > lastAcked) {
                        window.acknowledgeUpTo(highest);
                        lastAcked = highest;
                    } else {
                        Thread.sleep(1);
                    }
                }
            } catch (Throwable t) {
                error.set(t);
            } finally {
                done.countDown();
            }
        });

        sender.start();
        acker.start();

        assertTrue(done.await(30, TimeUnit.SECONDS));
        assertNull(error.get());
        assertTrue(window.isEmpty());
        assertEquals(numBatches, window.getTotalAcked());
    }

    @Test
    public void testTryAddInFlight() {
        InFlightWindow window = new InFlightWindow(2, 1000);

        // Should succeed
        assertTrue(window.tryAddInFlight(0));
        assertTrue(window.tryAddInFlight(1));

        // Should fail - window full
        assertFalse(window.tryAddInFlight(2));

        // After ACK, should succeed
        window.acknowledge(0);
        assertTrue(window.tryAddInFlight(2));
    }

    @Test
    public void testHasWindowSpace() {
        InFlightWindow window = new InFlightWindow(2, 1000);

        assertTrue(window.hasWindowSpace());
        window.addInFlight(0);
        assertTrue(window.hasWindowSpace());
        window.addInFlight(1);
        assertFalse(window.hasWindowSpace());

        window.acknowledge(0);
        assertTrue(window.hasWindowSpace());
    }

    @Test
    public void testHighConcurrencyStress() throws Exception {
        InFlightWindow window = new InFlightWindow(8, 30000);
        int numBatches = 10000;
        CountDownLatch done = new CountDownLatch(2);
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicInteger highestAdded = new AtomicInteger(-1);

        // Fast sender thread
        Thread sender = new Thread(() -> {
            try {
                for (int i = 0; i < numBatches; i++) {
                    window.addInFlight(i);
                    highestAdded.set(i);
                }
            } catch (Throwable t) {
                error.set(t);
            } finally {
                done.countDown();
            }
        });

        // Fast ACK thread
        Thread acker = new Thread(() -> {
            try {
                int lastAcked = -1;
                while (lastAcked < numBatches - 1) {
                    int highest = highestAdded.get();
                    if (highest > lastAcked) {
                        window.acknowledgeUpTo(highest);
                        lastAcked = highest;
                    }
                    // No sleep - maximum contention
                }
            } catch (Throwable t) {
                error.set(t);
            } finally {
                done.countDown();
            }
        });

        sender.start();
        acker.start();

        assertTrue(done.await(60, TimeUnit.SECONDS));
        if (error.get() != null) {
            error.get().printStackTrace();
        }
        assertNull(error.get());
        assertTrue(window.isEmpty());
        assertEquals(numBatches, window.getTotalAcked());
    }
}
