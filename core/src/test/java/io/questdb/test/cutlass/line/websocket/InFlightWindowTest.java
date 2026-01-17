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

import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.ilpv4.client.InFlightWindow;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class InFlightWindowTest {

    @Test
    public void testBasicAddAndAcknowledge() {
        InFlightWindow window = new InFlightWindow(8, 1000);

        assertTrue(window.isEmpty());
        assertEquals(0, window.getInFlightCount());

        // Add a batch
        window.addInFlight(1);
        assertFalse(window.isEmpty());
        assertEquals(1, window.getInFlightCount());

        // Acknowledge it
        assertTrue(window.acknowledge(1));
        assertTrue(window.isEmpty());
        assertEquals(0, window.getInFlightCount());
        assertEquals(1, window.getTotalAcked());
    }

    @Test
    public void testMultipleBatches() {
        InFlightWindow window = new InFlightWindow(8, 1000);

        // Add multiple batches
        for (long i = 1; i <= 5; i++) {
            window.addInFlight(i);
        }
        assertEquals(5, window.getInFlightCount());

        // Acknowledge in different order
        assertTrue(window.acknowledge(3));
        assertEquals(4, window.getInFlightCount());

        assertTrue(window.acknowledge(1));
        assertEquals(3, window.getInFlightCount());

        assertTrue(window.acknowledge(5));
        assertTrue(window.acknowledge(2));
        assertTrue(window.acknowledge(4));

        assertTrue(window.isEmpty());
        assertEquals(5, window.getTotalAcked());
    }

    @Test
    public void testAcknowledgeUnknownBatch() {
        InFlightWindow window = new InFlightWindow(8, 1000);

        window.addInFlight(1);

        // ACK for batch not in window
        assertFalse(window.acknowledge(999));

        // Original batch still there
        assertEquals(1, window.getInFlightCount());
    }

    @Test
    public void testWindowFull() {
        InFlightWindow window = new InFlightWindow(3, 1000);

        // Fill the window
        window.addInFlight(1);
        window.addInFlight(2);
        window.addInFlight(3);

        assertTrue(window.isFull());
        assertEquals(3, window.getInFlightCount());

        // Free one slot
        window.acknowledge(2);
        assertFalse(window.isFull());
        assertEquals(2, window.getInFlightCount());
    }

    @Test
    public void testWindowBlocksWhenFull() throws Exception {
        InFlightWindow window = new InFlightWindow(2, 5000);

        // Fill the window
        window.addInFlight(1);
        window.addInFlight(2);

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

        // Wait for thread to start and block
        assertTrue(started.await(1, TimeUnit.SECONDS));
        Thread.sleep(100); // Give time to block
        assertTrue(blocked.get());

        // Free a slot
        window.acknowledge(1);

        // Thread should complete
        assertTrue(finished.await(1, TimeUnit.SECONDS));
        assertFalse(blocked.get());
        assertEquals(2, window.getInFlightCount());
    }

    @Test
    public void testWindowBlocksTimeout() {
        InFlightWindow window = new InFlightWindow(2, 100); // 100ms timeout

        // Fill the window
        window.addInFlight(1);
        window.addInFlight(2);

        // Try to add another - should timeout
        long start = System.currentTimeMillis();
        try {
            window.addInFlight(3);
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

        window.addInFlight(1);
        window.addInFlight(2);
        window.addInFlight(3);

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

        // ACK all batches
        window.acknowledge(1);
        assertTrue(waiting.get()); // Still waiting

        window.acknowledge(2);
        assertTrue(waiting.get()); // Still waiting

        window.acknowledge(3);
        assertTrue(finished.await(1, TimeUnit.SECONDS));
        assertFalse(waiting.get());
    }

    @Test
    public void testAwaitEmptyTimeout() {
        InFlightWindow window = new InFlightWindow(8, 100); // 100ms timeout

        window.addInFlight(1);

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

        window.addInFlight(1);
        window.addInFlight(2);

        // Fail batch 1
        RuntimeException error = new RuntimeException("Test error");
        window.fail(1, error);

        assertEquals(1, window.getTotalFailed());
        assertEquals(1, window.getInFlightCount()); // Only batch 2 remains
        assertNotNull(window.getLastError());
    }

    @Test
    public void testFailPropagatesError() {
        InFlightWindow window = new InFlightWindow(8, 1000);

        window.addInFlight(1);
        window.fail(1, new RuntimeException("Test error"));

        // Subsequent operations should throw
        try {
            window.addInFlight(2);
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
    public void testClearError() {
        InFlightWindow window = new InFlightWindow(8, 1000);

        window.addInFlight(1);
        window.fail(1, new RuntimeException("Test error"));

        assertNotNull(window.getLastError());

        window.clearError();
        assertNull(window.getLastError());

        // Should work again
        window.addInFlight(2);
        assertEquals(1, window.getInFlightCount());
    }

    @Test
    public void testReset() {
        InFlightWindow window = new InFlightWindow(8, 1000);

        window.addInFlight(1);
        window.addInFlight(2);
        window.fail(3, new RuntimeException("Test"));

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

        // Sender thread
        Thread sender = new Thread(() -> {
            try {
                for (long i = 1; i <= numOperations; i++) {
                    window.addInFlight(i);
                    Thread.sleep(1); // Small delay
                }
            } catch (Throwable t) {
                error.set(t);
            } finally {
                done.countDown();
            }
        });

        // ACK thread (starts slightly delayed)
        Thread acker = new Thread(() -> {
            try {
                Thread.sleep(10); // Let sender get ahead
                for (long i = 1; i <= numOperations; i++) {
                    // Wait until batch is in window
                    while (!window.acknowledge(i)) {
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

        assertTrue(done.await(10, TimeUnit.SECONDS));
        assertNull(error.get());
        assertTrue(window.isEmpty());
        assertEquals(numOperations, window.getTotalAcked());
    }

    @Test
    public void testFailWakesBlockedAdder() throws Exception {
        InFlightWindow window = new InFlightWindow(2, 5000);

        // Fill the window
        window.addInFlight(1);
        window.addInFlight(2);

        CountDownLatch started = new CountDownLatch(1);
        AtomicReference<Throwable> caught = new AtomicReference<>();

        // Thread that will block on add
        Thread addThread = new Thread(() -> {
            started.countDown();
            try {
                window.addInFlight(3);
            } catch (LineSenderException e) {
                caught.set(e);
            }
        });
        addThread.start();

        assertTrue(started.await(1, TimeUnit.SECONDS));
        Thread.sleep(100); // Let it block

        // Fail a batch - should wake the blocked thread
        window.fail(1, new RuntimeException("Test error"));

        addThread.join(1000);
        assertFalse(addThread.isAlive());
        assertNotNull(caught.get());
        assertTrue(caught.get().getMessage().contains("failed"));
    }

    @Test
    public void testFailWakesAwaitEmpty() throws Exception {
        InFlightWindow window = new InFlightWindow(8, 5000);

        window.addInFlight(1);

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
        window.fail(1, new RuntimeException("Test error"));

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
    public void testDuplicateBatchId() {
        InFlightWindow window = new InFlightWindow(8, 1000);

        window.addInFlight(1);
        // Adding same ID again - LongHashSet ignores duplicates
        window.addInFlight(1);

        // Should still have only 1 entry
        assertEquals(1, window.getInFlightCount());
    }

    // ==================== ADDITIONAL EDGE CASE TESTS ====================

    @Test
    public void testLargeBatchIds() {
        InFlightWindow window = new InFlightWindow(8, 1000);

        // Test with large batch IDs
        long[] ids = {Long.MAX_VALUE - 1, Long.MAX_VALUE, 0, 1, Long.MAX_VALUE / 2};
        for (long id : ids) {
            window.addInFlight(id);
        }
        assertEquals(ids.length, window.getInFlightCount());

        for (long id : ids) {
            assertTrue(window.acknowledge(id));
        }
        assertTrue(window.isEmpty());
    }

    @Test
    public void testNegativeBatchIds() {
        InFlightWindow window = new InFlightWindow(8, 1000);

        // Negative IDs should work, except -1 which is used as the "no entry" sentinel
        // in the underlying LongHashSet. Use -2 instead of -1.
        window.addInFlight(-2);
        window.addInFlight(-100);
        window.addInFlight(Long.MIN_VALUE);

        assertEquals(3, window.getInFlightCount());

        assertTrue(window.acknowledge(-2));
        assertTrue(window.acknowledge(-100));
        assertTrue(window.acknowledge(Long.MIN_VALUE));

        assertTrue(window.isEmpty());
    }

    @Test
    public void testRapidAddAndAck() {
        InFlightWindow window = new InFlightWindow(16, 5000);

        // Rapid add and ack in same thread
        for (long i = 0; i < 10000; i++) {
            window.addInFlight(i);
            assertTrue(window.acknowledge(i));
        }

        assertTrue(window.isEmpty());
        assertEquals(10000, window.getTotalAcked());
    }

    @Test
    public void testFillAndDrainRepeatedly() {
        InFlightWindow window = new InFlightWindow(4, 1000);

        for (int cycle = 0; cycle < 100; cycle++) {
            // Fill
            for (int i = 0; i < 4; i++) {
                window.addInFlight(cycle * 4 + i);
            }
            assertTrue(window.isFull());
            assertEquals(4, window.getInFlightCount());

            // Drain
            for (int i = 0; i < 4; i++) {
                assertTrue(window.acknowledge(cycle * 4 + i));
            }
            assertTrue(window.isEmpty());
        }

        assertEquals(400, window.getTotalAcked());
    }

    @Test
    public void testOutOfOrderAcksStress() {
        InFlightWindow window = new InFlightWindow(100, 5000);

        // Add 100 batches
        for (long i = 0; i < 100; i++) {
            window.addInFlight(i);
        }
        assertEquals(100, window.getInFlightCount());

        // ACK in reverse order
        for (long i = 99; i >= 0; i--) {
            assertTrue(window.acknowledge(i));
        }
        assertTrue(window.isEmpty());
        assertEquals(100, window.getTotalAcked());
    }

    @Test
    public void testMixedAckAndFail() {
        InFlightWindow window = new InFlightWindow(10, 1000);

        for (long i = 0; i < 5; i++) {
            window.addInFlight(i);
        }

        // ACK some, fail some
        assertTrue(window.acknowledge(0));
        window.fail(1, new RuntimeException("Error 1"));
        assertTrue(window.acknowledge(2));

        assertEquals(2, window.getTotalAcked());
        assertEquals(1, window.getTotalFailed());
        assertNotNull(window.getLastError());
    }

    @Test
    public void testConcurrentAddAckFail() throws Exception {
        InFlightWindow window = new InFlightWindow(10, 10000);
        int iterations = 1000;
        CountDownLatch done = new CountDownLatch(3);
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicInteger addCount = new AtomicInteger(0);
        AtomicInteger ackCount = new AtomicInteger(0);

        // Add thread
        Thread adder = new Thread(() -> {
            try {
                for (long i = 0; i < iterations; i++) {
                    window.addInFlight(i);
                    addCount.incrementAndGet();
                }
            } catch (Throwable t) {
                if (!t.getMessage().contains("failed")) {
                    error.set(t);
                }
            } finally {
                done.countDown();
            }
        });

        // ACK thread (ACKs even numbers)
        Thread acker = new Thread(() -> {
            try {
                Thread.sleep(5);
                for (long i = 0; i < iterations; i += 2) {
                    // Check for error from fail() call using getLastError()
                    while (!window.acknowledge(i) && error.get() == null && window.getLastError() == null) {
                        Thread.sleep(1);
                    }
                    // Exit loop if window was failed
                    if (window.getLastError() != null) {
                        break;
                    }
                    ackCount.incrementAndGet();
                }
            } catch (Throwable t) {
                if (!t.getMessage().contains("failed")) {
                    error.set(t);
                }
            } finally {
                done.countDown();
            }
        });

        // Fail thread (fails a batch occasionally to trigger error propagation)
        Thread failer = new Thread(() -> {
            try {
                Thread.sleep(50);
                // Fail batch 101 (an odd number not ACKed by acker)
                window.fail(101, new RuntimeException("Intentional failure"));
            } catch (Throwable t) {
                // Expected
            } finally {
                done.countDown();
            }
        });

        adder.start();
        acker.start();
        failer.start();

        assertTrue(done.await(30, TimeUnit.SECONDS));

        // Either completed successfully or failed gracefully
        if (error.get() != null) {
            error.get().printStackTrace();
        }
        // We expect some adds and acks to succeed before the fail kicks in
        assertTrue(addCount.get() > 0);
        assertTrue(ackCount.get() > 0);
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

        window.addInFlight(1);
        window.fail(1, new RuntimeException("Error"));

        // Should not be able to add
        try {
            window.addInFlight(2);
            fail("Expected exception");
        } catch (LineSenderException e) {
            assertTrue(e.getMessage().contains("failed"));
        }

        // Clear error
        window.clearError();

        // Should work now
        window.addInFlight(2);
        assertEquals(1, window.getInFlightCount());
    }

    @Test
    public void testStatisticsAfterReset() {
        InFlightWindow window = new InFlightWindow(8, 1000);

        window.addInFlight(1);
        window.addInFlight(2);
        window.acknowledge(1);
        window.fail(2, new RuntimeException("Error"));

        assertEquals(1, window.getTotalAcked());
        assertEquals(1, window.getTotalFailed());

        window.reset();

        // Statistics should be preserved
        assertEquals(1, window.getTotalAcked());
        assertEquals(1, window.getTotalFailed());
    }

    @Test
    public void testDefaultWindowSize() {
        InFlightWindow window = new InFlightWindow();
        assertEquals(InFlightWindow.DEFAULT_WINDOW_SIZE, window.getMaxWindowSize());
    }

    @Test
    public void testSmallestPossibleWindow() {
        InFlightWindow window = new InFlightWindow(1, 1000);

        window.addInFlight(1);
        assertTrue(window.isFull());

        window.acknowledge(1);
        assertFalse(window.isFull());
    }

    @Test
    public void testVeryLargeWindow() {
        InFlightWindow window = new InFlightWindow(10000, 1000);

        // Add many batches
        for (long i = 0; i < 5000; i++) {
            window.addInFlight(i);
        }
        assertEquals(5000, window.getInFlightCount());
        assertFalse(window.isFull());

        // ACK half
        for (long i = 0; i < 2500; i++) {
            assertTrue(window.acknowledge(i));
        }
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
        for (long i = 0; i < 10; i++) {
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
    public void testAcknowledgeUpToNonContiguous() {
        InFlightWindow window = new InFlightWindow(16, 1000);

        // Add non-contiguous sequences
        window.addInFlight(0);
        window.addInFlight(2);
        window.addInFlight(5);
        window.addInFlight(7);
        window.addInFlight(10);

        assertEquals(5, window.getInFlightCount());

        // ACK up to 5 (should remove 0, 2, 5 - leaving 7, 10)
        int acked = window.acknowledgeUpTo(5);
        assertEquals(3, acked);
        assertEquals(2, window.getInFlightCount());
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
    public void testAcknowledgeUpToWithGaps() {
        InFlightWindow window = new InFlightWindow(16, 1000);

        // Add with large gaps
        window.addInFlight(10);
        window.addInFlight(20);
        window.addInFlight(30);
        window.addInFlight(40);

        // ACK up to 25 (should remove 10, 20)
        int acked = window.acknowledgeUpTo(25);
        assertEquals(2, acked);
        assertEquals(2, window.getInFlightCount());

        // ACK up to 100 (should remove remaining)
        acked = window.acknowledgeUpTo(100);
        assertEquals(2, acked);
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
        for (long i = 0; i < 10; i++) {
            window.addInFlight(i);
        }

        // ACK all with high sequence
        int acked = window.acknowledgeUpTo(Long.MAX_VALUE);
        assertEquals(10, acked);
        assertTrue(window.isEmpty());
    }

    @Test
    public void testAcknowledgeUpToWithNegativeSequences() {
        InFlightWindow window = new InFlightWindow(16, 1000);

        // -1 is NO_ENTRY_VALUE, so avoid it
        window.addInFlight(-100);
        window.addInFlight(-50);
        window.addInFlight(0);
        window.addInFlight(50);

        // ACK up to -25 (should remove -100, -50)
        int acked = window.acknowledgeUpTo(-25);
        assertEquals(2, acked);
        assertEquals(2, window.getInFlightCount());
    }

    @Test
    public void testAcknowledgeUpToMixedWithIndividualAck() {
        InFlightWindow window = new InFlightWindow(16, 1000);

        for (long i = 0; i < 10; i++) {
            window.addInFlight(i);
        }

        // Individual ACK
        assertTrue(window.acknowledge(5));
        assertEquals(9, window.getInFlightCount());

        // Cumulative ACK (5 already acked, should skip it)
        int acked = window.acknowledgeUpTo(7);
        assertEquals(7, acked); // 0,1,2,3,4,6,7 (5 was already removed)
        assertEquals(2, window.getInFlightCount()); // 8, 9 remain
    }

    @Test
    public void testConcurrentAddAndCumulativeAck() throws Exception {
        InFlightWindow window = new InFlightWindow(100, 10000);  // Larger window
        int numBatches = 500;  // Fewer batches
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

        // ACK thread using cumulative ACKs - more aggressive ACKing
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
}
