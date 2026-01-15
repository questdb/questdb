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

package io.questdb.cutlass.line.websocket;

import io.questdb.cutlass.line.LineSenderException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LongHashSet;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Tracks in-flight batches awaiting acknowledgment from the server.
 * <p>
 * This class implements a sliding window protocol for flow control:
 * <ul>
 *   <li>When a batch is sent, it's added to the window</li>
 *   <li>When the server ACKs a batch, it's removed from the window</li>
 *   <li>If the window is full, senders block until space is available</li>
 *   <li>flush() blocks until all in-flight batches are ACKed</li>
 * </ul>
 * <p>
 * Thread safety: This class is thread-safe. The sender thread adds batches,
 * and the response reader thread removes them.
 */
public class InFlightWindow {

    private static final Log LOG = LogFactory.getLog(InFlightWindow.class);

    public static final int DEFAULT_WINDOW_SIZE = 8;
    public static final long DEFAULT_TIMEOUT_MS = 30_000;

    private final int maxWindowSize;
    private final long timeoutMs;

    // Set of batch IDs currently in flight
    private final LongHashSet inFlightBatches;

    // Synchronization
    private final ReentrantLock lock;
    private final Condition notFull;      // Signaled when window has space
    private final Condition isEmpty;      // Signaled when window is empty

    // Error tracking
    private volatile Throwable lastError;
    private volatile long failedBatchId = -1;

    // Statistics
    private long totalAcked = 0;
    private long totalFailed = 0;

    /**
     * Creates a new InFlightWindow with default configuration.
     */
    public InFlightWindow() {
        this(DEFAULT_WINDOW_SIZE, DEFAULT_TIMEOUT_MS);
    }

    /**
     * Creates a new InFlightWindow with custom configuration.
     *
     * @param maxWindowSize maximum number of batches in flight
     * @param timeoutMs     timeout for blocking operations
     */
    public InFlightWindow(int maxWindowSize, long timeoutMs) {
        if (maxWindowSize <= 0) {
            throw new IllegalArgumentException("maxWindowSize must be positive");
        }
        this.maxWindowSize = maxWindowSize;
        this.timeoutMs = timeoutMs;
        this.inFlightBatches = new LongHashSet(maxWindowSize);
        this.lock = new ReentrantLock();
        this.notFull = lock.newCondition();
        this.isEmpty = lock.newCondition();
    }

    /**
     * Adds a batch to the in-flight window.
     * <p>
     * Blocks if the window is full until space becomes available or timeout.
     *
     * @param batchId the batch ID to track
     * @throws LineSenderException if timeout occurs or an error was reported
     */
    public void addInFlight(long batchId) {
        lock.lock();
        try {
            // Check for errors first
            checkError();

            // Wait for space in window
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (inFlightBatches.size() >= maxWindowSize) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    throw new LineSenderException("Timeout waiting for window space, window full with " +
                            inFlightBatches.size() + " batches");
                }
                try {
                    if (!notFull.await(remaining, TimeUnit.MILLISECONDS)) {
                        throw new LineSenderException("Timeout waiting for window space");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new LineSenderException("Interrupted while waiting for window space", e);
                }
                // Re-check for errors after waking
                checkError();
            }

            // Add to window
            inFlightBatches.add(batchId);
            LOG.debug().$("Added to window [batchId=").$(batchId)
                    .$(", windowSize=").$(inFlightBatches.size()).I$();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Acknowledges a batch, removing it from the in-flight window.
     * <p>
     * Called by the response reader thread when server ACKs a batch.
     *
     * @param batchId the batch ID that was acknowledged
     * @return true if the batch was in flight, false if not found
     */
    public boolean acknowledge(long batchId) {
        lock.lock();
        try {
            int idx = inFlightBatches.keyIndex(batchId);
            if (idx >= 0) {
                // Not found
                LOG.debug().$("ACK for unknown batch [batchId=").$(batchId).I$();
                return false;
            }

            inFlightBatches.removeAt(idx);
            totalAcked++;

            LOG.debug().$("Batch ACKed [batchId=").$(batchId)
                    .$(", windowSize=").$(inFlightBatches.size()).I$();

            // Signal waiters
            notFull.signal();
            if (inFlightBatches.size() == 0) {
                isEmpty.signalAll();
            }

            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Acknowledges all batches up to and including the given sequence (cumulative ACK).
     * <p>
     * This implements cumulative acknowledgment: receiving ACK(N) means all batches
     * with sequence &lt;= N have been successfully processed by the server.
     *
     * @param sequence the highest acknowledged sequence
     * @return the number of batches acknowledged
     */
    public int acknowledgeUpTo(long sequence) {
        lock.lock();
        try {
            int acknowledged = 0;
            int initialSize = inFlightBatches.size();

            // Iterate through a copy of keys since we're modifying during iteration
            // LongHashSet.get(i) returns the i-th key from its internal list
            for (int i = initialSize - 1; i >= 0; i--) {
                long key = inFlightBatches.get(i);
                if (key <= sequence) {
                    int idx = inFlightBatches.keyIndex(key);
                    if (idx < 0) {
                        inFlightBatches.removeAt(idx);
                        acknowledged++;
                    }
                }
            }

            totalAcked += acknowledged;

            if (acknowledged > 0) {
                LOG.debug().$("Cumulative ACK [upTo=").$(sequence)
                        .$(", acknowledged=").$(acknowledged)
                        .$(", remaining=").$(inFlightBatches.size()).I$();

                // Signal waiters - use signalAll since multiple slots may have freed
                notFull.signalAll();
                if (inFlightBatches.size() == 0) {
                    isEmpty.signalAll();
                }
            } else if (initialSize > 0) {
                LOG.debug().$("Cumulative ACK [upTo=").$(sequence)
                        .$(", no batches acknowledged, remaining=").$(inFlightBatches.size()).I$();
            }

            return acknowledged;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Marks a batch as failed, setting an error that will be propagated to waiters.
     *
     * @param batchId the batch ID that failed
     * @param error   the error that occurred
     */
    public void fail(long batchId, Throwable error) {
        lock.lock();
        try {
            // Remove from in-flight
            int idx = inFlightBatches.keyIndex(batchId);
            if (idx < 0) {
                inFlightBatches.removeAt(idx);
            }

            // Record error
            this.failedBatchId = batchId;
            this.lastError = error;
            totalFailed++;

            LOG.error().$("Batch failed [batchId=").$(batchId)
                    .$(", error=").$(error).I$();

            // Wake all waiters so they can see the error
            notFull.signalAll();
            isEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Waits until all in-flight batches are acknowledged.
     * <p>
     * Called by flush() to ensure all data is confirmed.
     *
     * @throws LineSenderException if timeout occurs or an error was reported
     */
    public void awaitEmpty() {
        lock.lock();
        try {
            checkError();

            long deadline = System.currentTimeMillis() + timeoutMs;
            while (inFlightBatches.size() > 0) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    throw new LineSenderException("Timeout waiting for batch acknowledgments, " +
                            inFlightBatches.size() + " batches still in flight");
                }
                try {
                    if (!isEmpty.await(remaining, TimeUnit.MILLISECONDS)) {
                        throw new LineSenderException("Timeout waiting for batch acknowledgments");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new LineSenderException("Interrupted while waiting for acknowledgments", e);
                }
                checkError();
            }

            LOG.debug().$("Window empty, all batches ACKed").I$();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the current number of batches in flight.
     */
    public int getInFlightCount() {
        lock.lock();
        try {
            return inFlightBatches.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns true if the window is empty.
     */
    public boolean isEmpty() {
        lock.lock();
        try {
            return inFlightBatches.size() == 0;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns true if the window is full.
     */
    public boolean isFull() {
        lock.lock();
        try {
            return inFlightBatches.size() >= maxWindowSize;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the maximum window size.
     */
    public int getMaxWindowSize() {
        return maxWindowSize;
    }

    /**
     * Returns the total number of batches acknowledged.
     */
    public long getTotalAcked() {
        return totalAcked;
    }

    /**
     * Returns the total number of batches that failed.
     */
    public long getTotalFailed() {
        return totalFailed;
    }

    /**
     * Returns the last error, or null if no error.
     */
    public Throwable getLastError() {
        return lastError;
    }

    /**
     * Clears the error state.
     */
    public void clearError() {
        lock.lock();
        try {
            lastError = null;
            failedBatchId = -1;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Resets the window, clearing all in-flight batches and errors.
     */
    public void reset() {
        lock.lock();
        try {
            inFlightBatches.clear();
            lastError = null;
            failedBatchId = -1;
            notFull.signalAll();
            isEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void checkError() {
        if (lastError != null) {
            throw new LineSenderException("Batch " + failedBatchId + " failed: " + lastError.getMessage(), lastError);
        }
    }
}
