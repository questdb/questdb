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

package io.questdb.cutlass.ilpv4.client;

import io.questdb.cutlass.line.LineSenderException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * Lock-free in-flight batch tracker for the sliding window protocol.
 * <p>
 * Concurrency model (lock-free):
 * <ul>
 *   <li><b>Async mode</b>: the WebSocket I/O thread sends and receives; it calls
 *       {@link #tryAddInFlight(long)} before send and {@link #acknowledgeUpTo(long)}
 *       on ACKs (single writer for sent and acked).</li>
 *   <li><b>Sync mode</b>: the caller thread sends and waits synchronously; it calls
 *       {@link #addInFlight(long)} (window size = 1) then waits for ACK itself on
 *       the same thread, so the window is always drained inline.</li>
 *   <li><b>Waiter</b>: in async mode the caller thread may call {@link #awaitEmpty()}
 *       during flush to wait for the window to drain; it only reads the counters and
 *       parks/unparks.</li>
 * </ul>
 * Assumptions that keep it simple and lock-free:
 * <ul>
 *   <li>Batch IDs are sequential (sender increments by 1)</li>
 *   <li>Single producer updates {@code highestSent}</li>
 *   <li>Single consumer updates {@code highestAcked}</li>
 * </ul>
 * With these constraints we can rely on volatile reads/writes (no CAS) and still
 * offer blocking waits for space/empty without protecting the counters with locks.
 */
public class InFlightWindow {

    private static final Log LOG = LogFactory.getLog(InFlightWindow.class);

    public static final int DEFAULT_WINDOW_SIZE = 8;
    public static final long DEFAULT_TIMEOUT_MS = 30_000;

    // Spin parameters
    private static final int SPIN_TRIES = 100;
    private static final long PARK_NANOS = 100_000; // 100 microseconds

    private final int maxWindowSize;
    private final long timeoutMs;

    // Core state
    // highestSent: the sequence number of the last batch added to the window
    private volatile long highestSent = -1;

    // highestAcked: the sequence number of the last acknowledged batch (cumulative)
    private volatile long highestAcked = -1;

    // Error state
    private final AtomicReference<Throwable> lastError = new AtomicReference<>();
    private volatile long failedBatchId = -1;

    // Thread waiting for space (sender thread)
    private volatile Thread waitingForSpace;

    // Thread waiting for empty (flush thread)
    private volatile Thread waitingForEmpty;

    // Statistics (not strictly accurate under contention, but good enough for monitoring)
    private volatile long totalAcked = 0;
    private volatile long totalFailed = 0;

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
    }

    /**
     * Checks if there's space in the window for another batch.
     * Wait-free operation.
     *
     * @return true if there's space, false if window is full
     */
    public boolean hasWindowSpace() {
        return getInFlightCount() < maxWindowSize;
    }

    /**
     * Tries to add a batch to the in-flight window without blocking.
     * Lock-free, assuming single producer for highestSent.
     *
     * Called by: async producer (WebSocket I/O thread) before sending a batch.
     * @param batchId the batch ID to track (must be sequential)
     * @return true if added, false if window is full
     */
    public boolean tryAddInFlight(long batchId) {
        // Check window space first
        long sent = highestSent;
        long acked = highestAcked;

        if (sent - acked >= maxWindowSize) {
            return false;
        }

        // Sequential caller: just publish the new highestSent
        highestSent = batchId;

        LOG.debug().$("Added to window [batchId=").$(batchId)
                .$(", windowSize=").$(getInFlightCount()).I$();
        return true;
    }

    /**
     * Adds a batch to the in-flight window.
     * <p>
     * Blocks if the window is full until space becomes available or timeout.
     * Uses spin-wait with exponential backoff, then parks. Blocking is only expected
     * in modes where another actor can make progress on acknowledgments. In normal
     * sync usage the window size is 1 and the same thread immediately waits for the
     * ACK, so this should never actually park. If a caller uses a larger window here
     * it must ensure ACKs are processed on another thread; a single-threaded caller
     * with window>1 would deadlock by parking while also being the only thread that
     * can advance {@link #acknowledgeUpTo(long)}.
     *
     * Called by: sync sender thread before sending a batch (window=1).
     * @param batchId the batch ID to track
     * @throws LineSenderException if timeout occurs or an error was reported
     */
    public void addInFlight(long batchId) {
        // Check for errors first
        checkError();

        // Fast path: try to add without waiting
        if (tryAddInFlightInternal(batchId)) {
            return;
        }

        // Slow path: need to wait for space
        long deadline = System.currentTimeMillis() + timeoutMs;
        int spins = 0;

        // Register as waiting thread
        waitingForSpace = Thread.currentThread();
        try {
            while (true) {
                // Check for errors
                checkError();

                // Try to add
                if (tryAddInFlightInternal(batchId)) {
                    return;
                }

                // Check timeout
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    throw new LineSenderException("Timeout waiting for window space, window full with " +
                            getInFlightCount() + " batches");
                }

                // Spin or park
                if (spins < SPIN_TRIES) {
                    Thread.onSpinWait();
                    spins++;
                } else {
                    // Park with timeout
                    LockSupport.parkNanos(Math.min(PARK_NANOS, remaining * 1_000_000));
                    if (Thread.interrupted()) {
                        throw new LineSenderException("Interrupted while waiting for window space");
                    }
                }
            }
        } finally {
            waitingForSpace = null;
        }
    }

    private boolean tryAddInFlightInternal(long batchId) {
        long sent = highestSent;
        long acked = highestAcked;

        if (sent - acked >= maxWindowSize) {
            return false;
        }

        // For sequential IDs, we just update highestSent
        // The caller guarantees batchId is the next in sequence
        highestSent = batchId;

        LOG.debug().$("Added to window [batchId=").$(batchId)
                .$(", windowSize=").$(getInFlightCount()).I$();
        return true;
    }

    /**
     * Acknowledges a batch, removing it from the in-flight window.
     * <p>
     * For sequential batch IDs, this is a cumulative acknowledgment -
     * acknowledging batch N means all batches up to N are acknowledged.
     *
     * Called by: acker (WebSocket I/O thread) after receiving an ACK.
     * @param batchId the batch ID that was acknowledged
     * @return true if the batch was in flight, false if already acknowledged
     */
    public boolean acknowledge(long batchId) {
        return acknowledgeUpTo(batchId) > 0 || highestAcked >= batchId;
    }

    /**
     * Acknowledges all batches up to and including the given sequence (cumulative ACK).
     * Lock-free with single consumer.
     *
     * Called by: acker (WebSocket I/O thread) after receiving an ACK.
     * @param sequence the highest acknowledged sequence
     * @return the number of batches acknowledged
     */
    public int acknowledgeUpTo(long sequence) {
        long sent = highestSent;

        // Nothing to acknowledge if window is empty or sequence is beyond what's sent
        if (sent < 0) {
            return 0; // No batches have been sent
        }

        // Cap sequence at highestSent - can't acknowledge what hasn't been sent
        long effectiveSequence = Math.min(sequence, sent);

        long prevAcked = highestAcked;
        if (effectiveSequence <= prevAcked) {
            // Already acknowledged up to this point
            return 0;
        }
        highestAcked = effectiveSequence;

        int acknowledged = (int) (effectiveSequence - prevAcked);
        totalAcked += acknowledged;

        LOG.debug().$("Cumulative ACK [upTo=").$(sequence)
                .$(", acknowledged=").$(acknowledged)
                .$(", remaining=").$(getInFlightCount()).I$();

        // Wake up waiting threads
        Thread waiter = waitingForSpace;
        if (waiter != null) {
            LockSupport.unpark(waiter);
        }

        waiter = waitingForEmpty;
        if (waiter != null && getInFlightCount() == 0) {
            LockSupport.unpark(waiter);
        }

        return acknowledged;
    }

    /**
     * Marks a batch as failed, setting an error that will be propagated to waiters.
     *
     * Called by: acker (WebSocket I/O thread) on error response or send failure.
     * @param batchId the batch ID that failed
     * @param error   the error that occurred
     */
    public void fail(long batchId, Throwable error) {
        this.failedBatchId = batchId;
        this.lastError.set(error);
        totalFailed++;

        LOG.error().$("Batch failed [batchId=").$(batchId)
                .$(", error=").$(error).I$();

        wakeWaiters();
    }

    /**
     * Marks all currently in-flight batches as failed.
     * <p>
     * Used for transport-level failures (disconnect/protocol violation) where
     * no further ACKs are expected and all waiters must be released.
     *
     * @param error terminal error to propagate
     */
    public void failAll(Throwable error) {
        long sent = highestSent;
        long acked = highestAcked;
        long inFlight = Math.max(0, sent - acked);

        this.failedBatchId = sent;
        this.lastError.set(error);
        totalFailed += Math.max(1, inFlight);

        LOG.error().$("All in-flight batches failed [inFlight=").$(inFlight)
                .$(", error=").$(error).I$();

        wakeWaiters();
    }

    /**
     * Waits until all in-flight batches are acknowledged.
     * <p>
     * Called by flush() to ensure all data is confirmed.
     *
     * Called by: waiter (flush thread), while producer/acker thread progresses.
     * @throws LineSenderException if timeout occurs or an error was reported
     */
    public void awaitEmpty() {
        checkError();

        // Fast path: already empty
        if (getInFlightCount() == 0) {
            LOG.debug().$("Window already empty").I$();
            return;
        }

        long deadline = System.currentTimeMillis() + timeoutMs;
        int spins = 0;

        // Register as waiting thread
        waitingForEmpty = Thread.currentThread();
        try {
            while (getInFlightCount() > 0) {
                checkError();

                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    throw new LineSenderException("Timeout waiting for batch acknowledgments, " +
                            getInFlightCount() + " batches still in flight");
                }

                if (spins < SPIN_TRIES) {
                    Thread.onSpinWait();
                    spins++;
                } else {
                    LockSupport.parkNanos(Math.min(PARK_NANOS, remaining * 1_000_000));
                    if (Thread.interrupted()) {
                        throw new LineSenderException("Interrupted while waiting for acknowledgments");
                    }
                }
            }

            LOG.debug().$("Window empty, all batches ACKed").I$();
        } finally {
            waitingForEmpty = null;
        }
    }

    /**
     * Returns the current number of batches in flight.
     * Wait-free operation.
     */
    public int getInFlightCount() {
        long sent = highestSent;
        long acked = highestAcked;
        // Ensure non-negative (can happen during initialization)
        return (int) Math.max(0, sent - acked);
    }

    /**
     * Returns true if the window is empty.
     * Wait-free operation.
     */
    public boolean isEmpty() {
        return getInFlightCount() == 0;
    }

    /**
     * Returns true if the window is full.
     * Wait-free operation.
     */
    public boolean isFull() {
        return getInFlightCount() >= maxWindowSize;
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
        return lastError.get();
    }

    /**
     * Clears the error state.
     */
    public void clearError() {
        lastError.set(null);
        failedBatchId = -1;
    }

    /**
     * Resets the window, clearing all state.
     */
    public void reset() {
        highestSent = -1;
        highestAcked = -1;
        lastError.set(null);
        failedBatchId = -1;

        wakeWaiters();
    }

    private void checkError() {
        Throwable error = lastError.get();
        if (error != null) {
            throw new LineSenderException("Batch " + failedBatchId + " failed: " + error.getMessage(), error);
        }
    }

    private void wakeWaiters() {
        Thread waiter = waitingForSpace;
        if (waiter != null) {
            LockSupport.unpark(waiter);
        }
        waiter = waitingForEmpty;
        if (waiter != null) {
            LockSupport.unpark(waiter);
        }
    }
}
