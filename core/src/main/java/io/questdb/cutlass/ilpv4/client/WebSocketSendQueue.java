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

import io.questdb.cutlass.http.client.WebSocketClient;
import io.questdb.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Asynchronous I/O handler for WebSocket microbatch transmission.
 * <p>
 * This class manages a dedicated I/O thread that handles both:
 * <ul>
 *   <li>Sending batches from a bounded queue</li>
 *   <li>Receiving and processing server ACK responses</li>
 * </ul>
 * Using a single thread eliminates concurrency issues with the WebSocket channel.
 * <p>
 * Thread safety:
 * <ul>
 *   <li>The send queue is thread-safe for concurrent access</li>
 *   <li>Only the I/O thread interacts with the WebSocket channel</li>
 *   <li>Buffer state transitions ensure safe hand-over</li>
 * </ul>
 * <p>
 * Backpressure:
 * <ul>
 *   <li>When the queue is full, {@link #enqueue} blocks</li>
 *   <li>This propagates backpressure to the user thread</li>
 * </ul>
 */
public class WebSocketSendQueue implements QuietCloseable {

    private static final Log LOG = LogFactory.getLog(WebSocketSendQueue.class);

    // Default configuration
    public static final int DEFAULT_QUEUE_CAPACITY = 16;
    public static final long DEFAULT_ENQUEUE_TIMEOUT_MS = 30_000;
    public static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 10_000;

    // Single pending buffer slot (double-buffering means at most 1 item in queue)
    // Zero allocation - just a volatile reference handoff
    private volatile MicrobatchBuffer pendingBuffer;

    // The WebSocket client for I/O (single-threaded access only)
    private final WebSocketClient client;

    // Optional InFlightWindow for tracking sent batches awaiting ACK
    @Nullable
    private final InFlightWindow inFlightWindow;

    // The I/O thread for async send/receive
    private final Thread ioThread;

    // Running state
    private volatile boolean running;
    private volatile boolean shuttingDown;

    // Synchronization for flush/close
    private final CountDownLatch shutdownLatch;

    // Error handling
    private volatile Throwable lastError;

    // Statistics - sending
    private final AtomicLong totalBatchesSent = new AtomicLong(0);
    private final AtomicLong totalBytesSent = new AtomicLong(0);

    // Statistics - receiving
    private final AtomicLong totalAcks = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);

    // Counter for batches currently being processed by the I/O thread
    // This tracks batches that have been dequeued but not yet fully sent
    private final AtomicInteger processingCount = new AtomicInteger(0);

    // Lock for all coordination between user thread and I/O thread.
    // Used for: queue poll + processingCount increment atomicity,
    // flush() waiting, I/O thread waiting when idle.
    private final Object processingLock = new Object();

    // Batch sequence counter (must match server's messageSequence)
    private long nextBatchSequence = 0;

    // Response parsing
    private final WebSocketResponse response = new WebSocketResponse();
    private final ResponseHandler responseHandler = new ResponseHandler();

    // Configuration
    private final long enqueueTimeoutMs;
    private final long shutdownTimeoutMs;

    // ==================== Pending Buffer Operations (zero allocation) ====================

    private boolean offerPending(MicrobatchBuffer buffer) {
        if (pendingBuffer != null) {
            return false; // slot occupied
        }
        pendingBuffer = buffer;
        return true;
    }

    private MicrobatchBuffer pollPending() {
        MicrobatchBuffer buffer = pendingBuffer;
        if (buffer != null) {
            pendingBuffer = null;
        }
        return buffer;
    }

    private boolean isPendingEmpty() {
        return pendingBuffer == null;
    }

    private int getPendingSize() {
        return pendingBuffer == null ? 0 : 1;
    }

    /**
     * Creates a new send queue with default configuration.
     *
     * @param client the WebSocket client for I/O
     */
    public WebSocketSendQueue(WebSocketClient client) {
        this(client, null, DEFAULT_QUEUE_CAPACITY, DEFAULT_ENQUEUE_TIMEOUT_MS, DEFAULT_SHUTDOWN_TIMEOUT_MS);
    }

    /**
     * Creates a new send queue with InFlightWindow for tracking sent batches.
     *
     * @param client         the WebSocket client for I/O
     * @param inFlightWindow the window to track sent batches awaiting ACK (may be null)
     */
    public WebSocketSendQueue(WebSocketClient client, @Nullable InFlightWindow inFlightWindow) {
        this(client, inFlightWindow, DEFAULT_QUEUE_CAPACITY, DEFAULT_ENQUEUE_TIMEOUT_MS, DEFAULT_SHUTDOWN_TIMEOUT_MS);
    }

    /**
     * Creates a new send queue with custom configuration.
     *
     * @param client            the WebSocket client for I/O
     * @param inFlightWindow    the window to track sent batches awaiting ACK (may be null)
     * @param queueCapacity     maximum number of pending batches
     * @param enqueueTimeoutMs  timeout for enqueue operations (ms)
     * @param shutdownTimeoutMs timeout for graceful shutdown (ms)
     */
    public WebSocketSendQueue(WebSocketClient client, @Nullable InFlightWindow inFlightWindow,
                              int queueCapacity, long enqueueTimeoutMs, long shutdownTimeoutMs) {
        if (client == null) {
            throw new IllegalArgumentException("client cannot be null");
        }
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queueCapacity must be positive");
        }

        this.client = client;
        this.inFlightWindow = inFlightWindow;
        this.enqueueTimeoutMs = enqueueTimeoutMs;
        this.shutdownTimeoutMs = shutdownTimeoutMs;
        this.running = true;
        this.shuttingDown = false;
        this.shutdownLatch = new CountDownLatch(1);

        // Start the I/O thread (handles both sending and receiving)
        this.ioThread = new Thread(this::ioLoop, "questdb-websocket-io");
        this.ioThread.setDaemon(true);
        this.ioThread.start();

        LOG.info().$("WebSocket I/O thread started [capacity=").$(queueCapacity).I$();
    }

    /**
     * Enqueues a sealed buffer for sending.
     * <p>
     * The buffer must be in SEALED state. After this method returns successfully,
     * ownership of the buffer transfers to the send queue.
     *
     * @param buffer the sealed buffer to send
     * @return true if enqueued successfully
     * @throws LineSenderException if the buffer is not sealed or an error occurred
     */
    public boolean enqueue(MicrobatchBuffer buffer) {
        if (buffer == null) {
            throw new IllegalArgumentException("buffer cannot be null");
        }
        if (!buffer.isSealed()) {
            throw new LineSenderException("Buffer must be sealed before enqueue, state=" +
                    MicrobatchBuffer.stateName(buffer.getState()));
        }
        if (!running || shuttingDown) {
            throw new LineSenderException("Send queue is not running");
        }

        // Check for errors from I/O thread
        checkError();

        final long deadline = System.currentTimeMillis() + enqueueTimeoutMs;
        synchronized (processingLock) {
            while (true) {
                if (!running || shuttingDown) {
                    throw new LineSenderException("Send queue is not running");
                }
                checkError();

                if (offerPending(buffer)) {
                    processingLock.notifyAll();
                    break;
                }

                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    throw new LineSenderException("Enqueue timeout after " + enqueueTimeoutMs + "ms");
                }
                try {
                    processingLock.wait(Math.min(10, remaining));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new LineSenderException("Interrupted while enqueueing", e);
                }
            }
        }

        LOG.debug().$("Enqueued batch [id=").$(buffer.getBatchId())
                .$(", bytes=").$(buffer.getBufferPos())
                .$(", rows=").$(buffer.getRowCount()).I$();
        return true;
    }

    /**
     * Waits for all pending batches to be sent.
     * <p>
     * This method blocks until the queue is empty and all in-flight sends complete.
     * It does not close the queue - new batches can still be enqueued after flush.
     *
     * @throws LineSenderException if an error occurs during flush
     */
    public void flush() {
        checkError();

        long startTime = System.currentTimeMillis();

        // Wait under lock - I/O thread will notify when processingCount decrements
        synchronized (processingLock) {
            while (running) {
                // Atomically check: queue empty AND not processing
                if (isPendingEmpty() && processingCount.get() == 0) {
                    break; // All done
                }

                try {
                    processingLock.wait(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new LineSenderException("Interrupted while flushing", e);
                }

                // Check timeout
                if (System.currentTimeMillis() - startTime > enqueueTimeoutMs) {
                    throw new LineSenderException("Flush timeout after " + enqueueTimeoutMs + "ms, " +
                            "queue=" + getPendingSize() + ", processing=" + processingCount.get());
                }

                // Check for errors
                checkError();
            }
        }

        // If loop exited because running=false we still need to surface the root cause.
        checkError();

        LOG.debug().$("Flush complete").I$();
    }

    /**
     * Returns the number of batches waiting to be sent.
     */
    public int getPendingCount() {
        return getPendingSize();
    }

    /**
     * Returns true if the queue is empty.
     */
    public boolean isEmpty() {
        return isPendingEmpty();
    }

    /**
     * Returns true if the queue is still running.
     */
    public boolean isRunning() {
        return running && !shuttingDown;
    }

    /**
     * Returns the total number of batches sent.
     */
    public long getTotalBatchesSent() {
        return totalBatchesSent.get();
    }

    /**
     * Returns the total number of bytes sent.
     */
    public long getTotalBytesSent() {
        return totalBytesSent.get();
    }

    /**
     * Returns the last error that occurred in the I/O thread, or null if no error.
     */
    public Throwable getLastError() {
        return lastError;
    }

    /**
     * Closes the send queue gracefully.
     * <p>
     * This method:
     * 1. Stops accepting new batches
     * 2. Waits for pending batches to be sent
     * 3. Stops the I/O thread
     * <p>
     * Note: This does NOT close the WebSocket channel - that's the caller's responsibility.
     */
    @Override
    public void close() {
        if (!running) {
            return;
        }

        LOG.info().$("Closing WebSocket send queue [pending=").$(getPendingSize()).I$();

        // Signal shutdown
        shuttingDown = true;

        // Wait for pending batches to be sent
        long startTime = System.currentTimeMillis();
        while (!isPendingEmpty()) {
            if (System.currentTimeMillis() - startTime > shutdownTimeoutMs) {
                LOG.error().$("Shutdown timeout, ").$(getPendingSize()).$(" batches not sent").I$();
                break;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // Stop the I/O thread
        running = false;

        // Wake up I/O thread if it's blocked on processingLock.wait()
        synchronized (processingLock) {
            processingLock.notifyAll();
        }
        ioThread.interrupt();

        // Wait for I/O thread to finish
        try {
            shutdownLatch.await(shutdownTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        LOG.info().$("WebSocket send queue closed [totalBatches=").$(totalBatchesSent.get())
                .$(", totalBytes=").$(totalBytesSent.get()).I$();
    }

    // ==================== I/O Thread ====================

    /**
     * I/O loop states for the state machine.
     * <ul>
     *   <li>IDLE: queue empty, no in-flight batches - can block waiting for work</li>
     *   <li>ACTIVE: have batches to send - non-blocking loop</li>
     *   <li>DRAINING: queue empty but ACKs pending - poll for ACKs, short wait</li>
     * </ul>
     */
    private enum IoState {
        IDLE, ACTIVE, DRAINING
    }

    /**
     * The main I/O loop that handles both sending batches and receiving ACKs.
     * <p>
     * Uses a state machine:
     * <ul>
     *   <li>IDLE: block on processingLock.wait() until work arrives</li>
     *   <li>ACTIVE: non-blocking poll queue, send batches, check for ACKs</li>
     *   <li>DRAINING: no batches but ACKs pending - poll for ACKs with short wait</li>
     * </ul>
     */
    private void ioLoop() {
        LOG.info().$("I/O loop started").I$();

        try {
            while (running || !isPendingEmpty()) {
                MicrobatchBuffer batch = null;
                boolean hasInFlight = (inFlightWindow != null && inFlightWindow.getInFlightCount() > 0);
                IoState state = computeState(hasInFlight);

                switch (state) {
                    case IDLE:
                        // Nothing to do - wait for work under lock
                        synchronized (processingLock) {
                            // Re-check under lock to avoid missed wakeup
                            if (isPendingEmpty() && running) {
                                try {
                                    processingLock.wait(100);
                                } catch (InterruptedException e) {
                                    if (!running) return;
                                }
                            }
                        }
                        break;

                    case ACTIVE:
                    case DRAINING:
                        // Try to receive any pending ACKs (non-blocking)
                        if (hasInFlight && client.isConnected()) {
                            tryReceiveAcks();
                        }

                        // Try to dequeue and send a batch
                        boolean hasWindowSpace = (inFlightWindow == null || inFlightWindow.hasWindowSpace());
                        if (hasWindowSpace) {
                            // Atomically: poll queue + increment processingCount
                            synchronized (processingLock) {
                                batch = pollPending();
                                if (batch != null) {
                                    processingCount.incrementAndGet();
                                }
                            }

                            if (batch != null) {
                                try {
                                    safeSendBatch(batch);
                                } finally {
                                    // Atomically: decrement + notify flush()
                                    synchronized (processingLock) {
                                        processingCount.decrementAndGet();
                                        processingLock.notifyAll();
                                    }
                                }
                            }
                        }

                        // In DRAINING state with no work, short wait to avoid busy loop
                        if (state == IoState.DRAINING && batch == null) {
                            synchronized (processingLock) {
                                try {
                                    processingLock.wait(10);
                                } catch (InterruptedException e) {
                                    if (!running) return;
                                }
                            }
                        }
                        break;
                }
            }
        } finally {
            shutdownLatch.countDown();
            LOG.info().$("I/O loop stopped [totalAcks=").$(totalAcks.get())
                    .$(", totalErrors=").$(totalErrors.get()).I$();
        }
    }

    /**
     * Computes the current I/O state based on queue and in-flight status.
     */
    private IoState computeState(boolean hasInFlight) {
        if (!isPendingEmpty()) {
            return IoState.ACTIVE;
        } else if (hasInFlight) {
            return IoState.DRAINING;
        } else {
            return IoState.IDLE;
        }
    }

    /**
     * Tries to receive ACKs from the server (non-blocking).
     */
    private void tryReceiveAcks() {
        try {
            client.tryReceiveFrame(responseHandler);
        } catch (Exception e) {
            if (running) {
                LOG.error().$("Error receiving response: ").$(e.getMessage()).I$();
                failTransport(new LineSenderException("Error receiving response: " + e.getMessage(), e));
            }
        }
    }

    /**
     * Sends a batch with error handling. Does NOT manage processingCount.
     */
    private void safeSendBatch(MicrobatchBuffer batch) {
        try {
            sendBatch(batch);
        } catch (Throwable t) {
            LOG.error().$("Error sending batch [id=").$(batch.getBatchId()).$("]")
                    .$(t).I$();
            failTransport(new LineSenderException("Error sending batch " + batch.getBatchId() + ": " + t.getMessage(), t));
            // Mark as recycled even on error to allow cleanup
            if (batch.isSealed()) {
                batch.markSending();
            }
            if (batch.isSending()) {
                batch.markRecycled();
            }
        }
    }

    /**
     * Sends a single batch over the WebSocket channel.
     */
    private void sendBatch(MicrobatchBuffer batch) {
        // Transition state: SEALED -> SENDING
        batch.markSending();

        // Use our own sequence counter (must match server's messageSequence)
        long batchSequence = nextBatchSequence++;
        int bytes = batch.getBufferPos();
        int rows = batch.getRowCount();

        LOG.debug().$("Sending batch [seq=").$(batchSequence)
                .$(", bytes=").$(bytes)
                .$(", rows=").$(rows)
                .$(", bufferId=").$(batch.getBatchId()).I$();

        // Add to in-flight window BEFORE sending (so we're ready for ACK)
        // Use non-blocking tryAddInFlight since we already checked window space in ioLoop
        if (inFlightWindow != null) {
            LOG.debug().$("Adding to in-flight window [seq=").$(batchSequence)
                    .$(", inFlight=").$(inFlightWindow.getInFlightCount())
                    .$(", max=").$(inFlightWindow.getMaxWindowSize()).I$();
            if (!inFlightWindow.tryAddInFlight(batchSequence)) {
                // Should not happen since we checked hasWindowSpace before polling
                throw new LineSenderException("In-flight window unexpectedly full");
            }
            LOG.debug().$("Added to in-flight window [seq=").$(batchSequence).I$();
        }

        // Send over WebSocket
        LOG.debug().$("Calling sendBinary [seq=").$(batchSequence).I$();
        client.sendBinary(batch.getBufferPtr(), bytes);
        LOG.debug().$("sendBinary returned [seq=").$(batchSequence).I$();

        // Update statistics
        totalBatchesSent.incrementAndGet();
        totalBytesSent.addAndGet(bytes);

        // Transition state: SENDING -> RECYCLED
        batch.markRecycled();

        LOG.debug().$("Batch sent and recycled [seq=").$(batchSequence)
                .$(", bufferId=").$(batch.getBatchId()).I$();
    }

    /**
     * Checks if an error occurred in the I/O thread and throws if so.
     */
    private void checkError() {
        Throwable error = lastError;
        if (error != null) {
            throw new LineSenderException("Error in send queue I/O thread: " + error.getMessage(), error);
        }
    }

    private void failTransport(LineSenderException error) {
        Throwable rootError = lastError;
        if (rootError == null) {
            lastError = error;
            rootError = error;
        }
        running = false;
        shuttingDown = true;
        if (inFlightWindow != null) {
            inFlightWindow.failAll(rootError);
        }
        synchronized (processingLock) {
            MicrobatchBuffer dropped = pollPending();
            if (dropped != null) {
                if (dropped.isSealed()) {
                    dropped.markSending();
                }
                if (dropped.isSending()) {
                    dropped.markRecycled();
                }
            }
            processingLock.notifyAll();
        }
    }

    /**
     * Returns total successful acknowledgments received.
     */
    public long getTotalAcks() {
        return totalAcks.get();
    }

    /**
     * Returns total error responses received.
     */
    public long getTotalErrors() {
        return totalErrors.get();
    }

    // ==================== Response Handler ====================

    /**
     * Handler for received WebSocket frames (ACKs from server).
     */
    private class ResponseHandler implements WebSocketFrameHandler {

        @Override
        public void onBinaryMessage(long payloadPtr, int payloadLen) {
            if (!WebSocketResponse.isStructurallyValid(payloadPtr, payloadLen)) {
                LineSenderException error = new LineSenderException(
                        "Invalid ACK response payload [length=" + payloadLen + ']'
                );
                LOG.error().$("Invalid ACK response payload [length=").$(payloadLen).I$();
                failTransport(error);
                return;
            }

            // Parse response from binary payload
            if (!response.readFrom(payloadPtr, payloadLen)) {
                LineSenderException error = new LineSenderException("Failed to parse ACK response");
                LOG.error().$("Failed to parse response").I$();
                failTransport(error);
                return;
            }

            long sequence = response.getSequence();

            if (response.isSuccess()) {
                // Cumulative ACK - acknowledge all batches up to this sequence
                if (inFlightWindow != null) {
                    int acked = inFlightWindow.acknowledgeUpTo(sequence);
                    if (acked > 0) {
                        totalAcks.addAndGet(acked);
                        LOG.debug().$("Cumulative ACK received [upTo=").$(sequence)
                                .$(", acked=").$(acked).I$();
                    } else {
                        LOG.debug().$("ACK for already-acknowledged sequences [upTo=").$(sequence).I$();
                    }
                }
            } else {
                // Error - fail the batch
                String errorMessage = response.getErrorMessage();
                LOG.error().$("Error response [seq=").$(sequence)
                        .$(", status=").$(response.getStatusName())
                        .$(", error=").$(errorMessage).I$();

                if (inFlightWindow != null) {
                    LineSenderException error = new LineSenderException(
                            "Server error for batch " + sequence + ": " +
                                    response.getStatusName() + " - " + errorMessage);
                    inFlightWindow.fail(sequence, error);
                }
                totalErrors.incrementAndGet();
            }
        }

        @Override
        public void onClose(int code, String reason) {
            LOG.info().$("WebSocket closed by server [code=").$(code)
                    .$(", reason=").$(reason).I$();
            failTransport(new LineSenderException("WebSocket closed by server [code=" + code + ", reason=" + reason + ']'));
        }
    }
}
