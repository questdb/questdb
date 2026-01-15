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
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Asynchronous send queue for WebSocket microbatch transmission.
 * <p>
 * This class manages a dedicated I/O thread that sends batches from a bounded queue.
 * The user thread enqueues sealed buffers, and the I/O thread sends them over the
 * WebSocket channel.
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

    // The bounded queue for pending batches
    private final ArrayBlockingQueue<MicrobatchBuffer> sendQueue;

    // The WebSocket channel for sending
    private final WebSocketChannel channel;

    // Optional InFlightWindow for tracking sent batches awaiting ACK
    @Nullable
    private final InFlightWindow inFlightWindow;

    // Optional ConnectionSymbolState for delta symbol dictionary tracking
    @Nullable
    private final ConnectionSymbolState connectionSymbolState;

    // The I/O thread for async sending
    private final Thread sendThread;

    // Running state
    private volatile boolean running;
    private volatile boolean shuttingDown;

    // Synchronization for flush/close
    private final CountDownLatch shutdownLatch;

    // Error handling
    private volatile Throwable lastError;

    // Statistics
    private final AtomicLong totalBatchesSent = new AtomicLong(0);
    private final AtomicLong totalBytesSent = new AtomicLong(0);

    // Counter for batches currently being processed by the I/O thread
    // This tracks batches that have been dequeued but not yet fully sent
    private final AtomicInteger processingCount = new AtomicInteger(0);

    // Batch sequence counter (must match server's messageSequence)
    private long nextBatchSequence = 0;

    // Configuration
    private final long enqueueTimeoutMs;
    private final long shutdownTimeoutMs;

    /**
     * Creates a new send queue with default configuration.
     *
     * @param channel the WebSocket channel to send on
     */
    public WebSocketSendQueue(WebSocketChannel channel) {
        this(channel, null, null, DEFAULT_QUEUE_CAPACITY, DEFAULT_ENQUEUE_TIMEOUT_MS, DEFAULT_SHUTDOWN_TIMEOUT_MS);
    }

    /**
     * Creates a new send queue with InFlightWindow for tracking sent batches.
     *
     * @param channel        the WebSocket channel to send on
     * @param inFlightWindow the window to track sent batches awaiting ACK (may be null)
     */
    public WebSocketSendQueue(WebSocketChannel channel, @Nullable InFlightWindow inFlightWindow) {
        this(channel, inFlightWindow, null, DEFAULT_QUEUE_CAPACITY, DEFAULT_ENQUEUE_TIMEOUT_MS, DEFAULT_SHUTDOWN_TIMEOUT_MS);
    }

    /**
     * Creates a new send queue with InFlightWindow and ConnectionSymbolState.
     *
     * @param channel               the WebSocket channel to send on
     * @param inFlightWindow        the window to track sent batches awaiting ACK (may be null)
     * @param connectionSymbolState the state for delta symbol dictionary tracking (may be null)
     */
    public WebSocketSendQueue(WebSocketChannel channel, @Nullable InFlightWindow inFlightWindow,
                              @Nullable ConnectionSymbolState connectionSymbolState) {
        this(channel, inFlightWindow, connectionSymbolState, DEFAULT_QUEUE_CAPACITY, DEFAULT_ENQUEUE_TIMEOUT_MS, DEFAULT_SHUTDOWN_TIMEOUT_MS);
    }

    /**
     * Creates a new send queue with custom configuration.
     *
     * @param channel               the WebSocket channel to send on
     * @param inFlightWindow        the window to track sent batches awaiting ACK (may be null)
     * @param connectionSymbolState the state for delta symbol dictionary tracking (may be null)
     * @param queueCapacity         maximum number of pending batches
     * @param enqueueTimeoutMs      timeout for enqueue operations (ms)
     * @param shutdownTimeoutMs     timeout for graceful shutdown (ms)
     */
    public WebSocketSendQueue(WebSocketChannel channel, @Nullable InFlightWindow inFlightWindow,
                              @Nullable ConnectionSymbolState connectionSymbolState,
                              int queueCapacity, long enqueueTimeoutMs, long shutdownTimeoutMs) {
        if (channel == null) {
            throw new IllegalArgumentException("channel cannot be null");
        }
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queueCapacity must be positive");
        }

        this.channel = channel;
        this.inFlightWindow = inFlightWindow;
        this.connectionSymbolState = connectionSymbolState;
        this.sendQueue = new ArrayBlockingQueue<>(queueCapacity);
        this.enqueueTimeoutMs = enqueueTimeoutMs;
        this.shutdownTimeoutMs = shutdownTimeoutMs;
        this.running = true;
        this.shuttingDown = false;
        this.shutdownLatch = new CountDownLatch(1);

        // Start the I/O thread
        this.sendThread = new Thread(this::sendLoop, "questdb-websocket-send");
        this.sendThread.setDaemon(true);
        this.sendThread.start();

        LOG.info().$("WebSocket send queue started [capacity=").$(queueCapacity).I$();
    }

    /**
     * Enqueues a sealed buffer for sending.
     * <p>
     * The buffer must be in SEALED state. After this method returns successfully,
     * ownership of the buffer transfers to the send queue.
     * <p>
     * This method blocks if the queue is full, providing backpressure to the caller.
     *
     * @param buffer the sealed buffer to send
     * @return true if enqueued successfully, false if timeout or shutdown
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

        try {
            boolean enqueued = sendQueue.offer(buffer, enqueueTimeoutMs, TimeUnit.MILLISECONDS);
            if (!enqueued) {
                LOG.error().$("Enqueue timeout after ").$(enqueueTimeoutMs).$("ms, queue full").I$();
                return false;
            }
            LOG.debug().$("Enqueued batch [id=").$(buffer.getBatchId())
                    .$(", bytes=").$(buffer.getBufferPos())
                    .$(", rows=").$(buffer.getRowCount()).I$();
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LineSenderException("Interrupted while enqueuing buffer", e);
        }
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

        // Wait for queue to drain AND any batch currently being processed to complete
        // This ensures all batches are registered in InFlightWindow before we return
        long startTime = System.currentTimeMillis();
        while ((!sendQueue.isEmpty() || processingCount.get() > 0) && running) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new LineSenderException("Interrupted while flushing", e);
            }

            // Check timeout
            if (System.currentTimeMillis() - startTime > enqueueTimeoutMs) {
                throw new LineSenderException("Flush timeout after " + enqueueTimeoutMs + "ms");
            }

            // Check for errors
            checkError();
        }

        LOG.debug().$("Flush complete").I$();
    }

    /**
     * Returns the number of batches waiting to be sent.
     */
    public int getPendingCount() {
        return sendQueue.size();
    }

    /**
     * Returns true if the queue is empty.
     */
    public boolean isEmpty() {
        return sendQueue.isEmpty();
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

        LOG.info().$("Closing WebSocket send queue [pending=").$(sendQueue.size()).I$();

        // Signal shutdown
        shuttingDown = true;

        // Wait for pending batches to be sent
        long startTime = System.currentTimeMillis();
        while (!sendQueue.isEmpty()) {
            if (System.currentTimeMillis() - startTime > shutdownTimeoutMs) {
                LOG.error().$("Shutdown timeout, ").$(sendQueue.size()).$(" batches not sent").I$();
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

        // Wake up I/O thread if it's blocked on take()
        sendThread.interrupt();

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
     * The main loop for the I/O thread.
     * Takes batches from the queue and sends them over the WebSocket channel.
     */
    private void sendLoop() {
        LOG.info().$("Send loop started").I$();

        try {
            while (running || !sendQueue.isEmpty()) {
                MicrobatchBuffer batch = null;
                try {
                    // Poll with timeout to allow checking running flag
                    batch = sendQueue.poll(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    if (!running) {
                        break;
                    }
                    // Continue if interrupted but still running
                    continue;
                }

                if (batch == null) {
                    // No batch available, loop back
                    continue;
                }

                // Track that we're processing a batch (for flush() synchronization)
                processingCount.incrementAndGet();
                try {
                    sendBatch(batch);
                } catch (Throwable t) {
                    LOG.error().$("Error sending batch [id=").$(batch.getBatchId()).$("]")
                            .$(t).I$();
                    lastError = t;
                    // Mark as recycled even on error to allow cleanup
                    // Buffer may be in SEALED or SENDING state depending on where error occurred
                    if (batch.isSealed()) {
                        batch.markSending();
                    }
                    if (batch.isSending()) {
                        batch.markRecycled();
                    }
                    // Don't propagate - continue processing other batches
                } finally {
                    processingCount.decrementAndGet();
                }
            }
        } finally {
            shutdownLatch.countDown();
            LOG.info().$("Send loop stopped").I$();
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
        if (inFlightWindow != null) {
            LOG.debug().$("Adding to in-flight window [seq=").$(batchSequence)
                    .$(", inFlight=").$(inFlightWindow.getInFlightCount())
                    .$(", max=").$(inFlightWindow.getMaxWindowSize()).I$();
            inFlightWindow.addInFlight(batchSequence);
            LOG.debug().$("Added to in-flight window [seq=").$(batchSequence).I$();
        }

        // Track symbol state for delta encoding
        if (connectionSymbolState != null) {
            int maxSymbolId = batch.getMaxSymbolId();
            connectionSymbolState.onBatchSent(batchSequence, maxSymbolId);
            LOG.debug().$("Tracked symbol state [seq=").$(batchSequence)
                    .$(", maxSymbolId=").$(maxSymbolId).I$();
        }

        // Send over WebSocket
        LOG.debug().$("Calling sendBinary [seq=").$(batchSequence).I$();
        channel.sendBinary(batch.getBufferPtr(), bytes);
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
}
