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
import io.questdb.std.MemoryTag;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reads server responses from WebSocket channel and updates InFlightWindow.
 * <p>
 * This class runs a dedicated thread that:
 * <ul>
 *   <li>Reads WebSocket frames from the server</li>
 *   <li>Parses binary responses containing ACK/error status</li>
 *   <li>Updates the InFlightWindow with acknowledgments or failures</li>
 *   <li>Updates the ConnectionSymbolState for delta symbol dictionary tracking</li>
 * </ul>
 * <p>
 * Thread safety: This class is thread-safe. The reader thread processes
 * responses independently of the sender thread.
 */
public class ResponseReader implements QuietCloseable {

    private static final Log LOG = LogFactory.getLog(ResponseReader.class);

    private static final int DEFAULT_READ_TIMEOUT_MS = 100;
    private static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 5_000;

    private final WebSocketChannel channel;
    private final InFlightWindow inFlightWindow;
    private final ConnectionSymbolState connectionSymbolState;
    private final Thread readerThread;
    private final CountDownLatch shutdownLatch;
    private final WebSocketResponse response;

    // Buffer for parsing responses
    private final long parseBufferPtr;
    private final int parseBufferSize;

    // State
    private volatile boolean running;
    private volatile Throwable lastError;

    // Statistics
    private final AtomicLong totalAcks = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);

    /**
     * Creates a new response reader with symbol state tracking.
     *
     * @param channel               the WebSocket channel to read from
     * @param inFlightWindow        the window to update with acknowledgments
     * @param connectionSymbolState the state for delta symbol dictionary tracking
     */
    public ResponseReader(WebSocketChannel channel, InFlightWindow inFlightWindow,
                          ConnectionSymbolState connectionSymbolState) {
        if (channel == null) {
            throw new IllegalArgumentException("channel cannot be null");
        }
        if (inFlightWindow == null) {
            throw new IllegalArgumentException("inFlightWindow cannot be null");
        }

        this.channel = channel;
        this.inFlightWindow = inFlightWindow;
        this.connectionSymbolState = connectionSymbolState;
        this.response = new WebSocketResponse();

        // Allocate parse buffer (enough for max response)
        this.parseBufferSize = 2048;
        this.parseBufferPtr = Unsafe.malloc(parseBufferSize, MemoryTag.NATIVE_DEFAULT);

        this.running = true;
        this.shutdownLatch = new CountDownLatch(1);

        // Start reader thread
        this.readerThread = new Thread(this::readLoop, "questdb-websocket-response-reader");
        this.readerThread.setDaemon(true);
        this.readerThread.start();

        LOG.info().$("Response reader started").I$();
    }

    /**
     * Returns the last error that occurred, or null if no error.
     */
    public Throwable getLastError() {
        return lastError;
    }

    /**
     * Returns true if the reader is still running.
     */
    public boolean isRunning() {
        return running;
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

    @Override
    public void close() {
        if (!running) {
            return;
        }

        LOG.info().$("Closing response reader").I$();

        running = false;

        // Wait for reader thread to finish
        try {
            shutdownLatch.await(DEFAULT_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Free parse buffer
        if (parseBufferPtr != 0) {
            Unsafe.free(parseBufferPtr, parseBufferSize, MemoryTag.NATIVE_DEFAULT);
        }

        LOG.info().$("Response reader closed [totalAcks=").$(totalAcks.get())
                .$(", totalErrors=").$(totalErrors.get()).I$();
    }

    // ==================== Reader Thread ====================

    /**
     * Main read loop that processes incoming WebSocket frames.
     */
    private void readLoop() {
        LOG.info().$("Read loop started").I$();

        try {
            while (running && channel.isConnected()) {
                try {
                    // Non-blocking read with short timeout
                    boolean received = channel.receiveFrame(new ResponseHandlerImpl(), DEFAULT_READ_TIMEOUT_MS);
                    if (!received) {
                        // No frame available, continue polling
                        continue;
                    }
                } catch (LineSenderException e) {
                    if (running) {
                        LOG.error().$("Error reading response: ").$(e.getMessage()).I$();
                        lastError = e;
                    }
                    // Continue trying to read unless we're shutting down
                } catch (Throwable t) {
                    if (running) {
                        LOG.error().$("Unexpected error in read loop: ").$(t).I$();
                        lastError = t;
                    }
                    break;
                }
            }
        } finally {
            shutdownLatch.countDown();
            LOG.info().$("Read loop stopped").I$();
        }
    }

    /**
     * Handler for received WebSocket frames.
     */
    private class ResponseHandlerImpl implements WebSocketChannel.ResponseHandler {

        @Override
        public void onBinaryMessage(long payload, int length) {
            if (length < WebSocketResponse.MIN_RESPONSE_SIZE) {
                LOG.error().$("Response too short [length=").$(length).I$();
                return;
            }

            // Parse response from binary payload
            if (!response.readFrom(payload, length)) {
                LOG.error().$("Failed to parse response").I$();
                return;
            }

            long sequence = response.getSequence();

            if (response.isSuccess()) {
                // Cumulative ACK - acknowledge all batches up to this sequence
                int acked = inFlightWindow.acknowledgeUpTo(sequence);
                // Update symbol watermark for delta encoding
                connectionSymbolState.onBatchesAcked(sequence);
                if (acked > 0) {
                    totalAcks.addAndGet(acked);
                    LOG.debug().$("Cumulative ACK received [upTo=").$(sequence)
                            .$(", acked=").$(acked).I$();
                } else {
                    LOG.debug().$("ACK for already-acknowledged sequences [upTo=").$(sequence).I$();
                }
            } else {
                // Error - fail the batch
                String errorMessage = response.getErrorMessage();
                LOG.error().$("Error response [seq=").$(sequence)
                        .$(", status=").$(response.getStatusName())
                        .$(", error=").$(errorMessage).I$();

                LineSenderException error = new LineSenderException(
                        "Server error for batch " + sequence + ": " +
                                response.getStatusName() + " - " + errorMessage);
                inFlightWindow.fail(sequence, error);
                totalErrors.incrementAndGet();
            }
        }

        @Override
        public void onClose(int code, String reason) {
            LOG.info().$("WebSocket closed by server [code=").$(code)
                    .$(", reason=").$(reason).I$();
            running = false;
        }
    }
}
