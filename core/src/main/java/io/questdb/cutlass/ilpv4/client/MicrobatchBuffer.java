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

import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.MemoryTag;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;

import java.util.concurrent.TimeUnit;

/**
 * A buffer for accumulating ILP data into microbatches before sending.
 * <p>
 * This class implements a state machine for buffer lifecycle management in the
 * double-buffering scheme used by {@link IlpV4WebSocketSender}:
 * <pre>
 * Buffer States:
 * ┌─────────────┐    seal()     ┌─────────────┐    markSending()  ┌─────────────┐
 * │   FILLING   │──────────────►│   SEALED    │──────────────────►│   SENDING   │
 * │ (user owns) │               │ (in queue)  │                   │ (I/O owns)  │
 * └─────────────┘               └─────────────┘                   └──────┬──────┘
 *        ▲                                                               │
 *        │                         markRecycled()                        │
 *        └───────────────────────────────────────────────────────────────┘
 *                              (after send complete)
 * </pre>
 * <p>
 * Thread safety: This class is NOT thread-safe for concurrent writes. However, it
 * supports safe hand-over between user thread and I/O thread through the state
 * machine. State transitions use volatile fields to ensure visibility.
 */
public class MicrobatchBuffer implements QuietCloseable {

    // Buffer states
    public static final int STATE_FILLING = 0;
    public static final int STATE_SEALED = 1;
    public static final int STATE_SENDING = 2;
    public static final int STATE_RECYCLED = 3;

    // Flush trigger thresholds
    private final int maxRows;
    private final int maxBytes;
    private final long maxAgeNanos;

    // Native memory buffer
    private long bufferPtr;
    private int bufferCapacity;
    private int bufferPos;

    // Row tracking
    private int rowCount;
    private long firstRowTimeNanos;

    // Symbol tracking for delta encoding
    private int maxSymbolId = -1;

    // Batch identification
    private long batchId;
    private static long nextBatchId = 0;

    // State machine
    private volatile int state = STATE_FILLING;

    // For waiting on recycle (user thread waits for I/O thread to finish)
    // SOCountDownLatch is resettable via setCount(), avoiding per-microbatch allocation
    private final SOCountDownLatch recycleLatch = new SOCountDownLatch(1);

    /**
     * Creates a new MicrobatchBuffer with specified flush thresholds.
     *
     * @param initialCapacity initial buffer size in bytes
     * @param maxRows         maximum rows before auto-flush (0 = unlimited)
     * @param maxBytes        maximum bytes before auto-flush (0 = unlimited)
     * @param maxAgeNanos     maximum age in nanoseconds before auto-flush (0 = unlimited)
     */
    public MicrobatchBuffer(int initialCapacity, int maxRows, int maxBytes, long maxAgeNanos) {
        if (initialCapacity <= 0) {
            throw new IllegalArgumentException("initialCapacity must be positive");
        }
        this.bufferCapacity = initialCapacity;
        this.bufferPtr = Unsafe.malloc(initialCapacity, MemoryTag.NATIVE_ILP_RSS);
        this.bufferPos = 0;
        this.rowCount = 0;
        this.firstRowTimeNanos = 0;
        this.maxRows = maxRows;
        this.maxBytes = maxBytes;
        this.maxAgeNanos = maxAgeNanos;
        this.batchId = nextBatchId++;
    }

    /**
     * Creates a new MicrobatchBuffer with default thresholds (no auto-flush).
     *
     * @param initialCapacity initial buffer size in bytes
     */
    public MicrobatchBuffer(int initialCapacity) {
        this(initialCapacity, 0, 0, 0);
    }

    // ==================== DATA OPERATIONS ====================

    /**
     * Returns the buffer pointer for writing data.
     * Only valid when state is FILLING.
     */
    public long getBufferPtr() {
        return bufferPtr;
    }

    /**
     * Returns the current write position in the buffer.
     */
    public int getBufferPos() {
        return bufferPos;
    }

    /**
     * Returns the buffer capacity.
     */
    public int getBufferCapacity() {
        return bufferCapacity;
    }

    /**
     * Sets the buffer position after external writes.
     * Only valid when state is FILLING.
     *
     * @param pos new position
     */
    public void setBufferPos(int pos) {
        if (state != STATE_FILLING) {
            throw new IllegalStateException("Cannot set position when state is " + stateName(state));
        }
        if (pos < 0 || pos > bufferCapacity) {
            throw new IllegalArgumentException("Position out of bounds: " + pos);
        }
        this.bufferPos = pos;
    }

    /**
     * Ensures the buffer has at least the specified capacity.
     * Grows the buffer if necessary.
     *
     * @param requiredCapacity minimum required capacity
     */
    public void ensureCapacity(int requiredCapacity) {
        if (state != STATE_FILLING) {
            throw new IllegalStateException("Cannot resize when state is " + stateName(state));
        }
        if (requiredCapacity > bufferCapacity) {
            int newCapacity = Math.max(bufferCapacity * 2, requiredCapacity);
            long newPtr = Unsafe.realloc(bufferPtr, bufferCapacity, newCapacity, MemoryTag.NATIVE_ILP_RSS);
            bufferPtr = newPtr;
            bufferCapacity = newCapacity;
        }
    }

    /**
     * Writes bytes to the buffer at the current position.
     * Grows the buffer if necessary.
     *
     * @param src    source address
     * @param length number of bytes to write
     */
    public void write(long src, int length) {
        if (state != STATE_FILLING) {
            throw new IllegalStateException("Cannot write when state is " + stateName(state));
        }
        ensureCapacity(bufferPos + length);
        Unsafe.getUnsafe().copyMemory(src, bufferPtr + bufferPos, length);
        bufferPos += length;
    }

    /**
     * Writes a single byte to the buffer.
     *
     * @param b byte to write
     */
    public void writeByte(byte b) {
        if (state != STATE_FILLING) {
            throw new IllegalStateException("Cannot write when state is " + stateName(state));
        }
        ensureCapacity(bufferPos + 1);
        Unsafe.getUnsafe().putByte(bufferPtr + bufferPos, b);
        bufferPos++;
    }

    /**
     * Increments the row count and records the first row time if this is the first row.
     */
    public void incrementRowCount() {
        if (state != STATE_FILLING) {
            throw new IllegalStateException("Cannot increment row count when state is " + stateName(state));
        }
        if (rowCount == 0) {
            firstRowTimeNanos = System.nanoTime();
        }
        rowCount++;
    }

    /**
     * Returns the number of rows in this buffer.
     */
    public int getRowCount() {
        return rowCount;
    }

    /**
     * Returns true if the buffer has any data.
     */
    public boolean hasData() {
        return bufferPos > 0;
    }

    /**
     * Returns the batch ID for this buffer.
     */
    public long getBatchId() {
        return batchId;
    }

    /**
     * Returns the maximum symbol ID used in this batch.
     * Used for delta symbol dictionary tracking.
     */
    public int getMaxSymbolId() {
        return maxSymbolId;
    }

    /**
     * Sets the maximum symbol ID used in this batch.
     * Used for delta symbol dictionary tracking.
     */
    public void setMaxSymbolId(int maxSymbolId) {
        this.maxSymbolId = maxSymbolId;
    }

    // ==================== FLUSH TRIGGER CHECKS ====================

    /**
     * Checks if the buffer should be flushed based on configured thresholds.
     *
     * @return true if any flush threshold is exceeded
     */
    public boolean shouldFlush() {
        if (!hasData()) {
            return false;
        }
        return isRowLimitExceeded() || isByteLimitExceeded() || isAgeLimitExceeded();
    }

    /**
     * Checks if the row count limit has been exceeded.
     */
    public boolean isRowLimitExceeded() {
        return maxRows > 0 && rowCount >= maxRows;
    }

    /**
     * Checks if the byte size limit has been exceeded.
     */
    public boolean isByteLimitExceeded() {
        return maxBytes > 0 && bufferPos >= maxBytes;
    }

    /**
     * Checks if the age limit has been exceeded.
     */
    public boolean isAgeLimitExceeded() {
        if (maxAgeNanos <= 0 || rowCount == 0) {
            return false;
        }
        long ageNanos = System.nanoTime() - firstRowTimeNanos;
        return ageNanos >= maxAgeNanos;
    }

    /**
     * Returns the age of the first row in nanoseconds, or 0 if no rows.
     */
    public long getAgeNanos() {
        if (rowCount == 0) {
            return 0;
        }
        return System.nanoTime() - firstRowTimeNanos;
    }

    // ==================== STATE MACHINE ====================

    /**
     * Returns the current state.
     */
    public int getState() {
        return state;
    }

    /**
     * Returns true if the buffer is in FILLING state (available for writing).
     */
    public boolean isFilling() {
        return state == STATE_FILLING;
    }

    /**
     * Returns true if the buffer is in SEALED state (ready to send).
     */
    public boolean isSealed() {
        return state == STATE_SEALED;
    }

    /**
     * Returns true if the buffer is in SENDING state (being sent by I/O thread).
     */
    public boolean isSending() {
        return state == STATE_SENDING;
    }

    /**
     * Returns true if the buffer is in RECYCLED state (available for reset).
     */
    public boolean isRecycled() {
        return state == STATE_RECYCLED;
    }

    /**
     * Returns true if the buffer is currently in use (not available for the user thread).
     */
    public boolean isInUse() {
        int s = state;
        return s == STATE_SEALED || s == STATE_SENDING;
    }

    /**
     * Seals the buffer, transitioning from FILLING to SEALED.
     * After sealing, no more data can be written.
     * Only the user thread should call this.
     *
     * @throws IllegalStateException if not in FILLING state
     */
    public void seal() {
        if (state != STATE_FILLING) {
            throw new IllegalStateException("Cannot seal buffer in state " + stateName(state));
        }
        state = STATE_SEALED;
    }

    /**
     * Rolls back a seal operation, transitioning from SEALED back to FILLING.
     * <p>
     * Used when enqueue fails after a buffer has been sealed but before ownership
     * was transferred to the I/O thread.
     *
     * @throws IllegalStateException if not in SEALED state
     */
    public void rollbackSealForRetry() {
        if (state != STATE_SEALED) {
            throw new IllegalStateException("Cannot rollback seal in state " + stateName(state));
        }
        state = STATE_FILLING;
    }

    /**
     * Marks the buffer as being sent, transitioning from SEALED to SENDING.
     * Only the I/O thread should call this.
     *
     * @throws IllegalStateException if not in SEALED state
     */
    public void markSending() {
        if (state != STATE_SEALED) {
            throw new IllegalStateException("Cannot mark sending in state " + stateName(state));
        }
        state = STATE_SENDING;
    }

    /**
     * Marks the buffer as recycled, transitioning from SENDING to RECYCLED.
     * This signals to the user thread that the buffer can be reused.
     * Only the I/O thread should call this.
     *
     * @throws IllegalStateException if not in SENDING state
     */
    public void markRecycled() {
        if (state != STATE_SENDING) {
            throw new IllegalStateException("Cannot mark recycled in state " + stateName(state));
        }
        state = STATE_RECYCLED;
        recycleLatch.countDown();
    }

    /**
     * Waits for the buffer to be recycled (transition to RECYCLED state).
     * Only the user thread should call this.
     */
    public void awaitRecycled() {
        recycleLatch.await();
    }

    /**
     * Waits for the buffer to be recycled with a timeout.
     *
     * @param timeout the maximum time to wait
     * @param unit    the time unit
     * @return true if recycled, false if timeout elapsed
     */
    public boolean awaitRecycled(long timeout, TimeUnit unit) {
        return recycleLatch.await(unit.toNanos(timeout));
    }

    /**
     * Resets the buffer to FILLING state, clearing all data.
     * Only valid when in RECYCLED state or when the buffer is fresh.
     *
     * @throws IllegalStateException if in SEALED or SENDING state
     */
    public void reset() {
        int s = state;
        if (s == STATE_SEALED || s == STATE_SENDING) {
            throw new IllegalStateException("Cannot reset buffer in state " + stateName(s));
        }
        bufferPos = 0;
        rowCount = 0;
        firstRowTimeNanos = 0;
        maxSymbolId = -1;
        batchId = nextBatchId++;
        state = STATE_FILLING;
        recycleLatch.setCount(1);
    }

    // ==================== LIFECYCLE ====================

    @Override
    public void close() {
        if (bufferPtr != 0) {
            Unsafe.free(bufferPtr, bufferCapacity, MemoryTag.NATIVE_ILP_RSS);
            bufferPtr = 0;
            bufferCapacity = 0;
        }
    }

    // ==================== UTILITIES ====================

    /**
     * Returns a human-readable name for the given state.
     */
    public static String stateName(int state) {
        switch (state) {
            case STATE_FILLING:
                return "FILLING";
            case STATE_SEALED:
                return "SEALED";
            case STATE_SENDING:
                return "SENDING";
            case STATE_RECYCLED:
                return "RECYCLED";
            default:
                return "UNKNOWN(" + state + ")";
        }
    }

    @Override
    public String toString() {
        return "MicrobatchBuffer{" +
                "batchId=" + batchId +
                ", state=" + stateName(state) +
                ", rows=" + rowCount +
                ", bytes=" + bufferPos +
                ", capacity=" + bufferCapacity +
                '}';
    }
}
