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

package io.questdb.cutlass.qwp.server;

import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;

/**
 * State management for ILP v4 WebSocket processing.
 * <p>
 * Manages buffer accumulation for incoming binary data and tracks
 * processing status.
 */
public class QwpWebSocketProcessorState implements Mutable, QuietCloseable {
    private long bufferAddress;
    private int bufferCapacity;
    private int bufferPosition;
    private String errorMessage;
    private boolean ok = true;

    // Response state
    private boolean hasResponse = false;
    private boolean responseSuccess = false;
    private int responseErrorCode = 0;
    private String responseErrorMessage = null;

    // Metrics
    private long bytesProcessed = 0;

    public QwpWebSocketProcessorState(int initialBufferSize) {
        this.bufferCapacity = initialBufferSize;
        this.bufferAddress = Unsafe.malloc(bufferCapacity, MemoryTag.NATIVE_HTTP_CONN);
        this.bufferPosition = 0;
    }

    /**
     * Adds data to the internal buffer.
     *
     * @param lo start address of data (inclusive)
     * @param hi end address of data (exclusive)
     */
    public void addData(long lo, long hi) {
        if (!ok) {
            return; // Ignore data when in error state
        }

        int length = (int) (hi - lo);
        if (length <= 0) {
            return;
        }

        ensureCapacity(length);
        Unsafe.getUnsafe().copyMemory(lo, bufferAddress + bufferPosition, length);
        bufferPosition += length;
    }

    /**
     * Ensures the buffer has enough capacity for additional bytes.
     */
    private void ensureCapacity(int additional) {
        int required = bufferPosition + additional;
        if (required <= bufferCapacity) {
            return;
        }

        // Grow buffer
        int newCapacity = Math.max(bufferCapacity * 2, required);
        long newAddress = Unsafe.malloc(newCapacity, MemoryTag.NATIVE_HTTP_CONN);
        if (bufferPosition > 0) {
            Unsafe.getUnsafe().copyMemory(bufferAddress, newAddress, bufferPosition);
        }
        Unsafe.free(bufferAddress, bufferCapacity, MemoryTag.NATIVE_HTTP_CONN);
        bufferAddress = newAddress;
        bufferCapacity = newCapacity;
    }

    /**
     * Returns the buffer address for reading accumulated data.
     */
    public long getBufferAddress() {
        return bufferAddress;
    }

    /**
     * Returns the current position in the buffer (amount of data accumulated).
     */
    public int getBufferPosition() {
        return bufferPosition;
    }

    /**
     * Returns the buffer capacity.
     */
    public int getBufferCapacity() {
        return bufferCapacity;
    }

    /**
     * Returns true if the state is OK (no error).
     */
    public boolean isOk() {
        return ok;
    }

    /**
     * Sets an error message and marks state as not OK.
     */
    public void setError(String message) {
        this.errorMessage = message;
        this.ok = false;
    }

    /**
     * Returns the error message, or null if no error.
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Processes the accumulated message data.
     * After processing, the buffer position is reset but bytesProcessed is incremented.
     */
    public void processMessage() {
        if (!ok) {
            return; // Don't process when in error state
        }

        // Update metrics
        bytesProcessed += bufferPosition;

        // Reset buffer position (data has been processed)
        bufferPosition = 0;
    }

    // ==================== RESPONSE STATE ====================

    /**
     * Returns true if there's a pending response.
     */
    public boolean hasResponse() {
        return hasResponse;
    }

    /**
     * Returns true if the response indicates success.
     */
    public boolean isResponseSuccess() {
        return responseSuccess;
    }

    /**
     * Returns the error code for error responses.
     */
    public int getResponseErrorCode() {
        return responseErrorCode;
    }

    /**
     * Returns the error message for error responses.
     */
    public String getResponseErrorMessage() {
        return responseErrorMessage;
    }

    /**
     * Sets a success response.
     */
    public void setSuccessResponse() {
        hasResponse = true;
        responseSuccess = true;
        responseErrorCode = 0;
        responseErrorMessage = null;
    }

    /**
     * Sets an error response.
     *
     * @param errorCode error code
     * @param message   error message
     */
    public void setErrorResponse(int errorCode, String message) {
        hasResponse = true;
        responseSuccess = false;
        responseErrorCode = errorCode;
        responseErrorMessage = message;
    }

    /**
     * Consumes (clears) the current response state.
     */
    public void consumeResponse() {
        hasResponse = false;
        responseSuccess = false;
        responseErrorCode = 0;
        responseErrorMessage = null;
    }

    // ==================== METRICS ====================

    /**
     * Returns the total number of bytes processed.
     */
    public long getBytesProcessed() {
        return bytesProcessed;
    }

    // ==================== LIFECYCLE ====================

    @Override
    public void clear() {
        bufferPosition = 0;
        errorMessage = null;
        ok = true;
        // Reset response state
        hasResponse = false;
        responseSuccess = false;
        responseErrorCode = 0;
        responseErrorMessage = null;
        // Note: bytesProcessed is NOT reset on clear (it's a cumulative metric)
    }

    @Override
    public void close() {
        if (bufferAddress != 0) {
            Unsafe.free(bufferAddress, bufferCapacity, MemoryTag.NATIVE_HTTP_CONN);
            bufferAddress = 0;
            bufferCapacity = 0;
        }
        bufferPosition = 0;
    }
}
