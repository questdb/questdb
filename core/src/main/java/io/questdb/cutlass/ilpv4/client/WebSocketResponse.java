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

import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

import java.nio.charset.StandardCharsets;

/**
 * Binary response format for WebSocket ILP v4 protocol.
 * <p>
 * Response format (little-endian):
 * <pre>
 * +--------+----------+------------------+
 * | status | sequence | error (if any)   |
 * | 1 byte | 8 bytes  | 2 bytes + UTF-8  |
 * +--------+----------+------------------+
 * </pre>
 * <p>
 * Status codes:
 * <ul>
 *   <li>0: Success (ACK)</li>
 *   <li>1: Parse error</li>
 *   <li>2: Schema error</li>
 *   <li>3: Write error</li>
 *   <li>4: Security error</li>
 *   <li>255: Internal error</li>
 * </ul>
 * <p>
 * The sequence number allows correlation with the original request.
 * Error message is only present when status != 0.
 */
public class WebSocketResponse {

    // Status codes
    public static final byte STATUS_OK = 0;
    public static final byte STATUS_PARSE_ERROR = 1;
    public static final byte STATUS_SCHEMA_ERROR = 2;
    public static final byte STATUS_WRITE_ERROR = 3;
    public static final byte STATUS_SECURITY_ERROR = 4;
    public static final byte STATUS_INTERNAL_ERROR = (byte) 255;

    // Minimum response size: status (1) + sequence (8)
    public static final int MIN_RESPONSE_SIZE = 9;
    public static final int MIN_ERROR_RESPONSE_SIZE = 11; // status + sequence + error length
    public static final int MAX_ERROR_MESSAGE_LENGTH = 1024;

    private byte status;
    private long sequence;
    private String errorMessage;

    public WebSocketResponse() {
        this.status = STATUS_OK;
        this.sequence = 0;
        this.errorMessage = null;
    }

    /**
     * Creates a success response.
     */
    public static WebSocketResponse success(long sequence) {
        WebSocketResponse response = new WebSocketResponse();
        response.status = STATUS_OK;
        response.sequence = sequence;
        return response;
    }

    /**
     * Creates an error response.
     */
    public static WebSocketResponse error(long sequence, byte status, String errorMessage) {
        WebSocketResponse response = new WebSocketResponse();
        response.status = status;
        response.sequence = sequence;
        response.errorMessage = errorMessage;
        return response;
    }

    /**
     * Validates binary response framing without allocating.
     * <p>
     * Accepted formats:
     * <ul>
     *   <li>OK: exactly 9 bytes (status + sequence)</li>
     *   <li>Error: exactly 11 + errorLength bytes</li>
     * </ul>
     *
     * @param ptr    response buffer pointer
     * @param length response frame payload length
     * @return true if payload structure is valid
     */
    public static boolean isStructurallyValid(long ptr, int length) {
        if (length < MIN_RESPONSE_SIZE) {
            return false;
        }

        byte status = Unsafe.getUnsafe().getByte(ptr);
        if (status == STATUS_OK) {
            return length == MIN_RESPONSE_SIZE;
        }

        if (length < MIN_ERROR_RESPONSE_SIZE) {
            return false;
        }

        int msgLen = Unsafe.getUnsafe().getShort(ptr + MIN_RESPONSE_SIZE) & 0xFFFF;
        return length == MIN_ERROR_RESPONSE_SIZE + msgLen;
    }

    /**
     * Returns true if this is a success response.
     */
    public boolean isSuccess() {
        return status == STATUS_OK;
    }

    /**
     * Returns the status code.
     */
    public byte getStatus() {
        return status;
    }

    /**
     * Returns the sequence number.
     */
    public long getSequence() {
        return sequence;
    }

    /**
     * Returns the error message, or null for success responses.
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Returns a human-readable status name.
     */
    public String getStatusName() {
        switch (status) {
            case STATUS_OK:
                return "OK";
            case STATUS_PARSE_ERROR:
                return "PARSE_ERROR";
            case STATUS_SCHEMA_ERROR:
                return "SCHEMA_ERROR";
            case STATUS_WRITE_ERROR:
                return "WRITE_ERROR";
            case STATUS_SECURITY_ERROR:
                return "SECURITY_ERROR";
            case STATUS_INTERNAL_ERROR:
                return "INTERNAL_ERROR";
            default:
                return "UNKNOWN(" + (status & 0xFF) + ")";
        }
    }

    /**
     * Calculates the serialized size of this response.
     */
    public int serializedSize() {
        int size = MIN_RESPONSE_SIZE;
        if (errorMessage != null && !errorMessage.isEmpty()) {
            byte[] msgBytes = errorMessage.getBytes(StandardCharsets.UTF_8);
            int msgLen = Math.min(msgBytes.length, MAX_ERROR_MESSAGE_LENGTH);
            size += 2 + msgLen; // 2 bytes for length prefix
        }
        return size;
    }

    /**
     * Writes this response to native memory.
     *
     * @param ptr destination address
     * @return number of bytes written
     */
    public int writeTo(long ptr) {
        int offset = 0;

        // Status (1 byte)
        Unsafe.getUnsafe().putByte(ptr + offset, status);
        offset += 1;

        // Sequence (8 bytes, little-endian)
        Unsafe.getUnsafe().putLong(ptr + offset, sequence);
        offset += 8;

        // Error message (if any)
        if (status != STATUS_OK && errorMessage != null && !errorMessage.isEmpty()) {
            byte[] msgBytes = errorMessage.getBytes(StandardCharsets.UTF_8);
            int msgLen = Math.min(msgBytes.length, MAX_ERROR_MESSAGE_LENGTH);

            // Length prefix (2 bytes, little-endian)
            Unsafe.getUnsafe().putShort(ptr + offset, (short) msgLen);
            offset += 2;

            // Message bytes
            for (int i = 0; i < msgLen; i++) {
                Unsafe.getUnsafe().putByte(ptr + offset + i, msgBytes[i]);
            }
            offset += msgLen;
        }

        return offset;
    }

    /**
     * Reads a response from native memory.
     *
     * @param ptr    source address
     * @param length available bytes
     * @return true if successfully parsed, false if not enough data
     */
    public boolean readFrom(long ptr, int length) {
        if (length < MIN_RESPONSE_SIZE) {
            return false;
        }

        int offset = 0;

        // Status (1 byte)
        status = Unsafe.getUnsafe().getByte(ptr + offset);
        offset += 1;

        // Sequence (8 bytes, little-endian)
        sequence = Unsafe.getUnsafe().getLong(ptr + offset);
        offset += 8;

        // Error message (if status != OK and more data available)
        if (status != STATUS_OK && length > offset + 2) {
            int msgLen = Unsafe.getUnsafe().getShort(ptr + offset) & 0xFFFF;
            offset += 2;

            if (length >= offset + msgLen && msgLen > 0) {
                byte[] msgBytes = new byte[msgLen];
                for (int i = 0; i < msgLen; i++) {
                    msgBytes[i] = Unsafe.getUnsafe().getByte(ptr + offset + i);
                }
                errorMessage = new String(msgBytes, StandardCharsets.UTF_8);
                offset += msgLen;
            }
        } else {
            errorMessage = null;
        }

        return true;
    }

    @Override
    public String toString() {
        if (isSuccess()) {
            return "WebSocketResponse{status=OK, seq=" + sequence + "}";
        } else {
            return "WebSocketResponse{status=" + getStatusName() + ", seq=" + sequence +
                    ", error=" + errorMessage + "}";
        }
    }
}
