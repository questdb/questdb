/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cutlass.qwp.protocol;

import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;

import org.jetbrains.annotations.NotNull;

/**
 * Exception thrown when parsing ILP v4 protocol data fails.
 * <p>
 * This exception is designed to be flyweight-friendly for error reporting
 * while avoiding allocations on hot paths by using cached instances for
 * common error types.
 */
public class QwpParseException extends Exception implements Sinkable, FlyweightMessageContainer {

    // Pre-allocated instances for common errors to avoid allocation on hot paths
    private static final QwpParseException INCOMPLETE_VARINT = new QwpParseException(ErrorCode.INCOMPLETE_VARINT, "incomplete varint: buffer underflow");
    private static final QwpParseException VARINT_OVERFLOW = new QwpParseException(ErrorCode.VARINT_OVERFLOW, "varint overflow: too many continuation bytes");
    private static final QwpParseException INVALID_MAGIC = new QwpParseException(ErrorCode.INVALID_MAGIC, "invalid magic bytes");
    private static final QwpParseException HEADER_TOO_SHORT = new QwpParseException(ErrorCode.HEADER_TOO_SHORT, "message header too short");
    private static final QwpParseException PAYLOAD_TOO_LARGE = new QwpParseException(ErrorCode.PAYLOAD_TOO_LARGE, "payload exceeds maximum size");
    private static final QwpParseException INVALID_UTF8 = new QwpParseException(ErrorCode.INVALID_UTF8, "invalid UTF-8 sequence");
    private static final QwpParseException INVALID_COLUMN_TYPE = new QwpParseException(ErrorCode.INVALID_COLUMN_TYPE, "invalid column type code");
    private static final QwpParseException SCHEMA_NOT_FOUND = new QwpParseException(ErrorCode.SCHEMA_NOT_FOUND, "schema hash not found in cache");
    private static final QwpParseException INSUFFICIENT_DATA = new QwpParseException(ErrorCode.INSUFFICIENT_DATA, "insufficient data for column");
    private static final QwpParseException BIT_READ_OVERFLOW = new QwpParseException(ErrorCode.BIT_READ_OVERFLOW, "attempt to read beyond available bits");
    private static final QwpParseException UNSUPPORTED_VERSION = new QwpParseException(ErrorCode.UNSUPPORTED_VERSION, "unsupported protocol version");

    private final ErrorCode errorCode;
    private final StringSink messageSink = new StringSink();
    private long byteOffset = -1;

    public QwpParseException(ErrorCode errorCode, CharSequence message) {
        this.errorCode = errorCode;
        this.messageSink.put(message);
    }

    /**
     * Returns a cached exception for incomplete varint.
     */
    public static QwpParseException incompleteVarint() {
        return INCOMPLETE_VARINT;
    }

    /**
     * Returns a cached exception for varint overflow.
     */
    public static QwpParseException varintOverflow() {
        return VARINT_OVERFLOW;
    }

    /**
     * Returns a cached exception for invalid magic bytes.
     */
    public static QwpParseException invalidMagic() {
        return INVALID_MAGIC;
    }

    /**
     * Returns a cached exception for header too short.
     */
    public static QwpParseException headerTooShort() {
        return HEADER_TOO_SHORT;
    }

    /**
     * Returns a cached exception for payload too large.
     */
    public static QwpParseException payloadTooLarge() {
        return PAYLOAD_TOO_LARGE;
    }

    /**
     * Returns a cached exception for invalid UTF-8.
     */
    public static QwpParseException invalidUtf8() {
        return INVALID_UTF8;
    }

    /**
     * Returns a cached exception for invalid column type.
     */
    public static QwpParseException invalidColumnType() {
        return INVALID_COLUMN_TYPE;
    }

    /**
     * Returns a cached exception for schema not found.
     */
    public static QwpParseException schemaNotFound() {
        return SCHEMA_NOT_FOUND;
    }

    /**
     * Returns a cached exception for insufficient data.
     */
    public static QwpParseException insufficientData() {
        return INSUFFICIENT_DATA;
    }

    /**
     * Returns a cached exception for bit read overflow.
     */
    public static QwpParseException bitReadOverflow() {
        return BIT_READ_OVERFLOW;
    }

    /**
     * Returns a cached exception for unsupported version.
     */
    public static QwpParseException unsupportedVersion() {
        return UNSUPPORTED_VERSION;
    }

    /**
     * Creates a new exception with a custom message and byte offset.
     *
     * @param errorCode  the error code
     * @param message    the error message
     * @param byteOffset the byte offset where the error occurred
     * @return a new exception instance
     */
    public static QwpParseException create(ErrorCode errorCode, CharSequence message, long byteOffset) {
        QwpParseException ex = new QwpParseException(errorCode, message);
        ex.byteOffset = byteOffset;
        return ex;
    }

    /**
     * Creates a new exception with a custom message.
     *
     * @param errorCode the error code
     * @param message   the error message
     * @return a new exception instance
     */
    public static QwpParseException create(ErrorCode errorCode, CharSequence message) {
        return new QwpParseException(errorCode, message);
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public long getByteOffset() {
        return byteOffset;
    }

    @Override
    public CharSequence getFlyweightMessage() {
        return messageSink;
    }

    @Override
    public String getMessage() {
        if (byteOffset >= 0) {
            return messageSink.toString() + " at byte offset " + byteOffset;
        }
        return messageSink.toString();
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.put(errorCode.name()).put(": ").put(messageSink);
        if (byteOffset >= 0) {
            sink.put(" at byte offset ").put(byteOffset);
        }
    }

    /**
     * Error codes for ILP v4 parsing errors.
     */
    public enum ErrorCode {
        INCOMPLETE_VARINT,
        VARINT_OVERFLOW,
        INVALID_MAGIC,
        HEADER_TOO_SHORT,
        PAYLOAD_TOO_LARGE,
        INVALID_UTF8,
        INVALID_COLUMN_TYPE,
        SCHEMA_NOT_FOUND,
        INSUFFICIENT_DATA,
        BIT_READ_OVERFLOW,
        UNSUPPORTED_VERSION,
        INVALID_TABLE_NAME,
        INVALID_COLUMN_NAME,
        INVALID_SCHEMA_MODE,
        COLUMN_COUNT_EXCEEDED,
        ROW_COUNT_EXCEEDED,
        DECOMPRESSION_ERROR,
        TABLE_COUNT_MISMATCH,
        INVALID_NULL_BITMAP,
        INVALID_OFFSET_ARRAY,
        INVALID_DICTIONARY_INDEX,
        GORILLA_DECODE_ERROR
    }
}
