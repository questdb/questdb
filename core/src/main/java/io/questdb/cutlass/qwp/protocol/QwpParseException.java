/*+*****************************************************************************
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

package io.questdb.cutlass.qwp.protocol;

import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Exception thrown when parsing QWP v1 protocol data fails.
 */
public class QwpParseException extends Exception implements Sinkable, FlyweightMessageContainer {
    private static final StackTraceElement[] EMPTY_STACK_TRACE = {};
    private static final ThreadLocal<QwpParseException> tlException = new ThreadLocal<>(QwpParseException::new);
    private final StringSink messageSink = new StringSink();
    private ErrorCode errorCode;

    private QwpParseException() {
    }

    public static QwpParseException bitReadOverflow() {
        return instance(ErrorCode.BIT_READ_OVERFLOW).put("attempt to read beyond available bits");
    }

    public static QwpParseException create(ErrorCode errorCode, CharSequence message) {
        return instance(errorCode).put(message);
    }

    public static QwpParseException headerTooShort() {
        return instance(ErrorCode.HEADER_TOO_SHORT).put("message header too short");
    }

    public static QwpParseException incompleteVarint() {
        return instance(ErrorCode.INCOMPLETE_VARINT).put("incomplete varint: buffer underflow");
    }

    public static QwpParseException instance(ErrorCode errorCode) {
        QwpParseException exception = tlException.get();
        // This is to have correct stack trace in local debugging with -ea option
        assert (exception = new QwpParseException()) != null;
        exception.errorCode = errorCode;
        exception.messageSink.clear();
        return exception;
    }

    public static QwpParseException invalidMagic() {
        return instance(ErrorCode.INVALID_MAGIC).put("invalid magic bytes");
    }

    public static QwpParseException payloadTooLarge() {
        return instance(ErrorCode.PAYLOAD_TOO_LARGE).put("payload exceeds maximum size");
    }

    public static QwpParseException unsupportedVersion() {
        return instance(ErrorCode.UNSUPPORTED_VERSION).put("unsupported protocol version");
    }

    public static QwpParseException varintOverflow() {
        return instance(ErrorCode.VARINT_OVERFLOW).put("varint overflow: too many continuation bytes");
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    @Override
    public CharSequence getFlyweightMessage() {
        return messageSink;
    }

    @Override
    public String getMessage() {
        return messageSink.toString();
    }

    @Override
    public StackTraceElement[] getStackTrace() {
        StackTraceElement[] result = EMPTY_STACK_TRACE;
        // This is to have correct stack trace reported in CI
        assert (result = super.getStackTrace()) != null;
        return result;
    }

    public QwpParseException put(@Nullable CharSequence message) {
        if (message != null) {
            messageSink.put(message);
        }
        return this;
    }

    public QwpParseException put(long value) {
        messageSink.put(value);
        return this;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.put(errorCode.name()).put(": ").put(messageSink);
    }

    /**
     * Error codes for QWP v1 parsing errors.
     */
    public enum ErrorCode {
        INCOMPLETE_VARINT,
        VARINT_OVERFLOW,
        INVALID_MAGIC,
        HEADER_TOO_SHORT,
        PAYLOAD_TOO_LARGE,
        INVALID_COLUMN_TYPE,
        SCHEMA_NOT_FOUND,
        INSUFFICIENT_DATA,
        BIT_READ_OVERFLOW,
        UNSUPPORTED_VERSION,
        INVALID_TABLE_NAME,
        INVALID_COLUMN_NAME,
        INVALID_SCHEMA_MODE,
        INVALID_SCHEMA_ID,
        SCHEMA_MISMATCH,
        COLUMN_COUNT_EXCEEDED,
        ROW_COUNT_EXCEEDED,
        INVALID_OFFSET_ARRAY,
        INVALID_DICTIONARY_INDEX
    }
}
