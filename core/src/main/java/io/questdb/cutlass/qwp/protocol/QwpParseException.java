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
 * Exception thrown when parsing QWP v1 protocol data fails.
 */
public class QwpParseException extends Exception implements Sinkable, FlyweightMessageContainer {

    private final ErrorCode errorCode;
    private final StringSink messageSink = new StringSink();

    public QwpParseException(ErrorCode errorCode, CharSequence message) {
        this.errorCode = errorCode;
        this.messageSink.put(message);
    }

    public static QwpParseException bitReadOverflow() {
        return new QwpParseException(ErrorCode.BIT_READ_OVERFLOW, "attempt to read beyond available bits");
    }

    public static QwpParseException create(ErrorCode errorCode, CharSequence message) {
        return new QwpParseException(errorCode, message);
    }

    public static QwpParseException headerTooShort() {
        return new QwpParseException(ErrorCode.HEADER_TOO_SHORT, "message header too short");
    }

    public static QwpParseException incompleteVarint() {
        return new QwpParseException(ErrorCode.INCOMPLETE_VARINT, "incomplete varint: buffer underflow");
    }

    public static QwpParseException invalidMagic() {
        return new QwpParseException(ErrorCode.INVALID_MAGIC, "invalid magic bytes");
    }

    public static QwpParseException payloadTooLarge() {
        return new QwpParseException(ErrorCode.PAYLOAD_TOO_LARGE, "payload exceeds maximum size");
    }

    public static QwpParseException unsupportedVersion() {
        return new QwpParseException(ErrorCode.UNSUPPORTED_VERSION, "unsupported protocol version");
    }

    public static QwpParseException varintOverflow() {
        return new QwpParseException(ErrorCode.VARINT_OVERFLOW, "varint overflow: too many continuation bytes");
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
        COLUMN_COUNT_EXCEEDED,
        ROW_COUNT_EXCEEDED,
        INVALID_OFFSET_ARRAY,
        INVALID_DICTIONARY_INDEX
    }
}
