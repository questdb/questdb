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

package io.questdb.log;

import io.questdb.cairo.TimestampDriver;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;

public interface LogRecord extends Utf8Sink {

    void $();

    /**
     * Puts an ASCII char sequence to this record.
     * <p>
     * <strong>WARNING:</strong> sequence must be all ASCII chars, but this method doesn't
     * validate it. It puts only the lower byte of each char to the record, so if this ends
     * up being a non-ASCII byte, the record's UTF-8 output breaks.
     * <p>
     * If the sequence may contain non-ASCII chars, use {@link #$safe(CharSequence)} instead.
     */
    LogRecord $(@Nullable CharSequence sequence);

    /**
     * Copies the UTF-8 sequence to the log record.
     * <p>
     * <strong>NOTE:</strong> This method doesn't perform any validation of UTF-8. If the byte
     * sequence is invalid UTF-8, it may break logging.
     */
    LogRecord $(@Nullable Utf8Sequence sequence);

    /**
     * Copies the UTF-8 sequence to the log record. Performs no validation of UTF-8.
     * <p>
     * <strong>NOTE:</strong> This method doesn't perform any validation of UTF-8. If
     * you're logging a byte sequence whose contents you don't control, use
     * {@link #$safe(DirectUtf8Sequence)} instead.
     */
    LogRecord $(@Nullable DirectUtf8Sequence sequence);

    LogRecord $(int x);

    LogRecord $(double x);

    LogRecord $(long l);

    LogRecord $(boolean x);

    LogRecord $(char c);

    LogRecord $(@Nullable Throwable e);

    LogRecord $(@Nullable File x);

    LogRecord $(@Nullable Object x);

    LogRecord $(@Nullable Sinkable x);

    LogRecord $256(long a, long b, long c, long d);

    LogRecord $hex(long value);

    LogRecord $hexPadded(long value);

    LogRecord $ip(long ip);

    LogRecord $safe(@NotNull CharSequence sequence, int lo, int hi);

    LogRecord $safe(@Nullable DirectUtf8Sequence sequence);

    LogRecord $safe(@Nullable Utf8Sequence sequence);

    LogRecord $safe(long lo, long hi);

    LogRecord $safe(@Nullable CharSequence sequence);

    LogRecord $size(long memoryBytes);

    LogRecord $substr(int from, @Nullable DirectUtf8Sequence sequence);

    LogRecord $ts(long x);

    LogRecord $ts(TimestampDriver driver, long x);

    LogRecord $uuid(long lo, long hi);

    default void I$() {
        $(']').$();
    }

    boolean isEnabled();

    LogRecord microTime(long x);

    LogRecord ts();
}
