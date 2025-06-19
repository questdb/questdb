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

package io.questdb.std.str;

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TimestampDriver;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A sink that does not expose its storage format. Users of this interface must
 * not make any assumptions about the storage format.
 */
@SuppressWarnings("unchecked")
public interface CharSink<T extends CharSink<?>> {

    default void escapeJsonStrChar(char c) {
        switch (c) {
            case '\b':
                putAscii("\\b");
                break;
            case '\f':
                putAscii("\\f");
                break;
            case '\n':
                putAscii("\\n");
                break;
            case '\r':
                putAscii("\\r");
                break;
            case '\t':
                putAscii("\\t");
                break;
            default:
                putAscii("\\u00");
                put(c >> 4);
                putAscii(Numbers.hexDigits[c & 15]);
                break;
        }
    }

    /**
     * Assumes the char is ASCII and appends it to the sink n times.
     * If the char is non-ASCII, it may append a corrupted char, depending
     * on the implementation.
     */
    default void fillAscii(char c, int n) {
        for (int i = 0; i < n; i++) {
            putAscii(c);
        }
    }

    int getEncoding();

    default T put(@NotNull CharSequence cs, int lo, int hi) {
        for (int i = lo; i < hi; i++) {
            put(cs.charAt(i));
        }
        return (T) this;
    }

    default T put(@Nullable Sinkable sinkable) {
        if (sinkable != null) {
            sinkable.toSink(this);
        }
        return (T) this;
    }

    T put(char c);

    default T put(@Nullable CharSequence cs) {
        if (cs != null) {
            for (int i = 0, n = cs.length(); i < n; i++) {
                put(cs.charAt(i));
            }
        }
        return (T) this;
    }

    /**
     * Appends a UTF-8-encoded sequence to this sink.
     * <br>
     * For impls that care about the distinction between ASCII and non-ASCII:
     * If the sequence's `isAscii` status is false, this sink's `isAscii`
     * status drops to false as well.
     */
    T put(@Nullable Utf8Sequence us);

    /**
     * Appends a string representation of the supplied number to this sink.
     */
    default T put(int value) {
        Numbers.append(this, value);
        return (T) this;
    }

    /**
     * Appends a string representation of the supplied number to this sink.
     */
    default T put(long value) {
        Numbers.append(this, value);
        return (T) this;
    }

    /**
     * Appends a string representation of the supplied number to this sink.
     */
    default T put(float value) {
        Numbers.append(this, value);
        return (T) this;
    }

    /**
     * Appends a string representation of the supplied number to this sink.
     */
    default T put(double value) {
        Numbers.append(this, value);
        return (T) this;
    }

    /**
     * Appends a string representation of the supplied boolean to this sink.
     */
    default T put(boolean value) {
        return putAscii(value ? "true" : "false");
    }

    /**
     * Appends an ASCII char to this sink. If the char is non-ASCII, it may append a
     * corrupted char, depending on the implementation.
     */
    T putAscii(char c);

    /**
     * Appends a sequence of ASCII chars to this sink. If some chars are non-ASCII,
     * it may append corrupted chars, depending on the implementation.
     */
    T putAscii(@Nullable CharSequence cs);

    /**
     * Appends a range of ASCII chars from the supplied array. If some chars are
     * non-ASCII, it may append corrupted chars, depending on the implementation.
     */
    default T putAscii(char @NotNull [] chars, int start, int len) {
        for (int i = 0; i < len; i++) {
            putAscii(chars[i + start]);
        }
        return (T) this;
    }

    /**
     * Appends a range of ASCII chars from the supplied sequence to this sink.
     * If some chars are non-ASCII, it may append corrupted chars, depending on
     * the implementation.
     */
    default T putAscii(@NotNull CharSequence cs, int start, int len) {
        for (int i = start; i < len; i++) {
            putAscii(cs.charAt(i));
        }
        return (T) this;
    }

    default T putAsciiQuoted(@NotNull CharSequence cs) {
        putAscii('\"').putAscii(cs).putAscii('\"');
        return (T) this;
    }

    default T putEOL() {
        return putAscii(Misc.EOL);
    }

    default T putISODate(long value) {
        return putISODate(MicrosTimestampDriver.INSTANCE, value);
    }

    default T putISODate(TimestampDriver driver, long value) {
        driver.append(this, value);
        return (T) this;
    }

    default T putISODateMillis(long value) {
        DateFormatUtils.appendDateTime(this, value);
        return (T) this;
    }

    /**
     * Accepts a range of memory addresses from lo to hi (exclusive), expecting it to
     * point to a block of valid UTF-8 bytes, and appends it to this sink.
     * <br>
     * For impls that care about the distinction between ASCII and non-ASCII:
     * Drops the `isAscii` status of this sink.
     */
    T putNonAscii(long lo, long hi);

    default T putQuoted(@NotNull CharSequence cs) {
        putAscii('\"').put(cs).putAscii('\"');
        return (T) this;
    }

    default T putQuoted(@NotNull Utf8Sequence cs) {
        putAscii('\"').put(cs).putAscii('\"');
        return (T) this;
    }

    default T putSize(long bytes) {
        long b = bytes == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(bytes);
        return (T) (b < 1024L ? put(bytes).put(' ').put('B')
                : b <= 0xfffccccccccccccL >> 40 ? put(Math.round(bytes / 0x1p10 * 1000.0) / 1000.0).put(" KiB")
                : b <= 0xfffccccccccccccL >> 30 ? put(Math.round(bytes / 0x1p20 * 1000.0) / 1000.0).put(" MiB")
                : b <= 0xfffccccccccccccL >> 20 ? put(Math.round(bytes / 0x1p30 * 1000.0) / 1000.0).put(" GiB")
                : b <= 0xfffccccccccccccL >> 10 ? put(Math.round(bytes / 0x1p40 * 1000.0) / 1000.0).put(" TiB")
                : b <= 0xfffccccccccccccL ? put(Math.round((bytes >> 10) / 0x1p40 * 1000.0) / 1000.0).put(" PiB")
                : put(Math.round((bytes >> 20) / 0x1p40 * 1000.0) / 1000.0).put(" EiB"));
    }
}
