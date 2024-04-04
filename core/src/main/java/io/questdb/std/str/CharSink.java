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

import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Sink interface that does not expose storage format. Users of this interface must not make any assumptions about
 * storage format.
 *
 * @param <T>
 */
@SuppressWarnings("unchecked")
public interface CharSink<T extends CharSink<?>> {

    /**
     * Treats the input char as an ASCII one and writes it into the sink n times.
     * If a UTF-8 char is provided instead, a corrupted char may be written into
     * the sink depending on the implementation.
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

    T put(@Nullable Utf8Sequence us);

    default T put(int value) {
        Numbers.append(this, value);
        return (T) this;
    }

    default T put(long value) {
        Numbers.append(this, value);
        return (T) this;
    }

    default T put(float value, int scale) {
        Numbers.append(this, value, scale);
        return (T) this;
    }

    default T put(double value) {
        Numbers.append(this, value);
        return (T) this;
    }

    default T put(double value, int scale) {
        Numbers.append(this, value, scale);
        return (T) this;
    }

    default T put(boolean value) {
        return putAscii(value ? "true" : "false");
    }

    /**
     * Treats the input char as an ASCII one. If a UTF-8 char is provided instead,
     * a corrupted char may be written into the sink depending on the implementation.
     */
    T putAscii(char c);

    /**
     * Treats the input char sequence as an ASCII-only one. If a sequence with UTF-8 chars
     * is provided instead, corrupted chars may be written into the sink depending on
     * the implementation.
     */
    T putAscii(@Nullable CharSequence cs);

    /**
     * Treats the input char array segment as an ASCII-only one. If an array with UTF-8 chars
     * is provided instead, corrupted chars may be written into the sink depending on
     * the implementation.
     */
    default T putAscii(char @NotNull [] chars, int start, int len) {
        for (int i = 0; i < len; i++) {
            putAscii(chars[i + start]);
        }
        return (T) this;
    }

    /**
     * Treats the input char sequence segment as an ASCII-only one. If a sequence
     * with UTF-8 chars is provided instead, corrupted chars may be written into
     * the sink depending on the implementation.
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
        TimestampFormatUtils.appendDateTimeUSec(this, value);
        return (T) this;
    }

    default T putISODateMillis(long value) {
        DateFormatUtils.appendDateTime(this, value);
        return (T) this;
    }

    default T putQuoted(@NotNull CharSequence cs) {
        putAscii('\"').put(cs).putAscii('\"');
        return (T) this;
    }

    default CharSink putSize(long bytes) {
        long b = bytes == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(bytes);
        return b < 1024L ? put(bytes).put(' ').put('B')
                : b <= 0xfffccccccccccccL >> 40 ? put(Math.round(bytes / 0x1p10 * 1000.0) / 1000.0).put(" KiB")
                : b <= 0xfffccccccccccccL >> 30 ? put(Math.round(bytes / 0x1p20 * 1000.0) / 1000.0).put(" MiB")
                : b <= 0xfffccccccccccccL >> 20 ? put(Math.round(bytes / 0x1p30 * 1000.0) / 1000.0).put(" GiB")
                : b <= 0xfffccccccccccccL >> 10 ? put(Math.round(bytes / 0x1p40 * 1000.0) / 1000.0).put(" TiB")
                : b <= 0xfffccccccccccccL ? put(Math.round((bytes >> 10) / 0x1p40 * 1000.0) / 1000.0).put(" PiB")
                : put(Math.round((bytes >> 20) / 0x1p40 * 1000.0) / 1000.0).put(" EiB");
    }

    T putUtf8(long lo, long hi);
}
