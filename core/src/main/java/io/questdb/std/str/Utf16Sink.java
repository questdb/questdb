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

package io.questdb.std.str;

import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.std.Numbers.hexDigits;

/**
 * Family of sinks that write out <b>character</b> value as UTF16 encoded bytes. This interface
 * is separate from {@link CharSink} to achieve two goals:
 * <ul>
 *     <li>Avoid using these sinks as the target of UTF16-to-UTF8 conversions</li>
 *     <li>Group implementations in easy to understand hierarchy</li>
 * </ul>
 */
public interface Utf16Sink extends CharSink<Utf16Sink> {
    default Utf16Sink escapeJsonStr(@NotNull CharSequence cs) {
        return escapeJsonStr(cs, 0, cs.length());
    }

    default Utf16Sink escapeJsonStr(@NotNull CharSequence cs, int lo, int hi) {
        int i = lo;
        while (i < hi) {
            char c = cs.charAt(i++);
            if (c < 32) {
                escapeJsonStrChar(c);
            } else {
                switch (c) {
                    case '\"':
                    case '\\':
                        putAscii('\\');
                        // intentional fall through
                    default:
                        put(c);
                        break;
                }
            }
        }
        return this;
    }

    @Override
    default int getEncoding() {
        return CharSinkEncoding.UTF16;
    }

    default Utf16Sink put(@Nullable Utf8Sequence us) {
        if (us != null) {
            Utf8s.utf8ToUtf16(us, this);
        }
        return this;
    }

    default Utf16Sink put(@Nullable Utf8Sequence us, int lo, int hi) {
        if (us != null) {
            Utf8s.utf8ToUtf16(us, lo, hi, this);
        }
        return this;
    }

    default Utf16Sink put(long lo, long hi) {
        for (long addr = lo; addr < hi; addr += Character.BYTES) {
            put(Unsafe.getUnsafe().getChar(addr));
        }
        return this;
    }

    default Utf16Sink put(char @NotNull [] chars, int start, int len) {
        for (int i = 0; i < len; i++) {
            put(chars[i + start]);
        }
        return this;
    }

    default void putAsPrintable(CharSequence nonPrintable) {
        for (int i = 0, n = nonPrintable.length(); i < n; i++) {
            char c = nonPrintable.charAt(i);
            putAsPrintable(c);
        }
    }

    default void putAsPrintable(char c) {
        if (c > 0x1F && c != 0x7F) {
            put(c);
        } else {
            put('\\');
            put('u');

            final int s = (int) c & 0xFF;
            put('0');
            put('0');
            put(hexDigits[s / 0x10]);
            put(hexDigits[s % 0x10]);
        }
    }

    /**
     * UTF16 sink stores ASCII character just like any other, as 16bit representation.
     *
     * @param c ascii character to write out.
     * @return this sink for daisy-chaining
     */
    @Override
    default Utf16Sink putAscii(char c) {
        return put(c);
    }

    /**
     * UTF16 sink does not make any special provisions for ASCII string. It will be stored just like any
     * other UTF16 encoded string.
     *
     * @param cs UTF16 encoded ASCII string
     * @return this sink for daisy-chaining
     */
    @Override
    default Utf16Sink putAscii(@Nullable CharSequence cs) {
        return put(cs);
    }

    default Utf16Sink putNonAscii(long lo, long hi) {
        Utf8s.utf8ToUtf16(lo, hi, this);
        return this;
    }

    default Utf16Sink repeat(@NotNull CharSequence value, int n) {
        for (int i = 0; i < n; i++) {
            put(value);
        }
        return this;
    }
}
