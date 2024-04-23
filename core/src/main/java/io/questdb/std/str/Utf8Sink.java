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

import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface Utf8Sink extends CharSink<Utf8Sink> {

    default Utf8Sink escapeJsonStr(@NotNull CharSequence cs) {
        return escapeJsonStr(cs, 0, cs.length());
    }

    default Utf8Sink escapeJsonStr(@NotNull CharSequence cs, int lo, int hi) {
        int i = lo;
        while (i < hi) {
            char c = cs.charAt(i++);
            if (c < 32) {
                escapeJsonStrChar(c);
            } else if (c < 128) {
                switch (c) {
                    case '\"':
                    case '\\':
                        putAscii('\\');
                        // intentional fall through
                    default:
                        putAscii(c);
                        break;
                }
            } else {
                i = Utf8s.encodeUtf16Char(this, cs, hi, i, c);
            }
        }
        return this;
    }

    default Utf8Sink escapeJsonStr(Utf8Sequence utf8) {
        int i = 0;
        final int hi = utf8.size();
        while (i < hi) {
            char c = (char) utf8.byteAt(i++);
            if (c > 0 && c < 32) {
                escapeJsonStrChar(c);
            } else if (c > 0 && c < 128) {
                switch (c) {
                    case '\"':
                    case '\\':
                        putAscii('\\');
                        // intentional fall through
                    default:
                        putAscii(c);
                        break;
                }
            } else {
                put((byte) c);
            }
        }
        return this;
    }

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

    @Override
    default int getEncoding() {
        return CharSinkEncoding.UTF8;
    }

    /**
     * For impls that care about the distinction between ASCII and non-ASCII:
     * Appends a non-ASCII byte, dropping the `isAscii()` status.
     * To append a known-ASCII byte, call {@link #putAscii(char)}.
     * <br/>
     * For impls that don't care about the ASCII/non-ASCII distinction:
     * Appends any kind of byte.
     *
     * @param b byte value
     * @return this sink for daisy-chaining
     */
    Utf8Sink put(byte b);

    /**
     * Encodes the given char sequence to UTF-8 and appends it to this sink.
     * <br/>
     * For impls that care about the distinction between ASCII and non-ASCII:
     * If the sequence's `isAscii` status is false, this sink's `isAscii` status
     * drops to false as well.
     */
    @Override
    default Utf8Sink put(@Nullable CharSequence cs) {
        if (cs != null) {
            put(cs, 0, cs.length());
        }
        return this;
    }

    /**
     * Encodes the given char to UTF-8 and appends it to this sink.
     * <br/>
     * For impls that care about the distinction between ASCII and non-ASCII:
     * If it is a non-ASCII char, this sink's `isAscii` status drops to false.
     */
    @Override
    default Utf8Sink put(char c) {
        if (c < 128) {
            putAscii(c);
        } else if (c < 2048) {
            put((byte) (192 | c >> 6)).put((byte) (128 | c & 63));
        } else if (Character.isSurrogate(c)) {
            putAscii('?');
        } else {
            put((byte) (224 | c >> 12)).put((byte) (128 | c >> 6 & 63)).put((byte) (128 | c & 63));
        }
        return this;
    }

    /**
     * Appends the supplied sequence of UTF-8 bytes to this sink.
     * <br/>
     * For impls that care about the distinction between ASCII and non-ASCII:
     * Assumes the sequence is non-ASCII and drops the `isAscii` status of this sink.
     */
    default Utf8Sink put(@Nullable DirectUtf8Sequence dus) {
        if (dus != null) {
            putNonAscii(dus.lo(), dus.hi());
        }
        return this;
    }

    /**
     * Appends the specified range of UTF-8 bytes from the supplied sequence
     * to this sink.
     * <br/>
     * For impls that care about the distinction between ASCII and non-ASCII:
     * Assumes the sequence is non-ASCII and drops the `isAscii` status of this sink.
     */
    default Utf8Sink put(Utf8Sequence seq, int lo, int hi) {
        if (seq != null) {
            for (int i = lo; i < hi; i++) {
                put(seq.byteAt(i));
            }
        }
        return this;
    }

    /**
     * Encodes the given segment of a char sequence to UTF-8 and appends it
     * to this sink.
     * <br/>
     * For impls that care about the distinction between ASCII and non-ASCII:
     * If any appended char is non-ASCII, this sink's `isAscii` status drops
     * to false.
     */
    @Override
    default Utf8Sink put(@NotNull CharSequence cs, int lo, int hi) {
        int i = lo;
        while (i < hi) {
            char c = cs.charAt(i++);
            if (c < 128) {
                putAscii(c);
            } else {
                i = Utf8s.encodeUtf16Char(this, cs, hi, i, c);
            }
        }
        return this;
    }

    /**
     * For impls that care about the distinction between ASCII and non-ASCII:
     * Appends a general UTF-8 byte. If the byte is non-ASCII, this sink's `isAscii`
     * status drops to false.
     * <br/>
     * For impls that don't care about the ASCII/non-ASCII distinction:
     * Synonymous with {@link #put(byte)}.
     */
    default Utf8Sink putAny(byte b) {
        // This works for impls that don't care about the ASCII/non-ASCII distinction.
        // Must override in impls that do care, and properly update the `isAscii` status.
        return put(b);
    }

    /**
     * For impls that care about the distinction between ASCII and non-ASCII:
     * Appends the specified range of a general UTF-8 sequence. If the range
     * contains a non-ASCII byte, this sink's `isAscii` status drops to false.
     * <br/>
     * For impls that don't care about the ASCII/non-ASCII distinction:
     * Synonymous with, but likely less performant than
     * {@link #put(Utf8Sequence, int, int)}.
     */
    default Utf8Sink putAny(@Nullable Utf8Sequence us) {
        if (us == null) {
            return this;
        }
        return putAny(us, 0, us.size());
    }

    /**
     * For impls that care about the distinction between ASCII and non-ASCII:
     * Appends a general UTF-8 sequence. If the sequence contains a non-ASCII byte,
     * this sink's `isAscii` status drops to false.
     * <br/>
     * For impls that don't care about the ASCII/non-ASCII distinction:
     * Synonymous with, but likely less performant than
     * {@link #put(Utf8Sequence, int, int)}.
     */
    default Utf8Sink putAny(Utf8Sequence seq, int lo, int hi) {
        for (int i = lo; i < hi; i++) {
            putAny(seq.byteAt(i));
        }
        return this;
    }

    @Override
    default Utf8Sink putAscii(char c) {
        // This works for impls that don't care about the ASCII/non-ASCII distinction.
        // In impls that do care, calling put(byte) would drop the `isAscii` status,
        // and therefore such impls must override it.
        return put((byte) c);
    }

    @Override
    default Utf8Sink putAscii(@Nullable CharSequence cs) {
        if (cs == null) {
            return this;
        }
        int l = cs.length();
        for (int i = 0; i < l; i++) {
            putAscii(cs.charAt(i));
        }
        return this;
    }

    default Utf8Sink putQuote() {
        putAscii('"');
        return this;
    }

    @Override
    default Utf8Sink putQuoted(@NotNull CharSequence cs) {
        putAscii('\"').put(cs).putAscii('\"');
        return this;
    }
}
