/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class LogRecordSink implements Utf8Sink, DirectUtf8Sequence, Sinkable, Mutable {
    public static final int EOL_LENGTH = Misc.EOL.length();
    private final static int UTF8_BYTE_CLASS_BAD = -1;
    private final static int UTF8_BYTE_CLASS_CONTINUATION = 0;
    protected final long address;
    protected final long lim;
    private final AsciiCharSequence asciiCharSequence = new AsciiCharSequence();
    protected long _wptr;
    private boolean done = false;
    private int level;

    public LogRecordSink(long address, long addressSize) {
        this.address = _wptr = address;
        this.lim = address + addressSize;
    }

    @Override
    public @NotNull CharSequence asAsciiCharSequence() {
        return asciiCharSequence.of(this);
    }

    @Override
    public byte byteAt(int index) {
        return Unsafe.getUnsafe().getByte(address + index);
    }

    @Override
    public void clear() {
        _wptr = address;
        done = false;
    }

    public int getLevel() {
        return level;
    }

    @Override
    public long ptr() {
        return address;
    }

    @Override
    public Utf8Sink put(@Nullable Utf8Sequence us) {
        if (us != null) {
            long rem = lim - _wptr - EOL_LENGTH;
            int size = us.size();
            int n = Math.min((int) rem, size);
            Utf8s.strCpy(us, n, _wptr);
            _wptr += n;
        }
        return this;
    }

    @Override
    public Utf8Sink put(byte b) {
        final long left = lim - _wptr - EOL_LENGTH;
        if (left >= 4) { // 4 is the maximum byte length for a UTF-8 character.
            Unsafe.getUnsafe().putByte(_wptr++, b);
            return this;
        }

        // We're now down to the last few bytes of the line.
        // As such we need to be careful not to write a partial UTF-8 character.

        if (done) {
            // If we've detected a character that is too long for the buffer,
            // then we need to stop processing any later characters.
            // In other words, we want to truncate the log line, not skip over the characters that don't fit.
            //
            // Take the following string:
            //     >>> "I'd like some apple π!".encode('utf-8')
            //     b"I'd like some apple \xcf\x80!"
            //     >>> len(_)
            //     23
            //
            // Encoded, it's 23 bytes. Let's assume that the buffer is only 22 bytes.
            // Without the `done` flag it would serialize out as: "I'd like some apple !"
            // incorrectly writing the '!' byte as there would have been enough space for it.
            return this;
        }

        long needed = utf8CharNeeded(b);
        if (needed == UTF8_BYTE_CLASS_BAD) {
            // Invalid UTF-8 byte, sentinel replacement -- this should never happen in practice.
            b = (byte) '?';
            needed = 1;
        }

        if (left >= needed) {
            Unsafe.getUnsafe().putByte(_wptr++, b);
        } else {
            done = true;
        }

        return this;
    }

    @Override
    public Utf8Sink put(long lo, long hi) {
        final long rem = (lim - _wptr - EOL_LENGTH);
        final long size = hi - lo;
        if (rem >= size) {
            // Common case where the buffer fits the available space.
            Unsafe.getUnsafe().copyMemory(lo, _wptr, size);
            _wptr += size;
            return this;
        }

        // The line is being truncated:
        // We determine a safe length to byte-copy.
        // We skip copying the last 4 bytes, as they may be a multibyte UTF-8 codepoint.
        // NOTE: The computed length may be negative.
        long safeLen = rem - 4;
        if (safeLen > 0) {
            Unsafe.getUnsafe().copyMemory(lo, _wptr, safeLen);
            _wptr += safeLen;
        }

        safeLen = Math.max(0, safeLen);
        for (long i = safeLen; i < rem; i++) {
            // Copying the final few bytes one at a time ensures we don't write any partial codepoints.
            put(Unsafe.getUnsafe().getByte(lo + i));
        }
        return this;
    }

    @Override
    public Utf8Sink putEOL() {
        int rem = (int) (lim - _wptr);
        int len = Misc.EOL.length();
        int n = Math.min(rem, len);
        Utf8s.strCpyAscii(Misc.EOL, n, _wptr);
        _wptr += n;
        return this;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    @Override
    public int size() {
        return (int) (_wptr - address);
    }

    @Override
    public void toSink(@NotNull CharSinkBase<?> sink) {
        Utf8s.utf8ToUtf16(address, _wptr, sink);
    }

    @Override
    public @NotNull String toString() {
        return Utf8s.stringFromUtf8Bytes(address, _wptr);
    }

    private static int utf8ByteClass(byte b) {
        // Reference the table at:
        // https://en.wikipedia.org/wiki/UTF-8#Encoding
        if (b >= 0) {
            // ASCII
            return 1;
        } else if ((b & 0xC0) == 0x80) {
            // 0xC0 = 1100 0000, 0x80 = 1000 0000, check if starts with 10
            return UTF8_BYTE_CLASS_CONTINUATION;
        } else if ((b & 0xE0) == 0xC0) {
            // 0xE0 = 1110 0000, 0xC0 = 1100 0000, check if starts with 110
            return 2;
        } else if ((b & 0xF0) == 0xE0) {
            // 0xF0 = 1111 0000, 0xE0 = 1110 0000m, check if starts with 1110
            return 3;
        } else if ((b & 0xF8) == 0xF0) {
            // 0xF8 = 1111 1000, 0xF0 = 1111 0000, check if starts with 1111 0
            return 4;
        } else {
            return UTF8_BYTE_CLASS_BAD;
        }
    }

    private int utf8CharNeeded(byte b) {
        final int byteClass = utf8ByteClass(b);
        switch (byteClass) {
            case UTF8_BYTE_CLASS_BAD:
                return UTF8_BYTE_CLASS_BAD;

            case UTF8_BYTE_CLASS_CONTINUATION: {
                // We've been dropped into the middle of a multibyte character
                // without prior knowledge of how long it is.
                // We now need to look back to find the start of the character.
                int multibyteLength = UTF8_BYTE_CLASS_BAD;
                long ptr = _wptr - 1;
                final long boundary = Math.max(address, _wptr - 4);

                lookback:
                for (; ptr >= boundary; --ptr) {
                    final byte prev = Unsafe.getUnsafe().getByte(ptr);
                    multibyteLength = utf8ByteClass(prev);
                    switch (multibyteLength) {
                        case UTF8_BYTE_CLASS_BAD:
                            return UTF8_BYTE_CLASS_BAD;
                        case UTF8_BYTE_CLASS_CONTINUATION:
                            continue;
                        default:
                            break lookback;
                    }
                }
                // Adjust the obtained length to account for the number of bytes looked back.
                multibyteLength -= (int) (_wptr - ptr);
                // Normalize errors in case of an illegal ascii character followed by one or more continuation bytes.
                if (multibyteLength < 1) {
                    multibyteLength = UTF8_BYTE_CLASS_BAD;
                }
                return multibyteLength;
            }

            default:
                return byteClass;
        }
    }
}
