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

import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.Sinkable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.AbstractCharSink;
import io.questdb.std.str.CharSink;

public class LogRecordSink extends AbstractCharSink implements Sinkable {
    public static final int EOL_LENGTH = Misc.EOL.length();
    public final static int UTF8_BYTE_CLASS_BAD = -1;
    public final static int UTF8_BYTE_CLASS_CONTINUATION = 0;
    protected final long address;
    protected final long lim;
    protected long _wptr;
    private boolean done = false;
    private int level;

    public LogRecordSink(long address, long addressSize) {
        this.address = _wptr = address;
        this.lim = address + addressSize;
    }

    public void clear() {
        _wptr = address;
        done = false;
    }

    public long getAddress() {
        return address;
    }

    public int getLevel() {
        return level;
    }

    public int length() {
        return (int) (_wptr - address);
    }

    /**
     * Log a string that contains only ASCII chars.
     * Call `encodeUtf8` otherwise.
     */
    @Override
    public CharSink put(CharSequence cs) {
        int rem = (int) (lim - _wptr - EOL_LENGTH);
        int len = cs.length();
        int n = Math.min(rem, len);
        Chars.asciiStrCpy(cs, n, _wptr);
        _wptr += n;
        return this;
    }

    /**
     * Log a substring that contains only ASCII chars.
     * Call `encodeUtf8` otherwise.
     */
    @Override
    public CharSink put(CharSequence cs, int lo, int hi) {
        int rem = (int) (lim - _wptr - EOL_LENGTH);
        int len = hi - lo;
        int n = Math.min(rem, len);
        Chars.asciiStrCpy(cs, lo, n, _wptr);
        _wptr += n;
        return this;
    }

    /**
     * Log a ASCII character or UTF-8 byte (cast as a `char`).
     * The signature breaks expectations: Read as if `char c` were `byte b`.
     */
    @Override
    public CharSink put(char c) {
        final long left = lim - _wptr - EOL_LENGTH;
        byte b = (byte) c;
        if (left >= 4) {  // 4 is the maximum byte length for a UTF-8 character.
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

        long needed = utf8charNeeded(b);
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
    public CharSink putEOL() {
        int rem = (int) (lim - _wptr);
        int len = Misc.EOL.length();
        int n = Math.min(rem, len);
        Chars.asciiStrCpy(Misc.EOL, n, _wptr);
        _wptr += n;
        return this;
    }

    @Override
    public CharSink putUtf8(long lo, long hi) {
        final long rem = (lim - _wptr - EOL_LENGTH);
        final long len = hi - lo;
        if (rem >= len) {
            // Common case where the buffer fits the available space.
            Unsafe.getUnsafe().copyMemory(lo, _wptr, len);
            _wptr += len;
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
            put((char) Unsafe.getUnsafe().getByte(lo + i));
        }
        return this;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    @Override
    public void toSink(CharSink sink) {
        Chars.utf8toUtf16(address, _wptr, sink);
    }

    private static int utf8byteClass(byte b) {
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

    private int utf8charNeeded(byte b) {
        final int byteClass = utf8byteClass(b);
        switch (byteClass) {
            case UTF8_BYTE_CLASS_BAD:
                return UTF8_BYTE_CLASS_BAD;

            case UTF8_BYTE_CLASS_CONTINUATION: {
                // We've been dropped into the middle of a multi-byte character
                // without prior knowledge of how long it is.
                // We now need to look back to find the start of the character.
                int multibyteLength = UTF8_BYTE_CLASS_BAD;
                long ptr = _wptr - 1;
                final long boundary = Math.max(address, _wptr - 4);

                lookback:
                for (; ptr >= boundary; --ptr) {
                    final byte prev = Unsafe.getUnsafe().getByte(ptr);
                    multibyteLength = utf8byteClass(prev);
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
