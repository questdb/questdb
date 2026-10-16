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

package io.questdb.log;

import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.AsciiCharSequence;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;

import static io.questdb.log.LogRecordUtf8Sink.*;

/**
 * Fixed-size, on-heap staging buffer for a log record under construction.
 * <p>
 * A log chain formats its message into this buffer and claims a log ring slot
 * only in {@code $()}, where {@link LogRecordUtf8Sink#copyFrom(HeapLogRecordUtf8Sink)}
 * copies the bytes into the slot and the chain publishes it right away. Nothing
 * between claiming and publishing can throw, so a chain that fails half-way can
 * never leave a claimed-but-unpublished slot behind and wedge the log queue.
 * <p>
 * The truncation rules mirror {@link LogRecordUtf8Sink}: the sink reserves room
 * for the EOL, never writes a partial UTF-8 character and stops at the first
 * character that does not fit. {@link #of(int)} limits each record to the
 * destination slot size, so the copy into the slot never needs to truncate.
 * <p>
 * String appenders check the remaining space once per string: when the whole
 * string fits, they write it without per-byte checks; otherwise they fall back
 * to the per-byte path, which truncates at a character boundary.
 * <p>
 * The buffer gets allocated once, in the constructor, and never grows. It lives
 * on the Java heap, so a carrier's staging sink needs no explicit release when
 * its thread exits.
 */
public class HeapLogRecordUtf8Sink implements Utf8Sink, Utf8Sequence, Mutable {
    private final AsciiCharSequence asciiCharSequence = new AsciiCharSequence();
    private final byte[] buffer;
    private boolean isDone;
    private int limit;
    private int pos;
    private int[] ryuE10;

    public HeapLogRecordUtf8Sink(int capacity) {
        this.buffer = new byte[capacity];
        this.limit = capacity;
    }

    @Override
    public @NotNull CharSequence asAsciiCharSequence() {
        return asciiCharSequence.of(this);
    }

    @Override
    public byte byteAt(int index) {
        return buffer[index];
    }

    public int capacity() {
        return limit;
    }

    @Override
    public void clear() {
        pos = 0;
        isDone = false;
    }

    /**
     * Prepares the sink for a new record bound for a slot of the given capacity.
     * Never allocates: a slot larger than the buffer caps the record at the
     * buffer size, which only happens across LogFactory instances configured
     * with different record lengths.
     */
    public void of(int slotCapacity) {
        limit = Math.min(slotCapacity, buffer.length);
        clear();
    }

    /**
     * Encodes the chars to UTF-8. One space check covers the whole range when it
     * fits even at the worst case of 3 bytes per char (a surrogate pair takes
     * 4 bytes for 2 chars); a range that may not fit takes the per-byte path.
     */
    @Override
    public Utf8Sink put(@NotNull CharSequence cs, int lo, int hi) {
        if (isDone || limit - pos - EOL_LENGTH < 3L * (hi - lo)) {
            return Utf8Sink.super.put(cs, lo, hi);
        }
        final byte[] buf = buffer;
        int i = lo;
        while (i < hi) {
            final char c = cs.charAt(i++);
            if (c < 128) {
                buf[pos++] = (byte) c;
            } else {
                // rare: writes through put(byte), whose checks pass as the range fits
                i = Utf8s.encodeUtf16Char(this, cs, hi, i, c);
            }
        }
        return this;
    }

    @Override
    public Utf8Sink put(@Nullable Utf8Sequence us) {
        if (us != null) {
            final int rem = limit - pos - EOL_LENGTH;
            final int size = us.size();
            if (rem >= size) {
                // Common case where the buffer fits the available space.
                copy(us, size);
                return this;
            }

            // The line is being truncated. Byte-copy a safe prefix, skipping
            // the last 4 bytes as they may be a multibyte UTF-8 codepoint.
            // NOTE: The computed length may be negative.
            int safeLen = rem - 4;
            if (safeLen > 0) {
                copy(us, safeLen);
            }

            safeLen = Math.max(0, safeLen);
            for (int i = safeLen; i < rem; i++) {
                // Copying the final few bytes one at a time ensures we don't write any partial codepoints.
                put(us.byteAt(i));
            }
        }
        return this;
    }

    @Override
    public Utf8Sink put(byte b) {
        final int left = limit - pos - EOL_LENGTH;
        if (left >= 4) { // 4 is the maximum byte length for a UTF-8 character.
            buffer[pos++] = b;
            return this;
        }

        // We're now down to the last few bytes of the line, so we must not
        // write a partial UTF-8 character. Once a character did not fit, we
        // truncate the line instead of skipping over that character.
        if (isDone) {
            return this;
        }

        int needed = utf8CharNeeded(b);
        if (needed == UTF8_BYTE_CLASS_BAD) {
            // Invalid UTF-8 byte, sentinel replacement -- this should never happen in practice.
            b = (byte) '?';
            needed = 1;
        }

        if (left >= needed) {
            buffer[pos++] = b;
        } else {
            isDone = true;
        }
        return this;
    }

    @Override
    public Utf8Sink putAscii(char c) {
        return put((byte) c);
    }

    /**
     * One space check covers the whole sequence when it fits; a sequence that
     * does not fit takes the per-byte path.
     */
    @Override
    public Utf8Sink putAscii(@Nullable CharSequence cs) {
        if (cs == null) {
            return this;
        }
        final int len = cs.length();
        if (isDone || limit - pos - EOL_LENGTH < len) {
            for (int i = 0; i < len; i++) {
                put((byte) cs.charAt(i));
            }
            return this;
        }
        final byte[] buf = buffer;
        final int p = pos;
        for (int i = 0; i < len; i++) {
            buf[p + i] = (byte) cs.charAt(i);
        }
        pos = p + len;
        return this;
    }

    @Override
    public Utf8Sink putEOL() {
        final CharSequence eol = Misc.EOL;
        final int n = Math.min(limit - pos, eol.length());
        for (int i = 0; i < n; i++) {
            buffer[pos++] = (byte) eol.charAt(i);
        }
        return this;
    }

    @Override
    public Utf8Sink putNonAscii(long lo, long hi) {
        final long rem = limit - pos - EOL_LENGTH;
        final long size = hi - lo;
        if (rem >= size) {
            // Common case where the buffer fits the available space.
            Unsafe.copyMemory(null, lo, buffer, Unsafe.BYTE_OFFSET + pos, size);
            pos += (int) size;
            return this;
        }

        // The line is being truncated, see put(Utf8Sequence).
        long safeLen = rem - 4;
        if (safeLen > 0) {
            Unsafe.copyMemory(null, lo, buffer, Unsafe.BYTE_OFFSET + pos, safeLen);
            pos += (int) safeLen;
        }

        safeLen = Math.max(0, safeLen);
        for (long i = safeLen; i < rem; i++) {
            put(Unsafe.getByte(lo + i));
        }
        return this;
    }

    @Override
    public int[] ryuScratch() {
        if (ryuE10 == null) {
            ryuE10 = new int[1];
        }
        return ryuE10;
    }

    @Override
    public int size() {
        return pos;
    }

    @Override
    public @NotNull String toString() {
        return new String(buffer, 0, pos, StandardCharsets.UTF_8);
    }

    private void copy(Utf8Sequence us, int len) {
        final byte[] buf = buffer;
        final int p = pos;
        for (int i = 0; i < len; i++) {
            buf[p + i] = us.byteAt(i);
        }
        pos = p + len;
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
                int p = pos - 1;
                final int boundary = Math.max(0, pos - 4);

                lookback:
                for (; p >= boundary; --p) {
                    multibyteLength = utf8ByteClass(buffer[p]);
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
                multibyteLength -= (pos - p);
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

    // package-private for LogRecordUtf8Sink.copyFrom()
    byte[] buffer() {
        return buffer;
    }
}
