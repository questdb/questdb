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

import io.questdb.std.Unsafe;

/**
 * Variable-length integer encoding/decoding utilities for QWP v1 protocol.
 * Uses unsigned LEB128 (Little Endian Base 128) encoding.
 * <p>
 * The encoding scheme:
 * - Values are split into 7-bit groups
 * - Each byte uses the high bit (0x80) as a continuation flag
 * - If high bit is set, more bytes follow
 * - If high bit is clear, this is the last byte
 * <p>
 * This implementation is designed for zero-allocation on hot paths.
 */
public final class QwpVarint {

    /**
     * Maximum number of bytes needed to encode a 64-bit varint.
     * ceil(64/7) = 10 bytes
     */
    public static final int MAX_VARINT_BYTES = 10;

    /**
     * Continuation bit mask - set in all bytes except the last.
     */
    private static final int CONTINUATION_BIT = 0x80;

    /**
     * Data mask - lower 7 bits of each byte.
     */
    private static final int DATA_MASK = 0x7F;

    private QwpVarint() {
        // utility class
    }

    /**
     * Decodes a varint from direct memory.
     *
     * @param address the memory address to read from
     * @param limit   the maximum address to read (exclusive)
     * @return the decoded value
     * @throws QwpParseException if the varint is malformed or buffer underflows
     */
    public static long decode(long address, long limit) throws QwpParseException {
        long result = 0;
        int shift = 0;
        int bytesRead = 0;
        long address1 = address;
        byte b;
        do {
            if (address1 >= limit) {
                throw QwpParseException.incompleteVarint();
            }
            if (bytesRead >= MAX_VARINT_BYTES) {
                throw QwpParseException.varintOverflow();
            }
            b = Unsafe.getUnsafe().getByte(address1++);
            if (shift == 63 && (b & 0x7E) != 0) {
                throw QwpParseException.varintOverflow();
            }
            result |= (long) (b & DATA_MASK) << shift;
            shift += 7;
            bytesRead++;
        } while ((b & CONTINUATION_BIT) != 0);
        return result;
    }

    /**
     * Decodes a varint from a byte array and stores both value and bytes consumed.
     *
     * @param buf    the buffer to read from
     * @param pos    the position to start reading
     * @param limit  the maximum position to read (exclusive)
     * @param result the result holder (must not be null)
     * @throws QwpParseException if the varint is malformed or buffer underflows
     */
    public static void decode(byte[] buf, int pos, int limit, DecodeResult result) throws QwpParseException {
        long value = 0;
        int shift = 0;
        int bytesRead = 0;
        byte b;

        do {
            if (pos >= limit) {
                throw QwpParseException.incompleteVarint();
            }
            if (bytesRead >= MAX_VARINT_BYTES) {
                throw QwpParseException.varintOverflow();
            }
            b = buf[pos++];
            if (shift == 63 && (b & 0x7E) != 0) {
                throw QwpParseException.varintOverflow();
            }
            value |= (long) (b & DATA_MASK) << shift;
            shift += 7;
            bytesRead++;
        } while ((b & CONTINUATION_BIT) != 0);

        result.value = value;
        result.bytesRead = bytesRead;
    }

    /**
     * Decodes a varint from direct memory and stores both value and bytes consumed.
     *
     * @param address the memory address to read from
     * @param limit   the maximum address to read (exclusive)
     * @param result  the result holder (must not be null)
     * @throws QwpParseException if the varint is malformed or buffer underflows
     */
    public static void decode(long address, long limit, DecodeResult result) throws QwpParseException {
        if (address >= limit) {
            throw QwpParseException.incompleteVarint();
        }
        // Fast path: single-byte varint (values 0-127) — the common case for symbol indices
        byte b = Unsafe.getUnsafe().getByte(address);
        if ((b & CONTINUATION_BIT) == 0) {
            result.value = b;
            result.bytesRead = 1;
            return;
        }
        decodeMultiByte(address, limit, result, b);
    }

    /**
     * Encodes a long value as a varint to direct memory.
     *
     * @param address the memory address to write to
     * @param value   the value to encode (treated as unsigned)
     * @return the new address after the encoded bytes
     */
    public static long encode(long address, long value) {
        while ((value & ~DATA_MASK) != 0) {
            Unsafe.getUnsafe().putByte(address++, (byte) ((value & DATA_MASK) | CONTINUATION_BIT));
            value >>>= 7;
        }
        Unsafe.getUnsafe().putByte(address++, (byte) value);
        return address;
    }

    /**
     * Encodes a long value as a varint into the given byte array.
     *
     * @param buf   the buffer to write to
     * @param pos   the position to start writing
     * @param value the value to encode (treated as unsigned)
     * @return the new position after the encoded bytes
     */
    public static int encode(byte[] buf, int pos, long value) {
        while ((value & ~DATA_MASK) != 0) {
            buf[pos++] = (byte) ((value & DATA_MASK) | CONTINUATION_BIT);
            value >>>= 7;
        }
        buf[pos++] = (byte) value;
        return pos;
    }

    /**
     * Calculates the number of bytes needed to encode the given value.
     *
     * @param value the value to measure (treated as unsigned)
     * @return number of bytes needed (1-10)
     */
    public static int encodedLength(long value) {
        if (value == 0) {
            return 1;
        }
        // Count leading zeros to determine the number of bits needed
        int bits = 64 - Long.numberOfLeadingZeros(value);
        // Each byte encodes 7 bits, round up
        return (bits + 6) / 7;
    }

    private static void decodeMultiByte(long address, long limit, DecodeResult result, byte firstByte) throws QwpParseException {
        long value = firstByte & DATA_MASK;
        int shift = 7;
        int bytesRead = 1;
        long addr = address + 1;
        byte b;
        do {
            if (addr >= limit) {
                throw QwpParseException.incompleteVarint();
            }
            if (bytesRead >= MAX_VARINT_BYTES) {
                throw QwpParseException.varintOverflow();
            }
            b = Unsafe.getUnsafe().getByte(addr++);
            if (shift == 63 && (b & 0x7E) != 0) {
                throw QwpParseException.varintOverflow();
            }
            value |= (long) (b & DATA_MASK) << shift;
            shift += 7;
            bytesRead++;
        } while ((b & CONTINUATION_BIT) != 0);
        result.value = value;
        result.bytesRead = bytesRead;
    }

    /**
     * Result holder for decoding varints when the number of bytes consumed matters.
     * This class is mutable and should be reused to avoid allocations.
     */
    public static class DecodeResult {
        public int bytesRead;
        public long value;

        public void reset() {
            value = 0;
            bytesRead = 0;
        }
    }
}
