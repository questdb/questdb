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

package io.questdb.cutlass.qwp.protocol;

import io.questdb.std.Unsafe;

/**
 * Gorilla delta-of-delta encoder for timestamps in ILP v4 format.
 * <p>
 * This encoder is used by the WebSocket encoder to compress timestamp columns.
 * It uses delta-of-delta compression where:
 * <pre>
 * DoD = (t[n] - t[n-1]) - (t[n-1] - t[n-2])
 *
 * if DoD == 0:              write '0'              (1 bit)
 * elif DoD in [-63, 64]:    write '10' + 7-bit     (9 bits)
 * elif DoD in [-255, 256]:  write '110' + 9-bit    (12 bits)
 * elif DoD in [-2047, 2048]: write '1110' + 12-bit (16 bits)
 * else:                     write '1111' + 32-bit  (36 bits)
 * </pre>
 * <p>
 * The encoder writes first two timestamps uncompressed, then encodes
 * remaining timestamps using delta-of-delta compression.
 */
public class QwpGorillaEncoder {

    private final QwpBitWriter bitWriter = new QwpBitWriter();

    /**
     * Creates a new Gorilla encoder.
     */
    public QwpGorillaEncoder() {
    }

    /**
     * Encodes a delta-of-delta value using bucket selection.
     * <p>
     * Prefix patterns are written LSB-first to match the decoder's read order:
     * <ul>
     *   <li>'0'    -> write bit 0</li>
     *   <li>'10'   -> write bit 1, then bit 0 (0b01 as 2-bit value)</li>
     *   <li>'110'  -> write bit 1, bit 1, bit 0 (0b011 as 3-bit value)</li>
     *   <li>'1110' -> write bit 1, bit 1, bit 1, bit 0 (0b0111 as 4-bit value)</li>
     *   <li>'1111' -> write bit 1, bit 1, bit 1, bit 1 (0b1111 as 4-bit value)</li>
     * </ul>
     *
     * @param deltaOfDelta the delta-of-delta value to encode
     */
    public void encodeDoD(long deltaOfDelta) {
        int bucket = QwpGorillaDecoder.getBucket(deltaOfDelta);
        switch (bucket) {
            case 0: // DoD == 0
                bitWriter.writeBit(0);
                break;
            case 1: // [-63, 64] -> '10' + 7-bit
                bitWriter.writeBits(0b01, 2);
                bitWriter.writeSigned(deltaOfDelta, 7);
                break;
            case 2: // [-255, 256] -> '110' + 9-bit
                bitWriter.writeBits(0b011, 3);
                bitWriter.writeSigned(deltaOfDelta, 9);
                break;
            case 3: // [-2047, 2048] -> '1110' + 12-bit
                bitWriter.writeBits(0b0111, 4);
                bitWriter.writeSigned(deltaOfDelta, 12);
                break;
            default: // '1111' + 32-bit
                bitWriter.writeBits(0b1111, 4);
                bitWriter.writeSigned(deltaOfDelta, 32);
                break;
        }
    }

    /**
     * Encodes an array of timestamps to native memory using Gorilla compression.
     * <p>
     * Format:
     * <pre>
     * - First timestamp: int64 (8 bytes, little-endian)
     * - Second timestamp: int64 (8 bytes, little-endian)
     * - Remaining timestamps: bit-packed delta-of-delta
     * </pre>
     * <p>
     * Note: This method does NOT write the encoding flag byte. The caller is
     * responsible for writing the ENCODING_GORILLA flag before calling this method.
     *
     * @param destAddress destination address in native memory
     * @param capacity    maximum number of bytes to write
     * @param timestamps  array of timestamp values
     * @param count       number of timestamps to encode
     * @return number of bytes written
     */
    public int encodeTimestamps(long destAddress, long capacity, long[] timestamps, int count) {
        if (count == 0) {
            return 0;
        }

        int pos;

        // Write first timestamp uncompressed
        if (capacity < 8) {
            return 0; // Not enough space
        }
        Unsafe.getUnsafe().putLong(destAddress, timestamps[0]);
        pos = 8;

        if (count == 1) {
            return pos;
        }

        // Write second timestamp uncompressed
        if (capacity < pos + 8) {
            return pos; // Not enough space
        }
        Unsafe.getUnsafe().putLong(destAddress + pos, timestamps[1]);
        pos += 8;

        if (count == 2) {
            return pos;
        }

        // Encode remaining with delta-of-delta
        bitWriter.reset(destAddress + pos, capacity - pos);
        long prevTs = timestamps[1];
        long prevDelta = timestamps[1] - timestamps[0];

        for (int i = 2; i < count; i++) {
            long delta = timestamps[i] - prevTs;
            long dod = delta - prevDelta;
            encodeDoD(dod);
            prevDelta = delta;
            prevTs = timestamps[i];
        }

        return pos + bitWriter.finish();
    }

    /**
     * Checks if Gorilla encoding can be used for the given timestamps.
     * <p>
     * Gorilla encoding uses 32-bit signed integers for delta-of-delta values,
     * so it cannot encode timestamps where the delta-of-delta exceeds the
     * 32-bit signed integer range.
     *
     * @param timestamps array of timestamp values
     * @param count      number of timestamps
     * @return true if Gorilla encoding can be used, false otherwise
     */
    public static boolean canUseGorilla(long[] timestamps, int count) {
        if (count < 3) {
            return true; // No DoD encoding needed for 0, 1, or 2 timestamps
        }

        long prevDelta = timestamps[1] - timestamps[0];
        for (int i = 2; i < count; i++) {
            long delta = timestamps[i] - timestamps[i - 1];
            long dod = delta - prevDelta;
            if (dod < Integer.MIN_VALUE || dod > Integer.MAX_VALUE) {
                return false;
            }
            prevDelta = delta;
        }
        return true;
    }

    /**
     * Calculates the encoded size in bytes for Gorilla-encoded timestamps.
     * <p>
     * Note: This does NOT include the encoding flag byte. Add 1 byte if
     * the encoding flag is needed.
     *
     * @param timestamps array of timestamp values
     * @param count      number of timestamps
     * @return encoded size in bytes (excluding encoding flag)
     */
    public static int calculateEncodedSize(long[] timestamps, int count) {
        if (count == 0) {
            return 0;
        }

        int size = 8; // first timestamp

        if (count == 1) {
            return size;
        }

        size += 8; // second timestamp

        if (count == 2) {
            return size;
        }

        // Calculate bits for delta-of-delta encoding
        long prevTimestamp = timestamps[1];
        long prevDelta = timestamps[1] - timestamps[0];
        int totalBits = 0;

        for (int i = 2; i < count; i++) {
            long delta = timestamps[i] - prevTimestamp;
            long deltaOfDelta = delta - prevDelta;

            totalBits += QwpGorillaDecoder.getBitsRequired(deltaOfDelta);

            prevDelta = delta;
            prevTimestamp = timestamps[i];
        }

        // Round up to bytes
        size += (totalBits + 7) / 8;

        return size;
    }
}
