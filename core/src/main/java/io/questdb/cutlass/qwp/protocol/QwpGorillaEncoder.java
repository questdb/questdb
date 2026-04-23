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

import io.questdb.cairo.CairoException;
import io.questdb.std.Unsafe;

/**
 * Server-side Gorilla delta-of-delta encoder for timestamp columns in QWP
 * egress {@code RESULT_BATCH} frames. Mirrors the client-side ingress encoder;
 * shares wire format with {@link QwpGorillaDecoder}.
 * <p>
 * Encoding:
 * <pre>
 * DoD = (t[n] - t[n-1]) - (t[n-1] - t[n-2])
 *
 * if DoD == 0:              write '0'              (1 bit)
 * elif DoD in [-64, 63]:    write '10' + 7-bit     (9 bits)
 * elif DoD in [-256, 255]:  write '110' + 9-bit    (12 bits)
 * elif DoD in [-2048, 2047]: write '1110' + 12-bit (16 bits)
 * else:                     write '1111' + 32-bit  (36 bits)
 * </pre>
 * <p>
 * First two timestamps ship uncompressed (8 bytes each); the remainder is a
 * delta-of-delta bitstream. Caller must pre-validate with
 * {@link #calculateEncodedSizeIfSupported}; a return of {@code -1} means the
 * stream cannot be encoded (a delta-of-delta exceeds signed int32 range) and
 * the caller should fall back to the uncompressed layout.
 */
public class QwpGorillaEncoder {

    private static final int BUCKET_12BIT_MAX = 2047;
    private static final int BUCKET_12BIT_MIN = -2048;
    private static final int BUCKET_7BIT_MAX = 63;
    // Bucket boundaries (two's complement signed ranges)
    private static final int BUCKET_7BIT_MIN = -64;
    private static final int BUCKET_9BIT_MAX = 255;
    private static final int BUCKET_9BIT_MIN = -256;
    private final QwpBitWriter bitWriter = new QwpBitWriter();

    public QwpGorillaEncoder() {
    }

    /**
     * Checks whether Gorilla encoding can be used and, if so, calculates the
     * encoded size in a single pass over the timestamp data.
     *
     * @param srcAddress source address of contiguous int64 timestamps in native memory
     * @param count      number of timestamps
     * @return encoded size in bytes (excluding encoding flag), or {@code -1} if
     * Gorilla cannot be used (a delta-of-delta exceeds int32 range)
     */
    public static int calculateEncodedSizeIfSupported(long srcAddress, int count) {
        if (count == 0) {
            return 0;
        }

        long size = 8; // first timestamp

        if (count == 1) {
            return (int) size;
        }

        size += 8; // second timestamp

        if (count == 2) {
            return (int) size;
        }

        long prevTimestamp = Unsafe.getUnsafe().getLong(srcAddress + 8);
        long prevDelta = prevTimestamp - Unsafe.getUnsafe().getLong(srcAddress);
        long totalBits = 0;

        for (int i = 2; i < count; i++) {
            long ts = Unsafe.getUnsafe().getLong(srcAddress + (long) i * 8);
            long delta = ts - prevTimestamp;
            long deltaOfDelta = delta - prevDelta;

            if (deltaOfDelta < Integer.MIN_VALUE || deltaOfDelta > Integer.MAX_VALUE) {
                return -1;
            }

            totalBits += getBitsRequired(deltaOfDelta);

            prevDelta = delta;
            prevTimestamp = ts;
        }

        size += (totalBits + 7) / 8;

        if (size > Integer.MAX_VALUE) {
            return -1;
        }
        return (int) size;
    }

    /**
     * Returns the number of bits required to encode a delta-of-delta value.
     */
    public static int getBitsRequired(long deltaOfDelta) {
        int bucket = getBucket(deltaOfDelta);
        return switch (bucket) {
            case 0 -> 1;
            case 1 -> 9;
            case 2 -> 12;
            case 3 -> 16;
            default -> 36;
        };
    }

    /**
     * Determines which bucket a delta-of-delta value falls into.
     *
     * @return bucket number (0 = 1-bit, 1 = 9-bit, 2 = 12-bit, 3 = 16-bit, 4 = 36-bit)
     */
    public static int getBucket(long deltaOfDelta) {
        if (deltaOfDelta == 0) {
            return 0;
        } else if (deltaOfDelta >= BUCKET_7BIT_MIN && deltaOfDelta <= BUCKET_7BIT_MAX) {
            return 1;
        } else if (deltaOfDelta >= BUCKET_9BIT_MIN && deltaOfDelta <= BUCKET_9BIT_MAX) {
            return 2;
        } else if (deltaOfDelta >= BUCKET_12BIT_MIN && deltaOfDelta <= BUCKET_12BIT_MAX) {
            return 3;
        } else {
            return 4;
        }
    }

    /**
     * Encodes a single delta-of-delta value using bucket selection.
     */
    public void encodeDoD(long deltaOfDelta) {
        int bucket = getBucket(deltaOfDelta);
        switch (bucket) {
            case 0:
                bitWriter.writeBit(0);
                break;
            case 1:
                bitWriter.writeBits(0b01, 2);
                bitWriter.writeSigned(deltaOfDelta, 7);
                break;
            case 2:
                bitWriter.writeBits(0b011, 3);
                bitWriter.writeSigned(deltaOfDelta, 9);
                break;
            case 3:
                bitWriter.writeBits(0b0111, 4);
                bitWriter.writeSigned(deltaOfDelta, 12);
                break;
            default:
                bitWriter.writeBits(0b1111, 4);
                bitWriter.writeSigned(deltaOfDelta, 32);
                break;
        }
    }

    /**
     * Encodes {@code count} contiguous int64 timestamps from {@code srcAddress}
     * into {@code destAddress} using Gorilla compression. First two timestamps
     * are uncompressed (8 bytes each); remainder is bit-packed delta-of-delta.
     * <p>
     * Precondition: {@link #calculateEncodedSizeIfSupported(long, int)} must
     * have returned {@code >= 0} for the same input. Skipping the check can
     * silently truncate out-of-int32-range values.
     * <p>
     * Does NOT write the encoding flag byte; the caller owns that byte.
     *
     * @return number of bytes written
     */
    public int encodeTimestamps(long destAddress, long capacity, long srcAddress, int count) {
        if (count == 0) {
            return 0;
        }

        int pos;

        if (capacity < 8) {
            throw CairoException.critical(0)
                    .put("QWP egress: Gorilla encoder buffer overflow on first timestamp [capacity=")
                    .put(capacity).put(']');
        }
        long ts0 = Unsafe.getUnsafe().getLong(srcAddress);
        Unsafe.getUnsafe().putLong(destAddress, ts0);
        pos = 8;

        if (count == 1) {
            return pos;
        }

        if (capacity < pos + 8) {
            throw CairoException.critical(0)
                    .put("QWP egress: Gorilla encoder buffer overflow on second timestamp [capacity=")
                    .put(capacity).put(']');
        }
        long ts1 = Unsafe.getUnsafe().getLong(srcAddress + 8);
        Unsafe.getUnsafe().putLong(destAddress + pos, ts1);
        pos += 8;

        if (count == 2) {
            return pos;
        }

        bitWriter.reset(destAddress + pos, capacity - pos);
        long prevTs = ts1;
        long prevDelta = ts1 - ts0;

        for (int i = 2; i < count; i++) {
            long ts = Unsafe.getUnsafe().getLong(srcAddress + (long) i * 8);
            long delta = ts - prevTs;
            long dod = delta - prevDelta;
            encodeDoD(dod);
            prevDelta = delta;
            prevTs = ts;
        }

        return pos + bitWriter.finish();
    }
}
