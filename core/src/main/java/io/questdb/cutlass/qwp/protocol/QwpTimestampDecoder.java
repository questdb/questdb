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

package io.questdb.cutlass.qwp.protocol;

import io.questdb.std.Unsafe;

/**
 * Decoder for TIMESTAMP columns in ILP v4 format.
 * <p>
 * Supports two encoding modes:
 * <ul>
 *   <li>Uncompressed (0x00): array of int64 values</li>
 *   <li>Gorilla (0x01): delta-of-delta compressed</li>
 * </ul>
 * <p>
 * Gorilla format:
 * <pre>
 * [Null bitmap if nullable]
 * First timestamp: int64 (8 bytes, little-endian)
 * Second timestamp: int64 (8 bytes, little-endian)
 * Remaining timestamps: bit-packed delta-of-delta
 * </pre>
 */
public final class QwpTimestampDecoder implements QwpColumnDecoder {

    /**
     * Encoding flag for uncompressed timestamps.
     */
    public static final byte ENCODING_UNCOMPRESSED = 0x00;

    /**
     * Encoding flag for Gorilla-encoded timestamps.
     */
    public static final byte ENCODING_GORILLA = 0x01;

    public static final QwpTimestampDecoder INSTANCE = new QwpTimestampDecoder();

    private final QwpGorillaDecoder gorillaDecoder = new QwpGorillaDecoder();

    private QwpTimestampDecoder() {
    }

    @Override
    public int decode(long sourceAddress, int sourceLength, int rowCount, boolean nullable, ColumnSink sink) throws QwpParseException {
        if (rowCount == 0) {
            return 0;
        }

        int offset = 0;

        // Parse null bitmap if nullable
        long nullBitmapAddress = 0;
        if (nullable) {
            int nullBitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            if (offset + nullBitmapSize > sourceLength) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                        "insufficient data for null bitmap"
                );
            }
            nullBitmapAddress = sourceAddress + offset;
            offset += nullBitmapSize;
        }

        // Read encoding flag
        if (offset + 1 > sourceLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "insufficient data for encoding flag"
            );
        }
        byte encoding = Unsafe.getUnsafe().getByte(sourceAddress + offset);
        offset++;

        if (encoding == ENCODING_UNCOMPRESSED) {
            offset = decodeUncompressed(sourceAddress, sourceLength, offset, rowCount, nullable, nullBitmapAddress, sink);
        } else if (encoding == ENCODING_GORILLA) {
            offset = decodeGorilla(sourceAddress, sourceLength, offset, rowCount, nullable, nullBitmapAddress, sink);
        } else {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INVALID_COLUMN_TYPE,
                    "unknown timestamp encoding: " + encoding
            );
        }

        return offset;
    }

    private int decodeUncompressed(long sourceAddress, int sourceLength, int offset, int rowCount,
                                   boolean nullable, long nullBitmapAddress, ColumnSink sink) throws QwpParseException {
        // Count nulls to determine actual value count
        int nullCount = 0;
        if (nullable) {
            nullCount = QwpNullBitmap.countNulls(nullBitmapAddress, rowCount);
        }
        int valueCount = rowCount - nullCount;

        // Uncompressed: valueCount * 8 bytes
        int valuesSize = valueCount * 8;
        if (offset + valuesSize > sourceLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "insufficient data for uncompressed timestamps"
            );
        }

        long valuesAddress = sourceAddress + offset;
        int valueOffset = 0;
        for (int i = 0; i < rowCount; i++) {
            if (nullable && QwpNullBitmap.isNull(nullBitmapAddress, i)) {
                sink.putNull(i);
            } else {
                long value = Unsafe.getUnsafe().getLong(valuesAddress + (long) valueOffset * 8);
                sink.putLong(i, value);
                valueOffset++;
            }
        }

        return offset + valuesSize;
    }

    private int decodeGorilla(long sourceAddress, int sourceLength, int offset, int rowCount,
                              boolean nullable, long nullBitmapAddress, ColumnSink sink) throws QwpParseException {
        // Count nulls to determine actual value count
        int nullCount = 0;
        if (nullable) {
            nullCount = QwpNullBitmap.countNulls(nullBitmapAddress, rowCount);
        }
        int valueCount = rowCount - nullCount;

        if (valueCount == 0) {
            // All nulls
            for (int i = 0; i < rowCount; i++) {
                sink.putNull(i);
            }
            return offset;
        }

        // First timestamp: 8 bytes
        if (offset + 8 > sourceLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "insufficient data for first timestamp"
            );
        }
        long firstTimestamp = Unsafe.getUnsafe().getLong(sourceAddress + offset);
        offset += 8;

        if (valueCount == 1) {
            // Only one non-null value, output it at the appropriate row position
            for (int i = 0; i < rowCount; i++) {
                if (nullable && QwpNullBitmap.isNull(nullBitmapAddress, i)) {
                    sink.putNull(i);
                } else {
                    sink.putLong(i, firstTimestamp);
                }
            }
            return offset;
        }

        // Second timestamp: 8 bytes
        if (offset + 8 > sourceLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "insufficient data for second timestamp"
            );
        }
        long secondTimestamp = Unsafe.getUnsafe().getLong(sourceAddress + offset);
        offset += 8;

        if (valueCount == 2) {
            // Two non-null values
            int valueIdx = 0;
            for (int i = 0; i < rowCount; i++) {
                if (nullable && QwpNullBitmap.isNull(nullBitmapAddress, i)) {
                    sink.putNull(i);
                } else {
                    sink.putLong(i, valueIdx == 0 ? firstTimestamp : secondTimestamp);
                    valueIdx++;
                }
            }
            return offset;
        }

        // Remaining timestamps: bit-packed delta-of-delta
        // Reset the Gorilla decoder with the initial state
        gorillaDecoder.reset(firstTimestamp, secondTimestamp);

        // Calculate remaining bytes for bit data
        int remainingBytes = sourceLength - offset;
        gorillaDecoder.resetReader(sourceAddress + offset, remainingBytes);

        // Decode timestamps and distribute to rows
        int valueIdx = 0;
        for (int i = 0; i < rowCount; i++) {
            if (nullable && QwpNullBitmap.isNull(nullBitmapAddress, i)) {
                sink.putNull(i);
            } else {
                long timestamp;
                if (valueIdx == 0) {
                    timestamp = firstTimestamp;
                } else if (valueIdx == 1) {
                    timestamp = secondTimestamp;
                } else {
                    timestamp = gorillaDecoder.decodeNext();
                }
                sink.putLong(i, timestamp);
                valueIdx++;
            }
        }

        // Calculate how many bytes were consumed
        // The bit reader has consumed some bits; round up to bytes
        long bitsRead = gorillaDecoder.getAvailableBits();
        long totalBits = remainingBytes * 8L;
        long bitsConsumed = totalBits - bitsRead;
        int bytesConsumed = (int) ((bitsConsumed + 7) / 8);

        return offset + bytesConsumed;
    }

    @Override
    public int expectedSize(int rowCount, boolean nullable) {
        // Minimum size: just encoding flag + uncompressed timestamps
        int size = 1; // encoding flag
        if (nullable) {
            size += QwpNullBitmap.sizeInBytes(rowCount);
        }
        size += rowCount * 8; // worst case: uncompressed
        return size;
    }

    // ==================== Static Encoding Methods (for testing) ====================

    /**
     * Encodes timestamps in uncompressed format to direct memory.
     * Only non-null values are written.
     *
     * @param destAddress destination address
     * @param timestamps  timestamp values
     * @param nulls       null flags (can be null if not nullable)
     * @return address after encoded data
     */
    public static long encodeUncompressed(long destAddress, long[] timestamps, boolean[] nulls) {
        int rowCount = timestamps.length;
        boolean nullable = nulls != null;
        long pos = destAddress;

        // Write null bitmap if nullable
        if (nullable) {
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            QwpNullBitmap.fillNoneNull(pos, rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (nulls[i]) {
                    QwpNullBitmap.setNull(pos, i);
                }
            }
            pos += bitmapSize;
        }

        // Write encoding flag
        Unsafe.getUnsafe().putByte(pos++, ENCODING_UNCOMPRESSED);

        // Write only non-null timestamps
        for (int i = 0; i < rowCount; i++) {
            if (nullable && nulls[i]) continue;
            Unsafe.getUnsafe().putLong(pos, timestamps[i]);
            pos += 8;
        }

        return pos;
    }

    /**
     * Encodes timestamps in Gorilla format to direct memory.
     * Only non-null values are encoded.
     *
     * @param destAddress destination address
     * @param timestamps  timestamp values
     * @param nulls       null flags (can be null if not nullable)
     * @return address after encoded data
     */
    public static long encodeGorilla(long destAddress, long[] timestamps, boolean[] nulls) {
        int rowCount = timestamps.length;
        boolean nullable = nulls != null;
        long pos = destAddress;

        // Write null bitmap if nullable
        if (nullable) {
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            QwpNullBitmap.fillNoneNull(pos, rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (nulls[i]) {
                    QwpNullBitmap.setNull(pos, i);
                }
            }
            pos += bitmapSize;
        }

        // Count non-null values
        int valueCount = 0;
        for (int i = 0; i < rowCount; i++) {
            if (!nullable || !nulls[i]) valueCount++;
        }

        // Write encoding flag
        Unsafe.getUnsafe().putByte(pos++, ENCODING_GORILLA);

        if (valueCount == 0) {
            return pos;
        }

        // Build array of non-null values
        long[] nonNullValues = new long[valueCount];
        int idx = 0;
        for (int i = 0; i < rowCount; i++) {
            if (nullable && nulls[i]) continue;
            nonNullValues[idx++] = timestamps[i];
        }

        // Write first timestamp
        Unsafe.getUnsafe().putLong(pos, nonNullValues[0]);
        pos += 8;

        if (valueCount == 1) {
            return pos;
        }

        // Write second timestamp
        Unsafe.getUnsafe().putLong(pos, nonNullValues[1]);
        pos += 8;

        if (valueCount == 2) {
            return pos;
        }

        // Encode remaining timestamps using Gorilla
        QwpBitWriter bitWriter = new QwpBitWriter();
        bitWriter.reset(pos, 1024 * 1024); // 1MB max for bit data

        long prevTimestamp = nonNullValues[1];
        long prevDelta = nonNullValues[1] - nonNullValues[0];

        for (int i = 2; i < valueCount; i++) {
            long delta = nonNullValues[i] - prevTimestamp;
            long deltaOfDelta = delta - prevDelta;

            encodeDoD(bitWriter, deltaOfDelta);

            prevDelta = delta;
            prevTimestamp = nonNullValues[i];
        }

        // Flush remaining bits
        int bytesWritten = bitWriter.finish();
        pos += bytesWritten;

        return pos;
    }

    /**
     * Encodes a delta-of-delta value to the bit writer.
     * <p>
     * Prefix patterns are written LSB-first to match the decoder's read order:
     * - '0'    -> write bit 0
     * - '10'   -> write bit 1, then bit 0 (0b01 as 2-bit value)
     * - '110'  -> write bit 1, bit 1, bit 0 (0b011 as 3-bit value)
     * - '1110' -> write bit 1, bit 1, bit 1, bit 0 (0b0111 as 4-bit value)
     * - '1111' -> write bit 1, bit 1, bit 1, bit 1 (0b1111 as 4-bit value)
     */
    private static void encodeDoD(QwpBitWriter writer, long deltaOfDelta) {
        if (deltaOfDelta == 0) {
            // '0' = DoD is 0
            writer.writeBit(0);
        } else if (deltaOfDelta >= -63 && deltaOfDelta <= 64) {
            // '10' prefix: first bit read=1, second bit read=0 -> write as 0b01 (LSB-first)
            writer.writeBits(0b01, 2);
            writer.writeSigned(deltaOfDelta, 7);
        } else if (deltaOfDelta >= -255 && deltaOfDelta <= 256) {
            // '110' prefix: bits read as 1,1,0 -> write as 0b011 (LSB-first)
            writer.writeBits(0b011, 3);
            writer.writeSigned(deltaOfDelta, 9);
        } else if (deltaOfDelta >= -2047 && deltaOfDelta <= 2048) {
            // '1110' prefix: bits read as 1,1,1,0 -> write as 0b0111 (LSB-first)
            writer.writeBits(0b0111, 4);
            writer.writeSigned(deltaOfDelta, 12);
        } else {
            // '1111' prefix: bits read as 1,1,1,1 -> write as 0b1111 (LSB-first)
            writer.writeBits(0b1111, 4);
            writer.writeSigned(deltaOfDelta, 32);
        }
    }

    /**
     * Calculates the encoded size in bytes for Gorilla-encoded timestamps.
     *
     * @param timestamps timestamp values
     * @param nullable   whether column is nullable
     * @return encoded size in bytes
     */
    public static int calculateGorillaSize(long[] timestamps, boolean nullable) {
        int rowCount = timestamps.length;
        int size = 0;

        if (nullable) {
            size += QwpNullBitmap.sizeInBytes(rowCount);
        }

        size += 1; // encoding flag

        if (rowCount == 0) {
            return size;
        }

        size += 8; // first timestamp

        if (rowCount == 1) {
            return size;
        }

        size += 8; // second timestamp

        if (rowCount == 2) {
            return size;
        }

        // Calculate bits for delta-of-delta encoding
        long prevTimestamp = timestamps[1];
        long prevDelta = timestamps[1] - timestamps[0];
        int totalBits = 0;

        for (int i = 2; i < rowCount; i++) {
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
