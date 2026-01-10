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

package io.questdb.cutlass.line.tcp.v4;

import io.questdb.std.Unsafe;

/**
 * Decoder for BOOLEAN columns in ILP v4 format.
 * <p>
 * Format:
 * <pre>
 * [Null bitmap if nullable]: ceil(rowCount / 8) bytes
 * Value bits: ceil(rowCount / 8) bytes
 *   bit[i] = boolean value for row[i]
 *   LSB first within each byte
 * </pre>
 * <p>
 * Note: Boolean values are bit-packed, not byte-per-value.
 */
public final class IlpV4BooleanDecoder implements IlpV4ColumnDecoder {

    public static final IlpV4BooleanDecoder INSTANCE = new IlpV4BooleanDecoder();

    private IlpV4BooleanDecoder() {
    }

    @Override
    public int decode(long sourceAddress, int sourceLength, int rowCount, boolean nullable, ColumnSink sink) throws IlpV4ParseException {
        if (rowCount == 0) {
            return 0;
        }

        int offset = 0;

        // Parse null bitmap if nullable
        long nullBitmapAddress = 0;
        int nullCount = 0;
        if (nullable) {
            int nullBitmapSize = IlpV4NullBitmap.sizeInBytes(rowCount);
            if (offset + nullBitmapSize > sourceLength) {
                throw IlpV4ParseException.create(
                        IlpV4ParseException.ErrorCode.INSUFFICIENT_DATA,
                        "insufficient data for null bitmap"
                );
            }
            nullBitmapAddress = sourceAddress + offset;
            nullCount = IlpV4NullBitmap.countNulls(nullBitmapAddress, rowCount);
            offset += nullBitmapSize;
        }

        // Value bits only for non-null values
        int valueCount = rowCount - nullCount;
        int valueBitmapSize = IlpV4NullBitmap.sizeInBytes(valueCount);

        // Validate value bits size
        if (offset + valueBitmapSize > sourceLength) {
            throw IlpV4ParseException.create(
                    IlpV4ParseException.ErrorCode.INSUFFICIENT_DATA,
                    "insufficient data for boolean values"
            );
        }

        long valueBitsAddress = sourceAddress + offset;
        offset += valueBitmapSize;

        // Decode boolean values
        int valueOffset = 0;
        for (int i = 0; i < rowCount; i++) {
            if (nullable && IlpV4NullBitmap.isNull(nullBitmapAddress, i)) {
                sink.putNull(i);
            } else {
                boolean value = getBit(valueBitsAddress, valueOffset);
                sink.putBoolean(i, value);
                valueOffset++;
            }
        }

        return offset;
    }

    /**
     * Gets a bit from a bit-packed array (LSB first within each byte).
     */
    private boolean getBit(long address, int bitIndex) {
        int byteIndex = bitIndex >>> 3;
        int bitOffset = bitIndex & 7;
        byte b = Unsafe.getUnsafe().getByte(address + byteIndex);
        return (b & (1 << bitOffset)) != 0;
    }

    @Override
    public int expectedSize(int rowCount, boolean nullable) {
        int bitmapSize = IlpV4NullBitmap.sizeInBytes(rowCount);
        int size = bitmapSize; // value bits
        if (nullable) {
            size += bitmapSize; // null bitmap
        }
        return size;
    }

    // ==================== Static Encoding Methods ====================

    /**
     * Encodes boolean values to direct memory.
     * Only non-null values are bit-packed.
     *
     * @param destAddress destination address
     * @param values      boolean values to encode
     * @param nulls       null flags (can be null if not nullable)
     * @return address after encoded data
     */
    public static long encode(long destAddress, boolean[] values, boolean[] nulls) {
        int rowCount = values.length;
        boolean nullable = nulls != null;
        long pos = destAddress;

        // Count non-null values
        int valueCount = rowCount;
        if (nullable) {
            valueCount = 0;
            for (int i = 0; i < rowCount; i++) {
                if (!nulls[i]) valueCount++;
            }
        }

        // Write null bitmap if nullable
        if (nullable) {
            int nullBitmapSize = IlpV4NullBitmap.sizeInBytes(rowCount);
            IlpV4NullBitmap.fillNoneNull(pos, rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (nulls[i]) {
                    IlpV4NullBitmap.setNull(pos, i);
                }
            }
            pos += nullBitmapSize;
        }

        // Write value bits (only for non-null values)
        int valueBitmapSize = IlpV4NullBitmap.sizeInBytes(valueCount);
        for (int i = 0; i < valueBitmapSize; i++) {
            Unsafe.getUnsafe().putByte(pos + i, (byte) 0);
        }

        int valueOffset = 0;
        for (int i = 0; i < rowCount; i++) {
            if (nullable && nulls[i]) continue;
            if (values[i]) {
                setBit(pos, valueOffset);
            }
            valueOffset++;
        }
        pos += valueBitmapSize;

        return pos;
    }

    /**
     * Sets a bit in a bit-packed array (LSB first within each byte).
     */
    private static void setBit(long address, int bitIndex) {
        int byteIndex = bitIndex >>> 3;
        int bitOffset = bitIndex & 7;
        long addr = address + byteIndex;
        byte b = Unsafe.getUnsafe().getByte(addr);
        b |= (1 << bitOffset);
        Unsafe.getUnsafe().putByte(addr, b);
    }

    /**
     * Encodes boolean values to a byte array.
     * Only non-null values are bit-packed.
     *
     * @param buf    destination buffer
     * @param offset starting offset
     * @param values boolean values to encode
     * @param nulls  null flags (can be null if not nullable)
     * @return offset after encoded data
     */
    public static int encode(byte[] buf, int offset, boolean[] values, boolean[] nulls) {
        int rowCount = values.length;
        boolean nullable = nulls != null;

        // Count non-null values
        int valueCount = rowCount;
        if (nullable) {
            valueCount = 0;
            for (int i = 0; i < rowCount; i++) {
                if (!nulls[i]) valueCount++;
            }
        }

        // Write null bitmap if nullable
        if (nullable) {
            int nullBitmapSize = IlpV4NullBitmap.sizeInBytes(rowCount);
            IlpV4NullBitmap.fillNoneNull(buf, offset, rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (nulls[i]) {
                    IlpV4NullBitmap.setNull(buf, offset, i);
                }
            }
            offset += nullBitmapSize;
        }

        // Initialize value bits to zeros (only for non-null values)
        int valueBitmapSize = IlpV4NullBitmap.sizeInBytes(valueCount);
        for (int i = 0; i < valueBitmapSize; i++) {
            buf[offset + i] = 0;
        }

        // Set true values (only non-null)
        int valueOffset = 0;
        for (int i = 0; i < rowCount; i++) {
            if (nullable && nulls[i]) continue;
            if (values[i]) {
                int byteIndex = valueOffset >>> 3;
                int bitOffset = valueOffset & 7;
                buf[offset + byteIndex] |= (1 << bitOffset);
            }
            valueOffset++;
        }
        offset += valueBitmapSize;

        return offset;
    }
}
