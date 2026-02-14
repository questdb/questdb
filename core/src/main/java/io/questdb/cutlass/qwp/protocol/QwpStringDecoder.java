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
 * Decoder for STRING and VARCHAR columns in ILP v4 format.
 * <p>
 * Format:
 * <pre>
 * [Null bitmap if nullable]: ceil(rowCount / 8) bytes
 * Offset array: (rowCount + 1) * uint32
 *   offset[0] = 0
 *   offset[i+1] = end position of string[i]
 *   string[i] bytes = data[offset[i]..offset[i+1]]
 * String data: concatenated UTF-8 bytes
 * </pre>
 */
public final class QwpStringDecoder implements QwpColumnDecoder {

    public static final QwpStringDecoder INSTANCE = new QwpStringDecoder();

    private QwpStringDecoder() {
    }

    @Override
    public int decode(long sourceAddress, int sourceLength, int rowCount, boolean nullable, ColumnSink sink) throws QwpParseException {
        if (rowCount == 0) {
            return 0;
        }

        int offset = 0;

        // Parse null bitmap if nullable
        long nullBitmapAddress = 0;
        int nullCount = 0;
        if (nullable) {
            int nullBitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            if (offset + nullBitmapSize > sourceLength) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                        "insufficient data for null bitmap"
                );
            }
            nullBitmapAddress = sourceAddress + offset;
            nullCount = QwpNullBitmap.countNulls(nullBitmapAddress, rowCount);
            offset += nullBitmapSize;
        }

        // Only non-null values have offset entries
        int valueCount = rowCount - nullCount;

        // Parse offset array: (valueCount + 1) * 4 bytes
        int offsetArraySize = (valueCount + 1) * 4;
        if (offset + offsetArraySize > sourceLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "insufficient data for offset array"
            );
        }
        long offsetArrayAddress = sourceAddress + offset;
        offset += offsetArraySize;

        // Validate first offset is 0
        int firstOffset = Unsafe.getUnsafe().getInt(offsetArrayAddress);
        if (firstOffset != 0) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INVALID_OFFSET_ARRAY,
                    "first offset must be 0, got " + firstOffset
            );
        }

        // Get total string data size from last offset
        int lastOffset = Unsafe.getUnsafe().getInt(offsetArrayAddress + (long) valueCount * 4);
        if (lastOffset < 0) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INVALID_OFFSET_ARRAY,
                    "negative offset: " + lastOffset
            );
        }

        // Validate string data size
        if (offset + lastOffset > sourceLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "insufficient data for string values: need " + lastOffset + " bytes"
            );
        }
        long stringDataAddress = sourceAddress + offset;

        // Decode strings
        int valueOffset = 0;
        for (int i = 0; i < rowCount; i++) {
            if (nullable && QwpNullBitmap.isNull(nullBitmapAddress, i)) {
                sink.putNull(i);
            } else {
                int startOffset = Unsafe.getUnsafe().getInt(offsetArrayAddress + (long) valueOffset * 4);
                int endOffset = Unsafe.getUnsafe().getInt(offsetArrayAddress + (long) (valueOffset + 1) * 4);

                // Validate offsets are monotonic
                if (endOffset < startOffset) {
                    throw QwpParseException.create(
                            QwpParseException.ErrorCode.INVALID_OFFSET_ARRAY,
                            "non-monotonic offsets at row " + i + ": " + startOffset + " > " + endOffset
                    );
                }

                int stringLength = endOffset - startOffset;
                long stringAddress = stringDataAddress + startOffset;
                ((StringColumnSink) sink).putString(i, stringAddress, stringLength);
                valueOffset++;
            }
        }

        offset += lastOffset;
        return offset;
    }

    @Override
    public int expectedSize(int rowCount, boolean nullable) {
        // Variable size - this is just the minimum (no string data)
        int size = 0;
        if (nullable) {
            size += QwpNullBitmap.sizeInBytes(rowCount);
        }
        size += (rowCount + 1) * 4; // offset array
        return size;
    }

    /**
     * Extended sink interface for string columns.
     */
    public interface StringColumnSink extends ColumnSink {
        /**
         * Called for each decoded string value.
         *
         * @param rowIndex row index
         * @param address  address of string data (UTF-8 bytes)
         * @param length   length in bytes
         */
        void putString(int rowIndex, long address, int length);
    }

    /**
     * Simple array-based sink for testing.
     */
    public static class ArrayStringSink implements StringColumnSink {
        private final String[] values;
        private final boolean[] nulls;

        public ArrayStringSink(int rowCount) {
            this.values = new String[rowCount];
            this.nulls = new boolean[rowCount];
        }

        @Override
        public void putString(int rowIndex, long address, int length) {
            byte[] bytes = new byte[length];
            for (int i = 0; i < length; i++) {
                bytes[i] = Unsafe.getUnsafe().getByte(address + i);
            }
            values[rowIndex] = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }

        @Override
        public void putNull(int rowIndex) {
            nulls[rowIndex] = true;
        }

        public String getValue(int rowIndex) {
            return values[rowIndex];
        }

        public boolean isNull(int rowIndex) {
            return nulls[rowIndex];
        }

        // Unused methods from ColumnSink interface
        @Override public void putByte(int rowIndex, byte value) {}
        @Override public void putShort(int rowIndex, short value) {}
        @Override public void putInt(int rowIndex, int value) {}
        @Override public void putLong(int rowIndex, long value) {}
        @Override public void putFloat(int rowIndex, float value) {}
        @Override public void putDouble(int rowIndex, double value) {}
        @Override public void putBoolean(int rowIndex, boolean value) {}
        @Override public void putUuid(int rowIndex, long hi, long lo) {}
        @Override public void putLong256(int rowIndex, long l0, long l1, long l2, long l3) {}
    }

    // ==================== Static Encoding Methods ====================

    /**
     * Encodes string values to direct memory.
     * Only non-null values are encoded.
     *
     * @param destAddress destination address
     * @param values      string values to encode
     * @param nulls       null flags (can be null if not nullable)
     * @return address after encoded data
     */
    public static long encode(long destAddress, String[] values, boolean[] nulls) {
        int rowCount = values.length;
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

        // Count non-null values and convert to bytes
        int valueCount = 0;
        for (int i = 0; i < rowCount; i++) {
            if (!nullable || !nulls[i]) valueCount++;
        }

        byte[][] stringBytes = new byte[valueCount][];
        int idx = 0;
        for (int i = 0; i < rowCount; i++) {
            if (nullable && nulls[i]) continue;
            if (values[i] != null) {
                stringBytes[idx] = values[i].getBytes(java.nio.charset.StandardCharsets.UTF_8);
            } else {
                stringBytes[idx] = new byte[0];
            }
            idx++;
        }

        // Write offset array for non-null values only
        int currentOffset = 0;
        for (int i = 0; i <= valueCount; i++) {
            Unsafe.getUnsafe().putInt(pos, currentOffset);
            pos += 4;
            if (i < valueCount) {
                currentOffset += stringBytes[i].length;
            }
        }

        // Write string data
        for (int i = 0; i < valueCount; i++) {
            byte[] bytes = stringBytes[i];
            for (byte b : bytes) {
                Unsafe.getUnsafe().putByte(pos++, b);
            }
        }

        return pos;
    }

    /**
     * Encodes string values to a byte array.
     * Only non-null values are encoded.
     *
     * @param buf    destination buffer
     * @param offset starting offset
     * @param values string values to encode
     * @param nulls  null flags (can be null if not nullable)
     * @return offset after encoded data
     */
    public static int encode(byte[] buf, int offset, String[] values, boolean[] nulls) {
        int rowCount = values.length;
        boolean nullable = nulls != null;

        // Write null bitmap if nullable
        if (nullable) {
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            QwpNullBitmap.fillNoneNull(buf, offset, rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (nulls[i]) {
                    QwpNullBitmap.setNull(buf, offset, i);
                }
            }
            offset += bitmapSize;
        }

        // Count non-null values and convert to bytes
        int valueCount = 0;
        for (int i = 0; i < rowCount; i++) {
            if (!nullable || !nulls[i]) valueCount++;
        }

        byte[][] stringBytes = new byte[valueCount][];
        int idx = 0;
        for (int i = 0; i < rowCount; i++) {
            if (nullable && nulls[i]) continue;
            if (values[i] != null) {
                stringBytes[idx] = values[i].getBytes(java.nio.charset.StandardCharsets.UTF_8);
            } else {
                stringBytes[idx] = new byte[0];
            }
            idx++;
        }

        // Write offset array for non-null values only
        int currentOffset = 0;
        for (int i = 0; i <= valueCount; i++) {
            buf[offset++] = (byte) (currentOffset & 0xFF);
            buf[offset++] = (byte) ((currentOffset >> 8) & 0xFF);
            buf[offset++] = (byte) ((currentOffset >> 16) & 0xFF);
            buf[offset++] = (byte) ((currentOffset >> 24) & 0xFF);
            if (i < valueCount) {
                currentOffset += stringBytes[i].length;
            }
        }

        // Write string data
        for (int i = 0; i < valueCount; i++) {
            byte[] bytes = stringBytes[i];
            System.arraycopy(bytes, 0, buf, offset, bytes.length);
            offset += bytes.length;
        }

        return offset;
    }

    /**
     * Calculates the encoded size for string values.
     *
     * @param values   string values
     * @param nulls    null flags (can be null if not nullable)
     * @return total encoded size in bytes
     */
    public static int encodedSize(String[] values, boolean[] nulls) {
        int rowCount = values.length;
        boolean nullable = nulls != null;
        int size = 0;

        if (nullable) {
            size += QwpNullBitmap.sizeInBytes(rowCount);
        }

        // Count non-null values
        int valueCount = 0;
        for (int i = 0; i < rowCount; i++) {
            if (!nullable || !nulls[i]) valueCount++;
        }

        size += (valueCount + 1) * 4; // offset array for non-null values

        for (int i = 0; i < rowCount; i++) {
            if (nullable && nulls[i]) continue;
            if (values[i] != null) {
                size += values[i].getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            }
        }

        return size;
    }
}
