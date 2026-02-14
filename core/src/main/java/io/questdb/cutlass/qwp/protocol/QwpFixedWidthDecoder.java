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

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;

/**
 * Decoder for fixed-width column types in ILP v4 format.
 * <p>
 * Supported types: BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DATE, UUID, LONG256
 * <p>
 * Format:
 * <pre>
 * [Null bitmap if nullable]: ceil(rowCount / 8) bytes
 * Values: rowCount * sizeof(type) bytes
 *   - All little-endian except UUID and LONG256 (big-endian)
 *   - NULL positions contain undefined bytes (skipped via bitmap)
 * </pre>
 */
public final class QwpFixedWidthDecoder implements QwpColumnDecoder {

    private final byte typeCode;
    private final int valueSize;

    /**
     * Creates a decoder for a specific fixed-width type.
     *
     * @param typeCode the ILP v4 type code
     * @throws IllegalArgumentException if type is not fixed-width
     */
    public QwpFixedWidthDecoder(byte typeCode) {
        this.typeCode = (byte) (typeCode & TYPE_MASK);
        this.valueSize = getFixedTypeSize(this.typeCode);
        if (this.valueSize < 0) {
            throw new IllegalArgumentException("Not a fixed-width type: " + getTypeName(typeCode));
        }
        if (this.typeCode == TYPE_BOOLEAN) {
            throw new IllegalArgumentException("Use QwpBooleanDecoder for BOOLEAN type");
        }
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

        // Calculate and validate value array size
        // Only non-null values are stored in the wire format
        int valueCount = rowCount - nullCount;
        int valuesSize = valueCount * valueSize;
        if (offset + valuesSize > sourceLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "insufficient data for values: need " + valuesSize + " bytes, have " + (sourceLength - offset)
            );
        }

        // Decode values
        long valuesAddress = sourceAddress + offset;
        decodeValues(valuesAddress, nullBitmapAddress, rowCount, nullable, sink);
        offset += valuesSize;

        return offset;
    }

    private void decodeValues(long valuesAddress, long nullBitmapAddress, int rowCount, boolean nullable, ColumnSink sink) {
        switch (typeCode) {
            case TYPE_BYTE:
                decodeBytes(valuesAddress, nullBitmapAddress, rowCount, nullable, sink);
                break;
            case TYPE_SHORT:
            case TYPE_CHAR:
                decodeShorts(valuesAddress, nullBitmapAddress, rowCount, nullable, sink);
                break;
            case TYPE_INT:
                decodeInts(valuesAddress, nullBitmapAddress, rowCount, nullable, sink);
                break;
            case TYPE_LONG:
            case TYPE_TIMESTAMP:
            case TYPE_TIMESTAMP_NANOS:
            case TYPE_DATE:
                decodeLongs(valuesAddress, nullBitmapAddress, rowCount, nullable, sink);
                break;
            case TYPE_FLOAT:
                decodeFloats(valuesAddress, nullBitmapAddress, rowCount, nullable, sink);
                break;
            case TYPE_DOUBLE:
                decodeDoubles(valuesAddress, nullBitmapAddress, rowCount, nullable, sink);
                break;
            case TYPE_UUID:
                decodeUuids(valuesAddress, nullBitmapAddress, rowCount, nullable, sink);
                break;
            case TYPE_LONG256:
                decodeLong256s(valuesAddress, nullBitmapAddress, rowCount, nullable, sink);
                break;
        }
    }

    private void decodeBytes(long valuesAddress, long nullBitmapAddress, int rowCount, boolean nullable, ColumnSink sink) {
        int valueOffset = 0;
        for (int i = 0; i < rowCount; i++) {
            if (nullable && QwpNullBitmap.isNull(nullBitmapAddress, i)) {
                sink.putNull(i);
            } else {
                byte value = Unsafe.getUnsafe().getByte(valuesAddress + valueOffset);
                sink.putByte(i, value);
                valueOffset++;
            }
        }
    }

    private void decodeShorts(long valuesAddress, long nullBitmapAddress, int rowCount, boolean nullable, ColumnSink sink) {
        int valueOffset = 0;
        for (int i = 0; i < rowCount; i++) {
            if (nullable && QwpNullBitmap.isNull(nullBitmapAddress, i)) {
                sink.putNull(i);
            } else {
                short value = Unsafe.getUnsafe().getShort(valuesAddress + (long) valueOffset * 2);
                sink.putShort(i, value);
                valueOffset++;
            }
        }
    }

    private void decodeInts(long valuesAddress, long nullBitmapAddress, int rowCount, boolean nullable, ColumnSink sink) {
        int valueOffset = 0;
        for (int i = 0; i < rowCount; i++) {
            if (nullable && QwpNullBitmap.isNull(nullBitmapAddress, i)) {
                sink.putNull(i);
            } else {
                int value = Unsafe.getUnsafe().getInt(valuesAddress + (long) valueOffset * 4);
                sink.putInt(i, value);
                valueOffset++;
            }
        }
    }

    private void decodeLongs(long valuesAddress, long nullBitmapAddress, int rowCount, boolean nullable, ColumnSink sink) {
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
    }

    private void decodeFloats(long valuesAddress, long nullBitmapAddress, int rowCount, boolean nullable, ColumnSink sink) {
        int valueOffset = 0;
        for (int i = 0; i < rowCount; i++) {
            if (nullable && QwpNullBitmap.isNull(nullBitmapAddress, i)) {
                sink.putNull(i);
            } else {
                int bits = Unsafe.getUnsafe().getInt(valuesAddress + (long) valueOffset * 4);
                float value = Float.intBitsToFloat(bits);
                sink.putFloat(i, value);
                valueOffset++;
            }
        }
    }

    private void decodeDoubles(long valuesAddress, long nullBitmapAddress, int rowCount, boolean nullable, ColumnSink sink) {
        int valueOffset = 0;
        for (int i = 0; i < rowCount; i++) {
            if (nullable && QwpNullBitmap.isNull(nullBitmapAddress, i)) {
                sink.putNull(i);
            } else {
                long bits = Unsafe.getUnsafe().getLong(valuesAddress + (long) valueOffset * 8);
                double value = Double.longBitsToDouble(bits);
                sink.putDouble(i, value);
                valueOffset++;
            }
        }
    }

    private void decodeUuids(long valuesAddress, long nullBitmapAddress, int rowCount, boolean nullable, ColumnSink sink) {
        // UUID is 16 bytes, big-endian
        int valueOffset = 0;
        for (int i = 0; i < rowCount; i++) {
            if (nullable && QwpNullBitmap.isNull(nullBitmapAddress, i)) {
                sink.putNull(i);
            } else {
                long addr = valuesAddress + (long) valueOffset * 16;
                // Big-endian: read hi first, then lo
                long hi = Long.reverseBytes(Unsafe.getUnsafe().getLong(addr));
                long lo = Long.reverseBytes(Unsafe.getUnsafe().getLong(addr + 8));
                sink.putUuid(i, hi, lo);
                valueOffset++;
            }
        }
    }

    private void decodeLong256s(long valuesAddress, long nullBitmapAddress, int rowCount, boolean nullable, ColumnSink sink) {
        // LONG256 is 32 bytes, big-endian
        int valueOffset = 0;
        for (int i = 0; i < rowCount; i++) {
            if (nullable && QwpNullBitmap.isNull(nullBitmapAddress, i)) {
                sink.putNull(i);
            } else {
                long addr = valuesAddress + (long) valueOffset * 32;
                // Big-endian: read most significant first
                long l0 = Long.reverseBytes(Unsafe.getUnsafe().getLong(addr));
                long l1 = Long.reverseBytes(Unsafe.getUnsafe().getLong(addr + 8));
                long l2 = Long.reverseBytes(Unsafe.getUnsafe().getLong(addr + 16));
                long l3 = Long.reverseBytes(Unsafe.getUnsafe().getLong(addr + 24));
                sink.putLong256(i, l0, l1, l2, l3);
                valueOffset++;
            }
        }
    }

    @Override
    public int expectedSize(int rowCount, boolean nullable) {
        int size = 0;
        if (nullable) {
            size += QwpNullBitmap.sizeInBytes(rowCount);
        }
        size += rowCount * valueSize;
        return size;
    }

    /**
     * Gets the type code this decoder handles.
     */
    public byte getTypeCode() {
        return typeCode;
    }

    /**
     * Gets the size in bytes of each value.
     */
    public int getValueSize() {
        return valueSize;
    }

    // ==================== Static Encoding Methods ====================

    /**
     * Encodes fixed-width values to direct memory.
     * Only non-null values are written to the buffer.
     *
     * @param destAddress destination address
     * @param values      values to encode
     * @param nulls       null flags (can be null if not nullable)
     * @param typeCode    the type code
     * @return address after encoded data
     */
    public static long encode(long destAddress, long[] values, boolean[] nulls, byte typeCode) {
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

        // Write only non-null values
        switch (typeCode & TYPE_MASK) {
            case TYPE_BYTE:
                for (int i = 0; i < rowCount; i++) {
                    if (nullable && nulls[i]) continue;
                    Unsafe.getUnsafe().putByte(pos++, (byte) values[i]);
                }
                break;
            case TYPE_SHORT:
            case TYPE_CHAR:
                for (int i = 0; i < rowCount; i++) {
                    if (nullable && nulls[i]) continue;
                    Unsafe.getUnsafe().putShort(pos, (short) values[i]);
                    pos += 2;
                }
                break;
            case TYPE_INT:
            case TYPE_FLOAT:
                for (int i = 0; i < rowCount; i++) {
                    if (nullable && nulls[i]) continue;
                    Unsafe.getUnsafe().putInt(pos, (int) values[i]);
                    pos += 4;
                }
                break;
            case TYPE_LONG:
            case TYPE_DOUBLE:
            case TYPE_TIMESTAMP:
            case TYPE_TIMESTAMP_NANOS:
            case TYPE_DATE:
                for (int i = 0; i < rowCount; i++) {
                    if (nullable && nulls[i]) continue;
                    Unsafe.getUnsafe().putLong(pos, values[i]);
                    pos += 8;
                }
                break;
        }

        return pos;
    }

    /**
     * Encodes double values to direct memory.
     * Only non-null values are written to the buffer.
     *
     * @param destAddress destination address
     * @param values      values to encode
     * @param nulls       null flags (can be null if not nullable)
     * @return address after encoded data
     */
    public static long encodeDoubles(long destAddress, double[] values, boolean[] nulls) {
        int rowCount = values.length;
        boolean nullable = nulls != null;
        long pos = destAddress;

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

        for (int i = 0; i < rowCount; i++) {
            if (nullable && nulls[i]) continue;
            Unsafe.getUnsafe().putLong(pos, Double.doubleToLongBits(values[i]));
            pos += 8;
        }

        return pos;
    }

    /**
     * Encodes float values to direct memory.
     * Only non-null values are written to the buffer.
     *
     * @param destAddress destination address
     * @param values      values to encode
     * @param nulls       null flags (can be null if not nullable)
     * @return address after encoded data
     */
    public static long encodeFloats(long destAddress, float[] values, boolean[] nulls) {
        int rowCount = values.length;
        boolean nullable = nulls != null;
        long pos = destAddress;

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

        for (int i = 0; i < rowCount; i++) {
            if (nullable && nulls[i]) continue;
            Unsafe.getUnsafe().putInt(pos, Float.floatToIntBits(values[i]));
            pos += 4;
        }

        return pos;
    }

    /**
     * Encodes UUID values (big-endian) to direct memory.
     * Only non-null values are written to the buffer.
     *
     * @param destAddress destination address
     * @param hiValues    high 64 bits of each UUID
     * @param loValues    low 64 bits of each UUID
     * @param nulls       null flags (can be null if not nullable)
     * @return address after encoded data
     */
    public static long encodeUuids(long destAddress, long[] hiValues, long[] loValues, boolean[] nulls) {
        int rowCount = hiValues.length;
        boolean nullable = nulls != null;
        long pos = destAddress;

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

        // Big-endian: write hi first, then lo, only for non-null values
        for (int i = 0; i < rowCount; i++) {
            if (nullable && nulls[i]) continue;
            Unsafe.getUnsafe().putLong(pos, Long.reverseBytes(hiValues[i]));
            Unsafe.getUnsafe().putLong(pos + 8, Long.reverseBytes(loValues[i]));
            pos += 16;
        }

        return pos;
    }
}
