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

import io.questdb.cairo.ColumnType;
import io.questdb.std.Unsafe;

/**
 * Decoder for GEOHASH columns in ILP v4 format.
 * <p>
 * GeoHash encoding:
 * <pre>
 * [Null bitmap if nullable]
 * Precision: varint (number of bits, 1-60)
 * For each row:
 *   Packed geohash value using ceil(precision/8) bytes
 * </pre>
 * <p>
 * QuestDB has four GeoHash storage types:
 * <ul>
 *   <li>GEOBYTE: 1-7 bits (stored in 1 byte)</li>
 *   <li>GEOSHORT: 8-15 bits (stored in 2 bytes)</li>
 *   <li>GEOINT: 16-31 bits (stored in 4 bytes)</li>
 *   <li>GEOLONG: 32-60 bits (stored in 8 bytes)</li>
 * </ul>
 */
public final class QwpGeoHashDecoder implements QwpColumnDecoder {

    /**
     * Maximum GeoHash precision in bits (matches QuestDB's GEOLONG_MAX_BITS).
     */
    public static final int MAX_BITS = ColumnType.GEOLONG_MAX_BITS;

    /**
     * Minimum GeoHash precision in bits.
     */
    public static final int MIN_BITS = ColumnType.GEOBYTE_MIN_BITS;

    public static final QwpGeoHashDecoder INSTANCE = new QwpGeoHashDecoder();

    private final QwpVarint.DecodeResult decodeResult = new QwpVarint.DecodeResult();

    private QwpGeoHashDecoder() {
    }

    @Override
    public int decode(long sourceAddress, int sourceLength, int rowCount, boolean nullable, ColumnSink sink) throws QwpParseException {
        if (rowCount == 0) {
            return 0;
        }

        int offset = 0;
        long limit = sourceAddress + sourceLength;

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

        // Parse precision (number of bits)
        QwpVarint.decode(sourceAddress + offset, limit, decodeResult);
        int precision = (int) decodeResult.value;
        offset += decodeResult.bytesRead;

        // Validate precision
        if (precision < MIN_BITS || precision > MAX_BITS) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INVALID_COLUMN_TYPE,
                    "invalid GeoHash precision: " + precision + " (must be " + MIN_BITS + "-" + MAX_BITS + ")"
            );
        }

        // Calculate value size in bytes based on precision
        int valueSize = (precision + 7) / 8;

        // Check if we have enough data for all values
        int valuesSize = rowCount * valueSize;
        if (offset + valuesSize > sourceLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "insufficient data for GeoHash values"
            );
        }

        // Decode values
        GeoHashColumnSink geoSink = (GeoHashColumnSink) sink;
        long valuesAddress = sourceAddress + offset;

        for (int i = 0; i < rowCount; i++) {
            if (nullable && QwpNullBitmap.isNull(nullBitmapAddress, i)) {
                sink.putNull(i);
            } else {
                long value = readValue(valuesAddress + (long) i * valueSize, valueSize);
                geoSink.putGeoHash(i, value, precision);
            }
        }

        return offset + valuesSize;
    }

    /**
     * Reads a geohash value of the specified size (1-8 bytes).
     */
    private static long readValue(long address, int valueSize) {
        return switch (valueSize) {
            case 1 -> Unsafe.getUnsafe().getByte(address) & 0xFFL;
            case 2 -> Unsafe.getUnsafe().getShort(address) & 0xFFFFL;
            case 3 -> (Unsafe.getUnsafe().getShort(address) & 0xFFFFL) |
                    ((Unsafe.getUnsafe().getByte(address + 2) & 0xFFL) << 16);
            case 4 -> Unsafe.getUnsafe().getInt(address) & 0xFFFFFFFFL;
            case 5 -> (Unsafe.getUnsafe().getInt(address) & 0xFFFFFFFFL) |
                    ((Unsafe.getUnsafe().getByte(address + 4) & 0xFFL) << 32);
            case 6 -> (Unsafe.getUnsafe().getInt(address) & 0xFFFFFFFFL) |
                    ((Unsafe.getUnsafe().getShort(address + 4) & 0xFFFFL) << 32);
            case 7 -> (Unsafe.getUnsafe().getInt(address) & 0xFFFFFFFFL) |
                    ((Unsafe.getUnsafe().getShort(address + 4) & 0xFFFFL) << 32) |
                    ((Unsafe.getUnsafe().getByte(address + 6) & 0xFFL) << 48);
            case 8 -> Unsafe.getUnsafe().getLong(address);
            default -> throw new IllegalArgumentException("Invalid value size: " + valueSize);
        };
    }

    @Override
    public int expectedSize(int rowCount, boolean nullable) {
        // Minimum size: 1-byte precision + 1 byte per value
        int size = 1; // precision varint (minimum 1 byte)
        if (nullable) {
            size += QwpNullBitmap.sizeInBytes(rowCount);
        }
        size += rowCount; // minimum 1 byte per value
        return size;
    }

    /**
     * Returns the QuestDB column type for a given precision.
     *
     * @param precision number of bits (1-60)
     * @return QuestDB column type (GEOBYTE, GEOSHORT, GEOINT, or GEOLONG)
     */
    public static int getColumnType(int precision) {
        return ColumnType.getGeoHashTypeWithBits(precision);
    }

    /**
     * Returns the storage size in bytes for a given precision.
     *
     * @param precision number of bits
     * @return storage size (1, 2, 4, or 8 bytes)
     */
    public static int getStorageSize(int precision) {
        if (precision <= ColumnType.GEOBYTE_MAX_BITS) {
            return 1;
        } else if (precision <= ColumnType.GEOSHORT_MAX_BITS) {
            return 2;
        } else if (precision <= ColumnType.GEOINT_MAX_BITS) {
            return 4;
        } else {
            return 8;
        }
    }

    /**
     * Extended sink interface for GeoHash columns.
     */
    public interface GeoHashColumnSink extends ColumnSink {
        /**
         * Called for each decoded GeoHash value.
         *
         * @param rowIndex  row index
         * @param value     geohash value (bit-packed)
         * @param precision number of bits
         */
        void putGeoHash(int rowIndex, long value, int precision);
    }

    /**
     * Simple array-based sink for testing.
     */
    public static class ArrayGeoHashSink implements GeoHashColumnSink {
        private final long[] values;
        private final int[] precisions;
        private final boolean[] nulls;

        public ArrayGeoHashSink(int rowCount) {
            this.values = new long[rowCount];
            this.precisions = new int[rowCount];
            this.nulls = new boolean[rowCount];
        }

        @Override
        public void putGeoHash(int rowIndex, long value, int precision) {
            values[rowIndex] = value;
            precisions[rowIndex] = precision;
        }

        @Override
        public void putNull(int rowIndex) {
            nulls[rowIndex] = true;
        }

        public long getValue(int rowIndex) {
            return values[rowIndex];
        }

        public int getPrecision(int rowIndex) {
            return precisions[rowIndex];
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
     * Encodes GeoHash values to direct memory.
     *
     * @param destAddress destination address
     * @param values      geohash values
     * @param precision   number of bits for all values
     * @param nulls       null flags (can be null if not nullable)
     * @return address after encoded data
     */
    public static long encode(long destAddress, long[] values, int precision, boolean[] nulls) {
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

        // Write precision
        pos = QwpVarint.encode(pos, precision);

        // Write values
        int valueSize = (precision + 7) / 8;
        for (int i = 0; i < rowCount; i++) {
            writeValue(pos, values[i], valueSize);
            pos += valueSize;
        }

        return pos;
    }

    /**
     * Writes a geohash value of the specified size (1-8 bytes).
     */
    private static void writeValue(long address, long value, int valueSize) {
        switch (valueSize) {
            case 1:
                Unsafe.getUnsafe().putByte(address, (byte) value);
                break;
            case 2:
                Unsafe.getUnsafe().putShort(address, (short) value);
                break;
            case 3:
                Unsafe.getUnsafe().putShort(address, (short) value);
                Unsafe.getUnsafe().putByte(address + 2, (byte) (value >> 16));
                break;
            case 4:
                Unsafe.getUnsafe().putInt(address, (int) value);
                break;
            case 5:
                Unsafe.getUnsafe().putInt(address, (int) value);
                Unsafe.getUnsafe().putByte(address + 4, (byte) (value >> 32));
                break;
            case 6:
                Unsafe.getUnsafe().putInt(address, (int) value);
                Unsafe.getUnsafe().putShort(address + 4, (short) (value >> 32));
                break;
            case 7:
                Unsafe.getUnsafe().putInt(address, (int) value);
                Unsafe.getUnsafe().putShort(address + 4, (short) (value >> 32));
                Unsafe.getUnsafe().putByte(address + 6, (byte) (value >> 48));
                break;
            case 8:
                Unsafe.getUnsafe().putLong(address, value);
                break;
            default:
                throw new IllegalArgumentException("Invalid value size: " + valueSize);
        }
    }

    /**
     * Calculates the encoded size in bytes.
     *
     * @param rowCount  number of rows
     * @param precision number of bits
     * @param nullable  whether column is nullable
     * @return encoded size in bytes
     */
    public static int calculateEncodedSize(int rowCount, int precision, boolean nullable) {
        int size = 0;
        if (nullable) {
            size += QwpNullBitmap.sizeInBytes(rowCount);
        }
        // Calculate varint size for precision (precision is 1-60, fits in 1 byte)
        size += varintSize(precision);
        size += rowCount * ((precision + 7) / 8);
        return size;
    }

    /**
     * Calculates the size of a varint-encoded value.
     */
    private static int varintSize(long value) {
        int size = 1;
        while (value >= 0x80) {
            size++;
            value >>>= 7;
        }
        return size;
    }
}
