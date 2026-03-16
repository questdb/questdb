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

import io.questdb.cairo.ColumnType;
import io.questdb.std.Unsafe;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_GEOHASH;

/**
 * Streaming cursor for GEOHASH columns.
 * <p>
 * Wire format:
 * <pre>
 * [null bitmap if nullable]: ceil(rowCount/8) bytes
 * [precision]: varint (number of bits, 1-60)
 * [values]: (rowCount - nullCount) * ceil(precision/8) bytes (packed, non-null only)
 * </pre>
 */
public final class QwpGeoHashColumnCursor implements QwpColumnCursor {

    private final QwpVarint.DecodeResult decodeResult = new QwpVarint.DecodeResult();
    private long currentGeoHash;
    private boolean currentIsNull;
    // Iteration state
    private int currentRow;
    private int currentValueIndex;
    // Wire pointers
    private long nullBitmapAddress;
    // Configuration
    private boolean nullable;
    private int precision;
    private int valueSize;
    private long valuesAddress;

    @Override
    public boolean advanceRow() {
        currentRow++;

        if (nullable && nullBitmapAddress != 0) {
            currentIsNull = QwpNullBitmap.isNull(nullBitmapAddress, currentRow);
            if (currentIsNull) {
                currentGeoHash = 0;
                return true;
            }
        } else {
            currentIsNull = false;
        }

        // Read geohash value from packed array (non-null values only)
        long valueAddress = valuesAddress + (long) currentValueIndex * valueSize;
        currentGeoHash = readValue(valueAddress, valueSize);
        currentValueIndex++;
        return false;
    }

    @Override
    public void clear() {
        nullable = false;
        precision = 0;
        valueSize = 0;
        nullBitmapAddress = 0;
        valuesAddress = 0;
        resetRowPosition();
    }

    /**
     * Returns current row's GeoHash value.
     */
    public long getGeoHash() {
        return currentGeoHash;
    }

    /**
     * Returns the GeoHash precision in bits.
     */
    public int getPrecision() {
        return precision;
    }

    @Override
    public byte getTypeCode() {
        return TYPE_GEOHASH;
    }

    @Override
    public boolean isNull() {
        return currentIsNull;
    }

    /**
     * Initializes this cursor for the given column data.
     *
     * @param dataAddress address of column data
     * @param dataLength  available bytes
     * @param rowCount    number of rows
     * @param nullable    whether column is nullable
     * @return bytes consumed from dataAddress
     * @throws QwpParseException if parsing fails
     */
    public int of(long dataAddress, int dataLength, int rowCount, boolean nullable) throws QwpParseException {
        this.nullable = nullable;

        int offset = 0;
        int nullCount = 0;
        long limit = dataAddress + dataLength;

        if (nullable) {
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            this.nullBitmapAddress = dataAddress;
            nullCount = QwpNullBitmap.countNulls(dataAddress, rowCount);
            offset += bitmapSize;
        } else {
            this.nullBitmapAddress = 0;
        }

        // Parse precision
        QwpVarint.decode(dataAddress + offset, limit, decodeResult);
        this.precision = (int) decodeResult.value;
        offset += decodeResult.bytesRead;

        // Validate precision
        if (precision < ColumnType.GEOBYTE_MIN_BITS || precision > ColumnType.GEOLONG_MAX_BITS) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INVALID_COLUMN_TYPE,
                    "invalid GeoHash precision: " + precision
            );
        }

        this.valueSize = (precision + 7) / 8;
        this.valuesAddress = dataAddress + offset;
        int valueCount = rowCount - nullCount;
        offset += valueCount * valueSize;

        resetRowPosition();
        return offset;
    }

    @Override
    public void resetRowPosition() {
        currentRow = -1;
        currentValueIndex = 0;
        currentIsNull = false;
        currentGeoHash = 0;
    }

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
}
