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

import io.questdb.cairo.ColumnType;
import io.questdb.std.Unsafe;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_GEOHASH;

/**
 * Streaming cursor for GEOHASH columns.
 * <p>
 * Wire format:
 * <pre>
 * [if null bitmap is present]: ceil(rowCount/8) bytes
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
    private int precision;
    private int valueSize;
    private long valuesAddress;

    @Override
    public boolean advanceRow() {
        currentRow++;

        if (nullBitmapAddress != 0) {
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
     * @return bytes consumed from dataAddress
     * @throws QwpParseException if parsing fails
     */
    public int of(long dataAddress, int dataLength, int rowCount) throws QwpParseException {
        int offset = 0;
        long limit = dataAddress + dataLength;

        // Read null bitmap flag
        if (offset >= dataLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "geohash column data truncated: expected null bitmap flag"
            );
        }
        int nullCount;
        if (Unsafe.getByte(dataAddress + offset) != 0) {
            offset++;
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            if (offset + (long) bitmapSize > dataLength) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                        "geohash column data truncated: expected null bitmap"
                );
            }
            this.nullBitmapAddress = dataAddress + offset;
            nullCount = QwpNullBitmap.countNulls(nullBitmapAddress, rowCount);
            offset += bitmapSize;
        } else {
            offset++;
            this.nullBitmapAddress = 0;
            nullCount = 0;
        }

        // Parse and validate precision (validate on long before casting to int
        // to prevent truncation from bypassing the range check)
        QwpVarint.decode(dataAddress + offset, limit, decodeResult);
        if (decodeResult.value < ColumnType.GEOBYTE_MIN_BITS || decodeResult.value > ColumnType.GEOLONG_MAX_BITS) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INVALID_COLUMN_TYPE,
                    "invalid GeoHash precision: " + decodeResult.value
            );
        }
        this.precision = (int) decodeResult.value;
        offset += decodeResult.bytesRead;

        this.valueSize = (precision + 7) / 8;
        int valueCount = rowCount - nullCount;
        long valuesSize = (long) valueCount * valueSize;
        if (offset + valuesSize > dataLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "geohash column data truncated: expected " + valueCount + " values"
            );
        }
        this.valuesAddress = dataAddress + offset;
        offset += (int) valuesSize;

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
            case 1 -> Unsafe.getByte(address) & 0xFFL;
            case 2 -> Unsafe.getShort(address) & 0xFFFFL;
            case 3 -> (Unsafe.getShort(address) & 0xFFFFL) |
                    ((Unsafe.getByte(address + 2) & 0xFFL) << 16);
            case 4 -> Unsafe.getInt(address) & 0xFFFFFFFFL;
            case 5 -> (Unsafe.getInt(address) & 0xFFFFFFFFL) |
                    ((Unsafe.getByte(address + 4) & 0xFFL) << 32);
            case 6 -> (Unsafe.getInt(address) & 0xFFFFFFFFL) |
                    ((Unsafe.getShort(address + 4) & 0xFFFFL) << 32);
            case 7 -> (Unsafe.getInt(address) & 0xFFFFFFFFL) |
                    ((Unsafe.getShort(address + 4) & 0xFFFFL) << 32) |
                    ((Unsafe.getByte(address + 6) & 0xFFL) << 48);
            case 8 -> Unsafe.getLong(address);
            default -> throw new IllegalArgumentException("Invalid value size: " + valueSize);
        };
    }
}
