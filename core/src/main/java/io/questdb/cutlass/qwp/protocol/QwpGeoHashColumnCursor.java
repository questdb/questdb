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
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_GEOHASH;

/**
 * Streaming cursor for GEOHASH columns.
 * <p>
 * Wire format:
 * <pre>
 * [null bitmap if nullable]: ceil(rowCount/8) bytes
 * [precision]: varint (number of bits, 1-60)
 * [values]: rowCount * ceil(precision/8) bytes
 * </pre>
 */
public final class QwpGeoHashColumnCursor implements QwpColumnCursor {

    private final DirectUtf8String nameUtf8 = new DirectUtf8String();
    private final QwpVarint.DecodeResult decodeResult = new QwpVarint.DecodeResult();

    // Configuration
    private boolean nullable;
    private int rowCount;
    private int precision;
    private int valueSize;

    // Wire pointers
    private long nullBitmapAddress;
    private long valuesAddress;

    // Iteration state
    private int currentRow;
    private boolean currentIsNull;
    private long currentGeoHash;

    /**
     * Initializes this cursor for the given column data.
     *
     * @param dataAddress   address of column data
     * @param dataLength    available bytes
     * @param rowCount      number of rows
     * @param nullable      whether column is nullable
     * @param nameAddress   address of column name UTF-8 bytes
     * @param nameLength    column name length in bytes
     * @return bytes consumed from dataAddress
     * @throws QwpParseException if parsing fails
     */
    public int of(long dataAddress, int dataLength, int rowCount, boolean nullable,
                  long nameAddress, int nameLength) throws QwpParseException {
        this.nullable = nullable;
        this.rowCount = rowCount;
        this.nameUtf8.of(nameAddress, nameAddress + nameLength);

        int offset = 0;
        long limit = dataAddress + dataLength;

        if (nullable) {
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            this.nullBitmapAddress = dataAddress;
            offset += bitmapSize;
        } else {
            this.nullBitmapAddress = 0;
        }

        // Parse precision
        QwpVarint.decode(dataAddress + offset, limit, decodeResult);
        this.precision = (int) decodeResult.value;
        offset += decodeResult.bytesRead;

        // Validate precision
        if (precision < QwpGeoHashDecoder.MIN_BITS || precision > QwpGeoHashDecoder.MAX_BITS) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INVALID_COLUMN_TYPE,
                    "invalid GeoHash precision: " + precision
            );
        }

        this.valueSize = (precision + 7) / 8;
        this.valuesAddress = dataAddress + offset;
        offset += rowCount * valueSize;

        resetRowPosition();
        return offset;
    }

    @Override
    public DirectUtf8Sequence getNameUtf8() {
        return nameUtf8;
    }

    @Override
    public byte getTypeCode() {
        return TYPE_GEOHASH;
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public boolean isNull() {
        return currentIsNull;
    }

    @Override
    public boolean advanceRow() throws QwpParseException {
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

        // Read geohash value
        long valueAddress = valuesAddress + (long) currentRow * valueSize;
        currentGeoHash = readValue(valueAddress, valueSize);
        return false;
    }

    private static long readValue(long address, int valueSize) {
        switch (valueSize) {
            case 1:
                return Unsafe.getUnsafe().getByte(address) & 0xFFL;
            case 2:
                return Unsafe.getUnsafe().getShort(address) & 0xFFFFL;
            case 3:
                return (Unsafe.getUnsafe().getShort(address) & 0xFFFFL) |
                        ((Unsafe.getUnsafe().getByte(address + 2) & 0xFFL) << 16);
            case 4:
                return Unsafe.getUnsafe().getInt(address) & 0xFFFFFFFFL;
            case 5:
                return (Unsafe.getUnsafe().getInt(address) & 0xFFFFFFFFL) |
                        ((Unsafe.getUnsafe().getByte(address + 4) & 0xFFL) << 32);
            case 6:
                return (Unsafe.getUnsafe().getInt(address) & 0xFFFFFFFFL) |
                        ((Unsafe.getUnsafe().getShort(address + 4) & 0xFFFFL) << 32);
            case 7:
                return (Unsafe.getUnsafe().getInt(address) & 0xFFFFFFFFL) |
                        ((Unsafe.getUnsafe().getShort(address + 4) & 0xFFFFL) << 32) |
                        ((Unsafe.getUnsafe().getByte(address + 6) & 0xFFL) << 48);
            case 8:
                return Unsafe.getUnsafe().getLong(address);
            default:
                throw new IllegalArgumentException("Invalid value size: " + valueSize);
        }
    }

    @Override
    public int getCurrentRow() {
        return currentRow;
    }

    @Override
    public void resetRowPosition() {
        currentRow = -1;
        currentIsNull = false;
        currentGeoHash = 0;
    }

    @Override
    public void clear() {
        nameUtf8.clear();
        nullable = false;
        rowCount = 0;
        precision = 0;
        valueSize = 0;
        nullBitmapAddress = 0;
        valuesAddress = 0;
        resetRowPosition();
    }

    /**
     * Returns the GeoHash precision in bits.
     */
    public int getPrecision() {
        return precision;
    }

    /**
     * Returns current row's GeoHash value.
     */
    public long getGeoHash() {
        return currentGeoHash;
    }
}
