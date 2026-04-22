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

import io.questdb.std.Unsafe;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;

/**
 * Streaming cursor for fixed-width column types.
 * <p>
 * Supports: BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DATE, UUID, LONG256.
 * <p>
 * Wire format:
 * <pre>
 * [if null bitmap is present]: ceil(rowCount/8) bytes
 * [values]: (rowCount - nullCount) * valueSize bytes
 * </pre>
 */
public final class QwpFixedWidthColumnCursor implements QwpColumnCursor {

    private double currentDouble;   // For FLOAT, DOUBLE
    private boolean currentIsNull;
    // Current value storage (to avoid reading twice)
    private long currentLong;       // For BYTE, SHORT, INT, LONG, DATE, TIMESTAMP
    private long currentLong256_0;  // For LONG256
    private long currentLong256_1;
    private long currentLong256_2;
    private long currentLong256_3;
    // Iteration state
    private int currentRow;
    private long currentUuidHi;     // For UUID
    private long currentUuidLo;
    private int currentValueIndex;  // Index into non-null values
    // Wire pointers
    private long nullBitmapAddress;
    // Configuration
    private byte typeCode;
    private int valueCount;
    private int valueSize;
    private long valuesAddress;

    @Override
    public boolean advanceRow() {
        currentRow++;

        if (nullBitmapAddress != 0) {
            currentIsNull = QwpNullBitmap.isNull(nullBitmapAddress, currentRow);
            if (currentIsNull) {
                return true;
            }
        } else {
            currentIsNull = false;
        }

        // Read value from wire
        long valueAddress = valuesAddress + (long) currentValueIndex * valueSize;
        readCurrentValue(valueAddress);
        currentValueIndex++;
        return false;
    }

    @Override
    public void clear() {
        typeCode = 0;
        valueCount = 0;
        valueSize = 0;
        nullBitmapAddress = 0;
        valuesAddress = 0;
        resetRowPosition();
    }

    /**
     * Returns current row's date value (milliseconds since epoch).
     */
    public long getDate() {
        return currentLong;
    }

    /**
     * Returns current row's double value.
     */
    public double getDouble() {
        return currentDouble;
    }

    /**
     * Returns current row's long value.
     */
    public long getLong() {
        return currentLong;
    }

    /**
     * Returns current row's LONG256 component 0 (least significant).
     */
    public long getLong256_0() {
        return currentLong256_0;
    }

    /**
     * Returns current row's LONG256 component 1.
     */
    public long getLong256_1() {
        return currentLong256_1;
    }

    /**
     * Returns current row's LONG256 component 2.
     */
    public long getLong256_2() {
        return currentLong256_2;
    }

    /**
     * Returns current row's LONG256 component 3 (most significant).
     */
    public long getLong256_3() {
        return currentLong256_3;
    }

    /**
     * Returns the address of the null bitmap, or 0 if no null bitmap.
     * <p>
     * Used for bulk columnar writes to determine which positions need
     * null sentinel values.
     *
     * @return the memory address of null bitmap, or 0 if no null bitmap
     */
    public long getNullBitmapAddress() {
        return nullBitmapAddress;
    }

    /**
     * Returns current row's short value.
     */
    public short getShort() {
        return (short) currentLong;
    }

    /**
     * Returns current row's timestamp value (microseconds or nanoseconds since epoch).
     */
    public long getTimestamp() {
        return currentLong;
    }

    @Override
    public byte getTypeCode() {
        return typeCode;
    }

    /**
     * Returns current row's UUID high bits.
     */
    public long getUuidHi() {
        return currentUuidHi;
    }

    /**
     * Returns current row's UUID low bits.
     */
    public long getUuidLo() {
        return currentUuidLo;
    }

    /**
     * Returns the number of non-null values in this column.
     * <p>
     * This equals rowCount minus the number of null rows.
     *
     * @return count of non-null values
     */
    public int getValueCount() {
        return valueCount;
    }

    /**
     * Returns the size of each value in bytes.
     *
     * @return value size in bytes
     */
    public int getValueSize() {
        return valueSize;
    }

    /**
     * Returns the address of the packed non-null values array.
     * <p>
     * Used for bulk columnar writes where values can be copied directly
     * from wire format to storage.
     *
     * @return the memory address of values array
     */
    public long getValuesAddress() {
        return valuesAddress;
    }

    @Override
    public boolean isNull() {
        return currentIsNull;
    }

    /**
     * Initializes this cursor for the given column data.
     *
     * @param dataAddress address of column data
     * @param dataLength  available bytes from dataAddress
     * @param rowCount    number of rows
     * @param typeCode    column type code
     * @return bytes consumed from dataAddress
     * @throws QwpParseException if data is truncated
     */
    public int of(
            long dataAddress,
            int dataLength,
            int rowCount,
            byte typeCode
    ) throws QwpParseException {
        this.typeCode = typeCode;
        this.valueSize = QwpConstants.getFixedTypeSize(typeCode);

        int offset = 0;

        // Read null bitmap flag
        if (offset >= dataLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "fixed-width column data truncated: expected null bitmap flag"
            );
        }
        int nullCount;
        if (Unsafe.getUnsafe().getByte(dataAddress + offset) != 0) {
            offset++;
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            if (offset + bitmapSize > dataLength) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                        "fixed-width column data truncated: expected null bitmap"
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

        this.valueCount = rowCount - nullCount;
        int valueCount = this.valueCount;
        long valuesSize = (long) valueCount * valueSize;
        if (offset + valuesSize > dataLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "fixed-width column data truncated: expected " + valueCount + " values"
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
    }

    private void readCurrentValue(long address) {
        switch (typeCode) {
            case TYPE_BYTE -> currentLong = Unsafe.getUnsafe().getByte(address);
            case TYPE_SHORT, TYPE_CHAR -> currentLong = Unsafe.getUnsafe().getShort(address);
            case TYPE_INT -> currentLong = Unsafe.getUnsafe().getInt(address);
            case TYPE_LONG, TYPE_DATE, TYPE_TIMESTAMP, TYPE_TIMESTAMP_NANOS ->
                    currentLong = Unsafe.getUnsafe().getLong(address);
            case TYPE_FLOAT -> currentDouble = Unsafe.getUnsafe().getFloat(address);
            case TYPE_DOUBLE -> currentDouble = Unsafe.getUnsafe().getDouble(address);
            case TYPE_UUID -> {
                // UUID is stored little-endian: lo bytes first, then hi bytes
                currentUuidLo = Unsafe.getUnsafe().getLong(address);
                currentUuidHi = Unsafe.getUnsafe().getLong(address + 8);
            }
            case TYPE_LONG256 -> {
                // LONG256 is stored little-endian: 4 longs, least significant first
                currentLong256_0 = Unsafe.getUnsafe().getLong(address);
                currentLong256_1 = Unsafe.getUnsafe().getLong(address + 8);
                currentLong256_2 = Unsafe.getUnsafe().getLong(address + 16);
                currentLong256_3 = Unsafe.getUnsafe().getLong(address + 24);
            }
        }
    }
}
