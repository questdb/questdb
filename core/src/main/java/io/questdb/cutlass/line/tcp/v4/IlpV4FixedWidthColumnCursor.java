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
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;

import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.*;

/**
 * Streaming cursor for fixed-width column types.
 * <p>
 * Supports: BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DATE, UUID, LONG256.
 * <p>
 * Wire format:
 * <pre>
 * [null bitmap if nullable]: ceil(rowCount/8) bytes
 * [values]: (rowCount - nullCount) * valueSize bytes
 * </pre>
 */
public class IlpV4FixedWidthColumnCursor implements IlpV4ColumnCursor {

    private final DirectUtf8String nameUtf8 = new DirectUtf8String();

    // Configuration
    private byte typeCode;
    private boolean nullable;
    private int rowCount;
    private int valueSize;

    // Wire pointers
    private long nullBitmapAddress;
    private long valuesAddress;

    // Iteration state
    private int currentRow;
    private int currentValueIndex;  // Index into non-null values
    private boolean currentIsNull;

    // Current value storage (to avoid reading twice)
    private long currentLong;       // For BYTE, SHORT, INT, LONG, DATE, TIMESTAMP
    private double currentDouble;   // For FLOAT, DOUBLE
    private long currentUuidHi;     // For UUID
    private long currentUuidLo;
    private long currentLong256_0;  // For LONG256
    private long currentLong256_1;
    private long currentLong256_2;
    private long currentLong256_3;

    /**
     * Initializes this cursor for the given column data.
     *
     * @param dataAddress   address of column data (starts at null bitmap if nullable, else values)
     * @param dataLength    available bytes
     * @param rowCount      number of rows
     * @param typeCode      column type code
     * @param nullable      whether column is nullable
     * @param nameAddress   address of column name UTF-8 bytes
     * @param nameLength    column name length in bytes
     * @return bytes consumed from dataAddress
     */
    public int of(long dataAddress, int dataLength, int rowCount, byte typeCode, boolean nullable,
                  long nameAddress, int nameLength) {
        this.typeCode = typeCode;
        this.nullable = nullable;
        this.rowCount = rowCount;
        this.valueSize = IlpV4Constants.getFixedTypeSize(typeCode);
        this.nameUtf8.of(nameAddress, nameAddress + nameLength);

        int offset = 0;
        int nullCount = 0;

        if (nullable) {
            int bitmapSize = IlpV4NullBitmap.sizeInBytes(rowCount);
            this.nullBitmapAddress = dataAddress;
            nullCount = IlpV4NullBitmap.countNulls(dataAddress, rowCount);
            offset += bitmapSize;
        } else {
            this.nullBitmapAddress = 0;
        }

        this.valuesAddress = dataAddress + offset;
        int valueCount = rowCount - nullCount;
        offset += valueCount * valueSize;

        resetRowPosition();
        return offset;
    }

    @Override
    public DirectUtf8Sequence getNameUtf8() {
        return nameUtf8;
    }

    @Override
    public byte getTypeCode() {
        return typeCode;
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
    public void advanceRow() throws IlpV4ParseException {
        currentRow++;

        if (nullable && nullBitmapAddress != 0) {
            currentIsNull = IlpV4NullBitmap.isNull(nullBitmapAddress, currentRow);
            if (currentIsNull) {
                return;
            }
        } else {
            currentIsNull = false;
        }

        // Read value from wire
        long valueAddress = valuesAddress + (long) currentValueIndex * valueSize;
        readCurrentValue(valueAddress);
        currentValueIndex++;
    }

    private void readCurrentValue(long address) {
        int type = typeCode & TYPE_MASK;
        switch (type) {
            case TYPE_BYTE:
                currentLong = Unsafe.getUnsafe().getByte(address);
                break;
            case TYPE_SHORT:
                currentLong = Unsafe.getUnsafe().getShort(address);
                break;
            case TYPE_INT:
                currentLong = Unsafe.getUnsafe().getInt(address);
                break;
            case TYPE_LONG:
            case TYPE_DATE:
            case TYPE_TIMESTAMP:
            case TYPE_TIMESTAMP_NANOS:
                currentLong = Unsafe.getUnsafe().getLong(address);
                break;
            case TYPE_FLOAT:
                currentDouble = Unsafe.getUnsafe().getFloat(address);
                break;
            case TYPE_DOUBLE:
                currentDouble = Unsafe.getUnsafe().getDouble(address);
                break;
            case TYPE_UUID:
                // UUID is stored big-endian: hi bytes first, then lo bytes
                currentUuidHi = Long.reverseBytes(Unsafe.getUnsafe().getLong(address));
                currentUuidLo = Long.reverseBytes(Unsafe.getUnsafe().getLong(address + 8));
                break;
            case TYPE_LONG256:
                // LONG256 is stored big-endian: 4 longs, most significant first
                currentLong256_3 = Long.reverseBytes(Unsafe.getUnsafe().getLong(address));
                currentLong256_2 = Long.reverseBytes(Unsafe.getUnsafe().getLong(address + 8));
                currentLong256_1 = Long.reverseBytes(Unsafe.getUnsafe().getLong(address + 16));
                currentLong256_0 = Long.reverseBytes(Unsafe.getUnsafe().getLong(address + 24));
                break;
        }
    }

    @Override
    public int getCurrentRow() {
        return currentRow;
    }

    @Override
    public void resetRowPosition() {
        currentRow = -1;
        currentValueIndex = 0;
        currentIsNull = false;
    }

    @Override
    public void clear() {
        nameUtf8.clear();
        typeCode = 0;
        nullable = false;
        rowCount = 0;
        valueSize = 0;
        nullBitmapAddress = 0;
        valuesAddress = 0;
        resetRowPosition();
    }

    // ==================== Type-Specific Getters ====================

    /**
     * Returns current row's byte value.
     */
    public byte getByte() {
        return (byte) currentLong;
    }

    /**
     * Returns current row's short value.
     */
    public short getShort() {
        return (short) currentLong;
    }

    /**
     * Returns current row's int value.
     */
    public int getInt() {
        return (int) currentLong;
    }

    /**
     * Returns current row's long value.
     */
    public long getLong() {
        return currentLong;
    }

    /**
     * Returns current row's float value.
     */
    public float getFloat() {
        return (float) currentDouble;
    }

    /**
     * Returns current row's double value.
     */
    public double getDouble() {
        return currentDouble;
    }

    /**
     * Returns current row's date value (milliseconds since epoch).
     */
    public long getDate() {
        return currentLong;
    }

    /**
     * Returns current row's timestamp value (microseconds or nanoseconds since epoch).
     */
    public long getTimestamp() {
        return currentLong;
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
}
