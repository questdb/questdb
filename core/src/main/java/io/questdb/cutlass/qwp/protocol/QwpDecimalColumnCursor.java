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
 * Streaming cursor for DECIMAL64, DECIMAL128, and DECIMAL256 columns.
 * <p>
 * Wire format:
 * <pre>
 * [null bitmap if nullable]: ceil(rowCount/8) bytes
 * [scale: 1 byte] - shared for entire column
 * [values]: (rowCount - nullCount) * valueSize bytes, big-endian
 * </pre>
 * <p>
 * Value sizes:
 * <ul>
 *   <li>DECIMAL64: 8 bytes (1 long)</li>
 *   <li>DECIMAL128: 16 bytes (2 longs: high, low)</li>
 *   <li>DECIMAL256: 32 bytes (4 longs: hh, hl, lh, ll)</li>
 * </ul>
 */
public final class QwpDecimalColumnCursor implements QwpColumnCursor {

    private boolean currentIsNull;
    // Iteration state
    private int currentRow;
    // For DECIMAL128
    private long currentValue128Hi;
    private long currentValue128Lo;
    // For DECIMAL256
    private long currentValue256Hh;
    private long currentValue256Hl;
    private long currentValue256Lh;
    private long currentValue256Ll;
    // Current value storage
    // For DECIMAL64
    private long currentValue64;
    private int currentValueIndex;
    // Wire pointers
    private long nullBitmapAddress;
    private boolean nullable;
    private byte scale;
    // Configuration
    private byte typeCode;
    private int valueSize;
    private long valuesAddress;

    @Override
    public boolean advanceRow() throws QwpParseException {
        currentRow++;

        if (nullable && nullBitmapAddress != 0) {
            currentIsNull = QwpNullBitmap.isNull(nullBitmapAddress, currentRow);
            if (currentIsNull) {
                return true;
            }
        } else {
            currentIsNull = false;
        }

        // Read value from wire (big-endian)
        long valueAddress = valuesAddress + (long) currentValueIndex * valueSize;
        readCurrentValue(valueAddress);
        currentValueIndex++;
        return false;
    }

    @Override
    public void clear() {
        typeCode = TYPE_DECIMAL64;
        nullable = false;
        valueSize = 8;
        scale = 0;
        nullBitmapAddress = 0;
        valuesAddress = 0;
        resetRowPosition();
    }

    @Override
    public int getCurrentRow() {
        return currentRow;
    }

    /**
     * Returns the current row's Decimal128 high bits (most significant).
     *
     * @return high 64 bits
     */
    public long getDecimal128Hi() {
        return currentValue128Hi;
    }

    /**
     * Returns the current row's Decimal128 low bits (least significant).
     *
     * @return low 64 bits
     */
    public long getDecimal128Lo() {
        return currentValue128Lo;
    }

    /**
     * Returns the current row's Decimal256 highest bits.
     *
     * @return highest 64 bits (bits 192-255)
     */
    public long getDecimal256Hh() {
        return currentValue256Hh;
    }

    /**
     * Returns the current row's Decimal256 high-low bits.
     *
     * @return high-low 64 bits (bits 128-191)
     */
    public long getDecimal256Hl() {
        return currentValue256Hl;
    }

    /**
     * Returns the current row's Decimal256 low-high bits.
     *
     * @return low-high 64 bits (bits 64-127)
     */
    public long getDecimal256Lh() {
        return currentValue256Lh;
    }

    /**
     * Returns the current row's Decimal256 lowest bits.
     *
     * @return lowest 64 bits (bits 0-63)
     */
    public long getDecimal256Ll() {
        return currentValue256Ll;
    }

    /**
     * Returns the current row's Decimal64 unscaled value.
     *
     * @return unscaled 64-bit value
     */
    public long getDecimal64() {
        return currentValue64;
    }

    /**
     * Returns the scale (number of decimal places) for this column.
     * All values in the column share the same scale.
     *
     * @return scale (0-127)
     */
    public byte getScale() {
        return scale;
    }

    @Override
    public byte getTypeCode() {
        return typeCode;
    }

    @Override
    public boolean isNull() {
        return currentIsNull;
    }

    /**
     * Initializes this cursor for the given column data.
     *
     * @param dataAddress address of column data
     * @param rowCount    number of rows
     * @param typeCode    column type code (TYPE_DECIMAL64, TYPE_DECIMAL128, or TYPE_DECIMAL256)
     * @param nullable    whether column is nullable
     * @return bytes consumed from dataAddress
     */
    public int of(long dataAddress, int rowCount, byte typeCode, boolean nullable) {
        this.typeCode = typeCode;
        this.nullable = nullable;
        this.valueSize = getDecimalValueSize(typeCode);

        int offset = 0;
        int nullCount = 0;

        if (nullable) {
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            this.nullBitmapAddress = dataAddress;
            nullCount = QwpNullBitmap.countNulls(dataAddress, rowCount);
            offset += bitmapSize;
        } else {
            this.nullBitmapAddress = 0;
        }

        // Read scale byte
        this.scale = Unsafe.getUnsafe().getByte(dataAddress + offset);
        offset += 1;

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
    }

    private static int getDecimalValueSize(byte typeCode) {
        int type = typeCode & TYPE_MASK;
        return switch (type) {
            case TYPE_DECIMAL64 -> 8;
            case TYPE_DECIMAL128 -> 16;
            case TYPE_DECIMAL256 -> 32;
            default -> throw new IllegalArgumentException("Not a decimal type: " + typeCode);
        };
    }

    private void readCurrentValue(long address) {
        int type = typeCode & TYPE_MASK;
        switch (type) {
            case TYPE_DECIMAL64:
                // Big-endian
                currentValue64 = Long.reverseBytes(Unsafe.getUnsafe().getLong(address));
                break;
            case TYPE_DECIMAL128:
                // Big-endian: high then low
                currentValue128Hi = Long.reverseBytes(Unsafe.getUnsafe().getLong(address));
                currentValue128Lo = Long.reverseBytes(Unsafe.getUnsafe().getLong(address + 8));
                break;
            case TYPE_DECIMAL256:
                // Big-endian: hh, hl, lh, ll
                currentValue256Hh = Long.reverseBytes(Unsafe.getUnsafe().getLong(address));
                currentValue256Hl = Long.reverseBytes(Unsafe.getUnsafe().getLong(address + 8));
                currentValue256Lh = Long.reverseBytes(Unsafe.getUnsafe().getLong(address + 16));
                currentValue256Ll = Long.reverseBytes(Unsafe.getUnsafe().getLong(address + 24));
                break;
        }
    }
}
