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
 * Streaming cursor for DECIMAL64, DECIMAL128, and DECIMAL256 columns.
 * <p>
 * Wire format:
 * <pre>
 * [if null bitmap is present]: ceil(rowCount/8) bytes
 * [scale: 1 byte] - shared for entire column
 * [values]: (rowCount - nullCount) * valueSize bytes, little-endian
 * </pre>
 * <p>
 * Value sizes:
 * <ul>
 *   <li>DECIMAL64: 8 bytes</li>
 *   <li>DECIMAL128: 16 bytes</li>
 *   <li>DECIMAL256: 32 bytes</li>
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
    private byte scale;
    // Configuration
    private byte typeCode;
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

        // Read value from wire (little-endian)
        long valueAddress = valuesAddress + (long) currentValueIndex * valueSize;
        readCurrentValue(valueAddress);
        currentValueIndex++;
        return false;
    }

    @Override
    public void clear() {
        typeCode = TYPE_DECIMAL64;
        valueSize = 8;
        scale = 0;
        nullBitmapAddress = 0;
        valuesAddress = 0;
        resetRowPosition();
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
     * @param dataLength  available bytes from dataAddress
     * @param rowCount    number of rows
     * @param typeCode    column type code (TYPE_DECIMAL64, TYPE_DECIMAL128, or TYPE_DECIMAL256)
     * @return bytes consumed from dataAddress
     * @throws QwpParseException if data is truncated
     */
    public int of(long dataAddress, int dataLength, int rowCount, byte typeCode) throws QwpParseException {
        this.typeCode = typeCode;
        this.valueSize = getDecimalValueSize(typeCode);

        int offset = 0;

        // Read null bitmap flag
        if (offset >= dataLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "decimal column data truncated: expected null bitmap flag"
            );
        }
        int nullCount;
        if (Unsafe.getUnsafe().getByte(dataAddress + offset) != 0) {
            offset++;
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            if (offset + bitmapSize > dataLength) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                        "decimal column data truncated: expected null bitmap"
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

        // Read scale byte
        if (offset + 1 > dataLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "decimal column data truncated: expected scale byte"
            );
        }
        this.scale = Unsafe.getUnsafe().getByte(dataAddress + offset);
        offset += 1;

        int valueCount = rowCount - nullCount;
        long valuesSize = (long) valueCount * valueSize;
        if (offset + valuesSize > dataLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "decimal column data truncated: expected " + valueCount + " values"
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

    private static int getDecimalValueSize(byte typeCode) {
        return switch (typeCode) {
            case TYPE_DECIMAL64 -> 8;
            case TYPE_DECIMAL128 -> 16;
            case TYPE_DECIMAL256 -> 32;
            default -> throw new IllegalArgumentException("Not a decimal type: " + typeCode);
        };
    }

    private void readCurrentValue(long address) {
        switch (typeCode) {
            case TYPE_DECIMAL64 -> currentValue64 = Unsafe.getUnsafe().getLong(address);
            case TYPE_DECIMAL128 -> {
                currentValue128Lo = Unsafe.getUnsafe().getLong(address);
                currentValue128Hi = Unsafe.getUnsafe().getLong(address + 8);
            }
            case TYPE_DECIMAL256 -> {
                currentValue256Ll = Unsafe.getUnsafe().getLong(address);
                currentValue256Lh = Unsafe.getUnsafe().getLong(address + 8);
                currentValue256Hl = Unsafe.getUnsafe().getLong(address + 16);
                currentValue256Hh = Unsafe.getUnsafe().getLong(address + 24);
            }
        }
    }
}
