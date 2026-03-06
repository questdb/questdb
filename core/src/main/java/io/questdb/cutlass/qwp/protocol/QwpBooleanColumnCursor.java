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

import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_BOOLEAN;

/**
 * Streaming cursor for boolean columns.
 * <p>
 * Wire format:
 * <pre>
 * [null bitmap if nullable]: ceil(rowCount/8) bytes
 * [value bitmap]: ceil(valueCount/8) bytes, bit[i]=1 means true
 * </pre>
 * <p>
 * Values are bit-packed, with one bit per non-null value.
 */
public final class QwpBooleanColumnCursor implements QwpColumnCursor {

    private boolean currentIsNull;
    // Iteration state
    private int currentRow;
    private boolean currentValue;
    private int currentValueIndex;  // Index into value bitmap (non-null values only)
    // Wire pointers
    private long nullBitmapAddress;
    // Configuration
    private boolean nullable;
    private long valueBitmapAddress;

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

        // Read value bit from value bitmap
        int byteIndex = currentValueIndex >>> 3;
        int bitIndex = currentValueIndex & 7;
        byte b = Unsafe.getUnsafe().getByte(valueBitmapAddress + byteIndex);
        currentValue = (b & (1 << bitIndex)) != 0;
        currentValueIndex++;
        return false;
    }

    @Override
    public void clear() {
        nullable = false;
        nullBitmapAddress = 0;
        valueBitmapAddress = 0;
        resetRowPosition();
    }

    @Override
    public int getCurrentRow() {
        return currentRow;
    }

    @Override
    public byte getTypeCode() {
        return TYPE_BOOLEAN;
    }

    /**
     * Returns current row's boolean value.
     */
    public boolean getValue() {
        return currentValue;
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
     * @param nullable    whether column is nullable
     * @return bytes consumed from dataAddress
     */
    public int of(
            long dataAddress,
            int rowCount,
            boolean nullable
    ) {
        this.nullable = nullable;

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

        this.valueBitmapAddress = dataAddress + offset;
        int valueCount = rowCount - nullCount;
        int valueBitmapSize = (valueCount + 7) / 8;
        offset += valueBitmapSize;

        resetRowPosition();
        return offset;
    }

    @Override
    public void resetRowPosition() {
        currentRow = -1;
        currentValueIndex = 0;
        currentIsNull = false;
        currentValue = false;
    }
}
