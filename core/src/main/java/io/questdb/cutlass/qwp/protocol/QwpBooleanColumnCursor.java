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

import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_BOOLEAN;

/**
 * Streaming cursor for boolean columns.
 * <p>
 * Wire format:
 * <pre>
 * [if null bitmap is present]: ceil(rowCount/8) bytes
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
    private long valueBitmapAddress;

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
        nullBitmapAddress = 0;
        valueBitmapAddress = 0;
        resetRowPosition();
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
     * @param dataLength  available bytes from dataAddress
     * @param rowCount    number of rows
     * @return bytes consumed from dataAddress
     * @throws QwpParseException if data is truncated
     */
    public int of(
            long dataAddress,
            int dataLength,
            int rowCount
    ) throws QwpParseException {
        int offset = 0;

        // Read null bitmap flag
        if (offset >= dataLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "boolean column data truncated: expected null bitmap flag"
            );
        }
        int nullCount;
        if (Unsafe.getUnsafe().getByte(dataAddress + offset) != 0) {
            offset++;
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            if (offset + bitmapSize > dataLength) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                        "boolean column data truncated: expected null bitmap"
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

        int valueCount = rowCount - nullCount;
        int valueBitmapSize = (valueCount + 7) / 8;
        if (offset + valueBitmapSize > dataLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "boolean column data truncated: expected value bitmap"
            );
        }
        this.valueBitmapAddress = dataAddress + offset;
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
