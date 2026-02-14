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
import io.questdb.std.str.Utf8s;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_STRING;

/**
 * Streaming cursor for STRING and VARCHAR columns.
 * <p>
 * Wire format:
 * <pre>
 * [null bitmap if nullable]: ceil(rowCount/8) bytes
 * [offset array]: (valueCount+1) * 4 bytes, uint32 little-endian
 * [string data]: concatenated UTF-8 bytes
 * </pre>
 * <p>
 * Returns {@link DirectUtf8Sequence} flyweights pointing directly to wire memory.
 * <b>Zero-allocation</b> on the hot path - no String objects created during iteration.
 */
public final class QwpStringColumnCursor implements QwpColumnCursor {

    private final DirectUtf8String nameUtf8 = new DirectUtf8String();
    private final DirectUtf8String valueUtf8 = new DirectUtf8String(); // Flyweight for current value

    // Configuration
    private byte typeCode;
    private boolean nullable;
    private int rowCount;

    // Wire pointers
    private long nullBitmapAddress;
    private long offsetArrayAddress;
    private long stringDataAddress;

    // Iteration state
    private int currentRow;
    private int currentValueIndex;
    private boolean currentIsNull;

    /**
     * Initializes this cursor for the given column data.
     *
     * @param dataAddress   address of column data
     * @param dataLength    available bytes
     * @param rowCount      number of rows
     * @param typeCode      column type code (TYPE_STRING or TYPE_VARCHAR)
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
        this.nameUtf8.of(nameAddress, nameAddress + nameLength, Utf8s.isAscii(nameAddress, nameLength));

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

        int valueCount = rowCount - nullCount;
        this.offsetArrayAddress = dataAddress + offset;
        int offsetArraySize = (valueCount + 1) * 4;
        offset += offsetArraySize;

        this.stringDataAddress = dataAddress + offset;

        // Calculate total string data size from offset array
        int lastOffset = Unsafe.getUnsafe().getInt(offsetArrayAddress + (long) valueCount * 4);
        offset += lastOffset;

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
    public boolean advanceRow() throws QwpParseException {
        currentRow++;

        if (nullable && nullBitmapAddress != 0) {
            currentIsNull = QwpNullBitmap.isNull(nullBitmapAddress, currentRow);
            if (currentIsNull) {
                valueUtf8.clear();
                return true;
            }
        } else {
            currentIsNull = false;
        }

        // Read string bounds from offset array
        int startOffset = Unsafe.getUnsafe().getInt(offsetArrayAddress + (long) currentValueIndex * 4);
        int endOffset = Unsafe.getUnsafe().getInt(offsetArrayAddress + (long) (currentValueIndex + 1) * 4);

        // Update flyweight to point to this string's bytes - NO ALLOCATION!
        int stringLen = endOffset - startOffset;
        long stringAddr = stringDataAddress + startOffset;
        boolean ascii = Utf8s.isAscii(stringAddr, stringLen);
        valueUtf8.of(stringAddr, stringDataAddress + endOffset, ascii);
        currentValueIndex++;
        return false;
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
        valueUtf8.clear();
    }

    @Override
    public void clear() {
        nameUtf8.clear();
        valueUtf8.clear();
        typeCode = TYPE_STRING;
        nullable = false;
        rowCount = 0;
        nullBitmapAddress = 0;
        offsetArrayAddress = 0;
        stringDataAddress = 0;
        resetRowPosition();
    }

    /**
     * Returns current row's value as a UTF-8 sequence.
     * <p>
     * <b>Zero-allocation:</b> Returns a flyweight pointing directly to wire memory.
     * The returned sequence is valid until the next {@link #advanceRow()} call.
     *
     * @return UTF-8 sequence, or empty sequence if NULL
     */
    public DirectUtf8Sequence getUtf8Value() {
        return valueUtf8;
    }

    /**
     * Returns current row's value as a String.
     * <p>
     * <b>Allocates:</b> Creates a new String object. Prefer {@link #getUtf8Value()}
     * for zero-allocation access when the consumer supports {@link DirectUtf8Sequence}.
     *
     * @return String value, or null if NULL
     */
    public String getStringValue() {
        if (currentIsNull) {
            return null;
        }
        return valueUtf8.toString();
    }
}
