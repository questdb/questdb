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
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8s;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_VARCHAR;

/**
 * Streaming cursor for VARCHAR columns.
 * <p>
 * Wire format:
 * <pre>
 * [if null bitmap is present]: ceil(rowCount/8) bytes
 * [offset array]: (valueCount+1) * 4 bytes, uint32 little-endian
 * [string data]: concatenated UTF-8 bytes
 * </pre>
 * <p>
 * Returns {@link DirectUtf8Sequence} flyweights pointing directly to wire memory.
 * <b>Zero-allocation</b> on the hot path - no String objects created during iteration.
 */
public final class QwpStringColumnCursor implements QwpColumnCursor {

    private final DirectUtf8String valueUtf8 = new DirectUtf8String(); // Flyweight for current value
    private boolean currentIsNull;
    // Iteration state
    private int currentRow;
    private int currentValueIndex;
    // Wire pointers
    private long nullBitmapAddress;
    private long offsetArrayAddress;
    private long stringDataAddress;
    private int stringDataLength;
    // Configuration
    private byte typeCode;

    @Override
    public boolean advanceRow() throws QwpParseException {
        currentRow++;

        if (nullBitmapAddress != 0) {
            currentIsNull = QwpNullBitmap.isNull(nullBitmapAddress, currentRow);
            if (currentIsNull) {
                valueUtf8.clear();
                return true;
            }
        } else {
            currentIsNull = false;
        }

        // Read string bounds from offset array
        int startOffset = Unsafe.getInt(offsetArrayAddress + (long) currentValueIndex * 4);
        int endOffset = Unsafe.getInt(offsetArrayAddress + (long) (currentValueIndex + 1) * 4);

        if (startOffset < 0 || endOffset < startOffset || endOffset > stringDataLength) {
            throw QwpParseException.instance(QwpParseException.ErrorCode.INVALID_OFFSET_ARRAY)
                    .put("invalid QWP string offset array: offset[")
                    .put(currentValueIndex + 1)
                    .put("]=")
                    .put(endOffset)
                    .put(" exceeds string data size ")
                    .put(stringDataLength);
        }

        // Update flyweight to point to this string's bytes - NO ALLOCATION!
        int stringLen = endOffset - startOffset;
        long stringAddr = stringDataAddress + startOffset;
        boolean ascii = Utf8s.isAscii(stringAddr, stringLen);
        valueUtf8.of(stringAddr, stringDataAddress + endOffset, ascii);
        currentValueIndex++;
        return false;
    }

    @Override
    public void clear() {
        valueUtf8.clear();
        typeCode = TYPE_VARCHAR;
        nullBitmapAddress = 0;
        offsetArrayAddress = 0;
        stringDataAddress = 0;
        stringDataLength = 0;
        resetRowPosition();
    }

    @Override
    public byte getTypeCode() {
        return typeCode;
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
     * @param typeCode    column type code (TYPE_VARCHAR)
     * @return bytes consumed from dataAddress
     * @throws QwpParseException if data is truncated
     */
    public int of(long dataAddress, int dataLength, int rowCount, byte typeCode) throws QwpParseException {
        this.typeCode = typeCode;

        int offset = 0;

        // Read null bitmap flag
        if (offset >= dataLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "string column data truncated: expected null bitmap flag"
            );
        }
        int nullCount;
        if (Unsafe.getByte(dataAddress + offset) != 0) {
            offset++;
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            if (offset + bitmapSize > dataLength) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                        "string column data truncated: expected null bitmap"
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
        long offsetArraySize = ((long) valueCount + 1) * 4;
        if (offset + offsetArraySize > dataLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "string column data truncated: expected offset array"
            );
        }
        this.offsetArrayAddress = dataAddress + offset;
        offset += (int) offsetArraySize;

        int firstOffset = Unsafe.getInt(offsetArrayAddress);
        if (firstOffset != 0) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INVALID_OFFSET_ARRAY,
                    "invalid QWP string offset array: offset[0]=" + firstOffset + ", expected 0"
            );
        }

        // Calculate total string data size from offset array
        int lastOffset = Unsafe.getInt(offsetArrayAddress + (long) valueCount * 4);
        long availableStringData = (long) dataLength - offset;
        if (lastOffset < 0 || (long) lastOffset > availableStringData) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "string column data truncated: expected string data"
            );
        }
        this.stringDataAddress = dataAddress + offset;
        this.stringDataLength = lastOffset;
        offset += lastOffset;

        resetRowPosition();
        return offset;
    }

    @Override
    public void resetRowPosition() {
        currentRow = -1;
        currentValueIndex = 0;
        currentIsNull = false;
        valueUtf8.clear();
    }
}
