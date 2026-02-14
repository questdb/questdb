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

import static io.questdb.cutlass.qwp.protocol.QwpTimestampDecoder.ENCODING_GORILLA;
import static io.questdb.cutlass.qwp.protocol.QwpTimestampDecoder.ENCODING_UNCOMPRESSED;

/**
 * Streaming cursor for TIMESTAMP and TIMESTAMP_NANOS columns.
 * <p>
 * Supports two encoding modes:
 * <ul>
 *   <li>Uncompressed (0x00): array of int64 values</li>
 *   <li>Gorilla (0x01): delta-of-delta compressed</li>
 * </ul>
 * <p>
 * The cursor maintains Gorilla decoder state internally for streaming decoding.
 */
public final class QwpTimestampColumnCursor implements QwpColumnCursor {

    private final DirectUtf8String nameUtf8 = new DirectUtf8String();
    private final QwpGorillaDecoder gorillaDecoder = new QwpGorillaDecoder();

    // Configuration
    private byte typeCode;
    private boolean nullable;
    private int rowCount;
    private boolean gorillaEnabled;

    // Wire pointers
    private long nullBitmapAddress;
    private long valuesAddress;  // For uncompressed mode
    private long gorillaDataAddress;  // For Gorilla mode
    private int gorillaDataLength;

    // Gorilla state
    private long firstTimestamp;
    private long secondTimestamp;
    private int valueCount;

    // Iteration state
    private int currentRow;
    private int currentValueIndex;
    private boolean currentIsNull;
    private long currentTimestamp;

    /**
     * Initializes this cursor for the given column data.
     *
     * @param dataAddress   address of column data
     * @param dataLength    available bytes
     * @param rowCount      number of rows
     * @param typeCode      column type code (TYPE_TIMESTAMP or TYPE_TIMESTAMP_NANOS)
     * @param nullable      whether column is nullable
     * @param nameAddress   address of column name UTF-8 bytes
     * @param nameLength    column name length in bytes
     * @param gorillaEnabled whether Gorilla encoding is enabled
     * @return bytes consumed from dataAddress
     * @throws QwpParseException if parsing fails
     */
    public int of(long dataAddress, int dataLength, int rowCount, byte typeCode, boolean nullable,
                  long nameAddress, int nameLength, boolean gorillaEnabled) throws QwpParseException {
        this.typeCode = typeCode;
        this.nullable = nullable;
        this.rowCount = rowCount;
        this.nameUtf8.of(nameAddress, nameAddress + nameLength);

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

        this.valueCount = rowCount - nullCount;

        if (!gorillaEnabled) {
            // Uncompressed mode
            this.gorillaEnabled = false;
            this.valuesAddress = dataAddress + offset;
            offset += valueCount * 8;
        } else {
            // Read encoding flag
            byte encoding = Unsafe.getUnsafe().getByte(dataAddress + offset);
            offset++;

            if (encoding == ENCODING_UNCOMPRESSED) {
                // Uncompressed fallback within Gorilla-enabled stream
                this.gorillaEnabled = false;
                this.valuesAddress = dataAddress + offset;
                offset += valueCount * 8;
            } else if (encoding == ENCODING_GORILLA) {
                this.gorillaEnabled = true;

                if (valueCount == 0) {
                    // All nulls, nothing more to read
                } else if (valueCount == 1) {
                    // First timestamp only
                    this.firstTimestamp = Unsafe.getUnsafe().getLong(dataAddress + offset);
                    offset += 8;
                } else if (valueCount == 2) {
                    // First and second timestamps
                    this.firstTimestamp = Unsafe.getUnsafe().getLong(dataAddress + offset);
                    offset += 8;
                    this.secondTimestamp = Unsafe.getUnsafe().getLong(dataAddress + offset);
                    offset += 8;
                } else {
                    // Full Gorilla encoding
                    this.firstTimestamp = Unsafe.getUnsafe().getLong(dataAddress + offset);
                    offset += 8;
                    this.secondTimestamp = Unsafe.getUnsafe().getLong(dataAddress + offset);
                    offset += 8;

                    this.gorillaDataAddress = dataAddress + offset;
                    this.gorillaDataLength = dataLength - offset;

                    gorillaDecoder.reset(firstTimestamp, secondTimestamp);
                    gorillaDecoder.resetReader(gorillaDataAddress, gorillaDataLength);

                    // Calculate bytes consumed by decoding all remaining values
                    // This is necessary to know where the next column's data starts
                    int remainingValues = valueCount - 2;
                    for (int i = 0; i < remainingValues; i++) {
                        gorillaDecoder.decodeNext();
                    }
                    // Calculate bytes consumed from bit position (round up to full bytes)
                    long bitsConsumed = gorillaDecoder.getBitPosition();
                    int gorillaBytesConsumed = (int) ((bitsConsumed + 7) / 8);
                    offset += gorillaBytesConsumed;

                    // Reset decoder state for iteration (caller will decode values again)
                    gorillaDecoder.reset(firstTimestamp, secondTimestamp);
                    gorillaDecoder.resetReader(gorillaDataAddress, gorillaDataLength);
                }
            } else {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INVALID_COLUMN_TYPE,
                        "unknown timestamp encoding: " + encoding
                );
            }
        }

        resetRowPosition();

        // Return consumed bytes (caller may need to calculate from outside)
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
                return true;
            }
        } else {
            currentIsNull = false;
        }

        // Read timestamp value
        if (gorillaEnabled) {
            if (currentValueIndex == 0) {
                currentTimestamp = firstTimestamp;
            } else if (currentValueIndex == 1) {
                currentTimestamp = secondTimestamp;
            } else {
                currentTimestamp = gorillaDecoder.decodeNext();
            }
        } else {
            currentTimestamp = Unsafe.getUnsafe().getLong(valuesAddress + (long) currentValueIndex * 8);
        }
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
        currentTimestamp = 0;

        // Reset Gorilla decoder state if enabled
        if (gorillaEnabled && valueCount > 2) {
            gorillaDecoder.reset(firstTimestamp, secondTimestamp);
            gorillaDecoder.resetReader(gorillaDataAddress, gorillaDataLength);
        }
    }

    @Override
    public void clear() {
        nameUtf8.clear();
        typeCode = 0;
        nullable = false;
        rowCount = 0;
        gorillaEnabled = false;
        nullBitmapAddress = 0;
        valuesAddress = 0;
        gorillaDataAddress = 0;
        gorillaDataLength = 0;
        firstTimestamp = 0;
        secondTimestamp = 0;
        valueCount = 0;
        resetRowPosition();
    }

    /**
     * Returns current row's timestamp value (microseconds or nanoseconds since epoch).
     */
    public long getTimestamp() {
        return currentTimestamp;
    }

    // ==================== Columnar Access Methods ====================

    /**
     * Returns whether direct memory access is possible for this column.
     * <p>
     * Direct access is only possible when Gorilla encoding is disabled.
     * When enabled, values must be accessed row-by-row through iteration.
     *
     * @return true if direct memory access is possible
     */
    public boolean supportsDirectAccess() {
        return !gorillaEnabled;
    }

    /**
     * Returns the address of the packed non-null values array.
     * <p>
     * Only valid when {@link #supportsDirectAccess()} returns true.
     *
     * @return the memory address of values array, or 0 if Gorilla-encoded
     */
    public long getValuesAddress() {
        return gorillaEnabled ? 0 : valuesAddress;
    }

    /**
     * Returns the address of the null bitmap, or 0 if not nullable.
     *
     * @return the memory address of null bitmap, or 0 if not nullable
     */
    public long getNullBitmapAddress() {
        return nullBitmapAddress;
    }

    /**
     * Returns the number of non-null values in this column.
     *
     * @return count of non-null values
     */
    public int getValueCount() {
        return valueCount;
    }

    /**
     * Returns the total row count (including nulls).
     *
     * @return total row count
     */
    public int getRowCount() {
        return rowCount;
    }
}
