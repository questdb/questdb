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
import static io.questdb.cutlass.line.tcp.v4.IlpV4TimestampDecoder.*;

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
public class IlpV4TimestampColumnCursor implements IlpV4ColumnCursor {

    private final DirectUtf8String nameUtf8 = new DirectUtf8String();
    private final IlpV4GorillaDecoder gorillaDecoder = new IlpV4GorillaDecoder();

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
     * @throws IlpV4ParseException if parsing fails
     */
    public int of(long dataAddress, int dataLength, int rowCount, byte typeCode, boolean nullable,
                  long nameAddress, int nameLength, boolean gorillaEnabled) throws IlpV4ParseException {
        this.typeCode = typeCode;
        this.nullable = nullable;
        this.rowCount = rowCount;
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
                throw IlpV4ParseException.create(
                        IlpV4ParseException.ErrorCode.INVALID_COLUMN_TYPE,
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
}
