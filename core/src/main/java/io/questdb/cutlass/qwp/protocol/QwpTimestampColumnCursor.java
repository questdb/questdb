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

    public static final byte ENCODING_GORILLA = 0x01;
    public static final byte ENCODING_UNCOMPRESSED = 0x00;

    private final QwpGorillaDecoder gorillaDecoder = new QwpGorillaDecoder();
    private boolean currentIsNull;
    // Iteration state
    private int currentRow;
    private long currentTimestamp;
    private int currentValueIndex;
    // Gorilla state
    private long firstTimestamp;
    private static final int INITIAL_GORILLA_CAPACITY = 1024;
    private long[] gorillaDecodedValues = new long[INITIAL_GORILLA_CAPACITY];  // Cached decoded values (index 2+)
    private boolean gorillaEnabled;
    // Wire pointers
    private long nullBitmapAddress;
    private long secondTimestamp;
    // Configuration
    private byte typeCode;
    private int valueCount;
    private long valuesAddress;  // For uncompressed mode

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

        // Read timestamp value
        if (gorillaEnabled) {
            if (currentValueIndex == 0) {
                currentTimestamp = firstTimestamp;
            } else if (currentValueIndex == 1) {
                currentTimestamp = secondTimestamp;
            } else {
                currentTimestamp = gorillaDecodedValues[currentValueIndex - 2];
            }
        } else {
            currentTimestamp = Unsafe.getUnsafe().getLong(valuesAddress + (long) currentValueIndex * 8);
        }
        currentValueIndex++;
        return false;
    }

    @Override
    public void clear() {
        typeCode = 0;
        gorillaEnabled = false;
        nullBitmapAddress = 0;
        valuesAddress = 0;
        firstTimestamp = 0;
        secondTimestamp = 0;
        valueCount = 0;
        resetRowPosition();
    }

    /**
     * Returns the total number of Gorilla decode operations performed
     * since the last {@link #of} call.
     *
     * @return decode count, or 0 if Gorilla encoding is not used
     */
    public int getGorillaDecodeCount() {
        return gorillaDecoder.getDecodeCount();
    }

    /**
     * Returns the address of the null bitmap, or 0 if no null bitmap.
     *
     * @return the memory address of null bitmap, or 0 if no null bitmap
     */
    public long getNullBitmapAddress() {
        return nullBitmapAddress;
    }

    /**
     * Returns current row's timestamp value (microseconds or nanoseconds since epoch).
     */
    public long getTimestamp() {
        return currentTimestamp;
    }

    @Override
    public byte getTypeCode() {
        return typeCode;
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
     * Returns the address of the packed non-null values array.
     * <p>
     * Only valid when {@link #supportsDirectAccess()} returns true.
     *
     * @return the memory address of values array, or 0 if Gorilla-encoded
     */
    public long getValuesAddress() {
        return gorillaEnabled ? 0 : valuesAddress;
    }

    @Override
    public boolean isNull() {
        return currentIsNull;
    }

    /**
     * Initializes this cursor for the given column data.
     *
     * @param dataAddress    address of column data
     * @param dataLength     available bytes
     * @param rowCount       number of rows
     * @param typeCode       column type code (TYPE_TIMESTAMP or TYPE_TIMESTAMP_NANOS)
     * @param gorillaEnabled whether Gorilla encoding is enabled
     * @return bytes consumed from dataAddress
     * @throws QwpParseException if parsing fails
     */
    public int of(
            long dataAddress,
            int dataLength,
            int rowCount,
            byte typeCode,
            boolean gorillaEnabled
    ) throws QwpParseException {
        this.typeCode = typeCode;

        int offset = 0;

        // Read null bitmap flag
        if (offset >= dataLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "timestamp column data truncated: expected null bitmap flag"
            );
        }
        int nullCount;
        if (Unsafe.getUnsafe().getByte(dataAddress + offset) != 0) {
            offset++;
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            if (offset + bitmapSize > dataLength) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                        "timestamp column data truncated: expected null bitmap"
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

        if (!gorillaEnabled) {
            // Uncompressed mode
            offset = getOffset(dataAddress, dataLength, offset);
        } else {
            // Read encoding flag
            if (offset + 1 > dataLength) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                        "timestamp column data truncated: expected encoding flag"
                );
            }
            byte encoding = Unsafe.getUnsafe().getByte(dataAddress + offset);
            offset++;

            if (encoding == ENCODING_UNCOMPRESSED) {
                // Uncompressed fallback within Gorilla-enabled stream
                offset = getOffset(dataAddress, dataLength, offset);
            } else if (encoding == ENCODING_GORILLA) {
                this.gorillaEnabled = true;

                if (valueCount == 1) {
                    // First timestamp only
                    if (offset + 8 > dataLength) {
                        throw QwpParseException.create(
                                QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                                "timestamp column data truncated: expected first timestamp"
                        );
                    }
                    this.firstTimestamp = Unsafe.getUnsafe().getLong(dataAddress + offset);
                    offset += 8;
                } else if (valueCount >= 2) {
                    // First and second timestamps
                    if (offset + 16 > dataLength) {
                        throw QwpParseException.create(
                                QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                                "timestamp column data truncated: expected first two timestamps"
                        );
                    }
                    this.firstTimestamp = Unsafe.getUnsafe().getLong(dataAddress + offset);
                    offset += 8;
                    this.secondTimestamp = Unsafe.getUnsafe().getLong(dataAddress + offset);
                    offset += 8;

                    if (valueCount > 2) {
                        // Full Gorilla encoding
                        long gorillaDataAddress = dataAddress + offset;
                        int remainingBytes = dataLength - offset;

                        // Each Gorilla-encoded value uses at most 36 bits
                        // (4-bit prefix '1111' + 32-bit signed delta-of-delta).
                        // Clamp the bit reader boundary to this theoretical
                        // maximum so that corrupted Gorilla data cannot read
                        // arbitrarily far into subsequent columns' data.
                        int remainingValues = valueCount - 2;
                        int maxGorillaBytes = (int) ((remainingValues * 36L + 7) / 8);
                        int gorillaDataLength = Math.min(remainingBytes, maxGorillaBytes);

                        gorillaDecoder.reset(firstTimestamp, secondTimestamp, gorillaDataAddress, gorillaDataLength);

                        // Decode all remaining values and cache them to avoid double decoding.
                        // This also computes the byte count of the compressed data.
                        if (gorillaDecodedValues.length < remainingValues) {
                            gorillaDecodedValues = new long[remainingValues];
                        }
                        for (int i = 0; i < remainingValues; i++) {
                            gorillaDecodedValues[i] = gorillaDecoder.decodeNext();
                        }
                        long bitsConsumed = gorillaDecoder.getBitPosition();
                        int gorillaBytesConsumed = (int) ((bitsConsumed + 7) / 8);
                        offset += gorillaBytesConsumed;
                    }
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
    public void resetRowPosition() {
        currentRow = -1;
        currentValueIndex = 0;
        currentIsNull = false;
        currentTimestamp = 0;
    }

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

    private int getOffset(long dataAddress, int dataLength, int offset) throws QwpParseException {
        this.gorillaEnabled = false;
        long valuesSize = (long) valueCount * 8;
        if (offset + valuesSize > dataLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "timestamp column data truncated: expected " + valueCount + " values"
            );
        }
        this.valuesAddress = dataAddress + offset;
        offset += (int) valuesSize;
        return offset;
    }
}
