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

import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_DOUBLE_ARRAY;

/**
 * Streaming cursor for DOUBLE_ARRAY and LONG_ARRAY columns.
 * <p>
 * Wire format per row (for non-null values):
 * <pre>
 * [nDims: 1 byte] [dim1..dimN: 4 bytes each, int32 LE] [values: 8 bytes each]
 * </pre>
 * <p>
 * For columns with a null bitmap, the column data starts with that bitmap.
 * <p>
 * <b>Zero-allocation</b> on the hot path after initialization.
 */
public final class QwpArrayColumnCursor implements QwpColumnCursor {

    private static final int INITIAL_ROW_CAPACITY = 64;
    private static final int MAX_DIMS = 32;
    private final int[] currentShape = new int[MAX_DIMS];
    private int currentElementCount;
    private boolean currentIsNull;
    private int currentNDims;
    private int currentRow;
    private long currentValuesAddress;
    private long dataAddress;
    private boolean isDoubleArray;
    private long nullBitmapAddress;
    private int[] rowDims = new int[INITIAL_ROW_CAPACITY];
    private int[] rowElementCounts = new int[INITIAL_ROW_CAPACITY];
    // Pre-computed row offsets for fast random access
    private long[] rowOffsets = new long[INITIAL_ROW_CAPACITY];
    private byte typeCode;

    @Override
    public boolean advanceRow() {
        currentRow++;

        if (nullBitmapAddress != 0 && QwpNullBitmap.isNull(nullBitmapAddress, currentRow)) {
            currentIsNull = true;
            currentNDims = 0;
            currentElementCount = 0;
            currentValuesAddress = 0;
            return true;
        }

        currentIsNull = false;
        currentNDims = rowDims[currentRow];
        currentElementCount = rowElementCounts[currentRow];

        // Position to current row's data
        long rowAddr = dataAddress + rowOffsets[currentRow];
        rowAddr += 1; // skip nDims byte

        // Read shape
        for (int d = 0; d < currentNDims; d++) {
            currentShape[d] = Unsafe.getUnsafe().getInt(rowAddr);
            rowAddr += 4;
        }

        currentValuesAddress = rowAddr;
        return false;
    }

    @Override
    public void clear() {
        typeCode = TYPE_DOUBLE_ARRAY;
        isDoubleArray = true;
        nullBitmapAddress = 0;
        dataAddress = 0;
        resetRowPosition();
    }

    /**
     * Returns the size of a specific dimension for the current row's array.
     *
     * @param dim dimension index (0-based)
     * @return dimension size
     */
    public int getDimSize(int dim) {
        return currentShape[dim];
    }

    /**
     * Returns the number of dimensions for the current row's array.
     *
     * @return number of dimensions (1-32), or 0 if null
     */
    public int getNDims() {
        return currentNDims;
    }

    /**
     * Returns the total number of elements in the current row's array.
     *
     * @return total element count
     */
    public int getTotalElements() {
        return currentElementCount;
    }

    @Override
    public byte getTypeCode() {
        return typeCode;
    }

    /**
     * Returns the address of the values data for direct memory access.
     *
     * @return memory address of values, or 0 if null
     */
    public long getValuesAddress() {
        return currentValuesAddress;
    }

    /**
     * Returns whether this is a double array (vs long array).
     *
     * @return true for double array, false for long array
     */
    public boolean isDoubleArray() {
        return isDoubleArray;
    }

    @Override
    public boolean isNull() {
        return currentIsNull;
    }

    /**
     * Initializes this cursor for the given column data.
     *
     * @param dataAddress address of column data
     * @param dataLength  available bytes
     * @param rowCount    number of rows
     * @param typeCode    column type code (TYPE_DOUBLE_ARRAY or TYPE_LONG_ARRAY)
     * @return bytes consumed from dataAddress
     */
    public int of(
            long dataAddress,
            int dataLength,
            int rowCount,
            byte typeCode
    ) throws QwpParseException {
        this.typeCode = typeCode;
        this.isDoubleArray = (typeCode == TYPE_DOUBLE_ARRAY);

        ensureRowCapacity(rowCount);
        int offset = 0;

        // Read null bitmap flag
        if (offset >= dataLength) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                    "array column data truncated: expected null bitmap flag"
            );
        }
        if (Unsafe.getUnsafe().getByte(dataAddress + offset) != 0) {
            offset++;
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            if (offset + (long) bitmapSize > dataLength) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                        "array column data truncated: expected null bitmap"
                );
            }
            this.nullBitmapAddress = dataAddress + offset;
            offset += bitmapSize;
        } else {
            offset++;
            this.nullBitmapAddress = 0;
        }

        this.dataAddress = dataAddress + offset;
        final long dataEnd = dataAddress + dataLength;

        // Pre-scan all non-null rows to build offset table
        long scanAddr = this.dataAddress;
        for (int row = 0; row < rowCount; row++) {
            if (nullBitmapAddress != 0 && QwpNullBitmap.isNull(nullBitmapAddress, row)) {
                rowOffsets[row] = -1; // Mark as null
                rowDims[row] = 0;
                rowElementCounts[row] = 0;
            } else {
                rowOffsets[row] = scanAddr - this.dataAddress;

                // Bounds check before reading nDims
                if (scanAddr >= dataEnd) {
                    throw QwpParseException.create(
                            QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                            "array data truncated: expected nDims byte"
                    );
                }

                // Read nDims and validate bounds
                int nDims = Unsafe.getUnsafe().getByte(scanAddr) & 0xFF;
                if (nDims == 0 || nDims > MAX_DIMS) {
                    throw QwpParseException.create(
                            QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                            "invalid array dimensions: " + nDims + " (must be 1-" + MAX_DIMS + ")"
                    );
                }
                rowDims[row] = nDims;
                scanAddr += 1;

                // Bounds check before reading shape
                long shapeBytes = (long) nDims * 4;
                if (scanAddr + shapeBytes > dataEnd) {
                    throw QwpParseException.create(
                            QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                            "array data truncated: expected " + nDims + " dimension sizes"
                    );
                }

                // Read shape and calculate element count (with overflow check)
                int elementCount = 1;
                for (int d = 0; d < nDims; d++) {
                    int dimSize = Unsafe.getUnsafe().getInt(scanAddr);
                    scanAddr += 4;
                    if (dimSize < 0) {
                        throw QwpParseException.create(
                                QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                                "invalid array dimension size: " + dimSize
                        );
                    }
                    try {
                        elementCount = Math.multiplyExact(elementCount, dimSize);
                    } catch (ArithmeticException e) {
                        throw QwpParseException.create(
                                QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                                "array element count overflow"
                        );
                    }
                }
                rowElementCounts[row] = elementCount;

                // Bounds check before skipping values
                long valueBytes = (long) elementCount * 8;
                if (scanAddr + valueBytes > dataEnd) {
                    throw QwpParseException.create(
                            QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                            "array data truncated: expected " + elementCount + " values"
                    );
                }

                // Skip values (8 bytes each for both double and long)
                scanAddr += valueBytes;
            }
        }

        offset += (int) (scanAddr - this.dataAddress);
        resetRowPosition();
        return offset;
    }

    @Override
    public void resetRowPosition() {
        currentRow = -1;
        currentIsNull = false;
        currentNDims = 0;
        currentElementCount = 0;
        currentValuesAddress = 0;
    }

    private void ensureRowCapacity(int required) {
        if (rowOffsets.length < required) {
            int newCapacity = Math.max(required, rowOffsets.length * 2);
            rowOffsets = new long[newCapacity];
            rowDims = new int[newCapacity];
            rowElementCounts = new int[newCapacity];
        }
    }
}
