/*******************************************************************************
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

package io.questdb.cutlass.line.array;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedFlatArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.client.Sender;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.std.QuietCloseable;

/**
 * Use this class to prepare data for an N-dimensional array column in QuestDB.
 * It manages a contiguous block of native memory to store the array data.
 * To avoid leaking this memory, make sure you use it in a try-with-resources block,
 * or call {@link #close()} explicitly.
 * <p>
 * Example of usage:
 * <pre>
 *    // Create a 2x3 array:
 *    try (DoubleArray matrix = new DoubleArray(2, 3)) {
 *
 *        // Append data in row-major order:
 *        matrix.append(1.0).append(2.0).append(3.0)  // first row
 *              .append(4.0).append(5.0).append(6.0); // second row
 *
 *        // Or, set a value at specific coordinates:
 *        matrix.set(1.5, 0, 1);  // set element at row 0, column 1
 *
 *        // Send to QuestDB
 *        sender.table("my_table").doubleArray("matrix_column", matrix).atNow();
 *    }
 * }</pre>
 */
public abstract class AbstractArray implements QuietCloseable {

    /**
     * The underlying direct array storage.
     */
    protected final DirectArray array = new DirectArray();
    /**
     * Flag indicating if this array has been closed.
     */
    protected boolean closed = false;
    /**
     * The total number of elements in the flattened array.
     */
    protected int flatLength;
    /**
     * Memory appender for writing array elements.
     */
    protected MemoryA memA = array.startMemoryA();

    /**
     * Constructs a new array with the given shape and element type.
     *
     * @param shape      the dimensions of the array
     * @param columnType the column type of array elements
     */
    protected AbstractArray(int[] shape, short columnType) {
        if (shape.length == 0) {
            throw new LineSenderException("Shape must have at least one dimension");
        }
        if (shape.length > ColumnType.ARRAY_NDIMS_LIMIT) {
            throw new LineSenderException("Maximum supported dimensionality is " +
                    ColumnType.ARRAY_NDIMS_LIMIT + "D, but got " + shape.length + "D");
        }
        for (int dim = 0; dim < shape.length; dim++) {
            if (shape[dim] < 0) {
                throw new LineSenderException("dimension length must not be negative [dim=" + dim +
                        ", dimLen=" + shape[dim] + "]");
            }
            if (shape[dim] > ArrayView.DIM_MAX_LEN) {
                throw new LineSenderException("dimension length out of range [dim=" + dim +
                        ", dimLen=" + shape[dim] + ", maxLen=" + ArrayView.DIM_MAX_LEN + "]");
            }
        }

        array.setType(ColumnType.encodeArrayType(columnType, shape.length));
        for (int dim = 0, size = shape.length; dim < size; dim++) {
            array.setDimLen(dim, shape[dim]);
        }
        array.applyShape();
        flatLength = array.getFlatViewLength();
    }

    /**
     * Appends this array to the ILP request buffer, in the proper protocol format.
     *
     * @param mem the buffer appender to write to
     */
    public void appendToBufPtr(ArrayBufferAppender mem) {
        assert !closed;
        byte nDims = (byte) array.getDimCount();
        mem.putByte(nDims);
        for (byte i = 0; i < nDims; i++) {
            mem.putInt(array.getDimLen(i));
        }
        if (!array.isNull() && !array.isEmpty()) {
            BorrowedFlatArrayView view = array.borrowedFlatView();
            mem.putBlockOfBytes(view.ptr(), view.size());
        }
    }

    /**
     * Resets the append position to the beginning of the array without modifying any data.
     * <p>
     * This method only resets the append position marker, and the array data remains unchanged.
     * Subsequent {@code append()} calls will start overwriting from the first element.
     * <p>
     * <strong>Use cases:</strong>
     * <ul>
     * <li>Reset the array after getting an error and/or calling {@link Sender#cancelRow()}</li>
     * <li>Start fresh without relying on auto-wrapping behavior</li>
     * </ul>
     * <strong>Note:</strong> to change the array dimensions, use {@code reshape()}.
     * This method only resets the position while maintaining the array shape.
     *
     * @see #reshape(int...)
     */
    public void clear() {
        assert !closed;
        memA = array.startMemoryA();
    }

    /**
     * Closes this array and releases all associated native memory resources.
     * <p>
     * <strong>Important:</strong> after calling this method, the array becomes unusable.
     * Any subsequent operations (append, set, reshape, etc.) will result in undefined
     * behavior or exceptions.
     * <p>
     * This method is idempotent &mdash; calling it multiple times has no additional effect.
     * <p>
     * <strong>Memory Management:</strong> since the class uses native memory, failing to call
     * this method will result in a native memory leak. Use it inside try-with-resources, or
     * call explicitly in a finally block.
     *
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    public void close() {
        if (!closed) {
            array.close();
        }
        closed = true;
    }

    /**
     * Reshapes the array to the specified dimensions, and resets the append
     * position to the start of the array.
     *
     * @param shape the new dimensions for the array
     * @throws LineSenderException if the array is already closed or shape has invalid dimensions
     * @see #reshape(int)
     * @see #reshape(int, int)
     * @see #reshape(int, int, int)
     */
    public void reshape(int... shape) {
        if (closed) {
            throw new LineSenderException("Cannot reshape a closed array");
        }
        int nDim = shape.length;
        if (nDim > ColumnType.ARRAY_NDIMS_LIMIT) {
            throw new LineSenderException("Maximum supported dimensionality is " +
                    ColumnType.ARRAY_NDIMS_LIMIT + "D, but got " + nDim + "D");
        }
        if (nDim == 0) {
            throw new LineSenderException("Shape must have at least one dimension");
        }
        for (int dim = 0; dim < nDim; dim++) {
            if (shape[dim] < 0) {
                throw new LineSenderException("dimension length must not be negative [dim=" + dim +
                        ", dimLen=" + shape[dim] + "]");
            }
            if (shape[dim] > ArrayView.DIM_MAX_LEN) {
                throw new LineSenderException("dimension length out of range [dim=" + dim +
                        ", dimLen=" + shape[dim] + ", maxLen=" + ArrayView.DIM_MAX_LEN + "]");
            }
        }
        array.setType(ColumnType.encodeArrayType(array.getElemType(), nDim));
        for (int dim = 0; dim < nDim; dim++) {
            array.setDimLen(dim, shape[dim]);
        }
        array.applyShape();
        flatLength = array.getFlatViewLength();
        memA = array.startMemoryA();
    }

    /**
     * Reshapes the array to a single dimension with the specified length, and resets
     * the append position to the start of the array.
     *
     * @param dimLen the length of the single dimension
     * @throws LineSenderException if the array is already closed or dimLen is negative
     */
    public void reshape(int dimLen) {
        if (closed) {
            throw new LineSenderException("Cannot reshape a closed array");
        }
        if (dimLen < 0) {
            throw new LineSenderException("Array size must not be negative, but got " + dimLen);
        }
        if (dimLen > ArrayView.DIM_MAX_LEN) {
            throw new LineSenderException("Array size out of range [dimLen=" + dimLen +
                    ", maxLen=" + ArrayView.DIM_MAX_LEN + "]");
        }
        array.setType(ColumnType.encodeArrayType(array.getElemType(), 1));
        array.setDimLen(0, dimLen);
        array.applyShape();
        flatLength = array.getFlatViewLength();
        memA = array.startMemoryA();
    }

    /**
     * Reshapes the array to two dimensions with the specified lengths, and resets
     * the append position to the start of the array.
     *
     * @param dim1 the length of the first dimension (rows)
     * @param dim2 the length of the second dimension (columns)
     * @throws LineSenderException if the array is already closed or any dimension is negative
     */
    public void reshape(int dim1, int dim2) {
        if (closed) {
            throw new LineSenderException("Cannot reshape a closed array");
        }
        if (dim1 < 0 || dim2 < 0) {
            throw new LineSenderException("Array dimensions must not be negative, but got [" + dim1 + ", " + dim2 + "]");
        }
        if (dim1 > ArrayView.DIM_MAX_LEN || dim2 > ArrayView.DIM_MAX_LEN) {
            throw new LineSenderException("Array dimensions out of range [dim1=" + dim1 +
                    ", dim2=" + dim2 + ", maxLen=" + ArrayView.DIM_MAX_LEN + "]");
        }
        array.setType(ColumnType.encodeArrayType(array.getElemType(), 2));
        array.setDimLen(0, dim1);
        array.setDimLen(1, dim2);
        array.applyShape();
        flatLength = array.getFlatViewLength();
        memA = array.startMemoryA();
    }

    /**
     * Reshapes the array to three dimensions with the specified lengths, and resets
     * the append position to the start of the array.
     *
     * @param dim1 the length of the first dimension
     * @param dim2 the length of the second dimension
     * @param dim3 the length of the third dimension
     * @throws LineSenderException if the array is already closed or any dimension is negative
     */
    public void reshape(int dim1, int dim2, int dim3) {
        if (closed) {
            throw new LineSenderException("Cannot reshape a closed array");
        }
        if (dim1 < 0 || dim2 < 0 || dim3 < 0) {
            throw new LineSenderException("Array dimensions must not be negative, but got [" +
                    dim1 + ", " + dim2 + ", " + dim3 + "]");
        }
        if (dim1 > ArrayView.DIM_MAX_LEN || dim2 > ArrayView.DIM_MAX_LEN || dim3 > ArrayView.DIM_MAX_LEN) {
            throw new LineSenderException("Array dimensions out of range [dim1=" + dim1 +
                    ", dim2=" + dim2 + ", dim3=" + dim3 + ", maxLen=" + ArrayView.DIM_MAX_LEN + "]");
        }
        array.setType(ColumnType.encodeArrayType(array.getElemType(), 3));
        array.setDimLen(0, dim1);
        array.setDimLen(1, dim2);
        array.setDimLen(2, dim3);
        array.applyShape();
        flatLength = array.getFlatViewLength();
        memA = array.startMemoryA();
    }

    /**
     * Ensures the append position is at a valid location, resetting if needed.
     */
    protected void ensureLegalAppendPosition() {
        long elementSize = ColumnType.sizeOf(array.getElemType());
        if (memA.getAppendOffset() == flatLength * elementSize) {
            memA = array.startMemoryA();
        }
    }

    /**
     * Computes the flat array offset from the given coordinates.
     * NOTE: the passed coordinates must be valid for the array's shape.
     *
     * @param coords the multi-dimensional coordinates
     * @return the flat array offset
     */
    protected int toFlatOffset(int[] coords) {
        if (coords == null || coords.length == 0) {
            return 0;
        }
        if (coords.length != array.getDimCount()) {
            throw new LineSenderException("coordinates and array shape do not match");
        }
        int flatOffset = 0;
        for (int dim = 0, n = coords.length; dim < n; dim++) {
            if (array.getDimLen(dim) <= coords[dim]) {
                throw new LineSenderException("coordinates and array shape do not match");
            }
            flatOffset += array.getStride(dim) * coords[dim];
        }
        return flatOffset;
    }
}
