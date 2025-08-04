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

package io.questdb.cutlass.line.array;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.BorrowedFlatArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.client.Sender;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.std.QuietCloseable;

/**
 * `AbstractArray` provides an interface for Java client users to create multi-dimensional arrays,
 * supporting up to 32 dimensions.
 * <p>It manages a contiguous block of memory to store the actual array data.
 * To prevent memory leaks, please ensure to invoke the {@link #close()} method after usage.
 * <p>Example of usage:
 * <pre>{@code
 *    // Creates a 2x3 matrix (2D array)
 *    DoubleArray matrix = new DoubleArray(2, 3);
 *
 *    // Fill with data using append (row-major order)
 *    matrix.append(1.0).append(2.0).append(3.0)  // first row
 *          .append(4.0).append(5.0).append(6.0); // second row
 *
 *    // Or set specific coordinates
 *    matrix.set(1.5, 0, 1);  // set element at row 0, column 1
 *
 *    // Send to QuestDB
 *    sender.table("my_table").doubleArray("matrix_column", matrix).atNow();
 *
 *    // Remember to close when done
 *    matrix.close();
 * }</pre>
 */
public abstract class AbstractArray implements QuietCloseable {

    protected final DirectArray array = new DirectArray();
    protected boolean closed = false;
    protected int flatLength;
    protected MemoryA memA = array.startMemoryA();

    protected AbstractArray(int[] shape, short columnType) {
        array.setType(ColumnType.encodeArrayType(columnType, shape.length));
        for (int dim = 0, size = shape.length; dim < size; dim++) {
            array.setDimLen(dim, shape[dim]);
        }
        array.applyShape();
        flatLength = array.getFlatViewLength();
    }

    /**
     * Appends this array to the ILP request buffer, in the proper protocol format.
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
     * This method only resets the internal append position counter. The actual array data
     * remains unchanged. Subsequent {@code append()} calls will start overwriting from
     * the first element.
     * <p>
     * <strong>Use cases:</strong>
     * <ul>
     * <li>Error recovery after {@link Sender#cancelRow()} when array is partially filled</li>
     * <li>Starting fresh without waiting for auto-wrapping behavior</li>
     * </ul>
     * <p>
     * <strong>Note:</strong> To change the array dimensions, use {@code reshape()} instead.
     * This method only resets the position within the current shape.
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
     * <strong>Important:</strong> After calling this method, the array becomes unusable.
     * Any subsequent operations (append, set, reshape, etc.) will result in undefined
     * behavior or exceptions.
     * <p>
     * This method is idempotent - calling it multiple times has no additional effect.
     * <p>
     * <strong>Memory Management:</strong> Since arrays use native memory, failing to call
     * this method will result in memory leaks. Use try-with-resources or ensure explicit
     * cleanup in finally blocks.
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
     * Reshapes the array to the specified dimensions.
     * <p>This method allows changing the array's shape after creation, enabling
     * dynamic resizing based on runtime requirements.
     * <br>
     * Note: It resets the append position to the start of the array.
     *
     * @param shape the new dimensions for the array
     * @throws IllegalStateException    if the array is already closed
     * @throws IllegalArgumentException if shape has invalid dimensions
     * @see #reshape(int)
     * @see #reshape(int, int)
     * @see #reshape(int, int, int)
     */
    public void reshape(int... shape) {
        if (closed) {
            throw new IllegalStateException("Cannot reshape a closed array");
        }
        int nDim = shape.length;
        if (nDim > ColumnType.ARRAY_NDIMS_LIMIT) {
            throw new IllegalArgumentException("Maximum supported dimensionality is " +
                    ColumnType.ARRAY_NDIMS_LIMIT + "D, but got " + nDim + "D");
        }
        if (nDim == 0) {
            throw new IllegalArgumentException("Shape must have at least one dimension");
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
     * Reshapes the array to a single dimension with the specified length.
     * <p>This is a convenience method for converting multi-dimensional arrays to
     * one-dimensional arrays or resizing existing one-dimensional arrays without allocating an array for the shape.
     *
     * @param dimLen the length of the single dimension
     * @throws IllegalStateException    if the array is already closed
     * @throws IllegalArgumentException if dimLen is negative
     */
    public void reshape(int dimLen) {
        if (closed) {
            throw new IllegalStateException("Cannot reshape a closed array");
        }
        if (dimLen < 0) {
            throw new IllegalArgumentException("Array size must not be negative, but got " + dimLen);
        }
        array.setType(ColumnType.encodeArrayType(array.getElemType(), 1));
        array.setDimLen(0, dimLen);
        array.applyShape();
        flatLength = array.getFlatViewLength();
        memA = array.startMemoryA();
    }

    /**
     * Reshapes the array to a two-dimensional array with the specified dimensions.
     * <p>This is a convenience method for creating or reshaping to 2D arrays (matrices)
     * without allocating an array for the shape.
     *
     * @param dim1 the length of the first dimension (rows)
     * @param dim2 the length of the second dimension (columns)
     * @throws IllegalStateException    if the array is already closed
     * @throws IllegalArgumentException if any dimension is negative
     */
    public void reshape(int dim1, int dim2) {
        if (closed) {
            throw new IllegalStateException("Cannot reshape a closed array");
        }
        if (dim1 < 0 || dim2 < 0) {
            throw new IllegalArgumentException("Array dimensions must not be negative, but got [" + dim1 + ", " + dim2 + "]");
        }
        array.setType(ColumnType.encodeArrayType(array.getElemType(), 2));
        array.setDimLen(0, dim1);
        array.setDimLen(1, dim2);
        array.applyShape();
        flatLength = array.getFlatViewLength();
        memA = array.startMemoryA();
    }

    /**
     * Reshapes the array to a three-dimensional array with the specified dimensions.
     * <p>This is a convenience method for creating or reshaping to 3D arrays
     * without allocating an array for the shape.
     *
     * @param dim1 the length of the first dimension
     * @param dim2 the length of the second dimension
     * @param dim3 the length of the third dimension
     * @throws IllegalStateException    if the array is already closed
     * @throws IllegalArgumentException if any dimension is negative
     */
    public void reshape(int dim1, int dim2, int dim3) {
        if (closed) {
            throw new IllegalStateException("Cannot reshape a closed array");
        }
        if (dim1 < 0 || dim2 < 0 || dim3 < 0) {
            throw new IllegalArgumentException("Array dimensions must not be negative, but got [" + dim1 + ", " + dim2 + ", " + dim3 + "]");
        }
        array.setType(ColumnType.encodeArrayType(array.getElemType(), 3));
        array.setDimLen(0, dim1);
        array.setDimLen(1, dim2);
        array.setDimLen(2, dim3);
        array.applyShape();
        flatLength = array.getFlatViewLength();
        memA = array.startMemoryA();
    }

    protected void ensureLegalAppendPosition() {
        long elementSize = ColumnType.sizeOf(array.getElemType());
        if (memA.getAppendOffset() == flatLength * elementSize) {
            memA = array.startMemoryA();
        }
    }

    /*
     * Computes the flat array offset from the given coordinates.
     * NOTE: the passed coordinates must be valid for the array's shape.
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
