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
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

/**
 * `AbstractArray` provides an interface for Java client users to create multi-dimensional arrays,
 * supporting up to 32 dimensions.
 * <p>It manages a contiguous block of memory to store the actual array data.
 * To prevent memory leaks, please ensure to invoke the {@link #close()}  method after usage.
 * <p>Additionally, the memory will also be released after calling {@link #appendToBufPtr(long, CapacityChecker, boolean)}.
 * <p>Example of usage:
 * <pre>{@code
 *    // Creates a 2x3x2 matrix (of rank 3)
 *    try (
 *        DoubleArray matrix3d = DoubleArray.create(2, 3, 2)) {
 *              matrix3d.set(DoubleArray.create(new double[]{1.0, 2.0}), true, 0, 0)
 *                  .set(DoubleArray.create(new double[]{3.0, 4.0}), true, 0, 1)
 *                  .set(DoubleArray.create(new double[]{5.0, 6.0}), true, 0, 2)
 *                  .set(DoubleArray.create(new double[]{7.0, 8.0}), true, 1, 0)
 *                  .set(DoubleArray.create(new double[]{9.0, 10.0}), true, 1, 1)
 *                  .set(DoubleArray.create(new double[]{11.0, 12.0}), true, 1, 2);
 *
 *                  // send matrix3d to line
 *                  sender.table(tableName).doubleArray(columnName, matrix3d);
 *        }
 *
 * }</pre>
 */
public abstract class AbstractArray implements QuietCloseable {

    protected final DirectArray array = new DirectArray();
    protected final int flatLength;
    protected boolean closed = false;
    protected MemoryA memA = array.startMemoryA();

    protected AbstractArray(int[] shape, short columnType) {
        array.setType(ColumnType.encodeArrayType(columnType, shape.length));
        for (int dim = 0, size = shape.length; dim < size; dim++) {
            array.setDimLen(dim, shape[dim]);
        }
        array.applyShape(-1);
        flatLength = array.getFlatViewLength();
    }

    /*
     * append current array to request buffer.
     */
    public long appendToBufPtr(long bufPtr, CapacityChecker checkCapacityFn, boolean move) {
        assert !closed;
        byte nDims = (byte) array.getDimCount();
        long size = array.size();
        CapacityChecker.check(checkCapacityFn, Byte.BYTES + nDims * Integer.BYTES + size);
        Unsafe.getUnsafe().putByte(bufPtr, nDims);
        bufPtr += Byte.BYTES;
        for (byte i = 0; i < nDims; i++) {
            Unsafe.getUnsafe().putInt(bufPtr, array.getDimLen(i));
            bufPtr += Integer.BYTES;
        }

        Vect.memcpy(bufPtr, array.ptr(), size);
        if (move) {
            close();
        }
        return bufPtr + size;
    }

    @Override
    public void close() {
        if (!closed) {
            array.close();
        }
        closed = true;
    }

    protected void ensureLegalAppendPosition() {
        if (memA.getAppendOffset() == flatLength) {
            memA = array.startMemoryA();
        }
    }

    /*
     * Computes the flat array offset from the given coordinates.
     * NOTE: the passed coordinates must be valid for the array's shape.
     */
    protected int toFlatOffset(int[] coords, boolean isValue) {
        if (coords == null || coords.length == 0) {
            return 0;
        }
        if (isValue) {
            if (coords.length != array.getDimCount()) {
                throw new LineSenderException("coordinates and array shape do not match");
            }
        } else if (coords.length >= array.getDimCount()) {
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

    protected void validateSubarrayShape(AbstractArray value, int[] shape) {
        if (value.array.getDimCount() + shape.length != array.getDimCount()) {
            throw new LineSenderException("subArray do not match current array's shape");
        }

        for (int i = 0, size = value.array.getDimCount(); i < size; i++) {
            if (value.array.getDimLen(i) != array.getDimLen(i + shape.length)) {
                throw new LineSenderException("subArray do not match current array's shape");
            }
        }
    }
}
