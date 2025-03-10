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
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

public class DoubleArray extends AbstractArray {

    private DoubleArray(int... shape) {
        super(shape, ColumnType.DOUBLE);
    }

    /**
     * Creates a DoubleArray based on the provided shape.
     */
    public static DoubleArray create(int... shape) {
        return new DoubleArray(shape);
    }

    /**
     * Create an 1 dim ndADoubleArray based on the provided `double[]`.
     */
    public static DoubleArray create(double[] values) {
        DoubleArray ndArray = new DoubleArray(values.length);
        FlattenArrayUtils.putDataToBuf(ndArray.array.ptr(), null, values);
        return ndArray;
    }

    /**
     * Create an 2 dims ndADoubleArray based on the provided `double[][]`.
     */
    public static DoubleArray create(double[][] values) {
        DoubleArray ndArray = new DoubleArray(values.length, values[0].length);
        FlattenArrayUtils.putDataToBuf(ndArray.array.ptr(), null, values);
        return ndArray;
    }

    /**
     * Create an 3 dims ndADoubleArray based on the provided `double[][][]`.
     */
    public static DoubleArray create(double[][][] values) {
        DoubleArray ndArray = new DoubleArray(values.length, values[0].length, values[0][0].length);
        FlattenArrayUtils.putDataToBuf(ndArray.array.ptr(), null, values);
        return ndArray;
    }

    /**
     * Sets all data points in the array to the supplied value.
     */
    public DoubleArray setAll(double value) {
        long ptr = array.ptr();
        for (int i = 0, size = array.getFlatViewLength(); i < size; i++) {
            Unsafe.getUnsafe().putDouble(ptr, value);
            ptr += Double.BYTES;
        }
        return this;
    }

    /**
     * Fill a sub-array at the specified coordinates within the current array.
     *
     * @param value  subarray, must match array's shapes in coordinates.
     * @param move   whether to release the memory of the subarray after using it.
     * @param coords the coordinates where to insert the sub-array
     */
    public DoubleArray setSubArray(DoubleArray value, boolean move, int... coords) {
        assert !closed && !value.closed;
        validateSubarrayShape(value, coords);
        int flawLength = toFlatOffset(coords, false);
        Vect.memcpy(array.ptr() + (long) flawLength * Double.BYTES, value.array.ptr(), value.array.size());
        if (move) {
            value.close();
        }
        return this;
    }

    /**
     * Fill a value at the specified coordinates within the current array.
     */
    public DoubleArray setValue(double value, int... coords) {
        assert !closed;
        array.putDouble(toFlatOffset(coords, true), value);
        return this;
    }
}
