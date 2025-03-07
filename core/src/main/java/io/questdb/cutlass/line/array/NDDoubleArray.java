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

public class NDDoubleArray extends AbstractNDArray {

    private static final double DEFAULT_VALUE = 0.0;

    private NDDoubleArray(int[] shape, boolean initDefault, double defaultValue) {
        super(shape, ColumnType.DOUBLE);
        if (initDefault) {
            // fill default value
            long ptr = array.ptr();
            for (int i = 0, size = array.getFlatViewLength(); i < size; i++) {
                Unsafe.getUnsafe().putDouble(ptr, defaultValue);
                ptr += Double.BYTES;
            }
        }
    }

    /**
     * Create an ndADoubleArray based on the provided shape and fill it with the {@link #DEFAULT_VALUE}`.
     */
    public static NDDoubleArray create(int... shape) {
        return new NDDoubleArray(shape, true, DEFAULT_VALUE);
    }

    /*
     * Create an ndADoubleArray based on the provided shape and defaultValue.
     */
    public static NDDoubleArray create(double defaultValue, int... shape) {
        return new NDDoubleArray(shape, true, defaultValue);
    }

    /**
     * Create an 1 dim ndADoubleArray based on the provided `double[]`.
     */
    public static NDDoubleArray create(double[] values) {
        NDDoubleArray ndArray = createWithoutDefault(values.length);
        NDArrayFlattener.processArrayData(ndArray.array.ptr(), null, values);
        return ndArray;
    }

    /**
     * Create an 2 dims ndADoubleArray based on the provided `double[][]`.
     */
    public static NDDoubleArray create(double[][] values) {
        NDDoubleArray ndArray = createWithoutDefault(values.length, values[0].length);
        NDArrayFlattener.processArrayData(ndArray.array.ptr(), null, values);
        return ndArray;
    }

    /**
     * Create an 3 dims ndADoubleArray based on the provided `double[][][]`.
     */
    public static NDDoubleArray create(double[][][] values) {
        NDDoubleArray ndArray = createWithoutDefault(values.length, values[0].length, values[0][0].length);
        NDArrayFlattener.processArrayData(ndArray.array.ptr(), null, values);
        return ndArray;
    }

    /**
     * Create an ndADoubleArray based on the provided shape and do not fill default value.
     */
    public static NDDoubleArray createWithoutDefault(int... shape) {
        return new NDDoubleArray(shape, false, DEFAULT_VALUE);
    }

    /**
     * Fill a sub-array at the specified coordinates within the current array.
     *
     * @param value  subarray, must match array's shapes in coordinates.
     * @param move   whether released the memory of the subarray after set.
     * @param coords current array's position.
     */
    public NDDoubleArray set(NDDoubleArray value, boolean move, int... coords) {
        assert !closed && !value.closed;
        validSubarrayShape(value, coords);
        int flawLength = flawLengthOf(coords, false);
        Vect.memcpy(array.ptr() + (long) flawLength * Double.BYTES, value.array.ptr(), value.array.size());
        if (move) {
            value.close();
        }
        return this;
    }

    /**
     * Fill a value at the specified coordinates within the current array.
     */
    public NDDoubleArray set(double value, int... shape) {
        assert !closed;
        array.putDoubleQuick(flawLengthOf(shape, true), value);
        return this;
    }
}
