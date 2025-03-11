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

import io.questdb.cutlass.line.LineSenderException;
import io.questdb.std.Unsafe;

/**
 * Utility class with methods that flatten an N-dimensional Java array into a
 * native-memory buffer.
 * <p>
 * Methods ensure that the array has a regular shape (i.e., is not jagged).
 */
public class FlattenArrayUtils {

    public static long putDataToBuf(long bufPtr, CapacityChecker checkFn, double[] array) {
        int length = array.length;
        CapacityChecker.check(checkFn, bufPtr, (long) length * Double.BYTES);
        for (int i = 0; i < length; i++) {
            double v = array[i];
            Unsafe.getUnsafe().putDouble(bufPtr, v);
            bufPtr += Double.BYTES;
        }
        return bufPtr;
    }

    public static long putDataToBuf(long bufPtr, CapacityChecker checkFn, double[][] array) {
        final int dim0Len = array.length;
        final int dim1Len = array[0].length;
        for (int i = 0; i < dim0Len; i++) {
            double[] v = array[i];
            if (v.length != dim1Len) {
                throw new LineSenderException("irregular array shape");
            }
            bufPtr = putDataToBuf(bufPtr, checkFn, v);
        }
        return bufPtr;
    }

    public static long putDataToBuf(long bufPtr, CapacityChecker checkFn, double[][][] array) {
        final int dim0Len = array.length;
        final int dim1Len = array[0].length;
        for (int i = 0; i < dim0Len; i++) {
            double[][] v = array[i];
            if (v.length != dim1Len) {
                throw new LineSenderException("array is not regular");
            }
            bufPtr = putDataToBuf(bufPtr, checkFn, v);
        }
        return bufPtr;
    }

    public static long putDataToBuf(long bufPtr, CapacityChecker checkFn, long[] array) {
        CapacityChecker.check(checkFn, bufPtr, (long) array.length * Double.BYTES);
        for (long v : array) {
            Unsafe.getUnsafe().putLong(bufPtr, v);
            bufPtr += Long.BYTES;
        }
        return bufPtr;
    }

    public static long putDataToBuf(long bufPtr, CapacityChecker checkFn, long[][] array) {
        int length = array[0].length;
        for (long[] v : array) {
            if (length != v.length) {
                throw new LineSenderException("array is not regular");
            }
            bufPtr = putDataToBuf(bufPtr, checkFn, v);
        }
        return bufPtr;
    }

    public static long putDataToBuf(long bufPtr, CapacityChecker checkFn, long[][][] array) {
        int length = array[0].length;
        for (long[][] v : array) {
            if (length != v.length) {
                throw new LineSenderException("array is not regular");
            }
            bufPtr = putDataToBuf(bufPtr, checkFn, v);
        }
        return bufPtr;
    }

    public static void putShapeToBuf(long bufPtr, double[] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(bufPtr, array.length);
    }

    public static void putShapeToBuf(long bufPtr, double[][] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(bufPtr, array.length);
        bufPtr += Integer.BYTES;
        putShapeToBuf(bufPtr, array[0]);
    }

    public static void putShapeToBuf(long bufPtr, double[][][] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(bufPtr, array.length);
        bufPtr += Integer.BYTES;
        putShapeToBuf(bufPtr, array[0]);
    }

    public static void putShapeToBuf(long bufPtr, long[] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(bufPtr, array.length);
    }

    public static void putShapeToBuf(long bufPtr, long[][] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(bufPtr, array.length);
        bufPtr += Integer.BYTES;
        putShapeToBuf(bufPtr, array[0]);
    }

    public static void putShapeToBuf(long bufPtr, long[][][] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(bufPtr, array.length);
        bufPtr += Integer.BYTES;
        putShapeToBuf(bufPtr, array[0]);
    }
}
