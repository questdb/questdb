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
 * Flattens an any dimensional array (up to 32 dimensions) and stores it in the request buffer.
 * <p>
 * Checks for regularity in the array's shape during processing.
 */
public class NDArrayFlattener {

    public static long processArrayData(long bufPtr, CheckCapacity checkFn, double[] array) {
        CheckCapacity.check(checkFn, (long) array.length * Double.BYTES);
        for (double v : array) {
            Unsafe.getUnsafe().putDouble(bufPtr, v);
            bufPtr += Double.BYTES;
        }
        return bufPtr;
    }

    public static long processArrayData(long bufPtr, CheckCapacity checkFn, double[][] array) {
        int length = array[0].length;
        for (double[] v : array) {
            if (length != v.length) {
                throw new LineSenderException("array is not regular");
            }
            bufPtr = processArrayData(bufPtr, checkFn, v);
        }
        return bufPtr;
    }

    public static long processArrayData(long bufPtr, CheckCapacity checkFn, double[][][] array) {
        int length = array[0].length;
        for (double[][] v : array) {
            if (length != v.length) {
                throw new LineSenderException("array is not regular");
            }
            bufPtr = processArrayData(bufPtr, checkFn, v);
        }
        return bufPtr;
    }

    public static long processArrayData(long bufPtr, CheckCapacity checkFn, long[] array) {
        CheckCapacity.check(checkFn, (long) array.length * Double.BYTES);
        for (long v : array) {
            Unsafe.getUnsafe().putLong(bufPtr, v);
            bufPtr += Long.BYTES;
        }
        return bufPtr;
    }

    public static long processArrayData(long bufPtr, CheckCapacity checkFn, long[][] array) {
        int length = array[0].length;
        for (long[] v : array) {
            if (length != v.length) {
                throw new LineSenderException("array is not regular");
            }
            bufPtr = processArrayData(bufPtr, checkFn, v);
        }
        return bufPtr;
    }

    public static long processArrayData(long bufPtr, CheckCapacity checkFn, long[][][] array) {
        int length = array[0].length;
        for (long[][] v : array) {
            if (length != v.length) {
                throw new LineSenderException("array is not regular");
            }
            bufPtr = processArrayData(bufPtr, checkFn, v);
        }
        return bufPtr;
    }

    public static void processArrayShape(long bufPtr, double[] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(bufPtr, array.length);
    }

    public static void processArrayShape(long bufPtr, double[][] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(bufPtr, array.length);
        bufPtr += Integer.BYTES;
        processArrayShape(bufPtr, array[0]);
    }

    public static void processArrayShape(long bufPtr, double[][][] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(bufPtr, array.length);
        bufPtr += Integer.BYTES;
        processArrayShape(bufPtr, array[0]);
    }

    public static void processArrayShape(long bufPtr, long[] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(bufPtr, array.length);
    }

    public static void processArrayShape(long bufPtr, long[][] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(bufPtr, array.length);
        bufPtr += Integer.BYTES;
        processArrayShape(bufPtr, array[0]);
    }

    public static void processArrayShape(long bufPtr, long[][][] array) {
        if (array.length == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        Unsafe.getUnsafe().putInt(bufPtr, array.length);
        bufPtr += Integer.BYTES;
        processArrayShape(bufPtr, array[0]);
    }
}
