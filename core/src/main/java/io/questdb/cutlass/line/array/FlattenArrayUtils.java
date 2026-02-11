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

import io.questdb.cutlass.line.LineSenderException;

/**
 * Utility class with methods that flatten an N-dimensional Java array into a
 * native-memory buffer.
 * <p>
 * Methods ensure that the array has a regular shape (i.e., is not jagged).
 */
public class FlattenArrayUtils {

    public static void putDataToBuf(ArrayBufferAppender mem, double[] array) {
        for (int n = array.length, i = 0; i < n; i++) {
            mem.putDouble(array[i]);
        }
    }

    public static void putDataToBuf(ArrayBufferAppender mem, double[][] array) {
        if (array.length == 0) {
            return;
        }
        final int dim1Len = array[0].length;
        for (int n = array.length, i = 0; i < n; i++) {
            double[] subArr = array[i];
            if (subArr.length != dim1Len) {
                throw new LineSenderException("irregular array shape");
            }
            putDataToBuf(mem, subArr);
        }
    }

    public static void putDataToBuf(ArrayBufferAppender mem, double[][][] array) {
        if (array.length == 0) {
            return;
        }
        final int dim1Len = array[0].length;
        for (int n = array.length, i = 0; i < n; i++) {
            double[][] v = array[i];
            if (v.length != dim1Len) {
                throw new LineSenderException("irregular array shape");
            }
            putDataToBuf(mem, v);
        }
    }

    public static void putDataToBuf(ArrayBufferAppender mem, long[] array) {
        for (int n = array.length, i = 0; i < n; i++) {
            long v = array[i];
            mem.putLong(v);
        }
    }

    public static void putDataToBuf(ArrayBufferAppender mem, long[][] array) {
        if (array.length == 0) {
            return;
        }
        final int dim1Len = array[0].length;
        for (int n = array.length, i = 0; i < n; i++) {
            long[] v = array[i];
            if (v.length != dim1Len) {
                throw new LineSenderException("irregular array shape");
            }
            putDataToBuf(mem, v);
        }
    }

    public static void putDataToBuf(ArrayBufferAppender mem, long[][][] array) {
        if (array.length == 0) {
            return;
        }
        final int dim1Len = array[0].length;
        for (int n = array.length, i = 0; i < n; i++) {
            long[][] v = array[i];
            if (v.length != dim1Len) {
                throw new LineSenderException("irregular array shape");
            }
            putDataToBuf(mem, v);
        }
    }

    public static void putShapeToBuf(ArrayBufferAppender mem, double[] array) {
        mem.putInt(array.length);
    }

    public static void putShapeToBuf(ArrayBufferAppender mem, double[][] array) {
        mem.putInt(array.length);
        if (array.length == 0) {
            mem.putInt(0);
        } else {
            putShapeToBuf(mem, array[0]);
        }
    }

    public static void putShapeToBuf(ArrayBufferAppender mem, double[][][] array) {
        mem.putInt(array.length);
        if (array.length == 0) {
            mem.putInt(0);
            mem.putInt(0);
        } else {
            putShapeToBuf(mem, array[0]);
        }
    }

    public static void putShapeToBuf(ArrayBufferAppender mem, long[] array) {
        mem.putInt(array.length);
    }

    public static void putShapeToBuf(ArrayBufferAppender mem, long[][] array) {
        mem.putInt(array.length);
        if (array.length == 0) {
            mem.putInt(0);
        } else {
            putShapeToBuf(mem, array[0]);
        }
    }

    public static void putShapeToBuf(ArrayBufferAppender mem, long[][][] array) {
        mem.putInt(array.length);
        if (array.length == 0) {
            mem.putInt(0);
            mem.putInt(0);
        } else {
            putShapeToBuf(mem, array[0]);
        }
    }
}
