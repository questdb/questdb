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

/**
 * Flattens an any dimensional array (up to 32 dimensions) and stores it in the request buffer.
 * <p>
 * Checks for regularity in the array's shape during processing.
 */
public class NDArrayFlattener {

    // Corresponding to index of `process_array_datas` and `process_array_shapes` function arrays in nd_array.cpp.
    public static final int ARRAY_INDEX_BOOLEAN = 0;
    public static final int ARRAY_INDEX_BYTE = 1;
    public static final int ARRAY_INDEX_INT = 2;
    public static final int ARRAY_INDEX_LONG = 3;
    public static final int ARRAY_INDEX_FLOAT = 4;
    public static final int ARRAY_INDEX_DOUBLE = 5;

    public static long processArray(long addr,
                                    CheckCapacity checkCapacity,
                                    Object array,
                                    int dims,
                                    int elementSize,
                                    int elemTypeIndex) {
        check(checkCapacity, (long) dims * Integer.BYTES);
        int flatLength = processArrayShape(addr, array, dims);
        if (flatLength == 0) {
            throw new LineSenderException("zero length array not supported");
        }
        check(checkCapacity, (long) flatLength * elementSize);
        addr += (long) dims * Integer.BYTES;
        long ptr = processArrayData(addr, array, dims, elemTypeIndex);
        if (ptr == 0) {
            throw new LineSenderException("array is not regular");
        }
        return ptr;
    }

    private native static int processArrayShape(long addr, Object array, int dims);

    private native static long processArrayData(long addr, Object array, int dims, int elemTypeIndex);

    private static void check(CheckCapacity func, long addr) {
        if (func != null) {
            func.checkCapacity(addr);
        }
    }
}
