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
import io.questdb.cutlass.line.LineSenderException;

/**
 * Flattens an any dimensional array (up to 32 dimensions) and stores it in the request buffer.
 * <p>
 * Checks for regularity in the array's shape during processing.
 */
public class NDArrayFlattener {

    private static final short[] ELEM_TYPE_INDEX = new short[ColumnType.NULL + 1];

    public static long processArray(long addr,
                                    CheckCapacity checkCapacity,
                                    Object array,
                                    int dims,
                                    short elemType) {
        check(checkCapacity, (long) dims * Integer.BYTES);
        int flatLength = processArrayShape(addr, array, dims);
        if (flatLength == 0) {
            throw new LineSenderException("zero length array not supported");
        }

        final int elementSize = ColumnType.sizeOf(elemType);
        check(checkCapacity, (long) flatLength * elementSize);
        addr += (long) dims * Integer.BYTES;
        long ptr = processArrayData(addr, array, dims, ELEM_TYPE_INDEX[elemType]);
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

    static {
        // Corresponding to index of `process_array_datas` and `process_array_shapes` function arrays in nd_array.cpp.
        ELEM_TYPE_INDEX[ColumnType.BOOLEAN] = 0;
        ELEM_TYPE_INDEX[ColumnType.BYTE] = 1;
        ELEM_TYPE_INDEX[ColumnType.SHORT] = 2;
        ELEM_TYPE_INDEX[ColumnType.INT] = 3;
        ELEM_TYPE_INDEX[ColumnType.LONG] = 4;
        ELEM_TYPE_INDEX[ColumnType.FLOAT] = 5;
        ELEM_TYPE_INDEX[ColumnType.DOUBLE] = 6;
    }
}
