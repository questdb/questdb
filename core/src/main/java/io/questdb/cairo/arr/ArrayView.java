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

package io.questdb.cairo.arr;

import io.questdb.cairo.ColumnType;

public interface ArrayView extends ArrayShape {

    /**
     * Returns a flat view over the elements of the N-dimensional array. It contains
     * the values stored in row-major order. For example, for a 4x3x2 array:
     * <pre>
     * {
     *     {{1, 2}, {3, 4}, {5, 6}},
     *     {{7, 8}, {9, 0}, {1, 2}},
     *     {{3, 4}, {5, 6}, {7, 8}},
     *     {{9, 0}, {1, 2}, {3, 4}}
     * }
     * </pre>
     * The flat array would contain a flat vector of elements with the numbers
     * <code>[1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4]</code>.
     */
    FlatArrayView flatView();

    /**
     * Returns the total number of data points (leaf values) in this array.
     */
    int getFlatElemCount();

    int getStride(int dimension);

    /**
     * Returns the encoded array type, as specified in {@link ColumnType#encodeArrayType(short, int)}.
     */
    int getType();

    default int getValuesOffset() {
        return 0;
    }

    /**
     * If the underling array has default strides, 0 values offset and is aligned, we
     * call it "vanilla" and we can persist the array without manipulating it.
     *
     * @return true for arrays lifted direct from storage or otherwise implementing "vanilla" layouts.
     */
    default boolean isVanilla() {
        return true;
    }
}
