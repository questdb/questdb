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

import io.questdb.cairo.vm.api.MemoryA;

public interface ArrayView {

    void appendRowMajor(MemoryA mem);

    int getDim();

    int getDimLength(int dim);

    default double getDouble1D(int x) {
        return getDoubleFromRowMajor(getValuesOffset() + x);
    }

    default double getDouble2D(int x, int y) {
        return getDoubleFromRowMajor(getValuesOffset() + x * getStride(0) + y);
    }

    default double getDouble3D(int x, int y, int z) {
        return getDoubleFromRowMajor(getValuesOffset() + x * getStride(0) + y * getStride(1) + z);
    }

    /**
     * Raw access into flat array with row major layout.
     *
     * @param flatIndex flat index into the flat array
     * @return double value from the array
     */
    double getDoubleFromRowMajor(int flatIndex);

    /**
     * Raw access into flat array with row major layout.
     *
     * @param flatIndex flat index into the flat array
     * @return long value from the array
     */
    long getLongFromRowMajor(int flatIndex);

    int getSize();

    int getStride(int dimension);

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
