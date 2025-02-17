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
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectIntSlice;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

/**
 * Algorithms to work with the shape (dimensions) and strides.
 * <p>
 * Note that, unlike in most array implementations, the strides are defined in terms
 * of elements, not bytes, so they don't depend on the element type.
 */
public class ArrayMetaUtils {

    /**
     * Maximum size of any given dimension.
     * <p>Why:
     * <ul>
     *   <li>Our buffers are at most Integer.MAX_VALUE long</li>
     *   <li>Our largest datatypes are 8 bytes</li>
     * </ul>
     * Assuming a 1-D array, <code>Integer.MAX_VALUE / Long.BYTES</code> gives us a maximum
     * of 2 ** 28 - 1, i.e. all bits 0 to (inc) 27 set.
     * <p><strong>NOTE</strong>: This value can also be used as a mask to extract the dim
     * from the lower bits of an int, packed with extra data in the remaining bits.</p>
     */
    public static final int DIM_MAX_SIZE = (1 << 28) - 1;

    private ArrayMetaUtils() {
    }

    public static void determineDefaultStrides(long shapePtr, int shapeLength, @NotNull DirectIntList stridesOut) {
        stridesOut.clear();
        if (shapeLength == 0) {
            return;
        }
        if (stridesOut.getCapacity() < shapeLength) {
            stridesOut.setCapacity(shapeLength);
        }
        for (int dimIndex = 0; dimIndex < shapeLength; dimIndex++) {
            stridesOut.add(0);
        }
        int stride = 1;
        for (int dimIndex = shapeLength - 1; dimIndex >= 0; --dimIndex) {
            stridesOut.set(dimIndex, stride);
            final long dimLenAddr = shapePtr + ((long) dimIndex * Integer.BYTES);
            final int dimLen = Unsafe.getUnsafe().getInt(dimLenAddr);
            stride *= dimLen;
        }
    }

    /**
     * Validates that each dimension in the shape is positive and doesn't exceed
     * {@value DIM_MAX_SIZE}. Validates that the flat element count fits into an {@code int}.
     * Returns the flat element count.
     */
    public static int validateShapeAndGetFlatElemCount(DirectIntSlice shape) {
        int nDims = shape.length();
        if (nDims > ColumnType.ARRAY_NDIMS_LIMIT) {
            throw new AssertionError("shape length exceeds max dimensionality: " + nDims);
        }
        int elemCount = 1;
        for (int dimIndex = 0; dimIndex < nDims; ++dimIndex) {
            final int dimLength = shape.get(dimIndex);
            if (dimLength <= 0 || dimLength >= DIM_MAX_SIZE) {
                throw new AssertionError(String.format("shape dimension %,d out of bounds: %,d", dimIndex, dimLength));
            }
            elemCount = Math.multiplyExact(elemCount, dimLength);
        }
        return elemCount;
    }
}
