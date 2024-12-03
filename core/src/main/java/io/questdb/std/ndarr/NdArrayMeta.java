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

package io.questdb.std.ndarr;

import io.questdb.std.DirectIntList;
import io.questdb.std.DirectIntSlice;
import org.jetbrains.annotations.NotNull;

/**
 * Algorithms to work with the shape (dimensions) and strides.
 * <p>Note that, unlike in most array implementations,
 * the strides are defined in element space, thus are the same regardless of type.</p>
 */
public class NdArrayMeta {

    /**
     * Maximum size of any given dimension.
     * <p>Why:
     * <ul>
     *   <li>Our buffers are at most Integer.MAX_VALUE long</li>
     *   <li>Our largest datatypes are 8 bytes</li>
     * </ul></p>
     * Assuming a 1-D array, <code>Integer.MAX_VALUE / Long.BYTES</code> gives us a maximum
     * of 2 ** 28 - 1.
     * For simplicity, we thus round this down to 2 ** 27.
     */
    public static final int MAX_DIM_SIZE = 1 << 27;

    private NdArrayMeta() {
    }

    /**
     * The product of all the shape's dimensions.
     * <p>This returns the number of elements contained in the values
     * vector returned by {@link NdArrayView#getValues()}.</p>
     */
    public static int flatLength(DirectIntSlice shape) {
        int length = 1;
        for (int dimIndex = 0, nDims = shape.length(); dimIndex < nDims; ++dimIndex) {
            final int dim = shape.get(dimIndex);
            length *= dim;
        }
        return length;
    }

    /**
     * Set the list to the default strides for a row-major vector of the specified dimensions.
     * <p>The strides are expressed in element space (not byte space).</p>
     */
    public static void setDefaultStrides(@NotNull DirectIntSlice shape, @NotNull DirectIntList strides) {
        strides.clear();
        if (shape.length() == 0) {
            return;
        }
        if (strides.getCapacity() < shape.length()) {
            strides.setCapacity(shape.length());
        }
        for (int dimIndex = 0, nDims = shape.length(); dimIndex < nDims; dimIndex++) {
            strides.add(0);
        }
        int stride = 1;
        for (int dimIndex = shape.length() - 1; dimIndex >= 0; --dimIndex) {
            strides.set(dimIndex, stride);
            stride *= shape.get(dimIndex);
        }
    }

    /**
     * Determine if the strides are the default strides.
     * <p>If they are, the data can be iterated in order simply by accessing the {@link NdArrayView#getValues()} vec.</p>
     */
    public static boolean isDefaultStrides(DirectIntSlice shape, DirectIntSlice strides) {
        assert shape.length() == strides.length();
        int expected = 1;
        for (int dimIndex = shape.length() - 1; dimIndex >= 0; --dimIndex) {
            final int actual = strides.get(dimIndex);
            if (actual != expected) {
                return false;
            }
            expected *= shape.get(dimIndex);
        }
        return true;
    }

    /**
     * Swap the axes.
     * <p>We hold data as row-major. If needed we can also use transposition to
     * represent it as column major for the purposes of library compatibility.</p>
     */
    public static void transpose(DirectIntList strides) {
        strides.reverse();
    }

    /** Check that each dimension in the shape is &gt;= 0. */
    public static boolean validShape(DirectIntSlice shape) {
        for (int dimIndex = 0, nDims = shape.length(); dimIndex < nDims; ++dimIndex) {
            final int dim = shape.get(dimIndex);
            if ((dim <= 0) || (dim >= MAX_DIM_SIZE))
                // having a zero or negative dimension is never valid.
                return false;
        }
        return true;
    }

}
