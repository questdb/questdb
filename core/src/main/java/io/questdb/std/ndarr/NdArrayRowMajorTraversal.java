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
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;

/**
 * An iterator-like utility to traverse an {@link NdArrayView} in row-major order.
 * <p>Instead of obtaining the values, this traversal class computes the coordinates.</p>
 * <p>It is down to the user to then pass those coordinates to the type-appropriate
 * getter method.</p>
 * <p>Example: <pre>{@code
 *     NdArrayView array = ...;
 *     try (NdArrayRowMajorTraversal traversal = new NdArrayRowMajorTraversal()) {
 *         traversal.of(array);
 *         DirectIntSlice coords;
 *         while ((coords = traversal.next()) != null) {
 *             final int value = array.getInt(coords);
 *             ...
 *         }
 *     }
 * }</pre></p>
 */
public class NdArrayRowMajorTraversal implements QuietCloseable {
    private final DirectIntList coordinates = new DirectIntList(0, MemoryTag.NATIVE_ND_ARRAY);
    private int flatIndex = -1;
    private int valuesLength = -1;
    /**
     * The array's shape
     */
    private DirectIntSlice shape;

    @Override
    public void close() {
        Misc.free(coordinates);
    }

    /**
     * Get the next coordinates to iterate.
     */
    public DirectIntSlice next() {
        if ((flatIndex + 1) >= valuesLength) {
            return null;
        }

        ++flatIndex;

        int tempIndex = flatIndex;
        for (int dimIndex = shape.length() - 1; dimIndex >= 0; --dimIndex) {
            coordinates.set(dimIndex, tempIndex % shape.get(dimIndex));
            tempIndex /= shape.get(dimIndex);
        }

        return coordinates.asSlice();
    }

    public NdArrayRowMajorTraversal of(NdArrayView array) {
        return of(array.getShape());
    }

    public NdArrayRowMajorTraversal of(DirectIntSlice shape) {
        reset();
        this.shape = shape;
        valuesLength = NdArrayMeta.flatLength(shape);
        for (int dimIndex = shape.length() - 1; dimIndex >= 0; --dimIndex) {
            coordinates.add(0);
        }
        return this;
    }

    private void reset() {
        coordinates.clear();
        shape = null;
        flatIndex = -1;
        valuesLength = -1;
    }
}
