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
import io.questdb.std.Unsafe;

/**
 * Used to accumulate the data of an N-dimensional array of {@code long} values.
 * You must close the array when done with it because it uses native memory.
 */
public class LongArray extends AbstractArray {

    public LongArray(int... shape) {
        super(shape, ColumnType.LONG);
    }

    /**
     * Appends the value at the current append positions, and then advances it.
     * If the append position is currently beyond the last element, it first resets
     * the position to zero and then appends the new value.
     * <p>
     * The intention is for this array object to be reused for all the rows you are
     * inserting, and this auto-wrapping behavior allows you to repeatedly fill the
     * array without the need for other lifecycle calls like {@code clear()}.
     */
    public LongArray append(long value) {
        ensureLegalAppendPosition();
        memA.putLong(value);
        return this;
    }

    /**
     * Sets a value at the supplied coordinates.
     */
    public LongArray set(long value, int... coords) {
        assert !closed;
        array.putLong(toFlatOffset(coords), value);
        return this;
    }

    /**
     * Sets all data points in the array to the supplied value.
     */
    public LongArray setAll(long value) {
        long ptr = array.ptr();
        for (int i = 0, size = array.getFlatViewLength(); i < size; i++) {
            Unsafe.getUnsafe().putLong(ptr, value);
            ptr += Long.BYTES;
        }
        return this;
    }

}
