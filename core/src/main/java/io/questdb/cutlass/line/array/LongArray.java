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

import io.questdb.cairo.ColumnType;
import io.questdb.std.Unsafe;

public class LongArray extends AbstractArray {

    public LongArray(int... shape) {
        super(shape, ColumnType.LONG);
    }

    /**
     * Appends the value at the current append position, and then advances it.
     * The append position advances in row-major order across the entire array.
     * If it is currently at the last element, it will automatically wrap around to the first
     * element after this call.
     * <p>
     * <strong>Auto-wrapping behavior:</strong> this array is designed to be reused across
     * multiple rows. As soon as you are done filling it up, append position wraps back to
     * the first element. After you have sent it to QuestDB, just start appending more data
     * for the next row.
     * <p>
     * <strong>Error recovery:</strong> If you need to abandon a partially-filled array
     * (e.g., after {@code sender.cancelRow()}), use {@code clear()} to reset the append
     * position.
     *
     * @param value the double value to append
     * @return this array instance for method chaining
     */
    public LongArray append(long value) {
        ensureLegalAppendPosition();
        memA.putLong(value);
        return this;
    }

    /**
     * Sets a value at the specified coordinates without affecting the append position.
     * <p>
     * This method allows direct access to an array element by its coordinates. Unlike {@code
     * append()}, this does not modify the current append position.
     *
     * @param value  the double value to set
     * @param coords the coordinates specifying the position (must match array dimensionality)
     * @return this array instance for method chaining
     * @throws io.questdb.cutlass.line.LineSenderException if coordinates don't match the array shape
     */
    public LongArray set(long value, int... coords) {
        assert !closed;
        array.putLong(toFlatOffset(coords), value);
        return this;
    }

    /**
     * Sets all data points in the array to the supplied value, without changing
     * the append position.
     * <p>
     * <strong>Append position behavior:</strong> this method does NOT change the current
     * append position. If you were in the middle of appending data, subsequent {@code
     * append()} calls will continue from where they left off, potentially overwriting the
     * values set by this method.
     * <p>
     * <strong>Use cases:</strong>
     * <ul>
     * <li>Initialize the array with default values before using {@code set()} for selective
     * updates</li>
     * <li>Set the whole array to a uniform value</li>
     * </ul>
     *
     * @param value the double value to set for all array elements
     * @return this array instance for method chaining
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
