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
 * Used to accumulate the data of an N-dimensional array of {@code double} values.
 * <p>
 * <strong>Important characteristics:</strong>
 * <ul>
 * <li><strong>Fixed Shape:</strong> Once created, the array shape cannot be changed without using {@code reshape()} methods</li>
 * <li><strong>Memory Management:</strong> Uses native memory - you must close the array when done to prevent memory leaks</li>
 * <li><strong>Reusability:</strong> Designed to be reused across multiple rows via auto-wrapping append behavior</li>
 * <li><strong>Long-lived:</strong> Typically bound to Sender lifecycle, not individual operations</li>
 * </ul>
 * <p>
 * <strong>Typical usage pattern:</strong>
 * <pre>{@code
 * // Create once, reuse many times
 * DoubleArray array = new DoubleArray(10, 5);
 *
 * for (DataRow row : manyRows) {
 *     // Fill array with row data - auto-wraps when full
 *     for (double value : row.getValues()) {
 *         array.append(value);
 *     }
 *     sender.table("my_table").doubleArray("my_column", array).atNow();
 * }
 *
 * // Close when Sender is done
 * array.close();
 * }</pre>
 */
public class DoubleArray extends AbstractArray {

    /**
     * Creates a new DoubleArray with the specified shape.
     * <p>
     * The shape defines the dimensions of the N-dimensional array. For example:
     * <ul>
     * <li>{@code new DoubleArray(10)} creates a 1D array with 10 elements</li>
     * <li>{@code new DoubleArray(3, 4)} creates a 2D array (3x4 matrix)</li>
     * <li>{@code new DoubleArray(2, 3, 4)} creates a 3D array (2x3x4)</li>
     * </ul>
     * <p>
     * The array shape is initially fixed but can be changed later using {@code reshape()} methods.
     * The append position starts at the beginning (first element).
     *
     * @param shape the dimensions of the array (must have at least one dimension)
     * @throws io.questdb.cutlass.line.LineSenderException if shape is empty or contains negative values
     * @see #reshape(int...)
     */
    public DoubleArray(int... shape) {
        super(shape, ColumnType.DOUBLE);
    }

    /**
     * Appends the value at the current append position, and then advances it.
     * The append position advances in row-major order across the entire array.
     * If the append position is currently beyond the last element, it automatically resets
     * to zero and then appends the new value.
     * <p>
     * <strong>Auto-wrapping behavior:</strong> This array is designed to be reused across multiple rows.
     * When you finish filling the array and send it to QuestDB, the next append operation will
     * automatically start from the beginning when the array boundary is reached.
     * <p>
     * <strong>Error recovery:</strong> If you need to abandon a partially-filled array state
     * (e.g., after {@code sender.cancelRow()}), use {@code clear()} to immediately reset
     * the append position, or rely on auto-wrapping when you reach the array boundary.
     *
     * @param value the double value to append
     * @return this array instance for method chaining
     */
    public DoubleArray append(double value) {
        ensureLegalAppendPosition();
        memA.putDouble(value);
        return this;
    }

    /**
     * Sets a value at the specified coordinates without affecting the append position.
     * <p>
     * This method allows direct access to array elements by their multi-dimensional coordinates.
     * Unlike {@code append()}, this does not modify the current append position.
     *
     * @param value  the double value to set
     * @param coords the coordinates specifying the position (must match array dimensionality)
     * @return this array instance for method chaining
     * @throws io.questdb.cutlass.line.LineSenderException if coordinates don't match the array shape
     */
    public DoubleArray set(double value, int... coords) {
        assert !closed;
        array.putDouble(toFlatOffset(coords), value);
        return this;
    }

    /**
     * Sets all data points in the array to the supplied value without affecting the append position.
     * <p>
     * <strong>Append position behavior:</strong> This method does NOT change the current append position.
     * If you were in the middle of appending data, subsequent {@code append()} calls will continue
     * from where they left off, potentially overwriting the values set by this method.
     * <p>
     * <strong>Use cases:</strong>
     * <ul>
     * <li>Initialize array with default values before using {@code set()} for selective updates</li>
     * <li>Fill array with a uniform value when append position is already at the end</li>
     * </ul>
     *
     * @param value the double value to set for all array elements
     * @return this array instance for method chaining
     */
    public DoubleArray setAll(double value) {
        long ptr = array.ptr();
        for (int i = 0, size = array.getFlatViewLength(); i < size; i++) {
            Unsafe.getUnsafe().putDouble(ptr, value);
            ptr += Double.BYTES;
        }
        return this;
    }
}
