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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.ColumnType;
import io.questdb.std.IntList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.ndarr.NdArrFormat;
import io.questdb.std.str.DirectUtf8String;

/**
 * Parse N-dimensional arrays for ILP input.
 *
 * <p>Here are a few examples:</p>
 *
 * <p>A 1-D array of longs:</p>
 * <pre><code>{64i1,2,3}</code></pre>
 *
 * <p>A 2-D array of doubles:</p>
 * <pre><code>
 * {
 *     -- a comment
 *     {64fNaN,1},
 *     {64f2.5,3}  -- yet another comment
 * }
 * </code></pre>
 *
 * <p>The type marker is as follows: <code>[precision][number_class]</code></p>
 * <dl>
 *     <dt>precision</dt>
 *     <dd>number of bits in the numeric type, e.g. <code>8</code>, <code>16</code>, <code>32</code>, <code>64</code></dd>
 *     <dt>number_class</dt>
 *     <dd>type of number, <code>i</code> for signed integer, <code>u</code> for unsigned,
 *         <code>f</code> for floating point</dd>
 * </dl>
 *
 * <p>There is also support for specifying the array as a CSC or CSR 1D vectors and 2D matrices.
 * For example:</p>
 * <pre><code>{32uR{0,1,3,4,5}{2,0,3,4,1}{3,5,4,7,8}}</code></pre>
 * <p>is equivalent to (extra invalid whitespace added for readability):</p>
 * <pre><code>
 * {32u
 *     {0, 0, 3, 0, 0},
 *     {5, 0, 0, 4, 0},
 *     {0, 0, 0, 0, 7},
 *     {0, 8, 0, 0, 0}
 * }
 * </code></pre>
 *
 * <p><code>R</code> tags <strong>CSR</strong> and <code>C</code> tags <strong>CSC</strong>.</p>
 * <p>The three following arrays of numbers indicate:</p>
 * <ul>
 *   <li><code>{row_pointers/col_pointers}</code></li>
 *   <li><code>{column_indices/row_indices}</code></li>
 *   <li><code>{values}</code></li>
 * </ul>
 */
public class NdArrParser implements QuietCloseable {
    /**
     * The current number of elements in the current dimension (for `NdArrFormat.RM` parsing).
     */
    private int currDimLen = 0;

    /**
     * The dimension index we're currently parsing (for `NdArrFormat.RM` parsing).
     */
    private int dimIndex = -1;

    /**
     * Stack-like state used to parse the dimensions.
     * <p>
     * When `NdArrFormat.RM`:
     * Each dimension is stored as a level.
     * Each dimension can be either known, or unknown.
     * Initially during parsing we don't know how many elements we will have:
     * As we start parsing, we start *DECREMENTING* the latest value each time we find a new value.
     * At some point during parsing, once a `}` closing brace is encountered, the dimension is locked and its sign
     * is flipped to positive.
     * In short, negative (uncertain) dimensions are bumped, positive (determined) dimensions validate future data.
     * <p>
     * When format is `NdArrFormat.CSR` or `NdArrFormat.CSC`, this is set after parsing.
     */
    private final IntList dims = new IntList(8);
    private int format = NdArrFormat.UNDEFINED;

    private int type = ColumnType.UNDEFINED;

    @Override
    public void close() {
        throw new UnsupportedOperationException("nyi");
    }

    /**
     * Get the array ColumnType.
     * <p><strong>N.B.</strong>: This method should only be called once parsing is complete.</p>
     */
    public int getType() {
        return type;
    }

    /**
     * Returns a value from the `NdArrFormat` enum indicating how the array is represented in memory.
     * <p><strong>N.B.</strong>: This method should only be called once parsing is complete.</p>
     */
    public int getFormat() {
        return format;
    }

    /**
     * The parsed array is effectively NULL, i.e. contains no values.
     * <p><strong>N.B.</strong>: This method should only be called once parsing is complete.</p>
     */
    public boolean isNullArray() {
        throw new UnsupportedOperationException("nyi");
    }

    /**
     * Get the address of the {row_pointers/col_pointers} vector for the CSR/CSC sparse array.
     * Returns a null address if `getFormat()` is dense (i.e. `RM`).
     * The returned buffer contains 32-bit integers.
     * <p><strong>N.B.</strong>: This method should only be called once parsing is complete.</p>
     */
    public long getSparsePointers() {
        throw new UnsupportedOperationException("nyi");
    }

    /**
     * Get the count of numbers present in the vector returned by `getSparsePointers()`.
     * Call `getSparsePointersSize()` to get the byte size.
     * <p><strong>N.B.</strong>: This method should only be called once parsing is complete.</p>
     */
    public int getSparsePointersCount() {
        throw new UnsupportedOperationException("nyi");
    }

    /**
     * Number of bytes in the vector returned by `getSparsePointers()`.
     * <p><strong>N.B.</strong>: This method should only be called once parsing is complete.</p>
     */
    public int getSparsePointersSize() {
        return getSparsePointersCount() * Integer.BYTES;
    }

    /**
     * Get the {column_indices/row_indices} vector for the CSR/CSC sparse array.
     * Returns a nullptr if `getFormat()` is dense (i.e. `RM`).
     * The returned buffer contains 32-bit integers.
     * <p><strong>N.B.</strong>: This method should only be called once parsing is complete.</p>
     */
    public long getSparseIndices() {
        throw new UnsupportedOperationException("nyi");
    }

    /**
     * Get the count of numbers present in the vector returned by `getSparseIndices()`.
     * Call `getSparseIndicesSize()` to get the byte size.
     * <p><strong>N.B.</strong>: This method should only be called once parsing is complete.</p>
     */
    public int getSparseIndicesCount() {
        throw new UnsupportedOperationException("nyi");
    }

    /**
     * Number of bytes in the vector returned by `getSparseIndices()`.
     * <p><strong>N.B.</strong>: This method should only be called once parsing is complete.</p>
     */
    public int getSparseIndicesSize() {
        return getSparseIndicesCount() * Integer.BYTES;
    }

    /**
     * Get the ptr address to the flattened values buffer.
     * Its content depends on the format (see `getFormat()`).
     * <p>
     * When the format is `NdArrFormat.RM` (dense, row-major) it's a
     * flattened array of all the elements.
     * <p>
     * For example, for the 4x3x2 matrix: {
     *     {{1, 2}, {3, 4}, {5, 6}},
     *     {{7, 8}, {9, 0}, {1, 2}},
     *     {{3, 4}, {5, 6}, {7, 8}},
     *     {{9, 0}, {1, 2}, {3, 4}}
     * }
     * The buffer would contain a flat vector of elements (see getElementType)
     * with the numbers [1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4]
     *
     * <p><strong>N.B.</strong>: This method should only be called once parsing is complete.</p>
     */
    public long getElements() {
        throw new UnsupportedOperationException("nyi");
    }

    /**
     * Number of bytes in the buffer.
     * <p><strong>N.B.</strong>: This method should only be called once parsing is complete.</p>
     */
    public int getElementsSize() {
        return ColumnType.sizeOf(getType()) * getElementsCount();
    }

    /**
     * Number of elements returned by `getElements()`
     * <p><strong>N.B.</strong>: This method should only be called once parsing is complete.</p>
     */
    public int getElementsCount() {
        throw new UnsupportedOperationException("nyi");
    }

    private void reset() {
        currDimLen = 0;
        dimIndex = -1;  // No dimensions until the first `{`
        dims.clear();
        type = ColumnType.UNDEFINED;
        format = NdArrFormat.UNDEFINED;
    }

    /**
     * Resets state and parses.
     * Returns the number of bytes advanced.
     * The `elementType` indicates the backing type of the array.
     * It can be `UNDEFINED` (if not yet known), `DOUBLE` or `LONG`.
     * The `str` and `size` indicate the UTF-8 bytes to parse.
     * <p>
     * The parser can operate in one of two modes:
     *   * `sqlMode==true` (for SQL)
     *       * NULL keyword supported.
     *       * White space allowed.
     *       * Comments allowed.
     *   * `sqlMode==false` (for ILP), disables these human-friendly features.
     * <p>
     * Grammar:
     * See the `nd_arr_grammar.py` for reference. It is more intended for syntax highlighting etc.
     * There are a few differences with the implementation here:
     *   * That grammar performs no validation, such as checking that all rows have the same element count.
     *   * That grammar does not support whitespace or comments.
     */
    public long parse(DirectUtf8String value) {
        reset();
        throw new UnsupportedOperationException("array parsing not yet implemented");
    }
}
