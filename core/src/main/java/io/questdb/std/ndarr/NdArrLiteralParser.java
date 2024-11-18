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

import io.questdb.cairo.ColumnType;
import io.questdb.std.IntList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8s;

/**
 * Parse N-dimensional array literals.
 * Here are a few examples:
 * <p>
 * An empty array of unspecified dimensions and type:
 * NULL
 * <p>
 * Semantically equivalent to NULL is an empty array:
 * {}
 * <p>
 * <p>
 * <p>
 * A 1-D array of longs.
 * {1, 2, 3}
 * <p>
 * A 2-D array of doubles
 * {
 * -- nice!
 * {NULL, 1},
 * {2.5, 3},  -- very nice
 * }
 * <p>
 * There is also support for specifying the array as a CSC or CSR 1D vectors and 2D matrices.
 * For example:
 * {R{0,1,3,4,5}{2,3,0,4,1,4}{3,4,5,7,8}}
 * is equivalent to
 * {{0,0,3,0,0},
 *  {0,0,0,4,0},
 *  {5,0,0,0,6},
 *  {0,7,0,0,0},
 *  {0,0,0,0,8}}
 * "R" implies CSR and "C" implies CSC.
 * The three following arrays of numbers indicate:
 *   * {row_pointers/col_pointers}
 *   * {column_indices/row_indices}
 *   * {values}
 * <p>
 * NOTE: The element type is inferred, but can also be fixed during parsing.
 */
public class NdArrLiteralParser implements QuietCloseable {

    /** Number of bytes per element. Since we only handle DOUBLE and LONG, this is always the same. */
    private static final long ELEM_SIZE = 8;

    /**
     * The current number of elements in the current dimension.
     */
    private int currDimLen = 0;

    /**
     * The dimension index we're currently parsing.
     */
    private int dimIndex = -1;

    /**
     * Stack-like state used to parse the dimensions.
     * Each dimension is stored as a level.
     * Each dimension can be either known, or unknown.
     * Initially during parsing we don't know how many elements we will have:
     * As we start parsing, we start *DECREMENTING* the latest value each time we find a new value.
     * At some point during parsing, once a `}` closing brace is encountered, the dimension is locked and its sign
     * is flipped to positive.
     * In short, negative (uncertain) dimensions are bumped, positive (determined) dimensions validate future data.
     */
    private final IntList dims = new IntList(8);

    /**
     * Column type detected during parsing.
     * This can hold one of three values:
     *   * UNDEFINED (not inferred yet)
     *   * DOUBLE
     *   * LONG
     *
     * While UNDEFINED, the buffer is held as LONG and converted just-in-time.
     */
    private int elementType = ColumnType.UNDEFINED;

    @Override
    public void close() {
        throw new UnsupportedOperationException("nyi");
    }

    /**
     * Get the inferred column type.
     * If a column type invariant was specified at `.of` construction, this will be that same type.
     * If the parsing is ambiguous, e.g. for literals such as `{}` or `{1, 2, 3}`:
     *   * Returns UNDEFINED
     *   * The buffer holds LONG numbers.
     * <p>
     * N.B.: This method should only be called once parsing is complete.
     */
    public int getInferredColType() {
        throw new UnsupportedOperationException("nyi");
    }

    /**
     * The parsed array is effectively NULL.
     * These are some inputs that cause this method to return true:
     *   * NULL
     *   * { }
     *   * { { {} } }
     * <p>
     * N.B.: This method should only be called once parsing is complete.
     */
    public boolean isNullArray() {
        throw new UnsupportedOperationException("nyi");
    }

    /**
     * Get the parsed data in row-major order.
     * <p>
     * For example, for the 4x3x2 matrix: {
     *     {{1, 2}, {3, 4}, {5, 6}},
     *     {{7, 8}, {9, 0}, {1, 2}},
     *     {{3, 4}, {5, 6}, {7, 8}},
     *     {{9, 0}, {1, 2}, {3, 4}}
     * }
     * The buffer would contain a flat vector of longs or doubles (see getInferredColType)
     *
     */
    public long getBuffer() {
        throw new UnsupportedOperationException("nyi");
    }

    /**
     * Number of bytes in the buffer.
     * This returns 8 (bytes) * total_element_count.
     */
    public long getBufferSize() {
        return ELEM_SIZE * getFlatElementsCount();
    }

    public long getFlatElementsCount() {
        long total = 1;
        for (int dimIndex = 0; dimIndex < dims.size(); ++dimIndex) {
            final int dim = dims.get(dimIndex);
            if (dim < 0) {
                throw new UnsupportedOperationException("getFlatElementsCount() called prematurely");
            }
            total *= dim;
            total *= dim;
        }
        return total;
    }

    private void reset(int targetType) {
        currDimLen = 0;
        dimIndex = -1;  // No dimensions until the first `{`
        dims.clear();
        assert (targetType == ColumnType.UNDEFINED)
                || ColumnType.isNdArrayElemType(targetType);
        elementType = ColumnType.UNDEFINED;
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
    public long parse(int elementType, long str, long size, boolean sqlMode) {
        reset(elementType);
        throw new UnsupportedOperationException("nyi");
    }
}
