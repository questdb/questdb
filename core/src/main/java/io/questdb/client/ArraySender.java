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

package io.questdb.client;

import io.questdb.cutlass.line.array.DoubleArray;
import io.questdb.cutlass.line.array.LongArray;
import org.jetbrains.annotations.NotNull;

public interface ArraySender<T> {
    /**
     * Convenience method to insert a 1D double array. Same semantics as
     * {@link #doubleArray(CharSequence, DoubleArray)} with a 1D {@code DoubleArray}.
     *
     * @param name   the column name
     * @param values the 1D double array values
     * @return this instance, to support method chaining
     */
    T doubleArray(@NotNull CharSequence name, double[] values);

    /**
     * Convenience method to insert a 2D double array. Same semantics as
     * {@link #doubleArray(CharSequence, DoubleArray)} with a 2D {@code DoubleArray}.
     *
     * @param name   the column name
     * @param values the 2D double array values
     * @return this instance, to support method chaining
     */
    T doubleArray(@NotNull CharSequence name, double[][] values);

    /**
     * Convenience method to insert a 3D double array. Same semantics as
     * {@link #doubleArray(CharSequence, DoubleArray)} with a 3D {@code DoubleArray}.
     *
     * @param name   the column name
     * @param values the 3D double array values
     * @return this instance, to support method chaining
     */
    T doubleArray(@NotNull CharSequence name, double[][][] values);

    /**
     * Adds a column containing a multidimensional array of {@code double} values.
     *
     * @param name  Column name identifier (non-null)
     * @param array N-dimensional array to be added. Supported dimensionality: 1D to 32D
     * @return this instance, to support method chaining
     */
    T doubleArray(CharSequence name, DoubleArray array);

    /**
     * Convenience method to insert a 1D {@code long} array. Same semantics as
     * {@link #longArray(CharSequence, LongArray)} with a 1D {@code LongArray}.
     *
     * @param name   the column name
     * @param values the 1D long array values
     * @return this instance, to support method chaining
     */
    T longArray(@NotNull CharSequence name, long[] values);

    /**
     * Convenience method to insert a 2D {@code long} array. Same semantics as
     * {@link #longArray(CharSequence, LongArray)} with a 2D {@code LongArray}.
     *
     * @param name   the column name
     * @param values the 2D long array values
     * @return this instance, to support method chaining
     */
    T longArray(@NotNull CharSequence name, long[][] values);

    /**
     * Convenience method to insert a 3D {@code long} array. Same semantics as
     * {@link #longArray(CharSequence, LongArray)} with a 3D {@code LongArray}.
     *
     * @param name   the column name
     * @param values the 3D long array values
     * @return this instance, to support method chaining
     */
    T longArray(@NotNull CharSequence name, long[][][] values);

    /**
     * Adds a column containing a multidimensional array of {@code long} values.
     *
     * @param name   Column name identifier (non-null)
     * @param values N-dimensional array to be added. Supported dimensionality: 1D to 32D
     * @return this instance, to support method chaining
     */
    T longArray(@NotNull CharSequence name, LongArray values);
}
