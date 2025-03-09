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

package io.questdb.client;

import io.questdb.cutlass.line.array.DoubleArray;
import io.questdb.cutlass.line.array.NDLongArray;

public interface INDArray<T> {
    /**
     * Adds a column containing multi-dimensional array values.
     *
     * <p>Overloaded methods support arrays from 1D to 32D dimensions. All variants share the same semantic meaning,
     * differing only in the dimensionality of the input array structure.
     *
     * <h3>Dimension Examples:</h3>
     * <pre>
     * - double[]        : 1D (vector)
     * - double[][]      : 2D (matrix)
     * - double[][][]    : 3D (tensor)
     * - {@link DoubleArray}: any-D (upto 32)
     * </pre>
     *
     * @param name   Column name identifier (non-null)
     * @param values N-dimensional double array to be added. Supported dimensions: 1D to 16D.
     * @return T instance to support method chaining
     */
    T doubleArray(CharSequence name, double[] values);

    T doubleArray(CharSequence name, double[][] values);

    T doubleArray(CharSequence name, double[][][] values);

    T doubleArray(CharSequence name, DoubleArray values);

    T longArray(CharSequence name, long[] values);

    T longArray(CharSequence name, long[][] values);

    T longArray(CharSequence name, long[][][] values);

    T longArray(CharSequence name, NDLongArray values);
}
