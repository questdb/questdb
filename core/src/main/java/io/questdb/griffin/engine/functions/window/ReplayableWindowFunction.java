/*+*****************************************************************************
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


package io.questdb.griffin.engine.functions.window;

/**
 * A streaming DOUBLE window function whose value at a row depends only on the values of its
 * argument up to that row, taken in order. A copy of it on another thread can then hand its
 * arguments over as they are, and this function computes its values from them with its own
 * arithmetic, from the state the rows before them left: the same operations, in the same order,
 * as computing them from the rows, so the values are the same bit for bit. See
 * {@code AsyncWindowSplitPlan.OP_REPLAY}.
 */
public interface ReplayableWindowFunction {

    /**
     * The function's value at the last row replayed, as {@code getDouble()} returns it.
     */
    double getReplayedValue();

    /**
     * Whether the function's value depends only on its argument's values, in order: no
     * partition, no timestamp.
     */
    boolean isReplayable();

    /**
     * Computes the next row as {@code computeNext()} would for a row whose argument is
     * {@code value}.
     */
    void replayNext(double value);

    /**
     * Starts the function afresh, as for the first row of a partition.
     */
    void toTop();
}
