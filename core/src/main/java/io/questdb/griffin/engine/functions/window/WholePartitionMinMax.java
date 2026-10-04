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

import io.questdb.griffin.engine.window.WindowFunction;

/**
 * Recognises the window functions that compute {@code min(x)} or {@code max(x)} over a whole
 * partition: {@code OVER (PARTITION BY ...)} with the default frame, or with a frame from
 * UNBOUNDED PRECEDING to UNBOUNDED FOLLOWING. Each row of a partition gets the same value, which a
 * GROUP BY of the partition keys can compute instead. The two-phase plan in
 * {@link io.questdb.griffin.engine.window.AsyncWindowMinMaxFilterRecordCursorFactory} relies on
 * the exact semantics of these classes' first pass, which its aggregation mirrors:
 * <ul>
 *     <li>DOUBLE: only finite values count. {@code max} keeps the greatest value under
 *     {@link Double#compare}, so the result does not depend on row order. {@code min} replaces
 *     its value only when {@code Numbers.compare(d, min) < 0}, which treats values within
 *     {@code Numbers.DOUBLE_TOLERANCE} as equal, so with such near ties the first of them in
 *     scan order wins.</li>
 *     <li>LONG, TIMESTAMP, DATE: NULL is skipped and the comparison is exact.</li>
 *     <li>A partition without a counted value reads NULL.</li>
 * </ul>
 */
public final class WholePartitionMinMax {
    public static final int MAX = 2;
    public static final int MIN = 1;
    public static final int NONE = 0;

    private WholePartitionMinMax() {
    }

    /**
     * {@link #MIN} or {@link #MAX} for a whole-partition min or max window function, otherwise
     * {@link #NONE}.
     */
    public static int kindOf(WindowFunction function) {
        if (function instanceof MaxDoubleWindowFunctionFactory.MaxMinOverPartitionFunction
                || function instanceof MaxLongWindowFunctionFactory.MaxMinOverPartitionFunction
                || function instanceof MaxMinWindowFunctionFactoryHelper.MaxMinOverPartitionBase) {
            final String name = function.getName();
            if (MinDoubleWindowFunctionFactory.NAME.equals(name)) {
                return MIN;
            }
            if (MaxDoubleWindowFunctionFactory.NAME.equals(name)) {
                return MAX;
            }
        }
        return NONE;
    }
}
