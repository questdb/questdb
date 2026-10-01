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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.FloatColumn;
import io.questdb.griffin.engine.functions.groupby.CountFloatGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstFloatGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullFloatGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastFloatGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastNotNullFloatGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MaxFloatGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinFloatGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.SumFloatGroupByFunction;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.IndirectFloatArg;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.allocArgBuffer;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.assertEquivalence;

/**
 * Asserts byte-for-byte equivalence of every {@code computeKeyedBatch} override
 * that reads from a {@code FLOAT} column against the default implementation
 * (loop over {@code computeFirst} / {@code computeNext}). Covers both the
 * direct-column fast path ({@code argAddr != 0}) and the record-based slow
 * path ({@code argAddr == 0}), with a mix of new and existing entries and a
 * mix of NaN/infinity (treated as NULL by {@code Numbers.isNull(float)}) and
 * finite argument values.
 */
public class FloatGroupByFunctionKeyedBatchTest {
    private static final int ARG_COLUMN_INDEX = 0;
    private static final float[] ARG_VALUES = {
            1.5f, Float.NaN, -2.5f, Float.NEGATIVE_INFINITY, -0.0f, Float.POSITIVE_INFINITY, -1.0f, 7.7f
    };

    @Test
    public void testCountFloatFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountFloatGroupByFunction(FloatColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testCountFloatIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountFloatGroupByFunction(new IndirectFloatArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testCountFloatSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountFloatGroupByFunction(FloatColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstFloatFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstFloatGroupByFunction(FloatColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testFirstFloatIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstFloatGroupByFunction(new IndirectFloatArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstFloatSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstFloatGroupByFunction(FloatColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstNotNullFloatFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstNotNullFloatGroupByFunction(FloatColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testFirstNotNullFloatIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstNotNullFloatGroupByFunction(new IndirectFloatArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstNotNullFloatSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstNotNullFloatGroupByFunction(FloatColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastFloatFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastFloatGroupByFunction(FloatColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testLastFloatIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastFloatGroupByFunction(new IndirectFloatArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastFloatSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastFloatGroupByFunction(FloatColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastNotNullFloatFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastNotNullFloatGroupByFunction(FloatColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testLastNotNullFloatIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastNotNullFloatGroupByFunction(new IndirectFloatArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastNotNullFloatSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastNotNullFloatGroupByFunction(FloatColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMaxFloatFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MaxFloatGroupByFunction(FloatColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testMaxFloatIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MaxFloatGroupByFunction(new IndirectFloatArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMaxFloatSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MaxFloatGroupByFunction(FloatColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMinFloatFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MinFloatGroupByFunction(FloatColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testMinFloatIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MinFloatGroupByFunction(new IndirectFloatArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMinFloatSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MinFloatGroupByFunction(FloatColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testSumFloatFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new SumFloatGroupByFunction(FloatColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testSumFloatIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new SumFloatGroupByFunction(new IndirectFloatArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testSumFloatSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new SumFloatGroupByFunction(FloatColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    private static void testEquivalence(GroupByFunction function, boolean fastPath) {
        assertEquivalence(function, fastPath, Float.BYTES,
                allocArgBuffer(ARG_VALUES), (long) ARG_VALUES.length * Float.BYTES);
    }
}
