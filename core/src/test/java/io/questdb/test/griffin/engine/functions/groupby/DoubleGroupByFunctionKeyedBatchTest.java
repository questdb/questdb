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
import io.questdb.griffin.engine.functions.columns.DoubleColumn;
import io.questdb.griffin.engine.functions.groupby.AvgDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastNotNullDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MaxDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.SumDoubleGroupByFunction;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.IndirectDoubleArg;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.allocArgBuffer;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.assertEquivalence;

/**
 * Asserts byte-for-byte equivalence of every {@code computeKeyedBatch} override
 * that reads from a {@code DOUBLE} column against the default implementation
 * (loop over {@code computeFirst} / {@code computeNext}). Covers both the
 * direct-column fast path ({@code argAddr != 0}) and the record-based slow
 * path ({@code argAddr == 0}), with a mix of new and existing entries and a
 * mix of NaN/infinity (treated as NULL by {@code Numbers.isNull(double)}) and
 * finite argument values.
 */
public class DoubleGroupByFunctionKeyedBatchTest {
    private static final int ARG_COLUMN_INDEX = 0;
    private static final double[] ARG_VALUES = {
            1.5, Double.NaN, -2.5, Double.NEGATIVE_INFINITY, -0.0, Double.POSITIVE_INFINITY, -1.0, 7.7
    };

    @Test
    public void testAvgDoubleFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new AvgDoubleGroupByFunction(DoubleColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testAvgDoubleIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new AvgDoubleGroupByFunction(new IndirectDoubleArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testAvgDoubleSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new AvgDoubleGroupByFunction(DoubleColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testCountDoubleFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountDoubleGroupByFunction(DoubleColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testCountDoubleIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountDoubleGroupByFunction(new IndirectDoubleArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testCountDoubleSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountDoubleGroupByFunction(DoubleColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstDoubleFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstDoubleGroupByFunction(DoubleColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testFirstDoubleIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstDoubleGroupByFunction(new IndirectDoubleArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstDoubleSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstDoubleGroupByFunction(DoubleColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstNotNullDoubleFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstNotNullDoubleGroupByFunction(DoubleColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testFirstNotNullDoubleIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstNotNullDoubleGroupByFunction(new IndirectDoubleArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstNotNullDoubleSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstNotNullDoubleGroupByFunction(DoubleColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastDoubleFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastDoubleGroupByFunction(DoubleColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testLastDoubleIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastDoubleGroupByFunction(new IndirectDoubleArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastDoubleSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastDoubleGroupByFunction(DoubleColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastNotNullDoubleFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastNotNullDoubleGroupByFunction(DoubleColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testLastNotNullDoubleIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastNotNullDoubleGroupByFunction(new IndirectDoubleArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastNotNullDoubleSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastNotNullDoubleGroupByFunction(DoubleColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMaxDoubleFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MaxDoubleGroupByFunction(DoubleColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testMaxDoubleIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MaxDoubleGroupByFunction(new IndirectDoubleArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMaxDoubleSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MaxDoubleGroupByFunction(DoubleColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMinDoubleFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MinDoubleGroupByFunction(DoubleColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testMinDoubleIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MinDoubleGroupByFunction(new IndirectDoubleArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMinDoubleSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MinDoubleGroupByFunction(DoubleColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testSumDoubleFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new SumDoubleGroupByFunction(DoubleColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testSumDoubleIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new SumDoubleGroupByFunction(new IndirectDoubleArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testSumDoubleSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new SumDoubleGroupByFunction(DoubleColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    private static void testEquivalence(GroupByFunction function, boolean fastPath) {
        assertEquivalence(function, fastPath, Double.BYTES,
                allocArgBuffer(ARG_VALUES), (long) ARG_VALUES.length * Double.BYTES);
    }
}
