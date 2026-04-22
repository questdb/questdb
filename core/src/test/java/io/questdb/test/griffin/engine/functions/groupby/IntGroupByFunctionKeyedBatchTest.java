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
import io.questdb.griffin.engine.functions.columns.IntColumn;
import io.questdb.griffin.engine.functions.groupby.BitAndIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.BitOrIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.BitXorIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastNotNullIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MaxIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.SumIntGroupByFunction;
import io.questdb.std.Numbers;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.IndirectIntArg;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.allocArgBuffer;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.assertEquivalence;

/**
 * Asserts byte-for-byte equivalence of every {@code computeKeyedBatch} override
 * that reads from an {@code INT} column against the default implementation
 * (loop over {@code computeFirst} / {@code computeNext}). Covers both the
 * direct-column fast path ({@code argAddr != 0}) and the record-based slow
 * path ({@code argAddr == 0}), with a mix of new and existing entries and a
 * mix of null ({@link Numbers#INT_NULL}) and non-null argument values.
 */
public class IntGroupByFunctionKeyedBatchTest {
    private static final int ARG_COLUMN_INDEX = 0;
    private static final int[] ARG_VALUES = {
            100, Numbers.INT_NULL, -50, 200, 0, Numbers.INT_NULL, -1, 7
    };

    @Test
    public void testBitAndIntFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitAndIntGroupByFunction(IntColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testBitAndIntIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitAndIntGroupByFunction(new IndirectIntArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testBitAndIntSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitAndIntGroupByFunction(IntColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testBitOrIntFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitOrIntGroupByFunction(IntColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testBitOrIntIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitOrIntGroupByFunction(new IndirectIntArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testBitOrIntSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitOrIntGroupByFunction(IntColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testBitXorIntFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitXorIntGroupByFunction(IntColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testBitXorIntIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitXorIntGroupByFunction(new IndirectIntArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testBitXorIntSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitXorIntGroupByFunction(IntColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testCountIntFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountIntGroupByFunction(IntColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testCountIntIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountIntGroupByFunction(new IndirectIntArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testCountIntSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountIntGroupByFunction(IntColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstIntFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstIntGroupByFunction(IntColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testFirstIntIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstIntGroupByFunction(new IndirectIntArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstIntSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstIntGroupByFunction(IntColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstNotNullIntFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstNotNullIntGroupByFunction(IntColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testFirstNotNullIntIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstNotNullIntGroupByFunction(new IndirectIntArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstNotNullIntSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstNotNullIntGroupByFunction(IntColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastIntFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastIntGroupByFunction(IntColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testLastIntIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastIntGroupByFunction(new IndirectIntArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastIntSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastIntGroupByFunction(IntColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastNotNullIntFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastNotNullIntGroupByFunction(IntColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testLastNotNullIntIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastNotNullIntGroupByFunction(new IndirectIntArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastNotNullIntSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastNotNullIntGroupByFunction(IntColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMaxIntFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MaxIntGroupByFunction(IntColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testMaxIntIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MaxIntGroupByFunction(new IndirectIntArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMaxIntSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MaxIntGroupByFunction(IntColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMinIntFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MinIntGroupByFunction(IntColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testMinIntIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MinIntGroupByFunction(new IndirectIntArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMinIntSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MinIntGroupByFunction(IntColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testSumIntFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new SumIntGroupByFunction(IntColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testSumIntIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new SumIntGroupByFunction(new IndirectIntArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testSumIntSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new SumIntGroupByFunction(IntColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    private static void testEquivalence(GroupByFunction function, boolean fastPath) {
        assertEquivalence(function, fastPath, Integer.BYTES,
                allocArgBuffer(ARG_VALUES), (long) ARG_VALUES.length * Integer.BYTES);
    }
}
