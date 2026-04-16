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
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.groupby.BitAndLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.BitOrLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.BitXorLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountLongConstGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastNotNullLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MaxLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.SumLongGroupByFunction;
import io.questdb.std.Numbers;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.IndirectLongArg;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.allocArgBuffer;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.assertEquivalence;

/**
 * Asserts byte-for-byte equivalence of every {@code computeKeyedBatch} override
 * that reads from a {@code LONG} column against the default implementation
 * (loop over {@code computeFirst} / {@code computeNext}). Covers both the
 * direct-column fast path ({@code argAddr != 0}) and the record-based slow
 * path ({@code argAddr == 0}), with a mix of new and existing entries and a
 * mix of null and non-null argument values — enough to cross every branch in
 * the override loops.
 */
public class LongGroupByFunctionKeyedBatchTest {
    private static final int ARG_COLUMN_INDEX = 0;
    private static final long[] ARG_VALUES = {
            100L, Numbers.LONG_NULL, -50L, 200L, 0L, Numbers.LONG_NULL, -1L, 7L
    };

    @Test
    public void testBitAndLongFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitAndLongGroupByFunction(LongColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testBitAndLongIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitAndLongGroupByFunction(new IndirectLongArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testBitAndLongSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitAndLongGroupByFunction(LongColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testBitOrLongFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitOrLongGroupByFunction(LongColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testBitOrLongIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitOrLongGroupByFunction(new IndirectLongArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testBitOrLongSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitOrLongGroupByFunction(LongColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testBitXorLongFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitXorLongGroupByFunction(LongColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testBitXorLongIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitXorLongGroupByFunction(new IndirectLongArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testBitXorLongSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitXorLongGroupByFunction(LongColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testCountLongConst() throws Exception {
        // count(*) ignores the argument; the override has no fast/slow branch.
        TestUtils.assertMemoryLeak(() -> testEquivalence(new CountLongConstGroupByFunction(), true));
    }

    @Test
    public void testCountLongFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountLongGroupByFunction(LongColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testCountLongIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountLongGroupByFunction(new IndirectLongArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testCountLongSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountLongGroupByFunction(LongColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstLongFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstLongGroupByFunction(LongColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testFirstLongIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstLongGroupByFunction(new IndirectLongArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstLongSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstLongGroupByFunction(LongColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstNotNullLongFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstNotNullLongGroupByFunction(LongColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testFirstNotNullLongIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstNotNullLongGroupByFunction(new IndirectLongArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstNotNullLongSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstNotNullLongGroupByFunction(LongColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastLongFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastLongGroupByFunction(LongColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testLastLongIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastLongGroupByFunction(new IndirectLongArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastLongSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastLongGroupByFunction(LongColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastNotNullLongFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastNotNullLongGroupByFunction(LongColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testLastNotNullLongIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastNotNullLongGroupByFunction(new IndirectLongArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastNotNullLongSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastNotNullLongGroupByFunction(LongColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMaxLongFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MaxLongGroupByFunction(LongColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testMaxLongIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MaxLongGroupByFunction(new IndirectLongArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMaxLongSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MaxLongGroupByFunction(LongColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMinLongFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MinLongGroupByFunction(LongColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testMinLongIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MinLongGroupByFunction(new IndirectLongArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMinLongSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MinLongGroupByFunction(LongColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testSumLongFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new SumLongGroupByFunction(LongColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testSumLongIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new SumLongGroupByFunction(new IndirectLongArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testSumLongSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new SumLongGroupByFunction(LongColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    private static void testEquivalence(GroupByFunction function, boolean fastPath) {
        assertEquivalence(function, fastPath, Long.BYTES,
                allocArgBuffer(ARG_VALUES), (long) ARG_VALUES.length * Long.BYTES);
    }
}
