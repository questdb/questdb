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
import io.questdb.griffin.engine.functions.columns.ShortColumn;
import io.questdb.griffin.engine.functions.groupby.AvgShortGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.BitAndShortGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.BitOrShortGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.BitXorShortGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstShortGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastShortGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.SumShortGroupByFunction;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.IndirectShortArg;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.allocArgBuffer;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.assertEquivalence;

/**
 * Asserts byte-for-byte equivalence of every {@code computeKeyedBatch} override
 * that reads from a {@code SHORT} column against the default implementation
 * (loop over {@code computeFirst} / {@code computeNext}). Covers both the
 * direct-column fast path ({@code argAddr != 0}) and the record-based slow
 * path ({@code argAddr == 0}).
 * <p>
 * SHORT has no NULL sentinel so the override loops do not branch on null
 * values. The test still crosses the {@code isNew} branch (present in
 * {@link AvgShortGroupByFunction} and {@link BitAndShortGroupByFunction}) by
 * priming some entries and leaving others at the etalon.
 */
public class ShortGroupByFunctionKeyedBatchTest {
    private static final int ARG_COLUMN_INDEX = 0;
    // Bit-diverse values that also stress sum accumulation (mix of sign and magnitudes).
    private static final short[] ARG_VALUES = {
            (short) 0x5555, (short) 0xAAAA, (short) 0x0F0F, (short) -1,
            (short) 0, (short) 0x7FFF, (short) 0x8000, (short) 1
    };

    @Test
    public void testAvgShortFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new AvgShortGroupByFunction(ShortColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testAvgShortIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new AvgShortGroupByFunction(new IndirectShortArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testAvgShortSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new AvgShortGroupByFunction(ShortColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testBitAndShortFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitAndShortGroupByFunction(ShortColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testBitAndShortIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitAndShortGroupByFunction(new IndirectShortArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testBitAndShortSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitAndShortGroupByFunction(ShortColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testBitOrShortFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitOrShortGroupByFunction(ShortColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testBitOrShortIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitOrShortGroupByFunction(new IndirectShortArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testBitOrShortSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitOrShortGroupByFunction(ShortColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testBitXorShortFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitXorShortGroupByFunction(ShortColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testBitXorShortIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitXorShortGroupByFunction(new IndirectShortArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testBitXorShortSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitXorShortGroupByFunction(ShortColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstShortFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstShortGroupByFunction(ShortColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testFirstShortIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstShortGroupByFunction(new IndirectShortArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstShortSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstShortGroupByFunction(ShortColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastShortFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastShortGroupByFunction(ShortColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testLastShortIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastShortGroupByFunction(new IndirectShortArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastShortSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastShortGroupByFunction(ShortColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testSumShortFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new SumShortGroupByFunction(ShortColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testSumShortIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new SumShortGroupByFunction(new IndirectShortArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testSumShortSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new SumShortGroupByFunction(ShortColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    private static void testEquivalence(GroupByFunction function, boolean fastPath) {
        assertEquivalence(function, fastPath, Short.BYTES,
                allocArgBuffer(ARG_VALUES), (long) ARG_VALUES.length * Short.BYTES);
    }
}
