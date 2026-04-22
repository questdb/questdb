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
import io.questdb.griffin.engine.functions.columns.Long256Column;
import io.questdb.griffin.engine.functions.groupby.CountLong256GroupByFunction;
import io.questdb.std.Numbers;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.IndirectLong256Arg;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.allocArgBufferLong256;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.assertEquivalence;

/**
 * Asserts byte-for-byte equivalence of {@link CountLong256GroupByFunction}'s
 * {@code computeKeyedBatch} override against the default implementation (loop
 * over {@code computeFirst} / {@code computeNext}). Covers both the
 * direct-column fast path ({@code argAddr != 0}) and the record-based slow
 * path ({@code argAddr == 0}). LONG256 is stored as 32 bytes per row (four
 * longs); a value is null only when all four longs equal
 * {@link Numbers#LONG_NULL}.
 */
public class Long256GroupByFunctionKeyedBatchTest {
    private static final int ARG_COLUMN_INDEX = 0;
    // Row layout: rows 1 and 5 are fully null; others mix non-null values across the
    // four long slots, including partial-null rows (3 and 6) to verify that the
    // all-four-longs null check does not short-circuit on the first slot.
    private static final long[] L0 = {
            0x0000_0000_0000_0001L,
            Numbers.LONG_NULL,
            0x1234_5678_9ABC_DEF0L,
            Numbers.LONG_NULL,
            0xDEAD_BEEF_CAFE_BABEL,
            Numbers.LONG_NULL,
            0x0L,
            0x7FFF_FFFF_FFFF_FFFFL
    };
    private static final long[] L1 = {
            0x0000_0000_0000_0002L,
            Numbers.LONG_NULL,
            0x1111_2222_3333_4444L,
            Numbers.LONG_NULL,
            0x1L,
            Numbers.LONG_NULL,
            0x0L,
            0xFFFF_FFFF_FFFF_FFFFL
    };
    private static final long[] L2 = {
            0x0000_0000_0000_0003L,
            Numbers.LONG_NULL,
            0x5555_6666_7777_8888L,
            Numbers.LONG_NULL,
            0x2L,
            0x1234_5678_9ABC_DEF0L,          // partial-null (L2 non-null but L1/L0 null)
            0x0L,
            0x8000_0000_0000_0000L
    };
    private static final long[] L3 = {
            0x0000_0000_0000_0004L,
            Numbers.LONG_NULL,
            0xAAAA_BBBB_CCCC_DDDDL,
            0x1234_5678_9ABC_DEF0L,          // partial-null (L3 non-null, others null)
            0x3L,
            Numbers.LONG_NULL,
            0x0L,
            0x0000_0000_0000_0001L
    };

    @Test
    public void testCountLong256FastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountLong256GroupByFunction(Long256Column.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testCountLong256IndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountLong256GroupByFunction(new IndirectLong256Arg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testCountLong256SlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountLong256GroupByFunction(Long256Column.newInstance(ARG_COLUMN_INDEX)), false));
    }

    private static void testEquivalence(GroupByFunction function, boolean fastPath) {
        final int rowStride = 32; // LONG256 = 4 longs
        assertEquivalence(function, fastPath, rowStride,
                allocArgBufferLong256(L0, L1, L2, L3), (long) L0.length * rowStride);
    }
}
