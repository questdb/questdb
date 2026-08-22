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
import io.questdb.griffin.engine.functions.columns.UuidColumn;
import io.questdb.griffin.engine.functions.groupby.CountUuidGroupByFunction;
import io.questdb.std.Numbers;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.IndirectUuidArg;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.allocArgBufferUuid;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.assertEquivalence;

/**
 * Asserts byte-for-byte equivalence of {@link CountUuidGroupByFunction}'s
 * {@code computeKeyedBatch} override against the default implementation (loop
 * over {@code computeFirst} / {@code computeNext}). Covers both the
 * direct-column fast path ({@code argAddr != 0}) and the record-based slow
 * path ({@code argAddr == 0}). UUID is stored as 16 bytes per row (a low
 * long followed by a high long); a UUID is null when both halves equal
 * {@link Numbers#LONG_NULL}.
 */
public class UuidGroupByFunctionKeyedBatchTest {
    private static final int ARG_COLUMN_INDEX = 0;
    // Mix full-null (both halves = LONG_NULL), partial-null (one half = LONG_NULL but
    // the other isn't; these are valid, non-null UUIDs) and ordinary values.
    private static final long[] LO_VALUES = {
            0x0123_4567_89AB_CDEFL,
            Numbers.LONG_NULL,                // full null (paired with LONG_NULL hi)
            0x1111_2222_3333_4444L,
            Numbers.LONG_NULL,                // non-null: hi is non-null below
            0x0L,
            Numbers.LONG_NULL,                // full null
            0xCAFE_BABE_DEAD_BEEFL,
            0x0000_0000_0000_0001L
    };
    private static final long[] HI_VALUES = {
            0x1FED_CBA9_8765_4321L,
            Numbers.LONG_NULL,                // full null
            0x5555_6666_7777_8888L,
            0x9999_AAAA_BBBB_CCCCL,           // non-null: lo is LONG_NULL but hi is not
            0x0L,
            Numbers.LONG_NULL,                // full null
            0xFEED_FACE_CAFE_D00DL,
            0x0000_0000_0000_0002L
    };

    @Test
    public void testCountUuidFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountUuidGroupByFunction(UuidColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testCountUuidIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountUuidGroupByFunction(new IndirectUuidArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testCountUuidSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountUuidGroupByFunction(UuidColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    private static void testEquivalence(GroupByFunction function, boolean fastPath) {
        final int rowStride = 16; // UUID = 2 longs
        assertEquivalence(function, fastPath, rowStride,
                allocArgBufferUuid(LO_VALUES, HI_VALUES), (long) LO_VALUES.length * rowStride);
    }
}
