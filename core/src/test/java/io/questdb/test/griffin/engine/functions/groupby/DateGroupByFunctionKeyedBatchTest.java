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
import io.questdb.griffin.engine.functions.columns.DateColumn;
import io.questdb.griffin.engine.functions.groupby.FirstDateGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullDateGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastDateGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastNotNullDateGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MaxDateGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinDateGroupByFunction;
import io.questdb.std.Numbers;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.IndirectDateArg;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.allocArgBuffer;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.assertEquivalence;

/**
 * Asserts byte-for-byte equivalence of every {@code computeKeyedBatch} override
 * that reads from a {@code DATE} column against the default implementation
 * (loop over {@code computeFirst} / {@code computeNext}). Covers both the
 * direct-column fast path ({@code argAddr != 0}) and the record-based slow
 * path ({@code argAddr == 0}). DATE is stored as a 64-bit millisecond offset
 * and uses {@link Numbers#LONG_NULL} as the null sentinel.
 */
public class DateGroupByFunctionKeyedBatchTest {
    private static final int ARG_COLUMN_INDEX = 0;
    private static final long[] ARG_VALUES = {
            1_700_000_000_000L, Numbers.LONG_NULL, 1_600_000_000_000L, 1_800_000_000_000L,
            0L, Numbers.LONG_NULL, -1L, 1_900_000_000_000L
    };

    @Test
    public void testFirstDateFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstDateGroupByFunction(DateColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testFirstDateIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstDateGroupByFunction(new IndirectDateArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstDateSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstDateGroupByFunction(DateColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstNotNullDateFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstNotNullDateGroupByFunction(DateColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testFirstNotNullDateIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstNotNullDateGroupByFunction(new IndirectDateArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstNotNullDateSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstNotNullDateGroupByFunction(DateColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastDateFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastDateGroupByFunction(DateColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testLastDateIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastDateGroupByFunction(new IndirectDateArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastDateSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastDateGroupByFunction(DateColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastNotNullDateFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastNotNullDateGroupByFunction(DateColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testLastNotNullDateIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastNotNullDateGroupByFunction(new IndirectDateArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastNotNullDateSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastNotNullDateGroupByFunction(DateColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMaxDateFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MaxDateGroupByFunction(DateColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testMaxDateIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MaxDateGroupByFunction(new IndirectDateArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMaxDateSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MaxDateGroupByFunction(DateColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMinDateFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MinDateGroupByFunction(DateColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testMinDateIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MinDateGroupByFunction(new IndirectDateArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMinDateSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MinDateGroupByFunction(DateColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    private static void testEquivalence(GroupByFunction function, boolean fastPath) {
        assertEquivalence(function, fastPath, Long.BYTES,
                allocArgBuffer(ARG_VALUES), (long) ARG_VALUES.length * Long.BYTES);
    }
}
