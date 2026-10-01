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

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.TimestampColumn;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullTimestampGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstTimestampGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastNotNullTimestampGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastTimestampGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MaxTimestampGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinTimestampGroupByFunction;
import io.questdb.std.Numbers;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.IndirectTimestampArg;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.allocArgBuffer;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.assertEquivalence;

/**
 * Asserts byte-for-byte equivalence of every {@code computeKeyedBatch} override
 * that reads from a {@code TIMESTAMP} column against the default implementation
 * (loop over {@code computeFirst} / {@code computeNext}). Covers both the
 * direct-column fast path ({@code argAddr != 0}) and the record-based slow
 * path ({@code argAddr == 0}). TIMESTAMP is stored as a 64-bit microsecond
 * offset and uses {@link Numbers#LONG_NULL} as the null sentinel.
 */
public class TimestampGroupByFunctionKeyedBatchTest {
    private static final int ARG_COLUMN_INDEX = 0;
    private static final long[] ARG_VALUES = {
            1_700_000_000_000_000L, Numbers.LONG_NULL, 1_600_000_000_000_000L, 1_800_000_000_000_000L,
            0L, Numbers.LONG_NULL, -1L, 1_900_000_000_000_000L
    };

    @Test
    public void testFirstNotNullTimestampFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstNotNullTimestampGroupByFunction(
                        TimestampColumn.newInstance(ARG_COLUMN_INDEX, ColumnType.TIMESTAMP),
                        ColumnType.TIMESTAMP), true));
    }

    @Test
    public void testFirstNotNullTimestampIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstNotNullTimestampGroupByFunction(
                        new IndirectTimestampArg(ARG_COLUMN_INDEX, ColumnType.TIMESTAMP),
                        ColumnType.TIMESTAMP), false));
    }

    @Test
    public void testFirstNotNullTimestampSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstNotNullTimestampGroupByFunction(
                        TimestampColumn.newInstance(ARG_COLUMN_INDEX, ColumnType.TIMESTAMP),
                        ColumnType.TIMESTAMP), false));
    }

    @Test
    public void testFirstTimestampFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstTimestampGroupByFunction(
                        TimestampColumn.newInstance(ARG_COLUMN_INDEX, ColumnType.TIMESTAMP),
                        ColumnType.TIMESTAMP), true));
    }

    @Test
    public void testFirstTimestampIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstTimestampGroupByFunction(
                        new IndirectTimestampArg(ARG_COLUMN_INDEX, ColumnType.TIMESTAMP),
                        ColumnType.TIMESTAMP), false));
    }

    @Test
    public void testFirstTimestampSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstTimestampGroupByFunction(
                        TimestampColumn.newInstance(ARG_COLUMN_INDEX, ColumnType.TIMESTAMP),
                        ColumnType.TIMESTAMP), false));
    }

    @Test
    public void testLastNotNullTimestampFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastNotNullTimestampGroupByFunction(
                        TimestampColumn.newInstance(ARG_COLUMN_INDEX, ColumnType.TIMESTAMP),
                        ColumnType.TIMESTAMP), true));
    }

    @Test
    public void testLastNotNullTimestampIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastNotNullTimestampGroupByFunction(
                        new IndirectTimestampArg(ARG_COLUMN_INDEX, ColumnType.TIMESTAMP),
                        ColumnType.TIMESTAMP), false));
    }

    @Test
    public void testLastNotNullTimestampSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastNotNullTimestampGroupByFunction(
                        TimestampColumn.newInstance(ARG_COLUMN_INDEX, ColumnType.TIMESTAMP),
                        ColumnType.TIMESTAMP), false));
    }

    @Test
    public void testLastTimestampFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastTimestampGroupByFunction(
                        TimestampColumn.newInstance(ARG_COLUMN_INDEX, ColumnType.TIMESTAMP),
                        ColumnType.TIMESTAMP), true));
    }

    @Test
    public void testLastTimestampIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastTimestampGroupByFunction(
                        new IndirectTimestampArg(ARG_COLUMN_INDEX, ColumnType.TIMESTAMP),
                        ColumnType.TIMESTAMP), false));
    }

    @Test
    public void testLastTimestampSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastTimestampGroupByFunction(
                        TimestampColumn.newInstance(ARG_COLUMN_INDEX, ColumnType.TIMESTAMP),
                        ColumnType.TIMESTAMP), false));
    }

    @Test
    public void testMaxTimestampFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MaxTimestampGroupByFunction(
                        TimestampColumn.newInstance(ARG_COLUMN_INDEX, ColumnType.TIMESTAMP),
                        ColumnType.TIMESTAMP), true));
    }

    @Test
    public void testMaxTimestampIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MaxTimestampGroupByFunction(
                        new IndirectTimestampArg(ARG_COLUMN_INDEX, ColumnType.TIMESTAMP),
                        ColumnType.TIMESTAMP), false));
    }

    @Test
    public void testMaxTimestampSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MaxTimestampGroupByFunction(
                        TimestampColumn.newInstance(ARG_COLUMN_INDEX, ColumnType.TIMESTAMP),
                        ColumnType.TIMESTAMP), false));
    }

    @Test
    public void testMinTimestampFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MinTimestampGroupByFunction(
                        TimestampColumn.newInstance(ARG_COLUMN_INDEX, ColumnType.TIMESTAMP),
                        ColumnType.TIMESTAMP), true));
    }

    @Test
    public void testMinTimestampIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MinTimestampGroupByFunction(
                        new IndirectTimestampArg(ARG_COLUMN_INDEX, ColumnType.TIMESTAMP),
                        ColumnType.TIMESTAMP), false));
    }

    @Test
    public void testMinTimestampSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MinTimestampGroupByFunction(
                        TimestampColumn.newInstance(ARG_COLUMN_INDEX, ColumnType.TIMESTAMP),
                        ColumnType.TIMESTAMP), false));
    }

    private static void testEquivalence(GroupByFunction function, boolean fastPath) {
        assertEquivalence(function, fastPath, Long.BYTES,
                allocArgBuffer(ARG_VALUES), (long) ARG_VALUES.length * Long.BYTES);
    }
}
