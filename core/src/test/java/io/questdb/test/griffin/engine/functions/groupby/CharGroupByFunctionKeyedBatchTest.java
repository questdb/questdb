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
import io.questdb.griffin.engine.functions.columns.CharColumn;
import io.questdb.griffin.engine.functions.groupby.FirstCharGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullCharGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastCharGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastNotNullCharGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MaxCharGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinCharGroupByFunction;
import io.questdb.std.Numbers;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.IndirectCharArg;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.allocArgBuffer;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.assertEquivalence;

/**
 * Asserts byte-for-byte equivalence of every {@code computeKeyedBatch} override
 * that reads from a {@code CHAR} column against the default implementation
 * (loop over {@code computeFirst} / {@code computeNext}). Covers both the
 * direct-column fast path ({@code argAddr != 0}) and the record-based slow
 * path ({@code argAddr == 0}). CHAR uses {@link Numbers#CHAR_NULL} (the
 * zero char) as the null sentinel.
 */
public class CharGroupByFunctionKeyedBatchTest {
    private static final int ARG_COLUMN_INDEX = 0;
    private static final char[] ARG_VALUES = {
            'A', Numbers.CHAR_NULL, 'Z', 'q', 'M', Numbers.CHAR_NULL, 'a', '5'
    };

    @Test
    public void testFirstCharFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstCharGroupByFunction(new CharColumn(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testFirstCharIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstCharGroupByFunction(new IndirectCharArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstCharSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstCharGroupByFunction(new CharColumn(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstNotNullCharFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstNotNullCharGroupByFunction(new CharColumn(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testFirstNotNullCharIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstNotNullCharGroupByFunction(new IndirectCharArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstNotNullCharSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstNotNullCharGroupByFunction(new CharColumn(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastCharFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastCharGroupByFunction(new CharColumn(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testLastCharIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastCharGroupByFunction(new IndirectCharArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastCharSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastCharGroupByFunction(new CharColumn(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastNotNullCharFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastNotNullCharGroupByFunction(new CharColumn(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testLastNotNullCharIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastNotNullCharGroupByFunction(new IndirectCharArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastNotNullCharSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastNotNullCharGroupByFunction(new CharColumn(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMaxCharFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MaxCharGroupByFunction(new CharColumn(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testMaxCharIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MaxCharGroupByFunction(new IndirectCharArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMaxCharSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MaxCharGroupByFunction(new CharColumn(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMinCharFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MinCharGroupByFunction(new CharColumn(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testMinCharIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MinCharGroupByFunction(new IndirectCharArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMinCharSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MinCharGroupByFunction(new CharColumn(ARG_COLUMN_INDEX)), false));
    }

    private static void testEquivalence(GroupByFunction function, boolean fastPath) {
        assertEquivalence(function, fastPath, Character.BYTES,
                allocArgBuffer(ARG_VALUES), (long) ARG_VALUES.length * Character.BYTES);
    }
}
