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
import io.questdb.griffin.engine.functions.columns.BooleanColumn;
import io.questdb.griffin.engine.functions.groupby.FirstBooleanGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastBooleanGroupByFunction;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.IndirectBoolArg;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.allocArgBuffer;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.assertEquivalence;

/**
 * Asserts byte-for-byte equivalence of every {@code computeKeyedBatch} override
 * that reads from a {@code BOOLEAN} column against the default implementation
 * (loop over {@code computeFirst} / {@code computeNext}). Covers both the
 * direct-column fast path ({@code argAddr != 0}) and the record-based slow
 * path ({@code argAddr == 0}). BOOLEAN has no NULL sentinel; the value is
 * stored as a 1-byte 0/1 flag.
 */
public class BooleanGroupByFunctionKeyedBatchTest {
    private static final int ARG_COLUMN_INDEX = 0;
    private static final byte[] ARG_VALUES_ALL_FALSE = {
            (byte) 0, (byte) 0, (byte) 0, (byte) 0,
            (byte) 0, (byte) 0, (byte) 0, (byte) 0
    };
    private static final byte[] ARG_VALUES_ALL_TRUE = {
            (byte) 1, (byte) 1, (byte) 1, (byte) 1,
            (byte) 1, (byte) 1, (byte) 1, (byte) 1
    };
    // Alternate true/false to keep bit-level value diversity across the batch.
    private static final byte[] ARG_VALUES_MIXED = {
            (byte) 1, (byte) 0, (byte) 1, (byte) 0,
            (byte) 0, (byte) 1, (byte) 1, (byte) 0
    };

    @Test
    public void testFirstBooleanAllFalse() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstBooleanGroupByFunction(BooleanColumn.newInstance(ARG_COLUMN_INDEX)), false, ARG_VALUES_ALL_FALSE));
    }

    @Test
    public void testFirstBooleanAllTrue() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstBooleanGroupByFunction(BooleanColumn.newInstance(ARG_COLUMN_INDEX)), false, ARG_VALUES_ALL_TRUE));
    }

    @Test
    public void testFirstBooleanFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstBooleanGroupByFunction(BooleanColumn.newInstance(ARG_COLUMN_INDEX)), true, ARG_VALUES_MIXED));
    }

    @Test
    public void testFirstBooleanIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstBooleanGroupByFunction(new IndirectBoolArg(ARG_COLUMN_INDEX)), false, ARG_VALUES_MIXED));
    }

    @Test
    public void testFirstBooleanSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstBooleanGroupByFunction(BooleanColumn.newInstance(ARG_COLUMN_INDEX)), false, ARG_VALUES_MIXED));
    }

    @Test
    public void testLastBooleanAllFalse() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastBooleanGroupByFunction(BooleanColumn.newInstance(ARG_COLUMN_INDEX)), false, ARG_VALUES_ALL_FALSE));
    }

    @Test
    public void testLastBooleanAllTrue() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastBooleanGroupByFunction(BooleanColumn.newInstance(ARG_COLUMN_INDEX)), false, ARG_VALUES_ALL_TRUE));
    }

    @Test
    public void testLastBooleanFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastBooleanGroupByFunction(BooleanColumn.newInstance(ARG_COLUMN_INDEX)), true, ARG_VALUES_MIXED));
    }

    @Test
    public void testLastBooleanIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastBooleanGroupByFunction(new IndirectBoolArg(ARG_COLUMN_INDEX)), false, ARG_VALUES_MIXED));
    }

    @Test
    public void testLastBooleanSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastBooleanGroupByFunction(BooleanColumn.newInstance(ARG_COLUMN_INDEX)), false, ARG_VALUES_MIXED));
    }

    private static void testEquivalence(GroupByFunction function, boolean fastPath, byte[] argValues) {
        assertEquivalence(function, fastPath, Byte.BYTES,
                allocArgBuffer(argValues), argValues.length);
    }
}
