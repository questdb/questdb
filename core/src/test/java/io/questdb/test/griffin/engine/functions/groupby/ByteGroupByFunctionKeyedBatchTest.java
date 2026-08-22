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
import io.questdb.griffin.engine.functions.columns.ByteColumn;
import io.questdb.griffin.engine.functions.groupby.BitAndByteGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.BitOrByteGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.BitXorByteGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstByteGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastByteGroupByFunction;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.IndirectByteArg;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.allocArgBuffer;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.assertEquivalence;

/**
 * Asserts byte-for-byte equivalence of every {@code computeKeyedBatch} override
 * that reads from a {@code BYTE} column against the default implementation
 * (loop over {@code computeFirst} / {@code computeNext}). Covers both the
 * direct-column fast path ({@code argAddr != 0}) and the record-based slow
 * path ({@code argAddr == 0}).
 * <p>
 * BYTE has no NULL sentinel so the override loops do not branch on null values.
 * The test still crosses the {@code isNew} branch (present in
 * {@link BitAndByteGroupByFunction}) by priming some entries and leaving
 * others at the etalon.
 */
public class ByteGroupByFunctionKeyedBatchTest {
    private static final int ARG_COLUMN_INDEX = 0;
    // Bit-diverse values so AND/OR/XOR produce distinct per-entry results.
    private static final byte[] ARG_VALUES = {
            (byte) 0x55, (byte) 0xAA, (byte) 0x0F, (byte) 0xFF,
            (byte) 0x00, (byte) 0x7F, (byte) 0x80, (byte) 0x01
    };

    @Test
    public void testBitAndByteFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitAndByteGroupByFunction(ByteColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testBitAndByteIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitAndByteGroupByFunction(new IndirectByteArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testBitAndByteSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitAndByteGroupByFunction(ByteColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testBitOrByteFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitOrByteGroupByFunction(ByteColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testBitOrByteIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitOrByteGroupByFunction(new IndirectByteArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testBitOrByteSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitOrByteGroupByFunction(ByteColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testBitXorByteFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitXorByteGroupByFunction(ByteColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testBitXorByteIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitXorByteGroupByFunction(new IndirectByteArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testBitXorByteSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new BitXorByteGroupByFunction(ByteColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstByteFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstByteGroupByFunction(ByteColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testFirstByteIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstByteGroupByFunction(new IndirectByteArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstByteSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstByteGroupByFunction(ByteColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastByteFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastByteGroupByFunction(ByteColumn.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testLastByteIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastByteGroupByFunction(new IndirectByteArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastByteSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastByteGroupByFunction(ByteColumn.newInstance(ARG_COLUMN_INDEX)), false));
    }

    private static void testEquivalence(GroupByFunction function, boolean fastPath) {
        assertEquivalence(function, fastPath, Byte.BYTES,
                allocArgBuffer(ARG_VALUES), ARG_VALUES.length);
    }
}
