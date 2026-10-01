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

import io.questdb.cairo.TableUtils;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.StrColumn;
import io.questdb.griffin.engine.functions.groupby.CountStrGroupByFunction;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.IndirectStrArg;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.allocArgBuffer;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.assertEquivalence;

/**
 * Asserts byte-for-byte equivalence of {@link CountStrGroupByFunction}'s
 * {@code computeKeyedBatch} override against the default implementation (loop
 * over {@code computeFirst} / {@code computeNext}). String storage uses an
 * aux+data layout that does not admit a direct-column fast path, so the
 * override always routes through {@code arg.getStrLen(record)}; the harness
 * still exercises both {@code fastPath} toggle values for parity with the
 * other {@code *KeyedBatchTest} classes.
 */
public class StrGroupByFunctionKeyedBatchTest {
    private static final int ARG_COLUMN_INDEX = 0;
    // Per-row string length (TableUtils.NULL_LEN marks a null string).
    private static final int[] ARG_LENGTHS = {
            3, TableUtils.NULL_LEN, 0, 42, 1, TableUtils.NULL_LEN, 7, 128
    };

    @Test
    public void testCountStrFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountStrGroupByFunction(new StrColumn(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testCountStrIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountStrGroupByFunction(new IndirectStrArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testCountStrSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountStrGroupByFunction(new StrColumn(ARG_COLUMN_INDEX)), false));
    }

    private static void testEquivalence(GroupByFunction function, boolean fastPath) {
        assertEquivalence(function, fastPath, Integer.BYTES,
                allocArgBuffer(ARG_LENGTHS), (long) ARG_LENGTHS.length * Integer.BYTES);
    }
}
