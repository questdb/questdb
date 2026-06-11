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
import io.questdb.griffin.engine.functions.columns.VarcharColumn;
import io.questdb.griffin.engine.functions.groupby.CountVarcharGroupByFunction;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.IndirectVarcharArg;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.allocArgBuffer;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.assertEquivalence;

/**
 * Asserts byte-for-byte equivalence of {@link CountVarcharGroupByFunction}'s
 * {@code computeKeyedBatch} override against the default implementation (loop
 * over {@code computeFirst} / {@code computeNext}). Varchar storage uses an
 * aux+data layout that does not admit a direct-column fast path, so the
 * override always routes through {@code arg.getVarcharSize(record)}; the
 * harness still exercises both {@code fastPath} toggle values for parity with
 * the other {@code *KeyedBatchTest} classes.
 */
public class VarcharGroupByFunctionKeyedBatchTest {
    private static final int ARG_COLUMN_INDEX = 0;
    // Per-row varchar size (TableUtils.NULL_LEN marks a null varchar).
    private static final int[] ARG_SIZES = {
            5, TableUtils.NULL_LEN, 0, 64, 1, TableUtils.NULL_LEN, 12, 256
    };

    @Test
    public void testCountVarcharFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountVarcharGroupByFunction(new VarcharColumn(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testCountVarcharIndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountVarcharGroupByFunction(new IndirectVarcharArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testCountVarcharSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountVarcharGroupByFunction(new VarcharColumn(ARG_COLUMN_INDEX)), false));
    }

    private static void testEquivalence(GroupByFunction function, boolean fastPath) {
        assertEquivalence(function, fastPath, Integer.BYTES,
                allocArgBuffer(ARG_SIZES), (long) ARG_SIZES.length * Integer.BYTES);
    }
}
