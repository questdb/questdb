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

import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.SymbolColumn;
import io.questdb.griffin.engine.functions.groupby.CountSymbolGroupByFunction;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.IndirectIntArg;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.allocArgBuffer;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.assertEquivalence;

/**
 * Asserts byte-for-byte equivalence of {@link CountSymbolGroupByFunction}'s
 * {@code computeKeyedBatch} override against the default implementation (loop
 * over {@code computeFirst} / {@code computeNext}). Covers both the
 * direct-column fast path ({@code argAddr != 0}) and the record-based slow
 * path ({@code argAddr == 0}). Symbols are stored as 4-byte ids using
 * {@link SymbolTable#VALUE_IS_NULL} as the null sentinel. The indirect-arg
 * variant uses an {@code IntFunction} so that the
 * {@link io.questdb.griffin.engine.groupby.GroupByUtils#directArgColumnIndex}
 * type check fails and {@code argColumnIndex} is set to {@code -1}.
 */
public class SymbolGroupByFunctionKeyedBatchTest {
    private static final int ARG_COLUMN_INDEX = 0;
    private static final int[] ARG_VALUES = {
            1, SymbolTable.VALUE_IS_NULL, 2, 3, 0, SymbolTable.VALUE_IS_NULL, 5, 7
    };

    @Test
    public void testCountSymbolFastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountSymbolGroupByFunction(new SymbolColumn(ARG_COLUMN_INDEX, true)), true));
    }

    @Test
    public void testCountSymbolIndirectArg() throws Exception {
        // IntFunction arg fails the SYMBOL type check in directArgColumnIndex, so
        // argColumnIndex is -1 and the override falls straight into the else branch.
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountSymbolGroupByFunction(new IndirectIntArg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testCountSymbolSlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountSymbolGroupByFunction(new SymbolColumn(ARG_COLUMN_INDEX, true)), false));
    }

    private static void testEquivalence(GroupByFunction function, boolean fastPath) {
        assertEquivalence(function, fastPath, Integer.BYTES,
                allocArgBuffer(ARG_VALUES), (long) ARG_VALUES.length * Integer.BYTES);
    }
}
