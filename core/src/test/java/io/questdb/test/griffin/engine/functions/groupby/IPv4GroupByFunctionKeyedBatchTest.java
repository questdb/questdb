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

import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.IPv4Column;
import io.questdb.griffin.engine.functions.groupby.CountIPv4GroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstIPv4GroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullIPv4GroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastIPv4GroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastNotNullIPv4GroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.MaxIPv4GroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinIPv4GroupByFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.IndirectIPv4Arg;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.allocArgBuffer;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.assertEquivalence;

/**
 * Asserts byte-for-byte equivalence of every {@code computeKeyedBatch} override
 * that reads from an {@code IPv4} column against the default implementation
 * (loop over {@code computeFirst} / {@code computeNext}). Covers both the
 * direct-column fast path ({@code argAddr != 0}) and the record-based slow
 * path ({@code argAddr == 0}). IPv4 is a 4-byte unsigned integer using
 * {@link Numbers#IPv4_NULL} (zero) as the null sentinel; comparisons go
 * through {@link Numbers#ipv4ToLong} for unsigned semantics.
 */
public class IPv4GroupByFunctionKeyedBatchTest {
    private static final int ARG_COLUMN_INDEX = 0;
    // Mix null (0), low, mid and high-bit values to cross unsigned comparison boundaries.
    private static final int[] ARG_VALUES = {
            0x0A_00_00_01, Numbers.IPv4_NULL, 0xFF_FF_FF_FF, 0x7F_00_00_01,
            0x80_00_00_00, Numbers.IPv4_NULL, 0x01_02_03_04, 0xC0_A8_00_01
    };
    // The not-null aggregators need the nulls placed differently from ARG_VALUES. The shared
    // fixture primes entries from rows {0, 2, 4, 6} and then replays rows {3, 1, 6, 5} at higher
    // rowIds. Under ARG_VALUES no primed entry ends up holding a NULL, so the branch that
    // overwrites a stored NULL with a later non-null value is never taken on a primed entry.
    //
    // Null at row 2 primes entry 1 with (rowId, NULL); entry 1's test row 1 is non-null at a
    // higher rowId, so first_not_null must replace the stored NULL - a plain "first" keeps it.
    // Null at row 3 lands on entry 0, primed non-null; a plain "last" overwrites it with the NULL
    // because the test rowId is higher, while last_not_null must keep the non-null value.
    private static final int[] NOT_NULL_ARG_VALUES = {
            0x0A_00_00_01, 0x7F_00_00_01, Numbers.IPv4_NULL, Numbers.IPv4_NULL,
            0xFF_FF_FF_FF, 0xC0_A8_00_01, 0x80_00_00_00, 0x01_02_03_04
    };

    @Test
    public void testCountIPv4FastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountIPv4GroupByFunction(IPv4Column.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testCountIPv4IndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountIPv4GroupByFunction(new IndirectIPv4Arg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testCountIPv4SlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new CountIPv4GroupByFunction(IPv4Column.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstIPv4FastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstIPv4GroupByFunction(IPv4Column.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testFirstIPv4IndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstIPv4GroupByFunction(new IndirectIPv4Arg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstIPv4SlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new FirstIPv4GroupByFunction(IPv4Column.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstNotNullIPv4FastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testNotNullEquivalence(
                newFunction(new FirstNotNullIPv4GroupByFunctionFactory(), IPv4Column.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testFirstNotNullIPv4IndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testNotNullEquivalence(
                newFunction(new FirstNotNullIPv4GroupByFunctionFactory(), new IndirectIPv4Arg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testFirstNotNullIPv4SlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testNotNullEquivalence(
                newFunction(new FirstNotNullIPv4GroupByFunctionFactory(), IPv4Column.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastIPv4FastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastIPv4GroupByFunction(IPv4Column.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testLastIPv4IndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastIPv4GroupByFunction(new IndirectIPv4Arg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastIPv4SlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new LastIPv4GroupByFunction(IPv4Column.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastNotNullIPv4FastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testNotNullEquivalence(
                newFunction(new LastNotNullIPv4GroupByFunctionFactory(), IPv4Column.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testLastNotNullIPv4IndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testNotNullEquivalence(
                newFunction(new LastNotNullIPv4GroupByFunctionFactory(), new IndirectIPv4Arg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testLastNotNullIPv4SlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testNotNullEquivalence(
                newFunction(new LastNotNullIPv4GroupByFunctionFactory(), IPv4Column.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMaxIPv4FastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MaxIPv4GroupByFunction(IPv4Column.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testMaxIPv4IndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MaxIPv4GroupByFunction(new IndirectIPv4Arg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMaxIPv4SlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MaxIPv4GroupByFunction(IPv4Column.newInstance(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMinIPv4FastPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MinIPv4GroupByFunction(IPv4Column.newInstance(ARG_COLUMN_INDEX)), true));
    }

    @Test
    public void testMinIPv4IndirectArg() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MinIPv4GroupByFunction(new IndirectIPv4Arg(ARG_COLUMN_INDEX)), false));
    }

    @Test
    public void testMinIPv4SlowPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> testEquivalence(
                new MinIPv4GroupByFunction(IPv4Column.newInstance(ARG_COLUMN_INDEX)), false));
    }

    // The not-null aggregators are private nested classes of their factories, so the factory is the
    // only way to reach them. newInstance() ignores the configuration and the execution context.
    private static GroupByFunction newFunction(FunctionFactory factory, Function arg) throws SqlException {
        final ObjList<Function> args = new ObjList<>();
        args.add(arg);
        final IntList argPositions = new IntList();
        argPositions.add(0);
        return (GroupByFunction) factory.newInstance(0, args, argPositions, null, null);
    }

    private static void testEquivalence(GroupByFunction function, boolean fastPath) {
        assertEquivalence(function, fastPath, Integer.BYTES,
                allocArgBuffer(ARG_VALUES), (long) ARG_VALUES.length * Integer.BYTES);
    }

    private static void testNotNullEquivalence(GroupByFunction function, boolean fastPath) {
        assertEquivalence(function, fastPath, Integer.BYTES,
                allocArgBuffer(NOT_NULL_ARG_VALUES), (long) NOT_NULL_ARG_VALUES.length * Integer.BYTES);
    }
}
