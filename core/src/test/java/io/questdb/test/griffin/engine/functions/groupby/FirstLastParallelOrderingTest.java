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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.CharFunction;
import io.questdb.griffin.engine.functions.DateFunction;
import io.questdb.griffin.engine.functions.DecimalFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.FloatFunction;
import io.questdb.griffin.engine.functions.GeoByteFunction;
import io.questdb.griffin.engine.functions.GeoIntFunction;
import io.questdb.griffin.engine.functions.GeoLongFunction;
import io.questdb.griffin.engine.functions.GeoShortFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.IPv4Function;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UuidFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.griffin.engine.functions.groupby.FirstDecimalGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.FirstGeoHashGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullCharGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullDateGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullDecimalGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullDoubleGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullFloatGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullGeoHashGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullIPv4GroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullIntGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullLongGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullStrGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullSymbolGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullTimestampGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullUuidGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullVarcharGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastDecimalGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastGeoHashGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastNotNullCharGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastNotNullDateGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastNotNullDecimalGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastNotNullDoubleGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastNotNullFloatGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastNotNullGeoHashGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastNotNullIPv4GroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastNotNullIntGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastNotNullLongGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastNotNullStrGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastNotNullSymbolGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastNotNullTimestampGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastNotNullUuidGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastNotNullVarcharGroupByFunctionFactory;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Deterministic regression test for the parallel GROUP BY first/last ordering bug.
 * <p>
 * Parallel keyed GROUP BY reduces page frames into per-worker "slot" maps. Slot-to-worker
 * affinity is not stable (see {@code PerWorkerLocks.acquireSlot}), so a single slot can
 * receive a higher-rowId frame's rows before a lower-rowId frame's rows. The order-sensitive
 * aggregates first/last/first_not_null/last_not_null must therefore pick the winning row by
 * comparing rowId in {@code computeNext}, never by relying on the order rows are processed.
 * <p>
 * Each test drives {@code computeFirst}/{@code computeNext} directly with rowIds in
 * descending order (so the row a non-guarded implementation would pick is the wrong one),
 * then asserts the stored rowId matches the minimum (first) or maximum (last) rowId among
 * the qualifying rows. The assertions check {@code getLong(valueIndex)}, the rowId the
 * aggregate committed, which is the definitive proof of which row won.
 * <p>
 * Before the fix these tests fail: empty / unconditional / null-only {@code computeNext}
 * implementations keep the first-processed (highest-rowId) row for first(), and the
 * last-processed (lowest-rowId) row for last().
 */
public class FirstLastParallelOrderingTest extends AbstractCairoTest {

    // Descending rowIds: the first one processed (100) is never the first()/last() winner,
    // so an order-dependent implementation visibly picks the wrong row.
    private static final long[] ROW_IDS = {100, 80, 60, 40, 20};

    @Test
    public void testFirstDecimal128PicksMinRowId() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL128, 30, 5));
        assertWinnerRowId(
                build(new FirstDecimalGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> arg.set(10L + i),
                20
        );
    }

    @Test
    public void testFirstDecimal16PicksMinRowId() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL16, 4, 1));
        assertWinnerRowId(
                build(new FirstDecimalGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> arg.set(10L + i),
                20
        );
    }

    @Test
    public void testFirstDecimal256PicksMinRowId() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL256, 50, 5));
        assertWinnerRowId(
                build(new FirstDecimalGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> arg.set(10L + i),
                20
        );
    }

    @Test
    public void testFirstDecimal32PicksMinRowId() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL32, 9, 2));
        assertWinnerRowId(
                build(new FirstDecimalGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> arg.set(10L + i),
                20
        );
    }

    @Test
    public void testFirstDecimal64PicksMinRowId() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL64, 18, 3));
        assertWinnerRowId(
                build(new FirstDecimalGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> arg.set(10L + i),
                20
        );
    }

    @Test
    public void testFirstDecimal8PicksMinRowId() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL8, 2, 0));
        assertWinnerRowId(
                build(new FirstDecimalGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> arg.set(10L + i),
                20
        );
    }

    @Test
    public void testFirstGeoBytePicksMinRowId() throws Exception {
        GeoByteArg arg = new GeoByteArg(ColumnType.getGeoHashTypeWithBits(5));
        assertWinnerRowId(
                build(new FirstGeoHashGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> arg.set((byte) (1 + i)),
                20
        );
    }

    @Test
    public void testFirstGeoIntPicksMinRowId() throws Exception {
        GeoIntArg arg = new GeoIntArg(ColumnType.getGeoHashTypeWithBits(20));
        assertWinnerRowId(
                build(new FirstGeoHashGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> arg.set(1 + i),
                20
        );
    }

    @Test
    public void testFirstGeoLongPicksMinRowId() throws Exception {
        GeoLongArg arg = new GeoLongArg(ColumnType.getGeoHashTypeWithBits(40));
        assertWinnerRowId(
                build(new FirstGeoHashGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> arg.set(1L + i),
                20
        );
    }

    @Test
    public void testFirstGeoShortPicksMinRowId() throws Exception {
        GeoShortArg arg = new GeoShortArg(ColumnType.getGeoHashTypeWithBits(10));
        assertWinnerRowId(
                build(new FirstGeoHashGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> arg.set((short) (1 + i)),
                20
        );
    }

    @Test
    public void testFirstNotNullCharMergeKeepsNonNullOverNullDest() throws Exception {
        CharArg arg = new CharArg();
        assertFirstNotNullMergeKeepsNonNull(
                build(new FirstNotNullCharGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set('a')
        );
    }

    @Test
    public void testFirstNotNullDateMergeKeepsNonNullOverNullDest() throws Exception {
        DateArg arg = new DateArg();
        assertFirstNotNullMergeKeepsNonNull(
                build(new FirstNotNullDateGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(42L)
        );
    }

    @Test
    public void testFirstNotNullDecimal128MergeKeepsNonNullOverNullDest() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL128, 30, 5));
        assertFirstNotNullMergeKeepsNonNull(
                build(new FirstNotNullDecimalGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(42L)
        );
    }

    @Test
    public void testFirstNotNullDecimal128SkipsNullsAndPicksMinNonNullRowId() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL128, 30, 5));
        // nulls at the lowest rowIds (indices 3, 4 -> rowIds 40, 20); min non-null rowId is 60.
        assertWinnerRowId(
                build(new FirstNotNullDecimalGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> {
                    if (i >= 3) {
                        arg.setNull();
                    } else {
                        arg.set(10L + i);
                    }
                },
                60
        );
    }

    @Test
    public void testFirstNotNullDecimal16MergeKeepsNonNullOverNullDest() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL16, 4, 1));
        assertFirstNotNullMergeKeepsNonNull(
                build(new FirstNotNullDecimalGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(42L)
        );
    }

    @Test
    public void testFirstNotNullDecimal16SkipsNullsAndPicksMinNonNullRowId() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL16, 4, 1));
        // nulls at indices 0 and 2 (rowIds 100, 60); non-null at 80, 40, 20; min non-null is 20.
        assertWinnerRowId(
                build(new FirstNotNullDecimalGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> {
                    if (i == 0 || i == 2) {
                        arg.setNull();
                    } else {
                        arg.set(10L + i);
                    }
                },
                20
        );
    }

    @Test
    public void testFirstNotNullDecimal256MergeKeepsNonNullOverNullDest() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL256, 50, 5));
        assertFirstNotNullMergeKeepsNonNull(
                build(new FirstNotNullDecimalGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(42L)
        );
    }

    @Test
    public void testFirstNotNullDecimal256SkipsNullsAndPicksMinNonNullRowId() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL256, 50, 5));
        // nulls at indices 0 and 2 (rowIds 100, 60); non-null at 80, 40, 20; min non-null is 20.
        assertWinnerRowId(
                build(new FirstNotNullDecimalGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> {
                    if (i == 0 || i == 2) {
                        arg.setNull();
                    } else {
                        arg.set(10L + i);
                    }
                },
                20
        );
    }

    @Test
    public void testFirstNotNullDecimal32MergeKeepsNonNullOverNullDest() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL32, 9, 2));
        assertFirstNotNullMergeKeepsNonNull(
                build(new FirstNotNullDecimalGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(42L)
        );
    }

    @Test
    public void testFirstNotNullDecimal32SkipsNullsAndPicksMinNonNullRowId() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL32, 9, 2));
        // nulls at indices 0 and 2 (rowIds 100, 60); non-null at 80, 40, 20; min non-null is 20.
        assertWinnerRowId(
                build(new FirstNotNullDecimalGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> {
                    if (i == 0 || i == 2) {
                        arg.setNull();
                    } else {
                        arg.set(10L + i);
                    }
                },
                20
        );
    }

    @Test
    public void testFirstNotNullDecimal64MergeKeepsNonNullOverNullDest() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL64, 18, 3));
        assertFirstNotNullMergeKeepsNonNull(
                build(new FirstNotNullDecimalGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(42L)
        );
    }

    @Test
    public void testFirstNotNullDecimal64SkipsNullsAndPicksMinNonNullRowId() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL64, 18, 3));
        // nulls at indices 0 and 2 (rowIds 100, 60); non-null at 80, 40, 20; min non-null is 20.
        assertWinnerRowId(
                build(new FirstNotNullDecimalGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> {
                    if (i == 0 || i == 2) {
                        arg.setNull();
                    } else {
                        arg.set(10L + i);
                    }
                },
                20
        );
    }

    @Test
    public void testFirstNotNullDecimal8MergeKeepsNonNullOverNullDest() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL8, 2, 0));
        assertFirstNotNullMergeKeepsNonNull(
                build(new FirstNotNullDecimalGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(42L)
        );
    }

    @Test
    public void testFirstNotNullDecimal8SkipsNullsAndPicksMinNonNullRowId() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL8, 2, 0));
        // nulls at indices 0 and 2 (rowIds 100, 60); non-null at 80, 40, 20; min non-null is 20.
        assertWinnerRowId(
                build(new FirstNotNullDecimalGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> {
                    if (i == 0 || i == 2) {
                        arg.setNull();
                    } else {
                        arg.set(10L + i);
                    }
                },
                20
        );
    }

    @Test
    public void testFirstNotNullDoubleMergeKeepsNonNullOverNullDest() throws Exception {
        DoubleArg arg = new DoubleArg();
        assertFirstNotNullMergeKeepsNonNull(
                build(new FirstNotNullDoubleGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(1.5)
        );
    }

    @Test
    public void testFirstNotNullFloatMergeKeepsNonNullOverNullDest() throws Exception {
        FloatArg arg = new FloatArg();
        assertFirstNotNullMergeKeepsNonNull(
                build(new FirstNotNullFloatGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(1.5f)
        );
    }

    @Test
    public void testFirstNotNullGeoByteMergeKeepsNonNullOverNullDest() throws Exception {
        GeoByteArg arg = new GeoByteArg(ColumnType.getGeoHashTypeWithBits(5));
        assertFirstNotNullMergeKeepsNonNull(
                build(new FirstNotNullGeoHashGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set((byte) 1)
        );
    }

    @Test
    public void testFirstNotNullGeoByteSkipsNullsAndPicksMinNonNullRowId() throws Exception {
        GeoByteArg arg = new GeoByteArg(ColumnType.getGeoHashTypeWithBits(5));
        // nulls at indices 0 and 2 (rowIds 100, 60); non-null at 80, 40, 20; min non-null is 20.
        assertWinnerRowId(
                build(new FirstNotNullGeoHashGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> {
                    if (i == 0 || i == 2) {
                        arg.setNull();
                    } else {
                        arg.set((byte) (1 + i));
                    }
                },
                20
        );
    }

    @Test
    public void testFirstNotNullGeoIntMergeKeepsNonNullOverNullDest() throws Exception {
        GeoIntArg arg = new GeoIntArg(ColumnType.getGeoHashTypeWithBits(20));
        assertFirstNotNullMergeKeepsNonNull(
                build(new FirstNotNullGeoHashGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(1)
        );
    }

    @Test
    public void testFirstNotNullGeoIntSkipsNullsAndPicksMinNonNullRowId() throws Exception {
        GeoIntArg arg = new GeoIntArg(ColumnType.getGeoHashTypeWithBits(20));
        // nulls at indices 0 and 2 (rowIds 100, 60); non-null at 80, 40, 20; min non-null is 20.
        assertWinnerRowId(
                build(new FirstNotNullGeoHashGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> {
                    if (i == 0 || i == 2) {
                        arg.setNull();
                    } else {
                        arg.set(1 + i);
                    }
                },
                20
        );
    }

    @Test
    public void testFirstNotNullGeoLongMergeKeepsNonNullOverNullDest() throws Exception {
        GeoLongArg arg = new GeoLongArg(ColumnType.getGeoHashTypeWithBits(40));
        assertFirstNotNullMergeKeepsNonNull(
                build(new FirstNotNullGeoHashGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(42L)
        );
    }

    @Test
    public void testFirstNotNullGeoLongSkipsNullsAndPicksMinNonNullRowId() throws Exception {
        GeoLongArg arg = new GeoLongArg(ColumnType.getGeoHashTypeWithBits(40));
        // nulls at indices 0 and 2 (rowIds 100, 60); non-null at 80, 40, 20; min non-null is 20.
        assertWinnerRowId(
                build(new FirstNotNullGeoHashGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> {
                    if (i == 0 || i == 2) {
                        arg.setNull();
                    } else {
                        arg.set(1L + i);
                    }
                },
                20
        );
    }

    @Test
    public void testFirstNotNullGeoShortMergeKeepsNonNullOverNullDest() throws Exception {
        GeoShortArg arg = new GeoShortArg(ColumnType.getGeoHashTypeWithBits(10));
        assertFirstNotNullMergeKeepsNonNull(
                build(new FirstNotNullGeoHashGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set((short) 1)
        );
    }

    @Test
    public void testFirstNotNullGeoShortSkipsNullsAndPicksMinNonNullRowId() throws Exception {
        GeoShortArg arg = new GeoShortArg(ColumnType.getGeoHashTypeWithBits(10));
        // nulls at indices 0 and 2 (rowIds 100, 60); non-null at 80, 40, 20; min non-null is 20.
        assertWinnerRowId(
                build(new FirstNotNullGeoHashGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> {
                    if (i == 0 || i == 2) {
                        arg.setNull();
                    } else {
                        arg.set((short) (1 + i));
                    }
                },
                20
        );
    }

    @Test
    public void testFirstNotNullIPv4MergeKeepsNonNullOverNullDest() throws Exception {
        IPv4Arg arg = new IPv4Arg();
        assertFirstNotNullMergeKeepsNonNull(
                build(new FirstNotNullIPv4GroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(0x0A000001)
        );
    }

    @Test
    public void testFirstNotNullIPv4SkipsNullsAndPicksMinNonNullRowId() throws Exception {
        IPv4Arg arg = new IPv4Arg();
        // nulls at indices 0 and 2 (rowIds 100, 60); non-null at 80, 40, 20; min non-null is 20.
        assertWinnerRowId(
                build(new FirstNotNullIPv4GroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> {
                    if (i == 0 || i == 2) {
                        arg.setNull();
                    } else {
                        arg.set(0x0A000001 + i);
                    }
                },
                20
        );
    }

    @Test
    public void testFirstNotNullIntMergeKeepsNonNullOverNullDest() throws Exception {
        IntArg arg = new IntArg();
        assertFirstNotNullMergeKeepsNonNull(
                build(new FirstNotNullIntGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(42)
        );
    }

    @Test
    public void testFirstNotNullLongMergeKeepsNonNullOverNullDest() throws Exception {
        LongArg arg = new LongArg();
        assertFirstNotNullMergeKeepsNonNull(
                build(new FirstNotNullLongGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(42L)
        );
    }

    @Test
    public void testFirstNotNullStrMergeKeepsNonNullOverNullDest() throws Exception {
        StrArg arg = new StrArg();
        assertFirstNotNullMergeKeepsNonNull(
                build(new FirstNotNullStrGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set("abc")
        );
    }

    @Test
    public void testFirstNotNullSymbolMergeKeepsNonNullOverNullDest() throws Exception {
        SymbolArg arg = new SymbolArg();
        assertFirstNotNullMergeKeepsNonNull(
                build(new FirstNotNullSymbolGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(5)
        );
    }

    @Test
    public void testFirstNotNullTimestampMergeKeepsNonNullOverNullDest() throws Exception {
        TimestampArg arg = new TimestampArg(ColumnType.TIMESTAMP);
        assertFirstNotNullMergeKeepsNonNull(
                build(new FirstNotNullTimestampGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(42L)
        );
    }

    @Test
    public void testFirstNotNullUuidMergeKeepsNonNullOverNullDest() throws Exception {
        UuidArg arg = new UuidArg();
        assertFirstNotNullMergeKeepsNonNull(
                build(new FirstNotNullUuidGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(42L)
        );
    }

    @Test
    public void testFirstNotNullVarcharMergeKeepsNonNullOverNullDest() throws Exception {
        VarcharArg arg = new VarcharArg();
        assertFirstNotNullMergeKeepsNonNull(
                build(new FirstNotNullVarcharGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set("abc")
        );
    }

    @Test
    public void testLastDecimal128PicksMaxRowId() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL128, 30, 5));
        assertWinnerRowId(
                build(new LastDecimalGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> arg.set(10L + i),
                100
        );
    }

    @Test
    public void testLastDecimal16PicksMaxRowId() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL16, 4, 1));
        assertWinnerRowId(
                build(new LastDecimalGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> arg.set(10L + i),
                100
        );
    }

    @Test
    public void testLastDecimal256PicksMaxRowId() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL256, 50, 5));
        assertWinnerRowId(
                build(new LastDecimalGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> arg.set(10L + i),
                100
        );
    }

    @Test
    public void testLastDecimal32PicksMaxRowId() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL32, 9, 2));
        assertWinnerRowId(
                build(new LastDecimalGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> arg.set(10L + i),
                100
        );
    }

    @Test
    public void testLastDecimal64PicksMaxRowId() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL64, 18, 3));
        assertWinnerRowId(
                build(new LastDecimalGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> arg.set(10L + i),
                100
        );
    }

    @Test
    public void testLastDecimal8PicksMaxRowId() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL8, 2, 0));
        assertWinnerRowId(
                build(new LastDecimalGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> arg.set(10L + i),
                100
        );
    }

    @Test
    public void testLastGeoBytePicksMaxRowId() throws Exception {
        GeoByteArg arg = new GeoByteArg(ColumnType.getGeoHashTypeWithBits(5));
        assertWinnerRowId(
                build(new LastGeoHashGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> arg.set((byte) (1 + i)),
                100
        );
    }

    @Test
    public void testLastGeoIntPicksMaxRowId() throws Exception {
        GeoIntArg arg = new GeoIntArg(ColumnType.getGeoHashTypeWithBits(20));
        assertWinnerRowId(
                build(new LastGeoHashGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> arg.set(1 + i),
                100
        );
    }

    @Test
    public void testLastGeoLongPicksMaxRowId() throws Exception {
        GeoLongArg arg = new GeoLongArg(ColumnType.getGeoHashTypeWithBits(40));
        assertWinnerRowId(
                build(new LastGeoHashGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> arg.set(1L + i),
                100
        );
    }

    @Test
    public void testLastGeoShortPicksMaxRowId() throws Exception {
        GeoShortArg arg = new GeoShortArg(ColumnType.getGeoHashTypeWithBits(10));
        assertWinnerRowId(
                build(new LastGeoHashGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> arg.set((short) (1 + i)),
                100
        );
    }

    @Test
    public void testLastNotNullCharMergeKeepsNonNullOverNullDest() throws Exception {
        CharArg arg = new CharArg();
        assertLastNotNullMergeKeepsNonNull(
                build(new LastNotNullCharGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set('a')
        );
    }

    @Test
    public void testLastNotNullDateMergeKeepsNonNullOverNullDest() throws Exception {
        DateArg arg = new DateArg();
        assertLastNotNullMergeKeepsNonNull(
                build(new LastNotNullDateGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(42L)
        );
    }

    @Test
    public void testLastNotNullDecimal128MergeKeepsNonNullOverNullDest() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL128, 30, 5));
        assertLastNotNullMergeKeepsNonNull(
                build(new LastNotNullDecimalGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(42L)
        );
    }

    @Test
    public void testLastNotNullDecimal128SkipsNullsAndPicksMaxNonNullRowId() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL128, 30, 5));
        // null at the highest rowId (index 0 -> rowId 100); max non-null rowId is 80.
        assertWinnerRowId(
                build(new LastNotNullDecimalGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> {
                    if (i == 0) {
                        arg.setNull();
                    } else {
                        arg.set(10L + i);
                    }
                },
                80
        );
    }

    @Test
    public void testLastNotNullDecimal16MergeKeepsNonNullOverNullDest() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL16, 4, 1));
        assertLastNotNullMergeKeepsNonNull(
                build(new LastNotNullDecimalGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(42L)
        );
    }

    @Test
    public void testLastNotNullDecimal16SkipsNullsAndPicksMaxNonNullRowId() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL16, 4, 1));
        // null at the highest rowId (index 0 -> rowId 100); max non-null rowId is 80.
        assertWinnerRowId(
                build(new LastNotNullDecimalGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> {
                    if (i == 0) {
                        arg.setNull();
                    } else {
                        arg.set(10L + i);
                    }
                },
                80
        );
    }

    @Test
    public void testLastNotNullDecimal256MergeKeepsNonNullOverNullDest() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL256, 50, 5));
        assertLastNotNullMergeKeepsNonNull(
                build(new LastNotNullDecimalGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(42L)
        );
    }

    @Test
    public void testLastNotNullDecimal256SkipsNullsAndPicksMaxNonNullRowId() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL256, 50, 5));
        // null at the highest rowId (index 0 -> rowId 100); max non-null rowId is 80.
        assertWinnerRowId(
                build(new LastNotNullDecimalGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> {
                    if (i == 0) {
                        arg.setNull();
                    } else {
                        arg.set(10L + i);
                    }
                },
                80
        );
    }

    @Test
    public void testLastNotNullDecimal32MergeKeepsNonNullOverNullDest() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL32, 9, 2));
        assertLastNotNullMergeKeepsNonNull(
                build(new LastNotNullDecimalGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(42L)
        );
    }

    @Test
    public void testLastNotNullDecimal32SkipsNullsAndPicksMaxNonNullRowId() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL32, 9, 2));
        // null at the highest rowId (index 0 -> rowId 100); max non-null rowId is 80.
        assertWinnerRowId(
                build(new LastNotNullDecimalGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> {
                    if (i == 0) {
                        arg.setNull();
                    } else {
                        arg.set(10L + i);
                    }
                },
                80
        );
    }

    @Test
    public void testLastNotNullDecimal64MergeKeepsNonNullOverNullDest() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL64, 18, 3));
        assertLastNotNullMergeKeepsNonNull(
                build(new LastNotNullDecimalGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(42L)
        );
    }

    @Test
    public void testLastNotNullDecimal64SkipsNullsAndPicksMaxNonNullRowId() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL64, 18, 3));
        // null at the highest rowId (index 0 -> rowId 100); max non-null rowId is 80.
        assertWinnerRowId(
                build(new LastNotNullDecimalGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> {
                    if (i == 0) {
                        arg.setNull();
                    } else {
                        arg.set(10L + i);
                    }
                },
                80
        );
    }

    @Test
    public void testLastNotNullDecimal8MergeKeepsNonNullOverNullDest() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL8, 2, 0));
        assertLastNotNullMergeKeepsNonNull(
                build(new LastNotNullDecimalGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(42L)
        );
    }

    @Test
    public void testLastNotNullDecimal8SkipsNullsAndPicksMaxNonNullRowId() throws Exception {
        DecimalArg arg = new DecimalArg(ColumnType.getDecimalType(ColumnType.DECIMAL8, 2, 0));
        // null at the highest rowId (index 0 -> rowId 100); max non-null rowId is 80.
        assertWinnerRowId(
                build(new LastNotNullDecimalGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> {
                    if (i == 0) {
                        arg.setNull();
                    } else {
                        arg.set(10L + i);
                    }
                },
                80
        );
    }

    @Test
    public void testLastNotNullDoubleMergeKeepsNonNullOverNullDest() throws Exception {
        DoubleArg arg = new DoubleArg();
        assertLastNotNullMergeKeepsNonNull(
                build(new LastNotNullDoubleGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(1.5)
        );
    }

    @Test
    public void testLastNotNullFloatMergeKeepsNonNullOverNullDest() throws Exception {
        FloatArg arg = new FloatArg();
        assertLastNotNullMergeKeepsNonNull(
                build(new LastNotNullFloatGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(1.5f)
        );
    }

    @Test
    public void testLastNotNullGeoByteMergeKeepsNonNullOverNullDest() throws Exception {
        GeoByteArg arg = new GeoByteArg(ColumnType.getGeoHashTypeWithBits(5));
        assertLastNotNullMergeKeepsNonNull(
                build(new LastNotNullGeoHashGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set((byte) 1)
        );
    }

    @Test
    public void testLastNotNullGeoByteSkipsNullsAndPicksMaxNonNullRowId() throws Exception {
        GeoByteArg arg = new GeoByteArg(ColumnType.getGeoHashTypeWithBits(5));
        // null at the highest rowId (index 0 -> rowId 100); max non-null rowId is 80.
        assertWinnerRowId(
                build(new LastNotNullGeoHashGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> {
                    if (i == 0) {
                        arg.setNull();
                    } else {
                        arg.set((byte) (1 + i));
                    }
                },
                80
        );
    }

    @Test
    public void testLastNotNullGeoIntMergeKeepsNonNullOverNullDest() throws Exception {
        GeoIntArg arg = new GeoIntArg(ColumnType.getGeoHashTypeWithBits(20));
        assertLastNotNullMergeKeepsNonNull(
                build(new LastNotNullGeoHashGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(1)
        );
    }

    @Test
    public void testLastNotNullGeoIntSkipsNullsAndPicksMaxNonNullRowId() throws Exception {
        GeoIntArg arg = new GeoIntArg(ColumnType.getGeoHashTypeWithBits(20));
        // null at the highest rowId (index 0 -> rowId 100); max non-null rowId is 80.
        assertWinnerRowId(
                build(new LastNotNullGeoHashGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> {
                    if (i == 0) {
                        arg.setNull();
                    } else {
                        arg.set(1 + i);
                    }
                },
                80
        );
    }

    @Test
    public void testLastNotNullGeoLongMergeKeepsNonNullOverNullDest() throws Exception {
        GeoLongArg arg = new GeoLongArg(ColumnType.getGeoHashTypeWithBits(40));
        assertLastNotNullMergeKeepsNonNull(
                build(new LastNotNullGeoHashGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(42L)
        );
    }

    @Test
    public void testLastNotNullGeoLongSkipsNullsAndPicksMaxNonNullRowId() throws Exception {
        GeoLongArg arg = new GeoLongArg(ColumnType.getGeoHashTypeWithBits(40));
        // null at the highest rowId (index 0 -> rowId 100); max non-null rowId is 80.
        assertWinnerRowId(
                build(new LastNotNullGeoHashGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> {
                    if (i == 0) {
                        arg.setNull();
                    } else {
                        arg.set(1L + i);
                    }
                },
                80
        );
    }

    @Test
    public void testLastNotNullGeoShortMergeKeepsNonNullOverNullDest() throws Exception {
        GeoShortArg arg = new GeoShortArg(ColumnType.getGeoHashTypeWithBits(10));
        assertLastNotNullMergeKeepsNonNull(
                build(new LastNotNullGeoHashGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set((short) 1)
        );
    }

    @Test
    public void testLastNotNullGeoShortSkipsNullsAndPicksMaxNonNullRowId() throws Exception {
        GeoShortArg arg = new GeoShortArg(ColumnType.getGeoHashTypeWithBits(10));
        // null at the highest rowId (index 0 -> rowId 100); max non-null rowId is 80.
        assertWinnerRowId(
                build(new LastNotNullGeoHashGroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> {
                    if (i == 0) {
                        arg.setNull();
                    } else {
                        arg.set((short) (1 + i));
                    }
                },
                80
        );
    }

    @Test
    public void testLastNotNullIPv4MergeKeepsNonNullOverNullDest() throws Exception {
        IPv4Arg arg = new IPv4Arg();
        assertLastNotNullMergeKeepsNonNull(
                build(new LastNotNullIPv4GroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(0x0A000001)
        );
    }

    @Test
    public void testLastNotNullIPv4SkipsNullsAndPicksMaxNonNullRowId() throws Exception {
        IPv4Arg arg = new IPv4Arg();
        // null at the highest rowId (index 0 -> rowId 100); max non-null rowId is 80.
        assertWinnerRowId(
                build(new LastNotNullIPv4GroupByFunctionFactory(), arg),
                ROW_IDS,
                i -> {
                    if (i == 0) {
                        arg.setNull();
                    } else {
                        arg.set(0x0A000001 + i);
                    }
                },
                80
        );
    }

    @Test
    public void testLastNotNullIntMergeKeepsNonNullOverNullDest() throws Exception {
        IntArg arg = new IntArg();
        assertLastNotNullMergeKeepsNonNull(
                build(new LastNotNullIntGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(42)
        );
    }

    @Test
    public void testLastNotNullLongMergeKeepsNonNullOverNullDest() throws Exception {
        LongArg arg = new LongArg();
        assertLastNotNullMergeKeepsNonNull(
                build(new LastNotNullLongGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(42L)
        );
    }

    @Test
    public void testLastNotNullStrMergeKeepsNonNullOverNullDest() throws Exception {
        StrArg arg = new StrArg();
        assertLastNotNullMergeKeepsNonNull(
                build(new LastNotNullStrGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set("abc")
        );
    }

    @Test
    public void testLastNotNullSymbolMergeKeepsNonNullOverNullDest() throws Exception {
        SymbolArg arg = new SymbolArg();
        assertLastNotNullMergeKeepsNonNull(
                build(new LastNotNullSymbolGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(5)
        );
    }

    @Test
    public void testLastNotNullTimestampMergeKeepsNonNullOverNullDest() throws Exception {
        TimestampArg arg = new TimestampArg(ColumnType.TIMESTAMP);
        assertLastNotNullMergeKeepsNonNull(
                build(new LastNotNullTimestampGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(42L)
        );
    }

    @Test
    public void testLastNotNullUuidMergeKeepsNonNullOverNullDest() throws Exception {
        UuidArg arg = new UuidArg();
        assertLastNotNullMergeKeepsNonNull(
                build(new LastNotNullUuidGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set(42L)
        );
    }

    @Test
    public void testLastNotNullVarcharMergeKeepsNonNullOverNullDest() throws Exception {
        VarcharArg arg = new VarcharArg();
        assertLastNotNullMergeKeepsNonNull(
                build(new LastNotNullVarcharGroupByFunctionFactory(), arg),
                arg::setNull,
                () -> arg.set("abc")
        );
    }

    private static void assertFunctionValueEquals(GroupByFunction func, Record expected, Record actual) {
        switch (ColumnType.tagOf(func.getType())) {
            case ColumnType.CHAR -> Assert.assertEquals(func.getChar(expected), func.getChar(actual));
            case ColumnType.DATE -> Assert.assertEquals(func.getDate(expected), func.getDate(actual));
            case ColumnType.DECIMAL8 -> Assert.assertEquals(func.getDecimal8(expected), func.getDecimal8(actual));
            case ColumnType.DECIMAL16 -> Assert.assertEquals(func.getDecimal16(expected), func.getDecimal16(actual));
            case ColumnType.DECIMAL32 -> Assert.assertEquals(func.getDecimal32(expected), func.getDecimal32(actual));
            case ColumnType.DECIMAL64 -> Assert.assertEquals(func.getDecimal64(expected), func.getDecimal64(actual));
            case ColumnType.DECIMAL128 -> {
                final Decimal128 expectedValue = new Decimal128();
                final Decimal128 actualValue = new Decimal128();
                func.getDecimal128(expected, expectedValue);
                func.getDecimal128(actual, actualValue);
                Assert.assertEquals(expectedValue, actualValue);
            }
            case ColumnType.DECIMAL256 -> {
                final Decimal256 expectedValue = new Decimal256();
                final Decimal256 actualValue = new Decimal256();
                func.getDecimal256(expected, expectedValue);
                func.getDecimal256(actual, actualValue);
                Assert.assertEquals(expectedValue, actualValue);
            }
            case ColumnType.DOUBLE -> Assert.assertEquals(func.getDouble(expected), func.getDouble(actual), 0.0);
            case ColumnType.FLOAT -> Assert.assertEquals(func.getFloat(expected), func.getFloat(actual), 0.0f);
            case ColumnType.GEOBYTE -> Assert.assertEquals(func.getGeoByte(expected), func.getGeoByte(actual));
            case ColumnType.GEOSHORT -> Assert.assertEquals(func.getGeoShort(expected), func.getGeoShort(actual));
            case ColumnType.GEOINT -> Assert.assertEquals(func.getGeoInt(expected), func.getGeoInt(actual));
            case ColumnType.GEOLONG -> Assert.assertEquals(func.getGeoLong(expected), func.getGeoLong(actual));
            case ColumnType.INT, ColumnType.IPv4, ColumnType.SYMBOL ->
                    Assert.assertEquals(func.getInt(expected), func.getInt(actual));
            case ColumnType.LONG -> Assert.assertEquals(func.getLong(expected), func.getLong(actual));
            case ColumnType.STRING -> {
                final String expectedValue = func.getStrA(expected).toString();
                TestUtils.assertEquals(expectedValue, func.getStrA(actual));
            }
            case ColumnType.TIMESTAMP -> Assert.assertEquals(func.getTimestamp(expected), func.getTimestamp(actual));
            case ColumnType.UUID -> {
                Assert.assertEquals(func.getLong128Hi(expected), func.getLong128Hi(actual));
                Assert.assertEquals(func.getLong128Lo(expected), func.getLong128Lo(actual));
            }
            case ColumnType.VARCHAR -> {
                final Utf8String expectedValue = Utf8String.newInstance(func.getVarcharA(expected));
                TestUtils.assertEquals(expectedValue, func.getVarcharA(actual));
            }
            default -> Assert.fail("unsupported aggregate payload type: " + ColumnType.nameOf(func.getType()));
        }
    }

    private void assertFirstNotNullMergeKeepsNonNull(GroupByFunction func, Runnable setNull, Runnable setNonNull) throws Exception {
        assertMemoryLeak(() -> {
            final ArrayColumnTypes types = new ArrayColumnTypes();
            func.initValueTypes(types);
            func.initValueIndex(0);
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration);
                 SimpleMapValue dest = new SimpleMapValue(types.getColumnCount());
                 SimpleMapValue src = new SimpleMapValue(types.getColumnCount())) {
                // Str/Varchar store the value via the allocator; for inline types setAllocator is a no-op.
                func.setAllocator(allocator);
                // dest: a group whose only seen row is NULL, committed at the LOWER rowId. The mirror of
                // the last_not_null case below: here the rowId comparison alone keeps the NULL, because
                // the dest already holds the smaller rowId a first() wants.
                func.setEmpty(dest);
                setNull.run();
                func.computeFirst(dest, null, 20);
                // src: the only non-null value in the whole group, at the higher rowId.
                func.setEmpty(src);
                setNonNull.run();
                func.computeFirst(src, null, 100);
                func.merge(dest, src);
                Assert.assertEquals(
                        "first_not_null merge dropped the only non-null value because the dest slot held an all-null group with a lower rowId",
                        100,
                        dest.getLong(func.getValueIndex())
                );
                assertFunctionValueEquals(func, src, dest);
            }
        });
    }

    private void assertLastNotNullMergeKeepsNonNull(GroupByFunction func, Runnable setNull, Runnable setNonNull) throws Exception {
        assertMemoryLeak(() -> {
            final ArrayColumnTypes types = new ArrayColumnTypes();
            func.initValueTypes(types);
            func.initValueIndex(0);
            try (GroupByAllocator allocator = GroupByAllocatorFactory.createAllocator(configuration);
                 SimpleMapValue dest = new SimpleMapValue(types.getColumnCount());
                 SimpleMapValue src = new SimpleMapValue(types.getColumnCount())) {
                // Str/Varchar store the value via the allocator; for inline types setAllocator is a no-op.
                func.setAllocator(allocator);
                // dest: a group whose only seen row is NULL, committed at the higher rowId.
                func.setEmpty(dest);
                setNull.run();
                func.computeFirst(dest, null, 100);
                // src: the only non-null value in the whole group, at the lower rowId.
                func.setEmpty(src);
                setNonNull.run();
                func.computeFirst(src, null, 20);
                func.merge(dest, src);
                Assert.assertEquals(
                        "last_not_null merge dropped the only non-null value because the dest slot held an all-null group with a higher rowId",
                        20,
                        dest.getLong(func.getValueIndex())
                );
                assertFunctionValueEquals(func, src, dest);
            }
        });
    }

    private void assertWinnerRowId(GroupByFunction func, long[] rowIds, RowArg setArg, long expectedRowId) throws Exception {
        assertMemoryLeak(() -> {
            final ArrayColumnTypes types = new ArrayColumnTypes();
            func.initValueTypes(types);
            func.initValueIndex(0);
            try (SimpleMapValue mapValue = new SimpleMapValue(types.getColumnCount())) {
                func.setEmpty(mapValue);
                for (int i = 0; i < rowIds.length; i++) {
                    setArg.apply(i);
                    if (i == 0) {
                        func.computeFirst(mapValue, null, rowIds[i]);
                    } else {
                        func.computeNext(mapValue, null, rowIds[i]);
                    }
                }
                Assert.assertEquals(
                        "aggregate committed the wrong row under out-of-order reduction",
                        expectedRowId,
                        mapValue.getLong(func.getValueIndex())
                );
            }
        });
    }

    private GroupByFunction build(FunctionFactory factory, Function arg) throws SqlException {
        final ObjList<Function> args = new ObjList<>();
        args.add(arg);
        final IntList argPositions = new IntList();
        argPositions.add(0);
        return (GroupByFunction) factory.newInstance(0, args, argPositions, configuration, sqlExecutionContext);
    }

    @FunctionalInterface
    private interface RowArg {
        void apply(int row);
    }

    private static final class CharArg extends CharFunction {
        private char value;

        @Override
        public char getChar(Record rec) {
            return value;
        }

        private void set(char v) {
            value = v;
        }

        private void setNull() {
            value = 0;
        }
    }

    private static final class DateArg extends DateFunction {
        private long value = Numbers.LONG_NULL;

        @Override
        public long getDate(Record rec) {
            return value;
        }

        private void set(long v) {
            value = v;
        }

        private void setNull() {
            value = Numbers.LONG_NULL;
        }
    }

    private static final class DecimalArg extends DecimalFunction {
        private final Decimal256 value = new Decimal256();

        private DecimalArg(int type) {
            super(type);
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
            if (value.isNull()) {
                sink.ofRawNull();
            } else {
                sink.ofRaw(value.getLh(), value.getLl());
            }
        }

        @Override
        public short getDecimal16(Record rec) {
            return value.isNull() ? Decimals.DECIMAL16_NULL : (short) value.getLl();
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
            sink.copyRaw(value);
        }

        @Override
        public int getDecimal32(Record rec) {
            return value.isNull() ? Decimals.DECIMAL32_NULL : (int) value.getLl();
        }

        @Override
        public long getDecimal64(Record rec) {
            return value.isNull() ? Decimals.DECIMAL64_NULL : value.getLl();
        }

        @Override
        public byte getDecimal8(Record rec) {
            return value.isNull() ? Decimals.DECIMAL8_NULL : (byte) value.getLl();
        }

        private void set(long raw) {
            value.ofRaw(raw);
        }

        private void setNull() {
            value.ofRawNull();
        }
    }

    private static final class DoubleArg extends DoubleFunction {
        private double value = Double.NaN;

        @Override
        public double getDouble(Record rec) {
            return value;
        }

        private void set(double v) {
            value = v;
        }

        private void setNull() {
            value = Double.NaN;
        }
    }

    private static final class FloatArg extends FloatFunction {
        private float value = Float.NaN;

        @Override
        public float getFloat(Record rec) {
            return value;
        }

        private void set(float v) {
            value = v;
        }

        private void setNull() {
            value = Float.NaN;
        }
    }

    private static final class GeoByteArg extends GeoByteFunction {
        private byte value = GeoHashes.BYTE_NULL;

        private GeoByteArg(int type) {
            super(type);
        }

        @Override
        public byte getGeoByte(Record rec) {
            return value;
        }

        private void set(byte v) {
            value = v;
        }

        private void setNull() {
            value = GeoHashes.BYTE_NULL;
        }
    }

    private static final class GeoIntArg extends GeoIntFunction {
        private int value = GeoHashes.INT_NULL;

        private GeoIntArg(int type) {
            super(type);
        }

        @Override
        public int getGeoInt(Record rec) {
            return value;
        }

        private void set(int v) {
            value = v;
        }

        private void setNull() {
            value = GeoHashes.INT_NULL;
        }
    }

    private static final class GeoLongArg extends GeoLongFunction {
        private long value = GeoHashes.NULL;

        private GeoLongArg(int type) {
            super(type);
        }

        @Override
        public long getGeoLong(Record rec) {
            return value;
        }

        private void set(long v) {
            value = v;
        }

        private void setNull() {
            value = GeoHashes.NULL;
        }
    }

    private static final class GeoShortArg extends GeoShortFunction {
        private short value = GeoHashes.SHORT_NULL;

        private GeoShortArg(int type) {
            super(type);
        }

        @Override
        public short getGeoShort(Record rec) {
            return value;
        }

        private void set(short v) {
            value = v;
        }

        private void setNull() {
            value = GeoHashes.SHORT_NULL;
        }
    }

    private static final class IPv4Arg extends IPv4Function {
        private int value = Numbers.IPv4_NULL;

        @Override
        public int getIPv4(Record rec) {
            return value;
        }

        private void set(int v) {
            value = v;
        }

        private void setNull() {
            value = Numbers.IPv4_NULL;
        }
    }

    private static final class IntArg extends IntFunction {
        private int value = Numbers.INT_NULL;

        @Override
        public int getInt(Record rec) {
            return value;
        }

        private void set(int v) {
            value = v;
        }

        private void setNull() {
            value = Numbers.INT_NULL;
        }
    }

    private static final class LongArg extends LongFunction {
        private long value = Numbers.LONG_NULL;

        @Override
        public long getLong(Record rec) {
            return value;
        }

        private void set(long v) {
            value = v;
        }

        private void setNull() {
            value = Numbers.LONG_NULL;
        }
    }

    private static final class StrArg extends StrFunction {
        private CharSequence value;

        @Override
        public CharSequence getStrA(Record rec) {
            return value;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return value;
        }

        private void set(CharSequence v) {
            value = v;
        }

        private void setNull() {
            value = null;
        }
    }

    private static final class SymbolArg extends SymbolFunction {
        private int value = SymbolTable.VALUE_IS_NULL;

        @Override
        public int getInt(Record rec) {
            return value;
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CharSequence getSymbolB(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isSymbolTableStatic() {
            return false;
        }

        @Override
        public CharSequence valueBOf(int key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CharSequence valueOf(int key) {
            throw new UnsupportedOperationException();
        }

        private void set(int v) {
            value = v;
        }

        private void setNull() {
            value = SymbolTable.VALUE_IS_NULL;
        }
    }

    private static final class TimestampArg extends TimestampFunction {
        private long value = Numbers.LONG_NULL;

        private TimestampArg(int timestampType) {
            super(timestampType);
        }

        @Override
        public long getTimestamp(Record rec) {
            return value;
        }

        private void set(long v) {
            value = v;
        }

        private void setNull() {
            value = Numbers.LONG_NULL;
        }
    }

    private static final class UuidArg extends UuidFunction {
        private long hi = Numbers.LONG_NULL;
        private long lo = Numbers.LONG_NULL;

        @Override
        public long getLong128Hi(Record rec) {
            return hi;
        }

        @Override
        public long getLong128Lo(Record rec) {
            return lo;
        }

        private void set(long v) {
            lo = v;
            hi = 0;
        }

        private void setNull() {
            lo = Numbers.LONG_NULL;
            hi = Numbers.LONG_NULL;
        }
    }

    private static final class VarcharArg extends VarcharFunction {
        private Utf8Sequence value;

        @Override
        public Utf8Sequence getVarcharA(Record rec) {
            return value;
        }

        @Override
        public Utf8Sequence getVarcharB(Record rec) {
            return value;
        }

        private void set(CharSequence v) {
            value = new Utf8String(v);
        }

        private void setNull() {
            value = null;
        }
    }
}
