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
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.DecimalFunction;
import io.questdb.griffin.engine.functions.GeoByteFunction;
import io.questdb.griffin.engine.functions.GeoIntFunction;
import io.questdb.griffin.engine.functions.GeoLongFunction;
import io.questdb.griffin.engine.functions.GeoShortFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.IPv4Function;
import io.questdb.griffin.engine.functions.groupby.FirstDecimalGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.FirstGeoHashGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullDecimalGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullGeoHashGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullIPv4GroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastDecimalGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastGeoHashGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastNotNullDecimalGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastNotNullGeoHashGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastNotNullIPv4GroupByFunctionFactory;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
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
}
