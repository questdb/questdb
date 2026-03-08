/*******************************************************************************
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

package io.questdb.test.griffin.engine.window;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.window.AvgDoubleWindowFunctionFactory;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.functions.window.CountConstWindowFunctionFactory;
import io.questdb.griffin.engine.functions.window.CountDoubleWindowFunctionFactory;
import io.questdb.griffin.engine.functions.window.CountFunctionFactoryHelper;
import io.questdb.griffin.engine.functions.window.FirstValueDoubleWindowFunctionFactory;
import io.questdb.griffin.engine.functions.window.LastValueDoubleWindowFunctionFactory;
import io.questdb.griffin.engine.functions.window.MaxDoubleWindowFunctionFactory;
import io.questdb.griffin.engine.functions.window.MinDoubleWindowFunctionFactory;
import io.questdb.griffin.engine.functions.window.SumDoubleWindowFunctionFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestDefaults;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.Collectors;

public class WindowFunctionUnitTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(WindowFunctionUnitTest.class);
    private static final short[] columnTypes = new short[]{ColumnType.TIMESTAMP, ColumnType.INT, ColumnType.LONG};

    @Test
    public void testAggOverPartitionRangeFuzz() throws Exception {
        fuzzTestBase(
                TestUtils.generateRandom(LOG),
                true,
                false,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(65536),
                rnd -> -rnd.nextLong(65536),
                (rangeLo, rangeHi) -> new SumDoubleWindowFunctionFactory.SumOverPartitionRangeFrameFunction(
                        TestDefaults.createOrderedMap(new SingleColumnType(columnTypes[1]), AvgDoubleWindowFunctionFactory.AVG_OVER_PARTITION_RANGE_COLUMN_TYPES),
                        TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                        TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                        rangeLo,
                        rangeHi,
                        TestDefaults.createLongFunction(x -> x.getLong(2)),
                        TestDefaults.createMemoryCARW(),
                        2,
                        0
                ),
                Double::sum
        );
    }

    @Test
    public void testAggOverPartitionRowsFuzz() throws Exception {
        fuzzTestBase(
                TestUtils.generateRandom(LOG),
                true,
                true,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(1024),
                rnd -> -rnd.nextLong(1024) - 1,
                (rangeLo, rangeHi) -> new SumDoubleWindowFunctionFactory.SumOverPartitionRowsFrameFunction(
                        TestDefaults.createOrderedMap(new SingleColumnType(columnTypes[1]), AvgDoubleWindowFunctionFactory.AVG_OVER_PARTITION_ROWS_COLUMN_TYPES),
                        TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                        TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                        rangeLo,
                        rangeHi,
                        TestDefaults.createLongFunction(x -> x.getLong(2)),
                        TestDefaults.createMemoryCARW()
                ),
                Double::sum
        );
    }

    @Test
    public void testAggOverRowsFuzz() throws Exception {
        fuzzTestBase(
                TestUtils.generateRandom(LOG),
                false,
                true,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(1024),
                rnd -> -rnd.nextLong(1024),
                (rangeLo, rangeHi) -> {
                    if (rangeLo == Long.MIN_VALUE && rangeHi == 0) {
                        return new SumDoubleWindowFunctionFactory.SumOverUnboundedRowsFrameFunction(
                                TestDefaults.createLongFunction(x -> x.getLong(2))
                        );
                    }
                    return new SumDoubleWindowFunctionFactory.SumOverRowsFrameFunction(
                            TestDefaults.createLongFunction(x -> x.getLong(2)),
                            rangeLo,
                            rangeHi,
                            TestDefaults.createMemoryCARW()
                    );
                },
                Double::sum
        );
    }

    @Test
    public void testAggRangeFuzz() throws Exception {
        fuzzTestBase(
                TestUtils.generateRandom(LOG),
                false,
                false,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(65536),
                rnd -> -rnd.nextLong(65536),
                (rangeLo, rangeHi) -> new SumDoubleWindowFunctionFactory.SumOverRangeFrameFunction(
                        rangeLo,
                        rangeHi,
                        TestDefaults.createLongFunction(x -> x.getLong(2)),
                        64,
                        TestDefaults.createMemoryCARW(),
                        0
                ),
                Double::sum
        );
    }

    @Test
    public void testCountConstOverPartitionRowsFuzz() throws Exception {
        fuzzTestLong(
                TestUtils.generateRandom(LOG),
                true,
                true,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(1024),
                rnd -> -rnd.nextLong(1024) - 1,
                (rangeLo, rangeHi) -> new CountConstWindowFunctionFactory.CountOverPartitionRowsFrameFunction(
                        TestDefaults.createOrderedMap(new SingleColumnType(columnTypes[1]), CountFunctionFactoryHelper.COUNT_COLUMN_TYPES),
                        TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                        TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                        rangeLo,
                        rangeHi
                ),
                Long::sum,
                CountConstWindowFunctionFactory.isRecordNotNull
        );
    }

    @Test
    public void testCountConstOverRowsFuzz() throws Exception {
        fuzzTestLong(
                TestUtils.generateRandom(LOG),
                false,
                true,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(1024),
                rnd -> -rnd.nextLong(1024),
                (rangeLo, rangeHi) -> {
                    if (rangeLo == Long.MIN_VALUE && rangeHi == 0) {
                        return new CountFunctionFactoryHelper.CountOverUnboundedRowsFrameFunction(
                                null, CountConstWindowFunctionFactory.isRecordNotNull);
                    }
                    return new CountConstWindowFunctionFactory.CountOverRowsFrameFunction(
                            rangeLo,
                            rangeHi
                    );
                },
                Long::sum,
                CountConstWindowFunctionFactory.isRecordNotNull
        );
    }

    @Test
    public void testCountDoubleOverPartitionRangeFuzz() throws Exception {
        fuzzTestLong(
                TestUtils.generateRandom(LOG),
                true,
                false,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(65536),
                rnd -> -rnd.nextLong(65536),
                (rangeLo, rangeHi) -> new CountFunctionFactoryHelper.CountOverPartitionRangeFrameFunction(
                        TestDefaults.createOrderedMap(new SingleColumnType(columnTypes[1]), CountFunctionFactoryHelper.COUNT_OVER_PARTITION_RANGE_COLUMN_TYPES),
                        TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                        TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                        rangeLo,
                        rangeHi,
                        TestDefaults.createMemoryCARW(),
                        2,
                        0,
                        TestDefaults.createLongFunction(x -> x.getLong(2)),
                        CountDoubleWindowFunctionFactory.isRecordNotNull
                ),
                Long::sum,
                CountDoubleWindowFunctionFactory.isRecordNotNull
        );
    }

    @Test
    public void testCountDoubleOverPartitionRowsFuzz() throws Exception {
        fuzzTestLong(
                TestUtils.generateRandom(LOG),
                true,
                true,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(1024),
                rnd -> -rnd.nextLong(1024) - 1,
                (rangeLo, rangeHi) -> new CountFunctionFactoryHelper.CountOverPartitionRowsFrameFunction(
                        TestDefaults.createOrderedMap(new SingleColumnType(columnTypes[1]), CountFunctionFactoryHelper.COUNT_OVER_PARTITION_ROWS_COLUMN_TYPES),
                        TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                        TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                        rangeLo,
                        rangeHi,
                        TestDefaults.createLongFunction(x -> x.getLong(2)),
                        TestDefaults.createMemoryCARW(),
                        CountDoubleWindowFunctionFactory.isRecordNotNull
                ),
                Long::sum,
                CountConstWindowFunctionFactory.isRecordNotNull
        );
    }

    @Test
    public void testCountDoubleOverRowsFuzz() throws Exception {
        fuzzTestLong(
                TestUtils.generateRandom(LOG),
                false,
                true,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(1024),
                rnd -> -rnd.nextLong(1024),
                (rangeLo, rangeHi) -> {
                    if (rangeLo == Long.MIN_VALUE && rangeHi == 0) {
                        return new CountFunctionFactoryHelper.CountOverUnboundedRowsFrameFunction(
                                null, CountConstWindowFunctionFactory.isRecordNotNull);
                    }
                    return new CountFunctionFactoryHelper.CountOverRowsFrameFunction(
                            TestDefaults.createLongFunction(x -> x.getLong(2)),
                            rangeLo,
                            rangeHi,
                            TestDefaults.createMemoryCARW(),
                            CountDoubleWindowFunctionFactory.isRecordNotNull
                    );
                },
                Long::sum,
                CountConstWindowFunctionFactory.isRecordNotNull
        );
    }

    @Test
    public void testCountDoubleRangeFuzz() throws Exception {
        fuzzTestLong(
                TestUtils.generateRandom(LOG),
                false,
                false,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(65536),
                rnd -> -rnd.nextLong(65536),
                (rangeLo, rangeHi) -> new CountFunctionFactoryHelper.CountOverRangeFrameFunction(
                        rangeLo,
                        rangeHi,
                        configuration,
                        0,
                        TestDefaults.createLongFunction(x -> x.getLong(2)),
                        CountDoubleWindowFunctionFactory.isRecordNotNull
                ),
                Long::sum,
                CountDoubleWindowFunctionFactory.isRecordNotNull
        );
    }

    @Test
    public void testFirstNotNullOverRowsFuzz() throws Exception {
        fuzzTestBase(
                TestUtils.generateRandom(LOG),
                false,
                true,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(1024),
                rnd -> -rnd.nextLong(1024),
                (rangeLo, rangeHi) -> {
                    if (rangeLo == Long.MIN_VALUE && rangeHi == 0) {
                        return new FirstValueDoubleWindowFunctionFactory.FirstValueOverWholeResultSetFunction(TestDefaults.createLongFunction(x -> x.getLong(2)));
                    }
                    return new FirstValueDoubleWindowFunctionFactory.FirstNotNullValueOverRowsFrameFunction(
                            TestDefaults.createLongFunction(x -> x.getLong(2)),
                            rangeLo,
                            rangeHi,
                            TestDefaults.createMemoryCARW()
                    );
                },
                (a, b) -> b
        );
    }

    @Test
    public void testFirstOverRowsFuzz() throws Exception {
        fuzzTestBase(
                TestUtils.generateRandom(LOG),
                false,
                true,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(1024),
                rnd -> -rnd.nextLong(1024),
                (rangeLo, rangeHi) -> {
                    if (rangeLo == Long.MIN_VALUE && rangeHi == 0) {
                        return new FirstValueDoubleWindowFunctionFactory.FirstValueOverWholeResultSetFunction(TestDefaults.createLongFunction(x -> x.getLong(2)));
                    }
                    return new FirstValueDoubleWindowFunctionFactory.FirstValueOverRowsFrameFunction(
                            TestDefaults.createLongFunction(x -> x.getLong(2)),
                            rangeLo,
                            rangeHi,
                            TestDefaults.createMemoryCARW()
                    );
                },
                (a, b) -> b
        );
    }

    @Test
    public void testLastNotNullOverPartitionRangeFuzz() throws Exception {
        fuzzTestBase(
                TestUtils.generateRandom(LOG),
                true,
                false,
                true,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(1024),
                rnd -> -rnd.nextLong(1024),
                (rangeLo, rangeHi) -> {
                    if (rangeHi == 0 && rangeLo == Long.MIN_VALUE) {
                        return new LastValueDoubleWindowFunctionFactory.LastNotNullValueOverUnboundedPartitionRowsFrameFunction(
                                TestDefaults.createOrderedMap(new SingleColumnType(columnTypes[1]), LastValueDoubleWindowFunctionFactory.LAST_VALUE_COLUMN_TYPES),
                                TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                                TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                                TestDefaults.createLongFunction(x -> x.getLong(2))
                        );
                    }
                    return new LastValueDoubleWindowFunctionFactory.LastNotNullValueOverPartitionRangeFrameFunction(
                            TestDefaults.createOrderedMap(new SingleColumnType(columnTypes[1]), LastValueDoubleWindowFunctionFactory.LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES),
                            TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                            TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                            rangeLo,
                            rangeHi,
                            TestDefaults.createLongFunction(x -> x.getLong(2)),
                            TestDefaults.createMemoryCARW(),
                            2,
                            0
                    );
                },
                (a, b) -> a
        );
    }

    @Test
    public void testLastNotNullOverPartitionRowsFuzz() throws Exception {
        fuzzTestBase(
                TestUtils.generateRandom(LOG),
                true,
                true,
                true,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(1024),
                rnd -> -rnd.nextLong(1024),
                (rangeLo, rangeHi) -> {
                    if (rangeHi == 0 && rangeLo == Long.MIN_VALUE) {
                        return new LastValueDoubleWindowFunctionFactory.LastNotNullValueOverUnboundedPartitionRowsFrameFunction(
                                TestDefaults.createOrderedMap(new SingleColumnType(columnTypes[1]), LastValueDoubleWindowFunctionFactory.LAST_VALUE_COLUMN_TYPES),
                                TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                                TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                                TestDefaults.createLongFunction(x -> x.getLong(2))
                        );
                    }
                    return new LastValueDoubleWindowFunctionFactory.LastNotNullValueOverPartitionRowsFrameFunction(
                            TestDefaults.createOrderedMap(new SingleColumnType(columnTypes[1]), LastValueDoubleWindowFunctionFactory.LAST_NOT_NULL_VALUE_PARTITION_ROWS_COLUMN_TYPES),
                            TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                            TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                            rangeLo,
                            rangeHi,
                            TestDefaults.createLongFunction(x -> x.getLong(2)),
                            TestDefaults.createMemoryCARW()
                    );
                },
                (a, b) -> a
        );
    }

    @Test
    public void testLastNotNullOverRangeFuzz() throws Exception {
        fuzzTestBase(
                TestUtils.generateRandom(LOG),
                false,
                false,
                true,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(1024),
                rnd -> -rnd.nextLong(1024),
                (rangeLo, rangeHi) -> {
                    if (rangeHi == 0 && rangeLo == Long.MIN_VALUE) {
                        return new LastValueDoubleWindowFunctionFactory.LastNotNullOverUnboundedRowsFrameFunction(
                                TestDefaults.createLongFunction(x -> x.getLong(2))
                        );
                    }
                    return new LastValueDoubleWindowFunctionFactory.LastNotNullValueOverRangeFrameFunction(
                            rangeLo,
                            rangeHi,
                            TestDefaults.createLongFunction(x -> x.getLong(2)),
                            configuration,
                            0
                    );
                },
                (a, b) -> a
        );
    }

    @Test
    public void testLastNotNullOverRowsFuzz() throws Exception {
        fuzzTestBase(
                TestUtils.generateRandom(LOG),
                false,
                true,
                true,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(1024),
                rnd -> -rnd.nextLong(1024),
                (rangeLo, rangeHi) -> {
                    if (rangeHi == 0 && rangeLo == Long.MIN_VALUE) {
                        return new LastValueDoubleWindowFunctionFactory.LastNotNullOverUnboundedRowsFrameFunction(
                                TestDefaults.createLongFunction(x -> x.getLong(2))
                        );
                    }
                    return new LastValueDoubleWindowFunctionFactory.LastNotNullValueOverRowsFrameFunction(
                            TestDefaults.createLongFunction(x -> x.getLong(2)),
                            rangeLo,
                            rangeHi,
                            TestDefaults.createMemoryCARW()
                    );
                },
                (a, b) -> a
        );
    }

    @Test
    public void testLastOverPartitionRangeFuzz() throws Exception {
        fuzzTestBase(
                TestUtils.generateRandom(LOG),
                true,
                false,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(1024),
                rnd -> -rnd.nextLong(1024),
                (rangeLo, rangeHi) -> {
                    if (rangeHi == 0) {
                        return new LastValueDoubleWindowFunctionFactory.LastValueIncludeCurrentPartitionRowsFrameFunction(rangeLo,
                                false,
                                TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                                TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                                TestDefaults.createLongFunction(x -> x.getLong(2)));
                    }
                    return new LastValueDoubleWindowFunctionFactory.LastValueOverPartitionRangeFrameFunction(
                            TestDefaults.createOrderedMap(new SingleColumnType(columnTypes[1]), LastValueDoubleWindowFunctionFactory.LAST_VALUE_PARTITION_RANGE_COLUMN_TYPES),
                            TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                            TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                            rangeLo,
                            rangeHi,
                            TestDefaults.createLongFunction(x -> x.getLong(2)),
                            TestDefaults.createMemoryCARW(),
                            2,
                            0
                    );
                },
                (a, b) -> a
        );
    }

    @Test
    public void testLastOverPartitionRowsFuzz() throws Exception {
        fuzzTestBase(
                TestUtils.generateRandom(LOG),
                true,
                true,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(1024),
                rnd -> -rnd.nextLong(1024),
                (rangeLo, rangeHi) -> {
                    if (rangeHi == 0) {
                        return new LastValueDoubleWindowFunctionFactory.LastValueIncludeCurrentPartitionRowsFrameFunction(rangeLo,
                                false,
                                TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                                TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                                TestDefaults.createLongFunction(x -> x.getLong(2)));
                    }
                    return new LastValueDoubleWindowFunctionFactory.LastValueOverPartitionRowsFrameFunction(
                            TestDefaults.createOrderedMap(new SingleColumnType(columnTypes[1]), LastValueDoubleWindowFunctionFactory.LAST_VALUE_PARTITION_ROWS_COLUMN_TYPES),
                            TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                            TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                            rangeLo,
                            rangeHi,
                            TestDefaults.createLongFunction(x -> x.getLong(2)),
                            TestDefaults.createMemoryCARW()
                    );
                },
                (a, b) -> a
        );
    }

    @Test
    public void testLastOverRangeFuzz() throws Exception {
        fuzzTestBase(
                TestUtils.generateRandom(LOG),
                false,
                false,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(1024),
                rnd -> -rnd.nextLong(1024),
                (rangeLo, rangeHi) -> {
                    if (rangeHi == 0) {
                        return new LastValueDoubleWindowFunctionFactory.LastValueIncludeCurrentFrameFunction(rangeLo, true, TestDefaults.createLongFunction(x -> x.getLong(2)));
                    }
                    return new LastValueDoubleWindowFunctionFactory.LastValueOverRangeFrameFunction(
                            rangeLo,
                            rangeHi,
                            TestDefaults.createLongFunction(x -> x.getLong(2)),
                            configuration,
                            0
                    );
                },
                (a, b) -> a
        );
    }

    @Test
    public void testLastOverRowsFuzz() throws Exception {
        fuzzTestBase(
                TestUtils.generateRandom(LOG),
                false,
                true,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(1024),
                rnd -> -rnd.nextLong(1024),
                (rangeLo, rangeHi) -> {
                    if (rangeHi == 0) {
                        return new LastValueDoubleWindowFunctionFactory.LastValueIncludeCurrentFrameFunction(rangeLo, false, TestDefaults.createLongFunction(x -> x.getLong(2)));
                    }
                    return new LastValueDoubleWindowFunctionFactory.LastValueOverRowsFrameFunction(
                            TestDefaults.createLongFunction(x -> x.getLong(2)),
                            rangeLo,
                            rangeHi,
                            TestDefaults.createMemoryCARW()
                    );
                },
                (a, b) -> a
        );
    }

    @Test
    public void testMaxOverPartitionRangeFuzz() throws Exception {
        fuzzTestBase(
                TestUtils.generateRandom(LOG),
                true,
                false,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(65536),
                rnd -> -rnd.nextLong(65536),
                (rangeLo, rangeHi) -> new MaxDoubleWindowFunctionFactory.MaxMinOverPartitionRangeFrameFunction(
                        TestDefaults.createOrderedMap(new SingleColumnType(columnTypes[1]), rangeLo == Long.MIN_VALUE ?
                                MaxDoubleWindowFunctionFactory.MAX_OVER_PARTITION_RANGE_COLUMN_TYPES :
                                MaxDoubleWindowFunctionFactory.MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES),
                        TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                        TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                        rangeLo,
                        rangeHi,
                        TestDefaults.createLongFunction(x -> x.getLong(2)),
                        TestDefaults.createMemoryCARW(),
                        TestDefaults.createMemoryCARW(),
                        2,
                        0,
                        MaxDoubleWindowFunctionFactory.GREATER_THAN,
                        MaxDoubleWindowFunctionFactory.NAME
                ),
                Double::max
        );
    }

    @Test
    public void testMaxOverPartitionRangeWithBothBounds() {
        MaxDoubleWindowFunctionFactory.MaxMinOverPartitionRangeFrameFunction f = new MaxDoubleWindowFunctionFactory.MaxMinOverPartitionRangeFrameFunction(
                TestDefaults.createOrderedMap(new SingleColumnType(columnTypes[1]), MaxDoubleWindowFunctionFactory.MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES),
                TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                -2,
                -2,
                TestDefaults.createLongFunction(x -> x.getLong(2)),
                TestDefaults.createMemoryCARW(),
                TestDefaults.createMemoryCARW(),
                configuration.getSqlWindowInitialRangeBufferSize(),
                0,
                MaxDoubleWindowFunctionFactory.GREATER_THAN,
                MaxDoubleWindowFunctionFactory.NAME
        );
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 1, 2, (long) 1));
        Assert.assertEquals(Double.NaN, f.getDouble(null), 0);

        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 2, 2, (long) 2));
        Assert.assertEquals(Double.NaN, f.getDouble(null), 0);

        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 4, 2, (long) 4));
        Assert.assertEquals(2, f.getDouble(null), 0);
    }

    @Test
    public void testMaxOverPartitionRowsFuzz() throws Exception {
        fuzzTestBase(
                TestUtils.generateRandom(LOG),
                true,
                true,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(1024),
                rnd -> -rnd.nextLong(1024) - 1,
                (rangeLo, rangeHi) -> new MaxDoubleWindowFunctionFactory.MaxMinOverPartitionRowsFrameFunction(
                        TestDefaults.createOrderedMap(new SingleColumnType(columnTypes[1]),
                                rangeLo == Long.MIN_VALUE ? MaxDoubleWindowFunctionFactory.MAX_OVER_PARTITION_ROWS_COLUMN_TYPES :
                                        MaxDoubleWindowFunctionFactory.MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES),
                        TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                        TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                        rangeLo,
                        rangeHi,
                        TestDefaults.createLongFunction(x -> x.getLong(2)),
                        TestDefaults.createMemoryCARW(),
                        TestDefaults.createMemoryCARW(),
                        MaxDoubleWindowFunctionFactory.GREATER_THAN,
                        MaxDoubleWindowFunctionFactory.NAME
                ),
                Double::max
        );
    }

    @Test
    public void testMaxOverRowsFuzz() throws Exception {
        fuzzTestBase(
                TestUtils.generateRandom(LOG),
                false,
                true,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(1024),
                rnd -> -rnd.nextLong(1024),
                (rangeLo, rangeHi) -> {
                    if (rangeLo == Long.MIN_VALUE && rangeHi == 0) {
                        return new MaxDoubleWindowFunctionFactory.MaxMinOverUnboundedRowsFrameFunction(
                                TestDefaults.createLongFunction(x -> x.getLong(2)),
                                MaxDoubleWindowFunctionFactory.GREATER_THAN,
                                MaxDoubleWindowFunctionFactory.NAME
                        );
                    }
                    return new MaxDoubleWindowFunctionFactory.MaxMinOverRowsFrameFunction(
                            TestDefaults.createLongFunction(x -> x.getLong(2)),
                            rangeLo,
                            rangeHi,
                            TestDefaults.createMemoryCARW(),
                            TestDefaults.createMemoryCARW(),
                            MaxDoubleWindowFunctionFactory.GREATER_THAN,
                            MaxDoubleWindowFunctionFactory.NAME
                    );
                },
                Double::max
        );
    }

    @Test
    public void testMaxRangeFuzz() throws Exception {
        fuzzTestBase(
                TestUtils.generateRandom(LOG),
                false,
                false,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(65536),
                rnd -> -rnd.nextLong(65536),
                (rangeLo, rangeHi) -> new MaxDoubleWindowFunctionFactory.MaxMinOverRangeFrameFunction(
                        rangeLo,
                        rangeHi,
                        TestDefaults.createLongFunction(x -> x.getLong(2)),
                        configuration,
                        TestDefaults.createMemoryCARW(),
                        TestDefaults.createMemoryCARW(),
                        0,
                        MaxDoubleWindowFunctionFactory.GREATER_THAN,
                        MaxDoubleWindowFunctionFactory.NAME
                ),
                Double::max
        );
    }

    @Test
    public void testMaxRangeUnbounded() {
        MaxDoubleWindowFunctionFactory.MaxMinOverRangeFrameFunction f = new MaxDoubleWindowFunctionFactory.MaxMinOverRangeFrameFunction(
                Long.MIN_VALUE,
                0,
                TestDefaults.createLongFunction(x -> x.getLong(2)),
                configuration,
                TestDefaults.createMemoryCARW(),
                TestDefaults.createMemoryCARW(),
                0,
                MaxDoubleWindowFunctionFactory.GREATER_THAN,
                MaxDoubleWindowFunctionFactory.NAME
        );
        long a = -1930193130;
        long b = -1137976524;
        long c = -1137976524;
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 46, 19, a));
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 119, 19, b));
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 200, 19, c));
        Assert.assertEquals(b, f.getDouble(null), 1e-6);
    }

    @Test
    public void testMaxWithPartitionBufferResize() {
        MaxDoubleWindowFunctionFactory.MaxMinOverPartitionRangeFrameFunction f = new MaxDoubleWindowFunctionFactory.MaxMinOverPartitionRangeFrameFunction(
                TestDefaults.createOrderedMap(new SingleColumnType(columnTypes[1]), MaxDoubleWindowFunctionFactory.MAX_OVER_PARTITION_RANGE_COLUMN_TYPES),
                TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                Long.MIN_VALUE,
                -13402,
                TestDefaults.createLongFunction(x -> x.getLong(2)),
                TestDefaults.createMemoryCARW(),
                null,
                2,
                0,
                MaxDoubleWindowFunctionFactory.GREATER_THAN,
                MaxDoubleWindowFunctionFactory.NAME
        );
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 1472, 6, (long) 1));
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 15169, 6, (long) 2));
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 18579, 6, (long) 3));
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 24096, 6, (long) 4));
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 29170, 6, (long) 5));
        Assert.assertEquals(2, f.getDouble(null), 0);
    }

    @Test
    public void testMaxWithPartitionRangeUnbounded() {
        MaxDoubleWindowFunctionFactory.MaxMinOverPartitionRangeFrameFunction f = new MaxDoubleWindowFunctionFactory.MaxMinOverPartitionRangeFrameFunction(
                TestDefaults.createOrderedMap(new SingleColumnType(columnTypes[1]), MaxDoubleWindowFunctionFactory.MAX_OVER_PARTITION_RANGE_COLUMN_TYPES),
                TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                Long.MIN_VALUE,
                0,
                TestDefaults.createLongFunction(x -> x.getLong(2)),
                TestDefaults.createMemoryCARW(),
                null,
                1024,
                0,
                MaxDoubleWindowFunctionFactory.GREATER_THAN,
                MaxDoubleWindowFunctionFactory.NAME
        );
        long a = -1930193130;
        long b = -1137976524;
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 46, 19, a));
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 119, 19, b));
        Assert.assertEquals(b, f.getDouble(null), 1e-6);
    }

    @Test
    public void testMinOverPartitionRangeFuzz() throws Exception {
        fuzzTestBase(
                TestUtils.generateRandom(LOG),
                true,
                false,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(65536),
                rnd -> -rnd.nextLong(65536),
                (rangeLo, rangeHi) -> new MaxDoubleWindowFunctionFactory.MaxMinOverPartitionRangeFrameFunction(
                        TestDefaults.createOrderedMap(new SingleColumnType(columnTypes[1]), rangeLo == Long.MIN_VALUE ?
                                MaxDoubleWindowFunctionFactory.MAX_OVER_PARTITION_RANGE_COLUMN_TYPES :
                                MaxDoubleWindowFunctionFactory.MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES),
                        TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                        TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                        rangeLo,
                        rangeHi,
                        TestDefaults.createLongFunction(x -> x.getLong(2)),
                        TestDefaults.createMemoryCARW(),
                        TestDefaults.createMemoryCARW(),
                        2,
                        0,
                        MinDoubleWindowFunctionFactory.LESS_THAN,
                        MinDoubleWindowFunctionFactory.NAME
                ),
                Double::min
        );
    }

    @Test
    public void testMinOverPartitionRangeWithBothBounds() {
        MaxDoubleWindowFunctionFactory.MaxMinOverPartitionRangeFrameFunction f = new MaxDoubleWindowFunctionFactory.MaxMinOverPartitionRangeFrameFunction(
                TestDefaults.createOrderedMap(new SingleColumnType(columnTypes[1]), MaxDoubleWindowFunctionFactory.MAX_OVER_PARTITION_RANGE_BOUNDED_COLUMN_TYPES),
                TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                -2,
                -2,
                TestDefaults.createLongFunction(x -> x.getLong(2)),
                TestDefaults.createMemoryCARW(),
                TestDefaults.createMemoryCARW(),
                configuration.getSqlWindowInitialRangeBufferSize(),
                0,
                MinDoubleWindowFunctionFactory.LESS_THAN,
                MinDoubleWindowFunctionFactory.NAME
        );
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 1, 2, (long) 1));
        Assert.assertEquals(Double.NaN, f.getDouble(null), 0);

        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 2, 2, (long) 2));
        Assert.assertEquals(Double.NaN, f.getDouble(null), 0);

        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 4, 2, (long) 4));
        Assert.assertEquals(2, f.getDouble(null), 0);
    }

    @Test
    public void testMinOverPartitionRowsFuzz() throws Exception {
        fuzzTestBase(
                TestUtils.generateRandom(LOG),
                true,
                true,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(1024),
                rnd -> -rnd.nextLong(1024) - 1,
                (rangeLo, rangeHi) -> new MaxDoubleWindowFunctionFactory.MaxMinOverPartitionRowsFrameFunction(
                        TestDefaults.createOrderedMap(new SingleColumnType(columnTypes[1]),
                                rangeLo == Long.MIN_VALUE ? MaxDoubleWindowFunctionFactory.MAX_OVER_PARTITION_ROWS_COLUMN_TYPES :
                                        MaxDoubleWindowFunctionFactory.MAX_OVER_PARTITION_ROWS_BOUNDED_COLUMN_TYPES),
                        TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                        TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                        rangeLo,
                        rangeHi,
                        TestDefaults.createLongFunction(x -> x.getLong(2)),
                        TestDefaults.createMemoryCARW(),
                        TestDefaults.createMemoryCARW(),
                        MinDoubleWindowFunctionFactory.LESS_THAN,
                        MinDoubleWindowFunctionFactory.NAME
                ),
                Double::min
        );
    }

    @Test
    public void testMinOverRowsFuzz() throws Exception {
        fuzzTestBase(
                TestUtils.generateRandom(LOG),
                false,
                true,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(1024),
                rnd -> -rnd.nextLong(1024),
                (rangeLo, rangeHi) -> {
                    if (rangeLo == Long.MIN_VALUE && rangeHi == 0) {
                        return new MaxDoubleWindowFunctionFactory.MaxMinOverUnboundedRowsFrameFunction(
                                TestDefaults.createLongFunction(x -> x.getLong(2)),
                                MinDoubleWindowFunctionFactory.LESS_THAN,
                                MinDoubleWindowFunctionFactory.NAME
                        );
                    }
                    return new MaxDoubleWindowFunctionFactory.MaxMinOverRowsFrameFunction(
                            TestDefaults.createLongFunction(x -> x.getLong(2)),
                            rangeLo,
                            rangeHi,
                            TestDefaults.createMemoryCARW(),
                            TestDefaults.createMemoryCARW(),
                            MinDoubleWindowFunctionFactory.LESS_THAN,
                            MinDoubleWindowFunctionFactory.NAME
                    );
                },
                Double::min
        );
    }

    @Test
    public void testMinRangeFuzz() throws Exception {
        fuzzTestBase(
                TestUtils.generateRandom(LOG),
                false,
                false,
                rnd -> rnd.nextInt(8) == 0 ? Long.MIN_VALUE : -rnd.nextLong(65536),
                rnd -> -rnd.nextLong(65536),
                (rangeLo, rangeHi) -> new MaxDoubleWindowFunctionFactory.MaxMinOverRangeFrameFunction(
                        rangeLo,
                        rangeHi,
                        TestDefaults.createLongFunction(x -> x.getLong(2)),
                        configuration,
                        TestDefaults.createMemoryCARW(),
                        TestDefaults.createMemoryCARW(),
                        0,
                        MinDoubleWindowFunctionFactory.LESS_THAN,
                        MinDoubleWindowFunctionFactory.NAME
                ),
                Double::min
        );
    }

    @Test
    public void testMinRangeUnbounded() {
        MaxDoubleWindowFunctionFactory.MaxMinOverRangeFrameFunction f = new MaxDoubleWindowFunctionFactory.MaxMinOverRangeFrameFunction(
                Long.MIN_VALUE,
                0,
                TestDefaults.createLongFunction(x -> x.getLong(2)),
                configuration,
                TestDefaults.createMemoryCARW(),
                TestDefaults.createMemoryCARW(),
                0,
                MinDoubleWindowFunctionFactory.LESS_THAN,
                MinDoubleWindowFunctionFactory.NAME
        );
        long a = -1930193130;
        long b = -1137976524;
        long c = -1137976524;
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 46, 19, a));
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 119, 19, b));
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 200, 19, c));
        Assert.assertEquals(a, f.getDouble(null), 1e-6);
    }

    @Test
    public void testMinWithPartitionBufferResize() {
        MaxDoubleWindowFunctionFactory.MaxMinOverPartitionRangeFrameFunction f = new MaxDoubleWindowFunctionFactory.MaxMinOverPartitionRangeFrameFunction(
                TestDefaults.createOrderedMap(new SingleColumnType(columnTypes[1]), MaxDoubleWindowFunctionFactory.MAX_OVER_PARTITION_RANGE_COLUMN_TYPES),
                TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                Long.MIN_VALUE,
                -13402,
                TestDefaults.createLongFunction(x -> x.getLong(2)),
                TestDefaults.createMemoryCARW(),
                null,
                2,
                0,
                MinDoubleWindowFunctionFactory.LESS_THAN,
                MinDoubleWindowFunctionFactory.NAME
        );
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 1472, 6, (long) 1));
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 15169, 6, (long) 2));
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 18579, 6, (long) 3));
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 24096, 6, (long) 4));
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 29170, 6, (long) 5));
        Assert.assertEquals(1, f.getDouble(null), 0);
    }

    @Test
    public void testMinWithPartitionRangeUnbounded() {
        MaxDoubleWindowFunctionFactory.MaxMinOverPartitionRangeFrameFunction f = new MaxDoubleWindowFunctionFactory.MaxMinOverPartitionRangeFrameFunction(
                TestDefaults.createOrderedMap(new SingleColumnType(columnTypes[1]), MaxDoubleWindowFunctionFactory.MAX_OVER_PARTITION_RANGE_COLUMN_TYPES),
                TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                Long.MIN_VALUE,
                0,
                TestDefaults.createLongFunction(x -> x.getLong(2)),
                TestDefaults.createMemoryCARW(),
                null,
                1024,
                0,
                MinDoubleWindowFunctionFactory.LESS_THAN,
                MinDoubleWindowFunctionFactory.NAME
        );
        long a = -1930193130;
        long b = -1137976524;
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 46, 19, a));
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 119, 19, b));
        Assert.assertEquals(a, f.getDouble(null), 1e-6);
    }

    @Test
    public void testSumOverPartitionRangeWithBothBounds() {
        SumDoubleWindowFunctionFactory.SumOverPartitionRangeFrameFunction f = new SumDoubleWindowFunctionFactory.SumOverPartitionRangeFrameFunction(
                TestDefaults.createOrderedMap(new SingleColumnType(columnTypes[1]), AvgDoubleWindowFunctionFactory.AVG_OVER_PARTITION_RANGE_COLUMN_TYPES),
                TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                -2,
                -2,
                TestDefaults.createLongFunction(x -> x.getLong(2)),
                TestDefaults.createMemoryCARW(),
                1024,
                0
        );
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 1, 2, (long) 1));
        Assert.assertEquals(Double.NaN, f.getDouble(null), 0);

        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 2, 2, (long) 2));
        Assert.assertEquals(Double.NaN, f.getDouble(null), 0);

        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 4, 2, (long) 4));
        Assert.assertEquals(2, f.getDouble(null), 0);
    }

    @Test
    public void testSumRangeUnbounded() {
        SumDoubleWindowFunctionFactory.SumOverRangeFrameFunction f = new SumDoubleWindowFunctionFactory.SumOverRangeFrameFunction(
                Long.MIN_VALUE,
                0,
                TestDefaults.createLongFunction(x -> x.getLong(2)),
                1024,
                TestDefaults.createMemoryCARW(),
                0
        );
        long a = -1930193130;
        long b = -1137976524;
        long c = -1137976524;
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 46, 19, a));
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 119, 19, b));
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 200, 19, c));
        Assert.assertEquals(a + b + c, f.getDouble(null), 1e-6);
    }

    @Test
    public void testSumWithPartitionBufferResize() {
        SumDoubleWindowFunctionFactory.SumOverPartitionRangeFrameFunction f = new SumDoubleWindowFunctionFactory.SumOverPartitionRangeFrameFunction(
                TestDefaults.createOrderedMap(new SingleColumnType(columnTypes[1]), AvgDoubleWindowFunctionFactory.AVG_OVER_PARTITION_RANGE_COLUMN_TYPES),
                TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                Long.MIN_VALUE,
                -13402,
                TestDefaults.createLongFunction(x -> x.getLong(2)),
                TestDefaults.createMemoryCARW(),
                2,
                0
        );
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 1472, 6, (long) 1));
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 15169, 6, (long) 2));
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 18579, 6, (long) 3));
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 24096, 6, (long) 4));
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 29170, 6, (long) 5));
        Assert.assertEquals(1 + 2, f.getDouble(null), 0);
    }

    @Test
    public void testSumWithPartitionRangeUnbounded() {
        SumDoubleWindowFunctionFactory.SumOverPartitionRangeFrameFunction f = new SumDoubleWindowFunctionFactory.SumOverPartitionRangeFrameFunction(
                TestDefaults.createOrderedMap(new SingleColumnType(columnTypes[1]), AvgDoubleWindowFunctionFactory.AVG_OVER_PARTITION_RANGE_COLUMN_TYPES),
                TestDefaults.createVirtualRecord(TestDefaults.createIntFunction(x -> x.getInt(1))),
                TestDefaults.createRecordSink((r, w) -> w.putInt(r.getInt(0))),
                Long.MIN_VALUE,
                0,
                TestDefaults.createLongFunction(x -> x.getLong(2)),
                TestDefaults.createMemoryCARW(),
                1024,
                0
        );
        long a = -1930193130;
        long b = -1137976524;
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 46, 19, a));
        f.computeNext(TestDefaults.createRecord(columnTypes, (long) 119, 19, b));
        Assert.assertEquals(a + b, f.getDouble(null), 1e-6);
    }

    private void fuzzTestBase(
            Rnd rnd,
            boolean partitioned,
            boolean rows,
            java.util.function.Function<Rnd, Long> rangeLoGen,
            java.util.function.Function<Rnd, Long> rangeHiGen,
            java.util.function.BiFunction<Long, Long, BaseWindowFunction> windowFunctionFactory,
            java.util.function.BiFunction<Double, Double, Double> sum
    ) throws Exception {
        fuzzTestBase(rnd, partitioned, rows, false, rangeLoGen, rangeHiGen, windowFunctionFactory, sum);
    }

    private void fuzzTestBase(
            Rnd rnd,
            boolean partitioned,
            boolean rows,
            boolean nullable,
            java.util.function.Function<Rnd, Long> rangeLoGen,
            java.util.function.Function<Rnd, Long> rangeHiGen,
            java.util.function.BiFunction<Long, Long, BaseWindowFunction> windowFunctionFactory,
            java.util.function.BiFunction<Double, Double, Double> sum
    ) throws Exception {
        final int count = rnd.nextInt(1024) + 1;
        Record[] records = nullable ?
                generateTestRecordsWithNull(rnd, count, 1 + rnd.nextInt(32), 1 + rnd.nextLong(65536)) :
                generateTestRecords(rnd, count, 1 + rnd.nextInt(32), 1 + rnd.nextLong(65536));
        Arrays.sort(records, Comparator.comparingLong(a -> a.getLong(0)));
        long rangeLo = rangeLoGen.apply(rnd);
        long rangeHi = rangeHiGen.apply(rnd);
        if (rangeLo > rangeHi) {
            long tmp = rangeLo;
            rangeLo = rangeHi;
            rangeHi = tmp;
        }

        try (BaseWindowFunction f = windowFunctionFactory.apply(rangeLo, rangeHi)) {
            for (int s = 0; s < records.length; s++) {
                try {
                    f.computeNext(records[s]);
                } catch (Error e) {
                    throw new Exception(String.format(
                            "count=%d, rangeLo=%d, rangeHi=%d, s=%d, data=[%s]",
                            count, rangeLo, rangeHi, s,
                            Arrays.stream(records).map(x -> String.format("%d:%d:%d", x.getLong(0), x.getInt(1), x.getLong(2))).collect(Collectors.joining(", "))
                    ), e);
                }
                double expected = Double.NaN;
                int row = 0;
                for (int q = s; q >= 0; q--) {
                    if (partitioned && records[q].getInt(1) != records[s].getInt(1)) {
                        continue;
                    }
                    if (!rows) {
                        if ((rangeLo == Long.MIN_VALUE || records[q].getLong(0) >= records[s].getLong(0) + rangeLo) && records[q].getLong(0) <= records[s].getLong(0) + rangeHi) {
                            long l = records[q].getLong(2);
                            double d = Numbers.LONG_NULL == l ? Double.NaN : (double) l;
                            if (Double.isNaN(expected)) {
                                expected = d;
                            } else {
                                expected = sum.apply(expected, d);
                            }
                        }
                        if (rangeLo != Long.MIN_VALUE && records[q].getLong(0) < records[s].getLong(0) + rangeLo) {
                            break;
                        }
                    } else {
                        if (row >= rangeLo && row <= rangeHi) {
                            long l = records[q].getLong(2);
                            double d = Numbers.LONG_NULL == l ? Double.NaN : (double) l;
                            if (Double.isNaN(expected)) {
                                expected = d;
                            } else {
                                expected = sum.apply(expected, d);
                            }
                        }
                        if (row < rangeLo) {
                            break;
                        }
                    }
                    row--;
                }
                if (Math.abs(expected - f.getDouble(null)) > 1e-6) {
                    Assert.fail(String.format(
                            "count=%d, rangeLo=%d, rangeHi=%d, s=%d, expected=%f, actual=%f, data=[%s]",
                            count, rangeLo, rangeHi, s, expected, f.getDouble(null),
                            Arrays.stream(records).map(x -> String.format("%d:%d:%d", x.getLong(0), x.getInt(1), x.getLong(2))).collect(Collectors.joining(", "))
                    ));
                }
            }
        }
    }

    private void fuzzTestLong(
            Rnd rnd,
            boolean partitioned,
            boolean rows,
            java.util.function.Function<Rnd, Long> rangeLoGen,
            java.util.function.Function<Rnd, Long> rangeHiGen,
            java.util.function.BiFunction<Long, Long, BaseWindowFunction> windowFunctionFactory,
            java.util.function.BiFunction<Long, Long, Long> cou,
            CountFunctionFactoryHelper.IsRecordNotNull isRecordNotNull
    ) throws Exception {
        Function arg = LongColumn.newInstance(2);
        final int count = rnd.nextInt(1024) + 1;
        Record[] records = generateTestRecords(rnd, count, 1 + rnd.nextInt(32), 1 + rnd.nextLong(65536));
        Arrays.sort(records, Comparator.comparingLong(a -> a.getLong(0)));
        long rangeLo = rangeLoGen.apply(rnd);
        long rangeHi = rangeHiGen.apply(rnd);
        if (rangeLo > rangeHi) {
            long tmp = rangeLo;
            rangeLo = rangeHi;
            rangeHi = tmp;
        }

        try (BaseWindowFunction f = windowFunctionFactory.apply(rangeLo, rangeHi)) {
            for (int s = 0; s < records.length; s++) {
                try {
                    f.computeNext(records[s]);
                } catch (Error e) {
                    throw new Exception(String.format(
                            "count=%d, rangeLo=%d, rangeHi=%d, s=%d, data=[%s]",
                            count, rangeLo, rangeHi, s,
                            Arrays.stream(records).map(x -> String.format("%d:%d:%d", x.getLong(0), x.getInt(1), x.getLong(2))).collect(Collectors.joining(", "))
                    ), e);
                }
                long expected = 0;
                int row = 0;
                for (int q = s; q >= 0; q--) {
                    if (partitioned && records[q].getInt(1) != records[s].getInt(1)) {
                        continue;
                    }
                    if (!rows) {
                        if ((rangeLo == Long.MIN_VALUE || records[q].getLong(0) >= records[s].getLong(0) + rangeLo) && records[q].getLong(0) <= records[s].getLong(0) + rangeHi) {
                            expected = cou.apply(expected, (long) (isRecordNotNull.isNotNull(arg, records[q]) ? 1 : 0));
                        }
                        if (rangeLo != Long.MIN_VALUE && records[q].getLong(0) < records[s].getLong(0) + rangeLo) {
                            break;
                        }
                    } else {
                        if (row >= rangeLo && row <= rangeHi) {
                            expected = cou.apply(expected, (long) (isRecordNotNull.isNotNull(arg, records[q]) ? 1 : 0));
                        }
                        if (row < rangeLo) {
                            break;
                        }
                    }
                    row--;
                }
                if (expected != f.getLong(null)) {
                    Assert.fail(String.format(
                            "count=%d, rangeLo=%d, rangeHi=%d, s=%d, expected=%d, actual=%d, data=[%s]",
                            count, rangeLo, rangeHi, s, expected, f.getLong(null),
                            Arrays.stream(records).map(x -> String.format("%d:%d:%d", x.getLong(0), x.getInt(1), x.getLong(2))).collect(Collectors.joining(", "))
                    ));
                }
            }
        }
    }

    private Record[] generateTestRecords(Rnd rnd, int count, int partitionsLimit, long timestampLimit) {
        Record[] records = new Record[count];
        for (int i = 0; i < count; i++) {
            records[i] = TestDefaults.createRecord(columnTypes, rnd.nextLong(timestampLimit), rnd.nextInt(partitionsLimit), (long) rnd.nextInt());
        }
        return records;
    }

    private Record[] generateTestRecordsWithNull(Rnd rnd, int count, int partitionsLimit, long timestampLimit) {
        Record[] records = new Record[count];
        for (int i = 0; i < count; i++) {
            long l = rnd.nextInt(10000000);
            // 20% null value
            if (l > 8000000) {
                l = Numbers.LONG_NULL;
            }
            records[i] = TestDefaults.createRecord(columnTypes, rnd.nextLong(timestampLimit), rnd.nextInt(partitionsLimit), l);
        }
        return records;
    }
}