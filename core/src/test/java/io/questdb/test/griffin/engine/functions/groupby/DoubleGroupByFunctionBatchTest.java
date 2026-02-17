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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.DoubleColumn;
import io.questdb.griffin.engine.functions.groupby.CountDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastNotNullDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MaxDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.SumDoubleGroupByFunction;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class DoubleGroupByFunctionBatchTest {
    private static final int COLUMN_INDEX = 123;
    private long lastAllocated;
    private long lastSize;

    @After
    public void tearDown() {
        if (lastAllocated != 0) {
            Unsafe.free(lastAllocated, lastSize, MemoryTag.NATIVE_DEFAULT);
            lastAllocated = 0;
            lastSize = 0;
        }
    }

    @Test
    public void testCountDoubleBatch() {
        CountDoubleGroupByFunction function = new CountDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(1.0, Double.NaN, 2.0, Double.NaN, 3.0);
            function.computeBatch(value, ptr, 5);

            Assert.assertEquals(3L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testCountDoubleBatchAllNaN() {
        CountDoubleGroupByFunction function = new CountDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(Double.NaN, Double.NaN, Double.NaN);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(0L, function.getLong(value));
        }
    }

    @Test
    public void testCountDoubleBatchZeroCountKeepsZero() {
        CountDoubleGroupByFunction function = new CountDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.computeBatch(value, 0, 0);

            Assert.assertEquals(0L, function.getLong(value));
        }
    }

    @Test
    public void testCountDoubleSetEmpty() {
        CountDoubleGroupByFunction function = new CountDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(0L, function.getLong(value));
        }
    }

    @Test
    public void testFirstDoubleBatch() {
        FirstDoubleGroupByFunction function = new FirstDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(5.5, 6.6, 7.7);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(5.5, function.getDouble(value), 0.0);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testFirstDoubleBatchAllNaN() {
        FirstDoubleGroupByFunction function = new FirstDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(Double.NaN, 1.0);
            function.computeBatch(value, ptr, 2);

            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testFirstDoubleBatchEmpty() {
        FirstDoubleGroupByFunction function = new FirstDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateDoubles();
            function.computeBatch(value, ptr, 0);

            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testFirstDoubleBatchNotCalled() {
        FirstDoubleGroupByFunction function = new FirstDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testFirstDoubleSetEmpty() {
        FirstDoubleGroupByFunction function = new FirstDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testFirstNotNullDoubleBatch() {
        FirstNotNullDoubleGroupByFunction function = new FirstNotNullDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(Double.NaN, 7.7, Double.NaN);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(7.7, function.getDouble(value), 0.0);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastDoubleBatch() {
        LastDoubleGroupByFunction function = new LastDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateDoubles(11.0, 22.0, 33.0);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(Numbers.LONG_NULL, value.getLong(0));
            Assert.assertEquals(33.0, function.getDouble(value), 0.0);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastDoubleBatchAllNaN() {
        LastDoubleGroupByFunction function = new LastDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateDoubles(11.0, Double.NaN);
            function.computeBatch(value, ptr, 2);

            Assert.assertEquals(Numbers.LONG_NULL, value.getLong(0));
            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testLastDoubleSetEmpty() {
        LastDoubleGroupByFunction function = new LastDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testLastNotNullDoubleBatch() {
        LastNotNullDoubleGroupByFunction function = new LastNotNullDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateDoubles(Double.NaN, 5.5, Double.NaN, 6.6);
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(6.6, function.getDouble(value), 0.0);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMaxDoubleBatch() {
        MaxDoubleGroupByFunction function = new MaxDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putDouble(0, -999.0);

            long ptr = allocateDoubles(-10.0, Double.NaN, 15.5, 7.0);
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(15.5, function.getDouble(value), 0.0);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMaxDoubleBatchAllNaN() {
        MaxDoubleGroupByFunction function = new MaxDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(Double.NaN, Double.NaN, Double.NaN);
            function.computeBatch(value, ptr, 3);

            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testMaxDoubleSetEmpty() {
        MaxDoubleGroupByFunction function = new MaxDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testMinDoubleBatch() {
        MinDoubleGroupByFunction function = new MinDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putDouble(0, 999.0);

            long ptr = allocateDoubles(Double.NaN, 4.0, 2.5, 3.0);
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(2.5, function.getDouble(value), 0.0);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMinDoubleBatchAllNaN() {
        MinDoubleGroupByFunction function = new MinDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(Double.NaN, Double.NaN);
            function.computeBatch(value, ptr, 2);

            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testMinDoubleSetEmpty() {
        MinDoubleGroupByFunction function = new MinDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testSumDoubleBatch() {
        SumDoubleGroupByFunction function = new SumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putDouble(0, 123.0);

            long ptr = allocateDoubles(1.0, Double.NaN, 2.5, 3.5);
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(7.0, function.getDouble(value), 0.0);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testSumDoubleBatchAllNaN() {
        SumDoubleGroupByFunction function = new SumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDoubles(Double.NaN, Double.NaN);
            function.computeBatch(value, ptr, 2);

            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testSumDoubleBatchZeroCountKeepsExistingValue() {
        SumDoubleGroupByFunction function = new SumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putDouble(0, 55.0);

            function.computeBatch(value, 0, 0);

            Assert.assertEquals(55.0, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testSumDoubleSetEmpty() {
        SumDoubleGroupByFunction function = new SumDoubleGroupByFunction(DoubleColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    private long allocateDoubles(double... values) {
        if (lastAllocated != 0) {
            Unsafe.free(lastAllocated, lastSize, MemoryTag.NATIVE_DEFAULT);
        }
        lastSize = (long) values.length * Double.BYTES;
        lastAllocated = Unsafe.malloc(lastSize, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < values.length; i++) {
            Unsafe.getUnsafe().putDouble(lastAllocated + (long) i * Double.BYTES, values[i]);
        }
        return lastAllocated;
    }

    private SimpleMapValue prepare(GroupByFunction function) {
        var columnTypes = new ArrayColumnTypes();
        function.initValueTypes(columnTypes);
        SimpleMapValue value = new SimpleMapValue(columnTypes.getColumnCount());
        function.initValueIndex(0);
        function.setEmpty(value);
        return value;
    }
}
