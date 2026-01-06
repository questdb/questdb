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
import io.questdb.griffin.engine.functions.columns.IntColumn;
import io.questdb.griffin.engine.functions.groupby.CountIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastNotNullIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MaxIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.SumIntGroupByFunction;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class IntGroupByFunctionBatchTest {
    private static final int COLUMN_INDEX = 321;
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
    public void testCountIntBatch() {
        CountIntGroupByFunction function = new CountIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(1, Numbers.INT_NULL, 2, Numbers.INT_NULL, 3);
            function.computeBatch(value, ptr, 5);

            Assert.assertEquals(3L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testCountIntBatchAllNull() {
        CountIntGroupByFunction function = new CountIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(Numbers.INT_NULL, Numbers.INT_NULL, Numbers.INT_NULL);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(0L, function.getLong(value));
        }
    }

    @Test
    public void testCountIntBatchZeroCountKeepsZero() {
        CountIntGroupByFunction function = new CountIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.computeBatch(value, 0, 0);

            Assert.assertEquals(0L, function.getLong(value));
        }
    }

    @Test
    public void testCountIntSetEmpty() {
        CountIntGroupByFunction function = new CountIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(0L, function.getLong(value));
        }
    }

    @Test
    public void testFirstIntBatch() {
        FirstIntGroupByFunction function = new FirstIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(5, 6, 7);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(5, function.getInt(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testFirstIntBatchAllNull() {
        FirstIntGroupByFunction function = new FirstIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(Numbers.INT_NULL, 1);
            function.computeBatch(value, ptr, 2);

            Assert.assertEquals(Numbers.INT_NULL, function.getInt(value));
        }
    }

    @Test
    public void testFirstIntBatchEmpty() {
        FirstIntGroupByFunction function = new FirstIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            function.computeBatch(value, 0, 0);

            Assert.assertEquals(Numbers.INT_NULL, function.getInt(value));
        }
    }

    @Test
    public void testFirstIntBatchNotCalled() {
        FirstIntGroupByFunction function = new FirstIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            Assert.assertEquals(Numbers.INT_NULL, function.getInt(value));
        }
    }

    @Test
    public void testFirstIntSetEmpty() {
        FirstIntGroupByFunction function = new FirstIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(Numbers.INT_NULL, function.getInt(value));
        }
    }

    @Test
    public void testFirstNotNullIntBatch() {
        FirstNotNullIntGroupByFunction function = new FirstNotNullIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(Numbers.INT_NULL, 42, Numbers.INT_NULL);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(42, function.getInt(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastIntBatch() {
        LastIntGroupByFunction function = new LastIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateInts(11, 22, 33);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(Numbers.LONG_NULL, value.getLong(0));
            Assert.assertEquals(33, function.getInt(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastIntBatchAllNull() {
        LastIntGroupByFunction function = new LastIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateInts(11, Numbers.INT_NULL);
            function.computeBatch(value, ptr, 2);

            Assert.assertEquals(Numbers.LONG_NULL, value.getLong(0));
            Assert.assertEquals(Numbers.INT_NULL, function.getInt(value));
        }
    }

    @Test
    public void testLastIntSetEmpty() {
        LastIntGroupByFunction function = new LastIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(Numbers.INT_NULL, function.getInt(value));
        }
    }

    @Test
    public void testLastNotNullIntBatch() {
        LastNotNullIntGroupByFunction function = new LastNotNullIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateInts(Numbers.INT_NULL, 10, Numbers.INT_NULL, 20);
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(20, function.getInt(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMaxIntBatch() {
        MaxIntGroupByFunction function = new MaxIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putInt(0, -999);

            long ptr = allocateInts(-10, Numbers.INT_NULL, 15, 7);
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(15, function.getInt(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMaxIntBatchAllNull() {
        MaxIntGroupByFunction function = new MaxIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(Numbers.INT_NULL, Numbers.INT_NULL, Numbers.INT_NULL);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(Numbers.INT_NULL, function.getInt(value));
        }
    }

    @Test
    public void testMaxIntSetEmpty() {
        MaxIntGroupByFunction function = new MaxIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(Numbers.INT_NULL, function.getInt(value));
        }
    }

    @Test
    public void testMinIntBatch() {
        MinIntGroupByFunction function = new MinIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putInt(0, 999);

            long ptr = allocateInts(Numbers.INT_NULL, 4, 2, 3);
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(2, function.getInt(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMinIntBatchAllNull() {
        MinIntGroupByFunction function = new MinIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(Numbers.INT_NULL, Numbers.INT_NULL);
            function.computeBatch(value, ptr, 2);

            Assert.assertEquals(Numbers.INT_NULL, function.getInt(value));
        }
    }

    @Test
    public void testMinIntSetEmpty() {
        MinIntGroupByFunction function = new MinIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(Numbers.INT_NULL, function.getInt(value));
        }
    }

    @Test
    public void testSumIntBatch() {
        SumIntGroupByFunction function = new SumIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putLong(0, 123);

            long ptr = allocateInts(1, 2, 3, 4);
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(10L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testSumIntBatchAllNull() {
        SumIntGroupByFunction function = new SumIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(Numbers.INT_NULL, Numbers.INT_NULL);
            function.computeBatch(value, ptr, 2);

            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    @Test
    public void testSumIntBatchZeroCountKeepsExistingValue() {
        SumIntGroupByFunction function = new SumIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putLong(0, 55);

            function.computeBatch(value, 0, 0);

            Assert.assertEquals(55L, function.getLong(value));
        }
    }

    @Test
    public void testSumIntSetEmpty() {
        SumIntGroupByFunction function = new SumIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    private long allocateInts(int... values) {
        if (lastAllocated != 0) {
            Unsafe.free(lastAllocated, lastSize, MemoryTag.NATIVE_DEFAULT);
        }
        lastSize = (long) values.length * Integer.BYTES;
        lastAllocated = Unsafe.malloc(lastSize, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < values.length; i++) {
            Unsafe.getUnsafe().putInt(lastAllocated + (long) i * Integer.BYTES, values[i]);
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
