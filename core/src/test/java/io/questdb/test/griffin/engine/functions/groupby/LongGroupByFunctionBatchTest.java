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
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.groupby.CountLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastNotNullLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MaxLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.SumLongGroupByFunction;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class LongGroupByFunctionBatchTest {
    private static final int COLUMN_INDEX = 456;
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
    public void testCountLongBatch() {
        CountLongGroupByFunction function = new CountLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(1, Numbers.LONG_NULL, 2, Numbers.LONG_NULL, 3);
            function.computeBatch(value, ptr, 5);

            Assert.assertEquals(3L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testCountLongBatchAllNull() {
        CountLongGroupByFunction function = new CountLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(0L, function.getLong(value));
        }
    }

    @Test
    public void testCountLongBatchZeroCountKeepsZero() {
        CountLongGroupByFunction function = new CountLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.computeBatch(value, 0, 0);

            Assert.assertEquals(0L, function.getLong(value));
        }
    }

    @Test
    public void testCountLongSetEmpty() {
        CountLongGroupByFunction function = new CountLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(0L, function.getLong(value));
        }
    }

    @Test
    public void testFirstLongBatch() {
        FirstLongGroupByFunction function = new FirstLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(5, 6, 7);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(5L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testFirstLongBatchAllNull() {
        FirstLongGroupByFunction function = new FirstLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(Numbers.LONG_NULL, 1);
            function.computeBatch(value, ptr, 2);

            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    @Test
    public void testFirstLongBatchEmpty() {
        FirstLongGroupByFunction function = new FirstLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            function.computeBatch(value, 0, 0);

            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    @Test
    public void testFirstLongBatchNotCalled() {
        FirstLongGroupByFunction function = new FirstLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    @Test
    public void testFirstLongSetEmpty() {
        FirstLongGroupByFunction function = new FirstLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    @Test
    public void testFirstNotNullLongBatch() {
        FirstNotNullLongGroupByFunction function = new FirstNotNullLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(Numbers.LONG_NULL, 100L, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(100L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastLongBatch() {
        LastLongGroupByFunction function = new LastLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateLongs(11, 22, 33);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(Numbers.LONG_NULL, value.getLong(0));
            Assert.assertEquals(33L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastLongBatchAllNull() {
        LastLongGroupByFunction function = new LastLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateLongs(11, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 2);

            Assert.assertEquals(Numbers.LONG_NULL, value.getLong(0));
            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    @Test
    public void testLastLongSetEmpty() {
        LastLongGroupByFunction function = new LastLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    @Test
    public void testLastNotNullLongBatch() {
        LastNotNullLongGroupByFunction function = new LastNotNullLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateLongs(Numbers.LONG_NULL, 5L, Numbers.LONG_NULL, 10L);
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(10L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMaxLongBatch() {
        MaxLongGroupByFunction function = new MaxLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putLong(0, -999);

            long ptr = allocateLongs(-10, Numbers.LONG_NULL, 15, 7);
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(15L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMaxLongBatchAllNull() {
        MaxLongGroupByFunction function = new MaxLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    @Test
    public void testMaxLongSetEmpty() {
        MaxLongGroupByFunction function = new MaxLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    @Test
    public void testMinLongBatch() {
        MinLongGroupByFunction function = new MinLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putLong(0, 999);

            long ptr = allocateLongs(Numbers.LONG_NULL, 4, 2, 3);
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(2L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMinLongBatchAllNull() {
        MinLongGroupByFunction function = new MinLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(Numbers.LONG_NULL, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 2);

            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    @Test
    public void testMinLongSetEmpty() {
        MinLongGroupByFunction function = new MinLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    @Test
    public void testSumLongBatch() {
        SumLongGroupByFunction function = new SumLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putLong(0, 123);

            long ptr = allocateLongs(1, 2, 3, 4);
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(10L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testSumLongBatchAllNull() {
        SumLongGroupByFunction function = new SumLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(Numbers.LONG_NULL, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 2);

            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    @Test
    public void testSumLongBatchZeroCountKeepsExistingValue() {
        SumLongGroupByFunction function = new SumLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putLong(0, 55);

            function.computeBatch(value, 0, 0);

            Assert.assertEquals(55L, function.getLong(value));
        }
    }

    @Test
    public void testSumLongSetEmpty() {
        SumLongGroupByFunction function = new SumLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    private long allocateLongs(long... values) {
        if (lastAllocated != 0) {
            Unsafe.free(lastAllocated, lastSize, MemoryTag.NATIVE_DEFAULT);
        }
        lastSize = (long) values.length * Long.BYTES;
        lastAllocated = Unsafe.malloc(lastSize, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < values.length; i++) {
            Unsafe.getUnsafe().putLong(lastAllocated + (long) i * Long.BYTES, values[i]);
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
