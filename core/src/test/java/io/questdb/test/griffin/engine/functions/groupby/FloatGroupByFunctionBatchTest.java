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
import io.questdb.griffin.engine.functions.columns.FloatColumn;
import io.questdb.griffin.engine.functions.groupby.CountFloatGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstFloatGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullFloatGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastFloatGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastNotNullFloatGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MaxFloatGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinFloatGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.SumFloatGroupByFunction;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class FloatGroupByFunctionBatchTest {
    private static final int COLUMN_INDEX = 567;
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
    public void testCountFloatBatch() {
        CountFloatGroupByFunction function = new CountFloatGroupByFunction(FloatColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateFloats(1.5f, Float.NaN, 2.5f, Float.POSITIVE_INFINITY, 3.5f);
            function.computeBatch(value, ptr, 5);

            Assert.assertEquals(3L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testCountFloatBatchAllNaN() {
        CountFloatGroupByFunction function = new CountFloatGroupByFunction(FloatColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateFloats(Float.NaN, Float.NaN, Float.NaN);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(0L, function.getLong(value));
        }
    }

    @Test
    public void testCountFloatBatchZeroCountKeepsZero() {
        CountFloatGroupByFunction function = new CountFloatGroupByFunction(FloatColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.computeBatch(value, 0, 0);

            Assert.assertEquals(0L, function.getLong(value));
        }
    }

    @Test
    public void testCountFloatSetEmpty() {
        CountFloatGroupByFunction function = new CountFloatGroupByFunction(FloatColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(0L, function.getLong(value));
        }
    }

    @Test
    public void testFirstFloatBatch() {
        FirstFloatGroupByFunction function = new FirstFloatGroupByFunction(FloatColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateFloats(5.5f, 6.6f, 7.7f);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(5.5f, function.getFloat(value), 0.000001f);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testFirstFloatBatchAllNaN() {
        FirstFloatGroupByFunction function = new FirstFloatGroupByFunction(FloatColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateFloats(Float.NaN, 1.0f);
            function.computeBatch(value, ptr, 2);

            Assert.assertTrue(Float.isNaN(function.getFloat(value)));
        }
    }

    @Test
    public void testFirstFloatBatchEmpty() {
        FirstFloatGroupByFunction function = new FirstFloatGroupByFunction(FloatColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            function.computeBatch(value, 0, 0);

            Assert.assertTrue(Float.isNaN(function.getFloat(value)));
        }
    }

    @Test
    public void testFirstFloatBatchNotCalled() {
        FirstFloatGroupByFunction function = new FirstFloatGroupByFunction(FloatColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            Assert.assertTrue(Float.isNaN(function.getFloat(value)));
        }
    }

    @Test
    public void testFirstFloatSetEmpty() {
        FirstFloatGroupByFunction function = new FirstFloatGroupByFunction(FloatColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertTrue(Float.isNaN(function.getFloat(value)));
        }
    }

    @Test
    public void testFirstNotNullFloatBatch() {
        FirstNotNullFloatGroupByFunction function = new FirstNotNullFloatGroupByFunction(FloatColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateFloats(Float.NaN, 4.4f, Float.NaN);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(4.4f, function.getFloat(value), 0.0f);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastFloatBatch() {
        LastFloatGroupByFunction function = new LastFloatGroupByFunction(FloatColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateFloats(11.0f, 22.0f, 33.0f);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(Numbers.LONG_NULL, value.getLong(0));
            Assert.assertEquals(33.0f, function.getFloat(value), 0.000001f);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastFloatBatchAllNaN() {
        LastFloatGroupByFunction function = new LastFloatGroupByFunction(FloatColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateFloats(11.0f, Float.NaN);
            function.computeBatch(value, ptr, 2);

            Assert.assertTrue(Float.isNaN(function.getFloat(value)));
        }
    }

    @Test
    public void testLastFloatSetEmpty() {
        LastFloatGroupByFunction function = new LastFloatGroupByFunction(FloatColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertTrue(Float.isNaN(function.getFloat(value)));
        }
    }

    @Test
    public void testLastNotNullFloatBatch() {
        LastNotNullFloatGroupByFunction function = new LastNotNullFloatGroupByFunction(FloatColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateFloats(Float.NaN, 1.5f, Float.NaN, 2.5f);
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(2.5f, function.getFloat(value), 0.0f);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMaxFloatBatch() {
        MaxFloatGroupByFunction function = new MaxFloatGroupByFunction(FloatColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putFloat(0, -999.0f);

            long ptr = allocateFloats(-10.0f, Float.NaN, 15.5f, 7.0f);
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(15.5f, function.getFloat(value), 0.000001f);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMaxFloatBatchAllNaN() {
        MaxFloatGroupByFunction function = new MaxFloatGroupByFunction(FloatColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateFloats(Float.NaN, Float.NaN);
            function.computeBatch(value, ptr, 2);

            Assert.assertTrue(Float.isNaN(function.getFloat(value)));
        }
    }

    @Test
    public void testMaxFloatSetEmpty() {
        MaxFloatGroupByFunction function = new MaxFloatGroupByFunction(FloatColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertTrue(Float.isNaN(function.getFloat(value)));
        }
    }

    @Test
    public void testMinFloatBatch() {
        MinFloatGroupByFunction function = new MinFloatGroupByFunction(FloatColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putFloat(0, 999.0f);

            long ptr = allocateFloats(Float.NaN, 4.0f, 2.5f, 3.0f);
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(2.5f, function.getFloat(value), 0.000001f);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMinFloatBatchAllNaN() {
        MinFloatGroupByFunction function = new MinFloatGroupByFunction(FloatColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateFloats(Float.NaN, Float.NaN);
            function.computeBatch(value, ptr, 2);

            Assert.assertTrue(Float.isNaN(function.getFloat(value)));
        }
    }

    @Test
    public void testMinFloatSetEmpty() {
        MinFloatGroupByFunction function = new MinFloatGroupByFunction(FloatColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertTrue(Float.isNaN(function.getFloat(value)));
        }
    }

    @Test
    public void testSumFloatBatch() {
        SumFloatGroupByFunction function = new SumFloatGroupByFunction(FloatColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putFloat(0, 123.0f);

            long ptr = allocateFloats(1.0f, Float.NaN, 2.5f, 3.5f);
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(7.0f, function.getFloat(value), 0.000001f);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testSumFloatBatchAllNaN() {
        SumFloatGroupByFunction function = new SumFloatGroupByFunction(FloatColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateFloats(Float.NaN, Float.POSITIVE_INFINITY);
            function.computeBatch(value, ptr, 2);

            Assert.assertTrue(Float.isNaN(function.getFloat(value)));
        }
    }

    @Test
    public void testSumFloatBatchZeroCountKeepsExistingValue() {
        SumFloatGroupByFunction function = new SumFloatGroupByFunction(FloatColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putFloat(0, 55.0f);

            function.computeBatch(value, 0, 0);

            Assert.assertEquals(55.0f, function.getFloat(value), 0.000001f);
        }
    }

    @Test
    public void testSumFloatSetEmpty() {
        SumFloatGroupByFunction function = new SumFloatGroupByFunction(FloatColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertTrue(Float.isNaN(function.getFloat(value)));
        }
    }

    private long allocateFloats(float... values) {
        if (values.length == 0) {
            return 0;
        }
        if (lastAllocated != 0) {
            Unsafe.free(lastAllocated, lastSize, MemoryTag.NATIVE_DEFAULT);
        }
        lastSize = (long) values.length * Float.BYTES;
        lastAllocated = Unsafe.malloc(lastSize, MemoryTag.NATIVE_DEFAULT);
        long addr = lastAllocated;
        for (float value : values) {
            Unsafe.getUnsafe().putFloat(addr, value);
            addr += Float.BYTES;
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
