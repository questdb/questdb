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
import io.questdb.griffin.engine.functions.columns.ShortColumn;
import io.questdb.griffin.engine.functions.groupby.AvgShortGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstShortGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastShortGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.SumShortGroupByFunction;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class ShortGroupByFunctionBatchTest {
    private static final int COLUMN_INDEX = 789;
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
    public void testAvgShortBatch() {
        AvgShortGroupByFunction function = new AvgShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateShorts((short) 2, (short) 4, (short) 6);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(4.0, function.getDouble(value), 0.0);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testAvgShortSetEmpty() {
        AvgShortGroupByFunction function = new AvgShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testFirstShortBatch() {
        FirstShortGroupByFunction function = new FirstShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateShorts((short) 5, (short) 6, (short) 7);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(5, function.getShort(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testFirstShortBatchAllNull() {
        FirstShortGroupByFunction function = new FirstShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateShorts(Short.MIN_VALUE, (short) 1);
            function.computeBatch(value, ptr, 2);

            Assert.assertEquals(Short.MIN_VALUE, function.getShort(value));
        }
    }

    @Test
    public void testFirstShortBatchEmpty() {
        FirstShortGroupByFunction function = new FirstShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            function.computeBatch(value, 0, 0);

            Assert.assertEquals(0, function.getShort(value));
        }
    }

    @Test
    public void testFirstShortSetEmpty() {
        FirstShortGroupByFunction function = new FirstShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(0, function.getShort(value));
        }
    }

    @Test
    public void testLastShortBatch() {
        LastShortGroupByFunction function = new LastShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateShorts((short) 11, (short) 22, (short) 33);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(Numbers.LONG_NULL, value.getLong(0));
            Assert.assertEquals(33, function.getShort(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastShortBatchAllNull() {
        LastShortGroupByFunction function = new LastShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateShorts((short) 11, Short.MIN_VALUE);
            function.computeBatch(value, ptr, 2);

            Assert.assertEquals(Short.MIN_VALUE, function.getShort(value));
        }
    }

    @Test
    public void testLastShortSetEmpty() {
        LastShortGroupByFunction function = new LastShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(0, function.getShort(value));
        }
    }

    @Test
    public void testSumShortBatch() {
        SumShortGroupByFunction function = new SumShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putLong(0, 10);

            long ptr = allocateShorts((short) 1, (short) 2, (short) 3, (short) 4);
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(10L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testSumShortBatchAllZero() {
        SumShortGroupByFunction function = new SumShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateShorts((short) 0, (short) 0);
            function.computeBatch(value, ptr, 2);

            Assert.assertEquals(0L, function.getLong(value));
        }
    }

    @Test
    public void testSumShortBatchZeroCountKeepsExistingValue() {
        SumShortGroupByFunction function = new SumShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putLong(0, 55);

            function.computeBatch(value, 0, 0);

            Assert.assertEquals(55L, function.getLong(value));
        }
    }

    @Test
    public void testSumShortSetEmpty() {
        SumShortGroupByFunction function = new SumShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    private long allocateShorts(short... values) {
        if (lastAllocated != 0) {
            Unsafe.free(lastAllocated, lastSize, MemoryTag.NATIVE_DEFAULT);
        }
        lastSize = (long) values.length * Short.BYTES;
        lastAllocated = Unsafe.malloc(lastSize, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < values.length; i++) {
            Unsafe.getUnsafe().putShort(lastAllocated + (long) i * Short.BYTES, values[i]);
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
