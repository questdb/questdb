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
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.DateColumn;
import io.questdb.griffin.engine.functions.groupby.FirstDateGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullDateGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastDateGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastNotNullDateGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MaxDateGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinDateGroupByFunction;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class DateGroupByFunctionBatchTest {
    private static final int COLUMN_INDEX = 876;
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
    public void testFirstDateBatch() {
        GroupByFunction function = newFirstDateFunction();
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDates(epochDay(1), epochDay(2));
            function.computeBatch(value, ptr, 2);

            Assert.assertEquals(epochDay(1), function.getDate(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testFirstDateBatchAllNulls() {
        GroupByFunction function = newFirstDateFunction();
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDates(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(Numbers.LONG_NULL, function.getDate(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testFirstDateBatchEmpty() {
        GroupByFunction function = newFirstDateFunction();
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDates();
            function.computeBatch(value, ptr, 0);

            Assert.assertEquals(Numbers.LONG_NULL, function.getDate(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testFirstDateSetEmpty() {
        GroupByFunction function = newFirstDateFunction();
        try (SimpleMapValue value = prepare(function)) {
            function.setEmpty(value);

            Assert.assertEquals(Numbers.LONG_NULL, function.getDate(value));
        }
    }

    @Test
    public void testFirstNotNullDateBatch() {
        FirstNotNullDateGroupByFunction function = new FirstNotNullDateGroupByFunction(DateColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDates(Numbers.LONG_NULL, epochDay(10), epochDay(20));
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(epochDay(10), function.getDate(value));
        }
    }

    @Test
    public void testFirstNotNullDateBatchAllNulls() {
        FirstNotNullDateGroupByFunction function = new FirstNotNullDateGroupByFunction(DateColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDates(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(Numbers.LONG_NULL, function.getDate(value));
        }
    }

    @Test
    public void testFirstNotNullDateBatchEmpty() {
        FirstNotNullDateGroupByFunction function = new FirstNotNullDateGroupByFunction(DateColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateDates();
            function.computeBatch(value, ptr, 0);

            Assert.assertEquals(Numbers.LONG_NULL, function.getDate(value));
        }
    }

    @Test
    public void testLastDateBatch() {
        GroupByFunction function = newLastDateFunction();
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateDates(epochDay(5), epochDay(7), epochDay(9));
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(epochDay(9), function.getDate(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastDateBatchAllNulls() {
        GroupByFunction function = newLastDateFunction();
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateDates(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(Numbers.LONG_NULL, function.getDate(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastDateBatchEmpty() {
        GroupByFunction function = newLastDateFunction();
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateDates();
            function.computeBatch(value, ptr, 0);

            Assert.assertEquals(Numbers.LONG_NULL, function.getDate(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastNotNullDateBatch() {
        LastNotNullDateGroupByFunction function = new LastNotNullDateGroupByFunction(DateColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateDates(epochDay(1), Numbers.LONG_NULL, epochDay(3));
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(epochDay(3), function.getDate(value));
        }
    }

    @Test
    public void testLastNotNullDateBatchAllNulls() {
        LastNotNullDateGroupByFunction function = new LastNotNullDateGroupByFunction(DateColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateDates(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(Numbers.LONG_NULL, function.getDate(value));
        }
    }

    @Test
    public void testLastNotNullDateBatchEmpty() {
        LastNotNullDateGroupByFunction function = new LastNotNullDateGroupByFunction(DateColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateDates();
            function.computeBatch(value, ptr, 0);

            Assert.assertEquals(Numbers.LONG_NULL, function.getDate(value));
        }
    }

    @Test
    public void testLastNotNullDateBatchFirst() {
        LastNotNullDateGroupByFunction function = new LastNotNullDateGroupByFunction(DateColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateDates(epochDay(1), Numbers.LONG_NULL, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(epochDay(1), function.getDate(value));
        }
    }

    @Test
    public void testMaxDateBatch() {
        MaxDateGroupByFunction function = new MaxDateGroupByFunction(DateColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putDate(function.getValueIndex(), Numbers.LONG_NULL);

            long ptr = allocateDates(epochDay(10), epochDay(20), Numbers.LONG_NULL, epochDay(5));
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(epochDay(20), function.getDate(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMinDateBatch() {
        MinDateGroupByFunction function = new MinDateGroupByFunction(DateColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putDate(function.getValueIndex(), Numbers.LONG_NULL);

            long ptr = allocateDates(Numbers.LONG_NULL, epochDay(100), epochDay(50));
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(epochDay(50), function.getDate(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    private long allocateDates(long... values) {
        if (values.length == 0) {
            return 0;
        }
        if (lastAllocated != 0) {
            Unsafe.free(lastAllocated, lastSize, MemoryTag.NATIVE_DEFAULT);
        }
        lastSize = (long) values.length * Long.BYTES;
        lastAllocated = Unsafe.malloc(lastSize, MemoryTag.NATIVE_DEFAULT);
        long addr = lastAllocated;
        for (long value : values) {
            Unsafe.getUnsafe().putLong(addr, value);
            addr += Long.BYTES;
        }
        return lastAllocated;
    }

    private long epochDay(int day) {
        return day * 86_400_000L;
    }

    private GroupByFunction newFirstDateFunction() {
        ObjList<Function> args = new ObjList<>();
        args.add(DateColumn.newInstance(COLUMN_INDEX));
        IntList argPositions = new IntList();
        argPositions.add(0);
        return (GroupByFunction) new FirstDateGroupByFunctionFactory().newInstance(0, args, argPositions, null, null);
    }

    private GroupByFunction newLastDateFunction() {
        ObjList<Function> args = new ObjList<>();
        args.add(DateColumn.newInstance(COLUMN_INDEX));
        IntList argPositions = new IntList();
        argPositions.add(0);
        return (GroupByFunction) new LastDateGroupByFunctionFactory().newInstance(0, args, argPositions, null, null);
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
