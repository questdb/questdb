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
import io.questdb.griffin.engine.functions.columns.IPv4Column;
import io.questdb.griffin.engine.functions.groupby.CountIPv4GroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstIPv4GroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullIPv4GroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastIPv4GroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastNotNullIPv4GroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.MaxIPv4GroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinIPv4GroupByFunction;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class IPv4GroupByFunctionBatchTest {
    private static final int COLUMN_INDEX = 654;
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
    public void testCountIPv4Batch() {
        CountIPv4GroupByFunction function = new CountIPv4GroupByFunction(IPv4Column.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {

            long ptr = allocateInts(ipv4("1.1.1.1"), Numbers.IPv4_NULL, ipv4("2.2.2.2"), ipv4("3.3.3.3"));
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(3L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testCountIPv4BatchAllNull() {
        CountIPv4GroupByFunction function = new CountIPv4GroupByFunction(IPv4Column.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(Numbers.IPv4_NULL, Numbers.IPv4_NULL);
            function.computeBatch(value, ptr, 2);

            Assert.assertEquals(0L, function.getLong(value));
        }
    }

    @Test
    public void testFirstIPv4Batch() {
        GroupByFunction function = newFirstIPv4Function(IPv4Column.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(ipv4("10.0.0.1"), ipv4("10.0.0.2"));
            function.computeBatch(value, ptr, 2);

            Assert.assertEquals(ipv4("10.0.0.1"), function.getIPv4(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testFirstIPv4SetEmpty() {
        GroupByFunction function = newFirstIPv4Function(IPv4Column.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(Numbers.IPv4_NULL, function.getIPv4(value));
        }
    }

    @Test
    public void testFirstNotNullIPv4Batch() {
        GroupByFunction function = newFirstNotNullIPv4Function(IPv4Column.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(Numbers.IPv4_NULL, ipv4("10.0.0.7"), Numbers.IPv4_NULL);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(ipv4("10.0.0.7"), function.getIPv4(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastIPv4Batch() {
        GroupByFunction function = newLastIPv4Function(IPv4Column.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateInts(ipv4("192.168.1.1"), ipv4("192.168.1.2"), Numbers.IPv4_NULL, ipv4("192.168.1.3"));
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(ipv4("192.168.1.3"), function.getIPv4(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastNotNullIPv4Batch() {
        GroupByFunction function = newLastNotNullIPv4Function(IPv4Column.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateInts(Numbers.IPv4_NULL, ipv4("1.2.3.4"), Numbers.IPv4_NULL, ipv4("5.6.7.8"));
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(ipv4("5.6.7.8"), function.getIPv4(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMaxIPv4Batch() {
        MaxIPv4GroupByFunction function = new MaxIPv4GroupByFunction(IPv4Column.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putInt(function.getValueIndex(), Numbers.IPv4_NULL);

            long ptr = allocateInts(ipv4("8.8.8.8"), ipv4("1.1.1.1"), Numbers.IPv4_NULL, ipv4("9.9.9.9"));
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(ipv4("9.9.9.9"), function.getIPv4(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMinIPv4Batch() {
        MinIPv4GroupByFunction function = new MinIPv4GroupByFunction(IPv4Column.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putInt(function.getValueIndex(), Numbers.IPv4_NULL);

            long ptr = allocateInts(Numbers.IPv4_NULL, ipv4("8.8.4.4"), ipv4("1.1.1.1"));
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(Numbers.IPv4_NULL, function.getIPv4(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMinIPv4BatchSimple() {
        MinIPv4GroupByFunction function = new MinIPv4GroupByFunction(IPv4Column.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putInt(function.getValueIndex(), Numbers.IPv4_NULL);

            long ptr = allocateInts(ipv4("8.8.4.4"), ipv4("1.1.1.1"));
            function.computeBatch(value, ptr, 2);

            Assert.assertEquals(ipv4("1.1.1.1"), function.getIPv4(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    private long allocateInts(int... values) {
        if (values.length == 0) {
            return 0;
        }
        if (lastAllocated != 0) {
            Unsafe.free(lastAllocated, lastSize, MemoryTag.NATIVE_DEFAULT);
        }
        lastSize = (long) values.length * Integer.BYTES;
        lastAllocated = Unsafe.malloc(lastSize, MemoryTag.NATIVE_DEFAULT);
        long addr = lastAllocated;
        for (int value : values) {
            Unsafe.getUnsafe().putInt(addr, value);
            addr += Integer.BYTES;
        }
        return lastAllocated;
    }

    private int ipv4(String value) {
        return Numbers.parseIPv4Quiet(value);
    }

    private GroupByFunction newFirstIPv4Function(Function columnFunction) {
        ObjList<Function> args = new ObjList<>();
        args.add(columnFunction);
        IntList argPositions = new IntList();
        argPositions.add(0);
        return (GroupByFunction) new FirstIPv4GroupByFunctionFactory().newInstance(0, args, argPositions, null, null);
    }

    private GroupByFunction newFirstNotNullIPv4Function(Function columnFunction) {
        ObjList<Function> args = new ObjList<>();
        args.add(columnFunction);
        IntList argPositions = new IntList();
        argPositions.add(0);
        return (GroupByFunction) new FirstNotNullIPv4GroupByFunctionFactory().newInstance(0, args, argPositions, null, null);
    }

    private GroupByFunction newLastIPv4Function(Function columnFunction) {
        ObjList<Function> args = new ObjList<>();
        args.add(columnFunction);
        IntList argPositions = new IntList();
        argPositions.add(0);
        return (GroupByFunction) new LastIPv4GroupByFunctionFactory().newInstance(0, args, argPositions, null, null);
    }

    private GroupByFunction newLastNotNullIPv4Function(Function columnFunction) {
        ObjList<Function> args = new ObjList<>();
        args.add(columnFunction);
        IntList argPositions = new IntList();
        argPositions.add(0);
        return (GroupByFunction) new LastNotNullIPv4GroupByFunctionFactory().newInstance(0, args, argPositions, null, null);
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
