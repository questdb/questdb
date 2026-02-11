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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.GeoByteColumn;
import io.questdb.griffin.engine.functions.columns.GeoIntColumn;
import io.questdb.griffin.engine.functions.columns.GeoLongColumn;
import io.questdb.griffin.engine.functions.columns.GeoShortColumn;
import io.questdb.griffin.engine.functions.groupby.CountGeoHashGroupByFunctionByte;
import io.questdb.griffin.engine.functions.groupby.CountGeoHashGroupByFunctionInt;
import io.questdb.griffin.engine.functions.groupby.CountGeoHashGroupByFunctionLong;
import io.questdb.griffin.engine.functions.groupby.CountGeoHashGroupByFunctionShort;
import io.questdb.griffin.engine.functions.groupby.FirstGeoHashGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullGeoHashGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastGeoHashGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastNotNullGeoHashGroupByFunctionFactory;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class GeoHashGroupByFunctionBatchTest {
    private static final int COLUMN_INDEX = 543;
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
    public void testCountGeoHashByteBatch() {
        int type = ColumnType.getGeoHashTypeWithBits(ColumnType.GEOBYTE_MAX_BITS);
        CountGeoHashGroupByFunctionByte function = new CountGeoHashGroupByFunctionByte(
                GeoByteColumn.newInstance(COLUMN_INDEX, type)
        );
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateBytes((byte) 1, GeoHashes.BYTE_NULL, (byte) 2, (byte) 3);
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(3L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testCountGeoHashIntBatch() {
        int type = ColumnType.getGeoHashTypeWithBits(ColumnType.GEOINT_MAX_BITS);
        CountGeoHashGroupByFunctionInt function = new CountGeoHashGroupByFunctionInt(
                GeoIntColumn.newInstance(COLUMN_INDEX, type)
        );
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(100, GeoHashes.INT_NULL, 200);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(2L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testCountGeoHashLongBatch() {
        int type = ColumnType.getGeoHashTypeWithBits(ColumnType.GEOLONG_MAX_BITS);
        CountGeoHashGroupByFunctionLong function = new CountGeoHashGroupByFunctionLong(
                GeoLongColumn.newInstance(COLUMN_INDEX, type)
        );
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(1L, GeoHashes.NULL, 2L, 3L);
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(3L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testCountGeoHashShortBatch() {
        int type = ColumnType.getGeoHashTypeWithBits(ColumnType.GEOSHORT_MAX_BITS);
        CountGeoHashGroupByFunctionShort function = new CountGeoHashGroupByFunctionShort(
                GeoShortColumn.newInstance(COLUMN_INDEX, type)
        );
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateShorts((short) 10, GeoHashes.SHORT_NULL, (short) 20);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(2L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testFirstGeoHashBatchByte() {
        int type = ColumnType.getGeoHashTypeWithBits(ColumnType.GEOBYTE_MAX_BITS);
        GroupByFunction function = newFirstGeoHashFunction(GeoByteColumn.newInstance(COLUMN_INDEX, type));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateBytes((byte) 11, (byte) 22);
            function.computeBatch(value, ptr, 2);

            Assert.assertEquals(11, function.getGeoByte(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testFirstGeoHashSetEmpty() {
        int type = ColumnType.getGeoHashTypeWithBits(ColumnType.GEOBYTE_MAX_BITS);
        GroupByFunction function = newFirstGeoHashFunction(GeoByteColumn.newInstance(COLUMN_INDEX, type));
        try (SimpleMapValue value = prepare(function)) {
            function.setEmpty(value);
            Assert.assertEquals(GeoHashes.BYTE_NULL, function.getGeoByte(value));
        }
    }

    @Test
    public void testFirstNotNullGeoHashBatchByte() {
        int type = ColumnType.getGeoHashTypeWithBits(ColumnType.GEOBYTE_MAX_BITS);
        GroupByFunction function = newFirstNotNullGeoHashFunction(GeoByteColumn.newInstance(COLUMN_INDEX, type));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateBytes(GeoHashes.BYTE_NULL, (byte) 42);
            function.computeBatch(value, ptr, 2);

            Assert.assertEquals(42, function.getGeoByte(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testFirstNotNullGeoHashBatchLong() {
        int type = ColumnType.getGeoHashTypeWithBits(ColumnType.GEOLONG_MAX_BITS);
        GroupByFunction function = newFirstNotNullGeoHashFunction(GeoLongColumn.newInstance(COLUMN_INDEX, type));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(GeoHashes.NULL, 123456789L, GeoHashes.NULL);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(123456789L, function.getGeoLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastGeoHashBatchByte() {
        int type = ColumnType.getGeoHashTypeWithBits(ColumnType.GEOBYTE_MAX_BITS);
        GroupByFunction function = newLastGeoHashFunction(GeoByteColumn.newInstance(COLUMN_INDEX, type));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateBytes((byte) 7, (byte) 9, (byte) 11);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(11, function.getGeoByte(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastGeoHashBatchInt() {
        int type = ColumnType.getGeoHashTypeWithBits(ColumnType.GEOINT_MAX_BITS);
        GroupByFunction function = newLastGeoHashFunction(GeoIntColumn.newInstance(COLUMN_INDEX, type));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateInts(1, 2, 3);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(3, function.getGeoInt(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastGeoHashBatchLong() {
        int type = ColumnType.getGeoHashTypeWithBits(ColumnType.GEOLONG_MAX_BITS);
        GroupByFunction function = newLastGeoHashFunction(GeoLongColumn.newInstance(COLUMN_INDEX, type));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateLongs(10L, 20L, 30L);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(30L, function.getGeoLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastGeoHashBatchShort() {
        int type = ColumnType.getGeoHashTypeWithBits(ColumnType.GEOSHORT_MAX_BITS);
        GroupByFunction function = newLastGeoHashFunction(GeoShortColumn.newInstance(COLUMN_INDEX, type));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateShorts((short) 5, (short) 10, (short) 15);
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals(15, function.getGeoShort(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastNotNullGeoHashBatchInt() {
        int type = ColumnType.getGeoHashTypeWithBits(ColumnType.GEOINT_MAX_BITS);
        GroupByFunction function = newLastNotNullGeoHashFunction(GeoIntColumn.newInstance(COLUMN_INDEX, type));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateInts(GeoHashes.INT_NULL, 11, GeoHashes.INT_NULL, 22);
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(22, function.getGeoInt(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastNotNullGeoHashBatchLong() {
        int type = ColumnType.getGeoHashTypeWithBits(ColumnType.GEOLONG_MAX_BITS);
        GroupByFunction function = newLastNotNullGeoHashFunction(GeoLongColumn.newInstance(COLUMN_INDEX, type));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateLongs(GeoHashes.NULL, 1L, GeoHashes.NULL, 5L);
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(5L, function.getGeoLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    private long allocateBytes(byte... values) {
        if (values.length == 0) {
            return 0;
        }
        if (lastAllocated != 0) {
            Unsafe.free(lastAllocated, lastSize, MemoryTag.NATIVE_DEFAULT);
        }
        lastSize = values.length;
        lastAllocated = Unsafe.malloc(lastSize, MemoryTag.NATIVE_DEFAULT);
        long addr = lastAllocated;
        for (byte value : values) {
            Unsafe.getUnsafe().putByte(addr, value);
            addr += Byte.BYTES;
        }
        return lastAllocated;
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

    private long allocateLongs(long... values) {
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

    private long allocateShorts(short... values) {
        if (values.length == 0) {
            return 0;
        }
        if (lastAllocated != 0) {
            Unsafe.free(lastAllocated, lastSize, MemoryTag.NATIVE_DEFAULT);
        }
        lastSize = (long) values.length * Short.BYTES;
        lastAllocated = Unsafe.malloc(lastSize, MemoryTag.NATIVE_DEFAULT);
        long addr = lastAllocated;
        for (short value : values) {
            Unsafe.getUnsafe().putShort(addr, value);
            addr += Short.BYTES;
        }
        return lastAllocated;
    }

    private GroupByFunction newFirstGeoHashFunction(Function columnFunction) {
        ObjList<Function> args = new ObjList<>();
        args.add(columnFunction);
        IntList argPositions = new IntList();
        argPositions.add(0);
        return (GroupByFunction) new FirstGeoHashGroupByFunctionFactory()
                .newInstance(0, args, argPositions, null, null);
    }

    private GroupByFunction newFirstNotNullGeoHashFunction(Function columnFunction) {
        ObjList<Function> args = new ObjList<>();
        args.add(columnFunction);
        IntList argPositions = new IntList();
        argPositions.add(0);
        return (GroupByFunction) new FirstNotNullGeoHashGroupByFunctionFactory()
                .newInstance(0, args, argPositions, null, null);
    }

    private GroupByFunction newLastGeoHashFunction(Function columnFunction) {
        ObjList<Function> args = new ObjList<>();
        args.add(columnFunction);
        IntList argPositions = new IntList();
        argPositions.add(0);
        return (GroupByFunction) new LastGeoHashGroupByFunctionFactory()
                .newInstance(0, args, argPositions, null, null);
    }

    private GroupByFunction newLastNotNullGeoHashFunction(Function columnFunction) {
        ObjList<Function> args = new ObjList<>();
        args.add(columnFunction);
        IntList argPositions = new IntList();
        argPositions.add(0);
        return (GroupByFunction) new LastNotNullGeoHashGroupByFunctionFactory()
                .newInstance(0, args, argPositions, null, null);
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
