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
import io.questdb.griffin.engine.functions.columns.CharColumn;
import io.questdb.griffin.engine.functions.constants.CharConstant;
import io.questdb.griffin.engine.functions.groupby.FirstCharGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullCharGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastCharGroupByFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.LastNotNullCharGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MaxCharGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinCharGroupByFunction;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class CharGroupByFunctionBatchTest {
    private static final int COLUMN_INDEX = 765;
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
    public void testFirstCharBatch() {
        GroupByFunction function = newFirstCharFunction();
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateChars('b', 'c');
            function.computeBatch(value, ptr, 2);

            Assert.assertEquals('b', function.getChar(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testFirstCharSetEmpty() {
        GroupByFunction function = newFirstCharFunction();
        try (SimpleMapValue value = prepare(function)) {
            function.setEmpty(value);

            Assert.assertEquals(CharConstant.ZERO.getChar(null), function.getChar(value));
        }
    }

    @Test
    public void testFirstNotNullCharBatch() {
        FirstNotNullCharGroupByFunction function = new FirstNotNullCharGroupByFunction(new CharColumn(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateChars(CharConstant.ZERO.getChar(null), 'x', 'y');
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals('x', function.getChar(value));
        }
    }

    @Test
    public void testLastCharBatch() {
        GroupByFunction function = newLastCharFunction();
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateChars('a', 'z', 'm');
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals('m', function.getChar(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastNotNullCharBatch() {
        LastNotNullCharGroupByFunction function = new LastNotNullCharGroupByFunction(new CharColumn(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateChars('a', CharConstant.ZERO.getChar(null), 'd');
            function.computeBatch(value, ptr, 3);

            Assert.assertEquals('d', function.getChar(value));
        }
    }

    @Test
    public void testMaxCharBatch() {
        MaxCharGroupByFunction function = new MaxCharGroupByFunction(new CharColumn(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putChar(function.getValueIndex(), CharConstant.ZERO.getChar(null));

            long ptr = allocateChars('a', 'y', CharConstant.ZERO.getChar(null), 'z');
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals('z', function.getChar(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMinCharBatch() {
        MinCharGroupByFunction function = new MinCharGroupByFunction(new CharColumn(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putChar(function.getValueIndex(), CharConstant.ZERO.getChar(null));

            long ptr = allocateChars(CharConstant.ZERO.getChar(null), 'd', 'b', 'c');
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals(0, function.getChar(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMinCharBatchSimple() {
        MinCharGroupByFunction function = new MinCharGroupByFunction(new CharColumn(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putChar(function.getValueIndex(), CharConstant.ZERO.getChar(null));

            long ptr = allocateChars('d', 'b', 'c');
            function.computeBatch(value, ptr, 4);

            Assert.assertEquals('b', function.getChar(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    private long allocateChars(char... values) {
        if (values.length == 0) {
            return 0;
        }
        if (lastAllocated != 0) {
            Unsafe.free(lastAllocated, lastSize, MemoryTag.NATIVE_DEFAULT);
        }
        lastSize = (long) values.length * Character.BYTES;
        lastAllocated = Unsafe.malloc(lastSize, MemoryTag.NATIVE_DEFAULT);
        long addr = lastAllocated;
        for (char value : values) {
            Unsafe.getUnsafe().putChar(addr, value);
            addr += Character.BYTES;
        }
        return lastAllocated;
    }

    private GroupByFunction newFirstCharFunction() {
        ObjList<Function> args = new ObjList<>();
        args.add(new CharColumn(COLUMN_INDEX));
        IntList argPositions = new IntList();
        argPositions.add(0);
        return (GroupByFunction) new FirstCharGroupByFunctionFactory().newInstance(0, args, argPositions, null, null);
    }

    private GroupByFunction newLastCharFunction() {
        ObjList<Function> args = new ObjList<>();
        args.add(new CharColumn(COLUMN_INDEX));
        IntList argPositions = new IntList();
        argPositions.add(0);
        return (GroupByFunction) new LastCharGroupByFunctionFactory().newInstance(0, args, argPositions, null, null);
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
