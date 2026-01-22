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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdaterFactory;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.ObjList;
import io.questdb.test.cairo.TestRecord;
import org.junit.Assert;
import org.junit.Test;

public class GroupByFunctionsUpdaterFactoryTest {

    @Test
    public void testFallbackUpdaterForManyFunctions() {
        // When there are many group by functions, the factory should return
        // SimpleGroupByFunctionUpdater to avoid "Bytecode is too long" error.
        // See issue #3326.
        final int functionCount = 6000;
        ObjList<GroupByFunction> functions = new ObjList<>();
        for (int i = 0; i < functionCount; i++) {
            functions.add(new TestGroupByFunction());
        }

        // Verify that getInstanceClass returns the fallback class
        Class<? extends GroupByFunctionsUpdater> clazz = GroupByFunctionsUpdaterFactory.getInstanceClass(
                new BytecodeAssembler(),
                functionCount
        );
        Assert.assertEquals("SimpleGroupByFunctionUpdater", clazz.getSimpleName());

        // Verify the updater works correctly
        GroupByFunctionsUpdater updater = GroupByFunctionsUpdaterFactory.getInstance(clazz, functions);

        try (
                SimpleMapValue value = new SimpleMapValue(2);
                SimpleMapValue destValue = new SimpleMapValue(2);
                SimpleMapValue srcValue = new SimpleMapValue(2)
        ) {
            Record record = new TestRecord();

            updater.updateNew(value, record, 42);
            Assert.assertEquals(1, value.getLong(0));
            Assert.assertEquals(42, value.getLong(1));

            updater.updateExisting(value, record, 43);
            Assert.assertEquals(1 + functionCount, value.getLong(0));
            Assert.assertEquals(43, value.getLong(1));

            updater.updateEmpty(value);
            Assert.assertEquals(-1, value.getLong(0));
            Assert.assertEquals(-1, value.getLong(1));

            destValue.putLong(0, 0);
            destValue.putLong(1, 0);

            srcValue.putLong(0, 42);
            srcValue.putLong(1, 1);
            updater.merge(destValue, srcValue);
            Assert.assertEquals(42, destValue.getLong(0));
            Assert.assertEquals(0, destValue.getLong(1));
        }
    }

    @Test
    public void testSmoke() {
        ObjList<GroupByFunction> functions = new ObjList<>();
        functions.add(new TestGroupByFunction());
        functions.add(new TestGroupByFunction());
        functions.add(new TestGroupByFunction());
        GroupByFunctionsUpdater updater = GroupByFunctionsUpdaterFactory.getInstance(new BytecodeAssembler(), functions);

        try (
                SimpleMapValue value = new SimpleMapValue(2);
                SimpleMapValue destValue = new SimpleMapValue(2);
                SimpleMapValue srcValue = new SimpleMapValue(2)
        ) {
            Record record = new TestRecord();

            updater.updateNew(value, record, 42);
            Assert.assertEquals(1, value.getLong(0));
            Assert.assertEquals(42, value.getLong(1));

            updater.updateExisting(value, record, 43);
            Assert.assertEquals(1 + functions.size(), value.getLong(0));
            Assert.assertEquals(43, value.getLong(1));

            updater.updateEmpty(value);
            Assert.assertEquals(-1, value.getLong(0));
            Assert.assertEquals(-1, value.getLong(1));

            destValue.putLong(0, 0);
            destValue.putLong(1, 0);

            srcValue.putLong(0, 42);
            srcValue.putLong(1, 1);
            updater.merge(destValue, srcValue);
            Assert.assertEquals(42, destValue.getLong(0));
            Assert.assertEquals(0, destValue.getLong(1));
        }
    }

    private static class TestGroupByFunction extends LongFunction implements GroupByFunction, UnaryFunction {

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
            mapValue.putLong(0, 1);
            mapValue.putLong(1, rowId);
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
            long value = mapValue.getLong(0);
            mapValue.putLong(0, value + 1);
            mapValue.putLong(1, rowId);
        }

        @Override
        public Function getArg() {
            return null;
        }

        @Override
        public long getLong(Record rec) {
            return 0;
        }

        @Override
        public int getValueIndex() {
            return 0;
        }

        @Override
        public void initValueIndex(int valueIndex) {
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
        }

        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            long value = srcValue.getLong(0);
            destValue.putLong(0, value);
            destValue.putLong(1, Math.min(srcValue.getLong(1), destValue.getLong(1)));
        }

        @Override
        public void setNull(MapValue mapValue) {
            mapValue.putLong(0, -1);
            mapValue.putLong(1, -1);
        }

        @Override
        public boolean supportsParallelism() {
            return false;
        }
    }
}