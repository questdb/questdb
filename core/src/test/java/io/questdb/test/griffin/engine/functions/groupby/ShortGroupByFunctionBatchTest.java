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
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

public class ShortGroupByFunctionBatchTest {
    private static final int COLUMN_INDEX = 789;

    @Test
    public void testAvgShortBatchNotSupported() {
        AvgShortGroupByFunction function = new AvgShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertFalse(function.supportsBatchComputation());
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
    public void testFirstShortBatchNotSupported() {
        FirstShortGroupByFunction function = new FirstShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertFalse(function.supportsBatchComputation());
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
    public void testLastShortBatchNotSupported() {
        LastShortGroupByFunction function = new LastShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertFalse(function.supportsBatchComputation());
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
    public void testSumShortBatchNotSupported() {
        SumShortGroupByFunction function = new SumShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertFalse(function.supportsBatchComputation());
        }
    }

    @Test
    public void testSumShortSetEmpty() {
        SumShortGroupByFunction function = new SumShortGroupByFunction(ShortColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
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
