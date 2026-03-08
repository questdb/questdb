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
import io.questdb.griffin.engine.functions.columns.BooleanColumn;
import io.questdb.griffin.engine.functions.groupby.FirstBooleanGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastBooleanGroupByFunction;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import org.junit.Assert;
import org.junit.Test;

public class BooleanGroupByFunctionBatchTest {
    private static final int COLUMN_INDEX = 222;

    @Test
    public void testFirstBooleanBatchNotSupported() {
        FirstBooleanGroupByFunction function = new FirstBooleanGroupByFunction(BooleanColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertFalse(function.supportsBatchComputation());
        }
    }

    @Test
    public void testFirstBooleanSetEmpty() {
        FirstBooleanGroupByFunction function = new FirstBooleanGroupByFunction(BooleanColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertFalse(function.getBool(value));
        }
    }

    @Test
    public void testLastBooleanBatchNotSupported() {
        LastBooleanGroupByFunction function = new LastBooleanGroupByFunction(BooleanColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertFalse(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastBooleanSetEmpty() {
        LastBooleanGroupByFunction function = new LastBooleanGroupByFunction(BooleanColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertFalse(function.getBool(value));
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
