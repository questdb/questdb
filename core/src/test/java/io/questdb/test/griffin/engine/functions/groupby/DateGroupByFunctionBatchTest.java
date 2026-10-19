/*+*****************************************************************************
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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
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
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

public class DateGroupByFunctionBatchTest extends AbstractGroupByFunctionBatchTest {
    // Stands in for a row whose column is NULL, as a column-top row reads.
    private static final Record NULL_RECORD = new Record() {
        @Override
        public long getDate(int col) {
            return Numbers.LONG_NULL;
        }
    };

    @Test
    public void testFirstDateBatch() {
        GroupByFunction function = newFirstDateFunction();
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(epochDay(1), epochDay(2));
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(epochDay(1), function.getDate(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testFirstDateBatchAccumulates() {
        GroupByFunction function = newFirstDateFunction();
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(epochDay(1), epochDay(2));
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateLongs(epochDay(3), epochDay(4));
            function.computeBatch(value, ptr, 2, 2);

            Assert.assertEquals(epochDay(1), function.getDate(value));
        }
    }

    @Test
    public void testFirstDateBatchAllNulls() {
        GroupByFunction function = newFirstDateFunction();
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(Numbers.LONG_NULL, function.getDate(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testFirstDateBatchEmpty() {
        GroupByFunction function = newFirstDateFunction();
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs();
            function.computeBatch(value, ptr, 0, 0);

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
            long ptr = allocateLongs(Numbers.LONG_NULL, epochDay(10), epochDay(20));
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(epochDay(10), function.getDate(value));
        }
    }

    @Test
    public void testFirstNotNullDateBatchAccumulates() {
        FirstNotNullDateGroupByFunction function = new FirstNotNullDateGroupByFunction(DateColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(Numbers.LONG_NULL, epochDay(10));
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateLongs(epochDay(20), Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 2, 2);

            Assert.assertEquals(epochDay(10), function.getDate(value));
        }
    }

    @Test
    public void testFirstNotNullDateBatchAllNulls() {
        FirstNotNullDateGroupByFunction function = new FirstNotNullDateGroupByFunction(DateColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(Numbers.LONG_NULL, function.getDate(value));
        }
    }

    @Test
    public void testFirstNotNullDateBatchEmpty() {
        FirstNotNullDateGroupByFunction function = new FirstNotNullDateGroupByFunction(DateColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs();
            function.computeBatch(value, ptr, 0, 0);

            Assert.assertEquals(Numbers.LONG_NULL, function.getDate(value));
        }
    }

    @Test
    public void testLastDateBatch() {
        GroupByFunction function = newLastDateFunction();
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateLongs(epochDay(5), epochDay(7), epochDay(9));
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(epochDay(9), function.getDate(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastDateBatchAccumulates() {
        GroupByFunction function = newLastDateFunction();
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateLongs(epochDay(5), epochDay(7));
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateLongs(epochDay(9), epochDay(11));
            function.computeBatch(value, ptr, 2, 2);

            Assert.assertEquals(epochDay(11), function.getDate(value));
        }
    }

    @Test
    public void testLastDateBatchAllNulls() {
        GroupByFunction function = newLastDateFunction();
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateLongs(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(Numbers.LONG_NULL, function.getDate(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastDateBatchEmpty() {
        GroupByFunction function = newLastDateFunction();
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateLongs();
            function.computeBatch(value, ptr, 0, 0);

            Assert.assertEquals(Numbers.LONG_NULL, function.getDate(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastNotNullDateBatch() {
        LastNotNullDateGroupByFunction function = new LastNotNullDateGroupByFunction(DateColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateLongs(epochDay(1), Numbers.LONG_NULL, epochDay(3));
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(epochDay(3), function.getDate(value));
        }
    }

    @Test
    public void testLastNotNullDateBatchAccumulates() {
        LastNotNullDateGroupByFunction function = new LastNotNullDateGroupByFunction(DateColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateLongs(epochDay(1), Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateLongs(Numbers.LONG_NULL, epochDay(3));
            function.computeBatch(value, ptr, 2, 2);

            Assert.assertEquals(epochDay(3), function.getDate(value));
        }
    }

    @Test
    public void testLastNotNullDateBatchAllNulls() {
        LastNotNullDateGroupByFunction function = new LastNotNullDateGroupByFunction(DateColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateLongs(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(Numbers.LONG_NULL, function.getDate(value));
        }
    }

    @Test
    public void testLastNotNullDateBatchEmpty() {
        LastNotNullDateGroupByFunction function = new LastNotNullDateGroupByFunction(DateColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateLongs();
            function.computeBatch(value, ptr, 0, 0);

            Assert.assertEquals(Numbers.LONG_NULL, function.getDate(value));
        }
    }

    @Test
    public void testLastNotNullDateBatchFirst() {
        LastNotNullDateGroupByFunction function = new LastNotNullDateGroupByFunction(DateColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateLongs(epochDay(1), Numbers.LONG_NULL, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(epochDay(1), function.getDate(value));
        }
    }

    @Test
    public void testLastNotNullDateBatchKeepsHigherRowIdNonNull() {
        LastNotNullDateGroupByFunction function = new LastNotNullDateGroupByFunction(DateColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            // A stored non-null must survive a batch that arrives at a lower rowId. See the class javadoc.
            long ptr = allocateLongs(epochDay(9));
            function.computeBatch(value, ptr, 1, 100);
            Assert.assertEquals(epochDay(9), function.getDate(value));

            ptr = allocateLongs(epochDay(2));
            function.computeBatch(value, ptr, 1, 10);

            Assert.assertEquals(epochDay(9), function.getDate(value));
        }
    }

    @Test
    public void testLastNotNullDateBatchReplacesStoredNull() {
        LastNotNullDateGroupByFunction function = new LastNotNullDateGroupByFunction(DateColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            // computeFirst writes NULL through with a real rowId; a non-null at a lower rowId must still
            // replace it. See the class javadoc.
            function.computeFirst(value, NULL_RECORD, 100);

            long ptr = allocateLongs(epochDay(2));
            function.computeBatch(value, ptr, 1, 10);

            Assert.assertEquals(epochDay(2), function.getDate(value));
        }
    }

    @Test
    public void testMaxDateBatch() {
        MaxDateGroupByFunction function = new MaxDateGroupByFunction(DateColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putDate(function.getValueIndex(), Numbers.LONG_NULL);

            long ptr = allocateLongs(epochDay(10), epochDay(20), Numbers.LONG_NULL, epochDay(5));
            function.computeBatch(value, ptr, 4, 0);

            Assert.assertEquals(epochDay(20), function.getDate(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMinDateBatch() {
        MinDateGroupByFunction function = new MinDateGroupByFunction(DateColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putDate(function.getValueIndex(), Numbers.LONG_NULL);

            long ptr = allocateLongs(Numbers.LONG_NULL, epochDay(100), epochDay(50));
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(epochDay(50), function.getDate(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMaxDateBatchAccumulates() {
        MaxDateGroupByFunction function = new MaxDateGroupByFunction(DateColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(epochDay(10), epochDay(20));
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateLongs(epochDay(5), epochDay(15));
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(epochDay(20), function.getDate(value));
        }
    }

    @Test
    public void testMinDateBatchAccumulates() {
        MinDateGroupByFunction function = new MinDateGroupByFunction(DateColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(epochDay(50), epochDay(100));
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateLongs(epochDay(30), epochDay(60));
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(epochDay(30), function.getDate(value));
        }
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
}
