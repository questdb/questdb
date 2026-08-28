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

import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.columns.IntColumn;
import io.questdb.griffin.engine.functions.groupby.AvgIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastNotNullIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MaxIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.SumIntGroupByFunction;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

public class IntGroupByFunctionBatchTest extends AbstractGroupByFunctionBatchTest {
    // Stands in for a row whose column is NULL, as a column-top row reads.
    private static final Record NULL_RECORD = new Record() {
        @Override
        public int getInt(int col) {
            return Numbers.INT_NULL;
        }
    };

    @Test
    public void testAvgIntBatch() {
        AvgIntGroupByFunction function = new AvgIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(1, Numbers.INT_NULL, 2, Numbers.INT_NULL, 3);
            function.computeBatch(value, ptr, 5, 0);

            Assert.assertEquals(2.0, function.getDouble(value), 0.0);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testAvgIntBatchAccumulates() {
        AvgIntGroupByFunction function = new AvgIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(1, 2, 3);
            function.computeBatch(value, ptr, 3, 0);

            ptr = allocateInts(4, 5, Numbers.INT_NULL);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals((1 + 2 + 3 + 4 + 5) / 5.0, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testAvgIntBatchAllNull() {
        AvgIntGroupByFunction function = new AvgIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(Numbers.INT_NULL, Numbers.INT_NULL, Numbers.INT_NULL);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    // After a finite batch followed by an all-null batch, the previous count must be
    // preserved (sumIntAcc overwrites the count pointer; restore on the empty path).
    @Test
    public void testAvgIntBatchAllNullPreservesPrevCount() {
        AvgIntGroupByFunction function = new AvgIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(10, 20);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateInts(Numbers.INT_NULL, Numbers.INT_NULL);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(15.0, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testAvgIntBatchMixedNull() {
        AvgIntGroupByFunction function = new AvgIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(10, Numbers.INT_NULL, 20);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(15.0, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testAvgIntBatchZeroCountKeepsNaN() {
        AvgIntGroupByFunction function = new AvgIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.computeBatch(value, 0, 0, 0);

            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testAvgIntSetEmpty() {
        AvgIntGroupByFunction function = new AvgIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testCountIntBatch() {
        CountIntGroupByFunction function = new CountIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(1, Numbers.INT_NULL, 2, Numbers.INT_NULL, 3);
            function.computeBatch(value, ptr, 5, 0);

            Assert.assertEquals(3L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testCountIntBatchAccumulates() {
        CountIntGroupByFunction function = new CountIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(1, Numbers.INT_NULL);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateInts(2, 3);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(3L, function.getLong(value));
        }
    }

    @Test
    public void testCountIntBatchAllNull() {
        CountIntGroupByFunction function = new CountIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(Numbers.INT_NULL, Numbers.INT_NULL, Numbers.INT_NULL);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(0L, function.getLong(value));
        }
    }

    @Test
    public void testCountIntBatchZeroCountKeepsZero() {
        CountIntGroupByFunction function = new CountIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.computeBatch(value, 0, 0, 0);

            Assert.assertEquals(0L, function.getLong(value));
        }
    }

    @Test
    public void testCountIntSetEmpty() {
        CountIntGroupByFunction function = new CountIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(0L, function.getLong(value));
        }
    }

    @Test
    public void testFirstIntBatch() {
        FirstIntGroupByFunction function = new FirstIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(5, 6, 7);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(5, function.getInt(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testFirstIntBatchAccumulates() {
        FirstIntGroupByFunction function = new FirstIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(5, 6);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateInts(7, 8);
            function.computeBatch(value, ptr, 2, 2);

            Assert.assertEquals(5, function.getInt(value));
        }
    }

    @Test
    public void testFirstIntBatchAllNull() {
        FirstIntGroupByFunction function = new FirstIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(Numbers.INT_NULL, 1);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(Numbers.INT_NULL, function.getInt(value));
        }
    }

    @Test
    public void testFirstIntBatchEmpty() {
        FirstIntGroupByFunction function = new FirstIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            function.computeBatch(value, 0, 0, 0);

            Assert.assertEquals(Numbers.INT_NULL, function.getInt(value));
        }
    }

    @Test
    public void testFirstIntBatchNotCalled() {
        FirstIntGroupByFunction function = new FirstIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            Assert.assertEquals(Numbers.INT_NULL, function.getInt(value));
        }
    }

    @Test
    public void testFirstIntSetEmpty() {
        FirstIntGroupByFunction function = new FirstIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(Numbers.INT_NULL, function.getInt(value));
        }
    }

    @Test
    public void testFirstNotNullIntBatch() {
        FirstNotNullIntGroupByFunction function = new FirstNotNullIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(Numbers.INT_NULL, 42, Numbers.INT_NULL);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(42, function.getInt(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testFirstNotNullIntBatchAccumulates() {
        FirstNotNullIntGroupByFunction function = new FirstNotNullIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(Numbers.INT_NULL, 42);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateInts(99, Numbers.INT_NULL);
            function.computeBatch(value, ptr, 2, 2);

            Assert.assertEquals(42, function.getInt(value));
        }
    }

    @Test
    public void testLastIntBatch() {
        LastIntGroupByFunction function = new LastIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateInts(11, 22, 33);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(2, value.getLong(0));
            Assert.assertEquals(33, function.getInt(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastIntBatchAccumulates() {
        LastIntGroupByFunction function = new LastIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateInts(11, 22);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateInts(33, 44);
            function.computeBatch(value, ptr, 2, 2);

            Assert.assertEquals(44, function.getInt(value));
        }
    }

    @Test
    public void testLastIntBatchAllNull() {
        LastIntGroupByFunction function = new LastIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateInts(11, Numbers.INT_NULL);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(1, value.getLong(0));
            Assert.assertEquals(Numbers.INT_NULL, function.getInt(value));
        }
    }

    @Test
    public void testLastIntSetEmpty() {
        LastIntGroupByFunction function = new LastIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(Numbers.INT_NULL, function.getInt(value));
        }
    }

    @Test
    public void testLastNotNullIntBatch() {
        LastNotNullIntGroupByFunction function = new LastNotNullIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateInts(Numbers.INT_NULL, 10, Numbers.INT_NULL, 20);
            function.computeBatch(value, ptr, 4, 0);

            Assert.assertEquals(20, function.getInt(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastNotNullIntBatchAccumulates() {
        LastNotNullIntGroupByFunction function = new LastNotNullIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateInts(10, Numbers.INT_NULL);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateInts(Numbers.INT_NULL, 20);
            function.computeBatch(value, ptr, 2, 2);

            Assert.assertEquals(20, function.getInt(value));
        }
    }

    @Test
    public void testLastNotNullIntBatchKeepsHigherRowIdNonNull() {
        LastNotNullIntGroupByFunction function = new LastNotNullIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            // A stored non-null must survive a batch that arrives at a lower rowId. See the class javadoc.
            long ptr = allocateInts(99);
            function.computeBatch(value, ptr, 1, 100);
            Assert.assertEquals(99, function.getInt(value));

            ptr = allocateInts(42);
            function.computeBatch(value, ptr, 1, 10);

            Assert.assertEquals(99, function.getInt(value));
        }
    }

    @Test
    public void testLastNotNullIntBatchReplacesStoredNull() {
        LastNotNullIntGroupByFunction function = new LastNotNullIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            // computeFirst writes NULL through with a real rowId; a non-null at a lower rowId must still
            // replace it. See the class javadoc.
            function.computeFirst(value, NULL_RECORD, 100);

            long ptr = allocateInts(42);
            function.computeBatch(value, ptr, 1, 10);

            Assert.assertEquals(42, function.getInt(value));
        }
    }

    @Test
    public void testMaxIntBatch() {
        MaxIntGroupByFunction function = new MaxIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putInt(0, -999);

            long ptr = allocateInts(-10, Numbers.INT_NULL, 15, 7);
            function.computeBatch(value, ptr, 4, 0);

            Assert.assertEquals(15, function.getInt(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMaxIntBatchAccumulates() {
        MaxIntGroupByFunction function = new MaxIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(1, 5);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateInts(3, 2);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(5, function.getInt(value));
        }
    }

    @Test
    public void testMaxIntBatchAllNull() {
        MaxIntGroupByFunction function = new MaxIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(Numbers.INT_NULL, Numbers.INT_NULL, Numbers.INT_NULL);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(Numbers.INT_NULL, function.getInt(value));
        }
    }

    @Test
    public void testMaxIntSetEmpty() {
        MaxIntGroupByFunction function = new MaxIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(Numbers.INT_NULL, function.getInt(value));
        }
    }

    @Test
    public void testMinIntBatch() {
        MinIntGroupByFunction function = new MinIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putInt(0, 999);

            long ptr = allocateInts(Numbers.INT_NULL, 4, 2, 3);
            function.computeBatch(value, ptr, 4, 0);

            Assert.assertEquals(2, function.getInt(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMinIntBatchAccumulates() {
        MinIntGroupByFunction function = new MinIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(5, 3);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateInts(4, 1);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(1, function.getInt(value));
        }
    }

    @Test
    public void testMinIntBatchAllNull() {
        MinIntGroupByFunction function = new MinIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(Numbers.INT_NULL, Numbers.INT_NULL);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(Numbers.INT_NULL, function.getInt(value));
        }
    }

    @Test
    public void testMinIntSetEmpty() {
        MinIntGroupByFunction function = new MinIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(Numbers.INT_NULL, function.getInt(value));
        }
    }

    @Test
    public void testSumIntBatch() {
        SumIntGroupByFunction function = new SumIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(1, 2, 3, 4);
            function.computeBatch(value, ptr, 4, 0);

            Assert.assertEquals(10L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testSumIntBatchAccumulates() {
        SumIntGroupByFunction function = new SumIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(1, 2);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateInts(3, 4);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(10L, function.getLong(value));
        }
    }

    @Test
    public void testSumIntBatchAllNull() {
        SumIntGroupByFunction function = new SumIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateInts(Numbers.INT_NULL, Numbers.INT_NULL);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    @Test
    public void testSumIntBatchZeroCountKeepsExistingValue() {
        SumIntGroupByFunction function = new SumIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putLong(0, 55);

            function.computeBatch(value, 0, 0, 0);

            Assert.assertEquals(55L, function.getLong(value));
        }
    }

    @Test
    public void testSumIntSetEmpty() {
        SumIntGroupByFunction function = new SumIntGroupByFunction(IntColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }
}
