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
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.groupby.AvgLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.FirstNotNullLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastNotNullLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MaxLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.MinLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.SumLongGroupByFunction;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

public class LongGroupByFunctionBatchTest extends AbstractGroupByFunctionBatchTest {
    // Stands in for a row whose column is NULL, as a column-top row reads.
    private static final Record NULL_RECORD = new Record() {
        @Override
        public long getLong(int col) {
            return Numbers.LONG_NULL;
        }
    };

    @Test
    public void testAvgLongBatch() {
        AvgLongGroupByFunction function = new AvgLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(1, Numbers.LONG_NULL, 2, Numbers.LONG_NULL, 3);
            function.computeBatch(value, ptr, 5, 0);

            Assert.assertEquals(2.0, function.getDouble(value), 0.0);
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testAvgLongBatchAccumulates() {
        AvgLongGroupByFunction function = new AvgLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(1, 2, 3);
            function.computeBatch(value, ptr, 3, 0);

            ptr = allocateLongs(4, 5, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals((1 + 2 + 3 + 4 + 5) / 5.0, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testAvgLongBatchAllNull() {
        AvgLongGroupByFunction function = new AvgLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    // After a finite batch followed by an all-null batch, the previous count must be
    // preserved (sumLongAcc overwrites the count pointer; restore on the empty path).
    @Test
    public void testAvgLongBatchAllNullPreservesPrevCount() {
        AvgLongGroupByFunction function = new AvgLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(10, 20);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateLongs(Numbers.LONG_NULL, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(15.0, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testAvgLongBatchMixedNull() {
        AvgLongGroupByFunction function = new AvgLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(10, Numbers.LONG_NULL, 20);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(15.0, function.getDouble(value), 0.0);
        }
    }

    @Test
    public void testAvgLongBatchZeroCountKeepsNaN() {
        AvgLongGroupByFunction function = new AvgLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.computeBatch(value, 0, 0, 0);

            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testAvgLongSetEmpty() {
        AvgLongGroupByFunction function = new AvgLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertTrue(Double.isNaN(function.getDouble(value)));
        }
    }

    @Test
    public void testCountLongBatch() {
        CountLongGroupByFunction function = new CountLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(1, Numbers.LONG_NULL, 2, Numbers.LONG_NULL, 3);
            function.computeBatch(value, ptr, 5, 0);

            Assert.assertEquals(3L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testCountLongBatchAccumulates() {
        CountLongGroupByFunction function = new CountLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(1, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateLongs(2, 3);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(3L, function.getLong(value));
        }
    }

    @Test
    public void testCountLongBatchAllNull() {
        CountLongGroupByFunction function = new CountLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(0L, function.getLong(value));
        }
    }

    @Test
    public void testCountLongBatchZeroCountKeepsZero() {
        CountLongGroupByFunction function = new CountLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.computeBatch(value, 0, 0, 0);

            Assert.assertEquals(0L, function.getLong(value));
        }
    }

    @Test
    public void testCountLongSetEmpty() {
        CountLongGroupByFunction function = new CountLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(0L, function.getLong(value));
        }
    }

    @Test
    public void testFirstLongBatch() {
        FirstLongGroupByFunction function = new FirstLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(5, 6, 7);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(5L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testFirstLongBatchAccumulates() {
        FirstLongGroupByFunction function = new FirstLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(5, 6);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateLongs(7, 8);
            function.computeBatch(value, ptr, 2, 2);

            Assert.assertEquals(5L, function.getLong(value));
        }
    }

    @Test
    public void testFirstLongBatchAllNull() {
        FirstLongGroupByFunction function = new FirstLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(Numbers.LONG_NULL, 1);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    @Test
    public void testFirstLongBatchEmpty() {
        FirstLongGroupByFunction function = new FirstLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            function.computeBatch(value, 0, 0, 0);

            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    @Test
    public void testFirstLongBatchNotCalled() {
        FirstLongGroupByFunction function = new FirstLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    @Test
    public void testFirstLongSetEmpty() {
        FirstLongGroupByFunction function = new FirstLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    @Test
    public void testFirstNotNullLongBatch() {
        FirstNotNullLongGroupByFunction function = new FirstNotNullLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(Numbers.LONG_NULL, 100L, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(100L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testFirstNotNullLongBatchAccumulates() {
        FirstNotNullLongGroupByFunction function = new FirstNotNullLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(Numbers.LONG_NULL, 100L);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateLongs(200L, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 2, 2);

            Assert.assertEquals(100L, function.getLong(value));
        }
    }

    @Test
    public void testLastLongBatch() {
        LastLongGroupByFunction function = new LastLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateLongs(11, 22, 33);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(2, value.getLong(0));
            Assert.assertEquals(33L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastLongBatchAccumulates() {
        LastLongGroupByFunction function = new LastLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateLongs(11, 22);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateLongs(33, 44);
            function.computeBatch(value, ptr, 2, 2);

            Assert.assertEquals(44L, function.getLong(value));
        }
    }

    @Test
    public void testLastLongBatchAllNull() {
        LastLongGroupByFunction function = new LastLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateLongs(11, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(1, value.getLong(0));
            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    @Test
    public void testLastLongSetEmpty() {
        LastLongGroupByFunction function = new LastLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    @Test
    public void testLastNotNullLongBatch() {
        LastNotNullLongGroupByFunction function = new LastNotNullLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateLongs(Numbers.LONG_NULL, 5L, Numbers.LONG_NULL, 10L);
            function.computeBatch(value, ptr, 4, 0);

            Assert.assertEquals(10L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastNotNullLongBatchAccumulates() {
        LastNotNullLongGroupByFunction function = new LastNotNullLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateLongs(5L, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateLongs(Numbers.LONG_NULL, 10L);
            function.computeBatch(value, ptr, 2, 2);

            Assert.assertEquals(10L, function.getLong(value));
        }
    }

    @Test
    public void testLastNotNullLongBatchKeepsHigherRowIdNonNull() {
        LastNotNullLongGroupByFunction function = new LastNotNullLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            // A stored non-null must survive a batch that arrives at a lower rowId. See the class javadoc.
            long ptr = allocateLongs(99L);
            function.computeBatch(value, ptr, 1, 100);
            Assert.assertEquals(99L, function.getLong(value));

            ptr = allocateLongs(42L);
            function.computeBatch(value, ptr, 1, 10);

            Assert.assertEquals(99L, function.getLong(value));
        }
    }

    @Test
    public void testLastNotNullLongBatchReplacesStoredNull() {
        LastNotNullLongGroupByFunction function = new LastNotNullLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            // computeFirst writes NULL through with a real rowId; a non-null at a lower rowId must still
            // replace it. See the class javadoc.
            function.computeFirst(value, NULL_RECORD, 100);

            long ptr = allocateLongs(42L);
            function.computeBatch(value, ptr, 1, 10);

            Assert.assertEquals(42L, function.getLong(value));
        }
    }

    @Test
    public void testMaxLongBatch() {
        MaxLongGroupByFunction function = new MaxLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putLong(0, -999);

            long ptr = allocateLongs(-10, Numbers.LONG_NULL, 15, 7);
            function.computeBatch(value, ptr, 4, 0);

            Assert.assertEquals(15L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMaxLongBatchAccumulates() {
        MaxLongGroupByFunction function = new MaxLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(1, 5);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateLongs(3, 2);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(5L, function.getLong(value));
        }
    }

    @Test
    public void testMaxLongBatchAllNull() {
        MaxLongGroupByFunction function = new MaxLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    @Test
    public void testMaxLongSetEmpty() {
        MaxLongGroupByFunction function = new MaxLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    @Test
    public void testMinLongBatch() {
        MinLongGroupByFunction function = new MinLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putLong(0, 999);

            long ptr = allocateLongs(Numbers.LONG_NULL, 4, 2, 3);
            function.computeBatch(value, ptr, 4, 0);

            Assert.assertEquals(2L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testMinLongBatchAccumulates() {
        MinLongGroupByFunction function = new MinLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(5, 3);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateLongs(4, 1);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(1L, function.getLong(value));
        }
    }

    @Test
    public void testMinLongBatchAllNull() {
        MinLongGroupByFunction function = new MinLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(Numbers.LONG_NULL, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    @Test
    public void testMinLongSetEmpty() {
        MinLongGroupByFunction function = new MinLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    @Test
    public void testSumLongBatch() {
        SumLongGroupByFunction function = new SumLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(1, 2, 3, 4);
            function.computeBatch(value, ptr, 4, 0);

            Assert.assertEquals(10L, function.getLong(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testSumLongBatchAccumulates() {
        SumLongGroupByFunction function = new SumLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(1, 2);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateLongs(3, 4);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(10L, function.getLong(value));
        }
    }

    @Test
    public void testSumLongBatchAllNull() {
        SumLongGroupByFunction function = new SumLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateLongs(Numbers.LONG_NULL, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }

    @Test
    public void testSumLongBatchZeroCountKeepsExistingValue() {
        SumLongGroupByFunction function = new SumLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            value.putLong(0, 55);

            function.computeBatch(value, 0, 0, 0);

            Assert.assertEquals(55L, function.getLong(value));
        }
    }

    @Test
    public void testSumLongSetEmpty() {
        SumLongGroupByFunction function = new SumLongGroupByFunction(LongColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(Numbers.LONG_NULL, function.getLong(value));
        }
    }
}
